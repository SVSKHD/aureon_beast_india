"""Tick -> M1 -> primary (M5) -> M15 / H1 / H4 closed-candle pipeline with continuity control.

Analysis callbacks fire ONLY on COMPLETE closed candles, exactly once per bar per
timeframe, in chronological order. Ticks never reach analysis.

Continuity rules
  * an M1 built from live ticks is TRUSTED only when trustworthy feed coverage existed
    from the minute's very start (`coverage_start <= minute open`). A minute that began
    before the connection (mid-minute connect / reconnect / rollover / feed restart) is
    PARTIAL: it is never fed to aggregation, it is queued for broker verification and the
    exact historical M1 replaces it;
  * a connected socket does NOT prove coverage: an open-market minute that produced no
    tick is a SILENT minute. It is never turned into a flat bar; it is queued for broker
    verification (exact historical M1). Only an explicitly enabled, documented
    zero-trade rule may fill it when the broker verifiably has no bar (see
    `verify_m1`); by default the minute stays unresolved and the symbol untrusted;
  * an aggregated bar with missing constituents (GAP_DETECTED) is never dispatched;
    the pipeline records the gap, suspends dispatch (analytics for the symbol fail
    closed) and keeps later complete bars deferred until the gap is repaired with
    the exact broker candle (`repair`);
  * during recovery (`recovering=True`) live ticks are buffered and replayed after
    the historical M1 candles have been fed, preserving chronological order.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import TYPE_CHECKING, Callable, NamedTuple

from .aggregation import AggregationResult, Completeness, TimeframeAggregator
from .candle import Candle
from .timeframe import Timeframe
from .timeutil import IST, ensure_utc, floor_to, utc_now

if TYPE_CHECKING:  # pragma: no cover
    from .sessions import SessionCalendar

log = logging.getLogger("aureon.candles")

REASON_PARTIAL = "partial_coverage"
REASON_SILENT = "silent_feed"


@dataclass(frozen=True)
class Tick:
    security_id: str
    price: float
    ts: datetime
    last_qty: float | None = None       # per-trade quantity (preferred for volume)
    day_volume: float | None = None     # cumulative day volume (fallback: delta)
    open_interest: float | None = None


@dataclass
class GapRecord:
    timeframe: Timeframe
    open_time: datetime
    expected: int
    present: int
    missing: tuple[datetime, ...]
    detected_at: datetime
    resolved_at: datetime | None = None

    @property
    def resolved(self) -> bool:
        return self.resolved_at is not None


class BuiltM1(NamedTuple):
    candle: Candle
    trusted: bool


class M1CandleBuilder:
    """Builds M1 bars from ticks and knows whether each bar had full coverage."""

    def __init__(self, symbol: str, security_id: str, expiry_date: str = "", tz=IST,
                 is_open: Callable[[datetime], bool] | None = None):
        self.symbol = symbol
        self.security_id = security_id
        self.expiry_date = expiry_date
        self.tz = tz
        self.is_open = is_open or (lambda ts: True)
        self._open_time: datetime | None = None
        self._o = self._h = self._l = self._c = 0.0
        self._vol = 0.0
        self._oi: float | None = None
        self._last_day_volume: float | None = None
        self._last_emitted: datetime | None = None
        self._last_close: float | None = None
        self._bucket_trusted = False
        self.coverage_start: datetime | None = None  # instant from which live coverage is trustworthy
        self.last_tick_at: datetime | None = None

    # -- coverage state ---------------------------------------------------
    def set_connected(self, ts: datetime) -> None:
        """Trustworthy coverage begins now. The minute containing `ts` is partial unless
        `ts` is exactly its open."""
        self.coverage_start = ensure_utc(ts)
        if self._open_time is not None and self._open_time < self.coverage_start:
            self._bucket_trusted = False

    def set_disconnected(self) -> None:
        self.coverage_start = None
        self._bucket_trusted = False  # whatever is open lost its coverage

    def minute_trusted(self, minute: datetime) -> bool:
        return self.coverage_start is not None and self.coverage_start <= ensure_utc(minute)

    @property
    def open_minute(self) -> datetime | None:
        return self._open_time

    @property
    def last_emitted(self) -> datetime | None:
        return self._last_emitted

    def mark_emitted_until(self, open_time: datetime) -> None:
        ts = floor_to(ensure_utc(open_time), 60, self.tz)
        if self._last_emitted is None or ts > self._last_emitted:
            self._last_emitted = ts
            if self._open_time is not None and self._open_time <= ts:
                self._open_time = None

    def _emit(self) -> BuiltM1:
        assert self._open_time is not None
        c = Candle(symbol=self.symbol, security_id=self.security_id, timeframe=Timeframe.M1, open_time=self._open_time,
                   open=self._o, high=self._h, low=self._l, close=self._c, volume=self._vol, open_interest=self._oi,
                   source="dhan" if self._bucket_trusted else "partial", is_closed=True, expiry_date=self.expiry_date)
        self._last_emitted = self._open_time
        self._last_close = self._c
        self._open_time = None
        return BuiltM1(c, self._bucket_trusted)

    def add_tick(self, tick: Tick) -> list[BuiltM1]:
        ts = ensure_utc(tick.ts)
        self.last_tick_at = ts
        start = floor_to(ts, 60, self.tz)
        if self._last_emitted is not None and start <= self._last_emitted:
            return []  # late tick for a closed minute: ignore, never reopen
        out: list[BuiltM1] = []
        if self._open_time is not None and start > self._open_time:
            out.append(self._emit())
        if self._open_time is None:
            self._open_time = start
            self._bucket_trusted = self.minute_trusted(start)
            self._o = self._h = self._l = self._c = tick.price
            self._vol = 0.0
            self._oi = None
        self._h = max(self._h, tick.price)
        self._l = min(self._l, tick.price)
        self._c = tick.price
        if tick.last_qty is not None:
            self._vol += tick.last_qty
        elif tick.day_volume is not None:
            if self._last_day_volume is not None and tick.day_volume >= self._last_day_volume:
                self._vol += tick.day_volume - self._last_day_volume
            self._last_day_volume = tick.day_volume
        if tick.open_interest is not None:
            self._oi = tick.open_interest
        return out

    def flush_at(self, now: datetime) -> list[BuiltM1]:
        """Close the open minute once the wall clock passed its end. Never invents bars."""
        now = ensure_utc(now)
        if self._open_time is not None and now >= self._open_time + timedelta(seconds=60):
            return [self._emit()]
        return []

    @property
    def partial(self) -> Candle | None:
        if self._open_time is None:
            return None
        return Candle(symbol=self.symbol, security_id=self.security_id, timeframe=Timeframe.M1, open_time=self._open_time,
                      open=self._o, high=self._h, low=self._l, close=self._c, volume=self._vol, open_interest=self._oi,
                      is_closed=False, expiry_date=self.expiry_date)


ClosedHandler = Callable[[Candle], None]
GapHandler = Callable[[GapRecord], None]
PendingHandler = Callable[[datetime, str], None]


class CandlePipeline:
    """Per-symbol pipeline: ticks -> M1 -> primary (M5) -> higher timeframes.

    `on_closed(candle)` is invoked once per COMPLETE closed candle for every timeframe
    in `timeframes` (primary first, then higher ones as their boundaries pass).
    """

    def __init__(self, symbol: str, security_id: str, expiry_date: str, primary: Timeframe, timeframes: list[Timeframe],
                 on_closed: ClosedHandler, tz=IST, calendar: "SessionCalendar | None" = None, on_gap: GapHandler | None = None,
                 clock: Callable[[], datetime] | None = None, on_pending: PendingHandler | None = None):
        if primary == Timeframe.M1:
            raise ValueError("primary timeframe must be above M1")
        self.symbol = symbol
        self.security_id = security_id
        self.expiry_date = expiry_date
        self.primary = primary
        self.timeframes = [t for t in timeframes if t != Timeframe.M1]
        self.on_closed = on_closed
        self.on_gap = on_gap or (lambda g: None)
        self.on_pending = on_pending or (lambda m, r: None)
        self.calendar = calendar
        self.tz = tz
        self._clock = clock or utc_now
        is_open = calendar.is_open if calendar is not None else None
        self.is_open = is_open or (lambda ts: True)
        self.m1 = M1CandleBuilder(symbol, security_id, expiry_date, tz, is_open)
        self.primary_agg = TimeframeAggregator(Timeframe.M1, primary, symbol, security_id, expiry_date, tz, calendar=calendar)
        self.higher: dict[Timeframe, TimeframeAggregator] = {}
        for tf in self.timeframes:
            if tf == primary:
                continue
            src = primary if tf.seconds % primary.seconds == 0 else Timeframe.M1
            if tf == Timeframe.H4:
                src = Timeframe.H1 if Timeframe.H1 in self.timeframes else primary
            self.higher[tf] = TimeframeAggregator(src, tf, symbol, security_id, expiry_date, tz, calendar=calendar)
        self.closed_counts: dict[Timeframe, int] = {tf: 0 for tf in self.timeframes}
        self.last_closed: dict[Timeframe, datetime] = {}
        self.last_m1_open: datetime | None = None
        # continuity control
        self.gaps: list[GapRecord] = []
        self.suspended = False
        self.recovering = False
        self._deferred: list[Candle] = []
        self._tick_buffer: list[Tick] = []
        self._seen_m1: set[datetime] = set()
        # minutes whose live coverage is not trustworthy (partial / silent): verified against the broker
        self.pending_verification: dict[datetime, str] = {}
        self.verified_minutes: int = 0

    # ----------------------------------------------------------- properties
    @property
    def unresolved_gaps(self) -> list[GapRecord]:
        return [g for g in self.gaps if not g.resolved]

    @property
    def continuity_ok(self) -> bool:
        return not self.suspended and not self.unresolved_gaps and not self.pending_verification

    def pending_minutes(self) -> list[datetime]:
        return sorted(self.pending_verification)

    @property
    def last_m1_close(self) -> datetime | None:
        return self.last_m1_open + timedelta(minutes=1) if self.last_m1_open else None

    # ---------------------------------------------------------- connection
    def set_connected(self, ts: datetime) -> None:
        self.m1.set_connected(ts)

    def set_disconnected(self) -> None:
        self.m1.set_disconnected()

    @property
    def coverage_start(self) -> datetime | None:
        return self.m1.coverage_start

    # ------------------------------------------------------------ dispatch
    def _handle(self, res: AggregationResult) -> None:
        if res.status is Completeness.GAP_DETECTED:
            gap = GapRecord(res.candle.timeframe, res.candle.open_time, res.expected, res.present, res.missing, self._clock())
            self.gaps.append(gap)
            self.suspended = True
            log.error("market_data_gap symbol=%s tf=%s open_time=%s expected=%d present=%d", self.symbol, res.candle.timeframe.value,
                      res.candle.open_time.isoformat(), res.expected, res.present)
            self.on_gap(gap)
            return
        if self.suspended:
            self._deferred.append(res.candle)
            return
        self._dispatch(res.candle)

    def _dispatch(self, candle: Candle) -> None:
        last = self.last_closed.get(candle.timeframe)
        if last is not None and candle.open_time <= last:
            return  # never dispatch twice or backwards
        self.closed_counts[candle.timeframe] = self.closed_counts.get(candle.timeframe, 0) + 1
        self.last_closed[candle.timeframe] = candle.open_time
        self.on_closed(candle)
        for agg in self.higher.values():
            if agg.source == candle.timeframe:
                for res in agg.add(candle):
                    self._handle(res)

    def on_m1_closed(self, m1: Candle) -> None:
        if m1.open_time in self._seen_m1:
            return  # duplicate delivery
        self._seen_m1.add(m1.open_time)
        if len(self._seen_m1) > 4096:
            for t in sorted(self._seen_m1)[:2048]:
                self._seen_m1.discard(t)
        if self.last_m1_open is None or m1.open_time > self.last_m1_open:
            self.last_m1_open = m1.open_time
        for agg in self.higher.values():
            if agg.source == Timeframe.M1:
                for res in agg.add(m1):
                    self._handle(res)
        results = self.primary_agg.add(m1)
        for res in results:
            self._handle(res)
        if not results:
            rebuilt = self.primary_agg.try_complete_gap(m1)  # late verified minute completing a GAP bucket
            if rebuilt is not None:
                self.repair(rebuilt)

    def _mark_pending(self, minute: datetime, reason: str) -> None:
        if minute in self._seen_m1 or minute in self.pending_verification:
            return
        self.pending_verification[minute] = reason
        log.warning("m1_needs_verification symbol=%s minute=%s reason=%s", self.symbol, minute.isoformat(), reason)
        self.on_pending(minute, reason)

    def _on_built(self, built: BuiltM1) -> None:
        if built.trusted:
            self.on_m1_closed(built.candle)
        else:
            self._mark_pending(built.candle.open_time, REASON_PARTIAL)

    def add_tick(self, tick: Tick) -> None:
        if self.recovering:
            self._tick_buffer.append(tick)
            return
        for built in self.m1.add_tick(tick):
            self._on_built(built)

    def detect_silent_minutes(self, now: datetime) -> list[datetime]:
        """Open-market minutes that fully elapsed while coverage was supposedly trustworthy but
        produced no M1 (no tick at all). They are queued for broker verification, never
        synthesised. Minutes before `coverage_start` belong to reconnect recovery."""
        cs = self.m1.coverage_start
        if cs is None:
            return []
        now = ensure_utc(now)
        current_minute = floor_to(now, 60, self.tz)
        first = floor_to(cs, 60, self.tz)
        if self.last_m1_open is not None and self.last_m1_open + timedelta(minutes=1) > first:
            first = self.last_m1_open + timedelta(minutes=1)
        found: list[datetime] = []
        m = first
        while m < current_minute:
            if m not in self._seen_m1 and m not in self.pending_verification and m != self.m1.open_minute and self.is_open(m):
                reason = REASON_PARTIAL if cs > m else REASON_SILENT
                self._mark_pending(m, reason)
                found.append(m)
            m += timedelta(minutes=1)
        return found

    def flush_at(self, now: datetime) -> None:
        """Wall-clock boundary check: closes bars whose boundary passed with no new tick and
        queues silent minutes for verification."""
        if self.recovering:
            return
        for built in self.m1.flush_at(now):
            self._on_built(built)
        self.detect_silent_minutes(now)
        res = self.primary_agg.flush_at(now)
        if res is not None:
            self._handle(res)
        for agg in self.higher.values():
            res = agg.flush_at(now)
            if res is not None:
                self._handle(res)

    # ------------------------------------------------------------ recovery
    def recovery_start(self) -> datetime | None:
        """Where a historical backfill must start: the minute left open at disconnect (it may
        have been only partially observed and is replaced by the broker's candle), else the
        minute after the last fully processed M1."""
        if self.m1._open_time is not None:
            return self.m1._open_time
        return self.last_m1_close

    def begin_recovery(self) -> None:
        self.recovering = True

    def verify_m1(self, candles: list[Candle], now: datetime, allow_zero_trade_fill: bool = False,
                  fetched_range: tuple[datetime, datetime] | None = None) -> tuple[list[datetime], list[datetime]]:
        """Resolve pending minutes with exact broker M1 candles. Returns (verified, still_pending).

        A minute with no broker bar stays pending unless `allow_zero_trade_fill` is on AND the
        broker response verifiably spans that minute (bars exist both before and after it inside
        the fetched range). That rule is documented in config: it treats a missing bar strictly
        inside a returned range as a zero-trade minute. It is OFF by default (fail closed)."""
        now = ensure_utc(now)
        by_time = {c.open_time: c for c in candles if c.is_closed and c.timeframe is Timeframe.M1}
        verified: list[datetime] = []
        for minute in self.pending_minutes():
            if minute + timedelta(minutes=1) > now:
                continue  # still open: cannot be verified yet
            candle = by_time.get(minute)
            if candle is None and allow_zero_trade_fill and by_time:
                before = [t for t in by_time if t < minute]
                after = [t for t in by_time if t > minute]
                inside = fetched_range is not None and fetched_range[0] <= minute < fetched_range[1]
                if before and after and inside:
                    p = by_time[max(before)].close
                    candle = Candle(symbol=self.symbol, security_id=self.security_id, timeframe=Timeframe.M1, open_time=minute, open=p,
                                    high=p, low=p, close=p, volume=0.0, open_interest=by_time[max(before)].open_interest,
                                    source="zero_trade_verified", is_closed=True, expiry_date=self.expiry_date)
                    log.warning("m1_zero_trade_fill symbol=%s minute=%s", self.symbol, minute.isoformat())
            if candle is None:
                continue
            self.pending_verification.pop(minute, None)
            self.m1.mark_emitted_until(minute)
            self.m1._last_close = candle.close
            self.on_m1_closed(candle)
            self.verified_minutes += 1
            verified.append(minute)
        return verified, self.pending_minutes()

    def recover_m1(self, candles: list[Candle]) -> int:
        """Feed historical CLOSED M1 candles (chronological) to fill a data gap. Minutes already
        processed are skipped; the M1 builder is advanced so late ticks for them are ignored."""
        fed = 0
        for c in sorted(candles, key=lambda c: c.open_time):
            if not c.is_closed or c.timeframe is not Timeframe.M1:
                continue
            if c.open_time in self._seen_m1:
                continue
            if self.m1._open_time is not None and c.open_time == self.m1._open_time:
                self.m1._open_time = None  # partially observed minute: the broker candle replaces it
            self.m1.mark_emitted_until(c.open_time)
            self.m1._last_close = c.close
            self.pending_verification.pop(c.open_time, None)
            self.on_m1_closed(c)
            fed += 1
        return fed

    def end_recovery(self) -> None:
        self.recovering = False
        buffered, self._tick_buffer = self._tick_buffer, []
        for t in buffered:
            self.add_tick(t)

    def repair(self, candle: Candle) -> bool:
        """Repair the oldest unresolved gap with the exact broker candle for that bucket."""
        gap = next((g for g in self.gaps if not g.resolved), None)
        if gap is None or candle.timeframe is not gap.timeframe or candle.open_time != gap.open_time or not candle.is_closed:
            return False
        gap.resolved_at = self._clock()
        log.warning("market_data_gap_repaired symbol=%s tf=%s open_time=%s source=%s", self.symbol, candle.timeframe.value,
                    candle.open_time.isoformat(), candle.source)
        self._dispatch(candle)
        if not self.unresolved_gaps:
            self.suspended = False
        self._drain_deferred()
        return True

    def _drain_deferred(self) -> None:
        next_gap = next((g for g in self.gaps if not g.resolved), None)
        remaining: list[Candle] = []
        for c in sorted(self._deferred, key=lambda c: (c.open_time, c.timeframe.rank)):
            if next_gap is not None and c.open_time >= next_gap.open_time:
                remaining.append(c)
            else:
                self._dispatch(c)
        self._deferred = remaining

    def seed_primary_history(self, candles: list[Candle]) -> None:
        """Seed higher-timeframe aggregators from stored closed primary candles (no dispatch)."""
        for c in candles:
            for agg in self.higher.values():
                if agg.source == c.timeframe:
                    agg.add(c)
