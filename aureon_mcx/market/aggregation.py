"""Closed-only timeframe aggregation with explicit constituent completeness.

Every emitted bar knows whether all expected source intervals were present:

  COMPLETE      all expected constituent open-times are present -> analysable
  INCOMPLETE    bucket still open (boundary not passed) -> never emitted
  GAP_DETECTED  boundary passed with constituents missing -> NOT analysable

Rules
  * a target bar is emitted only when its boundary has passed (a constituent
    closed on the boundary, a later bucket arrived, or `flush_at(now)`);
  * expected constituents come from a `BucketPolicy` (calendar-aware: only
    open-market intervals are expected, the last bucket of the day is shorter);
  * duplicates never count twice, misaligned open-times never count at all,
    out-of-order delivery is tolerated (OHLC is built from sorted constituents);
  * a bucket is never re-opened once emitted.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import TYPE_CHECKING, Protocol

from .candle import Candle
from .timeframe import Timeframe
from .timeutil import IST, ensure_utc, floor_to

if TYPE_CHECKING:  # pragma: no cover
    from .sessions import SessionCalendar

log = logging.getLogger("aureon.aggregation")


class Completeness(str, Enum):
    COMPLETE = "COMPLETE"
    INCOMPLETE = "INCOMPLETE"
    GAP_DETECTED = "GAP_DETECTED"


@dataclass(frozen=True)
class AggregationResult:
    candle: Candle
    status: Completeness
    expected: int
    present: int
    missing: tuple[datetime, ...] = ()

    @property
    def is_complete(self) -> bool:
        return self.status is Completeness.COMPLETE


# ------------------------------------------------------------------ policies
class BucketPolicy(Protocol):
    def bucket(self, ts: datetime, target: Timeframe) -> tuple[datetime, datetime]: ...
    def expected_opens(self, start: datetime, end: datetime, source: Timeframe) -> list[datetime]: ...


class ClockBucketPolicy:
    """Wall-clock buckets floored from local midnight; every interval is expected."""

    def __init__(self, tz=IST):
        self.tz = tz

    def bucket(self, ts: datetime, target: Timeframe) -> tuple[datetime, datetime]:
        start = floor_to(ts, target.seconds, self.tz)
        return start, start + timedelta(seconds=target.seconds)

    def expected_opens(self, start: datetime, end: datetime, source: Timeframe) -> list[datetime]:
        out, t = [], start
        while t < end:
            out.append(t)
            t += timedelta(seconds=source.seconds)
        return out


class ExchangeBucketPolicy:
    """Calendar-aware buckets: clock-aligned M5/M15/H1 capped at the session end,
    session-anchored H4, and only open-market intervals expected."""

    def __init__(self, calendar: "SessionCalendar"):
        self.calendar = calendar
        self.tz = calendar.tz

    def bucket(self, ts: datetime, target: Timeframe) -> tuple[datetime, datetime]:
        ts = ensure_utc(ts)
        if target is Timeframe.H4:
            return self.calendar.h4_bucket(ts)
        start = floor_to(ts, target.seconds, self.tz)
        nominal_end = start + timedelta(seconds=target.seconds)
        d = self.calendar.trading_date(ts)
        if self.calendar.is_trading_day(d):
            day_start, day_end = self.calendar.trading_day_span(d)
            if start < day_end <= nominal_end and start >= day_start:
                return start, day_end  # last bucket of the day is shorter
        return start, nominal_end

    def expected_opens(self, start: datetime, end: datetime, source: Timeframe) -> list[datetime]:
        out, t = [], ensure_utc(start)
        while t < end:
            if self.calendar.is_open(t):
                out.append(t)
            t += timedelta(seconds=source.seconds)
        return out


# ---------------------------------------------------------------- aggregator
@dataclass
class _Bucket:
    open_time: datetime
    end_time: datetime
    expected: list[datetime]
    constituents: dict[datetime, Candle] = field(default_factory=dict)
    extra: dict[datetime, Candle] = field(default_factory=dict)  # outside the expected set but inside the bucket


class TimeframeAggregator:
    def __init__(self, source: Timeframe, target: Timeframe, symbol: str, security_id: str, expiry_date: str = "",
                 tz=IST, policy: BucketPolicy | None = None, calendar: "SessionCalendar | None" = None):
        if target.seconds <= source.seconds or target.seconds % source.seconds != 0:
            raise ValueError(f"cannot aggregate {source.value} into {target.value}")
        self.source = source
        self.target = target
        self.symbol = symbol
        self.security_id = security_id
        self.expiry_date = expiry_date
        self.tz = tz
        if policy is not None:
            self.policy = policy
        elif calendar is not None:
            self.policy = ExchangeBucketPolicy(calendar)
        else:
            self.policy = ClockBucketPolicy(tz)
        self._bucket: _Bucket | None = None
        self._last_emitted_open: datetime | None = None
        self.results: list[AggregationResult] = []  # audit trail of GAP results (bounded)
        self._gap_buckets: dict[datetime, _Bucket] = {}  # emitted as GAP; completed later by verified constituents

    @property
    def expected_constituents(self) -> int:
        """Nominal count for a full bucket; the policy decides the real expectation per bucket."""
        return self.target.seconds // self.source.seconds

    def _open_bucket(self, ts: datetime) -> _Bucket:
        start, end = self.policy.bucket(ts, self.target)
        return _Bucket(open_time=start, end_time=end, expected=self.policy.expected_opens(start, end, self.source))

    def _emit(self) -> AggregationResult | None:
        b = self._bucket
        assert b is not None
        self._bucket = None
        self._last_emitted_open = b.open_time
        if not b.expected:
            # closed-market bucket (holiday / outside session): nothing is expected, nothing is emitted
            log.debug("aggregation_bucket_dropped symbol=%s target=%s bucket=%s reason=no_expected_constituents", self.symbol,
                      self.target.value, b.open_time.isoformat())
            return None
        ordered = sorted({**b.extra, **b.constituents}.values(), key=lambda c: c.open_time)
        first, last = ordered[0], ordered[-1]
        oi = next((c.open_interest for c in reversed(ordered) if c.open_interest is not None), None)
        candle = Candle(
            symbol=self.symbol, security_id=self.security_id, timeframe=self.target, open_time=b.open_time, open=first.open,
            high=max(c.high for c in ordered), low=min(c.low for c in ordered), close=last.close,
            volume=sum(c.volume for c in ordered), open_interest=oi, source="aggregated", is_closed=True, expiry_date=self.expiry_date,
        )
        missing = tuple(t for t in b.expected if t not in b.constituents)
        status = Completeness.COMPLETE if not missing else Completeness.GAP_DETECTED
        res = AggregationResult(candle=candle, status=status, expected=len(b.expected), present=len(b.constituents), missing=missing)
        if status is Completeness.GAP_DETECTED:
            log.warning("aggregation_gap symbol=%s target=%s bucket=%s expected=%d present=%d missing=%s", self.symbol,
                        self.target.value, b.open_time.isoformat(), len(b.expected), len(b.constituents),
                        ",".join(m.isoformat() for m in missing[:6]))
            self.results.append(res)
            if len(self.results) > 200:
                del self.results[:-100]
            self._gap_buckets[b.open_time] = b
            if len(self._gap_buckets) > 50:
                for k in sorted(self._gap_buckets)[:-25]:
                    self._gap_buckets.pop(k, None)
        return res

    def try_complete_gap(self, candle: Candle) -> Candle | None:
        """A late (broker-verified) constituent for a bucket already emitted as GAP: add it and,
        once every expected constituent is present, return the rebuilt COMPLETE bar so the
        pipeline can repair the gap without a separate broker fetch at the target timeframe."""
        if candle.timeframe != self.source or not candle.is_closed:
            return None
        ts = ensure_utc(candle.open_time)
        start, _ = self.policy.bucket(ts, self.target)
        b = self._gap_buckets.get(start)
        if b is None or ts not in b.expected or ts in b.constituents:
            return None
        b.constituents[ts] = candle
        if any(t not in b.constituents for t in b.expected):
            return None
        self._gap_buckets.pop(start, None)
        ordered = sorted(b.constituents.values(), key=lambda c: c.open_time)
        oi = next((c.open_interest for c in reversed(ordered) if c.open_interest is not None), None)
        return Candle(symbol=self.symbol, security_id=self.security_id, timeframe=self.target, open_time=b.open_time, open=ordered[0].open,
                      high=max(c.high for c in ordered), low=min(c.low for c in ordered), close=ordered[-1].close,
                      volume=sum(c.volume for c in ordered), open_interest=oi, source="aggregated", is_closed=True, expiry_date=self.expiry_date)

    def add(self, candle: Candle) -> list[AggregationResult]:
        """Feed a CLOSED source candle. Returns 0..2 results (a bucket closed by a later
        constituent, and/or the bucket closed by this constituent on its boundary)."""
        if not candle.is_closed:
            raise ValueError("aggregator only accepts closed source candles")
        if candle.timeframe != self.source:
            raise ValueError(f"expected {self.source.value} candle, got {candle.timeframe.value}")
        ts = ensure_utc(candle.open_time)
        if floor_to(ts, self.source.seconds, self.tz) != ts:
            log.warning("aggregation_misaligned symbol=%s source=%s open_time=%s", self.symbol, self.source.value, ts.isoformat())
            return []
        start, _ = self.policy.bucket(ts, self.target)
        if self._last_emitted_open is not None and start <= self._last_emitted_open:
            log.debug("aggregation_late_constituent symbol=%s target=%s open_time=%s", self.symbol, self.target.value, ts.isoformat())
            return []  # bucket already emitted: never re-open a closed bar
        out: list[AggregationResult] = []
        if self._bucket is not None and start > self._bucket.open_time:
            res = self._emit()  # a later bucket arrived: the previous boundary has passed
            if res is not None:
                out.append(res)
        elif self._bucket is not None and start < self._bucket.open_time:
            return out  # older than the open bucket and not emitted: out-of-order beyond repair window
        if self._bucket is None:
            self._bucket = self._open_bucket(ts)
        b = self._bucket
        if ts in b.constituents or ts in b.extra:
            return out  # duplicate constituent: never counted twice
        if ts in b.expected:
            b.constituents[ts] = candle
        else:
            b.extra[ts] = candle  # inside the bucket but outside configured market hours
            log.warning("aggregation_outside_session symbol=%s source=%s open_time=%s", self.symbol, self.source.value, ts.isoformat())
        # Emit on this constituent's own boundary only when the bucket is complete. An
        # incomplete bucket stays open so out-of-order constituents can still land; a
        # later bucket or `flush_at(now)` then closes it (as GAP_DETECTED if still short).
        if b.expected and all(t in b.constituents for t in b.expected):
            last_close = max(c.close_time for c in b.constituents.values())
            if last_close >= b.end_time:
                res = self._emit()
                if res is not None:
                    out.append(res)
        return out

    def mark_emitted_until(self, open_time: datetime) -> None:
        """Treat every bucket up to and including the one containing `open_time` as emitted."""
        start, _ = self.policy.bucket(ensure_utc(open_time), self.target)
        if self._last_emitted_open is None or start > self._last_emitted_open:
            self._last_emitted_open = start
            if self._bucket is not None and self._bucket.open_time <= start:
                self._bucket = None

    def flush_at(self, now: datetime) -> AggregationResult | None:
        """Emit the open bucket if wall-clock `now` is past its boundary."""
        if self._bucket is not None and ensure_utc(now) >= self._bucket.end_time:
            return self._emit()
        return None

    @property
    def partial(self) -> Candle | None:
        b = self._bucket
        if b is None or not (b.constituents or b.extra):
            return None
        ordered = sorted({**b.extra, **b.constituents}.values(), key=lambda c: c.open_time)
        return Candle(symbol=self.symbol, security_id=self.security_id, timeframe=self.target, open_time=b.open_time,
                      open=ordered[0].open, high=max(c.high for c in ordered), low=min(c.low for c in ordered), close=ordered[-1].close,
                      volume=sum(c.volume for c in ordered), open_interest=ordered[-1].open_interest, source="aggregated",
                      is_closed=False, expiry_date=self.expiry_date)

    @property
    def open_bucket_status(self) -> tuple[datetime, int, int] | None:
        b = self._bucket
        return (b.open_time, len(b.expected), len(b.constituents)) if b else None


def aggregate_closed(candles: list[Candle], target: Timeframe, tz=IST, now: datetime | None = None,
                     calendar: "SessionCalendar | None" = None, include_gaps: bool = False) -> list[Candle]:
    """Batch aggregation of a closed source series. Only COMPLETE bars are returned unless
    `include_gaps`; the trailing partial bucket is dropped unless `now` is past it."""
    if not candles:
        return []
    first = candles[0]
    agg = TimeframeAggregator(first.timeframe, target, first.symbol, first.security_id, first.expiry_date, tz, calendar=calendar)
    results: list[AggregationResult] = []
    for c in sorted(candles, key=lambda c: c.open_time):
        results += agg.add(c)
    if now is not None:
        tail = agg.flush_at(now)
        if tail is not None:
            results.append(tail)
    return [r.candle for r in results if r.is_complete or include_gaps]


def aggregate_with_status(candles: list[Candle], target: Timeframe, calendar: "SessionCalendar | None" = None,
                          now: datetime | None = None, tz=IST) -> list[AggregationResult]:
    if not candles:
        return []
    first = candles[0]
    agg = TimeframeAggregator(first.timeframe, target, first.symbol, first.security_id, first.expiry_date, tz, calendar=calendar)
    results: list[AggregationResult] = []
    for c in sorted(candles, key=lambda c: c.open_time):
        results += agg.add(c)
    if now is not None:
        tail = agg.flush_at(now)
        if tail is not None:
            results.append(tail)
    return results
