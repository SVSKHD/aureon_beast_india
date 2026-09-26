"""Market-data continuity: reconnect recovery, minute verification, gap repair and the
persistent retry scheduler that keeps unresolved incidents alive until they are resolved.

Threading contract (see CandlePipeline / LoopBound)
  WORKER THREAD (asyncio.to_thread)   ->  `fetch_*` only: blocking Dhan HTTP, returns an
                                          immutable FetchResult. Never touches a pipeline,
                                          the application, health or Discord.
  EVENT LOOP                          ->  `apply_*`: pipeline.recover_m1 / verify_m1 /
                                          repair / reconcile_m1, health, incidents, events.

Recovery (after every feed connect):
  1. `pipeline.recovery_start()` says where processed data ends;
  2. closed M1 candles for [start, now) are fetched (worker); the still-open minute is never
     used; nothing is invented for minutes the broker did not return (zero-trade rule below);
  3. they are fed through the SAME pipeline (`recover_m1`, loop) in chronological order while
     live ticks are buffered, then buffered ticks are replayed;
  4. the symbol is LIVE only when the pipeline reports continuity.

Verification (partial / silent / suspect minutes): the exact broker M1 for each pending minute
is fetched and fed through `verify_m1`. A minute the broker does not return stays pending and
is RETRIED on the incident schedule (2, 5, 10, 20, 30, 60 s ... by default) while the market
is open and the minute is relevant, without needing a WebSocket reconnect. The
`historical.allow_verified_zero_trade_fill` rule (default off) is the only path that may turn a
verifiably-spanned missing bar into a zero-volume flat bar.

Repair (after a GAP_DETECTED bar): the exact broker candle for the gap bucket is fetched at
that timeframe and dispatched through `pipeline.repair`, which also releases deferred bars.

Incidents are persisted (`continuity_incidents`) so a restart does not forget them; they are
never deleted: they end RESOLVED or ABANDONED (trading day over), each with a recorded reason.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from typing import TYPE_CHECKING, Callable, Iterable

from aureon_mcx.broker.dhan.errors import DhanError
from aureon_mcx.broker.dhan.symbol_resolver import ResolvedContract
from aureon_mcx.health import HealthState
from aureon_mcx.logging_setup import kv
from aureon_mcx.market.candle import Candle
from aureon_mcx.market.candle_builder import REASON_BROKER_MISSING, CandlePipeline, GapRecord
from aureon_mcx.market.sessions import SessionCalendar
from aureon_mcx.market.timeframe import Timeframe
from aureon_mcx.market.timeutil import ensure_utc, floor_to
from aureon_mcx.storage.repositories import Repositories

if TYPE_CHECKING:  # pragma: no cover
    from aureon_mcx.broker.dhan.historical import HistoricalProvider

log = logging.getLogger("aureon.continuity")

DEFAULT_BACKOFF: tuple[float, ...] = (2.0, 5.0, 10.0, 20.0, 30.0, 60.0)

KIND_PENDING_MINUTE = "pending_minute"
KIND_GAP = "gap"


def fill_flat_minutes(candles: list[Candle], calendar: SessionCalendar) -> list[Candle]:
    """Zero-trade fill for minutes strictly INSIDE a returned range (bars before and after
    them). Used only when `historical.allow_verified_zero_trade_fill` is enabled; the
    default pipeline never calls it. Minutes at the edges of the range are never invented."""
    if not candles:
        return []
    out: list[Candle] = []
    prev: Candle | None = None
    for c in sorted(candles, key=lambda c: c.open_time):
        if prev is not None:
            t = prev.open_time + timedelta(minutes=1)
            while t < c.open_time:
                if calendar.is_open(t):
                    p = prev.close
                    out.append(Candle(symbol=c.symbol, security_id=c.security_id, timeframe=Timeframe.M1, open_time=t, open=p, high=p, low=p,
                                      close=p, volume=0.0, open_interest=prev.open_interest, source="zero_trade_verified", is_closed=True,
                                      expiry_date=c.expiry_date))
                t += timedelta(minutes=1)
        out.append(c)
        prev = c
    return out


# ---------------------------------------------------------------- fetch results
@dataclass(frozen=True)
class FetchResult:
    """Immutable outcome of a worker-thread fetch. `error` is set when the broker call failed."""

    candles: tuple[Candle, ...]
    start: datetime
    end: datetime
    timeframe: Timeframe
    error: str | None = None
    attempts: int = 1

    @property
    def ok(self) -> bool:
        return self.error is None


@dataclass
class RecoveryOutcome:
    fed: int
    expected: int
    missing: list[datetime]
    error: str | None = None

    @property
    def ok(self) -> bool:
        return self.error is None and not self.missing


# ------------------------------------------------------------------- incidents
@dataclass
class ContinuityIncident:
    symbol: str
    security_id: str
    kind: str
    timeframe: Timeframe
    open_time: datetime
    reason: str
    first_detected_at: datetime
    next_retry_at: datetime
    last_attempt_at: datetime | None = None
    attempt_count: int = 0
    last_error: str | None = None
    state: str = "OPEN"
    resolved_at: datetime | None = None
    resolution: str | None = None

    @property
    def key(self) -> tuple[str, str, str, datetime]:
        return (self.security_id, self.kind, self.timeframe.value, self.open_time)

    def age(self, now: datetime) -> timedelta:
        return ensure_utc(now) - self.first_detected_at

    def to_record(self) -> dict:
        return {"symbol": self.symbol, "security_id": self.security_id, "kind": self.kind, "timeframe": self.timeframe.value,
                "open_time": self.open_time.isoformat(), "reason": self.reason, "state": self.state,
                "first_detected_at": self.first_detected_at.isoformat(),
                "last_attempt_at": self.last_attempt_at.isoformat() if self.last_attempt_at else None,
                "next_retry_at": self.next_retry_at.isoformat() if self.next_retry_at else None, "attempt_count": self.attempt_count,
                "last_error": self.last_error, "resolved_at": self.resolved_at.isoformat() if self.resolved_at else None,
                "resolution": self.resolution}

    def to_dict(self) -> dict:
        return self.to_record()


class IncidentTracker:
    """Persistent retry scheduling for unresolved market-data incidents (no busy loop).

    `due(now)` returns incidents whose `next_retry_at` has passed; after a failed attempt the
    caller records it and the next retry moves along the backoff schedule (capped at its last
    value, so a persistently absent broker minute is re-requested every 60 s by default while
    the market is open)."""

    def __init__(self, repos: Repositories | None, now: Callable[[], datetime], backoff: Iterable[float] = DEFAULT_BACKOFF):
        self.repos = repos
        self._now = now
        self.backoff = tuple(float(b) for b in backoff) or DEFAULT_BACKOFF
        self.incidents: dict[tuple, ContinuityIncident] = {}
        self.resolved: list[ContinuityIncident] = []

    # -- persistence -------------------------------------------------------
    def _persist(self, inc: ContinuityIncident) -> None:
        if self.repos is None:
            return
        try:
            self.repos.incidents.upsert(inc.to_record())
        except Exception as exc:  # noqa: BLE001 - persistence must never break the data path
            log.warning("incident_persist_failed %s", kv(security_id=inc.security_id, error=type(exc).__name__))

    def load(self, active: dict[str, str]) -> list[ContinuityIncident]:
        """Restore OPEN incidents for active contracts ({security_id: symbol}) after a restart."""
        if self.repos is None:
            return []
        restored: list[ContinuityIncident] = []
        for row in self.repos.incidents.open():
            if row["security_id"] not in active:
                continue
            from aureon_mcx.market.timeutil import from_db

            inc = ContinuityIncident(symbol=row["symbol"], security_id=row["security_id"], kind=row["kind"], timeframe=Timeframe(row["timeframe"]),
                                     open_time=from_db(row["open_time"]), reason=row["reason"], first_detected_at=from_db(row["first_detected_at"]),
                                     next_retry_at=self._now(), last_attempt_at=from_db(row["last_attempt_at"]),
                                     attempt_count=int(row["attempt_count"] or 0), last_error=row["last_error"])
            self.incidents[inc.key] = inc
            restored.append(inc)
        if restored:
            log.warning("continuity_incidents_restored %s", kv(count=len(restored)))
        return restored

    # -- lifecycle ---------------------------------------------------------
    def open(self, symbol: str, security_id: str, kind: str, timeframe: Timeframe, open_time: datetime, reason: str) -> ContinuityIncident:
        key = (security_id, kind, timeframe.value, ensure_utc(open_time))
        inc = self.incidents.get(key)
        if inc is not None:
            return inc
        now = self._now()
        inc = ContinuityIncident(symbol, security_id, kind, timeframe, ensure_utc(open_time), reason, now, now)
        self.incidents[key] = inc
        self._persist(inc)
        return inc

    def attempt_started(self, incs: Iterable[ContinuityIncident]) -> None:
        now = self._now()
        for inc in incs:
            inc.last_attempt_at = now
            inc.attempt_count += 1

    def attempt_failed(self, incs: Iterable[ContinuityIncident], error: str) -> None:
        now = self._now()
        for inc in incs:
            delay = self.backoff[min(max(inc.attempt_count - 1, 0), len(self.backoff) - 1)]
            inc.next_retry_at = now + timedelta(seconds=delay)
            inc.last_error = error[:200]
            self._persist(inc)

    def resolve(self, security_id: str, kind: str, timeframe: Timeframe, open_time: datetime, resolution: str) -> ContinuityIncident | None:
        inc = self.incidents.pop((security_id, kind, timeframe.value, ensure_utc(open_time)), None)
        if inc is None:
            return None
        inc.state, inc.resolved_at, inc.resolution = "RESOLVED", self._now(), resolution
        self.resolved.append(inc)
        del self.resolved[:-200]
        self._persist(inc)
        return inc

    def abandon(self, inc: ContinuityIncident, reason: str) -> None:
        """Stop retrying but keep the record (never silently deleted)."""
        self.incidents.pop(inc.key, None)
        inc.state, inc.resolved_at, inc.resolution = "ABANDONED", self._now(), reason
        self.resolved.append(inc)
        self._persist(inc)

    # -- queries -----------------------------------------------------------
    def open_for(self, security_id: str, kind: str | None = None) -> list[ContinuityIncident]:
        return sorted((i for i in self.incidents.values() if i.security_id == security_id and (kind is None or i.kind == kind)),
                      key=lambda i: i.open_time)

    def due(self, now: datetime, security_id: str | None = None, kind: str | None = None) -> list[ContinuityIncident]:
        now = ensure_utc(now)
        return [i for i in self.incidents.values() if i.next_retry_at <= now and (security_id is None or i.security_id == security_id)
                and (kind is None or i.kind == kind)]

    def oldest_open(self, security_id: str) -> ContinuityIncident | None:
        incs = self.open_for(security_id)
        return min(incs, key=lambda i: i.first_detected_at) if incs else None

    def snapshot(self) -> dict:
        return {"open": [i.to_dict() for i in sorted(self.incidents.values(), key=lambda i: i.open_time)],
                "recent_closed": [i.to_dict() for i in self.resolved[-20:]]}


# ---------------------------------------------------------------- the service
class ContinuityService:
    def __init__(self, historical: "HistoricalProvider", repos: Repositories, calendar: SessionCalendar, health: HealthState,
                 now: Callable[[], datetime], primary: Timeframe, allow_zero_trade_fill: bool = False, retries: int = 3,
                 backoff: Iterable[float] = DEFAULT_BACKOFF):
        self.historical = historical
        self.repos = repos
        self.calendar = calendar
        self.health = health
        self._now = now
        self.primary = primary
        self.allow_zero_trade_fill = allow_zero_trade_fill
        self.retries = max(1, retries)
        self.incidents = IncidentTracker(repos, now, backoff)

    # ============================================================ WORKER SIDE
    # These methods only call the broker and return immutable results. They are the ONLY part
    # of the service that may run in a worker thread.
    def fetch_m1(self, contract: ResolvedContract, start: datetime, end: datetime, attempts: int = 1) -> FetchResult:
        start, end = ensure_utc(start), ensure_utc(end)
        error: str | None = None
        for attempt in range(1, max(1, attempts) + 1):
            try:
                fetched = self.historical.fetch(contract.logical_symbol, contract.security_id, contract.exchange_segment,
                                                contract.instrument_type, contract.expiry_iso, Timeframe.M1, start, end)
                return FetchResult(tuple(c for c in fetched if c.is_closed), start, end, Timeframe.M1, None, attempt)
            except DhanError as exc:
                error = str(exc)
                log.error("continuity_fetch_failed %s", kv(symbol=contract.logical_symbol, attempt=attempt, error=error[:160]))
        return FetchResult((), start, end, Timeframe.M1, error or "unknown", max(1, attempts))

    def fetch_gap_candle(self, contract: ResolvedContract, timeframe: Timeframe, open_time: datetime) -> FetchResult:
        open_time = ensure_utc(open_time)
        end = open_time + timedelta(seconds=timeframe.seconds)
        if timeframe.dhan_interval is None:
            return FetchResult((), open_time, end, timeframe, f"{timeframe.value} is not downloadable from Dhan")
        try:
            candles = self.historical.fetch(contract.logical_symbol, contract.security_id, contract.exchange_segment, contract.instrument_type,
                                            contract.expiry_iso, timeframe, open_time, end)
        except DhanError as exc:
            return FetchResult((), open_time, end, timeframe, str(exc))
        return FetchResult(tuple(c for c in candles if c.is_closed), open_time, end, timeframe)

    # =============================================================== LOOP SIDE
    # ---- recovery
    def recovery_window(self, pipeline: CandlePipeline, fallback_start: datetime | None) -> tuple[datetime, datetime] | None:
        now = self._now()
        current_minute = floor_to(now, 60, self.calendar.tz)
        start = pipeline.recovery_start() or fallback_start
        if start is None:
            return None
        start = floor_to(start, 60, self.calendar.tz)
        if start >= current_minute:
            return None
        return start, current_minute

    def apply_recovery(self, contract: ResolvedContract, pipeline: CandlePipeline, result: FetchResult) -> RecoveryOutcome:
        """Feed fetched closed M1 candles through the pipeline (event loop). Minutes the broker
        did not return become pending incidents (retried on the schedule), never invented."""
        sym = contract.logical_symbol
        start, end = result.start, result.end
        expected = [t for t in _minutes(start, end) if self.calendar.is_open(t)]
        if not result.ok:
            outcome = RecoveryOutcome(0, len(expected), expected, result.error)
            for m in expected:
                if m not in pipeline._seen_m1:
                    pipeline._mark_pending(m, REASON_BROKER_MISSING)
            return outcome
        closed = [c for c in result.candles if start <= c.open_time < end]
        if self.allow_zero_trade_fill:
            closed = fill_flat_minutes(closed, self.calendar)
        fed = pipeline.recover_m1(closed)
        covered = {c.open_time for c in closed}
        missing = [t for t in expected if t not in covered and t not in pipeline._seen_m1]
        log.info("continuity_recovered %s", kv(symbol=sym, fetched=len(result.candles), fed=fed, expected=len(expected), missing=len(missing)))
        for m in missing:
            pipeline._mark_pending(m, REASON_BROKER_MISSING)  # -> incident, retried; nothing invented (fail closed)
        return RecoveryOutcome(fed, len(expected), missing)

    # ---- verification
    def verification_window(self, pipeline: CandlePipeline) -> tuple[datetime, datetime] | None:
        now = self._now()
        closed_pending = [m for m in pipeline.pending_minutes() if m + timedelta(minutes=1) <= now]
        if not closed_pending:
            return None
        start = closed_pending[0] - timedelta(minutes=1)  # widen by a minute so a zero-trade rule (if enabled) can prove the span
        end = min(closed_pending[-1] + timedelta(minutes=2), floor_to(now, 60, self.calendar.tz))
        return start, end

    def apply_verification(self, contract: ResolvedContract, pipeline: CandlePipeline, result: FetchResult) -> tuple[list[datetime], list[datetime]]:
        """Resolve pending minutes with the fetched broker bars (event loop). Returns (verified, still_pending)."""
        sym = contract.logical_symbol
        if not result.ok:
            log.error("continuity_verification_failed %s", kv(symbol=sym, error=(result.error or "")[:160], pending=len(pipeline.pending_minutes())))
            return [], pipeline.pending_minutes()
        verified, still = pipeline.verify_m1(list(result.candles), self._now(), self.allow_zero_trade_fill, (result.start, result.end))
        log.info("continuity_verified %s", kv(symbol=sym, fetched=len(result.candles), verified=len(verified), pending=len(still)))
        return verified, still

    # ---- reconcile (research validation mode)
    def reconcile_window(self, pipeline: CandlePipeline, delay_seconds: float) -> tuple[datetime, datetime] | None:
        if not pipeline.reconcile_queue:
            return None
        now = self._now()
        ready = [m for m in pipeline.reconcile_queue if m + timedelta(minutes=1, seconds=delay_seconds) <= now]
        if not ready:
            return None
        return min(ready), max(ready) + timedelta(minutes=1)

    # ---- repair
    def apply_repair(self, contract: ResolvedContract, pipeline: CandlePipeline, gap: GapRecord, result: FetchResult) -> bool:
        if not result.ok:
            log.error("continuity_repair_failed %s", kv(symbol=contract.logical_symbol, tf=gap.timeframe.value, error=(result.error or "")[:160]))
            return False
        match = next((c for c in result.candles if c.open_time == gap.open_time and c.is_closed), None)
        if match is None or not pipeline.repair(match):
            log.error("continuity_gap_unresolved %s", kv(symbol=contract.logical_symbol, tf=gap.timeframe.value, open_time=gap.open_time.isoformat()))
            return False
        self.repos.gaps.resolve(contract.security_id, gap.timeframe, gap.open_time, "broker_candle", self._now())
        return True

    def record_gap(self, contract: ResolvedContract, gap: GapRecord) -> None:
        self.repos.gaps.record(contract.logical_symbol, contract.security_id, gap.timeframe, gap.open_time, gap.expected, gap.present,
                               gap.missing, gap.detected_at)

    # ---- relevance
    def minute_relevant(self, open_time: datetime, now: datetime | None = None) -> bool:
        """An unresolved minute is retried while it belongs to the current trading day."""
        now = ensure_utc(now or self._now())
        return self.calendar.trading_date(open_time) >= self.calendar.trading_date(now)

    # ================================================= synchronous convenience
    # Fetch + apply in one call. Only for callers that are NOT on the event loop with bound
    # pipelines (tests, tools); the application uses the split form.
    def recover(self, contract: ResolvedContract, pipeline: CandlePipeline, fallback_start: datetime | None) -> bool:
        window = self.recovery_window(pipeline, fallback_start)
        if window is None:
            return pipeline.continuity_ok
        result = self.fetch_m1(contract, window[0], window[1], attempts=self.retries)
        return self.apply_recovery(contract, pipeline, result).ok and pipeline.continuity_ok

    def verify_pending(self, contract: ResolvedContract, pipeline: CandlePipeline) -> tuple[int, int]:
        window = self.verification_window(pipeline)
        if window is None:
            return 0, len(pipeline.pending_minutes())
        verified, still = self.apply_verification(contract, pipeline, self.fetch_m1(contract, window[0], window[1]))
        return len(verified), len(still)

    def repair(self, contract: ResolvedContract, pipeline: CandlePipeline) -> int:
        repaired = 0
        for gap in list(pipeline.unresolved_gaps):
            if not self.apply_repair(contract, pipeline, gap, self.fetch_gap_candle(contract, gap.timeframe, gap.open_time)):
                break
            repaired += 1
        return repaired


def _minutes(start: datetime, end: datetime):
    t = start
    while t < end:
        yield t
        t += timedelta(minutes=1)


def trading_date_of(calendar: SessionCalendar, ts: datetime) -> date:
    return calendar.trading_date(ts)
