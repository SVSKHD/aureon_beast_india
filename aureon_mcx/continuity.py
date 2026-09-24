"""Market-data continuity service: reconnect gap recovery and broker-candle gap repair.

Recovery (after every feed connect):
  1. `pipeline.recovery_start()` says where processed data ends (the minute left open at
     disconnect, or the minute after the last processed M1; after warmup: the end of the
     stored primary history);
  2. closed M1 candles for [start, now) are fetched from the Dhan historical API, flat
     minutes inside the fetched range are filled (no trades while the exchange was open),
     the still-open minute is never used;
  3. they are fed through the SAME pipeline (`recover_m1`) in chronological order while live
     ticks are buffered, then buffered ticks are replayed;
  4. the symbol is LIVE only when the pipeline reports continuity; otherwise it stays
     RECOVERING_GAP / ERROR and analytics remain suspended (fail closed).

Repair (after a GAP_DETECTED bar): the exact broker candle for the gap bucket is fetched at
that timeframe and dispatched through `pipeline.repair`, which also releases deferred bars.
"""
from __future__ import annotations

import logging
from datetime import datetime, timedelta
from typing import TYPE_CHECKING, Callable

from aureon_mcx.broker.dhan.errors import DhanError
from aureon_mcx.broker.dhan.symbol_resolver import ResolvedContract
from aureon_mcx.health import HealthState
from aureon_mcx.logging_setup import kv
from aureon_mcx.market.candle import Candle
from aureon_mcx.market.candle_builder import CandlePipeline, GapRecord
from aureon_mcx.market.sessions import SessionCalendar
from aureon_mcx.market.timeframe import Timeframe
from aureon_mcx.market.timeutil import floor_to
from aureon_mcx.storage.repositories import Repositories

if TYPE_CHECKING:  # pragma: no cover
    from aureon_mcx.broker.dhan.historical import HistoricalProvider

log = logging.getLogger("aureon.continuity")


def fill_flat_minutes(candles: list[Candle], calendar: SessionCalendar) -> list[Candle]:
    """Inside the fetched range, an open-market minute with no bar means no trades: fill it
    flat at the previous close. Minutes outside the range are never invented."""
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
                                      close=p, volume=0.0, open_interest=prev.open_interest, source="flat", is_closed=True,
                                      expiry_date=c.expiry_date))
                t += timedelta(minutes=1)
        out.append(c)
        prev = c
    return out


class ContinuityService:
    def __init__(self, historical: "HistoricalProvider", repos: Repositories, calendar: SessionCalendar, health: HealthState,
                 now: Callable[[], datetime], primary: Timeframe, flat_fill: bool = True):
        self.historical = historical
        self.repos = repos
        self.calendar = calendar
        self.health = health
        self._now = now
        self.primary = primary
        self.flat_fill = flat_fill

    # ------------------------------------------------------------- recovery
    def recover(self, contract: ResolvedContract, pipeline: CandlePipeline, fallback_start: datetime | None) -> bool:
        """Synchronous (call from a thread): backfill closed M1 candles and restore continuity."""
        sym = contract.logical_symbol
        now = self._now()
        current_minute = floor_to(now, 60, self.calendar.tz)
        start = pipeline.recovery_start() or fallback_start
        if start is None:
            log.info("continuity_no_reference %s", kv(symbol=sym))
            return pipeline.continuity_ok
        start = floor_to(start, 60, self.calendar.tz)
        if start >= current_minute:
            return pipeline.continuity_ok  # nothing closed was missed
        self.health.set_symbol_state(sym, "RECOVERING_GAP", f"backfilling M1 from {start.isoformat()} to {current_minute.isoformat()}")
        try:
            fetched = self.historical.fetch(sym, contract.security_id, contract.exchange_segment, contract.instrument_type, contract.expiry_iso,
                                            Timeframe.M1, start, current_minute)
        except DhanError as exc:
            log.error("continuity_recovery_failed %s", kv(symbol=sym, error=str(exc)))
            self.health.set_symbol_state(sym, "ERROR", f"gap recovery failed: {exc}")
            return False
        closed = [c for c in fetched if c.is_closed and start <= c.open_time < current_minute]
        if self.flat_fill:
            closed = fill_flat_minutes(closed, self.calendar)
        fed = pipeline.recover_m1(closed)
        expected = [t for t in _minutes(start, current_minute) if self.calendar.is_open(t)]
        covered = {c.open_time for c in closed}
        missing = [t for t in expected if t not in covered and t not in pipeline._seen_m1]
        log.info("continuity_recovered %s", kv(symbol=sym, fetched=len(fetched), fed=fed, expected=len(expected), missing=len(missing)))
        if missing:
            self.health.set_symbol_state(sym, "ERROR", f"{len(missing)} open-market minutes unavailable from broker after recovery")
            return False
        return True

    # --------------------------------------------------------------- repair
    def repair(self, contract: ResolvedContract, pipeline: CandlePipeline) -> int:
        """Fetch the exact broker candle for each unresolved gap and repair the pipeline. Returns repaired count."""
        repaired = 0
        for gap in list(pipeline.unresolved_gaps):
            tf = gap.timeframe
            if tf.dhan_interval is None:
                log.error("continuity_gap_unrepairable %s", kv(symbol=contract.logical_symbol, tf=tf.value, open_time=gap.open_time.isoformat()))
                break
            try:
                candles = self.historical.fetch(contract.logical_symbol, contract.security_id, contract.exchange_segment, contract.instrument_type,
                                                contract.expiry_iso, tf, gap.open_time, gap.open_time + timedelta(seconds=tf.seconds))
            except DhanError as exc:
                log.error("continuity_repair_failed %s", kv(symbol=contract.logical_symbol, tf=tf.value, error=str(exc)))
                break
            match = next((c for c in candles if c.open_time == gap.open_time and c.is_closed), None)
            if match is None or not pipeline.repair(match):
                log.error("continuity_gap_unresolved %s", kv(symbol=contract.logical_symbol, tf=tf.value, open_time=gap.open_time.isoformat()))
                break
            self.repos.gaps.resolve(contract.security_id, tf, gap.open_time, "broker_candle", self._now())
            repaired += 1
        return repaired

    def record_gap(self, contract: ResolvedContract, gap: GapRecord) -> None:
        self.repos.gaps.record(contract.logical_symbol, contract.security_id, gap.timeframe, gap.open_time, gap.expected, gap.present,
                               gap.missing, gap.detected_at)


def _minutes(start: datetime, end: datetime):
    t = start
    while t < end:
        yield t
        t += timedelta(minutes=1)
