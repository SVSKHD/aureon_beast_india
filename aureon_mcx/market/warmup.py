"""Historical warmup helpers: download window sizing, local H4 derivation, aggregator seeding."""
from __future__ import annotations

import math
from datetime import datetime, timedelta
from typing import TYPE_CHECKING

from aureon_mcx.market.aggregation import aggregate_closed
from aureon_mcx.market.candle import Candle
from aureon_mcx.market.candle_builder import CandlePipeline
from aureon_mcx.market.timeframe import Timeframe

if TYPE_CHECKING:  # pragma: no cover
    from aureon_mcx.market.sessions import SessionCalendar

TRADING_MINUTES_PER_DAY = 14.5 * 60  # MCX 09:00-23:30 IST (approximate; only used to size the download window)


def lookback_days(bars: int, timeframe: Timeframe) -> int:
    trading_days = bars * timeframe.minutes / TRADING_MINUTES_PER_DAY
    return int(math.ceil(trading_days * 7 / 5)) + 3  # weekends + holiday buffer


def warmup_window(bars: int, timeframe: Timeframe, now: datetime) -> tuple[datetime, datetime]:
    return now - timedelta(days=lookback_days(bars, timeframe)), now


def derive_h4(h1: list[Candle], now: datetime | None = None, calendar: "SessionCalendar | None" = None) -> list[Candle]:
    """H4 is never downloaded: it is aggregated locally from CLOSED, COMPLETE H1 candles only,
    using the session-aware H4 policy when a calendar is supplied."""
    return aggregate_closed(h1, Timeframe.H4, now=now, calendar=calendar) if h1 else []


def seed_pipeline(pipeline: CandlePipeline, stored: dict[Timeframe, list[Candle]]) -> None:
    """Seed live aggregators with the constituents of the currently-open higher-timeframe buckets.

    Every bucket that already exists in storage is marked emitted so it is never
    produced twice; only source candles inside the open bucket are replayed.
    """
    for tf, agg in pipeline.higher.items():
        source = stored.get(agg.source, [])
        existing = stored.get(tf, [])
        if existing:
            agg.mark_emitted_until(existing[-1].open_time)
            cutoff = existing[-1].close_time
        else:
            cutoff = None
        if not source:
            continue
        bucket_start, _ = agg.policy.bucket(source[-1].open_time, tf)
        for c in source:
            if c.open_time >= bucket_start and (cutoff is None or c.open_time >= cutoff):
                agg.add(c)
    m1 = stored.get(Timeframe.M1, [])
    last_primary = stored.get(pipeline.primary, [])
    if last_primary:
        pipeline.primary_agg.mark_emitted_until(last_primary[-1].open_time)
        pipeline.last_closed[pipeline.primary] = last_primary[-1].open_time
    for tf, cs in stored.items():
        if cs and tf in pipeline.timeframes:
            pipeline.last_closed[tf] = cs[-1].open_time
    for c in m1:
        if not last_primary or c.open_time >= last_primary[-1].close_time:
            pipeline.m1.mark_emitted_until(c.open_time)
            pipeline.m1._last_close = c.close
            pipeline.last_m1_open = c.open_time
            pipeline._seen_m1.add(c.open_time)
            pipeline.primary_agg.add(c)
