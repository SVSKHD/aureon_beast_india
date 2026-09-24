"""Historical warmup helpers: how far back to download and how to seed aggregators."""
from __future__ import annotations

import math
from datetime import datetime, timedelta

from aureon_mcx.market.aggregation import aggregate_closed
from aureon_mcx.market.candle import Candle
from aureon_mcx.market.candle_builder import CandlePipeline
from aureon_mcx.market.timeframe import Timeframe
from aureon_mcx.market.timeutil import floor_to

TRADING_MINUTES_PER_DAY = 14.5 * 60  # MCX 09:00-23:30 IST (approximate; only used to size the download window)


def lookback_days(bars: int, timeframe: Timeframe) -> int:
    trading_days = bars * timeframe.minutes / TRADING_MINUTES_PER_DAY
    return int(math.ceil(trading_days * 7 / 5)) + 3  # weekends + holiday buffer


def warmup_window(bars: int, timeframe: Timeframe, now: datetime) -> tuple[datetime, datetime]:
    return now - timedelta(days=lookback_days(bars, timeframe)), now


def derive_h4(h1: list[Candle], now: datetime | None = None) -> list[Candle]:
    """H4 is never downloaded: it is aggregated locally from CLOSED H1 candles only."""
    return aggregate_closed(h1, Timeframe.H4, now=now) if h1 else []


def seed_pipeline(pipeline: CandlePipeline, stored: dict[Timeframe, list[Candle]]) -> None:
    """Seed live aggregators with the constituents of the currently-open higher-timeframe buckets.

    Every bucket that already exists in storage is marked emitted so it is never
    produced twice; only candles inside the open bucket are replayed.
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
        bucket_start = floor_to(source[-1].open_time, tf.seconds)
        for c in source:
            if c.open_time >= bucket_start and (cutoff is None or c.open_time >= cutoff):
                agg.add(c)
                agg.drain_pending()
    primary_src = stored.get(Timeframe.M1, [])
    if primary_src:
        for c in primary_src:
            pipeline.primary_agg.add(c)
    last_primary = stored.get(pipeline.primary, [])
    if last_primary:
        pipeline.primary_agg.mark_emitted_until(last_primary[-1].open_time)
