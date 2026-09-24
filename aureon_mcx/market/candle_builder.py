"""Tick -> M1 -> M5 -> (M15 / H1 / H4) closed-candle pipeline.

Analysis callbacks fire ONLY on closed candles, exactly once per closed bar per
timeframe. Ticks never reach analysis.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Callable

from .aggregation import TimeframeAggregator
from .candle import Candle
from .timeframe import Timeframe
from .timeutil import IST, ensure_utc, floor_to

log = logging.getLogger("aureon.candles")


@dataclass(frozen=True)
class Tick:
    security_id: str
    price: float
    ts: datetime
    last_qty: float | None = None       # per-trade quantity (preferred for volume)
    day_volume: float | None = None     # cumulative day volume (fallback: delta)
    open_interest: float | None = None


class M1CandleBuilder:
    def __init__(self, symbol: str, security_id: str, expiry_date: str = "", tz=IST):
        self.symbol = symbol
        self.security_id = security_id
        self.expiry_date = expiry_date
        self.tz = tz
        self._open_time: datetime | None = None
        self._o = self._h = self._l = self._c = 0.0
        self._vol = 0.0
        self._oi: float | None = None
        self._last_day_volume: float | None = None
        self._last_emitted: datetime | None = None

    def _emit(self) -> Candle:
        c = Candle(symbol=self.symbol, security_id=self.security_id, timeframe=Timeframe.M1, open_time=self._open_time,
                   open=self._o, high=self._h, low=self._l, close=self._c, volume=self._vol, open_interest=self._oi,
                   source="dhan", is_closed=True, expiry_date=self.expiry_date)
        self._last_emitted = self._open_time
        self._open_time = None
        return c

    def add_tick(self, tick: Tick) -> Candle | None:
        ts = ensure_utc(tick.ts)
        start = floor_to(ts, 60, self.tz)
        if self._last_emitted is not None and start <= self._last_emitted:
            return None  # late tick for a closed minute: ignore, never reopen
        emitted = None
        if self._open_time is not None and start > self._open_time:
            emitted = self._emit()
        if self._open_time is None:
            self._open_time = start
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
        return emitted

    def flush_at(self, now: datetime) -> Candle | None:
        if self._open_time is not None and ensure_utc(now) >= self._open_time + timedelta(seconds=60):
            return self._emit()
        return None

    @property
    def partial(self) -> Candle | None:
        if self._open_time is None:
            return None
        return Candle(symbol=self.symbol, security_id=self.security_id, timeframe=Timeframe.M1, open_time=self._open_time,
                      open=self._o, high=self._h, low=self._l, close=self._c, volume=self._vol, open_interest=self._oi,
                      is_closed=False, expiry_date=self.expiry_date)


ClosedHandler = Callable[[Candle], None]


class CandlePipeline:
    """Per-symbol pipeline: ticks -> M1 -> primary (M5) -> higher timeframes.

    `on_closed(candle)` is invoked once per closed candle for every timeframe in
    `timeframes` (primary first, then higher ones as their boundaries pass).
    """

    def __init__(self, symbol: str, security_id: str, expiry_date: str, primary: Timeframe, timeframes: list[Timeframe],
                 on_closed: ClosedHandler, tz=IST):
        if primary == Timeframe.M1:
            raise ValueError("primary timeframe must be above M1")
        self.symbol = symbol
        self.security_id = security_id
        self.expiry_date = expiry_date
        self.primary = primary
        self.timeframes = [t for t in timeframes if t != Timeframe.M1]
        self.on_closed = on_closed
        self.m1 = M1CandleBuilder(symbol, security_id, expiry_date, tz)
        self.primary_agg = TimeframeAggregator(Timeframe.M1, primary, symbol, security_id, expiry_date, tz)
        self.higher: dict[Timeframe, TimeframeAggregator] = {}
        for tf in self.timeframes:
            if tf == primary:
                continue
            src = primary if tf.seconds % primary.seconds == 0 else Timeframe.M1
            if tf == Timeframe.H4:
                src = Timeframe.H1 if Timeframe.H1 in self.timeframes else primary
            self.higher[tf] = TimeframeAggregator(src, tf, symbol, security_id, expiry_date, tz)
        self.closed_counts: dict[Timeframe, int] = {tf: 0 for tf in self.timeframes}

    def _dispatch(self, candle: Candle) -> None:
        self.closed_counts[candle.timeframe] = self.closed_counts.get(candle.timeframe, 0) + 1
        self.on_closed(candle)
        # feed aggregators that source from this timeframe
        for tf, agg in self.higher.items():
            if agg.source == candle.timeframe:
                out = agg.add(candle)
                if out is not None:
                    self._dispatch(out)
                    pending = agg.drain_pending()
                    if pending is not None:
                        self._dispatch(pending)

    def on_m1_closed(self, m1: Candle) -> None:
        for tf, agg in self.higher.items():
            if agg.source == Timeframe.M1:
                out = agg.add(m1)
                if out is not None:
                    self._dispatch(out)
        p = self.primary_agg.add(m1)
        if p is not None:
            self._dispatch(p)
            pending = self.primary_agg.drain_pending()
            if pending is not None:
                self._dispatch(pending)

    def add_tick(self, tick: Tick) -> None:
        m1 = self.m1.add_tick(tick)
        if m1 is not None:
            self.on_m1_closed(m1)

    def flush_at(self, now: datetime) -> None:
        """Wall-clock boundary check: closes bars whose boundary passed with no new tick."""
        m1 = self.m1.flush_at(now)
        if m1 is not None:
            self.on_m1_closed(m1)
        p = self.primary_agg.flush_at(now)
        if p is not None:
            self._dispatch(p)
        for agg in self.higher.values():
            out = agg.flush_at(now)
            if out is not None:
                self._dispatch(out)

    def seed_primary_history(self, candles: list[Candle]) -> None:
        """Seed higher-timeframe aggregators from stored closed primary candles (no dispatch)."""
        for c in candles:
            for agg in self.higher.values():
                if agg.source == c.timeframe:
                    agg.add(c)
                    agg.drain_pending()
