"""Observer-side indicator engine: EMA fast/slow, RSI (Wilder), ATR (Wilder).

Incremental and deterministic. Values are None until warmed. Everything is
computed on CLOSED candles only and persisted aligned to the candle so Discord
renders exactly what the engine analysed.
"""
from __future__ import annotations

from dataclasses import dataclass, field

from aureon_mcx.market.candle import Candle

from .models import IndicatorRow, RsiDirection


@dataclass(frozen=True)
class IndicatorParams:
    ema_fast: int = 20
    ema_slow: int = 50
    rsi_period: int = 14
    atr_period: int = 14
    rsi_flat_tolerance: float = 0.5

    def __post_init__(self) -> None:
        if self.ema_fast >= self.ema_slow:
            raise ValueError("ema_fast must be < ema_slow")


@dataclass
class _EmaState:
    period: int
    value: float | None = None
    seed: list[float] = field(default_factory=list)

    def update(self, price: float) -> float | None:
        if self.value is None:
            self.seed.append(price)
            if len(self.seed) < self.period:
                return None
            self.value = sum(self.seed) / self.period
            return self.value
        k = 2.0 / (self.period + 1)
        self.value = (price - self.value) * k + self.value
        return self.value


@dataclass
class _RsiState:
    period: int
    prev_close: float | None = None
    avg_gain: float | None = None
    avg_loss: float | None = None
    seed_gains: list[float] = field(default_factory=list)
    seed_losses: list[float] = field(default_factory=list)

    def update(self, close: float) -> float | None:
        if self.prev_close is None:
            self.prev_close = close
            return None
        change = close - self.prev_close
        self.prev_close = close
        gain = max(change, 0.0)
        loss = max(-change, 0.0)
        if self.avg_gain is None:
            self.seed_gains.append(gain)
            self.seed_losses.append(loss)
            if len(self.seed_gains) < self.period:
                return None
            self.avg_gain = sum(self.seed_gains) / self.period
            self.avg_loss = sum(self.seed_losses) / self.period
        else:
            self.avg_gain = (self.avg_gain * (self.period - 1) + gain) / self.period
            self.avg_loss = (self.avg_loss * (self.period - 1) + loss) / self.period
        if self.avg_loss == 0:
            return 100.0 if self.avg_gain > 0 else 50.0
        rs = self.avg_gain / self.avg_loss
        return 100.0 - 100.0 / (1.0 + rs)


@dataclass
class _AtrState:
    period: int
    prev_close: float | None = None
    value: float | None = None
    seed: list[float] = field(default_factory=list)

    def update(self, c: Candle) -> float | None:
        if self.prev_close is None:
            tr = c.high - c.low
        else:
            tr = max(c.high - c.low, abs(c.high - self.prev_close), abs(c.low - self.prev_close))
        self.prev_close = c.close
        if self.value is None:
            self.seed.append(tr)
            if len(self.seed) < self.period:
                return None
            self.value = sum(self.seed) / self.period
            return self.value
        self.value = (self.value * (self.period - 1) + tr) / self.period
        return self.value


class IndicatorEngine:
    """One engine instance per (symbol, timeframe). Feed closed candles in order."""

    def __init__(self, params: IndicatorParams):
        self.params = params
        self._ema_fast = _EmaState(params.ema_fast)
        self._ema_slow = _EmaState(params.ema_slow)
        self._rsi = _RsiState(params.rsi_period)
        self._atr = _AtrState(params.atr_period)
        self._prev_ema_fast: float | None = None
        self._prev_rsi: float | None = None
        self._last_open_time = None
        self.rows: list[IndicatorRow] = []

    @property
    def latest(self) -> IndicatorRow | None:
        return self.rows[-1] if self.rows else None

    def update(self, candle: Candle) -> IndicatorRow:
        if not candle.is_closed:
            raise ValueError("IndicatorEngine only accepts closed candles")
        if self._last_open_time is not None and candle.open_time <= self._last_open_time:
            raise ValueError(f"candles must be strictly increasing in time: {candle.open_time} <= {self._last_open_time}")
        self._last_open_time = candle.open_time

        ema_fast = self._ema_fast.update(candle.close)
        ema_slow = self._ema_slow.update(candle.close)
        rsi = self._rsi.update(candle.close)
        atr = self._atr.update(candle)

        gap = (ema_fast - ema_slow) if (ema_fast is not None and ema_slow is not None) else None
        slope = (ema_fast - self._prev_ema_fast) if (ema_fast is not None and self._prev_ema_fast is not None) else None
        rsi_dir: RsiDirection | None = None
        if rsi is not None and self._prev_rsi is not None:
            d = rsi - self._prev_rsi
            if abs(d) <= self.params.rsi_flat_tolerance:
                rsi_dir = RsiDirection.FLAT
            else:
                rsi_dir = RsiDirection.RISING if d > 0 else RsiDirection.FALLING
        elif rsi is not None:
            rsi_dir = RsiDirection.FLAT

        self._prev_ema_fast = ema_fast
        self._prev_rsi = rsi

        row = IndicatorRow(
            symbol=candle.symbol, security_id=candle.security_id, expiry_date=candle.expiry_date, timeframe=candle.timeframe,
            open_time=candle.open_time, ema_fast=ema_fast, ema_slow=ema_slow, ema_gap=gap, ema_fast_slope=slope, rsi=rsi,
            rsi_direction=rsi_dir, atr=atr, volume=candle.volume, open_interest=candle.open_interest,
            ema_fast_period=self.params.ema_fast, ema_slow_period=self.params.ema_slow, rsi_period=self.params.rsi_period,
            atr_period=self.params.atr_period, candle_id=candle.id,
        )
        self.rows.append(row)
        if len(self.rows) > 5000:
            del self.rows[:-2500]
        return row

    def warm(self, candles: list[Candle]) -> list[IndicatorRow]:
        return [self.update(c) for c in candles]
