from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum

from aureon_mcx.market.timeframe import Timeframe


class RsiDirection(str, Enum):
    RISING = "rising"
    FALLING = "falling"
    FLAT = "flat"


@dataclass(frozen=True)
class IndicatorRow:
    """Indicator values aligned to exactly one closed candle."""

    symbol: str
    security_id: str
    expiry_date: str
    timeframe: Timeframe
    open_time: datetime
    ema_fast: float | None
    ema_slow: float | None
    ema_gap: float | None
    ema_fast_slope: float | None
    rsi: float | None
    rsi_direction: RsiDirection | None
    atr: float | None
    volume: float
    open_interest: float | None
    ema_fast_period: int
    ema_slow_period: int
    rsi_period: int
    atr_period: int
    candle_id: int | None = field(default=None, compare=False)
    id: int | None = field(default=None, compare=False)

    @property
    def warmed(self) -> bool:
        return None not in (self.ema_fast, self.ema_slow, self.rsi, self.atr)
