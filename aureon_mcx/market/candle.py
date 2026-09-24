"""Internal Candle model. Every broker response is normalised into this."""
from __future__ import annotations

from dataclasses import dataclass, field, replace
from datetime import datetime, timedelta

from .timeframe import Timeframe
from .timeutil import ensure_utc


@dataclass(frozen=True)
class Candle:
    symbol: str
    security_id: str
    timeframe: Timeframe
    open_time: datetime
    open: float
    high: float
    low: float
    close: float
    volume: float = 0.0
    open_interest: float | None = None
    source: str = "dhan"
    is_closed: bool = True
    expiry_date: str = ""
    id: int | None = field(default=None, compare=False)

    def __post_init__(self) -> None:
        object.__setattr__(self, "open_time", ensure_utc(self.open_time))
        if self.high < self.low:
            raise ValueError(f"candle high {self.high} < low {self.low} at {self.open_time}")
        if not (self.low <= self.open <= self.high and self.low <= self.close <= self.high):
            raise ValueError(f"candle open/close outside high/low at {self.open_time}")

    @property
    def close_time(self) -> datetime:
        return self.open_time + timedelta(seconds=self.timeframe.seconds)

    @property
    def range(self) -> float:
        return self.high - self.low

    @property
    def body(self) -> float:
        return abs(self.close - self.open)

    @property
    def upper_wick(self) -> float:
        return self.high - max(self.open, self.close)

    @property
    def lower_wick(self) -> float:
        return min(self.open, self.close) - self.low

    @property
    def is_bullish(self) -> bool:
        return self.close > self.open

    @property
    def is_bearish(self) -> bool:
        return self.close < self.open

    def with_id(self, id_: int) -> "Candle":
        return replace(self, id=id_)

    def closed(self) -> "Candle":
        return replace(self, is_closed=True)
