from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any

from aureon_mcx.market.timeframe import Timeframe


class Direction(str, Enum):
    BULLISH = "BULLISH"
    BEARISH = "BEARISH"
    NEUTRAL = "NEUTRAL"

    @property
    def opposite(self) -> "Direction":
        if self is Direction.BULLISH:
            return Direction.BEARISH
        if self is Direction.BEARISH:
            return Direction.BULLISH
        return Direction.NEUTRAL


class DetectionFamily(str, Enum):
    EMA = "ema"
    WICK = "wick"
    LIQUIDITY = "liquidity"
    BREAKOUT = "breakout"
    RSI = "rsi"
    STRUCTURE = "structure"


@dataclass
class Detection:
    """A raw observation. Never a trade instruction."""

    symbol: str
    security_id: str
    expiry_date: str
    timeframe: Timeframe
    open_time: datetime
    family: DetectionFamily
    kind: str
    direction: Direction
    price: float
    label: str
    session: str | None = None
    payload: dict[str, Any] = field(default_factory=dict)
    candle_id: int | None = None
    id: int | None = None

    @property
    def payload_json(self) -> str:
        return json.dumps(self.payload, sort_keys=True, default=str)

    @property
    def is_ema_cross(self) -> bool:
        return self.family is DetectionFamily.EMA and self.kind in ("BULL_CROSS", "BEAR_CROSS")
