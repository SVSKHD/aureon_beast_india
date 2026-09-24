from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum

from aureon_mcx.market.timeframe import Timeframe


class PivotKind(str, Enum):
    HIGH = "HIGH"
    LOW = "LOW"


class StructureLabel(str, Enum):
    SH = "SH"  # first swing high
    HH = "HH"
    LH = "LH"
    EH = "EH"
    SL = "SL"  # first swing low
    HL = "HL"
    LL = "LL"
    EL = "EL"

    @property
    def is_bullish(self) -> bool:
        return self in (StructureLabel.HH, StructureLabel.HL)

    @property
    def is_bearish(self) -> bool:
        return self in (StructureLabel.LH, StructureLabel.LL)

    @property
    def is_high(self) -> bool:
        return self in (StructureLabel.SH, StructureLabel.HH, StructureLabel.LH, StructureLabel.EH)


class StructureContext(str, Enum):
    BULLISH = "BULLISH"
    BEARISH = "BEARISH"
    MIXED = "MIXED"


@dataclass(frozen=True)
class Pivot:
    symbol: str
    security_id: str
    expiry_date: str
    timeframe: Timeframe
    kind: PivotKind
    price: float
    label: StructureLabel
    pivot_open_time: datetime
    confirmed_at_open_time: datetime
    strength: int
    candle_id: int | None = field(default=None, compare=False)
    confirmed_candle_id: int | None = field(default=None, compare=False)
    id: int | None = field(default=None, compare=False)
