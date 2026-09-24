"""Confirmed price levels shared by the liquidity and breakout detectors."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from enum import Enum

from aureon_mcx.structure.models import Pivot, PivotKind


class LevelSide(str, Enum):
    HIGH = "HIGH"  # liquidity resting above (resistance-like)
    LOW = "LOW"    # liquidity resting below (support-like)


@dataclass(frozen=True)
class Level:
    kind: str          # swing_high, swing_low, prev_day_high, prev_day_low, session_high, session_low, equal_highs, equal_lows
    side: LevelSide
    price: float
    source_time: datetime | None = None

    @property
    def key(self) -> str:
        return f"{self.kind}@{self.price:.4f}"

    @property
    def display(self) -> str:
        return self.kind.replace("_", " ")


def build_levels(pivots: list[Pivot], prev_day_high: float | None, prev_day_low: float | None, session_high: float | None,
                 session_low: float | None, equal_tolerance: float, max_swings: int = 3) -> list[Level]:
    """Levels are only built from CONFIRMED inputs (caller guarantees confirmation)."""
    levels: list[Level] = []
    highs = [p for p in pivots if p.kind is PivotKind.HIGH][-max_swings:]
    lows = [p for p in pivots if p.kind is PivotKind.LOW][-max_swings:]
    for p in highs:
        levels.append(Level("swing_high", LevelSide.HIGH, p.price, p.pivot_open_time))
    for p in lows:
        levels.append(Level("swing_low", LevelSide.LOW, p.price, p.pivot_open_time))
    if prev_day_high is not None:
        levels.append(Level("prev_day_high", LevelSide.HIGH, prev_day_high))
    if prev_day_low is not None:
        levels.append(Level("prev_day_low", LevelSide.LOW, prev_day_low))
    if session_high is not None:
        levels.append(Level("session_high", LevelSide.HIGH, session_high))
    if session_low is not None:
        levels.append(Level("session_low", LevelSide.LOW, session_low))
    # equal highs / lows: two confirmed swings within tolerance
    for i in range(1, len(highs)):
        if abs(highs[i].price - highs[i - 1].price) <= equal_tolerance:
            levels.append(Level("equal_highs", LevelSide.HIGH, max(highs[i].price, highs[i - 1].price), highs[i].pivot_open_time))
    for i in range(1, len(lows)):
        if abs(lows[i].price - lows[i - 1].price) <= equal_tolerance:
            levels.append(Level("equal_lows", LevelSide.LOW, min(lows[i].price, lows[i - 1].price), lows[i].pivot_open_time))
    return levels
