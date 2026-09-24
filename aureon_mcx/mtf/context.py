"""Multi-timeframe directional context from CLOSED-bar evidence only."""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum

from aureon_mcx.detection.models import Direction
from aureon_mcx.indicators.models import IndicatorRow
from aureon_mcx.market.timeframe import Timeframe
from aureon_mcx.structure.models import StructureContext


class TrendDirection(str, Enum):
    BULLISH = "BULLISH"
    BEARISH = "BEARISH"
    SIDEWAYS = "SIDEWAYS"
    UNAVAILABLE = "unavailable"

    @property
    def is_directional(self) -> bool:
        return self in (TrendDirection.BULLISH, TrendDirection.BEARISH)

    def agrees_with(self, d: Direction) -> bool:
        return self.value == d.value


class MtfAlignment(str, Enum):
    ALIGNED = "MTF ALIGNED"
    AGAINST = "MTF AGAINST"
    CONFLICT = "MTF CONFLICT"
    NO_CONTEXT = "MTF NO CONTEXT"


@dataclass(frozen=True)
class TimeframeRead:
    timeframe: Timeframe
    direction: TrendDirection
    ema_relation: str | None = None
    rsi: float | None = None
    structure: StructureContext | None = None
    open_time: datetime | None = None
    evidence: dict = field(default_factory=dict)

    def to_dict(self) -> dict:
        return {"timeframe": self.timeframe.value, "direction": self.direction.value, "ema_relation": self.ema_relation,
                "rsi": self.rsi, "structure": self.structure.value if self.structure else None,
                "open_time": self.open_time.isoformat() if self.open_time else None}


def classify_timeframe(timeframe: Timeframe, ind: IndicatorRow | None, structure: StructureContext | None,
                       rsi_threshold: float = 50.0, require_structure: bool = True) -> TimeframeRead:
    """EMA relation + RSI side + structure regime -> BULLISH / BEARISH / SIDEWAYS. Fails closed to SIDEWAYS/unavailable."""
    if ind is None or not ind.warmed:
        return TimeframeRead(timeframe, TrendDirection.UNAVAILABLE, evidence={"reason": "indicators not warmed"})
    ema_rel = "EMA20 > EMA50" if ind.ema_fast > ind.ema_slow else ("EMA20 < EMA50" if ind.ema_fast < ind.ema_slow else "EMA20 = EMA50")
    ema_dir = Direction.BULLISH if ind.ema_fast > ind.ema_slow else (Direction.BEARISH if ind.ema_fast < ind.ema_slow else Direction.NEUTRAL)
    rsi_dir = Direction.BULLISH if ind.rsi >= rsi_threshold else Direction.BEARISH
    if ind.rsi == rsi_threshold:
        rsi_dir = Direction.NEUTRAL
    direction = TrendDirection.SIDEWAYS
    if ema_dir is Direction.BULLISH and rsi_dir is Direction.BULLISH and (not require_structure or structure is not StructureContext.BEARISH):
        direction = TrendDirection.BULLISH
    elif ema_dir is Direction.BEARISH and rsi_dir is Direction.BEARISH and (not require_structure or structure is not StructureContext.BULLISH):
        direction = TrendDirection.BEARISH
    return TimeframeRead(timeframe, direction, ema_rel, ind.rsi, structure, ind.open_time,
                         evidence={"ema": ema_dir.value, "rsi_side": rsi_dir.value, "structure": structure.value if structure else None})


@dataclass
class MtfAssessment:
    setup_direction: Direction
    primary: Timeframe
    reads: dict[Timeframe, TimeframeRead]
    alignment: MtfAlignment
    consensus: TrendDirection | None
    early_reversal: bool
    notes: list[str] = field(default_factory=list)

    @property
    def mtf_direction(self) -> TrendDirection | None:
        return self.consensus

    def display_rows(self) -> list[tuple[str, str]]:
        return [(tf.value, r.direction.value) for tf, r in sorted(self.reads.items(), key=lambda kv: kv[0].rank)]

    def to_dict(self) -> dict:
        return {"setup_direction": self.setup_direction.value, "alignment": self.alignment.value,
                "consensus": self.consensus.value if self.consensus else None, "early_reversal": self.early_reversal,
                "reads": {tf.value: r.to_dict() for tf, r in self.reads.items()}, "notes": list(self.notes)}


def assess_mtf(reads: dict[Timeframe, TimeframeRead], setup_direction: Direction, primary: Timeframe) -> MtfAssessment:
    directional = {tf: r for tf, r in reads.items() if r.direction.is_directional}
    higher = {tf: r for tf, r in directional.items() if tf.is_higher_than(primary)}
    notes: list[str] = []
    if not higher:
        return MtfAssessment(setup_direction, primary, reads, MtfAlignment.NO_CONTEXT, None, False,
                             ["no directional higher-timeframe context"])
    higher_dirs = {r.direction for r in higher.values()}
    all_dirs = {r.direction for r in directional.values()}
    primary_read = reads.get(primary)
    if len(higher_dirs) > 1:
        return MtfAssessment(setup_direction, primary, reads, MtfAlignment.CONFLICT, None, False, ["higher timeframes disagree with one another"])
    higher_dir = next(iter(higher_dirs))
    if len(all_dirs) > 1:
        # primary disagrees with (unanimous) higher timeframes
        early = primary_read is not None and primary_read.direction.agrees_with(setup_direction) and not higher_dir.agrees_with(setup_direction)
        notes.append(f"{primary.value} disagrees with higher timeframes")
        if early:
            notes.append("EARLY REVERSAL / WATCH")
        return MtfAssessment(setup_direction, primary, reads, MtfAlignment.CONFLICT, higher_dir, early, notes)
    if higher_dir.agrees_with(setup_direction):
        return MtfAssessment(setup_direction, primary, reads, MtfAlignment.ALIGNED, higher_dir, False, ["all directional timeframes agree"])
    return MtfAssessment(setup_direction, primary, reads, MtfAlignment.AGAINST, higher_dir, False, ["higher-timeframe direction opposes setup"])
