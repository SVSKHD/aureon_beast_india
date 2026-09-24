"""Deterministic fakeout-risk flags. Factual labels only; no probabilities."""
from __future__ import annotations

from typing import TYPE_CHECKING

from aureon_mcx.detection.models import Direction
from aureon_mcx.mtf.context import MtfAlignment, TrendDirection
from aureon_mcx.setups.models import SetupState
from aureon_mcx.structure.models import StructureContext

if TYPE_CHECKING:  # pragma: no cover
    from .policy import ConfirmationInputs

FLAG_FAKEOUT_ACTIVE = "FAKEOUT_RISK ACTIVE"
FLAG_COUNTER_TREND = "COUNTER-TREND"
FLAG_MTF_CONFLICT = "MTF CONFLICT"
FLAG_STRUCTURE_UNCONFIRMED = "STRUCTURE UNCONFIRMED"
FLAG_BREAKOUT_FAILED = "BREAKOUT FAILED"


def fakeout_flags(inp: "ConfirmationInputs") -> list[str]:
    s = inp.setup
    bull = s.direction is Direction.BULLISH
    flags: list[str] = []
    reasons: list[str] = []
    ctx = s.context or {}
    if s.state is SetupState.FAKEOUT_RISK:
        flags.append(FLAG_FAKEOUT_ACTIVE)
        reasons.append("breakout returned inside range")
        reasons.append("no reclaim")
    pt = inp.present_trend
    if pt is not None and pt.is_directional and not pt.agrees_with(s.direction):
        flags.append(FLAG_COUNTER_TREND)
        if s.family == "ema_cross":
            reasons.append("counter-trend cross")
    if inp.mtf is not None and inp.mtf.alignment in (MtfAlignment.CONFLICT, MtfAlignment.AGAINST):
        flags.append(FLAG_MTF_CONFLICT if inp.mtf.alignment is MtfAlignment.CONFLICT else "MTF AGAINST")
    h1 = inp.h1_direction
    if s.family == "ema_cross" and h1 is not None and h1.is_directional and not h1.agrees_with(s.direction):
        reasons.append("EMA cross against H1")
    st = inp.structure
    if st is None or st is StructureContext.MIXED or (st is StructureContext.BEARISH and bull) or (st is StructureContext.BULLISH and not bull):
        flags.append(FLAG_STRUCTURE_UNCONFIRMED)
        reasons.append("structure has not changed")
    if int(ctx.get("failures", 0)) > 0 and s.state is not SetupState.FAKEOUT_RISK:
        flags.append(FLAG_BREAKOUT_FAILED)
        reasons.append("breakout returned inside range")
    if s.state in (SetupState.WATCH, SetupState.DEVELOPING) and int(ctx.get("age", 0)) >= 3 and int(ctx.get("beyond_closes", 0)) <= 1:
        reasons.append("weak follow-through")
    # dedupe, preserve order
    seen: set[str] = set()
    out = []
    for f in flags + [f"· {r}" for r in reasons]:
        if f not in seen:
            seen.add(f)
            out.append(f)
    return out
