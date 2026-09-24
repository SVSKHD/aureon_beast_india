"""INITIAL STRICT RESEARCH POLICY.

DETECTION -> CONTEXT -> CONFIRMATION -> CLEARANCE.
Descriptive evidence only; no confidence percentages. Every result carries
`policy_version`. Missing evidence is itself a blocker (fail closed).
These rules are a research starting point and are NOT claimed to be profitable.
"""
from __future__ import annotations

from dataclasses import dataclass, field

from aureon_mcx.config.yaml_models import ConfirmationPolicyConfig
from aureon_mcx.detection.models import Direction
from aureon_mcx.indicators.models import IndicatorRow
from aureon_mcx.market.candle import Candle
from aureon_mcx.mtf.context import MtfAlignment, MtfAssessment, TrendDirection
from aureon_mcx.setups.models import Setup, SetupState
from aureon_mcx.structure.models import StructureContext

from .fakeout import fakeout_flags
from .models import ClearanceResult


@dataclass
class ConfirmationInputs:
    setup: Setup
    candle: Candle
    indicators: IndicatorRow | None
    present_trend: TrendDirection | None
    mtf: MtfAssessment | None
    structure: StructureContext | None
    structure_sequence: str = ""
    session_trend: TrendDirection | None = None
    h1_direction: TrendDirection | None = None
    extra: dict = field(default_factory=dict)


def evaluate_clearance(inp: ConfirmationInputs, policy: ConfirmationPolicyConfig) -> ClearanceResult:
    rules = policy.rules
    s = inp.setup
    bull = s.direction is Direction.BULLISH
    blockers: list[str] = []
    badges: list[str] = []
    warnings: list[str] = []
    ind = inp.indicators

    # 1. lifecycle
    if rules.require_lifecycle_confirmed:
        if s.state is SetupState.CONFIRMED or s.state in (SetupState.PULLBACK, SetupState.CONTINUATION):
            badges.append("LIFECYCLE CONFIRMED")
        else:
            blockers.append("lifecycle has not reached CONFIRMED")
    if rules.block_on_fakeout_risk and s.state is SetupState.FAKEOUT_RISK:
        blockers.append("fakeout-risk lifecycle active")

    # 2. EMA relation
    if rules.require_ema_alignment:
        if ind is None or ind.ema_fast is None or ind.ema_slow is None:
            blockers.append("EMA20/EMA50 unavailable (indicators not warmed)")
        elif (ind.ema_fast > ind.ema_slow) if bull else (ind.ema_fast < ind.ema_slow):
            badges.append("EMA ALIGNED")
        else:
            blockers.append("EMA20/EMA50 do not confirm direction")
            warnings.append("⚠ EMA NOT ALIGNED")

    # 3. RSI side of 50
    if rules.require_rsi_side:
        thr = policy.rsi_threshold
        if ind is None or ind.rsi is None:
            blockers.append("RSI unavailable (indicators not warmed)")
        elif (ind.rsi >= thr) if bull else (ind.rsi <= thr):
            badges.append("RSI SUPPORTS")
        else:
            blockers.append(f"RSI is opposite side of {int(thr)}")
            warnings.append("⚠ RSI AGAINST")

    # 4. present trend
    if rules.require_present_trend:
        pt = inp.present_trend
        if pt is None or pt is TrendDirection.UNAVAILABLE:
            blockers.append("present trend unavailable")
        elif pt.agrees_with(s.direction):
            badges.append("PRESENT TREND ALIGNED")
        else:
            blockers.append(f"present trend is {pt.value.lower()}")
            if pt.is_directional:
                warnings.append("⚠ COUNTER-TREND")

    # 5. MTF
    mtf = inp.mtf
    if rules.require_higher_tf_context or rules.require_mtf_alignment:
        if mtf is None:
            blockers.append("MTF context unavailable")
        elif mtf.alignment is MtfAlignment.NO_CONTEXT:
            blockers.append("no directional higher-timeframe context")
        elif rules.require_mtf_alignment:
            if mtf.alignment is MtfAlignment.ALIGNED and mtf.consensus is not None and mtf.consensus.agrees_with(s.direction):
                badges.append("MTF ALIGNED")
            elif mtf.alignment is MtfAlignment.CONFLICT:
                blockers.append("M15/H1 disagree" if _higher_conflict(mtf) else f"{mtf.primary.value} disagrees with higher timeframes")
                warnings.append("⚠ MTF CONFLICT")
            else:
                blockers.append("MTF direction opposes setup")
                warnings.append("⚠ MTF AGAINST")

    # 6. structure
    if rules.require_structure_not_opposing:
        st = inp.structure
        if st is None:
            blockers.append("structure unavailable")
        elif (st is StructureContext.BEARISH and bull) or (st is StructureContext.BULLISH and not bull):
            blockers.append("structure remains LH/LL" if bull else "structure remains HH/HL")
        elif st is StructureContext.MIXED:
            badges.append("STRUCTURE MIXED (not opposing)")
        else:
            badges.append("STRUCTURE ALIGNED")

    flags = fakeout_flags(inp)
    cleared = not blockers
    evidence = {
        "warnings": warnings, "setup_state": s.state.value, "direction": s.direction.value,
        "ema_fast": ind.ema_fast if ind else None, "ema_slow": ind.ema_slow if ind else None, "rsi": ind.rsi if ind else None,
        "atr": ind.atr if ind else None, "present_trend": inp.present_trend.value if inp.present_trend else None,
        "session_trend": inp.session_trend.value if inp.session_trend else None,
        "structure": inp.structure.value if inp.structure else None, "structure_sequence": inp.structure_sequence,
        "mtf": mtf.to_dict() if mtf else None, "rules": rules.model_dump(), "policy_note": "INITIAL STRICT RESEARCH POLICY",
    }
    return ClearanceResult(setup_id=s.id or 0, candle_id=inp.candle.id or 0, open_time=inp.candle.open_time, cleared=cleared,
                           policy_version=policy.policy_version, mtf_state=mtf.alignment.value if mtf else MtfAlignment.NO_CONTEXT.value,
                           blockers=blockers, badges=badges, fakeout_flags=flags, evidence=evidence)


def _higher_conflict(mtf: MtfAssessment) -> bool:
    dirs = {r.direction for tf, r in mtf.reads.items() if tf.is_higher_than(mtf.primary) and r.direction.is_directional}
    return len(dirs) > 1
