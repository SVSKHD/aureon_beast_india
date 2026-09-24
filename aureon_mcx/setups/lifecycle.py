"""Setup lifecycle: explicit state machine with allowed transitions.

Every transition carries the closed candle that caused it and the evidence used.
The tracker is generic over an anchor level and a direction, so breakout,
EMA-cross and liquidity-reaction setups share one deterministic lifecycle.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from aureon_mcx.detection.models import Detection, DetectionFamily, Direction
from aureon_mcx.indicators.models import IndicatorRow
from aureon_mcx.market.candle import Candle

from .models import Setup, SetupState

ALLOWED: dict[SetupState, set[SetupState]] = {
    SetupState.OBSERVING: {SetupState.WATCH, SetupState.INVALIDATED, SetupState.COMPLETED},
    SetupState.WATCH: {SetupState.DEVELOPING, SetupState.FAKEOUT_RISK, SetupState.INVALIDATED, SetupState.COMPLETED},
    SetupState.DEVELOPING: {SetupState.CONFIRMED, SetupState.FAKEOUT_RISK, SetupState.INVALIDATED, SetupState.COMPLETED},
    SetupState.CONFIRMED: {SetupState.PULLBACK, SetupState.FAKEOUT_RISK, SetupState.INVALIDATED, SetupState.COMPLETED},
    SetupState.PULLBACK: {SetupState.CONTINUATION, SetupState.FAKEOUT_RISK, SetupState.INVALIDATED, SetupState.COMPLETED},
    SetupState.CONTINUATION: {SetupState.FAKEOUT_RISK, SetupState.COMPLETED, SetupState.INVALIDATED},
    SetupState.FAKEOUT_RISK: {SetupState.CONFIRMED, SetupState.INVALIDATED, SetupState.COMPLETED},
    SetupState.COMPLETED: set(),
    SetupState.INVALIDATED: set(),
}


class IllegalTransition(RuntimeError):
    pass


def can_transition(a: SetupState, b: SetupState) -> bool:
    return b in ALLOWED[a]


@dataclass
class Transition:
    from_state: SetupState
    to_state: SetupState
    reason: str
    evidence: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class LifecycleParams:
    acceptance_closes: int = 2
    retest_tolerance_atr: float = 0.25
    completion_bars: int = 12
    max_age_bars: int = 48


FAMILY_BREAKOUT = "breakout"
FAMILY_EMA_CROSS = "ema_cross"
FAMILY_LIQUIDITY = "liquidity_reaction"

SETUP_ORIGIN_KINDS = {
    DetectionFamily.BREAKOUT: {"BREAKOUT"},
    DetectionFamily.EMA: {"BULL_CROSS", "BEAR_CROSS"},
    DetectionFamily.LIQUIDITY: {"SWEEP", "RECLAIM"},
}


def family_for(detection: Detection) -> str | None:
    kinds = SETUP_ORIGIN_KINDS.get(detection.family)
    if not kinds or detection.kind not in kinds:
        return None
    return {DetectionFamily.BREAKOUT: FAMILY_BREAKOUT, DetectionFamily.EMA: FAMILY_EMA_CROSS,
            DetectionFamily.LIQUIDITY: FAMILY_LIQUIDITY}[detection.family]


def create_setup(detection: Detection, candle: Candle, ind: IndicatorRow) -> tuple[Setup, Transition] | None:
    """Create a setup in OBSERVING plus its first transition to WATCH. None if the detection is not a setup origin."""
    family = family_for(detection)
    if family is None or detection.direction is Direction.NEUTRAL or detection.id is None or candle.id is None:
        return None
    bull = detection.direction is Direction.BULLISH
    if family == FAMILY_EMA_CROSS:
        anchor = float(ind.ema_slow) if ind.ema_slow is not None else candle.close
    else:
        anchor = float(detection.payload.get("level_price", candle.close))
    invalidation = candle.low if bull else candle.high
    if bull and invalidation >= anchor:
        invalidation = anchor - (ind.atr or candle.range or 1.0)
    if not bull and invalidation <= anchor:
        invalidation = anchor + (ind.atr or candle.range or 1.0)
    setup = Setup(
        symbol=detection.symbol, security_id=detection.security_id, expiry_date=detection.expiry_date, timeframe=detection.timeframe,
        family=family, direction=detection.direction, state=SetupState.OBSERVING, anchor_price=anchor, invalidation_price=invalidation,
        origin_detection_id=detection.id, origin_candle_id=candle.id, created_open_time=candle.open_time, updated_open_time=candle.open_time,
        context={"origin_kind": detection.kind, "origin_label": detection.label, "level_kind": detection.payload.get("level_kind"),
                 "beyond_closes": 1 if _beyond(candle.close, anchor, bull) else 0, "extreme": candle.close, "age": 0,
                 "continuation_bars": 0, "failures": 0, "events": [detection.kind], "inside_streak": 0},
    )
    t = Transition(SetupState.OBSERVING, SetupState.WATCH, f"{detection.label}",
                   {"detection_id": detection.id, "detection_kind": detection.kind, "anchor": anchor, "invalidation": invalidation,
                    "close": candle.close, "atr": ind.atr})
    return setup, t


def _beyond(close: float, anchor: float, bull: bool) -> bool:
    return close > anchor if bull else close < anchor


class SetupTracker:
    def __init__(self, params: LifecycleParams):
        self.p = params

    def evaluate(self, setup: Setup, candle: Candle, ind: IndicatorRow) -> list[Transition]:
        """Apply one closed candle to the setup. Mutates setup.state / setup.context and returns the transitions."""
        if not candle.is_closed:
            raise ValueError("lifecycle only evaluates closed candles")
        if setup.state.is_terminal:
            return []
        if candle.open_time <= setup.updated_open_time and setup.state is not SetupState.OBSERVING:
            return []
        bull = setup.direction is Direction.BULLISH
        ctx = setup.context
        ctx["age"] = int(ctx.get("age", 0)) + 1
        beyond = _beyond(candle.close, setup.anchor_price, bull)
        atr = ind.atr or 0.0
        base_ev = {"close": candle.close, "high": candle.high, "low": candle.low, "anchor": setup.anchor_price,
                   "invalidation": setup.invalidation_price, "beyond_anchor": beyond, "atr": ind.atr, "age": ctx["age"],
                   "ema_fast": ind.ema_fast, "ema_slow": ind.ema_slow, "rsi": ind.rsi}
        out: list[Transition] = []

        def go(to: SetupState, reason: str, **ev) -> None:
            if not can_transition(setup.state, to):
                raise IllegalTransition(f"{setup.state.value} -> {to.value} not allowed")
            out.append(Transition(setup.state, to, reason, {**base_ev, **ev}))
            setup.state = to
            ctx.setdefault("events", []).append(to.value)
            if to.is_terminal:
                setup.closed_at = candle.close_time

        # -- hard invalidation --------------------------------------------
        invalidated = candle.close < setup.invalidation_price if bull else candle.close > setup.invalidation_price
        if invalidated:
            go(SetupState.INVALIDATED, "close beyond invalidation level")
            setup.updated_open_time = candle.open_time
            return out

        if beyond:
            ctx["beyond_closes"] = int(ctx.get("beyond_closes", 0)) + 1
            ctx["inside_streak"] = 0
        else:
            ctx["inside_streak"] = int(ctx.get("inside_streak", 0)) + 1
        prev_extreme = float(ctx.get("extreme", candle.close))
        follow_through = (candle.close > prev_extreme) if bull else (candle.close < prev_extreme)
        touched = (candle.low <= setup.anchor_price + self.p.retest_tolerance_atr * atr) if bull else \
                  (candle.high >= setup.anchor_price - self.p.retest_tolerance_atr * atr)

        st = setup.state
        if st is SetupState.OBSERVING:
            go(SetupState.WATCH, "observation opened")
            st = setup.state
        if st is SetupState.WATCH:
            if not beyond:
                ctx["failures"] = int(ctx.get("failures", 0)) + 1
                go(SetupState.FAKEOUT_RISK, "close back inside anchor", failures=ctx["failures"])
            elif ctx["beyond_closes"] >= self.p.acceptance_closes:
                go(SetupState.DEVELOPING, "close acceptance beyond anchor", beyond_closes=ctx["beyond_closes"])
        elif st is SetupState.DEVELOPING:
            if not beyond:
                ctx["failures"] = int(ctx.get("failures", 0)) + 1
                go(SetupState.FAKEOUT_RISK, "close back inside anchor", failures=ctx["failures"])
            elif follow_through:
                go(SetupState.CONFIRMED, "follow-through close beyond prior extreme", prior_extreme=prev_extreme)
        elif st is SetupState.CONFIRMED:
            if not beyond:
                ctx["failures"] = int(ctx.get("failures", 0)) + 1
                go(SetupState.FAKEOUT_RISK, "close back inside anchor", failures=ctx["failures"])
            elif touched:
                go(SetupState.PULLBACK, "retest of anchor held on close", retest_tolerance_atr=self.p.retest_tolerance_atr)
        elif st is SetupState.PULLBACK:
            if not beyond:
                ctx["failures"] = int(ctx.get("failures", 0)) + 1
                go(SetupState.FAKEOUT_RISK, "close back inside anchor", failures=ctx["failures"])
            elif follow_through:
                ctx["continuation_bars"] = 0
                go(SetupState.CONTINUATION, "continuation close beyond prior extreme", prior_extreme=prev_extreme)
        elif st is SetupState.CONTINUATION:
            ctx["continuation_bars"] = int(ctx.get("continuation_bars", 0)) + 1
            if not beyond:
                ctx["failures"] = int(ctx.get("failures", 0)) + 1
                go(SetupState.FAKEOUT_RISK, "close back inside anchor", failures=ctx["failures"])
            elif ctx["continuation_bars"] >= self.p.completion_bars:
                go(SetupState.COMPLETED, "continuation window complete", continuation_bars=ctx["continuation_bars"])
        elif st is SetupState.FAKEOUT_RISK:
            if beyond:
                go(SetupState.CONFIRMED, "reclaim: close beyond anchor", failures=ctx.get("failures", 0))
            else:
                go(SetupState.INVALIDATED, "second close back inside anchor", failures=ctx.get("failures", 0))

        if beyond and follow_through:
            ctx["extreme"] = candle.close
        if not setup.state.is_terminal and ctx["age"] >= self.p.max_age_bars:
            go(SetupState.COMPLETED, "max age reached", max_age_bars=self.p.max_age_bars)
        setup.updated_open_time = candle.open_time
        return out
