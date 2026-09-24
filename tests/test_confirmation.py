from __future__ import annotations

from datetime import datetime, timezone

from aureon_mcx.confirmation import ConfirmationInputs, evaluate_clearance
from aureon_mcx.confirmation.fakeout import FLAG_COUNTER_TREND, FLAG_FAKEOUT_ACTIVE, FLAG_STRUCTURE_UNCONFIRMED
from aureon_mcx.detection.models import Direction
from aureon_mcx.indicators.models import IndicatorRow, RsiDirection
from aureon_mcx.market.timeframe import Timeframe
from aureon_mcx.mtf import TimeframeRead, TrendDirection, assess_mtf
from aureon_mcx.setups.models import Setup, SetupState
from aureon_mcx.structure.models import StructureContext
from tests.conftest import make_candles


def _ind(ema_fast, ema_slow, rsi, atr=10.0, warmed=True):
    return IndicatorRow("GOLD", "1", "e", Timeframe.M5, datetime(2026, 9, 21, tzinfo=timezone.utc), ema_fast if warmed else None,
                        ema_slow if warmed else None, (ema_fast - ema_slow) if warmed else None, 0.0, rsi if warmed else None,
                        RsiDirection.FLAT, atr if warmed else None, 1.0, None, 20, 50, 14, 14)


def _setup(direction=Direction.BULLISH, state=SetupState.CONFIRMED, family="breakout", ctx=None):
    c = make_candles(1)[0]
    return Setup("GOLD", "1", "e", Timeframe.M5, family, direction, state, 70100.0, 70090.0, 1, 1, c.open_time, c.open_time,
                 context=ctx or {"failures": 0, "age": 5, "beyond_closes": 3}, id=7)


def _mtf(direction, m5, m15, h1, h4="unavailable"):
    reads = {Timeframe.M5: TimeframeRead(Timeframe.M5, TrendDirection(m5)), Timeframe.M15: TimeframeRead(Timeframe.M15, TrendDirection(m15)),
             Timeframe.H1: TimeframeRead(Timeframe.H1, TrendDirection(h1)), Timeframe.H4: TimeframeRead(Timeframe.H4, TrendDirection(h4))}
    return assess_mtf(reads, direction, Timeframe.M5)


def _inputs(setup, ind, present, mtf, structure, **kw):
    return ConfirmationInputs(setup=setup, candle=make_candles(1)[0].with_id(1), indicators=ind, present_trend=present, mtf=mtf,
                              structure=structure, structure_sequence="HL -> HH -> HL -> HH", **kw)


def test_bullish_aligned_mtf_clears(app_config):
    s = _setup()
    r = evaluate_clearance(_inputs(s, _ind(101, 100, 58), TrendDirection.BULLISH, _mtf(Direction.BULLISH, "BULLISH", "BULLISH", "BULLISH"),
                                   StructureContext.BULLISH), app_config.policy)
    assert r.cleared and r.blockers == [] and r.final_check == "✅ CLEARED FOR REVIEW"
    assert r.policy_version == "strict-research-v1" and r.mtf_state == "MTF ALIGNED"
    assert {"EMA ALIGNED", "RSI SUPPORTS", "PRESENT TREND ALIGNED", "MTF ALIGNED", "STRUCTURE ALIGNED", "LIFECYCLE CONFIRMED"} <= set(r.badges)
    assert r.evidence["policy_note"] == "INITIAL STRICT RESEARCH POLICY"


def test_bearish_aligned_mtf_clears(app_config):
    s = _setup(Direction.BEARISH)
    r = evaluate_clearance(_inputs(s, _ind(99, 100, 42), TrendDirection.BEARISH, _mtf(Direction.BEARISH, "BEARISH", "BEARISH", "BEARISH"),
                                   StructureContext.BEARISH), app_config.policy)
    assert r.cleared and r.blockers == []


def test_m15_h1_disagreement_blocks(app_config):
    s = _setup()
    r = evaluate_clearance(_inputs(s, _ind(101, 100, 58), TrendDirection.BULLISH, _mtf(Direction.BULLISH, "BULLISH", "BULLISH", "BEARISH"),
                                   StructureContext.BULLISH), app_config.policy)
    assert not r.cleared and "M15/H1 disagree" in r.blockers and r.mtf_state == "MTF CONFLICT"
    assert "⚠ MTF CONFLICT" in r.evidence["warnings"]


def test_counter_trend_setup_blocks(app_config):
    # raw bullish reaction; everything else bearish (section 17 example)
    s = _setup(Direction.BULLISH, state=SetupState.CONFIRMED, family="liquidity_reaction")
    r = evaluate_clearance(_inputs(s, _ind(99, 100, 44), TrendDirection.BEARISH, _mtf(Direction.BULLISH, "BEARISH", "BEARISH", "BEARISH"),
                                   StructureContext.BEARISH), app_config.policy)
    assert not r.cleared and r.final_check == "⛔ NOT CLEARED"
    assert "present trend is bearish" in r.blockers
    assert "EMA20/EMA50 do not confirm direction" in r.blockers
    assert "RSI is opposite side of 50" in r.blockers
    assert "MTF direction opposes setup" in r.blockers and r.mtf_state == "MTF AGAINST"
    assert "structure remains LH/LL" in r.blockers
    assert r.evidence["warnings"] == ["⚠ EMA NOT ALIGNED", "⚠ RSI AGAINST", "⚠ COUNTER-TREND", "⚠ MTF AGAINST"]
    assert FLAG_COUNTER_TREND in r.fakeout_flags and FLAG_STRUCTURE_UNCONFIRMED in r.fakeout_flags and "MTF AGAINST" in r.fakeout_flags


def test_ema_disagreement_blocks(app_config):
    s = _setup()
    r = evaluate_clearance(_inputs(s, _ind(99, 100, 58), TrendDirection.BULLISH, _mtf(Direction.BULLISH, "BULLISH", "BULLISH", "BULLISH"),
                                   StructureContext.BULLISH), app_config.policy)
    assert r.blockers == ["EMA20/EMA50 do not confirm direction"]


def test_rsi_disagreement_blocks(app_config):
    s = _setup()
    r = evaluate_clearance(_inputs(s, _ind(101, 100, 48), TrendDirection.BULLISH, _mtf(Direction.BULLISH, "BULLISH", "BULLISH", "BULLISH"),
                                   StructureContext.BULLISH), app_config.policy)
    assert r.blockers == ["RSI is opposite side of 50"]


def test_lifecycle_not_confirmed_blocks(app_config):
    for st in (SetupState.WATCH, SetupState.DEVELOPING, SetupState.OBSERVING):
        s = _setup(state=st)
        r = evaluate_clearance(_inputs(s, _ind(101, 100, 58), TrendDirection.BULLISH, _mtf(Direction.BULLISH, "BULLISH", "BULLISH", "BULLISH"),
                                       StructureContext.BULLISH), app_config.policy)
        assert r.blockers == ["lifecycle has not reached CONFIRMED"]
    s = _setup(state=SetupState.FAKEOUT_RISK)
    r = evaluate_clearance(_inputs(s, _ind(101, 100, 58), TrendDirection.BULLISH, _mtf(Direction.BULLISH, "BULLISH", "BULLISH", "BULLISH"),
                                   StructureContext.BULLISH), app_config.policy)
    assert "fakeout-risk lifecycle active" in r.blockers and FLAG_FAKEOUT_ACTIVE in r.fakeout_flags


def test_missing_inputs_block_fail_closed(app_config):
    s = _setup()
    good_mtf = _mtf(Direction.BULLISH, "BULLISH", "BULLISH", "BULLISH")
    r = evaluate_clearance(_inputs(s, _ind(1, 1, 1, warmed=False), TrendDirection.BULLISH, good_mtf, StructureContext.BULLISH), app_config.policy)
    assert not r.cleared and any("not warmed" in b for b in r.blockers)
    r = evaluate_clearance(_inputs(s, _ind(101, 100, 58), None, good_mtf, StructureContext.BULLISH), app_config.policy)
    assert "present trend unavailable" in r.blockers
    r = evaluate_clearance(_inputs(s, _ind(101, 100, 58), TrendDirection.BULLISH, None, StructureContext.BULLISH), app_config.policy)
    assert "MTF context unavailable" in r.blockers
    r = evaluate_clearance(_inputs(s, _ind(101, 100, 58), TrendDirection.BULLISH, _mtf(Direction.BULLISH, "BULLISH", "SIDEWAYS", "SIDEWAYS"),
                                   StructureContext.BULLISH), app_config.policy)
    assert "no directional higher-timeframe context" in r.blockers
    r = evaluate_clearance(_inputs(s, _ind(101, 100, 58), TrendDirection.BULLISH, good_mtf, None), app_config.policy)
    assert "structure unavailable" in r.blockers


def test_rules_are_toggleable(app_config):
    policy = app_config.policy.model_copy(update={"rules": app_config.policy.rules.model_copy(update={"require_present_trend": False})})
    s = _setup()
    r = evaluate_clearance(_inputs(s, _ind(101, 100, 58), TrendDirection.SIDEWAYS, _mtf(Direction.BULLISH, "BULLISH", "BULLISH", "BULLISH"),
                                   StructureContext.MIXED), policy)
    assert r.cleared
