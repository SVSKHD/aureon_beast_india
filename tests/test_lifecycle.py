from __future__ import annotations

import pytest

from aureon_mcx.detection.models import Detection, DetectionFamily, Direction
from aureon_mcx.indicators.models import IndicatorRow, RsiDirection
from aureon_mcx.market.candle import Candle
from aureon_mcx.setups import ALLOWED, IllegalTransition, LifecycleParams, SetupState, SetupTracker, can_transition, create_setup, family_for
from tests.conftest import make_candles


def _ind(c: Candle, atr=20.0):
    return IndicatorRow(c.symbol, c.security_id, c.expiry_date, c.timeframe, c.open_time, 101.0, 100.0, 1.0, 0.0, 55.0, RsiDirection.FLAT,
                        atr, c.volume, None, 20, 50, 14, 14, candle_id=c.id)


def _bar(base: Candle, close, low=None, high=None):
    low = close - 10 if low is None else low
    high = close + 10 if high is None else high
    return Candle(**{**base.__dict__, "open": close - 2, "close": close, "high": max(high, close), "low": min(low, close)})


def _breakout_setup(candles):
    c0 = _bar(candles[0], 70120, low=70090)
    det = Detection(c0.symbol, c0.security_id, c0.expiry_date, c0.timeframe, c0.open_time, DetectionFamily.BREAKOUT, "BREAKOUT",
                    Direction.BULLISH, c0.close, "BREAKOUT · above swing high", payload={"level_price": 70100.0, "level_kind": "swing_high"},
                    candle_id=11, id=5)
    setup, t = create_setup(det, c0.with_id(11), _ind(c0))
    return setup, t, c0


def test_state_machine_transitions_table():
    assert can_transition(SetupState.FAKEOUT_RISK, SetupState.CONFIRMED)
    assert can_transition(SetupState.FAKEOUT_RISK, SetupState.INVALIDATED)
    assert not can_transition(SetupState.COMPLETED, SetupState.WATCH)
    assert not can_transition(SetupState.WATCH, SetupState.CONFIRMED)
    assert ALLOWED[SetupState.INVALIDATED] == set()
    for st in SetupState:
        assert st in ALLOWED


def test_create_setup_from_breakout_sets_anchor_and_invalidation():
    candles = make_candles(2)
    setup, t, c0 = _breakout_setup(candles)
    assert setup.state is SetupState.OBSERVING and setup.family == "breakout"
    assert setup.anchor_price == 70100.0 and setup.invalidation_price == 70090
    assert t.from_state is SetupState.OBSERVING and t.to_state is SetupState.WATCH and t.evidence["detection_id"] == 5
    assert setup.context["beyond_closes"] == 1
    # non-origin detections do not create setups
    d = Detection(c0.symbol, c0.security_id, c0.expiry_date, c0.timeframe, c0.open_time, DetectionFamily.WICK, "UPPER_REJECTION",
                  Direction.BEARISH, 1.0, "x", candle_id=1, id=2)
    assert family_for(d) is None and create_setup(d, c0.with_id(1), _ind(c0)) is None


def test_full_breakout_path_confirmed_pullback_continuation_completed():
    candles = make_candles(30)
    setup, t, c0 = _breakout_setup(candles)
    setup.state = SetupState.WATCH
    tracker = SetupTracker(LifecycleParams(acceptance_closes=2, retest_tolerance_atr=0.25, completion_bars=3, max_age_bars=48))
    path = []
    plan = [(70130, 70115, 70140),  # acceptance -> DEVELOPING
            (70150, 70125, 70160),  # follow-through -> CONFIRMED
            (70140, 70103, 70150),  # retest (low within 0.25*20 of 70100) -> PULLBACK
            (70165, 70140, 70170),  # new extreme -> CONTINUATION
            (70170, 70150, 70180), (70175, 70160, 70185), (70180, 70170, 70190)]  # 3 continuation bars -> COMPLETED
    for i, (cl, lo, hi) in enumerate(plan, start=1):
        c = _bar(candles[i], cl, lo, hi).with_id(100 + i)
        for tr in tracker.evaluate(setup, c, _ind(c)):
            path.append((tr.from_state, tr.to_state, tr.reason))
            assert tr.evidence["close"] == cl and "anchor" in tr.evidence
    states = [p[1] for p in path]
    assert states == [SetupState.DEVELOPING, SetupState.CONFIRMED, SetupState.PULLBACK, SetupState.CONTINUATION, SetupState.COMPLETED]
    assert setup.closed_at is not None and setup.state.is_terminal
    assert tracker.evaluate(setup, _bar(candles[10], 70000).with_id(999), _ind(candles[10])) == []


def test_fakeout_risk_reclaim_and_second_failure_paths():
    candles = make_candles(10)
    setup, _, _ = _breakout_setup(candles)
    setup.state = SetupState.WATCH
    tracker = SetupTracker(LifecycleParams(acceptance_closes=2))
    # close back inside -> FAKEOUT_RISK ; reclaim -> CONFIRMED
    c1 = _bar(candles[1], 70095, 70092, 70110).with_id(1)
    t1 = tracker.evaluate(setup, c1, _ind(c1))
    assert [x.to_state for x in t1] == [SetupState.FAKEOUT_RISK] and t1[0].reason == "close back inside anchor"
    c2 = _bar(candles[2], 70125, 70100, 70130).with_id(2)
    t2 = tracker.evaluate(setup, c2, _ind(c2))
    assert [x.to_state for x in t2] == [SetupState.CONFIRMED] and "reclaim" in t2[0].reason
    # second failure path: inside -> FAKEOUT_RISK, inside again -> INVALIDATED
    c3 = _bar(candles[3], 70097, 70092, 70120).with_id(3)
    assert [x.to_state for x in tracker.evaluate(setup, c3, _ind(c3))] == [SetupState.FAKEOUT_RISK]
    c4 = _bar(candles[4], 70096, 70092, 70105).with_id(4)
    t4 = tracker.evaluate(setup, c4, _ind(c4))
    assert [x.to_state for x in t4] == [SetupState.INVALIDATED] and t4[0].reason == "second close back inside anchor"
    assert setup.context["failures"] == 2


def test_invalidation_level_and_max_age():
    candles = make_candles(10)
    setup, _, _ = _breakout_setup(candles)
    setup.state = SetupState.WATCH
    tracker = SetupTracker(LifecycleParams())
    c1 = _bar(candles[1], 70080, 70070, 70125).with_id(1)  # close below invalidation 70090
    t = tracker.evaluate(setup, c1, _ind(c1))
    assert [x.to_state for x in t] == [SetupState.INVALIDATED] and t[0].reason == "close beyond invalidation level"

    setup2, _, _ = _breakout_setup(candles)
    setup2.state = SetupState.WATCH
    tracker2 = SetupTracker(LifecycleParams(acceptance_closes=99, max_age_bars=3))
    out = []
    for i in range(1, 4):
        c = _bar(candles[i], 70120 + i, 70110, 70140).with_id(i)
        out += tracker2.evaluate(setup2, c, _ind(c))
    assert [x.to_state for x in out] == [SetupState.COMPLETED] and out[0].reason == "max age reached"


def test_bearish_mirror():
    candles = make_candles(6)
    c0 = _bar(candles[0], 69880, 69870, 69910)
    det = Detection(c0.symbol, c0.security_id, c0.expiry_date, c0.timeframe, c0.open_time, DetectionFamily.BREAKOUT, "BREAKOUT",
                    Direction.BEARISH, c0.close, "BREAKOUT · below swing low", payload={"level_price": 69900.0}, candle_id=1, id=1)
    setup, _ = create_setup(det, c0.with_id(1), _ind(c0))
    assert setup.invalidation_price == 69910
    setup.state = SetupState.WATCH
    tracker = SetupTracker(LifecycleParams(acceptance_closes=2))
    c1 = _bar(candles[1], 69870, 69860, 69890).with_id(2)
    c2 = _bar(candles[2], 69850, 69840, 69875).with_id(3)
    assert [x.to_state for x in tracker.evaluate(setup, c1, _ind(c1))] == [SetupState.DEVELOPING]
    assert [x.to_state for x in tracker.evaluate(setup, c2, _ind(c2))] == [SetupState.CONFIRMED]


def test_lifecycle_rejects_incomplete_candle():
    candles = make_candles(2)
    setup, _, _ = _breakout_setup(candles)
    with pytest.raises(ValueError):
        SetupTracker(LifecycleParams()).evaluate(setup, Candle(**{**candles[1].__dict__, "is_closed": False}), _ind(candles[1]))
