from __future__ import annotations

import sqlite3
from datetime import datetime, timedelta, timezone

import pytest

from aureon_mcx.detection.models import Detection, DetectionFamily, Direction
from aureon_mcx.indicators.models import IndicatorRow, RsiDirection
from aureon_mcx.market.candle import Candle
from aureon_mcx.market.sessions import SessionCalendar
from aureon_mcx.outcomes import FEATURE_SCHEMA_VERSION, LABEL_VERSION, Horizon, OutcomeService, build_snapshot, cohort_stats, observe
from aureon_mcx.outcomes.labels import FAKEOUT, GOOD_CONTINUATION, NO_FOLLOW_THROUGH, PENDING, WRONG_DIRECTION
from aureon_mcx.outcomes.models import FeatureSnapshot
from aureon_mcx.storage import StorageError
from tests.conftest import make_candles


def _snap(direction="BULLISH", ref=70000.0, atr=20.0, open_time=None, id_=1):
    open_time = open_time or datetime(2026, 9, 21, 3, 30, tzinfo=timezone.utc)
    return FeatureSnapshot(1, 1, "GOLD", "428291", "2026-10-05", make_candles(1)[0].timeframe, open_time, direction, "breakout", ref, atr,
                           55.0, 1.0, 1.0, "BULLISH", "MTF ALIGNED", FEATURE_SCHEMA_VERSION, {}, id=id_)


def _future(start, highs_lows_closes):
    base = make_candles(len(highs_lows_closes), start=start)
    out = []
    for c, (h, l, cl) in zip(base, highs_lows_closes):
        out.append(Candle(**{**c.__dict__, "open": cl, "close": cl, "high": max(h, cl), "low": min(l, cl)}))
    return out


def test_mfe_mae_long():
    s = _snap("BULLISH", 70000, 20)
    fut = _future(s.open_time + timedelta(minutes=5), [(70010, 69990, 70005), (70035, 69995, 70030), (70020, 69970, 69980)])
    obs = observe(s, fut, [Horizon("3b", 3)], follow_through_atr=1.0, invalidation_atr=1.0)
    o = obs[0]
    assert o.mfe == 35 and o.time_to_mfe_bars == 2
    assert o.mae == 30 and o.time_to_mae_bars == 3
    assert o.mfe_atr == 1.75 and o.mae_atr == 1.5
    assert o.bars_to_follow_through == 2 and o.bars_to_invalidation == 3
    assert o.final_move == -20 and o.is_final and o.label == FAKEOUT and o.label_version == LABEL_VERSION


def test_mfe_mae_short():
    s = _snap("BEARISH", 70000, 20)
    fut = _future(s.open_time + timedelta(minutes=5), [(70010, 69990, 69995), (70005, 69960, 69970), (70040, 69975, 70030)])
    o = observe(s, fut, [Horizon("3b", 3)])[0]
    assert o.mfe == 40 and o.time_to_mfe_bars == 2   # ref - min low
    assert o.mae == 40 and o.time_to_mae_bars == 3   # max high - ref
    assert o.bars_to_follow_through == 2 and o.bars_to_invalidation == 3 and o.final_move == -30


def test_labels_good_continuation_wrong_direction_no_follow_through_pending():
    s = _snap("BULLISH", 70000, 20)
    start = s.open_time + timedelta(minutes=5)
    good = _future(start, [(70010, 69995, 70005), (70030, 70000, 70025), (70040, 70015, 70035)])
    assert observe(s, good, [Horizon("3b", 3)])[0].label == GOOD_CONTINUATION
    wrong = _future(start, [(70002, 69985, 69990), (69995, 69960, 69970), (69975, 69950, 69960)])
    assert observe(s, wrong, [Horizon("3b", 3)])[0].label == WRONG_DIRECTION
    flat = _future(start, [(70003, 69998, 70001), (70004, 69997, 70002), (70003, 69998, 70000)])
    assert observe(s, flat, [Horizon("3b", 3)])[0].label == NO_FOLLOW_THROUGH
    o = observe(s, flat[:2], [Horizon("3b", 3)])[0]
    assert not o.is_final and o.label == PENDING and o.bars_observed == 2
    assert observe(s, [], [Horizon("3b", 3)]) == []


def test_session_horizon_uses_window_end():
    s = _snap("BULLISH", 70000, 20)
    start = s.open_time + timedelta(minutes=5)
    fut = _future(start, [(70010, 69995, 70005)] * 6)
    end = start + timedelta(minutes=15)  # window covers 3 candles
    o = observe(s, fut, [Horizon("session", None)], session_end=end)[0]
    assert o.bars_observed == 3 and o.is_final
    o2 = observe(s, fut[:2], [Horizon("session", None)], session_end=end)[0]
    assert o2.bars_observed == 2 and not o2.is_final
    o3 = observe(s, fut[:2], [Horizon("session", None)], session_end=end, session_closed=True)[0]
    assert o3.is_final


def test_no_future_candles_leak_into_observation():
    s = _snap("BULLISH", 70000, 20)
    # candles at or before the detection open_time must be ignored even if passed in
    past = _future(s.open_time - timedelta(minutes=10), [(80000, 60000, 70000)] * 3)  # includes the detection bar itself
    fut = _future(s.open_time + timedelta(minutes=5), [(70010, 69995, 70005)])
    o = observe(s, past + fut, [Horizon("1b", 1)])[0]
    assert o.mfe == 10 and o.mae == 5
    incomplete = Candle(**{**fut[0].__dict__, "is_closed": False, "high": 90000})
    o = observe(s, [incomplete], [Horizon("1b", 1)])
    assert o == []


def test_snapshot_has_no_future_features_and_is_immutable(repos):
    candles = repos.candles.insert_many(make_candles(5))
    c = candles[0]
    ind = IndicatorRow(c.symbol, c.security_id, c.expiry_date, c.timeframe, c.open_time, 101.0, 100.0, 1.0, 0.5, 56.0, RsiDirection.RISING,
                       20.0, c.volume, None, 20, 50, 14, 14, candle_id=c.id)
    det = repos.detections.insert(Detection(c.symbol, c.security_id, c.expiry_date, c.timeframe, c.open_time, DetectionFamily.BREAKOUT,
                                            "BREAKOUT", Direction.BULLISH, c.close, "BREAKOUT · above swing high", session="ASIA",
                                            payload={"level_price": 70000}, candle_id=c.id))
    ctx = {"present_trend": "BULLISH", "mtf_state": "MTF ALIGNED", "structure_context": "BULLISH", "structure_sequence": "HL -> HH"}
    snap = build_snapshot(det, c, ind, ctx, "breakout")
    f = snap.features
    assert f["ema_fast"] == 101.0 and f["ema_fast_period"] == 20 and f["rsi"] == 56.0 and f["present_trend"] == "BULLISH" and f["timestamp"] == c.open_time.isoformat()
    assert f["feature_schema_version"] == FEATURE_SCHEMA_VERSION and f["contract_expiry"] == "2026-10-05"
    # no key refers to anything after the detection candle
    assert not any(k.startswith(("future", "outcome", "mfe", "mae", "label")) for k in f)
    assert f["candle"]["close"] == c.close
    stored = repos.snapshots.insert(snap)
    with pytest.raises(sqlite3.IntegrityError):
        repos.db.execute("UPDATE feature_snapshots SET features_json = '{}' WHERE id = ?", (stored.id,))
    with pytest.raises(StorageError):
        repos.snapshots.update(stored)
    # snapshot must be built from the detection candle
    with pytest.raises(ValueError):
        build_snapshot(det, candles[1], ind, ctx, "breakout")


def test_outcome_service_end_to_end(repos, app_config):
    candles = repos.candles.insert_many(make_candles(20, prices=[70000 + 15 * i for i in range(20)]))
    c = candles[0]
    ind = IndicatorRow(c.symbol, c.security_id, c.expiry_date, c.timeframe, c.open_time, 101.0, 100.0, 1.0, 0.5, 56.0, RsiDirection.RISING,
                       20.0, c.volume, None, 20, 50, 14, 14, candle_id=c.id)
    det = repos.detections.insert(Detection(c.symbol, c.security_id, c.expiry_date, c.timeframe, c.open_time, DetectionFamily.BREAKOUT,
                                            "BREAKOUT", Direction.BULLISH, c.close, "x", payload={}, candle_id=c.id))
    svc = OutcomeService(repos, app_config.analysis, SessionCalendar(app_config.sessions))
    snap = svc.freeze(det, c, ind, {}, "breakout")
    assert svc.freeze(det, c, ind, {}, "breakout").id == snap.id
    n = svc.update_pending("428291", now=candles[-1].close_time)
    assert n == 1
    obs = svc.observations_for_detection(det.id)
    names = {o.horizon for o in obs}
    assert {"1b", "3b", "6b", "12b", "session"} == names
    by = {o.horizon: o for o in obs}
    assert by["1b"].is_final and by["12b"].is_final and not by["session"].is_final
    assert by["12b"].label == GOOD_CONTINUATION
    # final rows are frozen
    o = by["1b"]
    o.mfe = 999
    repos.outcomes.upsert(o)
    assert repos.outcomes.for_snapshot(snap.id)[0].mfe != 999
    # cohort below minimum sample -> None
    assert cohort_stats(obs, "12b", min_sample=30) is None
    stats = cohort_stats(obs, "12b", min_sample=1)
    assert stats.n == 1 and stats.continuation == 1 and "Historical cohort" in stats.lines()[0]
