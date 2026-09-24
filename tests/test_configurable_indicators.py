"""Non-default indicator periods must drive both calculations and every presentation label."""
from __future__ import annotations

import math

from aureon_mcx.config import load_config
from aureon_mcx.confirmation import ConfirmationInputs, evaluate_clearance
from aureon_mcx.detection import EmaDetector
from aureon_mcx.detection.models import Direction
from aureon_mcx.discord.card_builder import build_card
from aureon_mcx.discord.chart_renderer import plan_chart
from aureon_mcx.indicators import IndicatorEngine, IndicatorParams
from aureon_mcx.market.sessions import SessionCalendar
from aureon_mcx.market.timeframe import Timeframe
from aureon_mcx.mtf import TrendDirection, classify_timeframe
from aureon_mcx.mtf.context import ema_relation
from aureon_mcx.observer import NullSink, SymbolObserver
from aureon_mcx.outcomes import FEATURE_SCHEMA_VERSION, OutcomeService
from aureon_mcx.setups.models import Setup, SetupState
from aureon_mcx.structure.models import StructureContext
from tests.conftest import ROOT, make_candles
from tests.test_indicators import _ref_ema
from tests.test_observer import contract, trending_prices


def _cfg(monkeypatch, tmp_path):
    monkeypatch.setenv("EMA_FAST", "10")
    monkeypatch.setenv("EMA_SLOW", "30")
    monkeypatch.setenv("RSI_PERIOD", "7")
    monkeypatch.setenv("ATR_PERIOD", "10")
    return load_config(config_dir=ROOT / "config", env_file=tmp_path / "none.env")


def test_engine_uses_configured_periods():
    candles = make_candles(80)
    rows = IndicatorEngine(IndicatorParams(ema_fast=10, ema_slow=30, rsi_period=7, atr_period=10)).warm(candles)
    closes = [c.close for c in candles]
    for r, f, s in zip(rows, _ref_ema(closes, 10), _ref_ema(closes, 30)):
        if f is not None:
            assert math.isclose(r.ema_fast, f, rel_tol=1e-12)
        if s is not None:
            assert math.isclose(r.ema_slow, s, rel_tol=1e-12)
    assert rows[8].ema_fast is None and rows[9].ema_fast is not None
    assert rows[28].ema_slow is None and rows[29].ema_slow is not None
    assert rows[6].rsi is None and rows[7].rsi is not None
    assert rows[8].atr is None and rows[9].atr is not None
    assert (rows[-1].ema_fast_period, rows[-1].ema_slow_period, rows[-1].rsi_period, rows[-1].atr_period) == (10, 30, 7, 10)


def test_labels_follow_configured_periods(monkeypatch, tmp_path, db):
    from aureon_mcx.storage import Repositories

    cfg = _cfg(monkeypatch, tmp_path)
    assert (cfg.env.EMA_FAST, cfg.env.EMA_SLOW, cfg.env.RSI_PERIOD, cfg.env.ATR_PERIOD) == (10, 30, 7, 10)
    repos = Repositories(db)
    cal = SessionCalendar(cfg.sessions)
    sink = NullSink()
    obs = SymbolObserver(cfg, contract(), repos, cal, OutcomeService(repos, cfg.analysis, cal), sink)
    for c in make_candles(220, prices=trending_prices(220)):
        obs.on_closed_candle(c)
    ind = obs.indicators[Timeframe.M5].latest
    assert ind.ema_fast_period == 10 and ind.ema_slow_period == 30
    assert ema_relation(ind) in ("EMA10 > EMA30", "EMA10 < EMA30", "EMA10 = EMA30")
    assert obs.reads[Timeframe.M5].ema_relation.startswith("EMA10 ")
    # detection payloads carry period-agnostic keys plus the configured periods
    det = repos.db.query_one("SELECT payload_json FROM detections WHERE family = 'ema' ORDER BY id LIMIT 1")
    assert det is not None and '"ema_fast_period": 10' in det["payload_json"] and '"ema20"' not in det["payload_json"]
    # views / cards / charts
    assert sink.views, "expected at least one published view"
    v = sink.views[-1]
    assert (v.ema_fast_name, v.ema_slow_name, v.rsi_name, v.atr_name) == ("EMA10", "EMA30", "RSI7", "ATR10")
    card = build_card(v)
    mom = card.sections[2].body
    assert "EMA10:" in mom and "EMA30:" in mom and "RSI7:" in mom and "ATR10:" in mom
    assert "EMA20" not in card.text() and "RSI14" not in card.text() and "ATR14" not in card.text()
    candles = repos.candles.latest("428291", Timeframe.M5, 120)
    plan = plan_chart(v, candles, repos.indicators.for_candles([c.id for c in candles]), [], [], [], cfg.analysis.declutter)
    assert plan.ema_fast_label == "EMA10" and plan.ema_slow_label == "EMA30" and plan.rsi_label == "RSI7"
    assert any("EMA10" in l for l in plan.context_lines)
    # confirmation blockers name the configured EMAs
    s = Setup("GOLD", "428291", "e", Timeframe.M5, "breakout", Direction.BULLISH if ind.ema_fast < ind.ema_slow else Direction.BEARISH,
              SetupState.CONFIRMED, 1.0, 0.5, 1, 1, ind.open_time, ind.open_time, id=1)
    r = evaluate_clearance(ConfirmationInputs(setup=s, candle=candles[-1], indicators=ind, present_trend=TrendDirection.SIDEWAYS, mtf=None,
                                              structure=StructureContext.MIXED), cfg.policy)
    assert "EMA10/EMA30 do not confirm direction" in r.blockers
    # snapshots use the versioned, period-agnostic schema
    snap = repos.db.query_one("SELECT feature_schema_version, features_json FROM feature_snapshots ORDER BY id DESC LIMIT 1")
    assert snap["feature_schema_version"] == FEATURE_SCHEMA_VERSION == "features-v2"
    assert '"ema_fast_period": 10' in snap["features_json"] and '"rsi_period": 7' in snap["features_json"]


def test_snapshots_store_real_mtf_state(app_config, repos):
    cal = SessionCalendar(app_config.sessions)
    obs = SymbolObserver(app_config, contract(), repos, cal, OutcomeService(repos, app_config.analysis, cal), NullSink())
    from aureon_mcx.market.aggregation import aggregate_closed

    m5 = make_candles(300, prices=trending_prices(300))
    obs.warm({Timeframe.M5: m5[:200], Timeframe.M15: aggregate_closed(m5[:200], Timeframe.M15, calendar=cal),
              Timeframe.H1: aggregate_closed(m5[:200], Timeframe.H1, calendar=cal)})
    for c in m5[200:]:
        obs.on_closed_candle(c)
    rows = repos.db.query("SELECT mtf_state, features_json, direction FROM feature_snapshots ORDER BY id DESC LIMIT 40")
    assert rows
    import json

    seen_states = set()
    for r in rows:
        assert r["mtf_state"] is not None
        f = json.loads(r["features_json"])
        assert f["mtf_state"] == r["mtf_state"] and f["mtf_alignment"] in ("ALIGNED", "AGAINST", "CONFLICT", "NO_CONTEXT")
        assert set(f["mtf_reads"]) >= {"M5", "M15", "H1"}
        assert isinstance(f["mtf_higher_available"], bool) and isinstance(f["mtf_early_reversal"], bool)
        assert r["direction"] in ("BULLISH", "BEARISH")  # neutral detections never get a direction-based assessment
        seen_states.add(r["mtf_state"])
    assert seen_states <= {"MTF ALIGNED", "MTF AGAINST", "MTF CONFLICT", "MTF NO CONTEXT"}
    assert repos.db.query_one("SELECT COUNT(*) AS n FROM feature_snapshots WHERE mtf_state IS NULL")["n"] == 0
