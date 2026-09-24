from __future__ import annotations

import math
from datetime import date, datetime, timedelta, timezone

from aureon_mcx.broker.dhan.symbol_resolver import ResolvedContract
from aureon_mcx.config.yaml_models import ContractPolicy
from aureon_mcx.market.aggregation import aggregate_closed
from aureon_mcx.market.sessions import SessionCalendar
from aureon_mcx.market.timeframe import Timeframe
from aureon_mcx.observer import NullSink, SymbolObserver
from aureon_mcx.outcomes import OutcomeService
from aureon_mcx.setups.models import SetupState
from tests.conftest import make_candles


def contract(symbol="GOLD", sid="428291"):
    now = datetime(2026, 9, 21, tzinfo=timezone.utc)
    return ResolvedContract(symbol, f"{symbol} OCT FUT", sid, "MCX", "MCX_COMM", "FUTCOM", date(2026, 10, 5), 100, 1.0, f"{symbol}-Oct2026-FUT",
                            "", now, "fixture", now, ContractPolicy.NEAREST_LIQUID, 3)


def trending_prices(n):
    out = []
    for i in range(n):
        wave = 60 * math.sin(i / 9.0)
        trend = 1.2 * i if i < n * 0.6 else 1.2 * n * 0.6 - 2.0 * (i - n * 0.6)
        out.append(70000 + trend + wave + (7 if i % 5 == 0 else -4))
    return out


def build_observer(app_config, repos):
    cal = SessionCalendar(app_config.sessions)
    sink = NullSink()
    obs = SymbolObserver(app_config, contract(), repos, cal, OutcomeService(repos, app_config.analysis, cal), sink)
    return obs, sink


def test_observer_end_to_end_persists_pipeline_once_per_candle(app_config, repos):
    obs, sink = build_observer(app_config, repos)
    m5 = make_candles(400, prices=trending_prices(400))
    m15 = aggregate_closed(m5, Timeframe.M15)
    h1 = aggregate_closed(m5, Timeframe.H1)
    counts = obs.warm({Timeframe.M5: m5[:300], Timeframe.M15: [c for c in m15 if c.close_time <= m5[299].close_time],
                       Timeframe.H1: [c for c in h1 if c.close_time <= m5[299].close_time]})
    assert counts[Timeframe.M5] == 300 and counts[Timeframe.M15] > 0 and counts[Timeframe.H1] > 0
    assert sink.views == []  # warmup never publishes
    # live phase
    for c in m5[300:]:
        obs.on_closed_candle(c)
        obs.on_closed_candle(c)  # duplicate delivery is ignored (once per closed candle)
    assert obs.closed_count == 400
    assert repos.candles.count("428291", Timeframe.M5) == 400
    assert repos.db.query_one("SELECT COUNT(*) AS n FROM indicators WHERE timeframe='M5'")["n"] == 400
    dets = repos.db.query_one("SELECT COUNT(*) AS n FROM detections")["n"]
    assert dets > 0
    fams = {r["family"] for r in repos.db.query("SELECT DISTINCT family FROM detections")}
    assert "ema" in fams and "rsi" in fams
    pivots = repos.db.query_one("SELECT COUNT(*) AS n FROM structure_pivots WHERE timeframe='M5'")["n"]
    assert pivots > 4
    # every pivot was confirmed strictly after its own candle
    for r in repos.db.query("SELECT pivot_open_time, confirmed_at_open_time FROM structure_pivots"):
        assert r["confirmed_at_open_time"] > r["pivot_open_time"]
    setups = repos.db.query("SELECT * FROM setups")
    assert setups, "trending synthetic data should create at least one setup"
    events = repos.db.query_one("SELECT COUNT(*) AS n FROM setup_events")["n"]
    assert events >= len(setups)
    # each setup has an OBSERVING->WATCH first event and a clearance row
    for s in setups:
        first = repos.db.query_one("SELECT * FROM setup_events WHERE setup_id = ? ORDER BY id LIMIT 1", (s["id"],))
        assert first["from_state"] == "OBSERVING" and first["to_state"] == "WATCH"
        assert repos.clearances.latest_for_setup(s["id"]) is not None
    # snapshots for all directional detections, none for neutral ones
    n_dir = repos.db.query_one("SELECT COUNT(*) AS n FROM detections WHERE direction != 'NEUTRAL'")["n"]
    assert repos.db.query_one("SELECT COUNT(*) AS n FROM feature_snapshots")["n"] == n_dir
    assert repos.db.query_one("SELECT COUNT(*) AS n FROM outcome_observations WHERE horizon != '__all_final__'")["n"] > 0
    # symbol + session state persisted
    st = repos.symbol_state.get("GOLD")
    assert st is not None and st["present_trend"] in ("BULLISH", "BEARISH", "SIDEWAYS", "unavailable")
    assert repos.db.query_one("SELECT COUNT(*) AS n FROM session_state")["n"] > 0
    assert repos.db.query_one("SELECT COUNT(*) AS n FROM session_state WHERE is_current = 1")["n"] <= 2
    # published views: only when the material state changed, FINAL CHECK semantics honoured
    assert sink.views
    for v in sink.views:
        assert v.final_check in ("⛔ NOT CLEARED", "✅ CLEARED FOR REVIEW")
        assert (v.cleared and not v.blockers) or (not v.cleared and v.blockers)
        assert v.policy_version == "strict-research-v1"
        assert "BUY" not in v.title and "SELL" not in v.title
    last_hash: dict[int, str] = {}
    for v in sink.views:  # never two consecutive publishes of an unchanged card
        h = v.state_hash()
        assert last_hash.get(v.setup_id) != h
        last_hash[v.setup_id] = h
    # day frames: previous days closed, latest open
    frames = repos.db.query("SELECT trading_date, is_closed FROM market_day_frames ORDER BY trading_date")
    assert len(frames) >= 2 and all(f["is_closed"] == 1 for f in frames[:-1])


def test_observer_higher_tf_close_reevaluates_open_setups(app_config, repos):
    obs, sink = build_observer(app_config, repos)
    m5 = make_candles(200, prices=trending_prices(200))
    for c in m5:
        obs.on_closed_candle(c)
    open_setups = repos.setups.open_for("428291", Timeframe.M5)
    before = len(sink.views)
    for c in aggregate_closed(m5, Timeframe.H1):
        obs.on_closed_candle(c)
    assert obs.reads[Timeframe.H1].direction is not None
    assert len(sink.views) >= before
    if open_setups:
        assert any(v.setup_id == open_setups[0].id for v in sink.views)
