"""Restart / replay idempotency: the same history replayed into the same DB changes nothing."""
from __future__ import annotations

from aureon_mcx.market.aggregation import aggregate_closed
from aureon_mcx.market.sessions import SessionCalendar
from aureon_mcx.market.timeframe import Timeframe
from aureon_mcx.observer import NullSink, SymbolObserver
from aureon_mcx.outcomes import OutcomeService
from aureon_mcx.storage import Database, Repositories
from tests.conftest import make_candles
from tests.test_observer import contract, trending_prices

TABLES = ["candles", "indicators", "structure_pivots", "detections", "setups", "setup_events", "clearances", "feature_snapshots",
          "outcome_observations", "market_day_frames", "session_state"]


def _counts(repos):
    return {t: repos.db.query_one(f"SELECT COUNT(*) AS n FROM {t}")["n"] for t in TABLES}


def _run(app_config, db_path, history, live):
    db = Database(db_path)
    db.migrate()
    repos = Repositories(db)
    cal = SessionCalendar(app_config.sessions)
    sink = NullSink()
    obs = SymbolObserver(app_config, contract(), repos, cal, OutcomeService(repos, app_config.analysis, cal), sink)
    obs.warm(history)
    for c in live:
        obs.on_closed_candle(c)
    snapshot = {
        "counts": _counts(repos),
        "origins": [r["origin_detection_id"] for r in repos.db.query("SELECT origin_detection_id FROM setups ORDER BY id")],
        "states": [r["state"] for r in repos.db.query("SELECT state FROM setups ORDER BY id")],
        "frames": [(r["trading_date"], r["bars"], r["open"], r["high"], r["low"], r["close"], r["is_closed"])
                   for r in repos.db.query("SELECT * FROM market_day_frames ORDER BY trading_date")],
        "events": [(r["setup_id"], r["candle_id"], r["to_state"]) for r in repos.db.query("SELECT * FROM setup_events ORDER BY id")],
        "indicators": [(r["candle_id"], r["ema_fast"], r["rsi"]) for r in repos.db.query("SELECT * FROM indicators ORDER BY id LIMIT 50")],
    }
    db.close()
    return snapshot, sink


def test_restart_replay_is_idempotent(app_config, tmp_path):
    db_path = tmp_path / "restart.db"
    m5 = make_candles(360, prices=trending_prices(360))
    m15 = aggregate_closed(m5, Timeframe.M15)
    h1 = aggregate_closed(m5, Timeframe.H1)
    history = {Timeframe.M5: m5[:300], Timeframe.M15: [c for c in m15 if c.close_time <= m5[299].close_time],
               Timeframe.H1: [c for c in h1 if c.close_time <= m5[299].close_time]}
    live = [c for c in m5[300:]] + [c for c in m15 if c.close_time > m5[299].close_time] + [c for c in h1 if c.close_time > m5[299].close_time]
    live.sort(key=lambda c: (c.close_time, -c.timeframe.rank))
    first, _ = _run(app_config, db_path, history, live)
    assert first["counts"]["setups"] > 0 and first["counts"]["setup_events"] > 0 and first["counts"]["feature_snapshots"] > 0
    assert len(first["origins"]) == len(set(first["origins"]))
    assert len(first["events"]) == len(set(first["events"]))
    # restart: same DB, same history replayed through warmup, then the same live candles again
    replay_history = {Timeframe.M5: m5, Timeframe.M15: m15, Timeframe.H1: h1}
    second, sink = _run(app_config, db_path, replay_history, live)
    assert second["counts"] == first["counts"]
    assert second["origins"] == first["origins"] and second["states"] == first["states"]
    assert second["events"] == first["events"]
    assert second["frames"] == first["frames"]
    assert second["indicators"] == first["indicators"]  # first analysed values remain authoritative
    assert sink.views == []  # nothing published during a pure replay
    # a third run adds nothing either
    third, _ = _run(app_config, db_path, replay_history, live)
    assert third["counts"] == first["counts"]


def test_day_frame_bars_do_not_inflate_on_replay(app_config, repos):
    from aureon_mcx.market.timeutil import IST

    cal = SessionCalendar(app_config.sessions)
    candles = make_candles(100)
    stored = repos.candles.insert_many(candles)
    d = cal.trading_date(stored[0].open_time)
    s, e = cal.trading_day_span(d)
    for _ in range(2):
        for c in stored:
            repos.day_frames.refresh("GOLD", "428291", "2026-10-05", Timeframe.M5, d, s, e, False)
    frame = repos.day_frames.get("428291", Timeframe.M5, d)
    assert frame["bars"] == 100
    assert frame["open"] == candles[0].open and frame["close"] == candles[-1].close
    assert frame["high"] == max(c.high for c in candles) and frame["low"] == min(c.low for c in candles)
    assert frame["is_closed"] == 0
    repos.day_frames.close_frame("428291", Timeframe.M5, d)
    repos.day_frames.refresh("GOLD", "428291", "2026-10-05", Timeframe.M5, d, s, e, False)
    assert repos.day_frames.get("428291", Timeframe.M5, d)["is_closed"] == 1  # closed frames are frozen


def test_setup_insert_is_idempotent_per_origin_detection(repos):
    from aureon_mcx.detection.models import Detection, DetectionFamily, Direction
    from aureon_mcx.setups.models import Setup, SetupEvent, SetupState

    c = repos.candles.insert(make_candles(1)[0])
    det = repos.detections.insert(Detection(c.symbol, c.security_id, c.expiry_date, c.timeframe, c.open_time, DetectionFamily.BREAKOUT,
                                            "BREAKOUT", Direction.BULLISH, c.close, "x", candle_id=c.id))
    mk = lambda: Setup(c.symbol, c.security_id, c.expiry_date, c.timeframe, "breakout", Direction.BULLISH, SetupState.WATCH, 1.0, 0.5,  # noqa: E731
                       det.id, c.id, c.open_time, c.open_time)
    s1, created1 = repos.setups.insert_or_existing(mk())
    s1.state = SetupState.INVALIDATED
    repos.setups.update_state(s1)
    s2, created2 = repos.setups.insert_or_existing(mk())
    assert created1 and not created2 and s2.id == s1.id and s2.state is SetupState.INVALIDATED
    assert repos.db.query_one("SELECT COUNT(*) AS n FROM setups")["n"] == 1
    e1 = repos.setups.add_event(SetupEvent(s1.id, c.id, SetupState.OBSERVING, SetupState.WATCH, "r", {}, c.open_time))
    e2 = repos.setups.add_event(SetupEvent(s1.id, c.id, SetupState.OBSERVING, SetupState.WATCH, "r", {}, c.open_time))
    e3 = repos.setups.add_event(SetupEvent(s1.id, c.id, SetupState.WATCH, SetupState.DEVELOPING, "r", {}, c.open_time))
    assert e1.id == e2.id and e3.id != e1.id
    assert len(repos.setups.events(s1.id)) == 2
    assert repos.setups.by_origin(det.id).id == s1.id


def test_monitor_subscriptions_persist(repos):
    from aureon_mcx.detection.models import Detection, DetectionFamily, Direction
    from aureon_mcx.setups.models import Setup, SetupState

    c = repos.candles.insert(make_candles(1)[0])
    det = repos.detections.insert(Detection(c.symbol, c.security_id, c.expiry_date, c.timeframe, c.open_time, DetectionFamily.BREAKOUT,
                                            "BREAKOUT", Direction.BULLISH, c.close, "x", candle_id=c.id))
    s = repos.setups.insert(Setup(c.symbol, c.security_id, c.expiry_date, c.timeframe, "breakout", Direction.BULLISH, SetupState.WATCH,
                                  1.0, 0.5, det.id, c.id, c.open_time, c.open_time))
    assert repos.monitors.add(s.id, 42) and not repos.monitors.add(s.id, 42) and repos.monitors.add(s.id, 43)
    assert repos.monitors.users_for(s.id) == ["42", "43"] and repos.monitors.all() == {s.id: ["42", "43"]}
