from __future__ import annotations

import sqlite3
from datetime import datetime, timezone

import pytest

from aureon_mcx.detection.models import Detection, DetectionFamily, Direction
from aureon_mcx.indicators.models import IndicatorRow, RsiDirection
from aureon_mcx.market.candle import Candle
from aureon_mcx.market.timeframe import Timeframe
from aureon_mcx.outcomes.models import FeatureSnapshot, OutcomeObservation
from aureon_mcx.setups.models import Setup, SetupEvent, SetupState
from aureon_mcx.storage import StorageError
from aureon_mcx.storage.migrations import SCHEMA_VERSION
from aureon_mcx.storage.repositories import DependencyError
from tests.conftest import make_candles


def test_pragmas_and_schema_version(db):
    p = db.pragmas()
    assert p["journal_mode"].lower() == "wal"
    assert p["foreign_keys"] == 1
    assert p["busy_timeout"] == 5000
    assert db.query_one("SELECT MAX(version) AS v FROM schema_version")["v"] == SCHEMA_VERSION
    names = {r["name"] for r in db.query("SELECT name FROM sqlite_master WHERE type='table'")}
    for t in ["instruments", "candles", "indicators", "detections", "setup_events", "setups", "symbol_state",
              "session_state", "market_day_frames", "outcome_observations", "discord_message_refs", "feature_snapshots"]:
        assert t in names


def test_migrate_is_idempotent(db):
    assert db.migrate() == SCHEMA_VERSION
    assert db.migrate() == SCHEMA_VERSION


def _indicator_row(c: Candle, candle_id) -> IndicatorRow:
    return IndicatorRow(symbol=c.symbol, security_id=c.security_id, expiry_date=c.expiry_date, timeframe=c.timeframe,
                        open_time=c.open_time, ema_fast=1.0, ema_slow=2.0, ema_gap=-1.0, ema_fast_slope=0.1, rsi=55.0,
                        rsi_direction=RsiDirection.RISING, atr=10.0, volume=1, open_interest=None, ema_fast_period=20,
                        ema_slow_period=50, rsi_period=14, atr_period=14, candle_id=candle_id)


def test_candle_roundtrip_and_incomplete_rejected(repos):
    c = make_candles(1)[0]
    stored = repos.candles.insert(c)
    assert stored.id is not None
    back = repos.candles.get(stored.id)
    assert back.open_time == c.open_time and back.open_time.tzinfo is not None
    assert back.close == c.close
    # idempotent
    again = repos.candles.insert(c)
    assert again.id == stored.id
    with pytest.raises(StorageError):
        repos.candles.insert(Candle(**{**c.__dict__, "is_closed": False, "id": None}))


def test_dependent_rows_cannot_precede_parents(repos):
    c = make_candles(1)[0]
    # indicator before candle: no id -> DependencyError
    with pytest.raises(DependencyError):
        repos.indicators.insert(_indicator_row(c, None))
    # indicator referencing a nonexistent candle id -> FK violation
    with pytest.raises(sqlite3.IntegrityError):
        repos.indicators.insert(_indicator_row(c, 9999))
    stored = repos.candles.insert(c)
    row = repos.indicators.insert(_indicator_row(c, stored.id))
    assert row.id is not None

    det = Detection(symbol=c.symbol, security_id=c.security_id, expiry_date=c.expiry_date, timeframe=c.timeframe,
                    open_time=c.open_time, family=DetectionFamily.EMA, kind="BULL_CROSS", direction=Direction.BULLISH,
                    price=c.close, label="▲ BULL CROSS · ASIA")
    with pytest.raises(DependencyError):
        repos.detections.insert(det)
    det.candle_id = stored.id
    repos.detections.insert(det)
    assert det.id is not None

    # setup before detection persisted
    setup = Setup(symbol=c.symbol, security_id=c.security_id, expiry_date=c.expiry_date, timeframe=c.timeframe, family="ema_cross",
                  direction=Direction.BULLISH, state=SetupState.OBSERVING, anchor_price=c.close, invalidation_price=c.low,
                  origin_detection_id=4242, origin_candle_id=stored.id, created_open_time=c.open_time, updated_open_time=c.open_time)
    with pytest.raises(sqlite3.IntegrityError):
        repos.setups.insert(setup)
    setup.origin_detection_id = det.id
    repos.setups.insert(setup)
    # event before setup persisted
    with pytest.raises(sqlite3.IntegrityError):
        repos.setups.add_event(SetupEvent(setup_id=777, candle_id=stored.id, from_state=None, to_state=SetupState.WATCH,
                                          reason="x", evidence={}, open_time=c.open_time))
    ev = repos.setups.add_event(SetupEvent(setup_id=setup.id, candle_id=stored.id, from_state=None, to_state=SetupState.WATCH,
                                           reason="x", evidence={"a": 1}, open_time=c.open_time))
    assert ev.id is not None

    # outcome before snapshot
    with pytest.raises(sqlite3.IntegrityError):
        repos.outcomes.upsert(OutcomeObservation(snapshot_id=555, detection_id=det.id, horizon="1", horizon_bars=1, bars_observed=1,
                                                 mfe=1, mae=1, mfe_atr=None, mae_atr=None, time_to_mfe_bars=1, time_to_mae_bars=1,
                                                 bars_to_invalidation=None, bars_to_follow_through=None, final_move=1, label="x",
                                                 label_version="v", last_candle_open_time=c.open_time, is_final=False))


def test_feature_snapshot_is_immutable(repos):
    c = repos.candles.insert(make_candles(1)[0])
    det = repos.detections.insert(Detection(symbol=c.symbol, security_id=c.security_id, expiry_date=c.expiry_date,
                                            timeframe=c.timeframe, open_time=c.open_time, family=DetectionFamily.WICK,
                                            kind="UPPER_REJECTION", direction=Direction.BEARISH, price=c.close,
                                            label="WICK · upper rejection", candle_id=c.id))
    snap = repos.snapshots.insert(FeatureSnapshot(detection_id=det.id, candle_id=c.id, symbol=c.symbol, security_id=c.security_id,
                                                  expiry_date=c.expiry_date, timeframe=c.timeframe, open_time=c.open_time,
                                                  direction="BEARISH", setup_family="wick", reference_price=c.close, atr=1.0, rsi=50.0,
                                                  ema_fast=1.0, ema_slow=1.0, present_trend="BEARISH", mtf_state="ALIGNED",
                                                  feature_schema_version="features-v1", features={"rsi": 50.0}))
    assert snap.id is not None
    with pytest.raises(sqlite3.IntegrityError, match="immutable"):
        repos.db.execute("UPDATE feature_snapshots SET rsi = 99 WHERE id = ?", (snap.id,))
    with pytest.raises(StorageError):
        repos.snapshots.update(snap)
    assert repos.snapshots.get(snap.id).rsi == 50.0


def test_short_transaction_rollback(db):
    with pytest.raises(RuntimeError):
        with db.transaction() as conn:
            conn.execute("INSERT INTO schema_version(version, applied_at) VALUES (999, 'x')")
            raise RuntimeError("boom")
    assert db.query_one("SELECT COUNT(*) AS n FROM schema_version WHERE version = 999")["n"] == 0
    assert not db.conn.in_transaction
