from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
FIXTURES = Path(__file__).resolve().parent / "fixtures"


@pytest.fixture(autouse=True)
def _clean_env(monkeypatch):
    """Tests never see real credentials or a real .env file."""
    for k in ("DHAN_CLIENT_ID", "DHAN_ACCESS_TOKEN", "DISCORD_TOKEN", "DISCORD_CHANNEL_ID", "DISCORD_GUILD_ID"):
        monkeypatch.delenv(k, raising=False)
    monkeypatch.setenv("AUREON_CONFIG_DIR", str(ROOT / "config"))
    yield


@pytest.fixture
def app_config(tmp_path):
    from aureon_mcx.config import load_config

    return load_config(config_dir=ROOT / "config", env_file=tmp_path / "nonexistent.env")


@pytest.fixture
def db(tmp_path):
    from aureon_mcx.storage import Database

    d = Database(tmp_path / "test.db")
    d.migrate()
    yield d
    d.close()


@pytest.fixture
def repos(db):
    from aureon_mcx.storage import Repositories

    return Repositories(db)


def make_candles(n: int, start: datetime | None = None, tf=None, prices=None, symbol="GOLD", security_id="428291",
                 expiry="2026-10-05", step_seconds=None, base=70000.0, amplitude=50.0):
    """Deterministic synthetic candle series (closed candles)."""
    from aureon_mcx.market.candle import Candle
    from aureon_mcx.market.timeframe import Timeframe

    tf = tf or Timeframe.M5
    start = start or datetime(2026, 9, 21, 3, 30, tzinfo=timezone.utc)  # 09:00 IST
    step = step_seconds or tf.seconds
    out = []
    prev_close = base
    for i in range(n):
        if prices is not None:
            close = float(prices[i])
            o = prev_close
        else:
            import math

            close = base + amplitude * math.sin(i / 3.0) + i * 0.5
            o = prev_close
        hi = max(o, close) + 5.0
        lo = min(o, close) - 5.0
        out.append(
            Candle(symbol=symbol, security_id=security_id, timeframe=tf, open_time=start + timedelta(seconds=step * i),
                   open=o, high=hi, low=lo, close=close, volume=100 + i, open_interest=1000.0, expiry_date=expiry)
        )
        prev_close = close
    return out
