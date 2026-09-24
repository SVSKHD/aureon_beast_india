from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone

import pytest

from aureon_mcx.app import Application, StartupError
from aureon_mcx.broker.dhan.errors import DhanApiError
from aureon_mcx.market.aggregation import aggregate_closed
from aureon_mcx.market.candle_builder import Tick
from aureon_mcx.market.timeframe import Timeframe
from aureon_mcx.observer import NullSink
from tests.conftest import FIXTURES, ROOT, make_candles
from tests.test_observer import trending_prices

NOW = datetime(2026, 9, 21, 9, 0, tzinfo=timezone.utc)  # 14:30 IST Monday


class FakeHttp:
    def __init__(self, profile_ok=True):
        self.profile_ok = profile_ok
        self.closed = False

    def require_credentials(self):
        pass

    def get_json(self, path, context=None):
        if not self.profile_ok:
            raise DhanApiError("/profile", 401, "invalid token")
        return {"dhanClientId": "x", "tokenValidity": "30/09/2026 10:00"}

    def get_text(self, url, context=None):
        return (FIXTURES / "instrument_master_detailed.csv").read_text()

    def close(self):
        self.closed = True


class FakeHistorical:
    def __init__(self):
        self.calls = []

    def fetch(self, symbol, security_id, seg, itype, expiry, timeframe, start, end):
        self.calls.append((symbol, timeframe))
        n = 240
        m5 = make_candles(n, start=NOW - timedelta(minutes=5 * n), symbol=symbol, security_id=security_id, expiry=expiry,
                          prices=trending_prices(n))
        if timeframe is Timeframe.M5:
            return m5
        return aggregate_closed(m5, timeframe)


class FakeFeed:
    def __init__(self, on_tick, security_ids, health):
        self.on_tick = on_tick
        self.ids = security_ids
        self.subscribed = []
        self.connected = False
        self.disconnected = False

    async def subscribe(self, ids):
        self.subscribed += ids

    async def unsubscribe(self, ids):
        pass

    async def replace_subscription(self, old, new):
        self.subscribed = [s for s in self.subscribed if s != old] + [new]

    async def run(self, stop):
        self.connected = True
        t = NOW + timedelta(seconds=1)
        for i in range(0, 16 * 60, 15):  # 16 minutes of ticks for both symbols
            for sid, px in (("428291", 70500.0), ("429003", 84000.0)):
                self.on_tick(Tick(sid, px + (i % 40), t + timedelta(seconds=i), last_qty=1))
            await asyncio.sleep(0)
        stop.set()

    async def disconnect(self):
        self.disconnected = True


def _app(tmp_path, monkeypatch, profile_ok=True, symbols="GOLD,SILVER"):
    monkeypatch.setenv("DHAN_CLIENT_ID", "cid-1")
    monkeypatch.setenv("DHAN_ACCESS_TOKEN", "tok-secret-1")
    monkeypatch.setenv("AUREON_LOCAL_DB_PATH", str(tmp_path / "app.db"))
    monkeypatch.setenv("SYMBOLS", symbols)
    monkeypatch.chdir(tmp_path)
    from aureon_mcx.broker.dhan.instruments import DhanInstrumentProvider

    fake_http = FakeHttp(profile_ok)
    hist = FakeHistorical()
    feeds = []

    def feed_factory(cfg, on_tick, health):
        f = FakeFeed(on_tick, [], health)
        feeds.append(f)
        return f

    app = Application(config_dir=str(ROOT / "config"), env_file=str(tmp_path / "none.env"), http_factory=lambda cfg: fake_http,
                      instrument_provider_factory=lambda cfg, http: DhanInstrumentProvider("u", tmp_path / "m.csv", 24, http, now=lambda: NOW),
                      historical_factory=lambda cfg, http: hist, feed_factory=feed_factory,
                      sink_factory=lambda cfg, repos: (NullSink(), None), now=lambda: NOW)
    return app, fake_http, hist, feeds


def test_startup_order_and_live_run(tmp_path, monkeypatch, caplog):
    import logging

    caplog.set_level(logging.INFO)
    app, http, hist, feeds = _app(tmp_path, monkeypatch)
    app.startup()
    steps = [int(s.split(":")[0]) for s in app.startup_log]
    assert steps == [1, 2, 3, 4, 5, 6, 7, 8, 9]
    assert set(app.contracts) == {"GOLD", "SILVER"}
    assert app.contracts["GOLD"].security_id == "428291" and app.contracts["SILVER"].security_id == "429003"
    assert "GOLD -> security_id 428291 -> expiry 2026-10-05" in caplog.text
    assert "symbol_resolved logical=SILVER security_id=429003" in caplog.text
    assert "historical_warmup GOLD M5 bars=" in caplog.text and "historical_warmup SILVER M5 bars=" in caplog.text
    assert "tok-secret-1" not in caplog.text and "cid-1" not in caplog.text
    assert {tf for _, tf in hist.calls} == {Timeframe.M5, Timeframe.M15, Timeframe.H1}  # H4 never downloaded
    assert app.repos.instruments.active("GOLD")["security_id"] == "428291"
    assert app.history["GOLD"][Timeframe.H4]  # aggregated locally from closed H1
    warm_count = app.observers["GOLD"].closed_count
    assert warm_count >= 200
    assert app.repos.db.query_one("SELECT COUNT(*) AS n FROM setups")["n"] >= 0

    asyncio.run(app.run())
    steps = [int(s.split(":")[0]) for s in app.startup_log]
    assert steps == list(range(1, 15))
    assert "observer live" in caplog.text and "discord live" not in caplog.text  # headless sink in tests
    feed = feeds[0]
    assert sorted(feed.subscribed) == ["428291", "429003"] and feed.disconnected
    # live ticks produced closed M5 candles for both symbols, once each
    gold = app.observers["GOLD"]
    assert gold.closed_count >= warm_count + 3
    assert app.pipelines["428291"].closed_counts[Timeframe.M5] >= 3
    assert app.pipelines["429003"].closed_counts[Timeframe.M5] >= 3
    assert app.health.snapshot()["components"]["observer"]["status"] == "live"
    assert any("security_id 428291" in s for s in app.sink.statuses)
    assert http.closed


def test_startup_aborts_on_bad_credentials(tmp_path, monkeypatch):
    app, *_ = _app(tmp_path, monkeypatch, profile_ok=False)
    with pytest.raises(StartupError, match="credentials"):
        app.startup()
    assert app.startup_log == ["1:config loaded"]


def test_startup_aborts_without_credentials(tmp_path, monkeypatch):
    app, *_ = _app(tmp_path, monkeypatch)
    monkeypatch.delenv("DHAN_ACCESS_TOKEN")
    with pytest.raises(StartupError):
        app.startup()


def test_startup_aborts_on_symbol_resolution_failure(tmp_path, monkeypatch):
    app, http, *_ = _app(tmp_path, monkeypatch)
    text = (FIXTURES / "instrument_master_detailed.csv").read_text()
    http.get_text = lambda url, context=None: "\n".join(l for l in text.splitlines() if ",SILVER,SILVER," not in l)
    with pytest.raises(StartupError, match="SILVER"):
        app.startup()
    assert "5:resolved SILVER" not in app.startup_log


def test_rollover_swaps_subscription(tmp_path, monkeypatch):
    app, http, hist, feeds = _app(tmp_path, monkeypatch, symbols="GOLD")
    app.startup()

    async def go():
        app.stop = asyncio.Event()
        app.feed = FakeFeed(app._on_tick, [], app.health)
        await app.feed.subscribe(["428291"])
        app._build_pipelines()
        app.resolver._today = lambda: datetime(2026, 10, 3).date()  # inside rollover window -> DEC contract
        await app.check_rollover()

    asyncio.run(go())
    assert app.contracts["GOLD"].security_id == "431102"
    assert app.feed.subscribed == ["431102"] and "431102" in app.pipelines and "428291" not in app.pipelines
    assert app.repos.instruments.active("GOLD")["security_id"] == "431102"
