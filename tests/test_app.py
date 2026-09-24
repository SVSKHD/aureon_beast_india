from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone

import pytest

from aureon_mcx.app import Application, StartupError
from aureon_mcx.broker.dhan.errors import DhanApiError
from aureon_mcx.market.aggregation import aggregate_closed
from aureon_mcx.market.candle import Candle
from aureon_mcx.market.candle_builder import Tick
from aureon_mcx.market.sessions import SessionCalendar
from aureon_mcx.market.timeframe import Timeframe
from aureon_mcx.market.timeutil import IST
from aureon_mcx.observer import NullSink
from tests.conftest import FIXTURES, ROOT, make_candles
from tests.test_observer import trending_prices

NOW = datetime(2026, 9, 21, 9, 0, tzinfo=timezone.utc)  # 14:30 IST Monday
SECURITY_PRICES = {"428291": 70500.0, "429003": 84000.0, "431102": 71000.0, "429001": 84500.0}


def m1_for(security_id: str, start: datetime, end: datetime, symbol="GOLD", expiry="e") -> list[Candle]:
    """Deterministic 'market' M1 series shared by the fake feed (ticks) and the fake historical API."""
    base = SECURITY_PRICES.get(security_id, 1000.0)
    out = []
    t = start.replace(second=0, microsecond=0)
    i = int((t - NOW).total_seconds() // 60)
    while t < end:
        o = base + (i % 13) * 2.0
        c = base + ((i + 5) % 11) * 2.0
        out.append(Candle(symbol=symbol, security_id=security_id, timeframe=Timeframe.M1, open_time=t, open=o, high=max(o, c) + 3, low=min(o, c) - 3,
                          close=c, volume=10.0, expiry_date=expiry))
        t += timedelta(minutes=1)
        i += 1
    return out


def ticks_for(c: Candle) -> list[Tick]:
    return [Tick(c.security_id, c.open, c.open_time + timedelta(seconds=1), last_qty=5.0),
            Tick(c.security_id, c.high, c.open_time + timedelta(seconds=15), last_qty=0.0),
            Tick(c.security_id, c.low, c.open_time + timedelta(seconds=30), last_qty=0.0),
            Tick(c.security_id, c.close, c.open_time + timedelta(seconds=45), last_qty=5.0)]


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
    def __init__(self, clock, fail_for: set[str] | None = None):
        self.calls = []
        self.clock = clock
        self.fail_for = fail_for or set()

    def fetch(self, symbol, security_id, seg, itype, expiry, timeframe, start, end):
        self.calls.append((symbol, security_id, timeframe, start, end))
        if security_id in self.fail_for:
            raise DhanApiError("/charts/intraday", 500, "simulated failure", {"security_id": security_id})
        now = self.clock[0]
        if timeframe is Timeframe.M1:
            return [c for c in m1_for(security_id, start, end, symbol, expiry) if c.close_time <= now]
        if start >= NOW:  # live-period broker bars derive from the same fake market as the ticks
            m1 = [c for c in m1_for(security_id, start, end, symbol, expiry) if c.close_time <= now]
            return aggregate_closed(m1, timeframe) if m1 else []
        # per-timeframe broker series ending at NOW (like the real warmup: exact broker candles per interval)
        n = {Timeframe.M5: 240, Timeframe.M15: 300, Timeframe.H1: 200}[timeframe]
        from aureon_mcx.market.timeutil import floor_to

        first = floor_to(NOW - timedelta(seconds=timeframe.seconds * n), timeframe.seconds)  # IST-aligned like Dhan's bars
        return make_candles(n, start=first, tf=timeframe, symbol=symbol, security_id=security_id, expiry=expiry,
                            prices=trending_prices(n), base=SECURITY_PRICES.get(security_id, 70000.0))


class FakeFeed:
    """Emits ticks derived from the shared fake market; optionally simulates an outage."""

    def __init__(self, on_tick, clock, minutes=16, outage: tuple[int, int] | None = None, ids=("428291", "429003"), settle=None):
        self.on_tick = on_tick
        self.settle = settle  # optional callable: True when nothing is pending (simulates real-time verification latency)
        self.clock = clock
        self.minutes = minutes
        self.outage = outage
        self.ids = list(ids)
        self.on_status = lambda s, d: None
        self.subscribed: list[str] = []
        self.unsubscribed: list[str] = []
        self.connected = False
        self.disconnected = False
        self.reconnects = 0

    async def subscribe(self, ids):
        self.subscribed += list(ids)

    async def unsubscribe(self, ids):
        self.unsubscribed += list(ids)
        self.subscribed = [s for s in self.subscribed if s not in ids]

    async def replace_subscription(self, old, new):
        await self.unsubscribe([old])
        await self.subscribe([new])

    async def run(self, stop):
        self.connected = True
        self.on_status("connected", {"reconnects": 0})
        await asyncio.sleep(0.05)
        series = {sid: m1_for(sid, NOW, NOW + timedelta(minutes=self.minutes)) for sid in self.ids}
        for i in range(self.minutes):
            if self.outage and i == self.outage[0]:
                self.connected = False
                self.on_status("reconnecting", {"delay": 1, "attempt": 1})
            if self.outage and self.outage[0] <= i < self.outage[1]:
                self.clock[0] = NOW + timedelta(minutes=i + 1)
                continue
            if self.outage and i == self.outage[1]:
                self.connected = True
                self.reconnects += 1
                self.clock[0] = NOW + timedelta(minutes=i, seconds=1)
                self.on_status("connected", {"reconnects": self.reconnects})
                await asyncio.sleep(0.3)  # recovery runs in a thread meanwhile; ticks below are buffered/replayed
            for sid in self.ids:
                for t in ticks_for(series[sid][i]):
                    self.clock[0] = t.ts
                    self.on_tick(t)
            await self.wait_settled()
        self.clock[0] = NOW + timedelta(minutes=self.minutes, seconds=3)
        await asyncio.sleep(0.05)
        stop.set()

    async def wait_settled(self):
        await asyncio.sleep(0)
        if self.settle is None:
            return
        for _ in range(200):  # broker verification takes a few ms here, seconds in reality: well within a minute
            if self.settle():
                return
            await asyncio.sleep(0.005)

    async def disconnect(self):
        self.disconnected = True


def _app(tmp_path, monkeypatch, profile_ok=True, symbols="GOLD,SILVER", feed_kwargs=None, fail_for=None):
    monkeypatch.setenv("DHAN_CLIENT_ID", "cid-1")
    monkeypatch.setenv("DHAN_ACCESS_TOKEN", "tok-secret-1")
    monkeypatch.setenv("AUREON_LOCAL_DB_PATH", str(tmp_path / "app.db"))
    monkeypatch.setenv("SYMBOLS", symbols)
    monkeypatch.chdir(tmp_path)
    from aureon_mcx.broker.dhan.instruments import DhanInstrumentProvider

    clock = [NOW]
    fake_http = FakeHttp(profile_ok)
    hist = FakeHistorical(clock, fail_for)
    feeds = []

    holder = {}

    def settled():
        app_ = holder.get("app")
        if app_ is None:
            return True
        return all(rt.pipeline is None or not [m for m in rt.pipeline.pending_minutes() if m + timedelta(minutes=1) <= clock[0]]
                   for rt in app_.runtimes.values())

    def feed_factory(cfg, on_tick, health):
        f = FakeFeed(on_tick, clock, settle=settled, **(feed_kwargs or {}))
        feeds.append(f)
        return f

    app = Application(config_dir=str(ROOT / "config"), env_file=str(tmp_path / "none.env"), http_factory=lambda cfg: fake_http,
                      instrument_provider_factory=lambda cfg, http: DhanInstrumentProvider("u", tmp_path / "m.csv", 24, http, now=lambda: NOW),
                      historical_factory=lambda cfg, http: hist, feed_factory=feed_factory,
                      sink_factory=lambda cfg, repos: (NullSink(), None), now=lambda: clock[0], housekeeping_interval=0.2)
    holder["app"] = app
    return app, fake_http, hist, feeds, clock


def test_startup_order_and_live_run(tmp_path, monkeypatch, caplog):
    import logging

    caplog.set_level(logging.INFO)
    app, http, hist, feeds, clock = _app(tmp_path, monkeypatch)
    app.startup()
    steps = [int(s.split(":")[0]) for s in app.startup_log]
    assert steps == [1, 2, 3, 4, 5, 6, 7, 8, 9]
    assert set(app.contracts) == {"GOLD", "SILVER"}
    assert app.contracts["GOLD"].security_id == "428291" and app.contracts["SILVER"].security_id == "429003"
    assert "GOLD -> security_id 428291 -> expiry 2026-10-05" in caplog.text
    assert "symbol_resolved logical=SILVER security_id=429003" in caplog.text
    assert "historical_warmup GOLD M5 bars=" in caplog.text and "historical_warmup SILVER M5 bars=" in caplog.text
    assert "tok-secret-1" not in caplog.text and "cid-1" not in caplog.text
    assert {tf for _, _, tf, _, _ in hist.calls} == {Timeframe.M5, Timeframe.M15, Timeframe.H1}  # H4 never downloaded
    assert app.repos.instruments.active("GOLD")["security_id"] == "428291"
    assert app.history["GOLD"][Timeframe.H4]  # aggregated locally from closed H1
    warm_count = app.observers["GOLD"].closed_count
    assert warm_count >= 200
    assert app.health.symbols["GOLD"].state == "WARMING"

    asyncio.run(app.run())
    steps = [int(s.split(":")[0]) for s in app.startup_log]
    assert steps == list(range(1, 15))
    assert "observer live" in caplog.text
    feed = feeds[0]
    assert sorted(feed.subscribed) == ["428291", "429003"] and feed.disconnected
    gold = app.observers["GOLD"]
    assert gold.closed_count >= warm_count + 3
    assert app.pipelines["428291"].closed_counts[Timeframe.M5] >= 3
    assert app.pipelines["429003"].closed_counts[Timeframe.M5] >= 3
    for sym in ("GOLD", "SILVER"):
        sh = app.health.symbols[sym]
        assert sh.state == "LIVE" and sh.unresolved_gaps == 0 and sh.last_tick_at is not None and "M5" in sh.last_closed
    snap = app.health.snapshot()
    assert snap["symbols"]["GOLD"]["security_id"] == "428291" and snap["symbols"]["GOLD"]["expiry"] == "2026-10-05"
    assert app.health.snapshot()["components"]["observer"]["status"] == "live"
    assert any("security_id 428291" in s for s in app.sink.statuses)
    assert app.task_failures == []
    assert http.closed


def test_reconnect_recovers_gap_and_matches_uninterrupted_result(tmp_path, monkeypatch):
    # uninterrupted reference
    (tmp_path / "ref").mkdir(exist_ok=True)
    (tmp_path / "out").mkdir(exist_ok=True)
    app_ref, _, _, _, _ = _app(tmp_path / "ref", monkeypatch, symbols="GOLD", feed_kwargs={"minutes": 16, "ids": ("428291",)})
    app_ref.startup()
    asyncio.run(app_ref.run())
    ref_m5 = [(c.open_time, c.open, c.high, c.low, c.close, c.volume) for c in app_ref.observers["GOLD"].candles[Timeframe.M5] if c.open_time >= NOW]
    assert len(ref_m5) == 3
    # outage from minute 6 to minute 10 (4 minutes without ticks)
    app, http, hist, feeds, clock = _app(tmp_path / "out", monkeypatch, symbols="GOLD",
                                         feed_kwargs={"minutes": 16, "ids": ("428291",), "outage": (6, 10)})
    app.startup()
    asyncio.run(app.run())
    sh = app.health.symbols["GOLD"]
    assert sh.reconnects == 1 and sh.state == "LIVE" and sh.unresolved_gaps == 0
    p = app.pipelines["428291"]
    assert p.continuity_ok and not p.gaps
    m1_calls = [c for c in hist.calls if c[2] is Timeframe.M1]
    assert m1_calls, "reconnect must backfill M1 from the historical API"
    out_m5 = [(c.open_time, c.open, c.high, c.low, c.close, c.volume) for c in app.observers["GOLD"].candles[Timeframe.M5] if c.open_time >= NOW]
    assert out_m5 == ref_m5
    from aureon_mcx.storage import Database

    db = Database(tmp_path / "out" / "app.db")  # the app closed its handle on shutdown
    assert db.query_one("SELECT COUNT(*) AS n FROM market_data_gaps")["n"] == 0
    db.close()


def test_startup_mid_bucket_recovers_missing_minutes(tmp_path, monkeypatch):
    """Warmup history ends at 14:30 IST but the process starts at 14:32: the two closed minutes
    are backfilled on connect so the 14:30 M5 bar is complete."""
    app, http, hist, feeds, clock = _app(tmp_path, monkeypatch, symbols="GOLD", feed_kwargs={"minutes": 8, "ids": ("428291",)})
    app.startup()
    clock[0] = NOW + timedelta(minutes=2, seconds=5)  # process start time

    async def go():
        # first connect happens at 14:32:05; the feed then streams from minute 2 onward
        f_holder = {}

        def feed_factory(cfg, on_tick, health):
            f = FakeFeed(on_tick, clock, minutes=8, ids=("428291",))
            f_holder["f"] = f

            async def run(stop):
                f.connected = True
                f.on_status("connected", {"reconnects": 0})
                await asyncio.sleep(0.3)
                series = m1_for("428291", NOW, NOW + timedelta(minutes=8))
                for i in range(2, 8):
                    for t in ticks_for(series[i]):
                        clock[0] = t.ts
                        on_tick(t)
                    await f.wait_settled()
                clock[0] = NOW + timedelta(minutes=8, seconds=3)
                await asyncio.sleep(0.05)
                stop.set()

            f.run = run
            return f

        app._feed_factory = feed_factory
        await app.run()

    asyncio.run(go())
    p = app.pipelines["428291"]
    assert p.continuity_ok and all(g.resolved for g in p.gaps)  # a late-verified minute may have repaired the bucket
    m5 = [c for c in app.observers["GOLD"].candles[Timeframe.M5] if c.open_time >= NOW]
    assert [c.open_time for c in m5] == [NOW]
    ref = aggregate_closed(m1_for("428291", NOW, NOW + timedelta(minutes=5)), Timeframe.M5)[0]
    assert (m5[0].open, m5[0].high, m5[0].low, m5[0].close, m5[0].volume) == (ref.open, ref.high, ref.low, ref.close, ref.volume)
    assert app.health.symbols["GOLD"].state == "LIVE"


def test_recovery_failure_fails_closed(tmp_path, monkeypatch):
    app, http, hist, feeds, clock = _app(tmp_path, monkeypatch, symbols="GOLD", feed_kwargs={"minutes": 12, "ids": ("428291",), "outage": (3, 7)})
    app.startup()
    hist.fail_for = {"428291"}  # historical API unavailable during the run
    asyncio.run(app.run())
    sh = app.health.symbols["GOLD"]
    assert sh.state == "ERROR" and not app.health.ok
    p = app.pipelines["428291"]
    assert p.suspended or not p.continuity_ok
    # the gap bar never reached the observer; analytics stopped at the gap
    m5 = [c for c in app.observers["GOLD"].candles[Timeframe.M5] if c.open_time >= NOW]
    assert all(c.open_time < NOW + timedelta(minutes=5) for c in m5)
    from aureon_mcx.storage import Database, Repositories

    db = Database(tmp_path / "app.db")
    assert Repositories(db).gaps.unresolved("428291")
    db.close()
    assert "RECOVERING_GAP" in str(app.sink.statuses) or "ERROR" in str(app.sink.statuses)


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


def _prepare_rollover(app, clock):
    app.stop = asyncio.Event()
    app._loop = asyncio.get_running_loop()
    app.feed = FakeFeed(app._on_tick, clock, ids=("428291",))
    app.feed.connected = True
    return app.feed


def test_rollover_is_staged_and_warm(tmp_path, monkeypatch):
    app, http, hist, feeds, clock = _app(tmp_path, monkeypatch, symbols="GOLD")
    app.startup()
    old_obs = app.observers["GOLD"]

    async def go():
        feed = _prepare_rollover(app, clock)
        await feed.subscribe(["428291"])
        app._build_pipelines()
        app.resolver._today = lambda: datetime(2026, 10, 3).date()  # inside rollover window -> DEC contract
        events = []
        orig_sub, orig_unsub = feed.subscribe, feed.unsubscribe

        async def sub(ids):
            events.append(("subscribe", tuple(ids), app.contracts["GOLD"].security_id))
            await orig_sub(ids)

        async def unsub(ids):
            events.append(("unsubscribe", tuple(ids), app.contracts["GOLD"].security_id))
            await orig_unsub(ids)

        feed.subscribe, feed.unsubscribe = sub, unsub
        await app.check_rollover()
        return events

    events = asyncio.run(go())
    new = app.contracts["GOLD"]
    assert new.security_id == "431102"
    assert app.observers["GOLD"] is not old_obs
    obs = app.observers["GOLD"]
    ind = obs.indicators[Timeframe.M5].latest
    assert ind is not None and ind.ema_fast is not None and ind.ema_slow is not None and ind.rsi is not None and ind.atr is not None
    assert obs.reads[Timeframe.M15].direction.value != "unavailable" and obs.reads[Timeframe.H1].direction.value != "unavailable"
    assert obs.indicators[Timeframe.H1].latest is not None and obs.candles[Timeframe.H4]  # H1 and H4 context present
    assert app.history["GOLD"][Timeframe.H4] and app.history["GOLD"][Timeframe.M5]
    p = app.pipelines["431102"]
    assert p.last_closed[Timeframe.M5] == app.history["GOLD"][Timeframe.M5][-1].open_time  # pipeline seeded
    assert "428291" not in app.pipelines
    # subscription changed only after the new contract was prepared: subscribe(new) then unsubscribe(old)
    assert events == [("subscribe", ("431102",), "428291"), ("unsubscribe", ("428291",), "431102")]
    assert app.feed.subscribed == ["431102"]
    assert app.repos.instruments.active("GOLD")["security_id"] == "431102"
    sh = app.health.symbols["GOLD"]
    assert sh.state == "LIVE" and sh.security_id == "431102" and sh.expiry == "2026-12-04"
    assert any("rolled -> security_id 431102" in s for s in app.sink.statuses)


def _record_states(app):
    states = []
    orig = app.health.set_symbol_state

    def rec(sym, state, detail="", **fields):
        states.append(state)
        return orig(sym, state, detail, **fields)

    app.health.set_symbol_state = rec
    return states


def test_rollover_state_progression_never_live_before_recovery(tmp_path, monkeypatch):
    app, http, hist, feeds, clock = _app(tmp_path, monkeypatch, symbols="GOLD")
    app.startup()
    states = _record_states(app)

    async def go():
        feed = _prepare_rollover(app, clock)
        await feed.subscribe(["428291"])
        app._build_pipelines()
        app.health.set_symbol_state("GOLD", "LIVE")
        states.clear()
        app.resolver._today = lambda: datetime(2026, 10, 3).date()
        await app.check_rollover()

    asyncio.run(go())
    assert states[0] == "ROLLOVER_WARMING"
    assert "RECOVERING_GAP" in states and states[-1] == "LIVE"
    assert states.index("RECOVERING_GAP") < states.index("LIVE")
    assert "LIVE" not in states[: states.index("RECOVERING_GAP")]  # never ROLLOVER_WARMING -> LIVE -> RECOVERING


def test_rollover_keeps_recovering_when_current_minute_is_partial(tmp_path, monkeypatch):
    app, http, hist, feeds, clock = _app(tmp_path, monkeypatch, symbols="GOLD")
    app.startup()
    clock[0] = NOW + timedelta(seconds=30)  # rollover happens half way through a minute

    async def go():
        feed = _prepare_rollover(app, clock)
        await feed.subscribe(["428291"])
        app._build_pipelines()
        app.resolver._today = lambda: datetime(2026, 10, 3).date()
        await app.check_rollover()
        # a tick in the partial minute, then the minute closes
        app._on_tick(Tick("431102", 71000.0, NOW + timedelta(seconds=35), last_qty=1))
        assert app.health.symbols["GOLD"].state == "RECOVERING_GAP"
        clock[0] = NOW + timedelta(minutes=1, seconds=2)
        app._on_tick(Tick("431102", 71001.0, NOW + timedelta(minutes=1, seconds=1), last_qty=1))
        for _ in range(100):
            await asyncio.sleep(0.01)
            if app.health.symbols["GOLD"].state == "LIVE":
                break

    asyncio.run(go())
    p = app.pipelines["431102"]
    assert NOW not in p.pending_verification and app.health.symbols["GOLD"].state == "LIVE"
    assert [c for c in hist.calls if c[1] == "431102" and c[2] is Timeframe.M1], "partial minute verified against broker M1"
    assert p.last_m1_open == NOW and p.m1._last_close == m1_for("431102", NOW, NOW + timedelta(minutes=1))[0].close


def test_rollover_ticks_during_subscribe_are_not_lost(tmp_path, monkeypatch):
    app, http, hist, feeds, clock = _app(tmp_path, monkeypatch, symbols="GOLD")
    app.startup()
    injected = []

    async def go():
        feed = _prepare_rollover(app, clock)
        await feed.subscribe(["428291"])
        app._build_pipelines()
        orig_sub = feed.subscribe

        async def subscribe(ids):
            await orig_sub(ids)
            if "431102" in ids:
                # the broker starts streaming immediately: five full minutes + the first tick of the next one
                for c in m1_for("431102", NOW, NOW + timedelta(minutes=5)):
                    for t in ticks_for(c):
                        clock[0] = t.ts
                        injected.append(t)
                        app._on_tick(t)
                t = Tick("431102", 71000.0, NOW + timedelta(minutes=5, seconds=1), last_qty=0.0)
                clock[0] = t.ts
                injected.append(t)
                app._on_tick(t)

        feed.subscribe = subscribe
        app.resolver._today = lambda: datetime(2026, 10, 3).date()
        await app.check_rollover()

    asyncio.run(go())
    assert app.contracts["GOLD"].security_id == "431102" and injected
    p = app.pipelines["431102"]
    assert p.last_m1_open == NOW + timedelta(minutes=5) or p.m1.open_minute == NOW + timedelta(minutes=5)
    ref = aggregate_closed(m1_for("431102", NOW, NOW + timedelta(minutes=5)), Timeframe.M5)[0]
    live = [c for c in app.observers["GOLD"].candles[Timeframe.M5] if c.open_time >= NOW]
    assert len(live) == 1 and (live[0].open, live[0].high, live[0].low, live[0].close, live[0].volume) == (ref.open, ref.high, ref.low, ref.close, ref.volume)
    assert app.health.symbols["GOLD"].state == "LIVE" and app.health.symbols["GOLD"].last_tick_at == injected[-1].ts
    assert not app.pending_runtimes


def test_rollover_failure_keeps_old_contract(tmp_path, monkeypatch):
    app, http, hist, feeds, clock = _app(tmp_path, monkeypatch, symbols="GOLD")
    app.startup()
    old_obs = app.observers["GOLD"]

    async def go():
        feed = _prepare_rollover(app, clock)
        await feed.subscribe(["428291"])
        app._build_pipelines()
        app.health.set_symbol_state("GOLD", "LIVE")
        app.resolver._today = lambda: datetime(2026, 10, 3).date()
        hist.fail_for = {"431102"}  # history for the new contract cannot be fetched
        await app.check_rollover()

    asyncio.run(go())
    assert app.contracts["GOLD"].security_id == "428291" and app.observers["GOLD"] is old_obs
    assert app.feed.subscribed == ["428291"] and "428291" in app.pipelines and "431102" not in app.pipelines
    assert app.repos.instruments.active("GOLD")["security_id"] == "428291"
    sh = app.health.symbols["GOLD"]
    assert sh.state == "LIVE" and "rollover" in sh.detail and "failed" in sh.detail
    assert any("FAILED" in s for s in app.sink.statuses)


def test_supervisor_restarts_failed_feed_and_degrades_discord(tmp_path, monkeypatch):
    monkeypatch.setenv("DISCORD_TOKEN", "d-token")
    monkeypatch.setenv("DISCORD_CHANNEL_ID", "1")
    app, http, hist, feeds, clock = _app(tmp_path, monkeypatch, symbols="GOLD", feed_kwargs={"minutes": 3, "ids": ("428291",)})
    app.task_backoff_scale = 0.01
    app.startup()
    attempts = {"n": 0}

    class CrashingFeed(FakeFeed):
        async def run(self, stop):
            attempts["n"] += 1
            if attempts["n"] == 1:
                raise RuntimeError("socket exploded")
            await super().run(stop)

    def feed_factory(cfg, on_tick, health):
        f = CrashingFeed(on_tick, clock, minutes=3, ids=("428291",))
        feeds.append(f)
        return f

    class DeadDiscord:
        def __init__(self):
            self.starts = 0

        async def start(self, token):
            self.starts += 1
            raise ConnectionError("discord gateway unavailable")

        def is_closed(self):
            return True

        async def close(self):
            pass

    dead = DeadDiscord()
    app._feed_factory = feed_factory
    app._sink_factory = lambda cfg, repos: (NullSink(), dead)
    asyncio.run(app.run())
    assert attempts["n"] == 2  # crashed once, restarted by the supervisor, then ran to completion
    assert ("feed", "RuntimeError") in app.task_failures
    assert any(name == "discord" for name, _ in app.task_failures)
    assert app.health.components["discord"].status in ("error", "restarting")
    assert not app.health.ok
    assert app.observers["GOLD"].closed_count > 0 and app.health.symbols["GOLD"].state == "LIVE"  # observation continued
    assert dead.starts >= 2  # retried
    assert "discord=error" in app.health.status_line() or "discord=restarting" in app.health.status_line()


def test_critical_task_repeated_failure_shuts_down_safely(tmp_path, monkeypatch):
    app, http, hist, feeds, clock = _app(tmp_path, monkeypatch, symbols="GOLD")
    app.task_backoff_scale = 0.01
    app.startup()

    class AlwaysCrashing(FakeFeed):
        async def run(self, stop):
            raise RuntimeError("permanent failure")

    app._feed_factory = lambda cfg, on_tick, health: AlwaysCrashing(on_tick, clock, ids=("428291",))
    asyncio.run(app.run())
    assert app.stop.is_set()
    assert app.health.components["observer"].status == "error" and app.health.components["feed"].status == "error"
    assert sum(1 for n, _ in app.task_failures if n == "feed") == 6  # initial + 5 restarts
