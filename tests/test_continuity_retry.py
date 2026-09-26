"""Application-level continuity: persistent retry until the broker publishes the minute (no
reconnect needed), the socket-stall watchdog, event-loop-only mutation, incident persistence
across restarts, and reconcile mode."""
from __future__ import annotations

import asyncio
import threading
from datetime import timedelta

from aureon_mcx.broker.dhan.errors import DhanApiError
from aureon_mcx.events import EventType
from aureon_mcx.market.aggregation import aggregate_closed
from aureon_mcx.market.candle import Candle
from aureon_mcx.market.candle_builder import REASON_SUSPECT, CandlePipeline, M1TrustState
from aureon_mcx.market.timeframe import Timeframe
from tests.test_app import NOW, FakeFeed, FakeHistorical, _app, m1_for, ticks_for


class DelayedHistorical(FakeHistorical):
    """Dhan has not published the latest minutes yet: the first `empty_calls` live-period M1
    requests return nothing (no error), later ones return the bars."""

    def __init__(self, clock, empty_calls=3):
        super().__init__(clock)
        self.empty_calls = empty_calls
        self.live_m1_calls = 0

    def fetch(self, symbol, security_id, seg, itype, expiry, timeframe, start, end):
        out = super().fetch(symbol, security_id, seg, itype, expiry, timeframe, start, end)
        if timeframe is Timeframe.M1 and start >= NOW:
            self.live_m1_calls += 1
            if self.live_m1_calls <= self.empty_calls:
                return []
        return out


def _mid_minute_run(app, clock, minutes=8, first=2, start_offset=timedelta(minutes=2, seconds=5)):
    """Process starts mid-bucket at NOW+2:05, the feed streams minutes `first`.. of the fake market."""
    clock[0] = NOW + start_offset

    def feed_factory(cfg, on_tick, health):
        f = FakeFeed(on_tick, clock, minutes=minutes, ids=("428291",))

        async def run(stop):
            f.connected = True
            f.on_status("connected", {"reconnects": 0})
            await asyncio.sleep(0.3)
            series = m1_for("428291", NOW, NOW + timedelta(minutes=minutes))
            for i in range(first, minutes):
                for t in ticks_for(series[i]):
                    clock[0] = t.ts
                    on_tick(t)
                await f.wait_settled()
                await asyncio.sleep(0.15)  # let the retry scheduler (flush loop) run between minutes
            clock[0] = NOW + timedelta(minutes=minutes, seconds=3)
            await asyncio.sleep(0.3)
            stop.set()

        f.run = run
        return f

    app._feed_factory = feed_factory
    asyncio.run(app.run())


def test_pending_minutes_retry_until_broker_publishes_without_reconnect(tmp_path, monkeypatch):
    app, http, hist, feeds, clock = _app(tmp_path, monkeypatch, symbols="GOLD", historical=lambda c: DelayedHistorical(c, empty_calls=3))
    app.startup()
    _mid_minute_run(app, clock)
    p = app.pipelines["428291"]
    assert app.health.symbols["GOLD"].state == "LIVE" and p.continuity_ok and all(g.resolved for g in p.gaps)
    assert app.health.symbols["GOLD"].reconnects == 0  # no reconnect was needed
    assert hist.live_m1_calls >= 4  # first three answers were empty, the scheduler kept asking
    tracker = app.continuity.incidents
    assert tracker.open_for("428291") == []
    resolved = [i for i in tracker.resolved if i.security_id == "428291" and i.state == "RESOLVED" and i.kind == "pending_minute"]
    assert resolved and max(i.attempt_count for i in resolved) >= 2
    assert all(i.resolution == "broker_m1" and i.resolved_at is not None for i in resolved)
    m5 = [c for c in app.observers["GOLD"].candles[Timeframe.M5] if c.open_time >= NOW]
    ref = aggregate_closed(m1_for("428291", NOW, NOW + timedelta(minutes=5)), Timeframe.M5)[0]
    assert (m5[0].open, m5[0].high, m5[0].low, m5[0].close, m5[0].volume) == (ref.open, ref.high, ref.low, ref.close, ref.volume)
    types = [e.type for e in app.events.history]
    assert EventType.M1_PENDING in types and EventType.M1_VERIFIED in types and EventType.M1_VERIFICATION_FAILED in types
    assert any(e.type is EventType.SYMBOL_STATE_CHANGED and "recovered" in e.message for e in app.events.history)


def test_feed_stall_makes_minute_suspect_and_broker_replaces_it(tmp_path, monkeypatch):
    app, http, hist, feeds, clock = _app(tmp_path, monkeypatch, symbols="GOLD", feed_stall_seconds=20.0)
    app.startup()

    def feed_factory(cfg, on_tick, health):
        f = FakeFeed(on_tick, clock, minutes=8, ids=("428291",))

        async def run(stop):
            f.connected = True
            f.on_status("connected", {"reconnects": 0})
            await asyncio.sleep(0.3)
            series = m1_for("428291", NOW, NOW + timedelta(minutes=8))
            for t in ticks_for(series[0]):  # minute 0 healthy
                clock[0] = t.ts
                on_tick(t)
            await asyncio.sleep(0.05)
            first = ticks_for(series[1])[0]  # minute 1: one tick at :01, then delivery freezes
            clock[0] = first.ts
            on_tick(first)
            clock[0] = NOW + timedelta(minutes=1, seconds=40)  # 39 s without any packet on a connected socket
            await asyncio.sleep(0.3)  # watchdog runs
            assert app._feed_stalled and app.pipelines["428291"].feed_stale
            for i in range(2, 8):  # delivery resumes at 14:32:01
                for t in ticks_for(series[i]):
                    clock[0] = t.ts
                    on_tick(t)
                await f.wait_settled()
                await asyncio.sleep(0.1)
            clock[0] = NOW + timedelta(minutes=8, seconds=3)
            await asyncio.sleep(0.3)
            stop.set()

        f.run = run
        return f

    app._feed_factory = feed_factory
    asyncio.run(app.run())
    p = app.pipelines["428291"]
    rec = p.minute_trust(NOW + timedelta(minutes=1))
    assert rec.continuity_lost and rec.reason == REASON_SUSPECT and rec.state is M1TrustState.VERIFIED
    assert p.metrics.get("suspect_m1_replaced") >= 1
    assert app.health.symbols["GOLD"].state == "LIVE" and p.continuity_ok and all(g.resolved for g in p.gaps)
    types = [e.type for e in app.events.history]
    assert EventType.FEED_STALLED in types and EventType.FEED_RECOVERED in types
    assert not app._feed_stalled and app.health.components["feed"].status == "connected"
    m5 = [c for c in app.observers["GOLD"].candles[Timeframe.M5] if c.open_time >= NOW]
    ref = aggregate_closed(m1_for("428291", NOW, NOW + timedelta(minutes=5)), Timeframe.M5)[0]
    assert (m5[0].open, m5[0].high, m5[0].low, m5[0].close, m5[0].volume) == (ref.open, ref.high, ref.low, ref.close, ref.volume)


def test_worker_threads_fetch_only_and_pipeline_mutates_on_loop(tmp_path, monkeypatch):
    fetch_threads: list[int] = []
    mutation_threads: list[int] = []

    class RecordingHistorical(DelayedHistorical):
        def fetch(self, *a, **k):
            fetch_threads.append(threading.get_ident())
            return super().fetch(*a, **k)

    real_verify, real_recover, real_add = CandlePipeline.verify_m1, CandlePipeline.recover_m1, CandlePipeline.add_tick

    def rec(fn):
        def wrapper(self, *a, **k):
            mutation_threads.append(threading.get_ident())
            return fn(self, *a, **k)
        return wrapper

    monkeypatch.setattr(CandlePipeline, "verify_m1", rec(real_verify))
    monkeypatch.setattr(CandlePipeline, "recover_m1", rec(real_recover))
    monkeypatch.setattr(CandlePipeline, "add_tick", rec(real_add))
    app, http, hist, feeds, clock = _app(tmp_path, monkeypatch, symbols="GOLD", historical=lambda c: RecordingHistorical(c, empty_calls=1))
    app.startup()
    loop_thread = threading.get_ident()  # asyncio.run drives the loop on this thread
    _mid_minute_run(app, clock)
    live_fetches = fetch_threads[len([1 for c in hist.calls if c[3] < NOW]):]  # live-period fetches (after warmup)
    assert live_fetches and all(t != loop_thread for t in live_fetches), "broker fetches must run in worker threads"
    assert mutation_threads and all(t == loop_thread for t in mutation_threads), "pipeline mutations must stay on the event loop"
    assert app.task_failures == [] and app.health.symbols["GOLD"].state == "LIVE"


def test_unresolved_incidents_survive_restart(tmp_path, monkeypatch):
    app, http, hist, feeds, clock = _app(tmp_path, monkeypatch, symbols="GOLD", feed_kwargs={"minutes": 8, "ids": ("428291",), "outage": (2, 5)})
    app.startup()
    hist.fail_for = {"428291"}
    asyncio.run(app.run())
    open_before = app.continuity.incidents.open_for("428291")
    assert open_before and app.health.symbols["GOLD"].state in ("DEGRADED", "ERROR")
    minutes = {i.open_time for i in open_before if i.kind == "pending_minute"}
    # a fresh process over the same database restores them and keeps them pending (never forgotten)
    app2, _, _, _, clock2 = _app(tmp_path, monkeypatch, symbols="GOLD")
    app2.startup()
    restored = app2.continuity.incidents.open_for("428291")
    assert {i.open_time for i in restored if i.kind == "pending_minute"} == minutes
    assert all(i.attempt_count >= 1 for i in restored if i.kind == "pending_minute")
    p2 = app2.pipelines["428291"]
    assert set(p2.pending_minutes()) == minutes and not p2.continuity_ok


def test_reconcile_mode_verifies_live_m1_and_reports_mismatch(tmp_path, monkeypatch):
    class DivergingHistorical(FakeHistorical):
        def fetch(self, symbol, security_id, seg, itype, expiry, timeframe, start, end):
            out = super().fetch(symbol, security_id, seg, itype, expiry, timeframe, start, end)
            if timeframe is Timeframe.M1 and start >= NOW:
                target = NOW + timedelta(minutes=3)
                out = [Candle(**{**c.__dict__, "close": c.close + 7.0, "high": max(c.high, c.close + 7.0), "id": None}) if c.open_time == target else c
                       for c in out]
            return out

    app, http, hist, feeds, clock = _app(tmp_path, monkeypatch, symbols="GOLD", historical=DivergingHistorical,
                                         feed_kwargs={"minutes": 8, "ids": ("428291",)},
                                         analysis_updates={"historical": {"reconcile_every_live_m1": True, "reconcile_delay_seconds": 0}})
    app.startup()
    asyncio.run(app.run())
    p = app.pipelines["428291"]
    assert p.reconcile_live_m1
    assert p.metrics.get("live_m1_verified") >= 4 and p.metrics.get("live_m1_mismatches") == 1
    assert p.minute_trust(NOW + timedelta(minutes=3)).state is M1TrustState.REJECTED
    mism = [e for e in app.events.history if e.type is EventType.M1_MISMATCH]
    assert len(mism) == 1 and "close" in mism[0].payload["fields"] and mism[0].symbol == "GOLD"
    assert app.health.symbols["GOLD"].state == "LIVE"
