"""Agent registry state model, crash reporter durability / redaction / resolution, event bus dedupe."""
from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone

from aureon_mcx.agents import AGENT_DEFINITIONS, AgentRegistry
from aureon_mcx.crash import CrashReporter
from aureon_mcx.events import EventType, Severity, SystemEventBus
from aureon_mcx.logging_setup import RedactingFilter
from tests.test_app import NOW, FakeFeed, _app

T0 = datetime(2026, 9, 21, 9, 0, tzinfo=timezone.utc)


def clock():
    t = [T0]
    return t, (lambda: t[0])


# ------------------------------------------------------------------ event bus
def test_event_bus_dedupes_repeats_until_state_or_severity_changes():
    t, now = clock()
    bus = SystemEventBus(now=now, reminder_interval=timedelta(minutes=15))
    seen = []
    bus.subscribe(seen.append)
    assert bus.emit(EventType.FEED_DISCONNECTED, "feed down", severity=Severity.WARNING, dedupe_key="feed:disconnected:gen:8")
    for _ in range(50):  # "feed still disconnected" every second is suppressed
        t[0] += timedelta(seconds=1)
        assert not bus.emit(EventType.FEED_DISCONNECTED, "feed down", severity=Severity.WARNING, dedupe_key="feed:disconnected:gen:8")
    assert bus.suppressed == 50 and len(seen) == 1
    assert bus.emit(EventType.FEED_DISCONNECTED, "feed down", severity=Severity.ERROR, dedupe_key="feed:disconnected:gen:8")  # severity changed
    t[0] += timedelta(minutes=16)
    assert bus.emit(EventType.FEED_DISCONNECTED, "feed down", severity=Severity.ERROR, dedupe_key="feed:disconnected:gen:8")  # reminder interval
    assert bus.emit(EventType.FEED_DISCONNECTED, "feed down", severity=Severity.ERROR, dedupe_key="feed:disconnected:gen:9")  # new generation
    assert bus.emit(EventType.M1_VERIFIED, "no key: always published") and bus.emit(EventType.M1_VERIFIED, "no key: always published")
    assert [e.type for e in bus.recent(2)] == [EventType.M1_VERIFIED, EventType.M1_VERIFIED]
    bus.clear_key("feed:disconnected:gen:9")
    assert bus.emit(EventType.FEED_DISCONNECTED, "feed down", severity=Severity.ERROR, dedupe_key="feed:disconnected:gen:9")

    def broken(_e):
        raise RuntimeError("consumer bug")

    bus.subscribe(broken)
    assert bus.emit(EventType.APP_STARTED, "still delivered to the others")  # one broken consumer never blocks the rest
    assert seen[-1].type is EventType.APP_STARTED


# ---------------------------------------------------------------- agent registry
def test_agent_registry_states_heartbeats_and_events():
    t, now = clock()
    bus = SystemEventBus(now=now)
    reg = AgentRegistry(bus, now=now)
    assert set(reg.agents) == set(AGENT_DEFINITIONS) and all(a.state == "STOPPED" for a in reg.agents.values())
    reg.register("market_feed_agent", "Market feed", "Dhan WebSocket", heartbeat_timeout=30)
    a = reg.start("market_feed_agent", subscriptions=2)
    assert a.state == "HEALTHY" and a.started_at == T0 and a.details["subscriptions"] == 2
    reg.heartbeat("market_feed_agent", work=3, queue_depth=1)
    assert a.work_count == 3 and a.last_success == T0 and a.queue_depth == 1
    t[0] += timedelta(seconds=45)
    assert reg.check_heartbeats() == ["market_feed_agent"] and a.state == "STALE"
    reg.heartbeat("market_feed_agent")
    assert a.state == "HEALTHY" and a.last_heartbeat == t[0]
    reg.error("market_feed_agent", "socket exploded", state="DEGRADED")
    assert a.state == "DEGRADED" and a.last_error == "socket exploded" and a.last_error_at == t[0]
    reg.restarting("market_feed_agent", 2, "restart 2/5")
    assert a.state == "RESTARTING" and a.restart_count == 2
    reg.set_state("market_feed_agent", "HEALTHY", "recovered")
    reg.set_state("discord_agent", "FAILED", "gave up")
    assert reg.overall() == "FAILED"
    reg.set_state("discord_agent", "STOPPED")
    reg.set_state("continuity_agent", "RECOVERING", "verifying")
    assert reg.overall() == "DEGRADED"
    types = [e.type for e in bus.history]
    assert EventType.AGENT_RECOVERED in types and EventType.AGENT_FAILED in types and EventType.AGENT_STATE_CHANGED in types
    snap = {s["agent_id"]: s for s in reg.snapshot()}
    assert snap["market_feed_agent"]["restart_count"] == 2 and snap["market_feed_agent"]["role"] == "Dhan WebSocket"
    assert set(snap["market_feed_agent"]) >= {"agent_id", "name", "state", "started_at", "last_heartbeat", "last_success", "last_error",
                                             "last_error_at", "restart_count", "work_count", "queue_depth", "details", "version"}


# ---------------------------------------------------------------- crash reporter
def test_crash_reporter_persists_redacts_and_resolves(repos, monkeypatch):
    monkeypatch.setenv("AUREON_GIT_SHA", "abc1234def")
    from aureon_mcx import version

    version.git_sha.cache_clear()
    t, now = clock()
    bus = SystemEventBus(now=now)
    RedactingFilter.register(["tok-secret-XYZ"])
    try:
        rep_ = CrashReporter(repos, bus, now=now, started_at=T0 - timedelta(seconds=90))
        try:
            raise ConnectionError("gateway refused access_token=tok-secret-XYZ for client")
        except ConnectionError as exc:
            rep = rep_.report("feed", exc, agent="market_feed_agent", task="feed", symbol="GOLD", security_id="428291", restart_number=2)
        assert rep.crash_id.startswith("CR-") and rep.exception_class == "ConnectionError"
        assert "tok-secret-XYZ" not in rep.message and "tok-secret-XYZ" not in rep.stack_trace and "[REDACTED]" in rep.message
        assert rep.git_sha == "abc1234def" and rep.process_uptime_s == 90.0 and rep.restart_number == 2
        row = repos.crashes.recent(1)[0]
        assert row["crash_id"] == rep.crash_id and row["recovery_result"] == "pending" and row["agent"] == "market_feed_agent"
        assert "tok-secret-XYZ" not in row["stack_trace"] and "tok-secret-XYZ" not in row["message"]
        assert row["symbol"] == "GOLD" and row["security_id"] == "428291" and row["app_version"] and row["component"] == "feed"
        assert repos.crashes.unresolved_count() == 1
        crashed = [e for e in bus.history if e.type is EventType.AGENT_CRASHED]
        assert len(crashed) == 1 and crashed[0].payload["crash_id"] == rep.crash_id and "tok-secret" not in crashed[0].message
        t[0] += timedelta(seconds=5)
        rep_.resolve(rep.crash_id, "success")
        row = repos.crashes.recent(1)[0]
        assert row["recovery_result"] == "success" and row["resolved_at"] is not None and repos.crashes.unresolved_count() == 0
        assert [e.type for e in bus.history][-1] is EventType.AGENT_RECOVERED
        assert rep_.snapshot()["unresolved"] == 0 and rep_.snapshot()["last_crash"]["crash_id"] == rep.crash_id
        assert rep_.snapshot()["last_crash"]["stack_trace"] is None  # summaries never carry the trace
        assert repos.crashes.recent(5, agent="market_feed_agent", resolved=True)
    finally:
        RedactingFilter.clear()
        version.git_sha.cache_clear()


# ------------------------------------------------------------- application level
def test_supervised_crash_creates_report_restarts_and_resolves(tmp_path, monkeypatch):
    monkeypatch.setenv("DISCORD_TOKEN", "d-token-secret")
    monkeypatch.setenv("DISCORD_CHANNEL_ID", "1")
    app, http, hist, feeds, clock_ = _app(tmp_path, monkeypatch, symbols="GOLD", feed_kwargs={"minutes": 4, "ids": ("428291",)})
    app.task_backoff_scale = 0.01
    app.startup()
    attempts = {"n": 0}

    class CrashingFeed(FakeFeed):
        async def run(self, stop):
            attempts["n"] += 1
            if attempts["n"] == 1:
                raise RuntimeError("socket exploded with d-token-secret inside")
            await super().run(stop)

    def feed_factory(cfg, on_tick, health):
        f = CrashingFeed(on_tick, clock_, minutes=4, ids=("428291",))
        feeds.append(f)
        return f

    class DeadDiscord:
        async def start(self, token):
            raise ConnectionError("discord gateway unavailable")

        def is_closed(self):
            return True

        async def close(self):
            pass

    from aureon_mcx.observer import NullSink

    app._feed_factory = feed_factory
    app._sink_factory = lambda cfg, repos: (NullSink(), DeadDiscord())
    asyncio.run(app.run())
    assert attempts["n"] == 2
    reports = app.crashes.recent
    feed_reports = [r for r in reports if r.component == "feed"]
    assert len(feed_reports) == 1 and feed_reports[0].agent == "market_feed_agent" and feed_reports[0].restart_number == 1
    assert feed_reports[0].recovery_result == "success" and feed_reports[0].resolved_at is not None  # restarted feed ran cleanly
    assert "d-token-secret" not in feed_reports[0].message and "d-token-secret" not in feed_reports[0].stack_trace
    discord_reports = [r for r in reports if r.component == "discord"]
    assert discord_reports and all(r.agent == "discord_agent" for r in discord_reports)
    assert app.agents.get("market_feed_agent").state in ("HEALTHY", "STOPPED") and app.agents.get("market_feed_agent").restart_count == 1
    assert app.agents.get("discord_agent").restart_count >= 1
    assert app.health.symbols["GOLD"].state == "LIVE"  # observation continued through the crash
    from aureon_mcx.storage import Database, Repositories

    db = Database(tmp_path / "app.db")
    repos = Repositories(db)
    rows = repos.crashes.recent(50)
    assert any(r["component"] == "feed" and r["recovery_result"] == "success" for r in rows)
    assert all("d-token-secret" not in (r["message"] + r["stack_trace"]) for r in rows)
    events = repos.events.recent(500)
    types = {r["event_type"] for r in events}
    assert {"APP_STARTED", "AGENT_CRASHED", "AGENT_RECOVERED", "FEED_CONNECTED", "APP_STOPPED"} <= types
    db.close()
    assert app.metrics.get("crashes") >= 2 and app.metrics.get("restarts") >= 2


def test_background_operation_crash_is_reported_not_swallowed(tmp_path, monkeypatch):
    from tests.test_continuity_retry import _mid_minute_run

    app, http, hist, feeds, clock_ = _app(tmp_path, monkeypatch, symbols="GOLD")
    app.startup()

    def exploding(*a, **k):
        raise ValueError("historical parser bug")

    app.continuity.fetch_m1 = exploding  # every broker fetch explodes (a bug, not a DhanError)
    _mid_minute_run(app, clock_, minutes=6)  # starts mid-minute: recovery + verification must run
    reps = [r for r in app.crashes.recent if r.component == "continuity"]
    assert reps and reps[0].exception_class == "ValueError" and reps[0].symbol == "GOLD" and reps[0].security_id == "428291"
    assert reps[0].task.startswith(("recover:", "verify:"))
    assert app.agents.get("continuity_agent").last_error and "historical parser bug" in app.agents.get("continuity_agent").last_error
    assert app.health.symbols["GOLD"].state != "LIVE"  # never LIVE after exploding recovery: fail closed
    assert app.task_failures == [] or all(n.startswith(("verify", "repair", "recover")) for n, _ in app.task_failures)
    assert app.health.components["observer"].status == "live"  # the process stayed alive; only the operation was isolated
    from aureon_mcx.storage import Database, Repositories

    db = Database(tmp_path / "app.db")
    rows = Repositories(db).crashes.recent(10)
    db.close()
    assert rows and rows[0]["component"] == "continuity" and rows[0]["exception_class"] == "ValueError"


def test_market_watch_emits_open_close_session_events(tmp_path, monkeypatch):
    from aureon_mcx.market.timeutil import IST

    app, http, hist, feeds, clock_ = _app(tmp_path, monkeypatch, symbols="GOLD")
    app.startup()

    def at(h, m):
        return datetime(2026, 9, 21, h, m, tzinfo=IST)

    app._market_watch(at(8, 50))                    # first observation: closed, no transition event
    app._market_watch(at(9, 0))                     # MARKET OPEN
    app._market_watch(at(17, 0))                    # EVENING session
    app._market_watch(at(23, 21))                   # closing soon
    app._market_watch(at(23, 30))                   # EVENING SESSION CLOSED
    app._market_watch(datetime(2026, 9, 26, 12, 0, tzinfo=IST))   # weekend
    app._market_watch(datetime(2026, 10, 2, 12, 0, tzinfo=IST))   # Gandhi Jayanti
    app._market_watch(datetime(2026, 10, 20, 12, 0, tzinfo=IST))  # Dassera morning closed
    app._market_watch(datetime(2026, 10, 20, 17, 0, tzinfo=IST))  # evening open
    msgs = [(e.type, e.message) for e in app.events.history if e.agent == "calendar_agent"]
    text = "\n".join(m for _, m in msgs)
    assert "MCX MARKET OPEN" in text and "MCX EVENING session" in text and "MCX EVENING SESSION CLOSED" in text
    assert "MCX closes at 23:30 IST" in text and "weekend" in text and "Mahatma Gandhi Jayanti" in text
    assert "MCX MORNING SESSION CLOSED" in text and "MCX EVENING SESSION OPEN" in text
    assert app.agents.get("calendar_agent").details["reason"] == "OPEN"
