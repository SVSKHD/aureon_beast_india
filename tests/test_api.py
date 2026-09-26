"""Read-only status API: every endpoint answers, no secret ever leaks, the server runs as a
supervised task beside the observer (default port 1250) and never blocks market observation."""
from __future__ import annotations

import asyncio
import json
import socket
from datetime import timedelta

import httpx
import pytest
from fastapi.testclient import TestClient

from aureon_mcx.api import ApiServer, create_api
from aureon_mcx.config.env import EnvSettings
from tests.test_app import NOW, FakeFeed, _app, m1_for, ticks_for
from tests.test_scanner import prev_close_packet, quote_packet

SECRETS = ("tok-secret-1", "cid-1", "d-token-secret")

ENDPOINTS = ["/api/v1/health", "/api/v1/status", "/api/v1/agents", "/api/v1/agents/market_feed_agent", "/api/v1/symbols", "/api/v1/symbols/GOLD",
             "/api/v1/market", "/api/v1/market/winners", "/api/v1/market/losers", "/api/v1/market/breadth", "/api/v1/market/instruments",
             "/api/v1/crashes", "/api/v1/events", "/api/v1/gaps", "/api/v1/calendar", "/api/v1/metrics", "/api/v1/continuity"]


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


@pytest.fixture
def started_app(tmp_path, monkeypatch):
    monkeypatch.setenv("DISCORD_TOKEN", "d-token-secret")
    packets = [prev_close_packet(428291, 70000.0), quote_packet(428291, 70700.0), prev_close_packet(429003, 84000.0), quote_packet(429003, 82320.0)]
    app, http, hist, feeds, clock = _app(tmp_path, monkeypatch, scanner_packets=packets)
    app.startup()
    for p in packets:
        app.scanner.on_packet(p)
    app._market_watch(clock[0])
    try:
        raise RuntimeError("boom with tok-secret-1 inside")
    except RuntimeError as exc:
        app.crashes.report("feed", exc, agent="market_feed_agent", task="feed", restart_number=1)
    return app


def test_every_endpoint_answers_and_never_leaks_secrets(started_app):
    client = TestClient(create_api(started_app))
    for path in ENDPOINTS:
        r = client.get(path)
        assert r.status_code == 200, path
        body = r.text
        for secret in SECRETS:
            assert secret not in body, f"{secret} leaked via {path}"
        assert "[REDACTED]" in body or "tok-secret" not in body
    assert client.get("/api/v1/agents/nope").status_code == 404 and client.get("/api/v1/symbols/NOPE").status_code == 404
    assert client.get("/api/v1/events?type=bogus").status_code == 400
    assert started_app.metrics.get("api_requests") >= len(ENDPOINTS)


def test_status_report_shape(started_app):
    client = TestClient(create_api(started_app))
    s = client.get("/api/v1/status").json()
    assert set(s) >= {"status", "version", "market", "agents", "symbols", "scanner", "continuity", "crashes", "discord", "as_of"}
    assert s["status"] in ("HEALTHY", "DEGRADED", "ERROR", "FAILED")
    assert set(s["version"]) >= {"app", "git_sha", "started_at", "uptime_seconds"} and s["version"]["app"]
    m = s["market"]
    assert m["exchange"] == "MCX" and m["state"] == "OPEN" and m["session"] == "MORNING" and m["trading_date"] == "2026-09-21"
    assert m["calendar_verified"] is True and m["closes_at"] and m["opens_at"]
    ids = {a["agent_id"] for a in s["agents"]}
    assert {"market_feed_agent", "continuity_agent", "calendar_agent", "aggregation_agent", "scanner_agent", "analysis_agent", "setup_agent",
            "outcome_agent", "rollover_agent", "storage_agent", "discord_agent", "api_agent", "health_agent"} <= ids
    sym = {x["symbol"]: x for x in s["symbols"]}
    g = sym["GOLD"]
    assert g["security_id"] == "428291" and g["display_symbol"] and g["expiry"] == "2026-10-05" and g["state"] == "WARMING"
    assert set(g) >= {"last_price", "previous_close", "change", "change_pct", "last_tick_at", "lag_seconds", "pending_verification",
                      "unresolved_gaps", "last_closed", "analysis_enabled", "feed_connected"}
    assert g["previous_close"] == 70000.0 and g["change_pct"] == pytest.approx(1.0)
    sc = s["scanner"]
    assert sc["universe"] == 8 and sc["advancers"] == 1 and sc["decliners"] == 1 and sc["unchanged"] == 0
    assert sc["winners"][0]["symbol"] == "GOLD" and sc["losers"][0]["symbol"] == "SILVER"
    assert s["continuity"] == {"pending_minutes": 0, "unresolved_gaps": 0, "open_incidents": 0, "feed_stalled": False}
    assert s["crashes"]["unresolved"] == 1 and s["crashes"]["last_crash"]["crash_id"].startswith("CR-") and s["crashes"]["last_24h"] == 1
    assert s["crashes"]["last_crash"]["stack_trace"] is None  # summaries never carry traces
    assert s["discord"]["state"] in ("STOPPED", "HEALTHY") and "last_message_at" in s["discord"]


def test_operational_endpoints(started_app):
    client = TestClient(create_api(started_app))
    h = client.get("/api/v1/health").json()
    assert h["alive"] is True and h["ready"] is False and h["symbols"] == 2  # still WARMING: not trustworthy yet
    a = client.get("/api/v1/agents").json()
    assert a["overall"] in ("HEALTHY", "DEGRADED", "ERROR", "STOPPED") and len(a["agents"]) >= 13
    one = client.get("/api/v1/agents/storage_agent").json()
    assert one["state"] == "HEALTHY" and one["details"]["schema_version"] >= 5
    w = client.get("/api/v1/market/winners?limit=1").json()
    assert w["winners"][0]["rank"] == 1 and w["winners"][0]["security_id"] == "428291" and len(w["winners"]) == 1
    l = client.get("/api/v1/market/losers").json()
    assert l["losers"][0]["change_pct"] == pytest.approx(-2.0) and l["losers"][0]["stale"] is False
    b = client.get("/api/v1/market/breadth").json()
    assert b["overall"]["total"] == 8 and b["overall"]["unavailable"] == 6 and b["by_segment"]["MCX_COMM"]["advancing"] == 1
    assert b["overall"]["average_pct_move"] == pytest.approx(-0.5) and b["overall"]["median_pct_move"] == pytest.approx(-0.5)
    inst = client.get("/api/v1/market/instruments?segment=MCX_COMM").json()
    assert inst["count"] == 8 and {i["symbol"] for i in inst["instruments"]} >= {"GOLD", "SILVER", "GOLDM"}
    c = client.get("/api/v1/crashes?agent=market_feed_agent&resolved=false").json()
    assert c["unresolved"] == 1 and c["crashes"][0]["exception_class"] == "RuntimeError" and c["crashes"][0]["stack_trace"] is None
    c2 = client.get("/api/v1/crashes?trace=true").json()
    assert "Traceback" in c2["crashes"][0]["stack_trace"] and "tok-secret-1" not in c2["crashes"][0]["stack_trace"]
    assert client.get("/api/v1/crashes?resolved=true").json()["crashes"] == []
    e = client.get("/api/v1/events?limit=5").json()
    assert e["count"] >= 1 and {"type", "severity", "message", "created_at"} <= set(e["events"][0])
    assert client.get("/api/v1/events?type=AGENT_CRASHED").json()["events"][0]["type"] == "AGENT_CRASHED"
    g = client.get("/api/v1/gaps").json()
    assert g["unresolved"] == 0 and g["gaps"] == [] and g["incidents"]["open"] == []
    cal = client.get("/api/v1/calendar").json()
    assert cal["years"] == [2026] and cal["verified_against_official_circular"] is True and cal["today"]["date"] == "2026-09-21"
    assert len(cal["calendars"]["2026"]["holidays"]) == 16 and cal["calendars"]["2026"]["pending_special_sessions"][0]["date"] == "2026-11-08"
    m = client.get("/api/v1/metrics").json()
    assert {"counters", "gauges", "timestamps"} <= set(m)
    assert {"websocket_reconnects", "packets_received", "live_m1_built", "live_m1_verified", "live_m1_mismatches", "pending_m1",
            "aggregate_gaps", "gaps_repaired", "observer_candles_processed", "discord_messages", "discord_failures", "api_requests",
            "crash_count", "restart_count"} <= set(m["counters"]) | set(m["gauges"])
    assert m["gauges"]["setup_counts_by_state"] == {} or isinstance(m["gauges"]["setup_counts_by_state"], dict)
    cont = client.get("/api/v1/continuity").json()
    assert cont["pending_minutes"] == 0 and cont["incidents"] == []
    assert client.get("/api/v1/symbols/gold").json()["symbol"] == "GOLD"


def test_default_port_is_1250_and_env_overrides(monkeypatch):
    env = EnvSettings(_env_file=None)
    assert env.AUREON_API_PORT == 1250 and env.AUREON_API_HOST == "0.0.0.0" and env.AUREON_API_ENABLED is True
    monkeypatch.setenv("AUREON_API_PORT", "1899")
    monkeypatch.setenv("AUREON_API_HOST", "127.0.0.1")
    env2 = EnvSettings(_env_file=None)
    assert env2.AUREON_API_PORT == 1899 and env2.AUREON_API_HOST == "127.0.0.1"


def test_server_runs_under_the_observer_loop_and_serves_health(tmp_path, monkeypatch):
    port = _free_port()
    monkeypatch.setenv("AUREON_API_PORT", str(port))
    app, http, hist, feeds, clock = _app(tmp_path, monkeypatch, symbols="GOLD")
    monkeypatch.setenv("AUREON_API_ENABLED", "true")
    monkeypatch.setenv("AUREON_API_HOST", "127.0.0.1")
    app.startup()
    assert app.cfg.env.AUREON_API_PORT == port
    seen: dict = {}

    def feed_factory(cfg, on_tick, health):
        f = FakeFeed(on_tick, clock, minutes=4, ids=("428291",))

        async def run(stop):
            f.connected = True
            f.on_status("connected", {"reconnects": 0})
            await asyncio.sleep(0.3)
            series = m1_for("428291", NOW, NOW + timedelta(minutes=4))
            for i in range(4):
                for t in ticks_for(series[i]):
                    clock[0] = t.ts
                    on_tick(t)
                await f.wait_settled()
            async with httpx.AsyncClient() as client:  # the observer loop keeps running while we query the API
                for _ in range(100):
                    try:
                        r = await client.get(f"http://127.0.0.1:{port}/api/v1/health", timeout=2.0)
                        if r.status_code == 200:
                            seen["health"] = r.json()
                            seen["status"] = (await client.get(f"http://127.0.0.1:{port}/api/v1/status", timeout=2.0)).json()
                            break
                    except httpx.HTTPError:
                        pass
                    await asyncio.sleep(0.05)
            clock[0] = NOW + timedelta(minutes=4, seconds=3)
            await asyncio.sleep(0.1)
            stop.set()

        f.run = run
        return f

    app._feed_factory = feed_factory
    asyncio.run(app.run())
    assert seen["health"]["alive"] is True and seen["health"]["ready"] is True and seen["health"]["status"] == "HEALTHY"
    assert seen["status"]["symbols"][0]["state"] == "LIVE" and seen["status"]["symbols"][0]["analysis_enabled"] is True
    assert app.agents.get("api_agent").details["port"] == port and app.agents.get("api_agent").restart_count == 0
    assert any(e.type.value == "API_STARTED" for e in app.events.history)
    assert app.health.symbols["GOLD"].state == "LIVE" and app.task_failures == []
    body = json.dumps(seen)
    assert all(secret not in body for secret in SECRETS)


def test_api_bind_failure_degrades_api_agent_not_the_observer(tmp_path, monkeypatch):
    port = _free_port()
    blocker = socket.socket()
    blocker.bind(("127.0.0.1", port))
    blocker.listen(1)
    monkeypatch.setenv("AUREON_API_PORT", str(port))
    app, http, hist, feeds, clock = _app(tmp_path, monkeypatch, symbols="GOLD", feed_kwargs={"minutes": 8, "ids": ("428291",)})
    monkeypatch.setenv("AUREON_API_ENABLED", "true")
    monkeypatch.setenv("AUREON_API_HOST", "127.0.0.1")
    app.task_backoff_scale = 0.05
    app.startup()
    try:
        asyncio.run(app.run())
    finally:
        blocker.close()
    assert app.health.symbols["GOLD"].state == "LIVE"  # observation unaffected
    assert any(n == "api" for n, _ in app.task_failures) and app.agents.get("api_agent").restart_count >= 1
    assert any(r.component == "api" for r in app.crashes.recent)
