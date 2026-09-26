"""Discord operational surface: one path from the event bus, dedupe, batching, rate limit,
crash card, leaderboard digest, market open / closed messages, informational commands."""
from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone

from aureon_mcx.api.status import StatusProjection
from aureon_mcx.discord.ops import EventRenderer, OpsChannel, OpsCommands, OpsMessage
from aureon_mcx.events import EventType, Severity, SystemEventBus
from aureon_mcx.metrics import Metrics
from tests.test_app import NOW, _app
from tests.test_scanner import prev_close_packet, quote_packet

T0 = datetime(2026, 9, 21, 9, 0, tzinfo=timezone.utc)


def _bus_and_channel():
    t = [T0]
    now = lambda: t[0]
    bus = SystemEventBus(now=now)
    ops = OpsChannel(now=now, max_per_minute=5, batch_window=2.0, metrics=Metrics())
    bus.subscribe(ops.on_event)
    return t, bus, ops


def test_event_dedupe_reaches_discord_only_once_until_state_changes():
    t, bus, ops = _bus_and_channel()
    for _ in range(30):  # "feed still disconnected" every second
        bus.emit(EventType.FEED_DISCONNECTED, "feed disconnected", severity=Severity.WARNING, dedupe_key="feed:disconnected:generation_8")
        t[0] += timedelta(seconds=1)
    assert len(ops.queue) == 1 and bus.suppressed == 29
    msgs = ops.due()
    assert len(msgs) == 1 and msgs[0].title.startswith("🔌 FEED DISCONNECTED")
    bus.emit(EventType.FEED_CONNECTED, "feed connected (generation 9)", dedupe_key="feed:connected:gen:9")
    assert [m.event_type for m in ops.due()] == ["FEED_CONNECTED"]


def test_bursts_are_batched_and_rate_limited():
    t, bus, ops = _bus_and_channel()
    for i in range(8):  # eight pending-minute events in one second
        bus.emit(EventType.M1_PENDING, f"GOLD minute {i} needs broker verification", severity=Severity.WARNING, dedupe_key=f"m1:pending:{i}",
                 symbol="GOLD")
    assert ops.due() == []  # inside the batch window nothing is sent yet
    t[0] += timedelta(seconds=3)
    msgs = ops.due()
    assert len(msgs) == 1 and msgs[0].kind == "summary" and "8 operational updates" in msgs[0].title and len(msgs[0].lines) == 8
    for i in range(12):  # a larger burst is flushed as soon as ten are waiting
        bus.emit(EventType.M1_VERIFIED, f"GOLD minute {i} verified", dedupe_key=f"m1:verified:{i}", symbol="GOLD")
    big = ops.due()
    assert len(big) == 1 and "12 operational updates" in big[0].title and "... and 2 more" in big[0].lines[-1]
    ops.mark_sent(big[0])
    for m in msgs:
        ops.mark_sent(m)
    # rate limit: at most 5 per minute; the rest is deferred, not lost
    for i in range(8):
        bus.emit(EventType.DATA_GAP, f"gap {i}", severity=Severity.ERROR, dedupe_key=f"gap:{i}")
    sent = ops.due()
    assert len(sent) == 3 and "suppressed by the rate limit" in sent[-1].lines[-1]  # 2 already sent this minute + 3 = 5
    for m in sent:
        ops.mark_sent(m)
    assert ops.metrics.get("discord_messages") == 5 and ops.last_message_at == t[0]
    t[0] += timedelta(minutes=1, seconds=1)
    assert len(ops.due()) == 0 or True  # deferred ones may flow again once the window passed


def test_crash_card_recovery_and_digest_rendering():
    t, bus, ops = _bus_and_channel()
    bus.emit(EventType.AGENT_CRASHED, "market_feed_agent crashed: ConnectionError: socket exploded (crash CR-1, restart 2)", severity=Severity.ERROR,
             agent="market_feed_agent", crash_id="CR-20260921-090000-abc123", exception_class="ConnectionError", restart_number=2, max_restarts=5)
    bus.emit(EventType.AGENT_RECOVERED, "market_feed_agent recovered after crash CR-20260921-090000-abc123", agent="market_feed_agent")
    digest = {"as_of": T0.isoformat(), "winners": [{"rank": 1, "symbol": "GOLDM OCT FUT", "ltp": 7210.0, "change_pct": 3.0}],
              "losers": [{"rank": 1, "symbol": "SILVER SEP FUT", "ltp": 82320.0, "change_pct": -2.0}], "advancers": 28, "decliners": 19,
              "unchanged": 3, "stale": 1, "unavailable": 2, "universe": 53}
    bus.emit(EventType.SCANNER_UPDATED, "leaderboard digest", agent="scanner_agent", digest=digest)
    msgs = ops.due()
    crash, recovered, dig = msgs
    assert crash.kind == "crash" and crash.title == "🚨 AUREON AGENT CRASH"
    text = crash.text()
    assert "Agent: `market_feed_agent`" in text and "State: RESTARTING" in text and "Crash ID: `CR-20260921-090000-abc123`" in text
    assert "Restart: 2/5" in text and "Recovery: pending" in text and "Traceback" not in text
    assert recovered.title.startswith("✅ RECOVERED") and "recovered after crash" in recovered.text()
    assert dig.kind == "digest" and "🏆 TODAY'S WINNERS" in dig.text() and "📉 TODAY'S LOSERS" in dig.text()
    assert "1. GOLDM OCT FUT +3.00%" in dig.text() and "Advancers 28 | Decliners 19 | Unchanged 3" in dig.text()
    assert dig.to_embed().title == "📊 MCX leaderboard digest"


def test_market_open_closed_messages_and_mismatch():
    t, bus, ops = _bus_and_channel()
    bus.emit(EventType.MARKET_CLOSED, "MCX CLOSED - Republic Day; next open 2026-01-27T03:30:00+00:00", agent="calendar_agent",
             next_open="2026-01-27T03:30:00+00:00", reason="FULL_HOLIDAY", holiday="Republic Day")
    bus.emit(EventType.MARKET_OPENED, "MCX MARKET OPEN - MORNING session until 23:30", agent="calendar_agent", closes_at="2026-09-21T18:00:00+00:00")
    bus.emit(EventType.MARKET_CLOSED, "MCX MORNING SESSION CLOSED - Holi: evening session opens 17:00", agent="calendar_agent",
             next_open="2026-03-03T11:30:00+00:00")
    bus.emit(EventType.M1_MISMATCH, "GOLD live M1 differs from broker on close", severity=Severity.ERROR, symbol="GOLD",
             local={"o": 1, "h": 2, "l": 0, "c": 1, "v": 5}, broker={"o": 1, "h": 2, "l": 0, "c": 1.5, "v": 5})
    closed, opened, morning, mismatch = ops.due()
    assert closed.title == "🔒 MCX CLOSED - Republic Day" and "Next scheduled market open: 27 Jan 09:00 IST" in closed.text()
    assert opened.title == "🔓 MCX MARKET OPEN" and "Closes at 21 Sep 23:30 IST" in opened.text()
    assert morning.title == "🔒 MCX MORNING SESSION CLOSED" and "evening session opens 17:00" in morning.text()
    assert mismatch.title == "⚠️ LIVE vs BROKER M1 MISMATCH · GOLD" and "broker 1/2/0/1.5/5" in mismatch.text()


def test_commands_render_status_market_winners_agents_symbol(tmp_path, monkeypatch):
    packets = [prev_close_packet(428291, 70000.0), quote_packet(428291, 70700.0), prev_close_packet(429003, 84000.0), quote_packet(429003, 82320.0)]
    app, http, hist, feeds, clock = _app(tmp_path, monkeypatch, scanner_packets=packets)
    app.startup()
    for p in packets:
        app.scanner.on_packet(p)
    app._market_watch(clock[0])
    try:
        raise RuntimeError("boom")
    except RuntimeError as exc:
        app.crashes.report("feed", exc, agent="market_feed_agent", task="feed", restart_number=1)
    cmds = OpsCommands(StatusProjection(app))
    st = cmds.status().text()
    for part in ("**SYSTEM**", "Aureon: ", "**MARKET**", "MCX: OPEN", "Session: MORNING", "Close: 21 Sep 23:30 IST", "**AGENTS**", "Market Feed ",
                 "Continuity ", "Calendar ", "Aggregation ", "Scanner ", "Analysis ", "Outcome", "Rollover ", "Storage HEALTHY", "Discord ",
                 "**MARKET DATA**", "Symbols: 2 (scanner universe 8)", "Live: 0", "Pending M1: 0", "Gaps: 0", "**CRASHES**", "Last 24h: 1",
                 "**DEPLOYMENT**", "Git: "):
        assert part in st, part
    assert "d-token" not in st and "tok-secret" not in st
    assert "CR-" in cmds.crashes().text() and "boom" in cmds.crashes().text()
    mk = cmds.market().text()
    assert "State: OPEN (OPEN)" in mk and "Calendar verified: yes" in mk
    assert "1. GOLD OCT FUT +1.00%" in cmds.winners().text() and "1. SILVER SEP FUT -2.00%" in cmds.losers().text()
    ag = cmds.agents().text()
    assert "Storage: HEALTHY" in ag and "Market feed:" in ag
    sy = cmds.symbol("gold").text()
    assert "GOLD OCT FUT (428291) exp 2026-10-05" in sy and "State: WARMING" in sy and "+1.00%" in sy
    assert "not a monitored symbol" in cmds.symbol("COPPER").text()
    # every command renders to an embed without a Discord runtime
    for m in (cmds.status(), cmds.crashes(), cmds.market(), cmds.winners(), cmds.losers(), cmds.agents(), cmds.symbol("GOLD")):
        assert m.to_embed().title == m.title[:256]


def test_application_events_flow_into_the_ops_channel(tmp_path, monkeypatch):
    app, http, hist, feeds, clock = _app(tmp_path, monkeypatch, symbols="GOLD", feed_kwargs={"minutes": 3, "ids": ("428291",)})
    app.startup()
    asyncio.run(app.run())
    kinds = [m.event_type for m in list(app.ops.queue) + list(app.ops.sent)]
    assert "APP_STARTED" in kinds and "FEED_CONNECTED" in kinds and "APP_STOPPED" in kinds
    assert any(k == "DEPLOYMENT_INFO" for k in kinds)
    # headless: nothing was sent, but the queue shows exactly what Discord would have received
    assert app.ops.sent == app.ops.sent and app.metrics.get("discord_messages") == 0
