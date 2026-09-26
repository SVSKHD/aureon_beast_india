"""Discord as the operational monitoring surface.

Every meaningful STATE CHANGE / SYSTEM EVENT reaches Discord through ONE path:

    SystemEventBus -> EventRenderer -> OpsChannel (queue, rate limit, batching) -> Discord client

Nothing here computes market data; it renders events, digests and status cards. Ticks never
reach Discord. Deduplication happens on the bus (stable dedupe keys); the channel additionally
batches bursts into one embed and caps the outbound rate so Discord's limits are respected.
Informational commands (/status, /crashes, /market, /winners, /losers, /agents, /symbol)
render the same projections the HTTP API serves. No command places, modifies or cancels orders.
"""
from __future__ import annotations

import logging
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import TYPE_CHECKING, Any, Callable

from aureon_mcx.events import EventType, Severity, SystemEvent
from aureon_mcx.market.timeutil import IST, ensure_utc, fmt_ist, utc_now

if TYPE_CHECKING:  # pragma: no cover
    from aureon_mcx.api.status import StatusProjection

log = logging.getLogger("aureon.discord.ops")

COLOR = {"info": 0x3498DB, "ok": 0x2ECC71, "warn": 0xF1C40F, "error": 0xE74C3C, "critical": 0x8E44AD, "neutral": 0x95A5A6}

EMOJI = {
    EventType.APP_STARTED: "🟢", EventType.APP_STOPPED: "⏹️", EventType.DEPLOYMENT_INFO: "🚀", EventType.API_STARTED: "🌐",
    EventType.DATABASE_MIGRATED: "🗄️", EventType.CALENDAR_FAILURE: "📅",
    EventType.AGENT_STATE_CHANGED: "⚙️", EventType.AGENT_CRASHED: "🚨", EventType.AGENT_RECOVERED: "✅", EventType.AGENT_FAILED: "💀",
    EventType.FEED_CONNECTED: "🔌", EventType.FEED_DISCONNECTED: "🔌", EventType.FEED_RECONNECTING: "🔁", EventType.FEED_STALLED: "🧊",
    EventType.FEED_RECOVERED: "✅", EventType.FEED_SUBSCRIPTION_PROBLEM: "⚠️",
    EventType.MARKET_OPENED: "🔓", EventType.MARKET_CLOSED: "🔒", EventType.MARKET_SESSION_CHANGED: "🕔", EventType.MARKET_CLOSING_SOON: "⏰",
    EventType.M1_PENDING: "⏳", EventType.M1_VERIFICATION_STARTED: "🔎", EventType.M1_VERIFIED: "✔️", EventType.M1_VERIFICATION_FAILED: "❗",
    EventType.M1_MISMATCH: "⚠️", EventType.M1_ABANDONED: "🗂️", EventType.DATA_GAP: "❗", EventType.DATA_GAP_REPAIRED: "🩹",
    EventType.SYMBOL_STATE_CHANGED: "📶", EventType.ROLLOVER_CANDIDATE: "🔁", EventType.ROLLOVER_STARTED: "🔁", EventType.ROLLOVER_SWITCHED: "🔁",
    EventType.ROLLOVER_COMPLETED: "✅", EventType.ROLLOVER_FAILED: "❌", EventType.SCANNER_UPDATED: "🏆", EventType.SCANNER_LEADER_CHANGED: "🥇",
}

# Events that are operationally meaningful but too chatty for one message each: they are
# batched into a single embed per flush. Everything else is sent as its own message.
BATCHED = {EventType.M1_PENDING, EventType.M1_VERIFICATION_STARTED, EventType.M1_VERIFIED, EventType.M1_VERIFICATION_FAILED,
           EventType.SYMBOL_STATE_CHANGED, EventType.AGENT_STATE_CHANGED, EventType.SCANNER_LEADER_CHANGED, EventType.FEED_RECONNECTING}


@dataclass
class OpsMessage:
    kind: str                      # event | crash | digest | status | summary
    title: str
    lines: list[str] = field(default_factory=list)
    color: int = COLOR["info"]
    created_at: datetime = field(default_factory=utc_now)
    event_type: str | None = None
    dedupe_key: str | None = None
    batchable: bool = False

    def text(self) -> str:
        return "\n".join([self.title] + self.lines)

    def to_embed(self):
        import discord  # local import keeps the renderer testable without the Discord runtime

        embed = discord.Embed(title=self.title[:256], description="\n".join(self.lines)[:4096], color=self.color)
        embed.set_footer(text=f"Aureon · observation only · {fmt_ist(self.created_at, '%d %b %H:%M:%S IST')}")
        return embed


def _sev_color(sev: Severity) -> int:
    return {Severity.INFO: COLOR["info"], Severity.WARNING: COLOR["warn"], Severity.ERROR: COLOR["error"], Severity.CRITICAL: COLOR["critical"]}[sev]


def _t(value: str | None) -> str:
    if not value:
        return "n/a"
    try:
        return fmt_ist(datetime.fromisoformat(value), "%d %b %H:%M IST")
    except ValueError:
        return value


class EventRenderer:
    """SystemEvent -> OpsMessage. Returns None for events Discord should not see."""

    def render(self, e: SystemEvent) -> OpsMessage | None:
        emoji = EMOJI.get(e.type, "•")
        p = e.payload
        if e.type is EventType.AGENT_CRASHED:
            return self.crash_card(e)
        if e.type is EventType.SCANNER_UPDATED and "digest" in p:
            return self.digest_card(p["digest"], e.created_at)
        if e.type is EventType.MARKET_CLOSED:
            lines = [e.message]
            if p.get("next_open"):
                lines.append(f"Next scheduled market open: {_t(p['next_open'])}")
            return OpsMessage("event", f"{emoji} {self._market_title(e)}", lines, COLOR["neutral"], e.created_at, e.type.value, e.dedupe_key)
        if e.type is EventType.MARKET_OPENED:
            return OpsMessage("event", f"{emoji} {self._market_title(e)}", [e.message] + ([f"Closes at {_t(p['closes_at'])}"] if p.get("closes_at") else []),
                              COLOR["ok"], e.created_at, e.type.value, e.dedupe_key)
        if e.type is EventType.MARKET_CLOSING_SOON:
            return OpsMessage("event", f"{emoji} MCX approaching close", [e.message], COLOR["warn"], e.created_at, e.type.value, e.dedupe_key)
        if e.type is EventType.AGENT_RECOVERED:
            return OpsMessage("event", f"{emoji} RECOVERED · {e.agent or 'agent'}", [e.message], COLOR["ok"], e.created_at, e.type.value, e.dedupe_key)
        if e.type is EventType.AGENT_FAILED:
            return OpsMessage("event", f"{emoji} PERMANENT FAILURE · {e.agent or 'agent'}", [e.message], COLOR["critical"], e.created_at, e.type.value, e.dedupe_key)
        if e.type is EventType.CALENDAR_FAILURE:
            return OpsMessage("event", f"{emoji} CALENDAR FAILURE", [e.message], COLOR["critical"], e.created_at, e.type.value, e.dedupe_key)
        if e.type is EventType.DEPLOYMENT_INFO:
            return OpsMessage("event", f"{emoji} Aureon deployed", [f"Version: {p.get('version')}", f"Git: {(p.get('git_sha') or 'unknown')[:7]}",
                              f"Branch: {p.get('branch') or 'n/a'}", f"Build: {p.get('build') or 'n/a'}"], COLOR["info"], e.created_at, e.type.value, e.dedupe_key)
        if e.type is EventType.M1_MISMATCH:
            loc, br = p.get("local") or {}, p.get("broker") or {}
            lines = [e.message]
            if loc and br:
                lines.append(f"local O/H/L/C/V {loc.get('o')}/{loc.get('h')}/{loc.get('l')}/{loc.get('c')}/{loc.get('v')} · "
                             f"broker {br.get('o')}/{br.get('h')}/{br.get('l')}/{br.get('c')}/{br.get('v')}")
            return OpsMessage("event", f"{emoji} LIVE vs BROKER M1 MISMATCH · {e.symbol}", lines, COLOR["error"], e.created_at, e.type.value, e.dedupe_key)
        title = f"{emoji} {e.type.value.replace('_', ' ')}" + (f" · {e.symbol}" if e.symbol else "")
        return OpsMessage("event", title, [e.message], _sev_color(e.severity), e.created_at, e.type.value, e.dedupe_key, batchable=e.type in BATCHED)

    @staticmethod
    def _market_title(e: SystemEvent) -> str:
        label = e.message.split(";")[0]
        if e.payload.get("reason") in ("FULL_HOLIDAY", "SPECIAL_SESSION_PENDING", "WEEKEND", "CALENDAR_OUT_OF_RANGE"):
            return label[:120]                       # "MCX CLOSED - Republic Day"
        return label.split(" - ")[0][:120]           # "MCX MARKET OPEN", "MCX MORNING SESSION CLOSED"

    def crash_card(self, e: SystemEvent) -> OpsMessage:
        p = e.payload
        lines = [f"Agent: `{e.agent or 'unknown'}`", f"State: RESTARTING" if p.get("restart_number") else "State: CRASHED",
                 f"Error: {p.get('exception_class', 'Exception')}: {e.message.split(':', 1)[-1].strip()[:200]}",
                 f"Crash ID: `{p.get('crash_id', 'n/a')}`", f"Restart: {p.get('restart_number', 0)}"
                 + (f"/{p['max_restarts']}" if p.get("max_restarts") else ""), f"Recovery: pending"]
        return OpsMessage("crash", "🚨 AUREON AGENT CRASH", lines, COLOR["error"], e.created_at, e.type.value, e.dedupe_key)

    def digest_card(self, digest: dict[str, Any], at: datetime) -> OpsMessage:
        w = digest.get("winners") or []
        l = digest.get("losers") or []
        fmt = lambda r: f"{r['rank']}. {r['symbol']} {r['change_pct']:+.2f}% ({r['ltp']:,.2f})"
        lines = ["**🏆 TODAY'S WINNERS**"] + ([fmt(r) for r in w[:10]] or ["no verified movers"])
        lines += ["", "**📉 TODAY'S LOSERS**"] + ([fmt(r) for r in l[:10]] or ["no verified movers"])
        lines += ["", f"Advancers {digest.get('advancers', 0)} | Decliners {digest.get('decliners', 0)} | Unchanged {digest.get('unchanged', 0)}"
                  f" · stale {digest.get('stale', 0)} · no reference {digest.get('unavailable', 0)} · universe {digest.get('universe', 0)}",
                  "Full universe: GET /api/v1/market/instruments"]
        return OpsMessage("digest", "📊 MCX leaderboard digest", lines, COLOR["info"], at, EventType.SCANNER_UPDATED.value, "scanner:digest")


class OpsChannel:
    """Outbound queue for operational messages with rate limiting and burst batching.

    `on_event` is subscribed to the bus (event-loop thread); the Discord client drains
    `due()` from its flush loop and sends. Headless runs still record what would be sent."""

    def __init__(self, now: Callable[[], datetime] = utc_now, max_per_minute: int = 20, batch_window: float = 2.0, metrics=None):
        self._now = now
        self.renderer = EventRenderer()
        self.max_per_minute = max_per_minute
        self.batch_window = timedelta(seconds=batch_window)
        self.metrics = metrics
        self.queue: deque[OpsMessage] = deque()
        self.sent: deque[OpsMessage] = deque(maxlen=200)
        self._sent_times: deque[datetime] = deque(maxlen=1000)
        self.suppressed = 0
        self.dropped = 0
        self._last_batch_at: datetime | None = None
        self.last_message_at: datetime | None = None

    def on_event(self, e: SystemEvent) -> None:
        msg = self.renderer.render(e)
        if msg is None:
            return
        self.queue.append(msg)

    def offer(self, msg: OpsMessage) -> None:
        self.queue.append(msg)

    def _allowance(self, now: datetime) -> int:
        cutoff = now - timedelta(minutes=1)
        recent = sum(1 for t in self._sent_times if t > cutoff)
        return max(0, self.max_per_minute - recent)

    def due(self, now: datetime | None = None) -> list[OpsMessage]:
        """Messages ready to send now: single messages immediately, batchable ones merged after the batch window."""
        now = now or self._now()
        if not self.queue:
            return []
        singles = [m for m in self.queue if not m.batchable]
        batch = [m for m in self.queue if m.batchable]
        out: list[OpsMessage] = list(singles)
        if batch:
            oldest = min(m.created_at for m in batch)
            if now - oldest >= self.batch_window or len(batch) >= 10:
                if len(batch) == 1:
                    out.append(batch[0])
                else:
                    lines = [f"{m.title}: {' '.join(m.lines)[:180]}" for m in batch[:10]]
                    if len(batch) > 10:
                        lines.append(f"... and {len(batch) - 10} more")
                    out.append(OpsMessage("summary", f"📋 {len(batch)} operational updates", lines, max(m.color for m in batch), now))
                batch = []
        self.queue = deque(batch)
        allowance = self._allowance(now)
        if len(out) > allowance:
            self.dropped += len(out) - allowance
            kept = out[:allowance]
            if kept and allowance > 0:
                kept[-1].lines.append(f"({len(out) - allowance} further updates suppressed by the rate limit; see GET /api/v1/events)")
            elif allowance == 0:
                self.queue.extendleft(reversed(out[allowance:]))  # try again next flush
                return []
            out = kept
        for m in out:
            self._sent_times.append(now)
        return out

    def mark_sent(self, msg: OpsMessage, ok: bool = True) -> None:
        now = self._now()
        if ok:
            self.sent.append(msg)
            self.last_message_at = now
            if self.metrics is not None:
                self.metrics.inc("discord_messages")
                self.metrics.mark("discord_last_message", now)
        elif self.metrics is not None:
            self.metrics.inc("discord_failures")


# ------------------------------------------------------------------ commands
class OpsCommands:
    """Informational command bodies shared by the slash commands and tests (observation only)."""

    def __init__(self, proj: "StatusProjection"):
        self.proj = proj

    def status(self) -> OpsMessage:
        s = self.proj.status()
        m = s["market"]
        agents = {a["agent_id"]: a["state"] for a in s["agents"]}
        syms = s["symbols"]
        live = sum(1 for x in syms if x["state"] == "LIVE")
        stale = sum(1 for x in syms if x["state"] in ("STALE", "DEGRADED"))
        pending = sum(x["pending_verification"] for x in syms)
        gaps = sum(x["unresolved_gaps"] for x in syms)
        sc = s["scanner"]
        close = _t(m.get("closes_at")) if m.get("state") == "OPEN" else (f"next open {_t(m.get('next_open'))}" if m.get("next_open") else "n/a")
        lines = ["**SYSTEM**", f"Aureon: {s['status']}", "", "**MARKET**", f"MCX: {m.get('state')}" + (f" ({m.get('reason')})" if m.get('state') != 'OPEN' else ""),
                 f"Session: {m.get('session') or '-'}", f"Close: {close}", "", "**AGENTS**"]
        order = ["market_feed_agent", "continuity_agent", "calendar_agent", "aggregation_agent", "scanner_agent", "analysis_agent", "outcome_agent",
                 "rollover_agent", "storage_agent", "discord_agent", "api_agent"]
        lines += [f"{aid.replace('_agent', '').replace('_', ' ').title()} {agents.get(aid, 'STOPPED')}" for aid in order]
        lines += ["", "**MARKET DATA**", f"Symbols: {len(syms)} (scanner universe {sc.get('universe', 0)})", f"Live: {live}", f"Stale: {stale}",
                  f"Pending M1: {pending}", f"Gaps: {gaps}", "", "**CRASHES**", f"Last 24h: {s['crashes'].get('last_24h', 0)} · unresolved {s['crashes'].get('unresolved', 0)}",
                  "", "**DEPLOYMENT**", f"Git: {s['version'].get('git_short')} · v{s['version'].get('app')} · up {s['version'].get('uptime_seconds')}s"]
        color = COLOR["ok"] if s["status"] == "HEALTHY" else (COLOR["error"] if s["status"] in ("ERROR", "FAILED") else COLOR["warn"])
        return OpsMessage("status", "🛰️ Aureon status", lines, color, self.proj._now())

    def crashes(self, limit: int = 5) -> OpsMessage:
        recent = list(reversed(self.proj.app.crashes.recent))[:limit]
        lines = [f"`{r.crash_id}` {fmt_ist(r.timestamp, '%d %b %H:%M')} {r.agent or r.component}: {r.summary()[:120]} · {r.recovery_result}"
                 for r in recent] or ["no crashes recorded in this process"]
        return OpsMessage("status", f"🚨 Recent crashes (unresolved {len(self.proj.app.crashes.unresolved())})", lines,
                          COLOR["error"] if self.proj.app.crashes.unresolved() else COLOR["ok"], self.proj._now())

    def market(self) -> OpsMessage:
        m = self.proj.market()
        lines = [f"State: {m.get('state')} ({m.get('reason')})", f"Trading date: {m.get('trading_date')}", f"Session: {m.get('session') or '-'}",
                 f"Detail: {m.get('detail')}", f"Holiday: {m.get('holiday') or '-'}", f"Opens: {_t(m.get('opens_at'))} · Closes: {_t(m.get('closes_at'))}",
                 f"Next open: {_t(m.get('next_open'))} · Next close: {_t(m.get('next_close'))}",
                 f"Calendar verified: {'yes' if m.get('calendar_verified') else 'NO'} · year covered: {'yes' if m.get('calendar_year_covered') else 'NO'}"]
        color = COLOR["ok"] if m.get("state") == "OPEN" else COLOR["neutral"]
        return OpsMessage("status", f"{'🔓' if m.get('state') == 'OPEN' else '🔒'} MCX {m.get('state')}", lines, color, self.proj._now())

    def winners(self, limit: int = 10) -> OpsMessage:
        sc = self.proj.scanner()
        rows = sc.get("winners", [])[:limit]
        lines = [f"{r['rank']}. {r['display_symbol']} {r['change_pct']:+.2f}% ({r['ltp']:,.2f})" for r in rows] or [
            "no verified movers" + ("" if sc.get("market_open") else " (market closed)")]
        return OpsMessage("status", "🏆 TODAY'S WINNERS", lines, COLOR["ok"], self.proj._now())

    def losers(self, limit: int = 10) -> OpsMessage:
        sc = self.proj.scanner()
        rows = sc.get("losers", [])[:limit]
        lines = [f"{r['rank']}. {r['display_symbol']} {r['change_pct']:+.2f}% ({r['ltp']:,.2f})" for r in rows] or [
            "no verified movers" + ("" if sc.get("market_open") else " (market closed)")]
        return OpsMessage("status", "📉 TODAY'S LOSERS", lines, COLOR["error"], self.proj._now())

    def agents(self) -> OpsMessage:
        lines = [f"{a['name']}: {a['state']}" + (f" · restarts {a['restart_count']}" if a['restart_count'] else "")
                 + (f" · last error {a['last_error'][:60]}" if a.get('last_error') else "") for a in self.proj.agents()]
        return OpsMessage("status", f"⚙️ Agents ({self.proj.app.agents.overall()})", lines, COLOR["info"], self.proj._now())

    def symbol(self, sym: str) -> OpsMessage:
        s = self.proj.symbol(sym.upper())
        if s is None:
            return OpsMessage("status", f"❓ {sym.upper()}", ["not a monitored symbol"], COLOR["neutral"], self.proj._now())
        pct = f"{s['change_pct']:+.2f}%" if s.get("change_pct") is not None else "n/a (no verified previous close)"
        lines = [f"Contract: {s['display_symbol']} ({s['security_id']}) exp {s['expiry']}", f"State: {s['state']} · {s['detail']}",
                 f"Last: {s['last_price']} · prev close {s['previous_close']} · {pct}", f"Last tick: {_t(s['last_tick_at'])} (lag {s['lag_seconds']}s)",
                 f"Pending M1: {s['pending_verification']} · gaps {s['unresolved_gaps']} · feed {'up' if s['feed_connected'] else 'down'}"
                 + (" (stalled)" if s.get('feed_stalled') else ""),
                 "Closed: " + ", ".join(f"{tf} {_t(t)}" for tf, t in s["last_closed"].items())]
        color = COLOR["ok"] if s["state"] == "LIVE" else (COLOR["error"] if s["state"] == "ERROR" else COLOR["warn"])
        return OpsMessage("status", f"📈 {s['symbol']}", lines, color, self.proj._now())
