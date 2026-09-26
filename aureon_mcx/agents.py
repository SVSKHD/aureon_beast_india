"""Agent registry: the major system responsibilities as OBSERVABLE agents.

Agents are not separate processes; they are the named responsibilities of the running
application (feed, continuity, calendar, aggregation, scanner, analysis, setups, outcomes,
rollover, storage, discord, api, health). Each exposes one common status model so the
status API and Discord can show which part of the system is healthy, degraded, stale,
recovering, restarting, in error or stopped.

State machine (per agent): HEALTHY -> DEGRADED -> RESTARTING -> HEALTHY, or after the
configured restart threshold FAILED (terminal, reported). STALE means no heartbeat within
the agent's heartbeat timeout while it should be active.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any, Callable

from aureon_mcx.events import EventType, Severity, SystemEventBus
from aureon_mcx.market.timeutil import utc_now

log = logging.getLogger("aureon.agents")

AGENT_STATES = ("HEALTHY", "DEGRADED", "STALE", "RECOVERING", "RESTARTING", "ERROR", "FAILED", "STOPPED")

AGENT_DEFINITIONS: dict[str, tuple[str, str]] = {
    "market_feed_agent": ("Market feed", "Dhan WebSocket: subscriptions, packet timing, reconnects"),
    "continuity_agent": ("Continuity", "M1 verification, pending minutes, gap recovery, historical reconciliation"),
    "calendar_agent": ("Calendar", "MCX open/closed state, current session, next open/close, holidays, special sessions"),
    "aggregation_agent": ("Aggregation", "M1 -> M5 -> M15 / H1 / H4 closed-candle aggregation, unresolved aggregate gaps"),
    "scanner_agent": ("Scanner", "Universe scanning: LTP, previous close, % change, breadth, winners / losers"),
    "analysis_agent": ("Analysis", "EMA, RSI, ATR, structure, wick, liquidity, breakout, MTF analysis"),
    "setup_agent": ("Setups", "Setup lifecycle (observation only)"),
    "outcome_agent": ("Outcomes", "Outcome observations and labels"),
    "rollover_agent": ("Rollover", "Contract discovery and staged handover"),
    "storage_agent": ("Storage", "SQLite, migrations, persistence, cache health"),
    "discord_agent": ("Discord", "Outbound Discord communication, coalescing, failures / rate limits"),
    "api_agent": ("Status API", "Read-only HTTP status service"),
    "health_agent": ("Health", "Aggregates all component / agent status"),
}


@dataclass
class AgentStatus:
    agent_id: str
    name: str
    role: str
    state: str = "STOPPED"
    started_at: datetime | None = None
    last_heartbeat: datetime | None = None
    last_success: datetime | None = None
    last_error: str | None = None
    last_error_at: datetime | None = None
    restart_count: int = 0
    work_count: int = 0
    queue_depth: int = 0
    details: dict[str, Any] = field(default_factory=dict)
    version: str = "1"
    heartbeat_timeout: timedelta | None = None

    def to_dict(self) -> dict[str, Any]:
        return {"agent_id": self.agent_id, "name": self.name, "role": self.role, "state": self.state,
                "started_at": self.started_at.isoformat() if self.started_at else None,
                "last_heartbeat": self.last_heartbeat.isoformat() if self.last_heartbeat else None,
                "last_success": self.last_success.isoformat() if self.last_success else None,
                "last_error": self.last_error, "last_error_at": self.last_error_at.isoformat() if self.last_error_at else None,
                "restart_count": self.restart_count, "work_count": self.work_count, "queue_depth": self.queue_depth,
                "details": dict(self.details), "version": self.version}


class AgentRegistry:
    def __init__(self, events: SystemEventBus | None = None, now: Callable[[], datetime] = utc_now):
        self.events = events
        self._now = now
        self.agents: dict[str, AgentStatus] = {}
        for agent_id, (name, role) in AGENT_DEFINITIONS.items():
            self.agents[agent_id] = AgentStatus(agent_id, name, role)

    # ------------------------------------------------------------- basics
    def get(self, agent_id: str) -> AgentStatus:
        if agent_id not in self.agents:
            self.agents[agent_id] = AgentStatus(agent_id, agent_id, "")
        return self.agents[agent_id]

    def register(self, agent_id: str, name: str, role: str, heartbeat_timeout: float | None = None) -> AgentStatus:
        a = self.get(agent_id)
        a.name, a.role = name, role
        a.heartbeat_timeout = timedelta(seconds=heartbeat_timeout) if heartbeat_timeout else None
        return a

    def start(self, agent_id: str, **details: Any) -> AgentStatus:
        a = self.get(agent_id)
        now = self._now()
        a.started_at = a.started_at or now
        a.last_heartbeat = now
        a.details.update(details)
        self.set_state(agent_id, "HEALTHY", "started")
        return a

    def heartbeat(self, agent_id: str, work: int = 0, queue_depth: int | None = None, **details: Any) -> AgentStatus:
        a = self.get(agent_id)
        a.last_heartbeat = self._now()
        if work:
            a.work_count += work
            a.last_success = a.last_heartbeat
        if queue_depth is not None:
            a.queue_depth = queue_depth
        if details:
            a.details.update(details)
        if a.state == "STALE":
            self.set_state(agent_id, "HEALTHY", "heartbeat resumed")
        return a

    def success(self, agent_id: str, work: int = 1, **details: Any) -> AgentStatus:
        return self.heartbeat(agent_id, work=work, **details)

    def error(self, agent_id: str, error: str, state: str = "ERROR", **details: Any) -> AgentStatus:
        a = self.get(agent_id)
        a.last_error, a.last_error_at = error[:300], self._now()
        a.details.update(details)
        self.set_state(agent_id, state, error[:120])
        return a

    def restarting(self, agent_id: str, restart_number: int, reason: str) -> AgentStatus:
        a = self.get(agent_id)
        a.restart_count = restart_number
        self.set_state(agent_id, "RESTARTING", reason)
        return a

    def set_state(self, agent_id: str, state: str, reason: str = "", **details: Any) -> AgentStatus:
        if state not in AGENT_STATES:
            raise ValueError(f"unknown agent state {state}")
        a = self.get(agent_id)
        old = a.state
        a.state = state
        a.details.update(details)
        if reason:
            a.details["reason"] = reason
        if old != state:
            log.info("agent_state agent=%s %s -> %s reason=%r", agent_id, old, state, reason)
            if self.events is not None:
                sev = Severity.INFO if state in ("HEALTHY", "STOPPED") else (Severity.ERROR if state in ("ERROR", "FAILED") else Severity.WARNING)
                type_ = EventType.AGENT_RECOVERED if (state == "HEALTHY" and old in ("DEGRADED", "STALE", "RECOVERING", "RESTARTING", "ERROR")) \
                    else (EventType.AGENT_FAILED if state == "FAILED" else EventType.AGENT_STATE_CHANGED)
                self.events.emit(type_, f"{a.name} {old} -> {state}" + (f": {reason}" if reason else ""), severity=sev,
                                 dedupe_key=f"agent:{agent_id}:{old}->{state}", agent=agent_id, old_state=old, new_state=state)
        return a

    # ------------------------------------------------------------ queries
    def check_heartbeats(self) -> list[str]:
        """Mark agents STALE whose heartbeat is overdue (returns the ids that changed)."""
        now = self._now()
        changed = []
        for a in self.agents.values():
            if a.heartbeat_timeout is None or a.state not in ("HEALTHY", "DEGRADED") or a.last_heartbeat is None:
                continue
            if now - a.last_heartbeat > a.heartbeat_timeout:
                self.set_state(a.agent_id, "STALE", f"no heartbeat for {int((now - a.last_heartbeat).total_seconds())}s")
                changed.append(a.agent_id)
        return changed

    def overall(self) -> str:
        states = [a.state for a in self.agents.values() if a.state != "STOPPED"]
        if not states:
            return "STOPPED"
        if any(s == "FAILED" for s in states):
            return "FAILED"
        if any(s == "ERROR" for s in states):
            return "ERROR"
        if any(s in ("DEGRADED", "STALE", "RECOVERING", "RESTARTING") for s in states):
            return "DEGRADED"
        return "HEALTHY"

    def snapshot(self) -> list[dict[str, Any]]:
        return [a.to_dict() for a in self.agents.values()]
