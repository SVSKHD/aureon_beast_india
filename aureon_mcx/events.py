"""SystemEventBus: the ONE source of truth for meaningful operational state changes.

Producers (application, continuity, feed, rollover, scanner, supervisor) publish typed
`SystemEvent`s; consumers (persistent system_events repository, agent registry, Discord
renderer, status API) subscribe. Nobody calls Discord directly for operational updates.

Deduplication: every event carries a stable `dedupe_key` (e.g. ``feed:disconnected:gen:8``).
The bus publishes an event with a key only when the key is new, its severity changed, or the
configured reminder interval passed; a repeated identical condition is otherwise suppressed
(counted in `suppressed`). Events without a key are always published.
"""
from __future__ import annotations

import logging
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Callable

from aureon_mcx.market.timeutil import utc_now

log = logging.getLogger("aureon.events")


class EventType(str, Enum):
    APP_STARTED = "APP_STARTED"
    APP_STOPPED = "APP_STOPPED"
    AGENT_STATE_CHANGED = "AGENT_STATE_CHANGED"
    AGENT_CRASHED = "AGENT_CRASHED"
    AGENT_RECOVERED = "AGENT_RECOVERED"
    AGENT_FAILED = "AGENT_FAILED"
    FEED_CONNECTED = "FEED_CONNECTED"
    FEED_DISCONNECTED = "FEED_DISCONNECTED"
    FEED_RECONNECTING = "FEED_RECONNECTING"
    FEED_STALLED = "FEED_STALLED"
    FEED_RECOVERED = "FEED_RECOVERED"
    FEED_SUBSCRIPTION_PROBLEM = "FEED_SUBSCRIPTION_PROBLEM"
    MARKET_OPENED = "MARKET_OPENED"
    MARKET_CLOSED = "MARKET_CLOSED"
    MARKET_SESSION_CHANGED = "MARKET_SESSION_CHANGED"
    MARKET_CLOSING_SOON = "MARKET_CLOSING_SOON"
    CALENDAR_FAILURE = "CALENDAR_FAILURE"
    M1_PENDING = "M1_PENDING"
    M1_VERIFICATION_STARTED = "M1_VERIFICATION_STARTED"
    M1_VERIFIED = "M1_VERIFIED"
    M1_VERIFICATION_FAILED = "M1_VERIFICATION_FAILED"
    M1_MISMATCH = "M1_MISMATCH"
    M1_ABANDONED = "M1_ABANDONED"
    DATA_GAP = "DATA_GAP"
    DATA_GAP_REPAIRED = "DATA_GAP_REPAIRED"
    SYMBOL_STATE_CHANGED = "SYMBOL_STATE_CHANGED"
    ROLLOVER_CANDIDATE = "ROLLOVER_CANDIDATE"
    ROLLOVER_STARTED = "ROLLOVER_STARTED"
    ROLLOVER_SWITCHED = "ROLLOVER_SWITCHED"
    ROLLOVER_COMPLETED = "ROLLOVER_COMPLETED"
    ROLLOVER_FAILED = "ROLLOVER_FAILED"
    SCANNER_UPDATED = "SCANNER_UPDATED"
    SCANNER_LEADER_CHANGED = "SCANNER_LEADER_CHANGED"
    DATABASE_MIGRATED = "DATABASE_MIGRATED"
    API_STARTED = "API_STARTED"
    DEPLOYMENT_INFO = "DEPLOYMENT_INFO"


class Severity(str, Enum):
    INFO = "INFO"
    WARNING = "WARNING"
    ERROR = "ERROR"
    CRITICAL = "CRITICAL"


@dataclass(frozen=True)
class SystemEvent:
    type: EventType
    message: str
    severity: Severity = Severity.INFO
    dedupe_key: str | None = None
    agent: str | None = None
    symbol: str | None = None
    security_id: str | None = None
    payload: dict[str, Any] = field(default_factory=dict)
    created_at: datetime = field(default_factory=utc_now)

    def to_record(self) -> dict[str, Any]:
        import json

        return {"event_type": self.type.value, "severity": self.severity.value, "dedupe_key": self.dedupe_key, "agent": self.agent,
                "symbol": self.symbol, "security_id": self.security_id, "message": self.message,
                "payload_json": json.dumps(self.payload, default=str), "created_at": self.created_at.isoformat()}

    def to_dict(self) -> dict[str, Any]:
        return {"type": self.type.value, "severity": self.severity.value, "message": self.message, "dedupe_key": self.dedupe_key,
                "agent": self.agent, "symbol": self.symbol, "security_id": self.security_id, "payload": dict(self.payload),
                "created_at": self.created_at.isoformat()}


Subscriber = Callable[[SystemEvent], None]


class SystemEventBus:
    def __init__(self, now: Callable[[], datetime] = utc_now, reminder_interval: timedelta = timedelta(minutes=15), history: int = 500):
        self._now = now
        self.reminder_interval = reminder_interval
        self._subscribers: list[Subscriber] = []
        self._last_by_key: dict[str, tuple[Severity, datetime]] = {}
        self.history: deque[SystemEvent] = deque(maxlen=history)
        self.published = 0
        self.suppressed = 0

    def subscribe(self, fn: Subscriber) -> None:
        self._subscribers.append(fn)

    def unsubscribe(self, fn: Subscriber) -> None:
        if fn in self._subscribers:
            self._subscribers.remove(fn)

    def should_publish(self, event: SystemEvent) -> bool:
        if event.dedupe_key is None:
            return True
        last = self._last_by_key.get(event.dedupe_key)
        if last is None:
            return True
        severity, at = last
        if severity != event.severity:
            return True
        return event.created_at - at >= self.reminder_interval

    def publish(self, event: SystemEvent) -> bool:
        """Deliver to every subscriber unless suppressed by deduplication. Returns True when delivered."""
        if not self.should_publish(event):
            self.suppressed += 1
            return False
        if event.dedupe_key is not None:
            self._last_by_key[event.dedupe_key] = (event.severity, event.created_at)
        self.history.append(event)
        self.published += 1
        log.info("system_event type=%s severity=%s agent=%s symbol=%s message=%r", event.type.value, event.severity.value, event.agent,
                 event.symbol, event.message)
        for fn in list(self._subscribers):
            try:
                fn(event)
            except Exception as exc:  # noqa: BLE001 - one broken consumer never blocks the others
                log.exception("event_subscriber_failed subscriber=%s error=%s", getattr(fn, "__name__", fn), type(exc).__name__)
        return True

    def emit(self, type_: EventType, message: str, *, severity: Severity = Severity.INFO, dedupe_key: str | None = None,
             agent: str | None = None, symbol: str | None = None, security_id: str | None = None, **payload: Any) -> bool:
        return self.publish(SystemEvent(type_, message, severity, dedupe_key, agent, symbol, security_id, payload, self._now()))

    def clear_key(self, dedupe_key: str) -> None:
        """Forget a condition so its next occurrence publishes again (state returned to normal)."""
        self._last_by_key.pop(dedupe_key, None)

    def recent(self, limit: int = 50, type_: EventType | None = None) -> list[SystemEvent]:
        items = [e for e in self.history if type_ is None or e.type is type_]
        return list(items)[-limit:][::-1]
