"""CrashReporter: durable reports for every unexpected component failure.

detect -> report (persist + event) -> isolate -> restart -> recover (resolve the report).

Reports live in SQLite (`crash_reports`). Credentials are never persisted: messages and
stack traces pass through the same redaction as the logs. Discord gets a short summary
(the full trace stays in the database and the status API).
"""
from __future__ import annotations

import logging
import traceback
import uuid
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Callable

from aureon_mcx.events import EventType, Severity, SystemEventBus
from aureon_mcx.logging_setup import RedactingFilter
from aureon_mcx.market.timeutil import utc_now
from aureon_mcx.version import APP_VERSION, git_sha

log = logging.getLogger("aureon.crash")


@dataclass
class CrashReport:
    crash_id: str
    timestamp: datetime
    component: str
    agent: str | None
    task: str | None
    exception_class: str
    message: str
    stack_trace: str
    symbol: str | None = None
    security_id: str | None = None
    app_version: str = APP_VERSION
    git_sha: str | None = None
    process_uptime_s: float = 0.0
    restart_number: int = 0
    recovery_result: str = "pending"     # pending | success | failed | shutdown
    resolved_at: datetime | None = None
    extra: dict[str, Any] = field(default_factory=dict)

    def to_record(self) -> dict[str, Any]:
        return {"crash_id": self.crash_id, "timestamp": self.timestamp.isoformat(), "component": self.component, "agent": self.agent,
                "task": self.task, "exception_class": self.exception_class, "message": self.message, "stack_trace": self.stack_trace,
                "symbol": self.symbol, "security_id": self.security_id, "app_version": self.app_version, "git_sha": self.git_sha,
                "process_uptime_s": self.process_uptime_s, "restart_number": self.restart_number, "recovery_result": self.recovery_result,
                "resolved_at": self.resolved_at.isoformat() if self.resolved_at else None}

    def to_dict(self, include_trace: bool = False) -> dict[str, Any]:
        d = self.to_record()
        if not include_trace:
            d["stack_trace"] = None
            d["stack_trace_lines"] = self.stack_trace.count("\n") + 1
        return d

    def summary(self) -> str:
        return f"{self.exception_class}: {self.message[:160]}"


class CrashReporter:
    def __init__(self, repos=None, events: SystemEventBus | None = None, now: Callable[[], datetime] = utc_now,
                 started_at: datetime | None = None, max_trace_chars: int = 20000):
        self.repos = repos
        self.events = events
        self._now = now
        self.started_at = started_at or now()
        self.max_trace_chars = max_trace_chars
        self.reports: dict[str, CrashReport] = {}
        self.recent: list[CrashReport] = []

    @staticmethod
    def redact(text: str) -> str:
        return RedactingFilter.redact(text or "")

    def report(self, component: str, exc: BaseException, *, agent: str | None = None, task: str | None = None, symbol: str | None = None,
               security_id: str | None = None, restart_number: int = 0, **extra: Any) -> CrashReport:
        now = self._now()
        trace = "".join(traceback.format_exception(type(exc), exc, exc.__traceback__))
        rep = CrashReport(
            crash_id=f"CR-{now.strftime('%Y%m%d-%H%M%S')}-{uuid.uuid4().hex[:6]}", timestamp=now, component=component, agent=agent, task=task,
            exception_class=type(exc).__name__, message=self.redact(str(exc))[:1000], stack_trace=self.redact(trace)[-self.max_trace_chars:],
            symbol=symbol, security_id=security_id, git_sha=git_sha(), process_uptime_s=max(0.0, (now - self.started_at).total_seconds()),
            restart_number=restart_number, extra={k: v for k, v in extra.items() if isinstance(v, (str, int, float, bool))},
        )
        self.reports[rep.crash_id] = rep
        self.recent.append(rep)
        del self.recent[:-200]
        log.error("crash_report crash_id=%s component=%s agent=%s error=%s restart=%d", rep.crash_id, component, agent, rep.summary(), restart_number)
        if self.repos is not None:
            try:
                self.repos.crashes.insert(rep.to_record())
            except Exception as db_exc:  # noqa: BLE001 - a broken database must not hide the crash
                log.exception("crash_persist_failed crash_id=%s error=%s", rep.crash_id, type(db_exc).__name__)
        if self.events is not None:
            self.events.emit(EventType.AGENT_CRASHED, f"{agent or component} crashed: {rep.summary()} (crash {rep.crash_id}, restart {restart_number})",
                             severity=Severity.ERROR, agent=agent or component, symbol=symbol, security_id=security_id, crash_id=rep.crash_id,
                             exception_class=rep.exception_class, restart_number=restart_number)
        return rep

    def resolve(self, crash_id: str, result: str = "success") -> CrashReport | None:
        rep = self.reports.get(crash_id)
        if rep is None:
            return None
        rep.recovery_result = result
        rep.resolved_at = self._now()
        if self.repos is not None:
            try:
                self.repos.crashes.set_result(crash_id, result, rep.resolved_at)
            except Exception as db_exc:  # noqa: BLE001
                log.exception("crash_resolve_failed crash_id=%s error=%s", crash_id, type(db_exc).__name__)
        if self.events is not None and result == "success":
            self.events.emit(EventType.AGENT_RECOVERED, f"{rep.agent or rep.component} recovered after crash {crash_id}", agent=rep.agent or rep.component,
                             crash_id=crash_id)
        return rep

    def unresolved(self) -> list[CrashReport]:
        return [r for r in self.recent if r.resolved_at is None]

    def last(self) -> CrashReport | None:
        return self.recent[-1] if self.recent else None

    def count_since(self, since: datetime) -> int:
        return sum(1 for r in self.recent if r.timestamp >= since)

    def snapshot(self) -> dict[str, Any]:
        last = self.last()
        return {"unresolved": len(self.unresolved()), "total": len(self.recent),
                "last_crash": last.to_dict() if last else None}
