"""Service health snapshot (log + Discord status line)."""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime

from aureon_mcx.logging_setup import kv
from aureon_mcx.market.timeutil import fmt_ist, utc_now

log = logging.getLogger("aureon.health")


@dataclass
class ComponentHealth:
    name: str
    status: str = "starting"
    detail: str = ""
    updated_at: datetime = field(default_factory=utc_now)


@dataclass
class HealthState:
    components: dict[str, ComponentHealth] = field(default_factory=dict)
    started_at: datetime = field(default_factory=utc_now)
    last_candle: dict[str, datetime] = field(default_factory=dict)

    def set(self, name: str, status: str, detail: str = "") -> None:
        self.components[name] = ComponentHealth(name, status, detail, utc_now())
        log.info("health %s", kv(component=name, status=status, detail=detail or None))

    def candle_seen(self, symbol: str, open_time: datetime) -> None:
        self.last_candle[symbol] = open_time

    @property
    def ok(self) -> bool:
        return all(c.status in ("ok", "live") for c in self.components.values()) and bool(self.components)

    def snapshot(self) -> dict:
        return {"ok": self.ok, "started_at": self.started_at.isoformat(),
                "components": {k: {"status": c.status, "detail": c.detail, "updated_at": c.updated_at.isoformat()} for k, c in self.components.items()},
                "last_candle": {k: v.isoformat() for k, v in self.last_candle.items()}}

    def status_line(self) -> str:
        parts = [f"{k}={c.status}" for k, c in sorted(self.components.items())]
        candles = ", ".join(f"{s} {fmt_ist(t, '%H:%M')}" for s, t in sorted(self.last_candle.items()))
        return f"health {'OK' if self.ok else 'DEGRADED'} · " + " ".join(parts) + (f" · last closed: {candles}" if candles else "")
