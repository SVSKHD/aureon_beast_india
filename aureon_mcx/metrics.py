"""Process-wide observability counters (exposed by the status API and Discord).

Counters are plain integers updated on the event-loop thread; `snapshot()` returns a copy.
Nothing here is persisted: metrics describe the running process.
"""
from __future__ import annotations

import threading
from datetime import datetime

COUNTERS = (
    "websocket_reconnects", "packets_received", "packets_rejected", "ticks_received",
    "live_m1_built", "live_m1_verified", "live_m1_mismatches", "partial_m1_replaced", "silent_m1_replaced", "suspect_m1_replaced",
    "gaps_detected", "gaps_repaired", "observer_candles_processed",
    "discord_messages", "discord_failures", "api_requests", "crashes", "restarts", "scanner_updates",
)


class Metrics:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self.values: dict[str, int] = {k: 0 for k in COUNTERS}
        self.gauges: dict[str, float | int | None] = {}
        self.timestamps: dict[str, datetime] = {}

    def inc(self, name: str, n: int = 1) -> None:
        with self._lock:
            self.values[name] = self.values.get(name, 0) + n

    def get(self, name: str) -> int:
        return self.values.get(name, 0)

    def set_gauge(self, name: str, value: float | int | None) -> None:
        with self._lock:
            self.gauges[name] = value

    def mark(self, name: str, at: datetime) -> None:
        with self._lock:
            self.timestamps[name] = at

    def snapshot(self) -> dict:
        with self._lock:
            return {"counters": dict(self.values), "gauges": dict(self.gauges),
                    "timestamps": {k: v.isoformat() for k, v in self.timestamps.items()}}
