"""Service health: component statuses plus a per-symbol market-data trust state.

Symbol states
  WARMING            historical warmup in progress
  LIVE               feed connected, continuity intact, candles arriving
  RECOVERING_GAP     backfilling missed closed candles after a (re)connect
  ROLLOVER_WARMING   a new contract is being prepared before the switch
  STALE              connected but no closed primary candle / no packet for too long in open market
  DEGRADED           an incident stayed unresolved beyond `degraded_after_seconds`; retries continue
  ERROR              unresolved data gap or failed recovery: analytics suspended (fail closed)

A connected WebSocket with missing candles is NOT healthy.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime

from aureon_mcx.logging_setup import kv
from aureon_mcx.market.candle_builder import LoopBound
from aureon_mcx.market.timeutil import fmt_ist, utc_now

log = logging.getLogger("aureon.health")

SYMBOL_STATES = ("WARMING", "LIVE", "RECOVERING_GAP", "ROLLOVER_WARMING", "STALE", "DEGRADED", "ERROR")


@dataclass
class ComponentHealth:
    name: str
    status: str = "starting"
    detail: str = ""
    updated_at: datetime = field(default_factory=utc_now)


@dataclass
class SymbolHealth:
    symbol: str
    security_id: str = ""
    expiry: str = ""
    state: str = "WARMING"
    detail: str = ""
    last_tick_at: datetime | None = None
    last_closed: dict[str, datetime] = field(default_factory=dict)  # "M1", "M5", "M15", "H1", "H4"
    feed_connected: bool = False
    reconnects: int = 0
    unresolved_gaps: int = 0
    updated_at: datetime = field(default_factory=utc_now)

    @property
    def trusted(self) -> bool:
        return self.state == "LIVE" and self.unresolved_gaps == 0

    def to_dict(self) -> dict:
        return {"symbol": self.symbol, "security_id": self.security_id, "expiry": self.expiry, "state": self.state, "detail": self.detail,
                "last_tick_at": self.last_tick_at.isoformat() if self.last_tick_at else None,
                "last_closed": {k: v.isoformat() for k, v in self.last_closed.items()}, "feed_connected": self.feed_connected,
                "reconnects": self.reconnects, "unresolved_gaps": self.unresolved_gaps, "updated_at": self.updated_at.isoformat()}

    def line(self) -> str:
        closed = " ".join(f"{tf}={fmt_ist(t, '%H:%M')}" for tf, t in sorted(self.last_closed.items(), key=lambda kv_: kv_[0]))
        tick = fmt_ist(self.last_tick_at, "%H:%M:%S") if self.last_tick_at else "n/a"
        return (f"{self.symbol}[{self.security_id} exp {self.expiry}] state={self.state} feed={'up' if self.feed_connected else 'down'} "
                f"reconnects={self.reconnects} gaps={self.unresolved_gaps} tick={tick} {closed}" + (f" · {self.detail}" if self.detail else ""))


@dataclass
class HealthState(LoopBound):
    components: dict[str, ComponentHealth] = field(default_factory=dict)
    symbols: dict[str, SymbolHealth] = field(default_factory=dict)
    started_at: datetime = field(default_factory=utc_now)
    last_candle: dict[str, datetime] = field(default_factory=dict)
    on_symbol_state: "callable | None" = None  # (symbol, old_state, new_state, detail) -> None

    # ---------------------------------------------------------- components
    def set(self, name: str, status: str, detail: str = "") -> None:
        self._check_thread("set")
        self.components[name] = ComponentHealth(name, status, detail, utc_now())
        log.info("health %s", kv(component=name, status=status, detail=detail or None))

    # ------------------------------------------------------------- symbols
    def symbol(self, symbol: str) -> SymbolHealth:
        return self.symbols.setdefault(symbol, SymbolHealth(symbol))

    def set_symbol_state(self, symbol: str, state: str, detail: str = "", **fields) -> SymbolHealth:
        if state not in SYMBOL_STATES:
            raise ValueError(f"unknown symbol health state {state}")
        self._check_thread("set_symbol_state")
        sh = self.symbol(symbol)
        old_state = sh.state
        changed = sh.state != state
        sh.state, sh.detail, sh.updated_at = state, detail, utc_now()
        for k, v in fields.items():
            setattr(sh, k, v)
        if changed:
            log.info("symbol_health %s", kv(symbol=symbol, state=state, detail=detail or None, security_id=sh.security_id))
            if self.on_symbol_state is not None:
                self.on_symbol_state(symbol, old_state, state, detail)
        return sh

    def candle_seen(self, symbol: str, open_time: datetime) -> None:
        """`symbol` is "GOLD/M5" style (kept for the observer) or a plain symbol."""
        self._check_thread("candle_seen")
        self.last_candle[symbol] = open_time
        if "/" in symbol:
            name, tf = symbol.split("/", 1)
            self.symbol(name).last_closed[tf] = open_time

    def tick_seen(self, symbol: str, ts: datetime) -> None:
        self._check_thread("tick_seen")
        self.symbol(symbol).last_tick_at = ts

    # --------------------------------------------------------------- views
    @property
    def ok(self) -> bool:
        comps_ok = all(c.status in ("ok", "live", "connected") for c in self.components.values()) and bool(self.components)
        symbols_ok = all(s.trusted for s in self.symbols.values()) if self.symbols else True
        return comps_ok and symbols_ok

    def snapshot(self) -> dict:
        return {"ok": self.ok, "started_at": self.started_at.isoformat(),
                "components": {k: {"status": c.status, "detail": c.detail, "updated_at": c.updated_at.isoformat()} for k, c in self.components.items()},
                "symbols": {k: s.to_dict() for k, s in self.symbols.items()},
                "last_candle": {k: v.isoformat() for k, v in self.last_candle.items()}}

    def status_line(self) -> str:
        parts = [f"{k}={c.status}" for k, c in sorted(self.components.items())]
        line = f"health {'OK' if self.ok else 'DEGRADED'} · " + " ".join(parts)
        for s in sorted(self.symbols.values(), key=lambda x: x.symbol):
            line += "\n" + s.line()
        return line
