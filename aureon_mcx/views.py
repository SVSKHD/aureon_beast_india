"""Presentation view of a setup: plain data assembled by the observer from stored state.

Discord renders this and nothing else. The `state_hash` covers only material
fields so cards are patched only when something meaningful changed.
"""
from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass, field
from datetime import datetime


@dataclass
class SetupView:
    setup_id: int
    symbol: str
    display_symbol: str
    security_id: str
    expiry_date: str
    timeframe: str
    family: str
    direction: str
    state: str
    anchor_price: float
    invalidation_price: float
    origin_label: str
    origin_detection_id: int
    created_open_time: datetime
    updated_open_time: datetime
    last_price: float
    # setup section
    context_lines: list[str] = field(default_factory=list)
    events: list[dict] = field(default_factory=list)          # {"id", "to_state", "reason", "open_time"}
    # trend section
    present_trend: str = "unavailable"
    session_rows: list[dict] = field(default_factory=list)    # {"session", "group", "trend", "is_current", "session_date"}
    mtf_rows: list[tuple[str, str]] = field(default_factory=list)
    mtf_alignment: str = "MTF NO CONTEXT"
    mtf_notes: list[str] = field(default_factory=list)
    early_reversal: bool = False
    structure_context: str = "MIXED"
    structure_sequence: str = ""
    # momentum section
    ema_fast: float | None = None
    ema_slow: float | None = None
    ema_relation: str = "n/a"
    early_ema: str | None = None
    latest_cross: str | None = None
    rsi: float | None = None
    rsi_direction: str | None = None
    atr: float | None = None
    volume: float | None = None
    open_interest: float | None = None
    # confirmation section
    badges: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    fakeout_flags: list[str] = field(default_factory=list)
    blockers: list[str] = field(default_factory=list)
    cleared: bool = False
    policy_version: str = ""
    # trace / cohort
    detection_refs: list[str] = field(default_factory=list)
    cohort_lines: list[str] | None = None
    is_terminal: bool = False

    @property
    def final_check(self) -> str:
        return "✅ CLEARED FOR REVIEW" if self.cleared else "⛔ NOT CLEARED"

    @property
    def title(self) -> str:
        return f"MCX {self.symbol} {self.timeframe} · {self.origin_label_short} · {self.direction}"

    @property
    def origin_label_short(self) -> str:
        return self.origin_label.split("·")[0].strip().lower() if self.origin_label else self.family.replace("_", " ")

    @property
    def subtitle(self) -> str:
        return f"{self.state} · {self.final_check}"

    @property
    def last_event_id(self) -> int | None:
        return self.events[-1]["id"] if self.events else None

    def state_hash(self) -> str:
        material = {
            "state": self.state, "cleared": self.cleared, "blockers": self.blockers, "fakeout_flags": self.fakeout_flags,
            "mtf_rows": self.mtf_rows, "mtf_alignment": self.mtf_alignment, "present_trend": self.present_trend,
            "last_event_id": self.last_event_id, "badges": self.badges, "structure_context": self.structure_context,
            "early_ema": self.early_ema, "latest_cross": self.latest_cross,
        }
        return hashlib.sha256(json.dumps(material, sort_keys=True, default=str).encode()).hexdigest()

    def to_dict(self) -> dict:
        return asdict(self)
