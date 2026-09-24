from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any


@dataclass
class ClearanceResult:
    """Outcome of DETECTION -> CONTEXT -> CONFIRMATION -> CLEARANCE.

    `cleared` means only that the configured research policy's evidence agrees.
    It is never a trading recommendation.
    """

    setup_id: int
    candle_id: int
    open_time: datetime
    cleared: bool
    policy_version: str
    mtf_state: str
    blockers: list[str] = field(default_factory=list)
    badges: list[str] = field(default_factory=list)
    fakeout_flags: list[str] = field(default_factory=list)
    evidence: dict[str, Any] = field(default_factory=dict)
    id: int | None = None

    @property
    def final_check(self) -> str:
        return "✅ CLEARED FOR REVIEW" if self.cleared else "⛔ NOT CLEARED"

    def to_json_fields(self) -> dict[str, str]:
        return {
            "blockers_json": json.dumps(self.blockers),
            "badges_json": json.dumps(self.badges),
            "fakeout_flags_json": json.dumps(self.fakeout_flags),
            "evidence_json": json.dumps(self.evidence, sort_keys=True, default=str),
        }
