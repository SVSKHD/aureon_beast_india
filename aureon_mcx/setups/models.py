from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any

from aureon_mcx.detection.models import Direction
from aureon_mcx.market.timeframe import Timeframe


class SetupState(str, Enum):
    OBSERVING = "OBSERVING"
    WATCH = "WATCH"
    DEVELOPING = "DEVELOPING"
    CONFIRMED = "CONFIRMED"
    PULLBACK = "PULLBACK"
    CONTINUATION = "CONTINUATION"
    FAKEOUT_RISK = "FAKEOUT_RISK"
    COMPLETED = "COMPLETED"
    INVALIDATED = "INVALIDATED"

    @property
    def is_terminal(self) -> bool:
        return self in (SetupState.COMPLETED, SetupState.INVALIDATED)


@dataclass
class Setup:
    symbol: str
    security_id: str
    expiry_date: str
    timeframe: Timeframe
    family: str
    direction: Direction
    state: SetupState
    anchor_price: float
    invalidation_price: float
    origin_detection_id: int
    origin_candle_id: int
    created_open_time: datetime
    updated_open_time: datetime
    context: dict[str, Any] = field(default_factory=dict)
    closed_at: datetime | None = None
    id: int | None = None

    @property
    def context_json(self) -> str:
        return json.dumps(self.context, sort_keys=True, default=str)


@dataclass
class SetupEvent:
    setup_id: int
    candle_id: int
    from_state: SetupState | None
    to_state: SetupState
    reason: str
    evidence: dict[str, Any]
    open_time: datetime
    detection_id: int | None = None
    id: int | None = None

    @property
    def evidence_json(self) -> str:
        return json.dumps(self.evidence, sort_keys=True, default=str)
