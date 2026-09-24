from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

from aureon_mcx.market.timeframe import Timeframe


@dataclass(frozen=True)
class FeatureSnapshot:
    """Immutable feature snapshot frozen at detection time. Never updated."""

    detection_id: int
    candle_id: int
    symbol: str
    security_id: str
    expiry_date: str
    timeframe: Timeframe
    open_time: datetime
    direction: str
    setup_family: str
    reference_price: float
    atr: float | None
    rsi: float | None
    ema_fast: float | None
    ema_slow: float | None
    present_trend: str | None
    mtf_state: str | None
    feature_schema_version: str
    features: dict[str, Any]
    id: int | None = field(default=None, compare=False)

    @property
    def features_json(self) -> str:
        return json.dumps(self.features, sort_keys=True, default=str)


@dataclass
class OutcomeObservation:
    snapshot_id: int
    detection_id: int
    horizon: str
    horizon_bars: int
    bars_observed: int
    mfe: float
    mae: float
    mfe_atr: float | None
    mae_atr: float | None
    time_to_mfe_bars: int | None
    time_to_mae_bars: int | None
    bars_to_invalidation: int | None
    bars_to_follow_through: int | None
    final_move: float
    label: str
    label_version: str
    last_candle_open_time: datetime
    is_final: bool
    id: int | None = None
