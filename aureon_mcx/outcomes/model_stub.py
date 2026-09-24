"""Phase-3 model interfaces (stubs only; no training is implemented here).

Rules for any future implementation:
  * never retrain the live model intraday;
  * time-based / walk-forward splits only, never random shuffles across time;
  * separate GOLD / SILVER distributions or encode instrument identity;
  * version everything: feature_schema_version, label_version, model_version,
    training_start, training_end, sample_size, validation metrics;
  * a model provides evidence context only. It must NOT execute trades.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Protocol

from .models import FeatureSnapshot


@dataclass(frozen=True)
class ModelRegistryEntry:
    model_version: str
    feature_schema_version: str
    label_version: str
    training_start: datetime
    training_end: datetime
    sample_size: int
    validation_metrics: dict[str, Any]
    instrument_scope: str  # e.g. "GOLD", "SILVER", "GOLD+SILVER(encoded)"


@dataclass(frozen=True)
class ModelEvidence:
    """Separate evidence layer; never a trade instruction."""

    model_version: str
    cohort_context: dict[str, Any] = field(default_factory=dict)
    calibrated_failure_probability: float | None = None
    historical_move_distribution: dict[str, Any] = field(default_factory=dict)


class OutcomeModel(Protocol):
    def evidence(self, snapshot: FeatureSnapshot) -> ModelEvidence | None: ...


class NoModel:
    """Phase 1 default: no model exists, so no model evidence is produced."""

    def evidence(self, snapshot: FeatureSnapshot) -> ModelEvidence | None:
        return None


class TrainingNotImplemented(NotImplementedError):
    pass


def train_offline(*_args, **_kwargs):  # pragma: no cover - deliberately unimplemented
    raise TrainingNotImplemented("offline training is Phase 3; only the interfaces exist in this build")
