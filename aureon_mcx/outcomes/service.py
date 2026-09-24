"""OutcomeService: freezes snapshots and updates observations from stored closed candles."""
from __future__ import annotations

import logging
from datetime import datetime
from typing import TYPE_CHECKING

from aureon_mcx.config.yaml_models import AnalysisConfig
from aureon_mcx.detection.models import Detection
from aureon_mcx.indicators.models import IndicatorRow
from aureon_mcx.logging_setup import kv
from aureon_mcx.market.candle import Candle
from aureon_mcx.market.sessions import SessionCalendar
from aureon_mcx.setups.models import SetupState

from .models import FeatureSnapshot, OutcomeObservation
from .observation import Horizon, horizons_from_config, observe
from .snapshot import build_snapshot

if TYPE_CHECKING:  # pragma: no cover - avoids storage <-> outcomes import cycle
    from aureon_mcx.storage.repositories import Repositories

log = logging.getLogger("aureon.outcomes")


class OutcomeService:
    def __init__(self, repos: "Repositories", analysis: AnalysisConfig, calendar: SessionCalendar):
        self.repos = repos
        self.cfg = analysis
        self.calendar = calendar
        self.horizons: list[Horizon] = horizons_from_config(analysis.horizons.bars, analysis.horizons.include_session)

    @property
    def max_bars(self) -> int:
        return max(self.cfg.horizons.bars)

    def freeze(self, detection: Detection, candle: Candle, ind: IndicatorRow | None, context: dict, setup_family: str) -> FeatureSnapshot:
        existing = self.repos.snapshots.for_detection(detection.id)  # type: ignore[arg-type]
        if existing is not None:
            return existing
        return self.repos.snapshots.insert(build_snapshot(detection, candle, ind, context, setup_family))

    def update_pending(self, security_id: str, now: datetime, limit: int = 200) -> int:
        """Recompute observations for pending snapshots using only closed candles after the detection candle."""
        updated = 0
        for snap in self.repos.snapshots.pending_outcomes(limit):
            if snap.security_id != security_id:
                continue
            session_date = self.calendar.trading_date(snap.open_time)
            _, session_end = self.calendar.trading_day_span(session_date)
            session_bars = int((session_end - snap.open_time).total_seconds() // snap.timeframe.seconds) + 2
            future = self.repos.candles.after(security_id, snap.timeframe, snap.open_time, max(self.max_bars, session_bars))
            if not future:
                continue
            setup_invalidated = self._setup_invalidated(snap)
            obs = observe(snap, future, self.horizons, self.cfg.outcomes.follow_through_atr, self.cfg.outcomes.invalidation_atr,
                          session_end=session_end, session_closed=self.calendar.trading_day_closed(session_date, now),
                          setup_invalidated=setup_invalidated)
            for o in obs:
                self.repos.outcomes.upsert(o)
            updated += 1
            if obs and all(o.is_final for o in obs) and len(obs) == len(self.horizons):
                self.repos.outcomes.mark_all_final(snap.id, snap.detection_id, obs[-1].last_candle_open_time)  # type: ignore[arg-type]
                log.info("outcome_final %s", kv(snapshot_id=snap.id, detection_id=snap.detection_id, labels=",".join(o.label for o in obs)))
        return updated

    def _setup_invalidated(self, snap: FeatureSnapshot) -> bool:
        row = self.repos.db.query_one("SELECT state FROM setups WHERE origin_detection_id = ?", (snap.detection_id,))
        return bool(row and row["state"] == SetupState.INVALIDATED.value)

    def observations_for_detection(self, detection_id: int) -> list[OutcomeObservation]:
        snap = self.repos.snapshots.for_detection(detection_id)
        return self.repos.outcomes.for_snapshot(snap.id) if snap and snap.id else []
