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
        self._bars_final: set[int] = set()
        self._session_seen: set[int] = set()

    @property
    def max_bars(self) -> int:
        return max(self.cfg.horizons.bars)

    def freeze(self, detection: Detection, candle: Candle, ind: IndicatorRow | None, context: dict, setup_family: str) -> FeatureSnapshot:
        existing = self.repos.snapshots.for_detection(detection.id)  # type: ignore[arg-type]
        if existing is not None:
            return existing
        return self.repos.snapshots.insert(build_snapshot(detection, candle, ind, context, setup_family))

    def update_pending(self, security_id: str, now: datetime, limit: int = 500) -> int:
        """Incrementally recompute observations for pending snapshots.

        Bar horizons are recomputed only while the snapshot is younger than the
        longest bar horizon; the session horizon is computed once as pending and
        finalised when the trading day closes. Inputs are only CLOSED candles
        strictly after the detection candle.
        """
        updated = 0
        for snap in self.repos.snapshots.pending_outcomes(limit):
            if snap.security_id != security_id or snap.id is None:
                continue
            session_date = self.calendar.trading_date(snap.open_time)
            _, session_end = self.calendar.trading_day_span(session_date)
            day_closed = self.calendar.trading_day_closed(session_date, now)
            bars_final = snap.id in self._bars_final
            session_seen = snap.id in self._session_seen
            if bars_final and session_seen and not day_closed:
                continue
            need_session = day_closed or not session_seen
            if need_session:
                session_bars = int((session_end - snap.open_time).total_seconds() // snap.timeframe.seconds) + 2
                future = self.repos.candles.after(security_id, snap.timeframe, snap.open_time, max(self.max_bars, session_bars))
            else:
                future = self.repos.candles.after(security_id, snap.timeframe, snap.open_time, self.max_bars)
            if not future:
                continue
            horizons = [h for h in self.horizons if (h.bars is not None and not bars_final) or (h.bars is None and need_session)]
            obs = observe(snap, future, horizons, self.cfg.outcomes.follow_through_atr, self.cfg.outcomes.invalidation_atr,
                          session_end=session_end, session_closed=day_closed, setup_invalidated=self._setup_invalidated(snap))
            for o in obs:
                self.repos.outcomes.upsert(o)
            updated += 1
            bar_obs = [o for o in obs if o.horizon != "session"]
            if not bars_final and bar_obs and all(o.is_final for o in bar_obs) and len(bar_obs) == len(self.cfg.horizons.bars):
                self._bars_final.add(snap.id)
            if need_session:
                self._session_seen.add(snap.id)
            session_obs = [o for o in obs if o.horizon == "session"]
            session_done = (not self.cfg.horizons.include_session) or (bool(session_obs) and session_obs[0].is_final)
            if snap.id in self._bars_final and session_done:
                self.repos.outcomes.mark_all_final(snap.id, snap.detection_id, future[-1].open_time)
                self._bars_final.discard(snap.id)
                self._session_seen.discard(snap.id)
                log.info("outcome_final %s", kv(snapshot_id=snap.id, detection_id=snap.detection_id,
                                                labels=",".join(o.label for o in self.repos.outcomes.for_snapshot(snap.id))))
        return updated

    def _setup_invalidated(self, snap: FeatureSnapshot) -> bool:
        row = self.repos.db.query_one("SELECT state FROM setups WHERE origin_detection_id = ?", (snap.detection_id,))
        return bool(row and row["state"] == SetupState.INVALIDATED.value)

    def observations_for_detection(self, detection_id: int) -> list[OutcomeObservation]:
        snap = self.repos.snapshots.for_detection(detection_id)
        return self.repos.outcomes.for_snapshot(snap.id) if snap and snap.id else []
