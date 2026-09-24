"""Closed-candle observer: the one place the pipeline is orchestrated.

    candle -> indicators -> detections -> structure + MTF -> lifecycle
           -> confirmation / clearance -> storage -> presentation sink

Runs ONLY on closed candles, exactly once per closed candle per timeframe.
Discord (the sink) receives finished SetupView objects; it never computes.
"""
from __future__ import annotations

import logging
from collections import deque
from datetime import date, datetime
from typing import Protocol

from aureon_mcx.broker.dhan.symbol_resolver import ResolvedContract
from aureon_mcx.config import AppConfig
from aureon_mcx.confirmation import ConfirmationInputs, evaluate_clearance
from aureon_mcx.detection import (BreakoutDetector, Detection, DetectionFamily, Direction, EmaDetector, LiquidityDetector,
                                  RsiEventDetector, WickDetector, build_levels)
from aureon_mcx.health import HealthState
from aureon_mcx.indicators import IndicatorEngine, IndicatorParams, IndicatorRow
from aureon_mcx.logging_setup import kv
from aureon_mcx.market.candle import Candle
from aureon_mcx.market.sessions import SessionCalendar
from aureon_mcx.market.timeframe import Timeframe
from aureon_mcx.market.timeutil import fmt_ist
from aureon_mcx.mtf import MtfAssessment, SessionTrendTracker, TimeframeRead, TrendDirection, assess_mtf, classify_timeframe, present_trend
from aureon_mcx.outcomes import OutcomeService, cohort_stats
from aureon_mcx.setups import LifecycleParams, Setup, SetupEvent, SetupState, SetupTracker, create_setup, family_for
from aureon_mcx.storage.repositories import Repositories
from aureon_mcx.structure import StructureContext
from aureon_mcx.structure.engine import StructureEngine
from aureon_mcx.views import SetupView

log = logging.getLogger("aureon.observer")


class PresentationSink(Protocol):
    def publish(self, view: SetupView) -> None: ...
    def status(self, text: str) -> None: ...


class NullSink:
    def __init__(self):
        self.views: list[SetupView] = []
        self.statuses: list[str] = []

    def publish(self, view: SetupView) -> None:
        self.views.append(view)

    def status(self, text: str) -> None:
        self.statuses.append(text)


class SymbolObserver:
    def __init__(self, cfg: AppConfig, contract: ResolvedContract, repos: Repositories, calendar: SessionCalendar,
                 outcomes: OutcomeService, sink: PresentationSink, health: HealthState | None = None):
        self.cfg = cfg
        self.contract = contract
        self.repos = repos
        self.calendar = calendar
        self.outcomes = outcomes
        self.sink = sink
        self.health = health or HealthState()
        self.symbol = contract.logical_symbol
        self.security_id = contract.security_id
        self.expiry = contract.expiry_iso
        self.primary = cfg.primary_timeframe
        self.timeframes = cfg.mtf_timeframes
        a = cfg.analysis
        params = IndicatorParams(cfg.env.EMA_FAST, cfg.env.EMA_SLOW, cfg.env.RSI_PERIOD, cfg.env.ATR_PERIOD, a.rsi_events.flat_tolerance)
        self.indicators: dict[Timeframe, IndicatorEngine] = {tf: IndicatorEngine(params) for tf in self.timeframes}
        self.structures: dict[Timeframe, StructureEngine] = {
            tf: StructureEngine(cfg.env.SWING_STRENGTH, a.structure.equal_tolerance_mode, a.structure.equal_tolerance_value,
                                a.structure.dominance_lookback, contract.tick_size) for tf in self.timeframes}
        self.candles: dict[Timeframe, deque[Candle]] = {tf: deque(maxlen=max(a.declutter.chart_bars, 200)) for tf in self.timeframes}
        self.reads: dict[Timeframe, TimeframeRead] = {tf: TimeframeRead(tf, TrendDirection.UNAVAILABLE) for tf in self.timeframes}
        self.last_open_time: dict[Timeframe, datetime] = {}
        self.ema = EmaDetector(a.ema_detection.approach_atr_fraction, a.ema_detection.min_shrinking_bars)
        self.wick = WickDetector(a.wick.min_wick_range_fraction, a.wick.max_body_range_fraction, a.wick.min_range_atr)
        self.liquidity = LiquidityDetector(a.liquidity.proximity_atr_fraction)
        self.breakout = BreakoutDetector(a.breakout.acceptance_closes, a.breakout.retest_tolerance_atr)
        self.rsi_events = RsiEventDetector(a.rsi_events.levels)
        self.tracker = SetupTracker(LifecycleParams(a.breakout.acceptance_closes, a.breakout.retest_tolerance_atr,
                                                    a.breakout.completion_bars, a.breakout.max_age_bars))
        self.sessions = SessionTrendTracker(calendar, cfg.sessions.trend.min_move_atr)
        self.present = TrendDirection.UNAVAILABLE
        self.mtf_by_setup: dict[int, MtfAssessment] = {}
        self.latest_early_ema: str | None = None
        self.latest_cross: str | None = None
        self.current_trading_date: date | None = None
        self.published_hashes: dict[int, str] = {}
        self.closed_count = 0
        self._cohort_cache: dict[tuple, tuple[datetime, list[str] | None]] = {}

    # ------------------------------------------------------------------ helpers
    def _structure_ctx(self, tf: Timeframe) -> StructureContext:
        return self.structures[tf].context()

    def _update_read(self, tf: Timeframe) -> None:
        ind = self.indicators[tf].latest
        self.reads[tf] = classify_timeframe(tf, ind, self._structure_ctx(tf) if ind is not None else None,
                                            self.cfg.policy.rsi_threshold, self.cfg.analysis.mtf.require_structure)

    def _session_name(self, ts: datetime) -> str | None:
        return self.calendar.session_name(ts) or self.calendar.mcx_session_name(ts)

    def _persist_pivots(self, tf: Timeframe, confirmed, candle: Candle) -> None:
        for p in confirmed:
            pivot_candle = self.repos.candles.find(self.security_id, tf, p.pivot_open_time)
            if pivot_candle is None or pivot_candle.id is None:
                continue
            self.repos.pivots.insert(type(p)(**{**p.__dict__, "candle_id": pivot_candle.id, "confirmed_candle_id": candle.id}))

    def _session_levels(self) -> tuple[float | None, float | None]:
        row = self.sessions.current("global")
        if row is None or row.bars == 0:
            return None, None
        return row.high, row.low

    def _prev_day_levels(self, trading_date: date) -> tuple[float | None, float | None]:
        prev = self.repos.day_frames.previous_closed(self.security_id, self.primary, trading_date)
        if prev is None:
            return None, None
        return prev["high"], prev["low"]

    # ------------------------------------------------------------ entry point
    def on_closed_candle(self, candle: Candle, publish: bool = True) -> None:
        if not candle.is_closed:
            raise ValueError("observer only accepts closed candles")
        tf = candle.timeframe
        if tf not in self.timeframes:
            return
        last = self.last_open_time.get(tf)
        if last is not None and candle.open_time <= last:
            return  # already processed (idempotent, once per closed candle)
        stored = self.repos.candles.insert(candle)
        self.last_open_time[tf] = stored.open_time
        self.candles[tf].append(stored)
        self.health.candle_seen(f"{self.symbol}/{tf.value}", stored.open_time)
        ind = self.repos.indicators.insert(self.indicators[tf].update(stored))
        upd = self.structures[tf].update(stored, ind.atr)
        self._persist_pivots(tf, upd.confirmed, stored)
        self._update_read(tf)
        if tf == self.primary:
            self._on_primary(stored, ind, upd, publish)
        else:
            self._on_higher(stored, publish)

    # ------------------------------------------------------------ higher tfs
    def _on_higher(self, candle: Candle, publish: bool) -> None:
        log.debug("higher_tf_closed %s", kv(symbol=self.symbol, tf=candle.timeframe.value, open_time=fmt_ist(candle.open_time)))
        if not publish or not self.candles[self.primary]:
            return
        last_primary = self.candles[self.primary][-1]
        ind = self.indicators[self.primary].latest
        if ind is None:
            return
        # MTF changed: re-evaluate clearance for open setups (no lifecycle evaluation on a higher-tf bar)
        for setup in self.repos.setups.open_for(self.security_id, self.primary):
            self._clear_and_publish(setup, last_primary, ind, publish)

    # ------------------------------------------------------------- primary
    def _on_primary(self, candle: Candle, ind: IndicatorRow, upd, publish: bool) -> None:
        self.closed_count += 1
        tf = self.primary
        trading_date = self.calendar.trading_date(candle.open_time)
        if self.current_trading_date is not None and trading_date != self.current_trading_date:
            self.repos.day_frames.close_frame(self.security_id, tf, self.current_trading_date)
        self.current_trading_date = trading_date
        session = self._session_name(candle.open_time)
        structure = self.structures[tf]
        # levels from CONFIRMED inputs only (pivots confirmed by now, closed day frames, current session prior bars)
        pdh, pdl = self._prev_day_levels(trading_date)
        sh, sl = self._session_levels()
        tol = self.cfg.analysis.liquidity.equal_level_tolerance_atr * (ind.atr or 0.0)
        levels = build_levels(structure.all_pivots, pdh, pdl, sh, sl, tol)
        # day frame + session trend
        self.repos.day_frames.upsert(self.symbol, self.security_id, self.expiry, tf, trading_date, candle,
                                     self.calendar.trading_day_closed(trading_date, candle.close_time),
                                     structure.context().value, structure.sequence())
        for row in self.sessions.update(candle, ind):
            self.repos.session_state.upsert(row.to_record(self.symbol, self.security_id))
        # detections
        detections: list[Detection] = []
        detections += self.ema.update(candle, ind, session)
        detections += self.wick.update(candle, ind, session)
        detections += self.liquidity.update(candle, ind, session, levels)
        detections += self.breakout.update(candle, ind, session, levels)
        detections += self.rsi_events.update(candle, ind, session)
        if upd.context_changed and upd.context is not StructureContext.MIXED:
            direction = Direction.BULLISH if upd.context is StructureContext.BULLISH else Direction.BEARISH
            detections.append(Detection(self.symbol, self.security_id, self.expiry, tf, candle.open_time, DetectionFamily.STRUCTURE,
                                        f"CONTEXT_{upd.context.value}", direction, candle.close, f"STRUCTURE · {upd.context.value}",
                                        session, {"sequence": upd.sequence, "previous": upd.previous_context.value}))
        stored_dets = []
        for d in detections:
            d.candle_id = candle.id
            stored_dets.append(self.repos.detections.insert(d))
            if d.family is DetectionFamily.EMA:
                if d.is_ema_cross:
                    self.latest_cross = f"{d.label} @ {fmt_ist(d.open_time, '%d %b %H:%M')}"
                    self.latest_early_ema = None
                else:
                    self.latest_early_ema = f"{d.label} @ {fmt_ist(d.open_time, '%d %b %H:%M')}"
            log.info("detection %s", kv(symbol=self.symbol, tf=tf.value, kind=d.kind, direction=d.direction.value, price=d.price,
                                        open_time=fmt_ist(d.open_time), session=session))
        # context
        self.present = present_trend(list(self.candles[tf]), ind, structure.context(), self.cfg.sessions.trend.present_lookback_bars,
                                     self.cfg.sessions.trend.min_move_atr).direction
        # lifecycle for existing setups
        open_setups = self.repos.setups.open_for(self.security_id, tf)
        for setup in open_setups:
            for t in self.tracker.evaluate(setup, candle, ind):
                self.repos.setups.update_state(setup)
                self.repos.setups.add_event(SetupEvent(setup.id, candle.id, t.from_state, t.to_state, t.reason, t.evidence, candle.open_time))
                log.info("setup_transition %s", kv(symbol=self.symbol, setup_id=setup.id, from_state=t.from_state.value,
                                                  to_state=t.to_state.value, reason=t.reason, open_time=fmt_ist(candle.open_time)))
        # new setups from this candle's detections (evaluated from the next candle)
        for d in stored_dets:
            created = create_setup(d, candle, ind)
            if created is None:
                continue
            setup, t = created
            if any(s.family == setup.family and s.direction == setup.direction and abs(s.anchor_price - setup.anchor_price) <= 1e-9
                   for s in open_setups):
                continue
            setup.state = t.to_state
            self.repos.setups.insert(setup)
            self.repos.setups.add_event(SetupEvent(setup.id, candle.id, t.from_state, t.to_state, t.reason, t.evidence, candle.open_time, d.id))
            open_setups.append(setup)
            log.info("setup_created %s", kv(symbol=self.symbol, setup_id=setup.id, family=setup.family, direction=setup.direction.value,
                                            anchor=setup.anchor_price, invalidation=setup.invalidation_price, origin=d.kind))
        # feature snapshots (immutable) for meaningful detections
        snap_ctx = self._snapshot_context(structure)
        for d in stored_dets:
            if d.direction is Direction.NEUTRAL:
                continue
            self.outcomes.freeze(d, candle, ind, snap_ctx, family_for(d) or d.family.value)
        # clearance + presentation
        for setup in open_setups:
            self._clear_and_publish(setup, candle, ind, publish)
        # outcomes + symbol state
        self.outcomes.update_pending(self.security_id, now=candle.close_time)
        self.repos.symbol_state.upsert(self.symbol, self.security_id, self.expiry, self.present.value,
                                       {tf_.value: r.to_dict() for tf_, r in self.reads.items()}, structure.state_dict(), candle.open_time)

    def _snapshot_context(self, structure: StructureEngine) -> dict:
        lh, ll = structure.last_high(), structure.last_low()
        cur = self.sessions.current("global")
        mcx = self.sessions.current("mcx")
        return {"present_trend": self.present.value, "session_trend": cur.trend.value if cur else None,
                "mcx_session": mcx.session_name if mcx else None, "mtf_reads": {tf.value: r.direction.value for tf, r in self.reads.items()},
                "mtf_state": None, "structure_context": structure.context().value, "structure_sequence": structure.sequence(),
                "last_high_label": lh.label.value if lh else None, "last_low_label": ll.label.value if ll else None,
                "liquidity_state": self.latest_state(DetectionFamily.LIQUIDITY), "breakout_state": self.latest_state(DetectionFamily.BREAKOUT),
                "wick_state": self.latest_state(DetectionFamily.WICK)}

    def latest_state(self, family: DetectionFamily) -> str | None:
        dets = [d for d in self.repos.detections.latest(self.security_id, self.primary, 40) if d.family is family]
        return dets[-1].kind if dets else None

    # -------------------------------------------------------------- clearance
    def _clear_and_publish(self, setup: Setup, candle: Candle, ind: IndicatorRow, publish: bool) -> None:
        mtf = assess_mtf(self.reads, setup.direction, self.primary)
        self.mtf_by_setup[setup.id] = mtf
        structure = self.structures[self.primary]
        cur = self.sessions.current("global")
        inputs = ConfirmationInputs(setup=setup, candle=candle, indicators=ind, present_trend=self.present, mtf=mtf,
                                    structure=structure.context(), structure_sequence=structure.sequence(),
                                    session_trend=cur.trend if cur else None, h1_direction=self.reads.get(Timeframe.H1, TimeframeRead(Timeframe.H1, TrendDirection.UNAVAILABLE)).direction)
        clearance = evaluate_clearance(inputs, self.cfg.policy)
        clearance.setup_id, clearance.candle_id = setup.id, candle.id
        self.repos.clearances.insert(clearance)
        if setup.state.is_terminal:
            setup.closed_at = setup.closed_at or candle.close_time
            self.repos.setups.update_state(setup)
        view = self.build_view(setup, candle, ind, mtf, clearance)
        if publish:
            h = view.state_hash()
            if self.published_hashes.get(setup.id) != h:
                self.published_hashes[setup.id] = h
                self.sink.publish(view)
                log.info("clearance %s", kv(symbol=self.symbol, setup_id=setup.id, state=setup.state.value, cleared=clearance.cleared,
                                            blockers=len(clearance.blockers), mtf=clearance.mtf_state, policy=clearance.policy_version))

    def build_view(self, setup: Setup, candle: Candle, ind: IndicatorRow, mtf: MtfAssessment, clearance) -> SetupView:
        structure = self.structures[self.primary]
        events = [{"id": e.id, "to_state": e.to_state.value, "reason": e.reason, "open_time": e.open_time}
                  for e in self.repos.setups.events(setup.id)]
        refs = [f"detection #{setup.origin_detection_id} · {setup.context.get('origin_label', '')}"]
        refs += [f"event #{e['id']} · {e['to_state']} · {fmt_ist(e['open_time'], '%d %b %H:%M')}" for e in events[-5:]]
        cohort_lines = self._cohort_lines(setup.family, setup.direction.value, candle.open_time)
        ema_rel = "n/a"
        if ind.ema_fast is not None and ind.ema_slow is not None:
            ema_rel = "EMA20 > EMA50" if ind.ema_fast > ind.ema_slow else ("EMA20 < EMA50" if ind.ema_fast < ind.ema_slow else "EMA20 = EMA50")
        ctx_lines = [f"origin: {setup.context.get('origin_label', setup.family)}"]
        if setup.context.get("level_kind"):
            ctx_lines.append(f"level: {str(setup.context['level_kind']).replace('_', ' ')}")
        ctx_lines.append(f"session: {self._session_name(candle.open_time) or 'none'} · MCX {self.calendar.mcx_session_name(candle.open_time) or 'closed'}")
        return SetupView(
            setup_id=setup.id, symbol=self.symbol, display_symbol=self.contract.display_symbol, security_id=self.security_id,
            expiry_date=self.expiry, timeframe=self.primary.value, family=setup.family, direction=setup.direction.value,
            state=setup.state.value, anchor_price=setup.anchor_price, invalidation_price=setup.invalidation_price,
            origin_label=setup.context.get("origin_label", setup.family), origin_detection_id=setup.origin_detection_id,
            created_open_time=setup.created_open_time, updated_open_time=candle.open_time, last_price=candle.close,
            context_lines=ctx_lines, events=events, present_trend=self.present.value, session_rows=self.sessions.display_rows(),
            mtf_rows=mtf.display_rows(), mtf_alignment=mtf.alignment.value, mtf_notes=list(mtf.notes), early_reversal=mtf.early_reversal,
            structure_context=structure.context().value, structure_sequence=structure.sequence(), ema_fast=ind.ema_fast,
            ema_slow=ind.ema_slow, ema_relation=ema_rel, early_ema=self.latest_early_ema, latest_cross=self.latest_cross, rsi=ind.rsi,
            rsi_direction=ind.rsi_direction.value if ind.rsi_direction else None, atr=ind.atr, volume=ind.volume,
            open_interest=ind.open_interest, badges=list(clearance.badges), warnings=list(clearance.evidence.get("warnings", [])),
            fakeout_flags=list(clearance.fakeout_flags), blockers=list(clearance.blockers), cleared=clearance.cleared,
            policy_version=clearance.policy_version, detection_refs=refs, cohort_lines=cohort_lines, is_terminal=setup.state.is_terminal,
        )

    def _cohort_lines(self, family: str, direction: str, open_time: datetime) -> list[str] | None:
        key = (family, direction)
        cached = self._cohort_cache.get(key)
        if cached is not None and cached[0] == open_time:
            return cached[1]
        lines = None
        snaps = self.repos.snapshots.cohort(self.symbol, family, direction, self.primary)
        if snaps:
            horizon = f"{self.cfg.analysis.horizons.bars[-1]}b"
            obs = self.repos.outcomes.cohort([s.id for s in snaps if s.id], horizon)
            stats = cohort_stats(obs, horizon, self.cfg.analysis.outcomes.min_cohort_sample)
            lines = stats.lines() if stats else None
        self._cohort_cache[key] = (open_time, lines)
        return lines

    # ---------------------------------------------------------------- warmup
    def warm(self, history: dict[Timeframe, list[Candle]]) -> dict[Timeframe, int]:
        """Replay stored closed candles chronologically (higher timeframes first on ties). No publishing."""
        merged: list[Candle] = []
        for tf, cs in history.items():
            merged += [c for c in cs if c.is_closed and tf in self.timeframes]
        merged.sort(key=lambda c: (c.close_time, -c.timeframe.rank))
        counts: dict[Timeframe, int] = {tf: 0 for tf in self.timeframes}
        for c in merged:
            self.on_closed_candle(c, publish=False)
            counts[c.timeframe] += 1
        for tf, n in counts.items():
            log.info("historical_warmup %s %s bars=%d", self.symbol, tf.value, n)
        return counts
