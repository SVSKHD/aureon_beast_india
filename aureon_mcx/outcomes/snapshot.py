"""Immutable feature snapshot frozen at detection time.

Inputs are only what existed at the detection candle. Nothing learned later is
ever written into the snapshot (storage enforces immutability with a trigger).
"""
from __future__ import annotations

from typing import Any

from aureon_mcx.detection.models import Detection
from aureon_mcx.indicators.models import IndicatorRow
from aureon_mcx.market.candle import Candle

from .models import FeatureSnapshot
from .versions import FEATURE_SCHEMA_VERSION


def build_snapshot(detection: Detection, candle: Candle, ind: IndicatorRow | None, context: dict[str, Any], setup_family: str) -> FeatureSnapshot:
    if detection.id is None or candle.id is None:
        raise ValueError("detection and candle must be persisted before a snapshot is frozen")
    if candle.open_time != detection.open_time:
        raise ValueError("snapshot candle must be the detection candle")
    features = {
        "symbol": detection.symbol, "security_id": detection.security_id, "contract_expiry": detection.expiry_date,
        "timestamp": detection.open_time.isoformat(), "timeframe": detection.timeframe.value, "direction": detection.direction.value,
        "setup_family": setup_family, "detection_kind": detection.kind, "reference_price": detection.price,
        "ema_fast": ind.ema_fast if ind else None, "ema_slow": ind.ema_slow if ind else None, "ema_gap": ind.ema_gap if ind else None,
        "ema_fast_period": ind.ema_fast_period if ind else None, "ema_slow_period": ind.ema_slow_period if ind else None,
        "ema_slope": ind.ema_fast_slope if ind else None, "rsi": ind.rsi if ind else None, "rsi_period": ind.rsi_period if ind else None,
        "rsi_direction": ind.rsi_direction.value if ind and ind.rsi_direction else None, "atr": ind.atr if ind else None,
        "atr_period": ind.atr_period if ind else None,
        "volume": candle.volume, "open_interest": candle.open_interest,
        "present_trend": context.get("present_trend"), "session_trend": context.get("session_trend"), "session": detection.session,
        "mcx_session": context.get("mcx_session"), "mtf_state": context.get("mtf_state"), "mtf_alignment": context.get("mtf_alignment"),
        "mtf_reads": context.get("mtf_reads"), "mtf_consensus": context.get("mtf_consensus"),
        "mtf_higher_available": context.get("mtf_higher_available"), "mtf_early_reversal": context.get("mtf_early_reversal"),
        "mtf_notes": context.get("mtf_notes"),
        "structure_context": context.get("structure_context"), "structure_sequence": context.get("structure_sequence"),
        "last_high_label": context.get("last_high_label"), "last_low_label": context.get("last_low_label"),
        "liquidity_state": context.get("liquidity_state"), "breakout_state": context.get("breakout_state"),
        "wick_state": context.get("wick_state"), "candle": {"open": candle.open, "high": candle.high, "low": candle.low, "close": candle.close},
        "feature_schema_version": FEATURE_SCHEMA_VERSION,
    }
    return FeatureSnapshot(
        detection_id=detection.id, candle_id=candle.id, symbol=detection.symbol, security_id=detection.security_id,
        expiry_date=detection.expiry_date, timeframe=detection.timeframe, open_time=detection.open_time,
        direction=detection.direction.value, setup_family=setup_family, reference_price=detection.price,
        atr=ind.atr if ind else None, rsi=ind.rsi if ind else None, ema_fast=ind.ema_fast if ind else None,
        ema_slow=ind.ema_slow if ind else None, present_trend=context.get("present_trend"), mtf_state=context.get("mtf_state"),
        feature_schema_version=FEATURE_SCHEMA_VERSION, features=features,
    )
