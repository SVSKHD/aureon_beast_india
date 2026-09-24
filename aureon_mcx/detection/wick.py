"""Rejection wick detection with explicit, configurable rules."""
from __future__ import annotations

from aureon_mcx.indicators.models import IndicatorRow
from aureon_mcx.market.candle import Candle

from .models import Detection, DetectionFamily, Direction

LABEL_UPPER = "WICK · upper rejection"
LABEL_LOWER = "WICK · lower rejection"


class WickDetector:
    def __init__(self, min_wick_range_fraction: float = 0.6, max_body_range_fraction: float = 0.3, min_range_atr: float = 0.8):
        self.min_wick = min_wick_range_fraction
        self.max_body = max_body_range_fraction
        self.min_range_atr = min_range_atr

    def update(self, candle: Candle, ind: IndicatorRow, session: str | None) -> list[Detection]:
        atr = ind.atr
        rng = candle.range
        if atr is None or atr <= 0 or rng <= 0:
            return []  # fail closed
        if rng < self.min_range_atr * atr:
            return []
        body_frac = candle.body / rng
        if body_frac > self.max_body:
            return []
        out: list[Detection] = []
        rules = {
            "min_wick_range_fraction": self.min_wick, "max_body_range_fraction": self.max_body, "min_range_atr": self.min_range_atr,
            "range": rng, "atr": atr, "range_atr": rng / atr, "body_fraction": body_frac,
        }
        upper_frac = candle.upper_wick / rng
        lower_frac = candle.lower_wick / rng
        if upper_frac >= self.min_wick:
            out.append(self._det(candle, session, "UPPER_REJECTION", Direction.BEARISH, LABEL_UPPER, {**rules, "wick_fraction": upper_frac, "wick": "upper"}))
        if lower_frac >= self.min_wick:
            out.append(self._det(candle, session, "LOWER_REJECTION", Direction.BULLISH, LABEL_LOWER, {**rules, "wick_fraction": lower_frac, "wick": "lower"}))
        return out

    @staticmethod
    def _det(candle: Candle, session: str | None, kind: str, direction: Direction, label: str, payload: dict) -> Detection:
        return Detection(symbol=candle.symbol, security_id=candle.security_id, expiry_date=candle.expiry_date, timeframe=candle.timeframe,
                         open_time=candle.open_time, family=DetectionFamily.WICK, kind=kind, direction=direction, price=candle.close,
                         label=label, session=session, payload=payload, candle_id=candle.id)
