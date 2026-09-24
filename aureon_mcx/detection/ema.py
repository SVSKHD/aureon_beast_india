"""Early EMA approach and actual EMA cross detection.

EARLY approach and ACTUAL cross are different events; an approach is never
labelled as a cross. Thresholds are configurable to avoid firing on wobble.
"""
from __future__ import annotations

from aureon_mcx.indicators.models import IndicatorRow
from aureon_mcx.market.candle import Candle

from .models import Detection, DetectionFamily, Direction

LABEL_EARLY_BULLISH = "◐ EARLY BULLISH"
LABEL_EARLY_BEARISH = "◐ EARLY BEARISH"
LABEL_BULL_CROSS = "▲ BULL CROSS · {session}"
LABEL_BEAR_CROSS = "▼ BEAR CROSS · {session}"


class EmaDetector:
    def __init__(self, approach_atr_fraction: float = 0.5, min_shrinking_bars: int = 2):
        self.approach_atr_fraction = approach_atr_fraction
        self.min_shrinking_bars = min_shrinking_bars
        self._prev_gap: float | None = None
        self._streak = 0
        self._early_fired = False  # once per approach episode

    def update(self, candle: Candle, ind: IndicatorRow, session: str | None) -> list[Detection]:
        gap = ind.ema_gap
        if gap is None or ind.ema_fast is None or ind.ema_slow is None:
            return []
        prev = self._prev_gap
        self._prev_gap = gap
        if prev is None:
            return []
        out: list[Detection] = []
        base = dict(
            ema20=ind.ema_fast, ema50=ind.ema_slow, gap=gap, previous_gap=prev, rsi=ind.rsi, atr=ind.atr,
            timeframe=candle.timeframe.value, symbol=candle.symbol, security_id=candle.security_id,
            timestamp=candle.open_time.isoformat(), price=candle.close, session=session,
        )
        sess = session or "NO SESSION"
        # -- actual cross ---------------------------------------------------
        if prev <= 0 < gap:
            self._streak, self._early_fired = 0, False
            return [self._det(candle, ind, session, "BULL_CROSS", Direction.BULLISH, LABEL_BULL_CROSS.format(session=sess), base)]
        if prev >= 0 > gap:
            self._streak, self._early_fired = 0, False
            return [self._det(candle, ind, session, "BEAR_CROSS", Direction.BEARISH, LABEL_BEAR_CROSS.format(session=sess), base)]
        # -- early approach -------------------------------------------------
        shrinking = abs(gap) < abs(prev)
        if shrinking:
            self._streak += 1
        else:
            self._streak, self._early_fired = 0, False
        if ind.atr is None or ind.atr <= 0:
            return out  # fail closed: no ATR, no "approaching" judgement
        within = abs(gap) <= self.approach_atr_fraction * ind.atr
        if shrinking and self._streak >= self.min_shrinking_bars and within and not self._early_fired:
            self._early_fired = True
            payload = {**base, "shrinking_bars": self._streak, "approach_atr_fraction": self.approach_atr_fraction}
            if gap < 0:  # EMA20 below EMA50, approaching from below
                out.append(self._det(candle, ind, session, "EARLY_BULLISH", Direction.BULLISH, LABEL_EARLY_BULLISH, payload))
            elif gap > 0:
                out.append(self._det(candle, ind, session, "EARLY_BEARISH", Direction.BEARISH, LABEL_EARLY_BEARISH, payload))
        return out

    @staticmethod
    def _det(candle: Candle, ind: IndicatorRow, session: str | None, kind: str, direction: Direction, label: str, payload: dict) -> Detection:
        return Detection(symbol=candle.symbol, security_id=candle.security_id, expiry_date=candle.expiry_date, timeframe=candle.timeframe,
                         open_time=candle.open_time, family=DetectionFamily.EMA, kind=kind, direction=direction, price=candle.close,
                         label=label, session=session, payload=payload, candle_id=candle.id)
