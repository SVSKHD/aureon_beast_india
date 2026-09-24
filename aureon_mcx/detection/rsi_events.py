"""Stored RSI zone / momentum transitions (level crosses and direction turns)."""
from __future__ import annotations

from aureon_mcx.indicators.models import IndicatorRow, RsiDirection
from aureon_mcx.market.candle import Candle

from .models import Detection, DetectionFamily, Direction


class RsiEventDetector:
    def __init__(self, levels: list[float] | None = None):
        self.levels = sorted(levels or [30.0, 50.0, 70.0])
        self._prev_rsi: float | None = None
        self._prev_dir: RsiDirection | None = None

    def update(self, candle: Candle, ind: IndicatorRow, session: str | None) -> list[Detection]:
        rsi = ind.rsi
        if rsi is None:
            return []
        prev, self._prev_rsi = self._prev_rsi, rsi
        out: list[Detection] = []
        if prev is not None:
            for lvl in self.levels:
                if prev < lvl <= rsi:
                    out.append(self._det(candle, ind, session, f"CROSS_UP_{int(lvl)}", Direction.BULLISH, f"RSI · cross ↑ {int(lvl)}", prev))
                elif prev > lvl >= rsi:
                    out.append(self._det(candle, ind, session, f"CROSS_DOWN_{int(lvl)}", Direction.BEARISH, f"RSI · cross ↓ {int(lvl)}", prev))
        d = ind.rsi_direction
        if d in (RsiDirection.RISING, RsiDirection.FALLING):
            if self._prev_dir is not None and d != self._prev_dir:
                if d is RsiDirection.RISING:
                    out.append(self._det(candle, ind, session, "TURN_UP", Direction.BULLISH, "RSI · turn up", prev))
                else:
                    out.append(self._det(candle, ind, session, "TURN_DOWN", Direction.BEARISH, "RSI · turn down", prev))
            self._prev_dir = d
        return out

    @staticmethod
    def _det(candle: Candle, ind: IndicatorRow, session: str | None, kind: str, direction: Direction, label: str, prev: float | None) -> Detection:
        return Detection(symbol=candle.symbol, security_id=candle.security_id, expiry_date=candle.expiry_date, timeframe=candle.timeframe,
                         open_time=candle.open_time, family=DetectionFamily.RSI, kind=kind, direction=direction, price=candle.close,
                         label=label, session=session, candle_id=candle.id,
                         payload={"rsi": ind.rsi, "previous_rsi": prev, "rsi_direction": ind.rsi_direction.value if ind.rsi_direction else None})
