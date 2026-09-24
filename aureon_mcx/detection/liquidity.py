"""Liquidity interaction with CONFIRMED levels: proximity, sweep, reclaim.

Every event is an observation only. A bullish reaction is NOT a bullish setup.
"""
from __future__ import annotations

from aureon_mcx.indicators.models import IndicatorRow
from aureon_mcx.market.candle import Candle

from .levels import Level, LevelSide
from .models import Detection, DetectionFamily, Direction


class LiquidityDetector:
    def __init__(self, proximity_atr_fraction: float = 0.25):
        self.prox = proximity_atr_fraction
        self._prev_close: float | None = None

    def update(self, candle: Candle, ind: IndicatorRow, session: str | None, levels: list[Level]) -> list[Detection]:
        atr = ind.atr
        prev_close = self._prev_close
        self._prev_close = candle.close
        if atr is None or atr <= 0:
            return []
        out: list[Detection] = []
        seen: set[str] = set()
        for lv in levels:
            if lv.key in seen:
                continue
            seen.add(lv.key)
            payload = {"level_kind": lv.kind, "level_price": lv.price, "atr": atr, "close": candle.close,
                       "proximity_atr_fraction": self.prox}
            if lv.side is LevelSide.HIGH:
                # reclaim (price was already beyond and closes back) takes precedence over sweep
                if prev_close is not None and prev_close > lv.price and candle.close <= lv.price:
                    out.append(self._det(candle, session, "RECLAIM", Direction.BEARISH, f"LIQUIDITY · reclaim {lv.display}", payload))
                elif candle.high > lv.price and candle.close <= lv.price:
                    out.append(self._det(candle, session, "SWEEP", Direction.BEARISH, f"LIQUIDITY · sweep {lv.display}", payload))
                elif candle.close < lv.price and (lv.price - candle.close) <= self.prox * atr and candle.high <= lv.price:
                    out.append(self._det(candle, session, "PROXIMITY", Direction.NEUTRAL, f"LIQUIDITY · near {lv.display}",
                                         {**payload, "distance": lv.price - candle.close}))
            else:
                if prev_close is not None and prev_close < lv.price and candle.close >= lv.price:
                    out.append(self._det(candle, session, "RECLAIM", Direction.BULLISH, f"LIQUIDITY · reclaim {lv.display}", payload))
                elif candle.low < lv.price and candle.close >= lv.price:
                    out.append(self._det(candle, session, "SWEEP", Direction.BULLISH, f"LIQUIDITY · sweep {lv.display}", payload))
                elif candle.close > lv.price and (candle.close - lv.price) <= self.prox * atr and candle.low >= lv.price:
                    out.append(self._det(candle, session, "PROXIMITY", Direction.NEUTRAL, f"LIQUIDITY · near {lv.display}",
                                         {**payload, "distance": candle.close - lv.price}))
        return out

    @staticmethod
    def _det(candle: Candle, session: str | None, kind: str, direction: Direction, label: str, payload: dict) -> Detection:
        return Detection(symbol=candle.symbol, security_id=candle.security_id, expiry_date=candle.expiry_date, timeframe=candle.timeframe,
                         open_time=candle.open_time, family=DetectionFamily.LIQUIDITY, kind=kind, direction=direction,
                         price=candle.close, label=label, session=session, payload=payload, candle_id=candle.id)
