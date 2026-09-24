"""Breakout / acceptance / retest / failure detection against confirmed levels."""
from __future__ import annotations

from dataclasses import dataclass, field

from aureon_mcx.indicators.models import IndicatorRow
from aureon_mcx.market.candle import Candle

from .levels import Level, LevelSide
from .models import Detection, DetectionFamily, Direction


@dataclass
class _Episode:
    beyond_closes: int = 0
    accepted: bool = False
    failures: int = 0
    active: bool = False
    retested: bool = False


class BreakoutDetector:
    def __init__(self, acceptance_closes: int = 2, retest_tolerance_atr: float = 0.25):
        self.acceptance_closes = acceptance_closes
        self.retest_tol = retest_tolerance_atr
        self._episodes: dict[str, _Episode] = {}
        self._prev_close: float | None = None

    def update(self, candle: Candle, ind: IndicatorRow, session: str | None, levels: list[Level]) -> list[Detection]:
        out: list[Detection] = []
        atr = ind.atr
        prev_close = self._prev_close
        self._prev_close = candle.close
        if atr is None or atr <= 0 or prev_close is None:
            return []
        for lv in levels:
            if lv.kind not in ("swing_high", "swing_low", "prev_day_high", "prev_day_low", "equal_highs", "equal_lows"):
                continue
            ep = self._episodes.setdefault(lv.key, _Episode())
            bull = lv.side is LevelSide.HIGH
            direction = Direction.BULLISH if bull else Direction.BEARISH
            beyond = candle.close > lv.price if bull else candle.close < lv.price
            was_beyond = prev_close > lv.price if bull else prev_close < lv.price
            touched = (candle.low <= lv.price + self.retest_tol * atr) if bull else (candle.high >= lv.price - self.retest_tol * atr)
            payload = {"level_kind": lv.kind, "level_price": lv.price, "atr": atr, "close": candle.close, "prev_close": prev_close,
                       "acceptance_closes": self.acceptance_closes, "retest_tolerance_atr": self.retest_tol}
            side_txt = "above" if bull else "below"
            if beyond and not was_beyond:
                if ep.active and ep.failures > 0:
                    ep.beyond_closes = 1
                    out.append(self._det(candle, session, "RECLAIM", direction, f"RECLAIM · {side_txt} {lv.display}", {**payload, "failures": ep.failures}))
                else:
                    self._episodes[lv.key] = ep = _Episode(beyond_closes=1, active=True)
                    out.append(self._det(candle, session, "BREAKOUT", direction, f"BREAKOUT · {side_txt} {lv.display}", payload))
                continue
            if beyond and was_beyond and ep.active:
                ep.beyond_closes += 1
                if not ep.accepted and ep.beyond_closes >= self.acceptance_closes:
                    ep.accepted = True
                    out.append(self._det(candle, session, "ACCEPTANCE", direction, f"ACCEPTANCE · {side_txt} {lv.display}",
                                         {**payload, "beyond_closes": ep.beyond_closes}))
                elif ep.accepted and touched and not ep.retested:
                    ep.retested = True
                    out.append(self._det(candle, session, "RETEST", direction, f"RETEST · {lv.display}", payload))
                continue
            if not beyond and was_beyond and ep.active:
                ep.failures += 1
                ep.beyond_closes = 0
                if ep.failures >= 2:
                    out.append(self._det(candle, session, "SECOND_FAILURE", direction.opposite, f"INVALIDATED · second failure {lv.display}",
                                         {**payload, "failures": ep.failures}))
                    self._episodes[lv.key] = _Episode()
                else:
                    out.append(self._det(candle, session, "FAILURE_INSIDE", direction.opposite, f"FAILED · back inside {lv.display}",
                                         {**payload, "failures": ep.failures}))
        return out

    @staticmethod
    def _det(candle: Candle, session: str | None, kind: str, direction: Direction, label: str, payload: dict) -> Detection:
        return Detection(symbol=candle.symbol, security_id=candle.security_id, expiry_date=candle.expiry_date, timeframe=candle.timeframe,
                         open_time=candle.open_time, family=DetectionFamily.BREAKOUT, kind=kind, direction=direction,
                         price=candle.close, label=label, session=session, payload=payload, candle_id=candle.id)
