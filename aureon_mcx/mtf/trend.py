"""Present trend and per-session trend from closed primary-timeframe candles."""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime

from aureon_mcx.indicators.models import IndicatorRow
from aureon_mcx.market.candle import Candle
from aureon_mcx.market.sessions import SessionCalendar, SessionSpan
from aureon_mcx.structure.models import StructureContext

from .context import TrendDirection


@dataclass(frozen=True)
class TrendRead:
    direction: TrendDirection
    evidence: dict


def present_trend(candles: list[Candle], ind: IndicatorRow | None, structure: StructureContext | None,
                  lookback_bars: int = 6, min_move_atr: float = 0.25) -> TrendRead:
    if ind is None or not ind.warmed or len(candles) <= lookback_bars:
        return TrendRead(TrendDirection.UNAVAILABLE, {"reason": "insufficient closed-bar evidence"})
    move = candles[-1].close - candles[-1 - lookback_bars].close
    threshold = min_move_atr * ind.atr
    ev = {"move": move, "threshold": threshold, "ema_fast": ind.ema_fast, "ema_slow": ind.ema_slow, "structure": structure.value if structure else None,
          "lookback_bars": lookback_bars}
    if ind.ema_fast > ind.ema_slow and move >= threshold and structure is not StructureContext.BEARISH:
        return TrendRead(TrendDirection.BULLISH, ev)
    if ind.ema_fast < ind.ema_slow and move <= -threshold and structure is not StructureContext.BULLISH:
        return TrendRead(TrendDirection.BEARISH, ev)
    return TrendRead(TrendDirection.SIDEWAYS, ev)


@dataclass
class SessionTrendRow:
    session_date: date
    session_name: str
    session_group: str
    trend: TrendDirection
    is_current: bool
    open: float
    high: float
    low: float
    close: float
    bars: int
    opened_at: datetime
    closed_at: datetime | None
    last_open_time: datetime
    evidence: dict = field(default_factory=dict)

    @property
    def key(self) -> tuple[str, str]:
        return (self.session_date.isoformat(), self.session_name)

    def to_record(self, symbol: str, security_id: str) -> dict:
        import json

        return {"symbol": symbol, "security_id": security_id, "session_date": self.session_date.isoformat(),
                "session_name": self.session_name, "session_group": self.session_group, "trend": self.trend.value,
                "is_current": int(self.is_current), "open": self.open, "high": self.high, "low": self.low, "close": self.close,
                "bars": self.bars, "opened_at": self.opened_at.isoformat(), "closed_at": self.closed_at.isoformat() if self.closed_at else None,
                "last_open_time": self.last_open_time.isoformat(), "evidence_json": json.dumps(self.evidence, default=str)}


class SessionTrendTracker:
    """Tracks OHLC + trend per (session_date, session_name) from closed candles.

    A completed session is never shown as current: `is_current` flips off and
    `closed_at` is set as soon as a candle from a later window arrives or the
    window end passes.
    """

    def __init__(self, calendar: SessionCalendar, min_move_atr: float = 0.25):
        self.calendar = calendar
        self.min_move_atr = min_move_atr
        self.rows: dict[tuple[str, str], SessionTrendRow] = {}
        self._spans: dict[tuple[str, str], SessionSpan] = {}

    def _trend(self, row: SessionTrendRow, ind: IndicatorRow | None) -> TrendDirection:
        if ind is None or not ind.warmed:
            return TrendDirection.UNAVAILABLE
        move = row.close - row.open
        thr = self.min_move_atr * ind.atr
        if move >= thr and ind.ema_fast > ind.ema_slow:
            return TrendDirection.BULLISH
        if move <= -thr and ind.ema_fast < ind.ema_slow:
            return TrendDirection.BEARISH
        return TrendDirection.SIDEWAYS

    def update(self, candle: Candle, ind: IndicatorRow | None) -> list[SessionTrendRow]:
        """Returns rows changed by this candle (current ones plus any just closed)."""
        changed: list[SessionTrendRow] = []
        now = candle.close_time
        # close stale sessions whose window has ended
        for key, row in list(self.rows.items()):
            span = self._spans[key]
            if row.is_current and now >= span.end:
                row.is_current = False
                row.closed_at = span.end
                changed.append(row)
        for group in ("global", "mcx"):
            span = self.calendar.session_for(candle.open_time, group)
            if span is None:
                continue
            key = (span.session_date.isoformat(), span.name)
            row = self.rows.get(key)
            if row is None:
                row = SessionTrendRow(span.session_date, span.name, group, TrendDirection.UNAVAILABLE, True, candle.open, candle.high,
                                      candle.low, candle.close, 0, span.start, None, candle.open_time)
                self.rows[key] = row
                self._spans[key] = span
            else:
                row.high = max(row.high, candle.high)
                row.low = min(row.low, candle.low)
                row.close = candle.close
            row.bars += 1
            row.last_open_time = candle.open_time
            row.trend = self._trend(row, ind)
            row.evidence = {"open": row.open, "close": row.close, "atr": ind.atr if ind else None,
                            "ema_fast": ind.ema_fast if ind else None, "ema_slow": ind.ema_slow if ind else None}
            if now >= span.end:
                row.is_current = False
                row.closed_at = span.end
            else:
                row.is_current = True
            if row not in changed:
                changed.append(row)
        # prune old rows (keep last 3 days)
        keys = sorted(self.rows)
        for key in keys[:-12] if len(keys) > 12 else []:
            self.rows.pop(key, None)
            self._spans.pop(key, None)
        return changed

    def current(self, group: str = "global") -> SessionTrendRow | None:
        rows = [r for r in self.rows.values() if r.is_current and r.session_group == group]
        return rows[-1] if rows else None

    def latest_closed(self, name: str) -> SessionTrendRow | None:
        rows = sorted((r for r in self.rows.values() if r.session_name == name and r.closed_at is not None), key=lambda r: r.session_date)
        return rows[-1] if rows else None

    def display_rows(self) -> list[dict]:
        """Rows for the card: current sessions flagged, closed ones labelled with their date."""
        out = []
        for r in sorted(self.rows.values(), key=lambda r: (r.session_date, r.opened_at)):
            out.append({"session": r.session_name, "group": r.session_group, "trend": r.trend.value, "is_current": r.is_current,
                        "session_date": r.session_date.isoformat(), "closed_at": r.closed_at})
        return out
