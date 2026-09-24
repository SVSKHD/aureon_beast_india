"""Deterministic confirmed swing structure.

A pivot at bar c (strength s) is confirmed only once bars c+1..c+s are CLOSED.
Live logic sees the pivot at the confirming bar; `pivot_open_time` and
`confirmed_at_open_time` are recorded separately. Nothing is exposed early.
"""
from __future__ import annotations

from collections import deque
from dataclasses import dataclass, field

from aureon_mcx.market.candle import Candle

from .models import Pivot, PivotKind, StructureContext, StructureLabel


@dataclass
class StructureUpdate:
    confirmed: list[Pivot] = field(default_factory=list)
    context: StructureContext = StructureContext.MIXED
    previous_context: StructureContext = StructureContext.MIXED
    sequence: str = ""

    @property
    def context_changed(self) -> bool:
        return self.context != self.previous_context


class StructureEngine:
    def __init__(self, strength: int = 2, equal_tolerance_mode: str = "atr_fraction", equal_tolerance_value: float = 0.1,
                 dominance_lookback: int = 4, tick_size: float | None = None):
        if strength < 1:
            raise ValueError("strength must be >= 1")
        self.strength = strength
        self.tol_mode = equal_tolerance_mode
        self.tol_value = equal_tolerance_value
        self.lookback = dominance_lookback
        self.tick_size = tick_size
        self._window: deque[Candle] = deque(maxlen=2 * strength + 1)
        self.highs: list[Pivot] = []
        self.lows: list[Pivot] = []
        self._context = StructureContext.MIXED
        self._last_open_time = None

    # -- helpers -----------------------------------------------------------
    def _tolerance(self, atr: float | None) -> float:
        if self.tol_mode == "ticks":
            return self.tol_value * (self.tick_size or 0.0)
        return self.tol_value * (atr or 0.0)  # fail closed: no ATR -> strict comparison

    def _label(self, kind: PivotKind, price: float, atr: float | None) -> StructureLabel:
        prev = (self.highs[-1] if self.highs else None) if kind is PivotKind.HIGH else (self.lows[-1] if self.lows else None)
        if prev is None:
            return StructureLabel.SH if kind is PivotKind.HIGH else StructureLabel.SL
        tol = self._tolerance(atr)
        if abs(price - prev.price) <= tol:
            return StructureLabel.EH if kind is PivotKind.HIGH else StructureLabel.EL
        if kind is PivotKind.HIGH:
            return StructureLabel.HH if price > prev.price else StructureLabel.LH
        return StructureLabel.HL if price > prev.price else StructureLabel.LL

    @property
    def all_pivots(self) -> list[Pivot]:
        return sorted(self.highs + self.lows, key=lambda p: (p.pivot_open_time, p.kind.value))

    def context(self) -> StructureContext:
        recent = [p.label for p in self.all_pivots][-self.lookback:]
        bull = sum(1 for l in recent if l.is_bullish)
        bear = sum(1 for l in recent if l.is_bearish)
        # DECISION: dominance requires unanimity among the recent directional labels
        # (>= 2 of them). Any opposing label fails closed to MIXED.
        if bull >= 2 and bear == 0:
            return StructureContext.BULLISH
        if bear >= 2 and bull == 0:
            return StructureContext.BEARISH
        return StructureContext.MIXED

    def sequence(self, n: int | None = None) -> str:
        n = n or self.lookback
        labels = [p.label.value for p in self.all_pivots][-n:]
        return " -> ".join(labels)

    def last_high(self) -> Pivot | None:
        return self.highs[-1] if self.highs else None

    def last_low(self) -> Pivot | None:
        return self.lows[-1] if self.lows else None

    # -- update ------------------------------------------------------------
    def update(self, candle: Candle, atr: float | None = None) -> StructureUpdate:
        if not candle.is_closed:
            raise ValueError("StructureEngine only accepts closed candles")
        if self._last_open_time is not None and candle.open_time <= self._last_open_time:
            raise ValueError("candles must be strictly increasing")
        self._last_open_time = candle.open_time
        self._window.append(candle)
        prev_ctx = self._context
        confirmed: list[Pivot] = []
        s = self.strength
        if len(self._window) == 2 * s + 1:
            w = list(self._window)
            cand = w[s]
            left, right = w[:s], w[s + 1:]
            if all(cand.high > c.high for c in left) and all(cand.high > c.high for c in right):
                confirmed.append(self._make(cand, candle, PivotKind.HIGH, cand.high, atr))
            if all(cand.low < c.low for c in left) and all(cand.low < c.low for c in right):
                confirmed.append(self._make(cand, candle, PivotKind.LOW, cand.low, atr))
        for p in confirmed:
            (self.highs if p.kind is PivotKind.HIGH else self.lows).append(p)
        self._context = self.context()
        return StructureUpdate(confirmed=confirmed, context=self._context, previous_context=prev_ctx, sequence=self.sequence())

    def _make(self, pivot_candle: Candle, confirming: Candle, kind: PivotKind, price: float, atr: float | None) -> Pivot:
        return Pivot(
            symbol=pivot_candle.symbol, security_id=pivot_candle.security_id, expiry_date=pivot_candle.expiry_date,
            timeframe=pivot_candle.timeframe, kind=kind, price=price, label=self._label(kind, price, atr),
            pivot_open_time=pivot_candle.open_time, confirmed_at_open_time=confirming.open_time, strength=self.strength,
            candle_id=pivot_candle.id, confirmed_candle_id=confirming.id,
        )

    def state_dict(self) -> dict:
        lh, ll = self.last_high(), self.last_low()
        return {
            "context": self._context.value,
            "sequence": self.sequence(),
            "last_high": {"price": lh.price, "label": lh.label.value, "open_time": lh.pivot_open_time.isoformat()} if lh else None,
            "last_low": {"price": ll.price, "label": ll.label.value, "open_time": ll.pivot_open_time.isoformat()} if ll else None,
        }
