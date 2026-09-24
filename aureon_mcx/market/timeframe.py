"""Timeframe enum shared across the whole system.

Kept dependency-free so config, storage and market code can all import it.
"""
from __future__ import annotations

from enum import Enum


class Timeframe(str, Enum):
    M1 = "M1"
    M5 = "M5"
    M15 = "M15"
    H1 = "H1"
    H4 = "H4"

    @property
    def seconds(self) -> int:
        return _SECONDS[self]

    @property
    def minutes(self) -> int:
        return self.seconds // 60

    @property
    def dhan_interval(self) -> int | None:
        """Dhan v2 intraday interval code, or None when locally aggregated only."""
        return _DHAN_INTERVAL.get(self)

    @property
    def rank(self) -> int:
        return _ORDER.index(self)

    def is_higher_than(self, other: "Timeframe") -> bool:
        return self.rank > other.rank

    @classmethod
    def parse(cls, value: str) -> "Timeframe":
        v = value.strip().upper()
        try:
            return cls(v)
        except ValueError as exc:  # pragma: no cover - trivial
            raise ValueError(f"unknown timeframe {value!r}; expected one of {[t.value for t in cls]}") from exc


_SECONDS = {
    Timeframe.M1: 60,
    Timeframe.M5: 300,
    Timeframe.M15: 900,
    Timeframe.H1: 3600,
    Timeframe.H4: 14400,
}
_DHAN_INTERVAL = {
    Timeframe.M1: 1,
    Timeframe.M5: 5,
    Timeframe.M15: 15,
    Timeframe.H1: 60,
}
_ORDER = [Timeframe.M1, Timeframe.M5, Timeframe.M15, Timeframe.H1, Timeframe.H4]
