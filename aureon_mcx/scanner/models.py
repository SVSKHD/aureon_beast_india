from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any

REFERENCE_VERIFIED = "VERIFIED"   # previous close came from the broker's explicit previous-close data
REFERENCE_MISSING = "MISSING"     # no verified reference: change / change_pct stay null, never fabricated


@dataclass
class InstrumentQuote:
    security_id: str
    symbol: str                 # base name (GOLD, SILVERM, CRUDEOIL ...)
    display_symbol: str
    segment: str                # exchange segment (MCX_COMM, NSE_EQ ...)
    instrument_type: str        # FUTCOM, EQUITY ...
    expiry: str | None
    category: str | None
    ltp: float | None = None
    previous_close: float | None = None
    reference_status: str = REFERENCE_MISSING
    day_open: float | None = None
    day_high: float | None = None
    day_low: float | None = None
    day_close_field: float | None = None    # Dhan quote "close" field, informational only
    volume: float | None = None
    open_interest: float | None = None
    last_trade_at: datetime | None = None
    last_update: datetime | None = None
    packets: int = 0
    extra: dict[str, Any] = field(default_factory=dict)

    @property
    def change(self) -> float | None:
        if self.ltp is None or self.previous_close is None or self.reference_status != REFERENCE_VERIFIED:
            return None
        return self.ltp - self.previous_close

    @property
    def change_pct(self) -> float | None:
        if self.change is None or not self.previous_close:
            return None
        return (self.ltp - self.previous_close) / self.previous_close * 100.0

    def is_stale(self, now: datetime, threshold: timedelta) -> bool:
        return self.last_update is None or now - self.last_update > threshold

    def to_dict(self, now: datetime | None = None, threshold: timedelta | None = None) -> dict[str, Any]:
        stale = self.is_stale(now, threshold) if (now is not None and threshold is not None) else None
        return {"symbol": self.symbol, "display_symbol": self.display_symbol, "security_id": self.security_id, "segment": self.segment,
                "instrument_type": self.instrument_type, "expiry": self.expiry, "category": self.category, "ltp": self.ltp,
                "previous_close": self.previous_close, "reference_status": self.reference_status, "change": self.change,
                "change_pct": self.change_pct, "day_open": self.day_open, "day_high": self.day_high, "day_low": self.day_low,
                "volume": self.volume, "open_interest": self.open_interest,
                "last_trade_at": self.last_trade_at.isoformat() if self.last_trade_at else None,
                "last_update": self.last_update.isoformat() if self.last_update else None, "packets": self.packets, "stale": stale}


@dataclass(frozen=True)
class Ranked:
    rank: int
    quote: InstrumentQuote

    def to_dict(self, now: datetime, threshold: timedelta) -> dict[str, Any]:
        return {"rank": self.rank, **self.quote.to_dict(now, threshold)}
