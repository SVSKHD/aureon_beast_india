"""Closed-only timeframe aggregation (M1->M5, M5->M15/H1, H1->H4).

A target bar is emitted ONLY when its boundary has passed: either the last
constituent closed exactly on the boundary, a constituent from a later bucket
arrived, or `flush_at(now)` is called with `now` past the boundary. A partial
bucket is exposed only through `partial` (is_closed=False) and never through
`add()` / `flush_at()`.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta

from .candle import Candle
from .timeframe import Timeframe
from .timeutil import IST, ensure_utc, floor_to


@dataclass
class _Bucket:
    open_time: datetime
    end_time: datetime
    open: float
    high: float
    low: float
    close: float
    volume: float
    open_interest: float | None
    count: int = 0
    last_source_open_time: datetime | None = None
    sources: list[datetime] = field(default_factory=list)


class TimeframeAggregator:
    def __init__(self, source: Timeframe, target: Timeframe, symbol: str, security_id: str, expiry_date: str = "", tz=IST):
        if target.seconds <= source.seconds or target.seconds % source.seconds != 0:
            raise ValueError(f"cannot aggregate {source.value} into {target.value}")
        self.source = source
        self.target = target
        self.symbol = symbol
        self.security_id = security_id
        self.expiry_date = expiry_date
        self.tz = tz
        self._bucket: _Bucket | None = None
        self._last_emitted_open: datetime | None = None
        self._pending: Candle | None = None

    @property
    def expected_constituents(self) -> int:
        return self.target.seconds // self.source.seconds

    def _bucket_start(self, ts: datetime) -> datetime:
        return floor_to(ts, self.target.seconds, self.tz)

    def _emit(self) -> Candle:
        b = self._bucket
        assert b is not None
        self._bucket = None
        self._last_emitted_open = b.open_time
        return Candle(
            symbol=self.symbol, security_id=self.security_id, timeframe=self.target, open_time=b.open_time, open=b.open,
            high=b.high, low=b.low, close=b.close, volume=b.volume, open_interest=b.open_interest, source="aggregated",
            is_closed=True, expiry_date=self.expiry_date,
        )

    def add(self, candle: Candle) -> Candle | None:
        """Feed a CLOSED source candle. Returns a closed target candle when a boundary passes."""
        if not candle.is_closed:
            raise ValueError("aggregator only accepts closed source candles")
        if candle.timeframe != self.source:
            raise ValueError(f"expected {self.source.value} candle, got {candle.timeframe.value}")
        ts = ensure_utc(candle.open_time)
        start = self._bucket_start(ts)
        if self._last_emitted_open is not None and start <= self._last_emitted_open:
            return None  # late duplicate for an already-emitted bucket; ignore (never re-open a closed bar)
        emitted: Candle | None = None
        if self._bucket is not None and start > self._bucket.open_time:
            emitted = self._emit()  # later bucket arrived: previous boundary has passed
        if self._bucket is None:
            self._bucket = _Bucket(open_time=start, end_time=start + timedelta(seconds=self.target.seconds), open=candle.open,
                                   high=candle.high, low=candle.low, close=candle.close, volume=0.0, open_interest=None)
        b = self._bucket
        if b.last_source_open_time is not None and ts <= b.last_source_open_time:
            return emitted  # duplicate / out-of-order constituent; ignore
        b.high = max(b.high, candle.high)
        b.low = min(b.low, candle.low)
        b.close = candle.close
        b.volume += candle.volume
        if candle.open_interest is not None:
            b.open_interest = candle.open_interest
        b.count += 1
        b.last_source_open_time = ts
        b.sources.append(ts)
        if candle.close_time >= b.end_time:
            closed = self._emit()
            # DECISION: if two bars are emitted at once (rare gap case) return the
            # newest; the earlier one is returned through `emitted` first by the
            # caller's ordering below.
            if emitted is not None:
                self._pending = closed
                return emitted
            return closed
        return emitted

    def mark_emitted_until(self, open_time: datetime) -> None:
        """Treat every bucket up to and including `open_time` as already emitted (used when
        seeding from storage so a stored broker bar is never re-emitted)."""
        ts = self._bucket_start(ensure_utc(open_time))
        if self._last_emitted_open is None or ts > self._last_emitted_open:
            self._last_emitted_open = ts

    def flush_at(self, now: datetime) -> Candle | None:
        """Emit the open bucket if wall-clock `now` is past its boundary."""
        if self._bucket is not None and ensure_utc(now) >= self._bucket.end_time:
            return self._emit()
        return None

    def drain_pending(self) -> Candle | None:
        p = self._pending
        self._pending = None
        return p

    @property
    def partial(self) -> Candle | None:
        b = self._bucket
        if b is None:
            return None
        return Candle(symbol=self.symbol, security_id=self.security_id, timeframe=self.target, open_time=b.open_time, open=b.open,
                      high=b.high, low=b.low, close=b.close, volume=b.volume, open_interest=b.open_interest, source="aggregated",
                      is_closed=False, expiry_date=self.expiry_date)


def aggregate_closed(candles: list[Candle], target: Timeframe, tz=IST, now: datetime | None = None) -> list[Candle]:
    """Batch aggregation of a closed source series. Trailing partial bucket is dropped
    unless `now` is past its boundary."""
    if not candles:
        return []
    first = candles[0]
    agg = TimeframeAggregator(first.timeframe, target, first.symbol, first.security_id, first.expiry_date, tz)
    out: list[Candle] = []
    for c in candles:
        emitted = agg.add(c)
        if emitted is not None:
            out.append(emitted)
            pending = agg.drain_pending()
            if pending is not None:
                out.append(pending)
    if now is not None:
        tail = agg.flush_at(now)
        if tail is not None:
            out.append(tail)
    return out
