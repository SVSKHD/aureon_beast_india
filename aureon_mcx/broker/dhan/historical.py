"""Dhan v2 intraday historical provider + local cache.

Endpoint: POST /v2/charts/intraday  (intervals 1/5/15/60 minutes).
Every response is normalised into the internal Candle model; only CLOSED
candles (close_time <= now) are returned.
"""
from __future__ import annotations

import logging
from datetime import datetime, timedelta
from typing import TYPE_CHECKING, Any, Protocol

from aureon_mcx.logging_setup import kv
from aureon_mcx.market.candle import Candle
from aureon_mcx.market.timeframe import Timeframe
from aureon_mcx.market.timeutil import IST, ensure_utc, floor_to, from_epoch, utc_now
from aureon_mcx.storage.repositories import Repositories

from .client import DhanHttpClient
from .errors import DhanApiError, DhanError

if TYPE_CHECKING:
    from aureon_mcx.market.sessions import SessionCalendar

log = logging.getLogger("aureon.dhan.historical")


class HistoricalProvider(Protocol):
    def fetch(self, symbol: str, security_id: str, exchange_segment: str, instrument_type: str, expiry_date: str,
              timeframe: Timeframe, start: datetime, end: datetime) -> list[Candle]: ...


def normalize_intraday_response(payload: dict[str, Any], symbol: str, security_id: str, timeframe: Timeframe,
                                expiry_date: str, now: datetime | None = None) -> list[Candle]:
    """Convert Dhan's column-oriented intraday payload into closed Candles."""
    now = ensure_utc(now or utc_now())
    required = ("open", "high", "low", "close", "timestamp")
    for k in required:
        if k not in payload or not isinstance(payload[k], list):
            raise DhanError(f"intraday payload missing column {k!r} for security_id={security_id}")
    n = len(payload["timestamp"])
    cols = {k: payload.get(k) or [] for k in ("open", "high", "low", "close", "volume", "open_interest", "timestamp")}
    for k in ("open", "high", "low", "close"):
        if len(cols[k]) != n:
            raise DhanError(f"intraday payload column {k!r} length mismatch for security_id={security_id}")
    out: list[Candle] = []
    seen: set[datetime] = set()
    for i in range(n):
        ts = cols["timestamp"][i]
        if ts is None:
            continue
        # DECISION: Dhan returns epoch seconds; treated as a true UTC epoch.
        open_time = from_epoch(ts)
        if open_time in seen:
            continue
        seen.add(open_time)
        candle = Candle(
            symbol=symbol, security_id=security_id, timeframe=timeframe, open_time=open_time,
            open=float(cols["open"][i]), high=float(cols["high"][i]), low=float(cols["low"][i]), close=float(cols["close"][i]),
            volume=float(cols["volume"][i]) if i < len(cols["volume"]) and cols["volume"][i] is not None else 0.0,
            open_interest=float(cols["open_interest"][i]) if i < len(cols["open_interest"]) and cols["open_interest"][i] not in (None, 0) else None,
            source="dhan", is_closed=True, expiry_date=expiry_date,
        )
        if candle.close_time > now:
            continue  # incomplete bar: never leaks into analysis
        out.append(candle)
    out.sort(key=lambda c: c.open_time)
    return out


class DhanHistoricalProvider:
    def __init__(self, http: DhanHttpClient, max_days_per_request: int = 5, now=utc_now):
        self.http = http
        self.max_days = max_days_per_request
        self._now = now

    def fetch(self, symbol: str, security_id: str, exchange_segment: str, instrument_type: str, expiry_date: str,
              timeframe: Timeframe, start: datetime, end: datetime) -> list[Candle]:
        interval = timeframe.dhan_interval
        if interval is None:
            raise DhanError(f"{timeframe.value} is not downloadable from Dhan; aggregate locally instead")
        start = ensure_utc(start)
        end = ensure_utc(end)
        out: list[Candle] = []
        chunk_start = start
        while chunk_start < end:
            chunk_end = min(end, chunk_start + timedelta(days=self.max_days))
            body = {
                "securityId": str(security_id),
                "exchangeSegment": exchange_segment,
                "instrument": instrument_type,
                "interval": str(interval),
                "oi": True,
                "fromDate": chunk_start.astimezone(IST).strftime("%Y-%m-%d %H:%M:%S"),
                "toDate": chunk_end.astimezone(IST).strftime("%Y-%m-%d %H:%M:%S"),
            }
            ctx = {"security_id": security_id, "timeframe": timeframe.value, "from": body["fromDate"], "to": body["toDate"]}
            try:
                payload = self.http.post_json("/charts/intraday", body, context=ctx)
            except DhanApiError:
                log.error("historical_fetch_failed %s", kv(**ctx))
                raise
            if not isinstance(payload, dict):
                raise DhanError(f"unexpected intraday payload type {type(payload).__name__}")
            candles = normalize_intraday_response(payload, symbol, security_id, timeframe, expiry_date, now=self._now())
            log.info("historical_fetch %s", kv(symbol=symbol, **ctx, bars=len(candles)))
            out.extend(candles)
            chunk_start = chunk_end
        dedup = {c.open_time: c for c in out}
        return [dedup[k] for k in sorted(dedup)]


def verified_coverage(candles: list[Candle], timeframe: Timeframe, start: datetime, end: datetime, *,
                      calendar: "SessionCalendar | None" = None, now: datetime | None = None,
                      tz=IST) -> list[tuple[datetime, datetime]]:
    """Intervals of [start, end) that ``candles`` provably cover.

    ``potential`` = every timeframe-aligned open inside the range at which the market
    is open (all aligned opens when no calendar is given); ``expected`` = the potential
    bars that have already closed at ``now``.  A maximal run of consecutive expected
    bars that are all present yields one interval.  The interval may stretch to
    ``start`` / ``end`` only when nothing else could ever be expected there (closed
    market), never across a bar that is still open or that the broker did not return.
    Fabrication is impossible: an empty response over an open market covers nothing.
    """
    start, end = ensure_utc(start), ensure_utc(end)
    now = ensure_utc(now or utc_now())
    if end <= start:
        return []
    step = timedelta(seconds=timeframe.seconds)
    potential: list[datetime] = []
    t = floor_to(start, timeframe.seconds, tz)
    if t < start:
        t += step
    while t < end:
        if calendar is None or calendar.is_open(t):
            potential.append(t)
        t += step
    if not potential:
        return [(start, end)]  # nothing can ever exist here (holiday / closed hours)
    expected = [t for t in potential if t + step <= now]
    if not expected:
        return []
    present = {ensure_utc(c.open_time) for c in candles if c.timeframe is timeframe}
    out: list[tuple[datetime, datetime]] = []
    i = 0
    while i < len(expected):
        if expected[i] not in present:
            i += 1
            continue
        j = i
        while j + 1 < len(expected) and expected[j + 1] in present:
            j += 1
        s = start if i == 0 else expected[i]
        e = end if expected[j] == potential[-1] else expected[j] + step
        out.append((s, e))
        i = j + 1
    return out


class CachedHistoricalProvider:
    """Downloads only what verified coverage lacks; candles + coverage live in SQLite.

    Coverage is recorded from what the broker actually returned (``verified_coverage``),
    never from the requested range, so a partial / empty / truncated response leaves
    the missing part uncovered and it is re-requested next time.
    """

    def __init__(self, provider: HistoricalProvider, repos: Repositories, archive=None,
                 calendar: "SessionCalendar | None" = None, now=utc_now):
        self.provider = provider
        self.repos = repos
        self.archive = archive
        self.calendar = calendar
        self._now = now

    def load(self, symbol: str, security_id: str, exchange_segment: str, instrument_type: str, expiry_date: str,
             timeframe: Timeframe, start: datetime, end: datetime) -> list[Candle]:
        start, end = ensure_utc(start), ensure_utc(end)
        for gap_start, gap_end in self.repos.historical_cache.uncovered(security_id, timeframe, start, end):
            fetched = self.provider.fetch(symbol, security_id, exchange_segment, instrument_type, expiry_date, timeframe,
                                          gap_start, gap_end)
            fetched = [c for c in fetched if gap_start <= ensure_utc(c.open_time) < gap_end]
            stored = self.repos.candles.insert_many(fetched)
            if self.archive is not None and stored:
                self.archive.archive(stored)
            covered = verified_coverage(stored, timeframe, gap_start, gap_end, calendar=self.calendar, now=self._now())
            for s, e in covered:
                self.repos.historical_cache.record(security_id, timeframe, s, e, sum(1 for c in stored if s <= c.open_time < e))
            missing = self.repos.historical_cache.uncovered(security_id, timeframe, gap_start, gap_end)
            if missing:
                log.warning("historical_coverage_incomplete %s",
                            kv(symbol=symbol, security_id=security_id, timeframe=timeframe.value, bars=len(stored),
                               missing=";".join(f"{a.isoformat()}..{b.isoformat()}" for a, b in missing[:5]), gaps=len(missing)))
        return self.repos.candles.range(security_id, timeframe, start, end)
