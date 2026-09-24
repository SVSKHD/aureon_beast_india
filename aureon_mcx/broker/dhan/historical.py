"""Dhan v2 intraday historical provider + local cache.

Endpoint: POST /v2/charts/intraday  (intervals 1/5/15/60 minutes).
Every response is normalised into the internal Candle model; only CLOSED
candles (close_time <= now) are returned.
"""
from __future__ import annotations

import logging
from datetime import datetime, timedelta
from typing import Any, Protocol

from aureon_mcx.logging_setup import kv
from aureon_mcx.market.candle import Candle
from aureon_mcx.market.timeframe import Timeframe
from aureon_mcx.market.timeutil import IST, ensure_utc, from_epoch, utc_now
from aureon_mcx.storage.repositories import Repositories

from .client import DhanHttpClient
from .errors import DhanApiError, DhanError

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


class CachedHistoricalProvider:
    """Never downloads the same range twice: candles + covered ranges live in SQLite."""

    def __init__(self, provider: HistoricalProvider, repos: Repositories, archive=None):
        self.provider = provider
        self.repos = repos
        self.archive = archive

    def load(self, symbol: str, security_id: str, exchange_segment: str, instrument_type: str, expiry_date: str,
             timeframe: Timeframe, start: datetime, end: datetime) -> list[Candle]:
        start, end = ensure_utc(start), ensure_utc(end)
        if not self.repos.historical_cache.is_cached(security_id, timeframe, start, end):
            fetched = self.provider.fetch(symbol, security_id, exchange_segment, instrument_type, expiry_date, timeframe, start, end)
            stored = self.repos.candles.insert_many(fetched)
            if self.archive is not None and stored:
                self.archive.archive(stored)
            self.repos.historical_cache.record(security_id, timeframe, start, end, len(stored))
        return self.repos.candles.range(security_id, timeframe, start, end)
