"""Scanner universe: the enabled instruments from Dhan's instrument master.

MCX instruments are commodity contracts, so the universe is built per exchange segment and
instrument type, optionally keeping only the nearest non-expired contract per base name
(the front month, where liquidity lives). Subscription limits are respected by partitioning
the universe into per-connection chunks; nothing is subscribed beyond the documented limits.
"""
from __future__ import annotations

import fnmatch
import logging
from datetime import date

from aureon_mcx.broker.dhan.instruments import InstrumentMaster, InstrumentRecord
from aureon_mcx.config.yaml_models import ScannerConfig

log = logging.getLogger("aureon.scanner")

EXCHANGE_FOR_SEGMENT = {"MCX_COMM": "MCX", "NSE_EQ": "NSE", "NSE_FNO": "NSE", "BSE_EQ": "BSE", "NSE_CURRENCY": "NSE", "BSE_FNO": "BSE",
                        "BSE_CURRENCY": "BSE", "IDX_I": "IDX"}


def _matches(name: str, patterns: list[str]) -> bool:
    return any(fnmatch.fnmatchcase(name, p.upper()) for p in patterns)


def build_universe(master: InstrumentMaster, cfg: ScannerConfig, today: date) -> dict[str, list[InstrumentRecord]]:
    """Eligible instruments keyed by exchange segment, sorted by base name then expiry."""
    out: dict[str, list[InstrumentRecord]] = {}
    for segment in cfg.segments:
        exchange = EXCHANGE_FOR_SEGMENT.get(segment)
        rows: list[InstrumentRecord] = []
        for rec in master.records:
            if exchange is not None and rec.exchange.upper() != exchange:
                continue
            if not rec.matches_segment(segment):
                continue
            if rec.instrument_type.upper() not in cfg.instrument_types:
                continue
            if rec.is_option or not rec.base_name:
                continue
            if rec.expiry_date is not None and rec.expiry_date < today:
                continue
            if not _matches(rec.base_name, cfg.include) or _matches(rec.base_name, cfg.exclude):
                continue
            rows.append(rec)
        if cfg.nearest_expiry_only:
            nearest: dict[str, InstrumentRecord] = {}
            for rec in rows:
                cur = nearest.get(rec.base_name)
                key = (rec.expiry_date or date.max, rec.security_id)
                if cur is None or key < (cur.expiry_date or date.max, cur.security_id):
                    nearest[rec.base_name] = rec
            rows = list(nearest.values())
        rows.sort(key=lambda r: (r.base_name, r.expiry_date or date.max, r.security_id))
        out[segment] = rows
    total = sum(len(v) for v in out.values())
    cap = cfg.max_subscriptions_per_connection * cfg.max_connections
    if total > cap:
        log.warning("scanner_universe_truncated total=%d cap=%d (max_subscriptions_per_connection x max_connections)", total, cap)
        remaining = cap
        for segment in list(out):
            out[segment] = out[segment][:remaining]
            remaining -= len(out[segment])
    log.info("scanner_universe %s", " ".join(f"{seg}={len(rows)}" for seg, rows in out.items()))
    return out


def partition(security_ids: list[str], per_connection: int) -> list[list[str]]:
    """Split ids into per-connection chunks (each chunk is further split into 100-id requests by the feed)."""
    per_connection = max(1, per_connection)
    return [security_ids[i:i + per_connection] for i in range(0, len(security_ids), per_connection)] or []
