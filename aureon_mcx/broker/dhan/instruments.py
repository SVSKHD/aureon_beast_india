"""Dhan instrument master: download, cache, normalise.

Supports both the detailed (`api-scrip-master-detailed.csv`) and compact
(`api-scrip-master.csv`) column layouts. Nothing here hard-codes a security id.
"""
from __future__ import annotations

import csv
import hashlib
import io
import json
import logging
import re
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Protocol

from aureon_mcx.logging_setup import kv
from aureon_mcx.market.timeutil import utc_now

from .client import DhanHttpClient
from .errors import DhanError

log = logging.getLogger("aureon.dhan.instruments")

MCX_EXCHANGE = "MCX"
SEGMENT_ALIASES: dict[str, set[str]] = {
    "MCX_COMM": {"M", "COMM", "MCX_COMM", "COMMODITY"},
    "NSE_EQ": {"E", "EQ", "NSE_EQ", "EQUITY"},
    "BSE_EQ": {"E", "EQ", "BSE_EQ", "EQUITY"},
    "NSE_FNO": {"D", "FNO", "NSE_FNO", "DERIVATIVES"},
    "BSE_FNO": {"D", "FNO", "BSE_FNO", "DERIVATIVES"},
    "NSE_CURRENCY": {"C", "CURRENCY", "NSE_CURRENCY"},
    "BSE_CURRENCY": {"C", "CURRENCY", "BSE_CURRENCY"},
    "IDX_I": {"I", "INDEX", "IDX_I"},
}
OPTION_INSTRUMENT_TYPES = {"OPTFUT", "OPTCOM", "OPTIDX", "OPTSTK", "OPTCUR"}


@dataclass(frozen=True)
class InstrumentRecord:
    exchange: str
    segment: str
    security_id: str
    instrument_type: str
    base_name: str
    symbol_name: str
    display_name: str
    trading_symbol: str
    custom_symbol: str
    expiry_date: date | None
    lot_size: float | None
    tick_size: float | None
    option_type: str
    strike_price: float | None

    @property
    def is_option(self) -> bool:
        return self.instrument_type.upper() in OPTION_INSTRUMENT_TYPES or bool(self.option_type and self.option_type not in ("", "XX", "NA"))

    def matches_segment(self, exchange_segment: str) -> bool:
        aliases = SEGMENT_ALIASES.get(exchange_segment.upper(), {exchange_segment.upper()})
        return self.segment.upper() in aliases or self.segment.upper() == exchange_segment.upper()

    def brief(self) -> dict:
        return {
            "security_id": self.security_id, "base_name": self.base_name, "trading_symbol": self.trading_symbol,
            "instrument_type": self.instrument_type, "expiry": self.expiry_date.isoformat() if self.expiry_date else None,
        }


@dataclass(frozen=True)
class InstrumentMaster:
    records: list[InstrumentRecord]
    version: str
    downloaded_at: datetime
    source: str


class InstrumentProvider(Protocol):
    def load(self, force_refresh: bool = False) -> InstrumentMaster: ...


# ------------------------------------------------------------------- parsing
_DETAILED_MAP = {
    "exchange": ["EXCH_ID", "SEM_EXM_EXCH_ID"],
    "segment": ["SEGMENT", "SEM_SEGMENT"],
    "security_id": ["SECURITY_ID", "SEM_SMST_SECURITY_ID"],
    "instrument_type": ["INSTRUMENT", "SEM_INSTRUMENT_NAME", "INSTRUMENT_TYPE"],
    "underlying_symbol": ["UNDERLYING_SYMBOL"],
    "symbol_name": ["SYMBOL_NAME", "SM_SYMBOL_NAME"],
    "display_name": ["DISPLAY_NAME", "SEM_CUSTOM_SYMBOL"],
    "trading_symbol": ["TRADING_SYMBOL", "SEM_TRADING_SYMBOL"],
    "custom_symbol": ["CUSTOM_SYMBOL", "SEM_CUSTOM_SYMBOL", "DISPLAY_NAME"],
    "expiry_date": ["SM_EXPIRY_DATE", "SEM_EXPIRY_DATE", "EXPIRY_DATE"],
    "lot_size": ["LOT_SIZE", "SEM_LOT_UNITS"],
    "tick_size": ["TICK_SIZE", "SEM_TICK_SIZE"],
    "option_type": ["OPTION_TYPE", "SEM_OPTION_TYPE"],
    "strike_price": ["STRIKE_PRICE", "SEM_STRIKE_PRICE"],
}

_EXPIRY_FORMATS = ("%Y-%m-%d", "%Y-%m-%d %H:%M:%S", "%d-%m-%Y", "%d/%m/%Y", "%Y/%m/%d", "%d-%b-%Y", "%d%b%Y")
_TOKEN_SPLIT = re.compile(r"[\s\-_]+")


def _pick(row: dict[str, str], keys: list[str]) -> str:
    for k in keys:
        if k in row and row[k] is not None:
            v = str(row[k]).strip()
            if v:
                return v
    return ""


def _parse_float(v: str) -> float | None:
    try:
        return float(v) if v not in ("", "NA", "None") else None
    except ValueError:
        return None


def parse_expiry(v: str) -> date | None:
    v = v.strip()
    if not v or v.upper() in ("NA", "0", "NONE", "1980-01-01", "1980-01-01 00:00:00"):
        return None
    for fmt in _EXPIRY_FORMATS:
        try:
            return datetime.strptime(v, fmt).date()
        except ValueError:
            continue
    try:
        return datetime.fromisoformat(v).date()
    except ValueError:
        return None


def derive_base_name(symbol_name: str, trading_symbol: str, underlying_symbol: str) -> str:
    """Derive the exact underlying/base name for a futures row.

    # DECISION: the symbol name (first token) wins over UNDERLYING_SYMBOL because
    # mini/micro variants (GOLDM, SILVERMIC, ...) may share an underlying with
    # the standard contract. Exact matching on the base name is what keeps
    # GOLDM from resolving as GOLD. If symbol name and trading-symbol prefix
    # disagree the row is treated as inconsistent and excluded (fail closed).
    """
    from_symbol = _TOKEN_SPLIT.split(symbol_name.strip().upper())[0] if symbol_name.strip() else ""
    from_trading = _TOKEN_SPLIT.split(trading_symbol.strip().upper())[0] if trading_symbol.strip() else ""
    if from_symbol and from_trading and from_symbol != from_trading:
        return ""
    return from_symbol or from_trading or underlying_symbol.strip().upper()


def parse_instrument_master_csv(text: str) -> list[InstrumentRecord]:
    reader = csv.DictReader(io.StringIO(text))
    if not reader.fieldnames:
        raise DhanError("instrument master is empty")
    fields = {f.strip().upper(): f for f in reader.fieldnames}

    def norm_row(raw: dict[str, str]) -> dict[str, str]:
        return {k.strip().upper(): (v or "") for k, v in raw.items() if k}

    required = ["security_id", "exchange", "segment", "instrument_type"]
    for key in required:
        if not any(k in fields for k in _DETAILED_MAP[key]):
            raise DhanError(f"instrument master missing a column for {key}; columns={sorted(fields)}")

    out: list[InstrumentRecord] = []
    for raw in reader:
        row = norm_row(raw)
        symbol_name = _pick(row, _DETAILED_MAP["symbol_name"])
        trading_symbol = _pick(row, _DETAILED_MAP["trading_symbol"])
        underlying = _pick(row, _DETAILED_MAP["underlying_symbol"])
        rec = InstrumentRecord(
            exchange=_pick(row, _DETAILED_MAP["exchange"]).upper(),
            segment=_pick(row, _DETAILED_MAP["segment"]).upper(),
            security_id=_pick(row, _DETAILED_MAP["security_id"]),
            instrument_type=_pick(row, _DETAILED_MAP["instrument_type"]).upper(),
            base_name=derive_base_name(symbol_name, trading_symbol, underlying),
            symbol_name=symbol_name,
            display_name=_pick(row, _DETAILED_MAP["display_name"]),
            trading_symbol=trading_symbol,
            custom_symbol=_pick(row, _DETAILED_MAP["custom_symbol"]),
            expiry_date=parse_expiry(_pick(row, _DETAILED_MAP["expiry_date"])),
            lot_size=_parse_float(_pick(row, _DETAILED_MAP["lot_size"])),
            tick_size=_parse_float(_pick(row, _DETAILED_MAP["tick_size"])),
            option_type=_pick(row, _DETAILED_MAP["option_type"]).upper(),
            strike_price=_parse_float(_pick(row, _DETAILED_MAP["strike_price"])),
        )
        if rec.security_id:
            out.append(rec)
    return out


def master_version(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()[:16]


# ------------------------------------------------------------------ provider
class DhanInstrumentProvider:
    """Downloads the Dhan scrip master, caches it on disk, refreshes daily."""

    def __init__(self, url: str, cache_path: str | Path, refresh_hours: float, http: DhanHttpClient | None = None,
                 now=utc_now):
        self.url = url
        self.cache_path = Path(cache_path)
        self.meta_path = self.cache_path.with_suffix(self.cache_path.suffix + ".meta.json")
        self.refresh_hours = refresh_hours
        self._http = http
        self._now = now
        self._master: InstrumentMaster | None = None

    def _cache_fresh(self) -> bool:
        if not (self.cache_path.exists() and self.meta_path.exists()):
            return False
        try:
            meta = json.loads(self.meta_path.read_text(encoding="utf-8"))
            downloaded = datetime.fromisoformat(meta["downloaded_at"])
        except (ValueError, KeyError, json.JSONDecodeError):
            return False
        return self._now() - downloaded < timedelta(hours=self.refresh_hours)

    def _download(self) -> str:
        if self._http is None:
            raise DhanError("no HTTP client configured for instrument master download")
        log.info("instrument_master_download %s", kv(url=self.url))
        text = self._http.get_text(self.url, context={"endpoint": "instrument_master"})
        if not text or "," not in text.splitlines()[0]:
            raise DhanError("instrument master download returned no CSV content")
        self.cache_path.parent.mkdir(parents=True, exist_ok=True)
        self.cache_path.write_text(text, encoding="utf-8")
        self.meta_path.write_text(
            json.dumps({"downloaded_at": self._now().isoformat(), "version": master_version(text), "url": self.url}),
            encoding="utf-8",
        )
        return text

    def load(self, force_refresh: bool = False) -> InstrumentMaster:
        text: str | None = None
        source = "cache"
        if force_refresh or not self._cache_fresh():
            try:
                text = self._download()
                source = "download"
            except DhanError as exc:
                if self.cache_path.exists():
                    log.warning("instrument_master_download_failed_using_stale_cache %s", kv(error=str(exc)))
                else:
                    raise
        if text is None:
            text = self.cache_path.read_text(encoding="utf-8")
        meta_downloaded = self._now()
        if self.meta_path.exists():
            try:
                meta_downloaded = datetime.fromisoformat(json.loads(self.meta_path.read_text(encoding="utf-8"))["downloaded_at"])
            except (ValueError, KeyError, json.JSONDecodeError):
                pass
        records = parse_instrument_master_csv(text)
        self._master = InstrumentMaster(records=records, version=master_version(text), downloaded_at=meta_downloaded, source=source)
        log.info("instrument_master_loaded %s", kv(records=len(records), version=self._master.version, source=source))
        return self._master
