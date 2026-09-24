"""SymbolResolver: logical GOLD / SILVER -> the current MCX futures contract.

Fails closed on zero or ambiguous candidates (typed SymbolResolutionError).
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import date, datetime
from typing import Callable

from aureon_mcx.config.yaml_models import ContractPolicy, SymbolsConfig
from aureon_mcx.logging_setup import kv
from aureon_mcx.market.timeutil import IST, utc_now

from .errors import SymbolResolutionError
from .instruments import MCX_EXCHANGE, InstrumentMaster, InstrumentProvider, InstrumentRecord

log = logging.getLogger("aureon.resolver")


@dataclass(frozen=True)
class ResolvedContract:
    logical_symbol: str
    display_symbol: str
    security_id: str
    exchange: str
    exchange_segment: str
    instrument_type: str
    expiry_date: date
    lot_size: float | None
    tick_size: float | None
    trading_symbol: str
    custom_symbol: str
    resolved_at: datetime
    instrument_master_version: str
    instrument_master_downloaded_at: datetime
    contract_policy: ContractPolicy
    rollover_days_before_expiry: int

    def as_row(self) -> dict:
        return {
            "logical_symbol": self.logical_symbol, "display_symbol": self.display_symbol, "security_id": self.security_id,
            "exchange": self.exchange, "exchange_segment": self.exchange_segment, "instrument_type": self.instrument_type,
            "expiry_date": self.expiry_date.isoformat(), "lot_size": self.lot_size, "tick_size": self.tick_size,
            "trading_symbol": self.trading_symbol, "custom_symbol": self.custom_symbol,
            "resolved_at": self.resolved_at.isoformat(), "instrument_master_version": self.instrument_master_version,
        }

    @property
    def expiry_iso(self) -> str:
        return self.expiry_date.isoformat()


class SymbolResolver:
    def __init__(self, config: SymbolsConfig, provider: InstrumentProvider, today: Callable[[], date] | None = None):
        self.config = config
        self.provider = provider
        self._today = today or (lambda: utc_now().astimezone(IST).date())
        self._master: InstrumentMaster | None = None

    # -- master handling ---------------------------------------------------
    def refresh(self, force: bool = False) -> InstrumentMaster:
        self._master = self.provider.load(force_refresh=force)
        return self._master

    @property
    def master(self) -> InstrumentMaster:
        if self._master is None:
            self.refresh()
        assert self._master is not None
        return self._master

    # -- candidate filtering -----------------------------------------------
    def candidates(self, logical: str) -> list[InstrumentRecord]:
        spec = self.config.symbols[logical]
        out = []
        for rec in self.master.records:
            if rec.exchange != MCX_EXCHANGE:
                continue
            if not rec.matches_segment(spec.exchange_segment):
                continue
            if rec.instrument_type != spec.instrument_type.upper():
                continue
            if rec.is_option:
                continue
            if rec.base_name != spec.underlying:  # exact base-name match, never a prefix match
                continue
            if rec.expiry_date is None:
                continue
            out.append(rec)
        return sorted(out, key=lambda r: (r.expiry_date, r.security_id))

    # -- policy ------------------------------------------------------------
    def _apply_policy(self, logical: str, cands: list[InstrumentRecord], today: date) -> list[InstrumentRecord]:
        spec = self.config.symbols[logical]
        rollover_days = self.config.rollover_days(logical)
        if spec.contract_policy is ContractPolicy.NEAREST_LIQUID:
            # DECISION: liquidity is not part of the instrument master, so
            # "nearest_liquid" is implemented as the nearest non-expired standard
            # contract outside the rollover window. Within the window the next
            # contract is active (that is where liquidity migrates on MCX).
            eligible = [c for c in cands if (c.expiry_date - today).days > rollover_days]
            if not eligible:
                return []
            nearest_expiry = eligible[0].expiry_date
            return [c for c in eligible if c.expiry_date == nearest_expiry]
        raise SymbolResolutionError(logical, f"unsupported contract policy {spec.contract_policy}")

    # -- resolution --------------------------------------------------------
    def resolve(self, logical: str) -> ResolvedContract:
        logical = logical.strip().upper()
        if logical not in self.config.symbols:
            raise SymbolResolutionError(logical, "not configured in symbols.yaml")
        today = self._today()
        cands = self.candidates(logical)
        if not cands:
            log.error("symbol_resolution_failed %s", kv(logical=logical, reason="zero candidates"))
            raise SymbolResolutionError(logical, "zero candidates after filtering", [])
        selected = self._apply_policy(logical, cands, today)
        if len(selected) != 1:
            reason = "zero candidates after policy (all within rollover window or expired)" if not selected else \
                f"{len(selected)} candidates share the nearest expiry"
            for c in (selected or cands):
                log.error("symbol_resolution_candidate %s", kv(logical=logical, **c.brief()))
            log.error("symbol_resolution_failed %s", kv(logical=logical, reason=reason))
            raise SymbolResolutionError(logical, reason, [c.brief() for c in (selected or cands)])
        rec = selected[0]
        spec = self.config.symbols[logical]
        resolved = ResolvedContract(
            logical_symbol=logical,
            display_symbol=rec.display_name or rec.custom_symbol or rec.trading_symbol or f"{logical} {rec.expiry_date:%b %Y} FUT",
            security_id=rec.security_id, exchange=rec.exchange, exchange_segment=spec.exchange_segment,
            instrument_type=rec.instrument_type, expiry_date=rec.expiry_date, lot_size=rec.lot_size, tick_size=rec.tick_size,
            trading_symbol=rec.trading_symbol, custom_symbol=rec.custom_symbol, resolved_at=utc_now(),
            instrument_master_version=self.master.version, instrument_master_downloaded_at=self.master.downloaded_at,
            contract_policy=spec.contract_policy, rollover_days_before_expiry=self.config.rollover_days(logical),
        )
        log.info("%s -> security_id %s -> expiry %s", logical, resolved.security_id, resolved.expiry_iso)
        log.info("symbol_resolved %s", kv(logical=logical, security_id=resolved.security_id, expiry=resolved.expiry_iso,
                                          display=resolved.display_symbol, master_version=resolved.instrument_master_version))
        return resolved

    def resolve_all(self, logical_symbols: list[str]) -> dict[str, ResolvedContract]:
        return {s: self.resolve(s) for s in logical_symbols}
