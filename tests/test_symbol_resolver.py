from __future__ import annotations

import logging
from datetime import date, datetime, timezone

import pytest

from aureon_mcx.broker.dhan import InstrumentMaster, SymbolResolutionError, SymbolResolver
from aureon_mcx.broker.dhan.instruments import derive_base_name, parse_instrument_master_csv
from aureon_mcx.config.yaml_models import SymbolsConfig
from tests.conftest import FIXTURES


class FakeProvider:
    def __init__(self, text: str):
        self.text = text
        self.loads = 0

    def load(self, force_refresh: bool = False) -> InstrumentMaster:
        self.loads += 1
        return InstrumentMaster(records=parse_instrument_master_csv(self.text), version="fixture-v1",
                                downloaded_at=datetime(2026, 9, 21, tzinfo=timezone.utc), source="fixture")


def _cfg(rollover=3) -> SymbolsConfig:
    return SymbolsConfig.model_validate({
        "instrument_master": {"url": "https://example.invalid/master.csv"},
        "rollover_days_before_expiry": rollover,
        "symbols": {
            "GOLD": {"underlying": "GOLD", "exchange_segment": "MCX_COMM", "instrument_type": "FUTCOM", "contract_policy": "nearest_liquid"},
            "SILVER": {"underlying": "SILVER", "exchange_segment": "MCX_COMM", "instrument_type": "FUTCOM", "contract_policy": "nearest_liquid"},
        },
    })


def _resolver(text=None, today=date(2026, 9, 21), rollover=3):
    text = text if text is not None else (FIXTURES / "instrument_master_detailed.csv").read_text()
    return SymbolResolver(_cfg(rollover), FakeProvider(text), today=lambda: today)


def test_gold_resolves_standard_gold_not_variants(caplog):
    caplog.set_level(logging.INFO, logger="aureon.resolver")
    r = _resolver().resolve("GOLD")
    assert r.security_id == "428291"
    assert r.expiry_date == date(2026, 10, 5)
    assert r.instrument_type == "FUTCOM" and r.exchange == "MCX" and r.exchange_segment == "MCX_COMM"
    assert r.lot_size == 100 and r.tick_size == 1.0
    assert "GOLD -> security_id 428291 -> expiry 2026-10-05" in caplog.text
    ids = {c.security_id for c in _resolver().candidates("GOLD")}
    assert "428292" not in ids  # GOLDM
    assert "428293" not in ids  # GOLDPETAL
    assert "428294" not in ids  # GOLDGUINEA
    assert "428295" not in ids  # GOLD option


def test_silver_resolves_standard_silver_not_variants():
    r = _resolver().resolve("SILVER")
    # SEP contract (2026-09-25) is within the 3-day rollover window? 4 days out -> still active
    assert r.security_id == "429003"
    ids = {c.security_id for c in _resolver().candidates("SILVER")}
    assert ids == {"429001", "429002", "429003"}
    for variant in ("429004", "429005", "429006"):  # SILVERM, SILVERMIC, SILVER1000
        assert variant not in ids


def test_rollover_window_moves_to_next_contract():
    # 2026-09-23: SILVER SEP expires 2026-09-25 -> 2 days left <= 3 -> roll to DEC
    r = _resolver(today=date(2026, 9, 23)).resolve("SILVER")
    assert r.security_id == "429001" and r.expiry_date == date(2026, 12, 4)
    # exact boundary: days == rollover -> rolled
    r = _resolver(today=date(2026, 9, 22)).resolve("SILVER")
    assert r.security_id == "429001"
    # expired contracts are never selected
    r = _resolver(today=date(2026, 10, 20)).resolve("GOLD")
    assert r.security_id == "431102"
    # rollover window configurable: zero -> SEP stays active until expiry day passes
    r = _resolver(today=date(2026, 9, 24), rollover=0).resolve("SILVER")
    assert r.security_id == "429003"


def test_ambiguity_fails_closed(caplog):
    caplog.set_level(logging.ERROR, logger="aureon.resolver")
    text = (FIXTURES / "instrument_master_detailed.csv").read_text()
    dup = text + "MCX,M,999999,NA,FUTCOM,NA,GOLD,GOLD,GOLD OCT FUT DUP,FUTCOM,NA,100,2026-10-05 23:30:00,0.0,NA,1.0,M\n"
    with pytest.raises(SymbolResolutionError) as exc:
        _resolver(text=dup).resolve("GOLD")
    assert "2 candidates" in str(exc.value)
    assert len(exc.value.candidates) == 2
    assert "999999" in caplog.text and "428291" in caplog.text


def test_zero_candidates_fails_closed():
    text = (FIXTURES / "instrument_master_detailed.csv").read_text()
    only_variants = "\n".join(l for l in text.splitlines() if ",GOLD,GOLD," not in l)
    with pytest.raises(SymbolResolutionError, match="zero candidates"):
        _resolver(text=only_variants).resolve("GOLD")
    # all contracts inside rollover window / expired -> fail closed
    with pytest.raises(SymbolResolutionError):
        _resolver(today=date(2027, 3, 4)).resolve("SILVER")


def test_compact_master_layout_supported():
    text = (FIXTURES / "instrument_master_compact.csv").read_text()
    r = _resolver(text=text).resolve("GOLD")
    assert r.security_id == "428291" and r.trading_symbol == "GOLD-Oct2026-FUT"
    s = _resolver(text=text).resolve("SILVER")
    assert s.security_id == "429001"


def test_base_name_derivation_is_exact():
    assert derive_base_name("GOLDM", "GOLDM-Oct2026-FUT", "GOLD") == "GOLDM"
    assert derive_base_name("GOLD", "GOLD-Oct2026-FUT", "GOLD") == "GOLD"
    assert derive_base_name("", "SILVERMIC-Nov2026-FUT", "SILVER") == "SILVERMIC"
    assert derive_base_name("GOLD", "GOLDM-Oct2026-FUT", "GOLD") == ""  # inconsistent -> excluded


def test_unconfigured_symbol_fails_closed():
    with pytest.raises(SymbolResolutionError):
        _resolver().resolve("COPPER")


def test_instrument_provider_cache(tmp_path, monkeypatch):
    from aureon_mcx.broker.dhan.instruments import DhanInstrumentProvider

    text = (FIXTURES / "instrument_master_detailed.csv").read_text()

    class Http:
        calls = 0

        def get_text(self, url, context=None):
            Http.calls += 1
            return text

    now = [datetime(2026, 9, 21, 3, 0, tzinfo=timezone.utc)]
    p = DhanInstrumentProvider("https://example.invalid/m.csv", tmp_path / "m.csv", refresh_hours=24, http=Http(), now=lambda: now[0])
    m1 = p.load()
    assert Http.calls == 1 and m1.source == "download" and len(m1.records) == 14
    m2 = p.load()
    assert Http.calls == 1 and m2.source == "cache"
    now[0] = now[0].replace(day=22, hour=4)
    p.load()
    assert Http.calls == 2  # daily refresh
    p.load(force_refresh=True)
    assert Http.calls == 3
