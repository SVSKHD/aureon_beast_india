"""Pydantic models for the YAML configuration files.

All models are strict (`extra="forbid"`) so a typo in YAML fails closed at
startup instead of silently disabling a rule.
"""
from __future__ import annotations

import re
from datetime import time
from enum import Enum
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from aureon_mcx.market.timeframe import Timeframe

_HHMM = re.compile(r"^([01]\d|2[0-3]):([0-5]\d)$")


def parse_hhmm(value: str) -> time:
    m = _HHMM.match(value.strip())
    if not m:
        raise ValueError(f"expected HH:MM, got {value!r}")
    return time(int(m.group(1)), int(m.group(2)))


class StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


# ---------------------------------------------------------------- symbols.yaml
class ContractPolicy(str, Enum):
    """Contract-selection policy. Enum so new policies can be added later."""

    NEAREST_LIQUID = "nearest_liquid"


class SymbolSpec(StrictModel):
    underlying: str
    exchange_segment: str = "MCX_COMM"
    instrument_type: str = "FUTCOM"
    contract_policy: ContractPolicy = ContractPolicy.NEAREST_LIQUID
    # Optional per-symbol override of the global rollover window.
    rollover_days_before_expiry: int | None = Field(default=None, ge=0)

    @field_validator("underlying")
    @classmethod
    def _upper(cls, v: str) -> str:
        v = v.strip().upper()
        if not v:
            raise ValueError("underlying must not be blank")
        return v


class InstrumentMasterSpec(StrictModel):
    url: str
    cache_path: str = "data/cache/instrument_master.csv"
    refresh_hours: float = Field(default=24.0, gt=0)


class SymbolsConfig(StrictModel):
    instrument_master: InstrumentMasterSpec
    rollover_days_before_expiry: int = Field(default=3, ge=0)
    symbols: dict[str, SymbolSpec]

    @field_validator("symbols")
    @classmethod
    def _keys_upper(cls, v: dict[str, SymbolSpec]) -> dict[str, SymbolSpec]:
        out: dict[str, SymbolSpec] = {}
        for k, spec in v.items():
            key = k.strip().upper()
            if key in out:
                raise ValueError(f"duplicate symbol key {key}")
            out[key] = spec
        if not out:
            raise ValueError("symbols must not be empty")
        return out

    def rollover_days(self, logical: str) -> int:
        spec = self.symbols[logical]
        return spec.rollover_days_before_expiry if spec.rollover_days_before_expiry is not None else self.rollover_days_before_expiry


# --------------------------------------------------------------- sessions.yaml
class SessionWindow(StrictModel):
    name: str
    start: str
    end: str

    @field_validator("name")
    @classmethod
    def _name(cls, v: str) -> str:
        v = v.strip().upper().replace(" ", "_")
        if not v:
            raise ValueError("session name must not be blank")
        return v

    @field_validator("start", "end")
    @classmethod
    def _hhmm(cls, v: str) -> str:
        parse_hhmm(v)
        return v.strip()

    @property
    def start_time(self) -> time:
        return parse_hhmm(self.start)

    @property
    def end_time(self) -> time:
        return parse_hhmm(self.end)

    @property
    def crosses_midnight(self) -> bool:
        return self.end_time <= self.start_time


class TradingDaySpec(StrictModel):
    start: str = "09:00"
    end: str = "23:30"

    @field_validator("start", "end")
    @classmethod
    def _hhmm(cls, v: str) -> str:
        parse_hhmm(v)
        return v.strip()


class TrendSpec(StrictModel):
    min_move_atr: float = Field(default=0.25, ge=0)
    present_lookback_bars: int = Field(default=6, ge=1)


class SessionOverride(StrictModel):
    """Date-specific exchange rule: a full holiday or a special/partial session."""

    date: str
    closed: bool = False
    start: str | None = None
    end: str | None = None
    note: str = ""

    @field_validator("date")
    @classmethod
    def _date(cls, v: str) -> str:
        from datetime import date as _date

        _date.fromisoformat(v.strip())
        return v.strip()

    @field_validator("start", "end")
    @classmethod
    def _hhmm(cls, v: str | None) -> str | None:
        if v is None:
            return None
        parse_hhmm(v)
        return v.strip()

    @model_validator(mode="after")
    def _consistent(self) -> "SessionOverride":
        if self.closed and (self.start or self.end):
            raise ValueError(f"override {self.date}: closed=true cannot also set start/end")
        if not self.closed and not (self.start or self.end):
            raise ValueError(f"override {self.date}: must set closed=true or a start/end")
        return self


class SessionOverridesConfig(StrictModel):
    """Optional `session_overrides.yaml`: holidays and date-specific sessions."""

    weekend_closed: bool = True
    holidays: list[str] = Field(default_factory=list)
    overrides: list[SessionOverride] = Field(default_factory=list)

    @field_validator("holidays")
    @classmethod
    def _holidays(cls, v: list[str]) -> list[str]:
        from datetime import date as _date

        out = []
        for d in v:
            _date.fromisoformat(str(d).strip())
            out.append(str(d).strip())
        return out

    @model_validator(mode="after")
    def _unique_dates(self) -> "SessionOverridesConfig":
        dates = [o.date for o in self.overrides]
        if len(dates) != len(set(dates)):
            raise ValueError("session overrides must have unique dates")
        return self


class CalendarSessionSpec(StrictModel):
    start: str = "09:00"
    evening_start: str = "17:00"
    end: str = "23:55"

    @field_validator("start", "evening_start", "end")
    @classmethod
    def _hhmm(cls, v: str) -> str:
        parse_hhmm(v)
        return v.strip()

    @model_validator(mode="after")
    def _order(self) -> "CalendarSessionSpec":
        if not (parse_hhmm(self.start) < parse_hhmm(self.evening_start) < parse_hhmm(self.end)):
            raise ValueError("default_session must satisfy start < evening_start < end")
        return self


class ClosePeriod(StrictModel):
    """Inclusive date range with its own trading-day end (seasonal close, e.g. US DST)."""

    from_date: str = Field(alias="from")
    to_date: str = Field(alias="to")
    end: str
    note: str = ""

    model_config = ConfigDict(extra="forbid", frozen=True, populate_by_name=True)

    @field_validator("from_date", "to_date")
    @classmethod
    def _date(cls, v: str) -> str:
        from datetime import date as _date

        _date.fromisoformat(v.strip())
        return v.strip()

    @field_validator("end")
    @classmethod
    def _hhmm(cls, v: str) -> str:
        parse_hhmm(v)
        return v.strip()

    @model_validator(mode="after")
    def _range(self) -> "ClosePeriod":
        if self.to_date < self.from_date:
            raise ValueError(f"close period {self.from_date}..{self.to_date} is reversed")
        return self


class CalendarHoliday(StrictModel):
    date: str
    name: str
    closed: Literal["full", "morning", "evening"]
    note: str = ""

    @field_validator("date")
    @classmethod
    def _date(cls, v: str) -> str:
        from datetime import date as _date

        _date.fromisoformat(v.strip())
        return v.strip()


class CalendarSpecialSession(StrictModel):
    date: str
    start: str
    end: str
    note: str = ""

    @field_validator("date")
    @classmethod
    def _date(cls, v: str) -> str:
        from datetime import date as _date

        _date.fromisoformat(v.strip())
        return v.strip()

    @field_validator("start", "end")
    @classmethod
    def _hhmm(cls, v: str) -> str:
        parse_hhmm(v)
        return v.strip()

    @model_validator(mode="after")
    def _order(self) -> "CalendarSpecialSession":
        if parse_hhmm(self.end) <= parse_hhmm(self.start):
            raise ValueError(f"special session {self.date}: end must be after start")
        return self


class ExchangeCalendarConfig(StrictModel):
    """`config/exchange_calendar.yaml`: the authoritative exchange calendar (fails loudly if malformed)."""

    exchange: str = "MCX"
    segment: str = "commodity_derivatives_non_agri"
    year: int | None = None
    verified_against_official_circular: bool = False
    weekend_closed: bool = True
    default_session: CalendarSessionSpec = CalendarSessionSpec()
    close_periods: list[ClosePeriod] = Field(default_factory=list)
    holidays: list[CalendarHoliday] = Field(default_factory=list)
    special_sessions: list[CalendarSpecialSession] = Field(default_factory=list)

    @model_validator(mode="after")
    def _consistent(self) -> "ExchangeCalendarConfig":
        dates = [h.date for h in self.holidays]
        if len(dates) != len(set(dates)):
            raise ValueError("exchange calendar: duplicate holiday dates")
        specials = [x.date for x in self.special_sessions]
        if len(specials) != len(set(specials)):
            raise ValueError("exchange calendar: duplicate special session dates")
        periods = sorted(self.close_periods, key=lambda p: p.from_date)
        for a, b in zip(periods, periods[1:]):
            if b.from_date <= a.to_date:
                raise ValueError(f"exchange calendar: overlapping close periods {a.from_date}..{a.to_date} and {b.from_date}..{b.to_date}")
        for p in self.close_periods:
            if parse_hhmm(p.end) <= parse_hhmm(self.default_session.evening_start):
                raise ValueError(f"exchange calendar: close period end {p.end} must be after evening_start")
        # An unpopulated calendar would silently treat every holiday as an open market day and
        # every DST-season evening as open until 23:55: refuse it instead of guessing.
        if not self.holidays:
            raise ValueError("exchange calendar: no holidays listed; populate it from the MCX circular")
        if self.year is None:
            raise ValueError("exchange calendar: 'year' is required")
        all_dates = dates + specials + [p.from_date for p in self.close_periods] + [p.to_date for p in self.close_periods]
        wrong_year = [d for d in all_dates if not str(d).startswith(f"{self.year}-")]
        if wrong_year:
            raise ValueError(f"exchange calendar: dates outside year {self.year}: {', '.join(str(d) for d in wrong_year[:5])}")
        return self


class SessionsConfig(StrictModel):
    timezone: str = "Asia/Kolkata"
    trading_day: TradingDaySpec = TradingDaySpec()
    sessions: list[SessionWindow]
    mcx_sessions: list[SessionWindow] = Field(default_factory=list)
    trend: TrendSpec = TrendSpec()
    # Legacy date overrides (config/session_overrides.yaml); applied on top of the calendar.
    overrides: SessionOverridesConfig = SessionOverridesConfig()
    # Authoritative exchange calendar (config/exchange_calendar.yaml); the loader requires the
    # file. None only for unit tests that build a SessionsConfig by hand (weekday defaults apply).
    calendar: ExchangeCalendarConfig | None = None

    @field_validator("timezone")
    @classmethod
    def _tz(cls, v: str) -> str:
        from zoneinfo import ZoneInfo

        ZoneInfo(v)  # raises if unknown
        return v

    @model_validator(mode="after")
    def _unique_names(self) -> "SessionsConfig":
        names = [s.name for s in self.sessions] + [s.name for s in self.mcx_sessions]
        if len(names) != len(set(names)):
            raise ValueError(f"session names must be unique across sessions and mcx_sessions: {names}")
        if not self.sessions:
            raise ValueError("at least one session window is required")
        return self


# --------------------------------------------------- confirmation_policy.yaml
class PolicyRules(StrictModel):
    require_lifecycle_confirmed: bool = True
    require_ema_alignment: bool = True
    require_rsi_side: bool = True
    require_present_trend: bool = True
    require_mtf_alignment: bool = True
    require_higher_tf_context: bool = True
    require_structure_not_opposing: bool = True
    block_on_fakeout_risk: bool = True


class ConfirmationPolicyConfig(StrictModel):
    """INITIAL STRICT RESEARCH POLICY - not a proven trading edge."""

    policy_version: str
    rsi_threshold: float = Field(default=50.0, ge=0, le=100)
    rules: PolicyRules = PolicyRules()

    @field_validator("policy_version")
    @classmethod
    def _pv(cls, v: str) -> str:
        if not v.strip():
            raise ValueError("policy_version must not be blank")
        return v.strip()


# --------------------------------------------------------------- analysis.yaml
class EmaDetectionSpec(StrictModel):
    approach_atr_fraction: float = Field(default=0.5, gt=0)
    min_shrinking_bars: int = Field(default=2, ge=1)


class StructureSpec(StrictModel):
    equal_tolerance_mode: Literal["atr_fraction", "ticks"] = "atr_fraction"
    equal_tolerance_value: float = Field(default=0.1, ge=0)
    dominance_lookback: int = Field(default=4, ge=2)


class WickSpec(StrictModel):
    min_wick_range_fraction: float = Field(default=0.6, gt=0, le=1)
    max_body_range_fraction: float = Field(default=0.3, ge=0, le=1)
    min_range_atr: float = Field(default=0.8, ge=0)


class LiquiditySpec(StrictModel):
    proximity_atr_fraction: float = Field(default=0.25, gt=0)
    equal_level_tolerance_atr: float = Field(default=0.1, ge=0)


class BreakoutSpec(StrictModel):
    acceptance_closes: int = Field(default=2, ge=1)
    retest_tolerance_atr: float = Field(default=0.25, ge=0)
    completion_bars: int = Field(default=12, ge=1)
    max_age_bars: int = Field(default=48, ge=1)


class RsiEventsSpec(StrictModel):
    flat_tolerance: float = Field(default=0.5, ge=0)
    levels: list[float] = Field(default_factory=lambda: [30.0, 50.0, 70.0])


class MtfSpec(StrictModel):
    require_structure: bool = True


class DeclutterSpec(StrictModel):
    max_labeled_detections: int = Field(default=4, ge=0)
    max_labeled_setup_events: int = Field(default=3, ge=0)
    context_panel_lines: int = Field(default=10, ge=1)
    chart_bars: int = Field(default=120, ge=20)


class HorizonsSpec(StrictModel):
    bars: list[int] = Field(default_factory=lambda: [1, 3, 6, 12])
    include_session: bool = True

    @field_validator("bars")
    @classmethod
    def _bars(cls, v: list[int]) -> list[int]:
        if not v or any(b <= 0 for b in v) or sorted(set(v)) != v:
            raise ValueError("horizons.bars must be a strictly increasing list of positive ints")
        return v


class ContractSpec(StrictModel):
    lot_size: int = Field(ge=1)
    point_value: float = Field(gt=0)
    quantity_lots: int = Field(ge=1)


class OutcomesSpec(StrictModel):
    min_cohort_sample: int = Field(default=30, ge=1)
    follow_through_atr: float = Field(default=1.0, gt=0)
    invalidation_atr: float = Field(default=1.0, gt=0)
    contract_specs: dict[str, ContractSpec] = Field(default_factory=dict)


class HistoricalSpec(StrictModel):
    warmup_bars: dict[Timeframe, int] = Field(default_factory=lambda: {Timeframe.M5: 600, Timeframe.M15: 300, Timeframe.H1: 200})
    max_days_per_request: int = Field(default=5, ge=1, le=90)
    # Zero-trade fill: when the broker's M1 response verifiably spans an open-market minute
    # (bars before AND after it inside the fetched range) but has no bar for it, treat it as a
    # minute with no trades and insert a flat zero-volume bar. Dhan does not document this
    # guarantee, so the safe default is False: the minute stays unresolved and the symbol
    # untrusted until the broker returns it.
    allow_verified_zero_trade_fill: bool = False
    # Bounded retries for broker M1 verification / recovery before the symbol is marked ERROR.
    verification_retries: int = Field(default=3, ge=1)


class DiscordSpec(StrictModel):
    debounce_seconds: float = Field(default=5.0, ge=0)


class ExecutionSpec(StrictModel):
    enabled: bool = False


class AnalysisConfig(StrictModel):
    ema_detection: EmaDetectionSpec = EmaDetectionSpec()
    structure: StructureSpec = StructureSpec()
    wick: WickSpec = WickSpec()
    liquidity: LiquiditySpec = LiquiditySpec()
    breakout: BreakoutSpec = BreakoutSpec()
    rsi_events: RsiEventsSpec = RsiEventsSpec()
    mtf: MtfSpec = MtfSpec()
    declutter: DeclutterSpec = DeclutterSpec()
    horizons: HorizonsSpec = HorizonsSpec()
    outcomes: OutcomesSpec = OutcomesSpec()
    historical: HistoricalSpec = HistoricalSpec()
    discord: DiscordSpec = DiscordSpec()
    execution: ExecutionSpec = ExecutionSpec()
