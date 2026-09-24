"""Config loader: env + YAML -> validated AppConfig. Fails closed on any error."""
from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml
from pydantic import ValidationError

from aureon_mcx.market.timeframe import Timeframe

from .env import EnvSettings
from .yaml_models import AnalysisConfig, ConfirmationPolicyConfig, ExchangeCalendarConfig, SessionOverridesConfig, SessionsConfig, SymbolsConfig


class ConfigError(RuntimeError):
    """Raised when configuration is missing or invalid. Startup must abort."""


@dataclass(frozen=True)
class AppConfig:
    env: EnvSettings
    symbols: SymbolsConfig
    sessions: SessionsConfig
    policy: ConfirmationPolicyConfig
    analysis: AnalysisConfig
    config_dir: Path

    @property
    def logical_symbols(self) -> list[str]:
        return self.env.symbols

    @property
    def primary_timeframe(self) -> Timeframe:
        return self.env.primary_timeframe

    @property
    def mtf_timeframes(self) -> list[Timeframe]:
        return self.env.mtf_timeframes

    @property
    def higher_timeframes(self) -> list[Timeframe]:
        p = self.primary_timeframe
        return [t for t in self.mtf_timeframes if t.is_higher_than(p)]


def _read_yaml(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise ConfigError(f"missing config file: {path}")
    try:
        with path.open("r", encoding="utf-8") as fh:
            data = yaml.safe_load(fh) or {}
    except yaml.YAMLError as exc:
        raise ConfigError(f"invalid YAML in {path}: {exc}") from exc
    if not isinstance(data, dict):
        raise ConfigError(f"top level of {path} must be a mapping")
    return data


def _validate(model, data: dict[str, Any], path: Path):
    try:
        return model.model_validate(data)
    except ValidationError as exc:
        raise ConfigError(f"invalid config {path}: {exc}") from exc


def load_env(env_file: str | os.PathLike | None = None) -> EnvSettings:
    try:
        if env_file is not None:
            return EnvSettings(_env_file=str(env_file))  # type: ignore[call-arg]
        return EnvSettings()
    except ValidationError as exc:
        raise ConfigError(f"invalid environment configuration: {exc}") from exc


def load_config(config_dir: str | os.PathLike | None = None, env_file: str | os.PathLike | None = None) -> AppConfig:
    env = load_env(env_file)
    cdir = Path(config_dir) if config_dir is not None else Path(env.AUREON_CONFIG_DIR)
    symbols = _validate(SymbolsConfig, _read_yaml(cdir / "symbols.yaml"), cdir / "symbols.yaml")
    sessions = _validate(SessionsConfig, _read_yaml(cdir / "sessions.yaml"), cdir / "sessions.yaml")
    overrides_path = cdir / "session_overrides.yaml"
    if overrides_path.exists():
        overrides = _validate(SessionOverridesConfig, _read_yaml(overrides_path), overrides_path)
        sessions = sessions.model_copy(update={"overrides": overrides})
    calendar_path = cdir / "exchange_calendar.yaml"
    if calendar_path.exists():
        calendar = _validate(ExchangeCalendarConfig, _read_yaml(calendar_path), calendar_path)
        sessions = sessions.model_copy(update={"calendar": calendar})
    policy = _validate(ConfirmationPolicyConfig, _read_yaml(cdir / "confirmation_policy.yaml"), cdir / "confirmation_policy.yaml")
    analysis_path = cdir / "analysis.yaml"
    analysis = _validate(AnalysisConfig, _read_yaml(analysis_path) if analysis_path.exists() else {}, analysis_path)

    # Cross-file checks (fail closed).
    for sym in env.symbols:
        if sym not in symbols.symbols:
            raise ConfigError(f"SYMBOLS lists {sym!r} but symbols.yaml has no entry for it")
        spec = symbols.symbols[sym]
        if spec.exchange_segment != env.EXCHANGE_SEGMENT:
            raise ConfigError(
                f"symbol {sym}: exchange_segment {spec.exchange_segment!r} != EXCHANGE_SEGMENT {env.EXCHANGE_SEGMENT!r}"
            )
    for tf in env.mtf_timeframes:
        if tf.dhan_interval is None and tf != Timeframe.H4:
            raise ConfigError(f"timeframe {tf.value} is neither downloadable nor aggregatable")
    if env.primary_timeframe not in analysis.historical.warmup_bars:
        raise ConfigError(f"analysis.historical.warmup_bars has no entry for primary timeframe {env.primary_timeframe.value}")
    return AppConfig(env=env, symbols=symbols, sessions=sessions, policy=policy, analysis=analysis, config_dir=cdir)
