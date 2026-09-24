from __future__ import annotations

import pytest

from aureon_mcx.config import ConfigError, load_config
from aureon_mcx.config.env import EnvSettings
from aureon_mcx.config.yaml_models import ContractPolicy
from aureon_mcx.market.timeframe import Timeframe
from tests.conftest import ROOT


def test_load_config_defaults(app_config):
    cfg = app_config
    assert cfg.logical_symbols == ["GOLD", "SILVER"]
    assert cfg.primary_timeframe is Timeframe.M5
    assert cfg.mtf_timeframes == [Timeframe.M5, Timeframe.M15, Timeframe.H1, Timeframe.H4]
    assert cfg.higher_timeframes == [Timeframe.M15, Timeframe.H1, Timeframe.H4]
    assert cfg.symbols.symbols["GOLD"].contract_policy is ContractPolicy.NEAREST_LIQUID
    assert cfg.policy.policy_version == "strict-research-v1"
    assert cfg.analysis.execution.enabled is False
    assert cfg.analysis.declutter.max_labeled_detections == 4
    assert cfg.env.has_dhan_credentials is False


def test_secrets_never_rendered(monkeypatch, tmp_path):
    monkeypatch.setenv("DHAN_CLIENT_ID", "client-123")
    monkeypatch.setenv("DHAN_ACCESS_TOKEN", "super-secret-token")
    env = EnvSettings(_env_file=str(tmp_path / "none.env"))
    assert env.has_dhan_credentials
    assert "super-secret-token" not in repr(env)
    assert "super-secret-token" not in str(env)
    assert "client-123" not in repr(env)


def test_env_validation_fails_closed(monkeypatch, tmp_path):
    monkeypatch.setenv("EMA_FAST", "60")
    with pytest.raises(ConfigError):
        load_config(config_dir=ROOT / "config", env_file=tmp_path / "none.env")


def test_primary_must_be_in_mtf(monkeypatch, tmp_path):
    monkeypatch.setenv("MTF", "M15,H1")
    with pytest.raises(ConfigError):
        load_config(config_dir=ROOT / "config", env_file=tmp_path / "none.env")


def test_unknown_symbol_fails_closed(monkeypatch, tmp_path):
    monkeypatch.setenv("SYMBOLS", "GOLD,COPPER")
    with pytest.raises(ConfigError, match="COPPER"):
        load_config(config_dir=ROOT / "config", env_file=tmp_path / "none.env")


def test_yaml_typo_fails_closed(tmp_path, monkeypatch):
    import shutil

    cdir = tmp_path / "config"
    shutil.copytree(ROOT / "config", cdir)
    (cdir / "confirmation_policy.yaml").write_text(
        'policy_version: "x"\nrules:\n  require_ema_alignmnt: true\n', encoding="utf-8"
    )
    with pytest.raises(ConfigError):
        load_config(config_dir=cdir, env_file=tmp_path / "none.env")


def test_missing_yaml_fails_closed(tmp_path):
    with pytest.raises(ConfigError, match="missing config file"):
        load_config(config_dir=tmp_path, env_file=tmp_path / "none.env")


def test_blank_discord_ids_are_none(monkeypatch, tmp_path):
    monkeypatch.setenv("DISCORD_CHANNEL_ID", "")
    monkeypatch.setenv("DISCORD_GUILD_ID", "   # optional")
    env = EnvSettings(_env_file=str(tmp_path / "none.env"))
    assert env.DISCORD_CHANNEL_ID is None and env.DISCORD_GUILD_ID is None
