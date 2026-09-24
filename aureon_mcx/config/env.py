"""Environment (.env) settings.

Credentials are SecretStr and are never rendered by repr/str. They are read
from the environment only; nothing here writes them anywhere.
"""
from __future__ import annotations

from typing import Literal

from pydantic import Field, SecretStr, field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

from aureon_mcx.market.timeframe import Timeframe


def _split_csv(value: str) -> list[str]:
    return [p.strip().upper() for p in value.split(",") if p.strip()]


class EnvSettings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore", case_sensitive=False)

    BROKER: Literal["dhan"] = "dhan"

    DHAN_CLIENT_ID: SecretStr | None = None
    DHAN_ACCESS_TOKEN: SecretStr | None = None

    SYMBOLS: str = "GOLD,SILVER"
    EXCHANGE_SEGMENT: str = "MCX_COMM"
    PRIMARY_TIMEFRAME: str = "M5"

    EMA_FAST: int = Field(default=20, ge=2)
    EMA_SLOW: int = Field(default=50, ge=3)
    RSI_PERIOD: int = Field(default=14, ge=2)
    ATR_PERIOD: int = Field(default=14, ge=1)

    SWING_STRENGTH: int = Field(default=2, ge=1)

    MTF: str = "M5,M15,H1,H4"

    AUREON_STORAGE_BACKEND: Literal["sqlite", "postgres"] = "sqlite"
    AUREON_LOCAL_DB_PATH: str = "data/aureon_mcx.db"
    AUREON_CONFIG_DIR: str = "config"
    AUREON_PARQUET_ARCHIVE: bool = False
    AUREON_PARQUET_DIR: str = "data/parquet"
    AUREON_LOG_LEVEL: str = "INFO"

    DISCORD_TOKEN: SecretStr | None = None
    DISCORD_CHANNEL_ID: int | None = None
    DISCORD_GUILD_ID: int | None = None

    @field_validator("DISCORD_CHANNEL_ID", "DISCORD_GUILD_ID", mode="before")
    @classmethod
    def _blank_to_none(cls, v):
        if v is None:
            return None
        if isinstance(v, str):
            s = v.split("#", 1)[0].strip()
            return int(s) if s else None
        return v

    @field_validator("DHAN_CLIENT_ID", "DHAN_ACCESS_TOKEN", "DISCORD_TOKEN", mode="before")
    @classmethod
    def _blank_secret_to_none(cls, v):
        if isinstance(v, str) and not v.strip():
            return None
        return v

    @property
    def symbols(self) -> list[str]:
        return _split_csv(self.SYMBOLS)

    @property
    def primary_timeframe(self) -> Timeframe:
        return Timeframe.parse(self.PRIMARY_TIMEFRAME)

    @property
    def mtf_timeframes(self) -> list[Timeframe]:
        return [Timeframe.parse(t) for t in _split_csv(self.MTF)]

    @property
    def has_dhan_credentials(self) -> bool:
        return bool(self.DHAN_CLIENT_ID and self.DHAN_ACCESS_TOKEN
                    and self.DHAN_CLIENT_ID.get_secret_value().strip()
                    and self.DHAN_ACCESS_TOKEN.get_secret_value().strip())

    @model_validator(mode="after")
    def _check(self) -> "EnvSettings":
        if self.EMA_FAST >= self.EMA_SLOW:
            raise ValueError("EMA_FAST must be smaller than EMA_SLOW")
        if not self.symbols:
            raise ValueError("SYMBOLS must list at least one logical symbol")
        primary = self.primary_timeframe
        mtf = self.mtf_timeframes
        if primary not in mtf:
            raise ValueError(f"PRIMARY_TIMEFRAME {primary.value} must be included in MTF {self.MTF!r}")
        if Timeframe.M1 in mtf:
            raise ValueError("MTF must not include M1; M1 is a build-only timeframe")
        for t in mtf:
            if t.rank < primary.rank:
                raise ValueError(f"MTF timeframe {t.value} is lower than PRIMARY_TIMEFRAME {primary.value}")
        return self
