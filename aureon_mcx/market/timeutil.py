"""Timezone helpers. Storage is UTC; presentation is Asia/Kolkata."""
from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from zoneinfo import ZoneInfo

UTC = timezone.utc
IST = ZoneInfo("Asia/Kolkata")


def utc_now() -> datetime:
    return datetime.now(tz=UTC)


def ensure_utc(dt: datetime) -> datetime:
    if dt.tzinfo is None:
        raise ValueError("naive datetime is not allowed; all timestamps must be timezone-aware")
    return dt.astimezone(UTC)


def to_ist(dt: datetime) -> datetime:
    return ensure_utc(dt).astimezone(IST)


def to_db(dt: datetime | None) -> str | None:
    """Canonical sortable UTC ISO-8601 text for SQLite."""
    if dt is None:
        return None
    return ensure_utc(dt).replace(microsecond=0).strftime("%Y-%m-%dT%H:%M:%S+00:00")


def from_db(value: str | None) -> datetime | None:
    if value is None:
        return None
    dt = datetime.fromisoformat(value)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)
    return dt.astimezone(UTC)


def from_epoch(seconds: int | float) -> datetime:
    return datetime.fromtimestamp(float(seconds), tz=UTC)


def ist_date(dt: datetime) -> date:
    return to_ist(dt).date()


def floor_to(dt: datetime, seconds: int, tz=IST) -> datetime:
    """Floor a timestamp to a bucket of `seconds` measured from local midnight in `tz`.

    MCX candles are aligned to exchange wall-clock (IST) boundaries, and IST is
    UTC+05:30, so hour buckets must be floored in local time, not UTC.
    """
    local = ensure_utc(dt).astimezone(tz)
    midnight = local.replace(hour=0, minute=0, second=0, microsecond=0)
    elapsed = int((local - midnight).total_seconds())
    floored = midnight + timedelta(seconds=(elapsed // seconds) * seconds)
    return floored.astimezone(UTC)


def fmt_ist(dt: datetime | None, fmt: str = "%Y-%m-%d %H:%M IST") -> str:
    if dt is None:
        return "n/a"
    return to_ist(dt).strftime(fmt)
