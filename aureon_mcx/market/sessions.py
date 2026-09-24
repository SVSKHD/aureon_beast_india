"""Session calendar driven entirely by sessions.yaml (IST windows)."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, time, timedelta
from zoneinfo import ZoneInfo

from aureon_mcx.config.yaml_models import SessionWindow, SessionsConfig, parse_hhmm

from .timeutil import UTC, ensure_utc


@dataclass(frozen=True)
class SessionSpan:
    name: str
    group: str  # "global" | "mcx"
    session_date: date
    start: datetime  # UTC
    end: datetime    # UTC

    def contains(self, ts: datetime) -> bool:
        ts = ensure_utc(ts)
        return self.start <= ts < self.end


class SessionCalendar:
    def __init__(self, config: SessionsConfig):
        self.config = config
        self.tz = ZoneInfo(config.timezone)

    def _local(self, ts: datetime) -> datetime:
        return ensure_utc(ts).astimezone(self.tz)

    def trading_date(self, ts: datetime) -> date:
        """Exchange trading date for a timestamp. Times before the trading-day start
        belong to the previous trading date (covers sessions that run past midnight)."""
        local = self._local(ts)
        start = parse_hhmm(self.config.trading_day.start)
        end = parse_hhmm(self.config.trading_day.end)
        if end <= start and local.time() < start:
            return (local - timedelta(days=1)).date()
        return local.date()

    def _span(self, w: SessionWindow, group: str, session_date: date) -> SessionSpan:
        start = datetime.combine(session_date, w.start_time, tzinfo=self.tz)
        end = datetime.combine(session_date, w.end_time, tzinfo=self.tz)
        if w.crosses_midnight:
            end += timedelta(days=1)
        return SessionSpan(name=w.name, group=group, session_date=session_date, start=start.astimezone(UTC), end=end.astimezone(UTC))

    def spans_for(self, session_date: date) -> list[SessionSpan]:
        out = [self._span(w, "global", session_date) for w in self.config.sessions]
        out += [self._span(w, "mcx", session_date) for w in self.config.mcx_sessions]
        return out

    def session_for(self, ts: datetime, group: str = "global") -> SessionSpan | None:
        d = self.trading_date(ts)
        for candidate_date in (d, d - timedelta(days=1)):
            for s in self.spans_for(candidate_date):
                if s.group == group and s.contains(ts):
                    return s
        return None

    def session_name(self, ts: datetime) -> str | None:
        s = self.session_for(ts, "global")
        return s.name if s else None

    def mcx_session_name(self, ts: datetime) -> str | None:
        s = self.session_for(ts, "mcx")
        return s.name if s else None

    def trading_day_span(self, session_date: date) -> tuple[datetime, datetime]:
        start = datetime.combine(session_date, parse_hhmm(self.config.trading_day.start), tzinfo=self.tz)
        end = datetime.combine(session_date, parse_hhmm(self.config.trading_day.end), tzinfo=self.tz)
        if end <= start:
            end += timedelta(days=1)
        return start.astimezone(UTC), end.astimezone(UTC)

    def trading_day_closed(self, session_date: date, now: datetime) -> bool:
        _, end = self.trading_day_span(session_date)
        return ensure_utc(now) >= end
