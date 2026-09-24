"""Exchange calendar + session windows, driven entirely by configuration.

`SessionCalendar` is the single place that answers:
  * is the exchange open at this instant?
  * which trading date does a timestamp belong to?
  * when does that trading day start / end (holidays and overrides applied)?
  * which named session (ASIA / LONDON / ... and MCX MORNING / EVENING) is active?
  * how are session-aware H4 buckets laid out for a trading day?
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, time, timedelta
from zoneinfo import ZoneInfo

from aureon_mcx.config.yaml_models import SessionWindow, SessionsConfig, parse_hhmm

from .timeutil import UTC, ensure_utc

H4_SECONDS = 4 * 3600


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
        ov = config.overrides
        self._holidays = {date.fromisoformat(d) for d in ov.holidays}
        self._overrides = {date.fromisoformat(o.date): o for o in ov.overrides}
        self._weekend_closed = ov.weekend_closed

    # ------------------------------------------------------------ basics
    def _local(self, ts: datetime) -> datetime:
        return ensure_utc(ts).astimezone(self.tz)

    def _day_times(self, session_date: date) -> tuple[time, time] | None:
        """(start, end) local times for a trading date, or None when closed."""
        o = self._overrides.get(session_date)
        if o is not None and o.closed:
            return None
        if session_date in self._holidays and (o is None):
            return None
        if self._weekend_closed and session_date.weekday() >= 5 and o is None:
            return None
        start = parse_hhmm(o.start) if (o and o.start) else parse_hhmm(self.config.trading_day.start)
        end = parse_hhmm(o.end) if (o and o.end) else parse_hhmm(self.config.trading_day.end)
        return start, end

    def is_trading_day(self, session_date: date) -> bool:
        return self._day_times(session_date) is not None

    def trading_date(self, ts: datetime) -> date:
        """Exchange trading date for a timestamp. Times before the configured trading-day
        start belong to the previous date when the day runs past midnight."""
        local = self._local(ts)
        start = parse_hhmm(self.config.trading_day.start)
        end = parse_hhmm(self.config.trading_day.end)
        if end <= start and local.time() < start:
            return (local - timedelta(days=1)).date()
        return local.date()

    def trading_day_span(self, session_date: date) -> tuple[datetime, datetime]:
        """UTC (start, end) of the trading day. Closed days return the configured
        default span so callers still get a deterministic window; use
        `is_trading_day` to know whether anything is expected inside it."""
        times = self._day_times(session_date)
        if times is None:
            start_t, end_t = parse_hhmm(self.config.trading_day.start), parse_hhmm(self.config.trading_day.end)
        else:
            start_t, end_t = times
        start = datetime.combine(session_date, start_t, tzinfo=self.tz)
        end = datetime.combine(session_date, end_t, tzinfo=self.tz)
        if end <= start:
            end += timedelta(days=1)
        return start.astimezone(UTC), end.astimezone(UTC)

    def session_end(self, ts: datetime) -> datetime:
        return self.trading_day_span(self.trading_date(ts))[1]

    def is_open(self, ts: datetime) -> bool:
        d = self.trading_date(ts)
        if not self.is_trading_day(d):
            return False
        start, end = self.trading_day_span(d)
        return start <= ensure_utc(ts) < end

    def trading_day_closed(self, session_date: date, now: datetime) -> bool:
        _, end = self.trading_day_span(session_date)
        return ensure_utc(now) >= end

    # ------------------------------------------------------- H4 policy
    def h4_bucket(self, ts: datetime) -> tuple[datetime, datetime]:
        """Session-aware H4 bucket: anchored at the trading-day start, 4h steps, the last
        bucket capped at the session end (e.g. 09-13, 13-17, 17-21, 21-23:30 IST).

        # DECISION: MCX H4 bars are anchored to the configured exchange day start
        # rather than the wall clock, so the first H4 of the day is 09:00-13:00
        # and never a synthetic 08:00-12:00 clock bucket.
        """
        d = self.trading_date(ts)
        day_start, day_end = self.trading_day_span(d)
        t = ensure_utc(ts)
        k = int((t - day_start).total_seconds() // H4_SECONDS) if t >= day_start else -1
        if k < 0:  # before the day start (should not happen for exchange data): previous clock bucket
            start = day_start - timedelta(seconds=H4_SECONDS)
            return start, day_start
        start = day_start + timedelta(seconds=H4_SECONDS * k)
        end = min(start + timedelta(seconds=H4_SECONDS), day_end) if start < day_end else start + timedelta(seconds=H4_SECONDS)
        return start, end

    def h4_buckets(self, session_date: date) -> list[tuple[datetime, datetime]]:
        day_start, day_end = self.trading_day_span(session_date)
        out = []
        start = day_start
        while start < day_end:
            end = min(start + timedelta(seconds=H4_SECONDS), day_end)
            out.append((start, end))
            start = end
        return out

    # --------------------------------------------------- named sessions
    def _span(self, w: SessionWindow, group: str, session_date: date) -> SessionSpan:
        start = datetime.combine(session_date, w.start_time, tzinfo=self.tz)
        end = datetime.combine(session_date, w.end_time, tzinfo=self.tz)
        if w.crosses_midnight:
            end += timedelta(days=1)
        # windows never extend past the (possibly shortened) trading day
        _, day_end = self.trading_day_span(session_date)
        end_utc = min(end.astimezone(UTC), day_end) if self.is_trading_day(session_date) else end.astimezone(UTC)
        return SessionSpan(name=w.name, group=group, session_date=session_date, start=start.astimezone(UTC), end=max(end_utc, start.astimezone(UTC)))

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
