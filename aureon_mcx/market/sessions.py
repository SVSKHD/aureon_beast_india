"""Exchange calendar + session windows, driven entirely by configuration.

`SessionCalendar` is the single place that answers:
  * is the exchange open at this instant, and if not WHY (weekend / holiday / session closed /
    outside hours / special session pending / calendar out of range)?
  * which trading date does a timestamp belong to?
  * when does that trading day start / end (holidays and overrides applied)?
  * which named session (ASIA / LONDON / ... and MCX MORNING / EVENING) is active?
  * how are session-aware H4 buckets laid out for a trading day?
  * when is the next open / close transition?

Calendar coverage: every configured calendar year is known; a date outside them is
CALENDAR_OUT_OF_RANGE. Such a date is reported CLOSED (nothing is expected, nothing is
trusted) and `require_coverage()` raises `CalendarOutOfRange` so startup / live mode fail
closed instead of assuming normal trading hours for a year nobody configured.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, time, timedelta
from zoneinfo import ZoneInfo

from aureon_mcx.config.yaml_models import ExchangeCalendarConfig, SessionWindow, SessionsConfig, parse_hhmm

from .timeutil import UTC, ensure_utc

H4_SECONDS = 4 * 3600

# market-state reasons (closed) - stable identifiers used by the API and Discord
OPEN = "OPEN"
WEEKEND = "WEEKEND"
FULL_HOLIDAY = "FULL_HOLIDAY"
MORNING_SESSION_CLOSED = "MORNING_SESSION_CLOSED"
EVENING_SESSION_CLOSED = "EVENING_SESSION_CLOSED"
OUTSIDE_TRADING_HOURS = "OUTSIDE_TRADING_HOURS"
SPECIAL_SESSION_PENDING = "SPECIAL_SESSION_PENDING"
CALENDAR_OUT_OF_RANGE = "CALENDAR_OUT_OF_RANGE"
CLOSED_BY_OVERRIDE = "CLOSED_BY_OVERRIDE"


class CalendarOutOfRange(RuntimeError):
    """The trading date is not covered by any configured exchange calendar year."""

    def __init__(self, session_date: date, years: list[int]):
        self.session_date = session_date
        self.years = years
        super().__init__(f"MCX calendar for {session_date.year} not installed (configured years: {', '.join(map(str, years)) or 'none'})")


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


@dataclass(frozen=True)
class MarketState:
    state: str                      # OPEN | CLOSED
    reason: str                     # OPEN or one of the closed reasons above
    trading_date: date
    session: str | None             # MORNING | EVENING | special session note | None
    opens_at: datetime | None       # today's (or the special) session start, UTC
    closes_at: datetime | None      # today's session end, UTC
    next_open: datetime | None      # next instant the exchange opens (>= now), UTC
    next_close: datetime | None     # next instant it closes (>= now), UTC
    holiday: str | None
    detail: str
    calendar_verified: bool
    calendar_year_covered: bool

    @property
    def is_open(self) -> bool:
        return self.state == "OPEN"

    def to_dict(self) -> dict:
        return {"state": self.state, "reason": self.reason, "trading_date": self.trading_date.isoformat(), "session": self.session,
                "opens_at": self.opens_at.isoformat() if self.opens_at else None, "closes_at": self.closes_at.isoformat() if self.closes_at else None,
                "next_open": self.next_open.isoformat() if self.next_open else None, "next_close": self.next_close.isoformat() if self.next_close else None,
                "holiday": self.holiday, "detail": self.detail, "calendar_verified": self.calendar_verified,
                "calendar_year_covered": self.calendar_year_covered}


class SessionCalendar:
    def __init__(self, config: SessionsConfig):
        self.config = config
        self.tz = ZoneInfo(config.timezone)
        ov = config.overrides
        self._legacy_holidays = {date.fromisoformat(d) for d in ov.holidays}
        self._legacy_overrides = {date.fromisoformat(o.date): o for o in ov.overrides}
        cals: dict[int, ExchangeCalendarConfig] = dict(config.calendars)
        if config.calendar is not None and config.calendar.year is not None:
            cals[config.calendar.year] = config.calendar  # the explicit calendar wins for its year
        self._calendars = cals
        self.years = sorted(cals)
        self._weekend_closed = ov.weekend_closed and all(c.weekend_closed for c in cals.values())
        self._cal_holidays = {date.fromisoformat(h.date): h for c in cals.values() for h in c.holidays}
        self._cal_specials = {date.fromisoformat(x.date): x for c in cals.values() for x in c.special_sessions}
        self._cal_pending = {date.fromisoformat(x.date): x for c in cals.values() for x in c.pending_special_sessions}
        self._close_periods = [(date.fromisoformat(p.from_date), date.fromisoformat(p.to_date), parse_hhmm(p.end))
                               for c in cals.values() for p in c.close_periods]
        primary = cals[max(cals)] if cals else None
        if primary is not None:
            self._default_start = parse_hhmm(primary.default_session.start)
            self._default_end = parse_hhmm(primary.default_session.end)
            self._evening_start = parse_hhmm(primary.default_session.evening_start)
        else:
            self._default_start = parse_hhmm(config.trading_day.start)
            self._default_end = parse_hhmm(config.trading_day.end)
            self._evening_start = parse_hhmm("17:00")

    # ------------------------------------------------------------ coverage
    @property
    def verified(self) -> bool:
        return bool(self._calendars) and all(c.verified_against_official_circular for c in self._calendars.values())

    def covers(self, session_date: date) -> bool:
        """True when a configured calendar year covers the date (no calendar at all = legacy, uncovered-free)."""
        return not self._calendars or session_date.year in self._calendars

    def require_coverage(self, ts_or_date: datetime | date) -> date:
        d = ts_or_date if isinstance(ts_or_date, date) and not isinstance(ts_or_date, datetime) else self.trading_date(ts_or_date)
        if not self.covers(d):
            raise CalendarOutOfRange(d, self.years)
        return d

    def calendar_for(self, session_date: date) -> ExchangeCalendarConfig | None:
        return self._calendars.get(session_date.year)

    def _session_times(self, session_date: date) -> tuple[time, time, time]:
        c = self._calendars.get(session_date.year)
        if c is None:
            return self._default_start, self._evening_start, self._default_end
        return parse_hhmm(c.default_session.start), parse_hhmm(c.default_session.evening_start), parse_hhmm(c.default_session.end)

    # ------------------------------------------------------------ basics
    def _local(self, ts: datetime) -> datetime:
        return ensure_utc(ts).astimezone(self.tz)

    def regular_end(self, session_date: date) -> time:
        """Trading-day end for a date ignoring holidays: seasonal close periods, else the default."""
        for start_d, end_d, end_t in self._close_periods:
            if start_d <= session_date <= end_d:
                return end_t
        return self._session_times(session_date)[2]

    def _day_times(self, session_date: date) -> tuple[time, time] | None:
        times, _reason = self._resolve_day(session_date)
        return times

    def _resolve_day(self, session_date: date) -> tuple[tuple[time, time] | None, str]:
        """(start, end) local times for a trading date (None when closed) plus the reason.

        Resolution order: calendar coverage > legacy date override > special session > pending
        special session > holiday (full / morning / evening) > weekend closure > seasonal close
        period > default session.
        """
        if not self.covers(session_date):
            return None, CALENDAR_OUT_OF_RANGE
        start_t, evening_t, _ = self._session_times(session_date)
        o = self._legacy_overrides.get(session_date)
        if o is not None:
            if o.closed:
                return None, CLOSED_BY_OVERRIDE
            start = parse_hhmm(o.start) if o.start else start_t
            end = parse_hhmm(o.end) if o.end else self.regular_end(session_date)
            return (start, end), OPEN
        if session_date in self._legacy_holidays:
            return None, FULL_HOLIDAY
        sp = self._cal_specials.get(session_date)
        if sp is not None:
            return (parse_hhmm(sp.start), parse_hhmm(sp.end)), OPEN
        if session_date in self._cal_pending:
            return None, SPECIAL_SESSION_PENDING
        h = self._cal_holidays.get(session_date)
        if h is not None:
            if h.closed == "full":
                return None, FULL_HOLIDAY
            if h.closed == "morning":
                return (evening_t, self.regular_end(session_date)), MORNING_SESSION_CLOSED
            return (start_t, evening_t), EVENING_SESSION_CLOSED
        if self._weekend_closed and session_date.weekday() >= 5:
            return None, WEEKEND
        return (start_t, self.regular_end(session_date)), OPEN

    def day_info(self, session_date: date) -> dict:
        """Human-readable description of a date (health / status output)."""
        times, reason = self._resolve_day(session_date)
        h = self._cal_holidays.get(session_date)
        pend = self._cal_pending.get(session_date)
        return {"date": session_date.isoformat(), "open": times is not None, "reason": reason,
                "start": times[0].strftime("%H:%M") if times else None, "end": times[1].strftime("%H:%M") if times else None,
                "holiday": h.name if h else (pend.name if pend else None), "closure": h.closed if h else None,
                "special": self._cal_specials[session_date].note if session_date in self._cal_specials else None,
                "special_pending": pend.name if pend else None, "calendar_year_covered": self.covers(session_date)}

    def is_trading_day(self, session_date: date) -> bool:
        return self._day_times(session_date) is not None

    def trading_date(self, ts: datetime) -> date:
        """Exchange trading date for a timestamp. Times before the configured trading-day
        start belong to the previous date when the day runs past midnight."""
        local = self._local(ts)
        start = self._default_start
        end = self._default_end
        if end <= start and local.time() < start:
            return (local - timedelta(days=1)).date()
        return local.date()

    def trading_day_span(self, session_date: date) -> tuple[datetime, datetime]:
        """UTC (start, end) of the trading day. Closed days return the configured
        default span so callers still get a deterministic window; use
        `is_trading_day` to know whether anything is expected inside it."""
        times = self._day_times(session_date)
        if times is None:
            start_t, end_t = self._session_times(session_date)[0], self.regular_end(session_date)
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

    def effective_close_time(self, open_time: datetime, timeframe_seconds: int) -> datetime:
        """The instant a bar starting at `open_time` is really closed: its nominal end, capped at
        the session end when the bar is the shortened last bar of the day (23:00 H1 closing at
        23:30, 23:45 M15 closing at 23:55, ...). Bars outside a trading day keep their nominal end."""
        open_time = ensure_utc(open_time)
        nominal = open_time + timedelta(seconds=timeframe_seconds)
        d = self.trading_date(open_time)
        if not self.is_trading_day(d):
            return nominal
        start, end = self.trading_day_span(d)
        if start <= open_time < end < nominal:
            return end
        return nominal

    # ---------------------------------------------------------- market state
    def _session_label(self, ts: datetime, session_date: date) -> str | None:
        local = self._local(ts)
        if session_date in self._cal_specials:
            return "SPECIAL"
        _, evening_t, _ = self._session_times(session_date)
        return "EVENING" if local.time() >= evening_t else "MORNING"

    def next_open(self, ts: datetime, horizon_days: int = 21) -> datetime | None:
        """First instant >= ts at which the exchange is open (within `horizon_days`)."""
        ts = ensure_utc(ts)
        d = self.trading_date(ts)
        for i in range(horizon_days):
            day = d + timedelta(days=i)
            if not self.covers(day):
                return None
            if not self.is_trading_day(day):
                continue
            start, end = self.trading_day_span(day)
            if ts < start:
                return start
            if start <= ts < end:
                return ts
        return None

    def next_close(self, ts: datetime, horizon_days: int = 21) -> datetime | None:
        ts = ensure_utc(ts)
        d = self.trading_date(ts)
        for i in range(horizon_days):
            day = d + timedelta(days=i)
            if not self.covers(day):
                return None
            if not self.is_trading_day(day):
                continue
            _, end = self.trading_day_span(day)
            if ts < end:
                return end
        return None

    def market_state(self, ts: datetime) -> MarketState:
        ts = ensure_utc(ts)
        d = self.trading_date(ts)
        times, reason = self._resolve_day(d)
        info = self.day_info(d)
        holiday = info["holiday"]
        covered = self.covers(d)
        if not covered:
            return MarketState("CLOSED", CALENDAR_OUT_OF_RANGE, d, None, None, None, None, None, None,
                               f"MCX calendar for {d.year} not installed (configured: {', '.join(map(str, self.years)) or 'none'})",
                               self.verified, False)
        if times is None:
            nxt = self.next_open(ts)
            detail = {WEEKEND: "weekend", FULL_HOLIDAY: f"holiday: {holiday}", SPECIAL_SESSION_PENDING: f"{holiday}: timings not yet published",
                      CLOSED_BY_OVERRIDE: "closed by override"}.get(reason, reason)
            return MarketState("CLOSED", reason, d, None, None, None, nxt, self.next_close(ts), holiday, detail, self.verified, True)
        start, end = self.trading_day_span(d)
        if start <= ts < end:
            session = self._session_label(ts, d)
            note = {MORNING_SESSION_CLOSED: f" ({holiday}: morning session closed)", EVENING_SESSION_CLOSED: f" ({holiday}: evening session closed)"}.get(reason, "")
            return MarketState("OPEN", OPEN, d, session, start, end, ts, end, holiday,
                               f"{session} session until {end.astimezone(self.tz).strftime('%H:%M')}{note}", self.verified, True)
        if ts < start:
            why = MORNING_SESSION_CLOSED if reason == MORNING_SESSION_CLOSED else OUTSIDE_TRADING_HOURS
            detail = (f"{holiday}: morning session closed, evening session opens {start.astimezone(self.tz).strftime('%H:%M')}"
                      if why == MORNING_SESSION_CLOSED else f"opens {start.astimezone(self.tz).strftime('%H:%M')}")
            return MarketState("CLOSED", why, d, None, start, end, start, end, holiday, detail, self.verified, True)
        why = EVENING_SESSION_CLOSED if reason == EVENING_SESSION_CLOSED else OUTSIDE_TRADING_HOURS
        nxt = self.next_open(ts)
        detail = (f"{holiday}: evening session closed" if why == EVENING_SESSION_CLOSED
                  else f"closed at {end.astimezone(self.tz).strftime('%H:%M')}")
        return MarketState("CLOSED", why, d, None, start, end, nxt, self.next_close(ts), holiday, detail, self.verified, True)

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

    def describe(self) -> dict:
        """Configured calendar summary for the status API."""
        out = {"timezone": self.config.timezone, "years": self.years, "verified_against_official_circular": self.verified, "calendars": {}}
        for year, c in sorted(self._calendars.items()):
            out["calendars"][str(year)] = {
                "exchange": c.exchange, "segment": c.segment, "verified": c.verified_against_official_circular,
                "default_session": {"start": c.default_session.start, "evening_start": c.default_session.evening_start, "end": c.default_session.end},
                "close_periods": [{"from": p.from_date, "to": p.to_date, "end": p.end, "note": p.note} for p in c.close_periods],
                "holidays": [{"date": h.date, "name": h.name, "closed": h.closed} for h in c.holidays],
                "special_sessions": [{"date": x.date, "start": x.start, "end": x.end, "note": x.note} for x in c.special_sessions],
                "pending_special_sessions": [{"date": x.date, "name": x.name, "note": x.note} for x in c.pending_special_sessions],
            }
        return out
