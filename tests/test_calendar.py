"""Authoritative MCX exchange calendar: holidays, partial sessions, seasonal close, special sessions."""
from __future__ import annotations

import shutil
from datetime import date, datetime, timedelta, timezone

import pytest

from aureon_mcx.config import ConfigError, load_config
from aureon_mcx.config.yaml_models import CalendarSpecialSession, ExchangeCalendarConfig
from aureon_mcx.market.aggregation import aggregate_with_status
from aureon_mcx.market.sessions import SessionCalendar
from aureon_mcx.market.timeframe import Timeframe
from aureon_mcx.market.timeutil import IST
from tests.conftest import ROOT, make_candles


def ist(y, mo, d, h, mi):
    return datetime(y, mo, d, h, mi, tzinfo=IST).astimezone(timezone.utc)


def hhmm(dt):
    return dt.astimezone(IST).strftime("%H:%M")


@pytest.fixture
def cal(app_config):
    assert app_config.sessions.calendar is not None, "config/exchange_calendar.yaml must be loaded"
    return SessionCalendar(app_config.sessions)


def test_calendar_file_is_populated_and_documented(app_config):
    c = app_config.sessions.calendar
    assert c.exchange == "MCX" and c.year == 2026
    assert len(c.holidays) >= 15 and len(c.close_periods) >= 1
    assert sum(1 for h in c.holidays if h.closed == "morning") == 11   # MCX 2026: eleven morning-session closures
    assert sum(1 for h in c.holidays if h.closed == "full" and date.fromisoformat(h.date).weekday() < 5) == 4  # four weekday full holidays
    text = (ROOT / "config" / "exchange_calendar.yaml").read_text()
    assert "SOURCE" in text and "circular" in text.lower()


def test_normal_weekday(cal):
    d = date(2026, 9, 21)  # Monday inside the US-DST period
    assert cal.is_trading_day(d)
    s, e = cal.trading_day_span(d)
    assert (hhmm(s), hhmm(e)) == ("09:00", "23:30")
    assert cal.is_open(ist(2026, 9, 21, 9, 0)) and cal.is_open(ist(2026, 9, 21, 23, 29)) and not cal.is_open(ist(2026, 9, 21, 23, 30))
    assert cal.day_info(d)["holiday"] is None


def test_weekend_closed(cal):
    assert not cal.is_trading_day(date(2026, 9, 26)) and not cal.is_trading_day(date(2026, 9, 27))
    assert not cal.is_open(ist(2026, 9, 26, 12, 0))
    m5 = make_candles(12, start=ist(2026, 9, 26, 10, 0))
    assert aggregate_with_status(m5, Timeframe.H1, calendar=cal) == []  # nothing expected, nothing emitted


def test_full_holidays(cal):
    for d in (date(2026, 1, 26), date(2026, 4, 3), date(2026, 10, 2), date(2026, 12, 25)):
        assert not cal.is_trading_day(d), d
        assert not cal.is_open(datetime.combine(d, datetime.min.time(), tzinfo=IST) + timedelta(hours=12))
        info = cal.day_info(d)
        assert info["closure"] == "full" and info["holiday"]
    assert not cal.is_trading_day(date(2026, 11, 8))  # Diwali Laxmi Pujan (Sunday, Muhurat timings not yet published -> closed)


def test_morning_session_closed_holidays_open_at_evening_start(cal):
    for d, name in ((date(2026, 3, 3), "Holi"), (date(2026, 5, 1), "Maharashtra Day"), (date(2026, 10, 20), "Dussehra"), (date(2026, 11, 24), "Guru Nanak Jayanti")):
        assert cal.is_trading_day(d), name
        s, e = cal.trading_day_span(d)
        assert hhmm(s) == "17:00", name
        assert cal.day_info(d)["closure"] == "morning" and cal.day_info(d)["holiday"] == name
        noon = datetime.combine(d, datetime.min.time(), tzinfo=IST) + timedelta(hours=12)
        assert not cal.is_open(noon) and cal.is_open(noon + timedelta(hours=6))
    # Holi is in the 23:55 period, Dussehra in the 23:30 period: both come from the same calendar
    assert hhmm(cal.trading_day_span(date(2026, 3, 3))[1]) == "23:55"
    assert hhmm(cal.trading_day_span(date(2026, 10, 20))[1]) == "23:30"
    # H4 buckets on a morning-closed day start at 17:00
    buckets = [(hhmm(s), hhmm(e)) for s, e in cal.h4_buckets(date(2026, 10, 20))]
    assert buckets == [("17:00", "21:00"), ("21:00", "23:30")]
    # expected constituents: nothing before 17:00
    m5 = make_candles(6, start=ist(2026, 10, 20, 16, 30))
    res = aggregate_with_status(m5, Timeframe.H1, calendar=cal, now=ist(2026, 10, 20, 17, 5))
    assert res == []  # the 16:00 bucket expects nothing on a morning-closed day: dropped, never emitted


def test_seasonal_close_23_30_vs_23_55(cal):
    # 23:55 period (US standard time)
    assert hhmm(cal.trading_day_span(date(2026, 1, 15))[1]) == "23:55"
    assert cal.is_open(ist(2026, 1, 15, 23, 40)) and not cal.is_open(ist(2026, 1, 15, 23, 55))
    assert cal.session_end(ist(2026, 1, 15, 12, 0)) == ist(2026, 1, 15, 23, 55)
    # 23:30 period (US DST): the last H1 expects six M5 bars, in winter eleven
    winter = aggregate_with_status(make_candles(11, start=ist(2026, 1, 15, 23, 0)), Timeframe.H1, calendar=cal)
    assert len(winter) == 1 and winter[0].is_complete and winter[0].expected == 11
    summer = aggregate_with_status(make_candles(6, start=ist(2026, 9, 21, 23, 0)), Timeframe.H1, calendar=cal)
    assert len(summer) == 1 and summer[0].is_complete and summer[0].expected == 6
    # last H4 bucket ends at the resolved close
    assert hhmm(cal.h4_bucket(ist(2026, 1, 15, 22, 0))[1]) == "23:55"
    assert hhmm(cal.h4_bucket(ist(2026, 9, 21, 22, 0))[1]) == "23:30"


def test_seasonal_close_transitions(cal):
    assert hhmm(cal.trading_day_span(date(2026, 3, 6))[1]) == "23:55"   # Friday before the switch
    assert hhmm(cal.trading_day_span(date(2026, 3, 9))[1]) == "23:30"   # Monday 9 March: revised hours
    assert hhmm(cal.trading_day_span(date(2026, 10, 30))[1]) == "23:30"  # last Friday of US DST
    assert not cal.is_trading_day(date(2026, 11, 1))                     # Sunday
    assert hhmm(cal.trading_day_span(date(2026, 11, 2))[1]) == "23:55"   # reverted
    assert cal.trading_day_closed(date(2026, 3, 6), ist(2026, 3, 6, 23, 40)) is False
    assert cal.trading_day_closed(date(2026, 3, 9), ist(2026, 3, 9, 23, 40)) is True


def test_special_session_overrides_the_day(app_config):
    cal_cfg = app_config.sessions.calendar.model_copy(update={"special_sessions": [
        CalendarSpecialSession(date="2026-11-08", start="18:00", end="19:00", note="Muhurat trading")]})
    cal = SessionCalendar(app_config.sessions.model_copy(update={"calendar": cal_cfg}))
    assert cal.is_trading_day(date(2026, 11, 8))
    s, e = cal.trading_day_span(date(2026, 11, 8))
    assert (hhmm(s), hhmm(e)) == ("18:00", "19:00")
    assert cal.is_open(ist(2026, 11, 8, 18, 30)) and not cal.is_open(ist(2026, 11, 8, 19, 0)) and not cal.is_open(ist(2026, 11, 8, 12, 0))
    assert cal.day_info(date(2026, 11, 8))["special"] == "Muhurat trading"
    assert [(hhmm(a), hhmm(b)) for a, b in cal.h4_buckets(date(2026, 11, 8))] == [("18:00", "19:00")]


def test_legacy_overrides_still_apply_on_top(app_config):
    from aureon_mcx.config.yaml_models import SessionOverridesConfig

    ov = SessionOverridesConfig(overrides=[{"date": "2026-09-22", "closed": True}])
    cal = SessionCalendar(app_config.sessions.model_copy(update={"overrides": ov}))
    assert not cal.is_trading_day(date(2026, 9, 22)) and cal.is_trading_day(date(2026, 9, 23))


def test_malformed_calendar_fails_loudly(tmp_path):
    cdir = tmp_path / "config"
    shutil.copytree(ROOT / "config", cdir)
    bad_cases = [
        'holidays:\n  - { date: "2026-13-01", name: "x", closed: full }\n',
        'holidays:\n  - { date: "2026-01-26", name: "x", closed: afternoon }\n',
        'close_periods:\n  - { from: "2026-03-09", to: "2026-11-01", end: "23:30" }\n  - { from: "2026-10-01", to: "2026-12-01", end: "23:55" }\n',
        'close_periods:\n  - { from: "2026-11-01", to: "2026-03-09", end: "23:30" }\n',
        'special_sessions:\n  - { date: "2026-11-08", start: "19:00", end: "18:00" }\n',
        'default_session: { start: "09:00", evening_start: "08:00", end: "23:55" }\n',
        'holidays:\n  - { date: "2026-01-26", name: "a", closed: full }\n  - { date: "2026-01-26", name: "b", closed: full }\n',
        'unknown_key: 1\n',
    ]
    for body in bad_cases:
        (cdir / "exchange_calendar.yaml").write_text(body)
        with pytest.raises(ConfigError):
            load_config(config_dir=cdir, env_file=tmp_path / "none.env")
    with pytest.raises(Exception):
        ExchangeCalendarConfig.model_validate({"close_periods": [{"from": "2026-03-09", "to": "2026-11-01", "end": "16:00"}]})
