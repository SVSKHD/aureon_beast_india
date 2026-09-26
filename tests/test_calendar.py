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
    assert len(c.holidays) == 16 and len(c.close_periods) >= 1
    assert sum(1 for h in c.holidays if h.closed == "morning") == 11   # MCX 2026: eleven morning-session closures
    assert sum(1 for h in c.holidays if h.closed == "full") == 4         # 26 Jan, 03 Apr, 02 Oct, 25 Dec
    assert sum(1 for h in c.holidays if h.closed == "evening") == 1      # 01 Jan: morning only
    assert all(date.fromisoformat(h.date).weekday() < 5 for h in c.holidays)  # weekend dates are not listed as holidays
    assert [x.date for x in c.pending_special_sessions] == ["2026-11-08"] and c.special_sessions == []
    assert c.verified_against_official_circular is True
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
    assert not cal.is_trading_day(date(2026, 11, 8))  # Diwali Muhurat trading announced, timings not yet published -> closed


def test_morning_session_closed_holidays_open_at_evening_start(cal):
    for d, name in ((date(2026, 3, 3), "Holi"), (date(2026, 5, 1), "Maharashtra Day"), (date(2026, 10, 20), "Dassera"), (date(2026, 11, 24), "Guru Nanak Jayanti")):
        assert cal.is_trading_day(d), name
        s, e = cal.trading_day_span(d)
        assert hhmm(s) == "17:00", name
        assert cal.day_info(d)["closure"] == "morning" and cal.day_info(d)["holiday"] == name
        noon = datetime.combine(d, datetime.min.time(), tzinfo=IST) + timedelta(hours=12)
        assert not cal.is_open(noon) and cal.is_open(noon + timedelta(hours=6))
    # Holi is in the 23:55 period, Dassera in the 23:30 period: both come from the same calendar
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
    bad_cases += [
        'year: 2026\nholidays: []\n',  # unpopulated
        'holidays:\n  - { date: "2026-01-26", name: "x", closed: full }\n',  # no year
        'year: 2026\nholidays:\n  - { date: "2025-01-26", name: "x", closed: full }\n',  # wrong year
    ]
    for body in bad_cases:
        (cdir / "exchange_calendar.yaml").write_text(body)
        with pytest.raises(ConfigError):
            load_config(config_dir=cdir, env_file=tmp_path / "none.env")
    (cdir / "exchange_calendar.yaml").unlink()
    with pytest.raises(ConfigError, match="missing config file"):  # the calendar is mandatory
        load_config(config_dir=cdir, env_file=tmp_path / "none.env")
    with pytest.raises(Exception):
        ExchangeCalendarConfig.model_validate({"close_periods": [{"from": "2026-03-09", "to": "2026-11-01", "end": "16:00"}]})


# ------------------------------------------------ official MCX 2026 list (section E)
def test_new_year_day_morning_open_evening_closed(cal):
    d = date(2026, 1, 1)
    assert cal.is_trading_day(d)
    s, e = cal.trading_day_span(d)
    assert (hhmm(s), hhmm(e)) == ("09:00", "17:00")
    assert cal.is_open(ist(2026, 1, 1, 9, 0)) and cal.is_open(ist(2026, 1, 1, 16, 59))
    assert not cal.is_open(ist(2026, 1, 1, 17, 0)) and not cal.is_open(ist(2026, 1, 1, 20, 0))
    info = cal.day_info(d)
    assert info["closure"] == "evening" and info["holiday"] == "New Year Day"
    ms = cal.market_state(ist(2026, 1, 1, 18, 0))
    assert ms.state == "CLOSED" and ms.reason == "EVENING_SESSION_CLOSED" and ms.holiday == "New Year Day"
    assert ms.next_open == ist(2026, 1, 2, 9, 0)
    assert [(hhmm(a), hhmm(b)) for a, b in cal.h4_buckets(d)] == [("09:00", "13:00"), ("13:00", "17:00")]


@pytest.mark.parametrize("d,name", [(date(2026, 1, 26), "Republic Day"), (date(2026, 4, 3), "Good Friday"),
                                    (date(2026, 10, 2), "Mahatma Gandhi Jayanti"), (date(2026, 12, 25), "Christmas")])
def test_fully_closed_days(cal, d, name):
    assert not cal.is_trading_day(d)
    ms = cal.market_state(datetime.combine(d, datetime.min.time(), tzinfo=IST) + timedelta(hours=12))
    assert ms.state == "CLOSED" and ms.reason == "FULL_HOLIDAY" and ms.holiday == name and ms.next_open is not None


@pytest.mark.parametrize("d,name", [(date(2026, 3, 3), "Holi"), (date(2026, 9, 14), "Ganesh Chaturthi"), (date(2026, 10, 20), "Dassera"),
                                    (date(2026, 11, 10), "Diwali-Balipratipada")])
def test_morning_closed_evening_open_days(cal, d, name):
    noon = datetime.combine(d, datetime.min.time(), tzinfo=IST) + timedelta(hours=12)
    before = cal.market_state(noon)
    assert before.state == "CLOSED" and before.reason == "MORNING_SESSION_CLOSED" and before.holiday == name
    assert hhmm(before.next_open) == "17:00" and before.next_open.astimezone(IST).date() == d
    evening = cal.market_state(noon + timedelta(hours=6))
    assert evening.state == "OPEN" and evening.session == "EVENING" and evening.holiday == name


def test_muhurat_pending_timings_is_closed_until_configured(cal, app_config):
    d = date(2026, 11, 8)
    assert not cal.is_trading_day(d)
    ms = cal.market_state(ist(2026, 11, 8, 18, 30))
    assert ms.state == "CLOSED" and ms.reason == "SPECIAL_SESSION_PENDING" and "Muhurat" in (ms.holiday or "")
    assert cal.day_info(d)["special_pending"] == "Diwali Muhurat trading"
    # once MCX publishes the hours, a special_sessions entry overrides the Sunday closure
    cal_cfg = app_config.sessions.calendar.model_copy(update={
        "special_sessions": [CalendarSpecialSession(date="2026-11-08", start="18:00", end="19:00", note="Muhurat trading")],
        "pending_special_sessions": []})
    cal2 = SessionCalendar(app_config.sessions.model_copy(update={"calendar": cal_cfg}))
    assert cal2.is_trading_day(d) and cal2.market_state(ist(2026, 11, 8, 18, 30)).state == "OPEN"
    assert cal2.market_state(ist(2026, 11, 8, 18, 30)).session == "SPECIAL"
    with pytest.raises(Exception):  # both at once is a configuration error
        ExchangeCalendarConfig.model_validate({**app_config.sessions.calendar.model_dump(by_alias=True),
                                               "special_sessions": [{"date": "2026-11-08", "start": "18:00", "end": "19:00"}]})


def test_weekend_and_hours_market_state_reasons(cal):
    sat = cal.market_state(ist(2026, 9, 26, 12, 0))
    assert sat.state == "CLOSED" and sat.reason == "WEEKEND" and sat.next_open == ist(2026, 9, 28, 9, 0)
    early = cal.market_state(ist(2026, 9, 21, 8, 30))
    assert early.state == "CLOSED" and early.reason == "OUTSIDE_TRADING_HOURS" and early.next_open == ist(2026, 9, 21, 9, 0)
    late = cal.market_state(ist(2026, 9, 21, 23, 45))
    assert late.state == "CLOSED" and late.reason == "OUTSIDE_TRADING_HOURS" and late.next_open == ist(2026, 9, 22, 9, 0)
    open_ = cal.market_state(ist(2026, 9, 21, 10, 0))
    assert open_.state == "OPEN" and open_.session == "MORNING" and open_.closes_at == ist(2026, 9, 21, 23, 30)
    assert cal.market_state(ist(2026, 9, 21, 18, 0)).session == "EVENING"
    assert cal.market_state(ist(2026, 1, 15, 23, 40)).state == "OPEN"        # 23:55 period
    assert cal.market_state(ist(2026, 9, 21, 23, 40)).state == "CLOSED"      # 23:30 period
    assert open_.calendar_verified is True and open_.calendar_year_covered is True


def test_calendar_out_of_range_fails_closed(cal):
    from aureon_mcx.market.sessions import CalendarOutOfRange

    d = date(2027, 1, 2)
    assert not cal.covers(d) and cal.covers(date(2026, 6, 1))
    assert not cal.is_trading_day(d) and not cal.is_open(ist(2027, 1, 2, 12, 0))  # never "normal trading" for an unconfigured year
    ms = cal.market_state(ist(2027, 1, 2, 12, 0))
    assert ms.state == "CLOSED" and ms.reason == "CALENDAR_OUT_OF_RANGE" and "MCX calendar for 2027 not installed" in ms.detail
    assert ms.calendar_year_covered is False and ms.next_open is None
    with pytest.raises(CalendarOutOfRange, match="MCX calendar for 2027 not installed"):
        cal.require_coverage(ist(2027, 1, 2, 12, 0))
    assert cal.require_coverage(ist(2026, 12, 31, 12, 0)) == date(2026, 12, 31)
    m5 = make_candles(12, start=ist(2027, 1, 4, 10, 0))
    assert aggregate_with_status(m5, Timeframe.H1, calendar=cal) == []  # nothing expected: no fabricated buckets


def test_multi_year_calendar_files_are_merged(tmp_path):
    cdir = tmp_path / "config"
    shutil.copytree(ROOT / "config", cdir)
    base = (cdir / "exchange_calendar.yaml").read_text()
    (cdir / "exchange_calendar_2027.yaml").write_text(
        'exchange: MCX\nyear: 2027\nverified_against_official_circular: false\n'
        'holidays:\n  - { date: "2027-01-26", name: "Republic Day", closed: full }\n')
    cfg = load_config(config_dir=cdir, env_file=tmp_path / "none.env")
    assert cfg.sessions.calendar_years == [2026, 2027]
    cal = SessionCalendar(cfg.sessions)
    assert cal.covers(date(2027, 3, 3)) and not cal.is_trading_day(date(2027, 1, 26)) and cal.is_trading_day(date(2027, 1, 27))
    assert cal.verified is False  # every configured year must be verified for the whole calendar to count as verified
    assert cal.describe()["years"] == [2026, 2027]
    (cdir / "exchange_calendar_dup.yaml").write_text(base)
    with pytest.raises(ConfigError, match="duplicate exchange calendar"):
        load_config(config_dir=cdir, env_file=tmp_path / "none.env")


# ------------------------------------------------- session-capped bars (section I)
def test_effective_close_time_for_session_end_candles(cal):
    # 23:00 H1 when the market closes at 23:30 (US DST period)
    assert cal.effective_close_time(ist(2026, 9, 21, 23, 0), 3600) == ist(2026, 9, 21, 23, 30)
    # 23:00 H1 when it closes at 23:55
    assert cal.effective_close_time(ist(2026, 1, 15, 23, 0), 3600) == ist(2026, 1, 15, 23, 55)
    # M15: 23:15 and 23:45 in the 23:55 period, 23:15 in the 23:30 period
    assert cal.effective_close_time(ist(2026, 1, 15, 23, 15), 900) == ist(2026, 1, 15, 23, 30)
    assert cal.effective_close_time(ist(2026, 1, 15, 23, 45), 900) == ist(2026, 1, 15, 23, 55)
    assert cal.effective_close_time(ist(2026, 9, 21, 23, 15), 900) == ist(2026, 9, 21, 23, 30)
    # a mid-day bar keeps its nominal end; an evening-closed day (01 Jan) caps the 16:00 H1 at 17:00
    assert cal.effective_close_time(ist(2026, 9, 21, 10, 0), 3600) == ist(2026, 9, 21, 11, 0)
    assert cal.effective_close_time(ist(2026, 1, 1, 16, 0), 3600) == ist(2026, 1, 1, 17, 0)


def test_dhan_session_end_candles_are_closed_at_the_exchange_close(cal):
    """Recorded Dhan convention: the last bar of the day is stamped at its open time and spans
    only until the exchange close. At 23:35 the 23:00 H1 (closing 23:30) is a CLOSED bar."""
    import json

    from aureon_mcx.broker.dhan.historical import normalize_intraday_response, verified_coverage
    from tests.conftest import FIXTURES

    payload = json.loads((FIXTURES / "dhan_intraday_session_end_h1.json").read_text())
    now = ist(2026, 9, 21, 23, 35)
    without = normalize_intraday_response(payload, "GOLD", "428291", Timeframe.H1, "2026-10-05", now=now)
    assert [hhmm(c.open_time) for c in without] == ["21:00", "22:00"]          # nominal end 00:00 > now: wrongly dropped
    with_cal = normalize_intraday_response(payload, "GOLD", "428291", Timeframe.H1, "2026-10-05", now=now, calendar=cal)
    assert [hhmm(c.open_time) for c in with_cal] == ["21:00", "22:00", "23:00"]  # capped at 23:30: closed
    assert with_cal[-1].close_time == ist(2026, 9, 22, 0, 0)  # nominal Candle.close_time is unchanged (aggregation keys)
    cov = verified_coverage(with_cal, Timeframe.H1, ist(2026, 9, 21, 21, 0), ist(2026, 9, 21, 23, 30), calendar=cal, now=now)
    assert cov == [(ist(2026, 9, 21, 21, 0), ist(2026, 9, 21, 23, 30))]
    m15 = json.loads((FIXTURES / "dhan_intraday_session_end_m15.json").read_text())
    bars = normalize_intraday_response(m15, "GOLD", "428291", Timeframe.M15, "2026-10-05", now=ist(2026, 1, 15, 23, 56), calendar=cal)
    assert [hhmm(c.open_time) for c in bars][-3:] == ["23:15", "23:30", "23:45"]   # 23:45 M15 closes at 23:55 in the 23:55 period
    bars_dst = normalize_intraday_response(m15, "GOLD", "428291", Timeframe.M15, "2026-10-05", now=ist(2026, 1, 15, 23, 50), calendar=cal)
    assert [hhmm(c.open_time) for c in bars_dst][-1] == "23:30"                     # 23:45 still open at 23:50
