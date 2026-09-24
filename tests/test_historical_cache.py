"""Historical cache integrity: coverage is recorded from what the broker actually
returned, never from the requested range.  A partial, empty, or truncated response
must leave the missing part uncovered so it is re-requested, and never fabricates
"cached" data for a range that was never verified."""
from __future__ import annotations

from datetime import datetime, timedelta

import pytest

from aureon_mcx.broker.dhan.historical import CachedHistoricalProvider, verified_coverage
from aureon_mcx.market.candle import Candle
from aureon_mcx.market.sessions import SessionCalendar
from aureon_mcx.market.timeframe import Timeframe
from aureon_mcx.market.timeutil import IST
from aureon_mcx.storage.repositories import merge_intervals

M5 = Timeframe.M5
SID = "428291"


def ist(y, m, d, hh, mm=0):
    return datetime(y, m, d, hh, mm, tzinfo=IST)


def bars(opens: list[datetime]) -> list[Candle]:
    return [Candle(symbol="GOLD", security_id=SID, timeframe=M5, open_time=t, open=70000, high=70010, low=69990, close=70005,
                   volume=10, expiry_date="2026-02-05") for t in opens]


def opens(start: datetime, end: datetime) -> list[datetime]:
    out, t = [], start
    while t < end:
        out.append(t)
        t += timedelta(minutes=5)
    return out


class ScriptedProvider:
    """Returns exactly the bars scripted per call (in order), recording every request."""

    def __init__(self, responses):
        self.responses = list(responses)
        self.calls: list[tuple[datetime, datetime]] = []

    def fetch(self, symbol, security_id, seg, itype, expiry, timeframe, start, end):
        self.calls.append((start, end))
        if not self.responses:
            raise AssertionError("unexpected historical fetch")
        r = self.responses.pop(0)
        return r(start, end) if callable(r) else r


@pytest.fixture
def calendar(app_config):
    return SessionCalendar(app_config.sessions)


FAR_FUTURE = ist(2026, 12, 31, 23)  # every 2026 bar is closed


def load(cp, start, end):
    return cp.load("GOLD", SID, "MCX_COMM", "FUTCOM", "2026-02-05", M5, start, end)


# ------------------------------------------------------------- verified_coverage
def test_coverage_complete_response_covers_whole_range(calendar):
    start, end = ist(2026, 1, 14, 10), ist(2026, 1, 14, 11)
    cov = verified_coverage(bars(opens(start, end)), M5, start, end, calendar=calendar, now=FAR_FUTURE)
    assert cov == [(start, end)]


def test_coverage_partial_response_covers_only_returned_runs(calendar):
    start, end = ist(2026, 1, 14, 10), ist(2026, 1, 14, 11)
    all_opens = opens(start, end)
    missing = {ist(2026, 1, 14, 10, 20), ist(2026, 1, 14, 10, 25)}
    cov = verified_coverage(bars([t for t in all_opens if t not in missing]), M5, start, end, calendar=calendar, now=FAR_FUTURE)
    assert cov == [(start, ist(2026, 1, 14, 10, 20)), (ist(2026, 1, 14, 10, 30), end)]


def test_coverage_empty_response_over_open_market_covers_nothing(calendar):
    start, end = ist(2026, 1, 14, 10), ist(2026, 1, 14, 11)
    assert verified_coverage([], M5, start, end, calendar=calendar, now=FAR_FUTURE) == []


def test_coverage_holiday_range_is_verified_empty(calendar):
    start, end = ist(2026, 1, 26, 10), ist(2026, 1, 26, 12)  # Republic Day: MCX closed all day
    assert verified_coverage([], M5, start, end, calendar=calendar, now=FAR_FUTURE) == [(start, end)]


def test_coverage_spans_closed_hours_but_not_missing_bars(calendar):
    # Wed 23:00 -> Thu 09:30 (overnight close between 23:55 and 09:00)
    start, end = ist(2026, 1, 14, 23), ist(2026, 1, 15, 9, 30)
    expected = opens(ist(2026, 1, 14, 23), ist(2026, 1, 14, 23, 55)) + opens(ist(2026, 1, 15, 9), ist(2026, 1, 15, 9, 30))
    assert verified_coverage(bars(expected), M5, start, end, calendar=calendar, now=FAR_FUTURE) == [(start, end)]
    # drop the last bar of the day: coverage must stop before it, then resume at the next morning
    without_last = [t for t in expected if t != ist(2026, 1, 14, 23, 50)]
    cov = verified_coverage(bars(without_last), M5, start, end, calendar=calendar, now=FAR_FUTURE)
    assert cov == [(start, ist(2026, 1, 14, 23, 50)), (ist(2026, 1, 15, 9), end)]


def test_coverage_never_extends_over_an_unclosed_bar(calendar):
    start, end = ist(2026, 1, 14, 10), ist(2026, 1, 14, 10, 30)
    now = ist(2026, 1, 14, 10, 22)  # 10:20 bar still open
    cov = verified_coverage(bars(opens(start, ist(2026, 1, 14, 10, 20))), M5, start, end, calendar=calendar, now=now)
    assert cov == [(start, ist(2026, 1, 14, 10, 20))]
    assert verified_coverage([], M5, ist(2026, 1, 14, 10, 20), end, calendar=calendar, now=now) == []


def test_merge_intervals_merges_overlaps_and_touching():
    a, b, c, d = ist(2026, 1, 14, 10), ist(2026, 1, 14, 11), ist(2026, 1, 14, 12), ist(2026, 1, 14, 13)
    assert merge_intervals([(b, c), (a, b), (c + timedelta(minutes=5), d)]) == [(a, c), (c + timedelta(minutes=5), d)]
    assert merge_intervals([(a, c), (b, d)]) == [(a, d)]
    assert merge_intervals([(a, a)]) == []


# ---------------------------------------------------------- cached provider
def test_partial_response_is_refetched_only_for_the_hole(repos, calendar):
    start, end = ist(2026, 1, 14, 10), ist(2026, 1, 14, 11)
    hole = {ist(2026, 1, 14, 10, 20), ist(2026, 1, 14, 10, 25)}
    prov = ScriptedProvider([bars([t for t in opens(start, end) if t not in hole]), bars(sorted(hole))])
    cp = CachedHistoricalProvider(prov, repos, calendar=calendar, now=lambda: FAR_FUTURE)
    first = load(cp, start, end)
    assert len(first) == 10
    assert not repos.historical_cache.is_cached(SID, M5, start, end)
    assert repos.historical_cache.uncovered(SID, M5, start, end) == [(ist(2026, 1, 14, 10, 20), ist(2026, 1, 14, 10, 30))]
    second = load(cp, start, end)  # only the hole is requested, then everything is covered
    assert prov.calls[-1] == (ist(2026, 1, 14, 10, 20), ist(2026, 1, 14, 10, 30))
    assert len(second) == 12 and repos.historical_cache.is_cached(SID, M5, start, end)
    load(cp, start, end)
    assert len(prov.calls) == 2


def test_empty_response_is_never_cached(repos, calendar):
    start, end = ist(2026, 1, 14, 10), ist(2026, 1, 14, 11)
    prov = ScriptedProvider([[], bars(opens(start, end))])
    cp = CachedHistoricalProvider(prov, repos, calendar=calendar, now=lambda: FAR_FUTURE)
    assert load(cp, start, end) == []
    assert repos.historical_cache.coverage(SID, M5) == []
    assert len(load(cp, start, end)) == 12 and len(prov.calls) == 2  # retried, not served from a poisoned cache


def test_holiday_range_is_cached_as_verified_empty(repos, calendar):
    start, end = ist(2026, 1, 26, 10), ist(2026, 1, 26, 12)
    prov = ScriptedProvider([[]])
    cp = CachedHistoricalProvider(prov, repos, calendar=calendar, now=lambda: FAR_FUTURE)
    assert load(cp, start, end) == [] and load(cp, start, end) == []
    assert len(prov.calls) == 1 and repos.historical_cache.is_cached(SID, M5, start, end)


def test_overlapping_loads_merge_coverage(repos, calendar):
    a, b, c = ist(2026, 1, 14, 10), ist(2026, 1, 14, 11), ist(2026, 1, 14, 12)
    prov = ScriptedProvider([lambda s, e: bars(opens(s, e))] * 3)
    cp = CachedHistoricalProvider(prov, repos, calendar=calendar, now=lambda: FAR_FUTURE)
    load(cp, a, b)
    load(cp, ist(2026, 1, 14, 10, 30), c)
    assert prov.calls[1] == (b, c)  # only the uncovered tail was requested
    assert repos.historical_cache.coverage(SID, M5) == [(a, c)]
    assert repos.historical_cache.is_cached(SID, M5, ist(2026, 1, 14, 10, 15), ist(2026, 1, 14, 11, 45))
    assert len(load(cp, a, c)) == 24 and len(prov.calls) == 2


def test_complete_response_never_refetches(repos, calendar):
    start, end = ist(2026, 1, 14, 10), ist(2026, 1, 14, 11)
    prov = ScriptedProvider([bars(opens(start, end))])
    cp = CachedHistoricalProvider(prov, repos, calendar=calendar, now=lambda: FAR_FUTURE)
    assert len(load(cp, start, end)) == 12 and len(load(cp, start, end)) == 12
    assert len(prov.calls) == 1 and repos.historical_cache.coverage(SID, M5) == [(start, end)]


def test_truncated_response_leaves_tail_uncovered(repos, calendar):
    start, end = ist(2026, 1, 14, 10), ist(2026, 1, 14, 12)
    prov = ScriptedProvider([bars(opens(start, ist(2026, 1, 14, 11)))])  # broker stopped early
    cp = CachedHistoricalProvider(prov, repos, calendar=calendar, now=lambda: FAR_FUTURE)
    load(cp, start, end)
    assert repos.historical_cache.uncovered(SID, M5, start, end) == [(ist(2026, 1, 14, 11), end)]
    assert not repos.historical_cache.is_cached(SID, M5, start, end)


def test_bars_outside_the_request_do_not_extend_coverage(repos, calendar):
    start, end = ist(2026, 1, 14, 10), ist(2026, 1, 14, 11)
    prov = ScriptedProvider([bars(opens(ist(2026, 1, 14, 9), ist(2026, 1, 14, 12)))])  # broker returned more than asked
    cp = CachedHistoricalProvider(prov, repos, calendar=calendar, now=lambda: FAR_FUTURE)
    assert len(load(cp, start, end)) == 12
    assert repos.historical_cache.coverage(SID, M5) == [(start, end)]
