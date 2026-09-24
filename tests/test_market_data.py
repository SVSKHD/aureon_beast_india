from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone

import pytest

from aureon_mcx.broker.dhan.historical import CachedHistoricalProvider, normalize_intraday_response
from aureon_mcx.market.aggregation import Completeness, TimeframeAggregator, aggregate_closed
from aureon_mcx.market.candle import Candle
from aureon_mcx.market.candle_builder import CandlePipeline, M1CandleBuilder, Tick
from aureon_mcx.market.sessions import SessionCalendar
from aureon_mcx.market.timeframe import Timeframe
from aureon_mcx.market.timeutil import IST, floor_to, from_epoch
from tests.conftest import FIXTURES, make_candles

PAYLOAD = json.loads((FIXTURES / "dhan_intraday_m5.json").read_text())


def test_dhan_candle_normalization():
    now = from_epoch(PAYLOAD["timestamp"][-1]) + timedelta(minutes=10)
    candles = normalize_intraday_response(PAYLOAD, "GOLD", "428291", Timeframe.M5, "2026-10-05", now=now)
    assert len(candles) == 8
    c0 = candles[0]
    assert c0.symbol == "GOLD" and c0.security_id == "428291" and c0.timeframe is Timeframe.M5
    assert c0.open_time == from_epoch(1789962600) and c0.open_time.tzinfo is not None
    assert (c0.open, c0.high, c0.low, c0.close, c0.volume, c0.open_interest) == (70010.0, 70030.0, 70000.0, 70025.0, 120.0, 15020.0)
    assert c0.source == "dhan" and c0.is_closed
    assert all(candles[i].open_time < candles[i + 1].open_time for i in range(7))
    assert c0.open_time.astimezone(IST).minute % 5 == 0


def test_normalization_drops_incomplete_candle():
    # "now" is inside the last candle -> it must be excluded
    now = from_epoch(PAYLOAD["timestamp"][-1]) + timedelta(minutes=2)
    candles = normalize_intraday_response(PAYLOAD, "GOLD", "428291", Timeframe.M5, "2026-10-05", now=now)
    assert len(candles) == 7
    assert candles[-1].open_time == from_epoch(PAYLOAD["timestamp"][-2])


def test_cached_provider_never_refetches_same_range(repos):
    calls = []

    class Prov:
        def fetch(self, symbol, security_id, seg, itype, expiry, timeframe, start, end):
            calls.append((start, end))
            return normalize_intraday_response(PAYLOAD, symbol, security_id, timeframe, expiry,
                                               now=from_epoch(PAYLOAD["timestamp"][-1]) + timedelta(hours=1))

    cp = CachedHistoricalProvider(Prov(), repos)
    start = from_epoch(PAYLOAD["timestamp"][0])
    end = from_epoch(PAYLOAD["timestamp"][-1]) + timedelta(minutes=5)
    a = cp.load("GOLD", "428291", "MCX_COMM", "FUTCOM", "2026-10-05", Timeframe.M5, start, end)
    b = cp.load("GOLD", "428291", "MCX_COMM", "FUTCOM", "2026-10-05", Timeframe.M5, start, end)
    assert len(a) == 8 and len(b) == 8 and len(calls) == 1
    assert all(c.id is not None for c in b)


def test_m1_builder_closed_candles_only():
    b = M1CandleBuilder("GOLD", "428291", "2026-10-05")
    t0 = datetime(2026, 9, 21, 3, 30, 5, tzinfo=timezone.utc)
    assert b.add_tick(Tick("428291", 70000, t0, last_qty=2)) == []
    assert b.add_tick(Tick("428291", 70010, t0 + timedelta(seconds=20), last_qty=3)) == []
    assert b.partial is not None and b.partial.is_closed is False
    out = b.add_tick(Tick("428291", 69990, t0 + timedelta(seconds=61), last_qty=1))
    assert len(out) == 1 and out[0].is_closed
    m1 = out[0]
    assert (m1.open, m1.high, m1.low, m1.close, m1.volume) == (70000, 70010, 70000, 70010, 5)
    # late tick for the closed minute is ignored, never reopens
    assert b.add_tick(Tick("428291", 1.0, t0 + timedelta(seconds=30), last_qty=1)) == []
    assert b.partial.low == 69990
    # wall clock flush
    assert b.flush_at(t0 + timedelta(seconds=90)) == []
    flushed = b.flush_at(t0 + timedelta(seconds=120))
    assert len(flushed) == 1 and flushed[0].close == 69990


def test_m1_builder_day_volume_delta():
    b = M1CandleBuilder("GOLD", "428291")
    t0 = datetime(2026, 9, 21, 3, 30, 0, tzinfo=timezone.utc)
    b.add_tick(Tick("428291", 1, t0, day_volume=100))
    b.add_tick(Tick("428291", 1, t0 + timedelta(seconds=10), day_volume=130))
    c = b.add_tick(Tick("428291", 1, t0 + timedelta(seconds=70), day_volume=135))[0]
    assert c.volume == 30


def test_aggregation_no_incomplete_bar_leakage():
    m5 = make_candles(9)  # 09:00 .. 09:40 IST
    agg = TimeframeAggregator(Timeframe.M5, Timeframe.M15, "GOLD", "428291", "2026-10-05")
    emitted = []
    for i, c in enumerate(m5):
        for res in agg.add(c):
            emitted.append((i, res))
    # bars close after the 3rd, 6th and 9th constituent (i=2,5,8)
    assert [i for i, _ in emitted] == [2, 5, 8]
    first = emitted[0][1]
    assert first.is_complete and first.expected == 3 and first.present == 3
    fc = first.candle
    assert fc.timeframe is Timeframe.M15 and fc.is_closed
    assert fc.open == m5[0].open and fc.close == m5[2].close
    assert fc.high == max(c.high for c in m5[:3]) and fc.low == min(c.low for c in m5[:3])
    assert fc.volume == sum(c.volume for c in m5[:3])
    assert fc.open_time == m5[0].open_time

    # partial bucket is never emitted
    agg2 = TimeframeAggregator(Timeframe.M5, Timeframe.M15, "GOLD", "428291")
    assert agg2.add(m5[0]) == [] and agg2.add(m5[1]) == []
    assert agg2.partial is not None and agg2.partial.is_closed is False
    assert aggregate_closed(m5[:8], Timeframe.M15) == [e.candle for _, e in emitted[:2]]
    # flush_at only emits after the boundary passes
    assert agg2.flush_at(m5[2].open_time) is None
    agg2.add(m5[2])
    assert agg2.flush_at(m5[3].open_time) is None  # already emitted on its own boundary
    agg3 = TimeframeAggregator(Timeframe.M5, Timeframe.M15, "GOLD", "428291")
    agg3.add(m5[3]); agg3.add(m5[4])
    assert agg3.flush_at(m5[5].open_time) is None
    res = agg3.flush_at(m5[6].open_time)
    assert res is not None and res.status.value == "GAP_DETECTED"  # boundary passed with one constituent missing


def test_h1_and_h4_alignment_in_ist(app_config):
    from aureon_mcx.market.sessions import SessionCalendar

    cal = SessionCalendar(app_config.sessions)
    # 09:00 IST == 03:30 UTC; H1 buckets must align to IST hours, not UTC hours
    ts = datetime(2026, 9, 21, 3, 30, tzinfo=timezone.utc)
    assert floor_to(ts, 3600).astimezone(IST).strftime("%H:%M") == "09:00"
    assert floor_to(ts + timedelta(minutes=50), 3600).astimezone(IST).strftime("%H:%M") == "09:00"
    assert floor_to(ts + timedelta(minutes=60), 3600).astimezone(IST).strftime("%H:%M") == "10:00"
    m5 = make_candles(24)  # 09:00 .. 10:55 IST
    h1 = aggregate_closed(m5, Timeframe.H1, calendar=cal)
    assert len(h1) == 2
    assert h1[0].open_time.astimezone(IST).strftime("%H:%M") == "09:00"
    assert h1[1].open_time.astimezone(IST).strftime("%H:%M") == "10:00"
    # session-aware H4: the 09:00-13:00 bucket is incomplete with two H1 bars -> nothing leaks
    assert aggregate_closed(h1, Timeframe.H4, calendar=cal) == []
    s, e = cal.h4_bucket(ts)
    assert (s.astimezone(IST).strftime("%H:%M"), e.astimezone(IST).strftime("%H:%M")) == ("09:00", "13:00")


def test_pipeline_fires_once_per_closed_candle():
    closed: list[Candle] = []
    p = CandlePipeline("GOLD", "428291", "2026-10-05", Timeframe.M5, [Timeframe.M5, Timeframe.M15, Timeframe.H1, Timeframe.H4], closed.append)
    t0 = datetime(2026, 9, 21, 3, 30, tzinfo=timezone.utc)
    # one tick every 20 seconds for 31 minutes
    n = 0
    for s in range(0, 31 * 60, 20):
        p.add_tick(Tick("428291", 70000 + (s % 7), t0 + timedelta(seconds=s), last_qty=1))
        n += 1
    m5 = [c for c in closed if c.timeframe is Timeframe.M5]
    m15 = [c for c in closed if c.timeframe is Timeframe.M15]
    assert len(m5) == 6 and len(m15) == 2
    assert all(c.is_closed for c in closed)
    assert [c.open_time for c in m5] == [t0 + timedelta(minutes=5 * i) for i in range(6)]
    assert m15[0].open_time == t0 and m15[1].open_time == t0 + timedelta(minutes=15)
    assert not [c for c in closed if c.timeframe in (Timeframe.H1, Timeframe.H4)]
    assert p.closed_counts[Timeframe.M5] == 6


def test_session_calendar(app_config):
    cal = SessionCalendar(app_config.sessions)
    ts = datetime(2026, 9, 21, 4, 0, tzinfo=timezone.utc)  # 09:30 IST
    assert cal.session_name(ts) == "ASIA"
    assert cal.mcx_session_name(ts) == "MORNING"
    ts2 = datetime(2026, 9, 21, 14, 0, tzinfo=timezone.utc)  # 19:30 IST
    assert cal.session_name(ts2) == "NEW_YORK" and cal.mcx_session_name(ts2) == "EVENING"
    assert cal.session_name(datetime(2026, 9, 21, 1, 0, tzinfo=timezone.utc)) is None  # 06:30 IST: no session
    assert cal.trading_date(ts2).isoformat() == "2026-09-21"
    start, end = cal.trading_day_span(cal.trading_date(ts2))
    assert start.astimezone(IST).strftime("%H:%M") == "09:00" and end.astimezone(IST).strftime("%H:%M") == "23:30"
    assert not cal.trading_day_closed(cal.trading_date(ts2), ts2)
    assert cal.trading_day_closed(cal.trading_date(ts2), ts2 + timedelta(hours=5))
