"""Market-data continuity: completeness, gaps, flat fill, recovery, repair, calendar, H4 policy."""
from __future__ import annotations

from datetime import date, datetime, timedelta, timezone

import pytest

from aureon_mcx.config.yaml_models import SessionOverridesConfig
from aureon_mcx.market.aggregation import Completeness, TimeframeAggregator, aggregate_closed, aggregate_with_status
from aureon_mcx.market.candle import Candle
from aureon_mcx.market.candle_builder import CandlePipeline, M1CandleBuilder, Tick
from aureon_mcx.market.sessions import SessionCalendar
from aureon_mcx.market.timeframe import Timeframe
from aureon_mcx.market.timeutil import IST
from tests.conftest import make_candles

T0 = datetime(2026, 9, 21, 3, 30, tzinfo=timezone.utc)  # Monday 09:00 IST


def ist(y, mo, d, h, mi):
    return datetime(y, mo, d, h, mi, tzinfo=IST).astimezone(timezone.utc)


def m1_series(n, start=T0, base=70000.0):
    return make_candles(n, start=start, tf=Timeframe.M1, prices=[base + i for i in range(n)])


@pytest.fixture
def cal(app_config):
    return SessionCalendar(app_config.sessions)


# ------------------------------------------------------------ completeness
def test_startup_mid_bucket_at_10_02_does_not_emit_partial_m5(cal):
    """Bot starts at 10:02: M1 10:02, 10:03, 10:04 exist, 10:00 and 10:01 do not."""
    start = ist(2026, 9, 21, 10, 2)
    m1 = m1_series(4, start=start)  # 10:02 .. 10:05
    agg = TimeframeAggregator(Timeframe.M1, Timeframe.M5, "GOLD", "428291", calendar=cal)
    results = []
    for c in m1:
        results += agg.add(c)
    assert len(results) == 1
    r = results[0]
    assert r.status is Completeness.GAP_DETECTED and r.expected == 5 and r.present == 3
    assert [m.astimezone(IST).strftime("%H:%M") for m in r.missing] == ["10:00", "10:01"]
    # GAP results are never returned by the closed-only batch helper
    assert aggregate_closed(m1[:3], Timeframe.M5, calendar=cal, now=ist(2026, 9, 21, 10, 6)) == []


def test_missing_one_m1_inside_m5_window(cal):
    m1 = m1_series(10)
    del m1[3]  # 09:03 missing
    res = aggregate_with_status(m1, Timeframe.M5, calendar=cal)
    assert [r.status for r in res] == [Completeness.GAP_DETECTED, Completeness.COMPLETE]
    assert res[0].present == 4 and res[0].missing[0] == T0 + timedelta(minutes=3)
    assert res[1].expected == 5 and res[1].present == 5


def test_missing_one_m5_inside_m15(cal):
    m5 = make_candles(6)
    del m5[1]
    res = aggregate_with_status(m5, Timeframe.M15, calendar=cal)
    assert [r.status for r in res] == [Completeness.GAP_DETECTED, Completeness.COMPLETE]
    assert res[0].expected == 3 and res[0].present == 2


def test_duplicate_source_candles_are_counted_once(cal):
    m1 = m1_series(5)
    dup = [m1[0], m1[1], m1[1], m1[2], m1[2], m1[3], m1[4], m1[4]]
    res = aggregate_with_status(dup, Timeframe.M5, calendar=cal)
    assert len(res) == 1 and res[0].is_complete and res[0].present == 5
    clean = aggregate_with_status(m1, Timeframe.M5, calendar=cal)[0].candle
    assert res[0].candle == clean and res[0].candle.volume == clean.volume


def test_out_of_order_source_candles(cal):
    m1 = m1_series(5)
    shuffled = [m1[2], m1[0], m1[4], m1[1], m1[3]]
    agg = TimeframeAggregator(Timeframe.M1, Timeframe.M5, "GOLD", "428291", calendar=cal)
    results = []
    for c in shuffled:
        results += agg.add(c)
    assert len(results) == 1 and results[0].is_complete
    c = results[0].candle
    assert c.open == m1[0].open and c.close == m1[4].close
    assert c.high == max(x.high for x in m1) and c.low == min(x.low for x in m1)


def test_misaligned_constituent_is_rejected(cal):
    agg = TimeframeAggregator(Timeframe.M1, Timeframe.M5, "GOLD", "428291", calendar=cal)
    c = m1_series(1)[0]
    off = Candle(**{**c.__dict__, "open_time": c.open_time + timedelta(seconds=20)})
    assert agg.add(off) == [] and agg.partial is None


def test_complete_aggregation_unchanged_from_previous_behaviour(cal):
    m5 = make_candles(36)  # 09:00 .. 11:55 IST
    h1 = aggregate_closed(m5, Timeframe.H1, calendar=cal)
    assert len(h1) == 3
    for i, h in enumerate(h1):
        block = m5[i * 12:(i + 1) * 12]
        assert (h.open, h.high, h.low, h.close, h.volume) == (block[0].open, max(c.high for c in block), min(c.low for c in block),
                                                             block[-1].close, sum(c.volume for c in block))
    assert aggregate_closed(m5, Timeframe.H1) == h1  # clock policy agrees inside the session


def test_session_end_caps_last_bucket(cal):
    # 23:00-23:30 IST H1 has only six M5 constituents and is complete when the 23:25 bar closes
    m5 = make_candles(6, start=ist(2026, 9, 21, 23, 0))
    res = aggregate_with_status(m5, Timeframe.H1, calendar=cal)
    assert len(res) == 1 and res[0].is_complete and res[0].expected == 6
    assert res[0].candle.open_time == ist(2026, 9, 21, 23, 0)


# ------------------------------------------------------------- H4 policy
def test_h4_buckets_are_session_anchored(cal):
    buckets = [(s.astimezone(IST).strftime("%H:%M"), e.astimezone(IST).strftime("%H:%M")) for s, e in cal.h4_buckets(date(2026, 9, 21))]
    assert buckets == [("09:00", "13:00"), ("13:00", "17:00"), ("17:00", "21:00"), ("21:00", "23:30")]
    h1 = [Candle(**{**c.__dict__}) for c in make_candles(15, start=ist(2026, 9, 21, 9, 0), tf=Timeframe.H1)]
    # 09:00 .. 23:00 H1 bars (the 23:00 bar represents 23:00-23:30)
    res = aggregate_with_status(h1[:14] + [Candle(**{**h1[14].__dict__})], Timeframe.H4, calendar=cal)
    opens = [r.candle.open_time.astimezone(IST).strftime("%H:%M") for r in res]
    assert opens == ["09:00", "13:00", "17:00", "21:00"]
    assert all(r.is_complete for r in res)
    assert res[-1].expected == 3  # 21:00, 22:00, 23:00
    assert cal.h4_bucket(ist(2026, 9, 21, 12, 59))[0] == ist(2026, 9, 21, 9, 0)
    assert cal.h4_bucket(ist(2026, 9, 21, 13, 0))[0] == ist(2026, 9, 21, 13, 0)
    assert cal.h4_bucket(ist(2026, 9, 21, 22, 10))[1] == ist(2026, 9, 21, 23, 30)


# --------------------------------------------------------------- calendar
def test_exchange_calendar_holidays_overrides_and_weekends(app_config):
    ov = SessionOverridesConfig(holidays=["2026-10-02"], overrides=[
        {"date": "2026-11-01", "start": "17:00", "end": "21:00", "note": "special"},
        {"date": "2026-09-24", "closed": True}])
    cal = SessionCalendar(app_config.sessions.model_copy(update={"overrides": ov}))
    assert not cal.is_trading_day(date(2026, 10, 2))            # holiday
    assert not cal.is_trading_day(date(2026, 9, 26))            # Saturday
    assert not cal.is_trading_day(date(2026, 9, 24))            # closed override
    assert cal.is_trading_day(date(2026, 11, 1))                # Sunday special session
    s, e = cal.trading_day_span(date(2026, 11, 1))
    assert (s.astimezone(IST).strftime("%H:%M"), e.astimezone(IST).strftime("%H:%M")) == ("17:00", "21:00")
    assert cal.is_open(ist(2026, 11, 1, 18, 0)) and not cal.is_open(ist(2026, 11, 1, 10, 0))
    assert not cal.is_open(ist(2026, 10, 2, 12, 0))
    assert cal.is_open(ist(2026, 9, 21, 9, 0)) and not cal.is_open(ist(2026, 9, 21, 23, 30)) and not cal.is_open(ist(2026, 9, 21, 8, 59))
    assert cal.session_end(ist(2026, 9, 21, 12, 0)) == ist(2026, 9, 21, 23, 30)
    assert cal.session_end(ist(2026, 11, 1, 18, 0)) == ist(2026, 11, 1, 21, 0)
    # expected constituents follow the special session
    m5 = make_candles(48, start=ist(2026, 11, 1, 17, 0))
    h4 = aggregate_with_status(m5, Timeframe.H4, calendar=cal) if False else aggregate_with_status(aggregate_closed(m5, Timeframe.H1, calendar=cal), Timeframe.H4, calendar=cal)
    assert len(h4) == 1 and h4[0].is_complete and h4[0].expected == 4
    # a full weekend day yields no expected intervals -> nothing is ever expected/emitted
    assert cal.trading_date(ist(2026, 9, 21, 23, 0)) == date(2026, 9, 21)


def test_session_overrides_file_is_loaded(tmp_path, monkeypatch):
    import shutil

    from aureon_mcx.config import ConfigError, load_config
    from tests.conftest import ROOT

    cdir = tmp_path / "config"
    shutil.copytree(ROOT / "config", cdir)
    (cdir / "session_overrides.yaml").write_text('holidays: ["2026-10-02"]\noverrides:\n  - date: "2026-11-01"\n    start: "17:00"\n    end: "21:00"\n')
    cfg = load_config(config_dir=cdir, env_file=tmp_path / "none.env")
    assert cfg.sessions.overrides.holidays == ["2026-10-02"] and cfg.sessions.overrides.overrides[0].end == "21:00"
    (cdir / "session_overrides.yaml").write_text('overrides:\n  - date: "2026-11-01"\n    closed: true\n    end: "21:00"\n')
    with pytest.raises(ConfigError):
        load_config(config_dir=cdir, env_file=tmp_path / "none.env")


# ------------------------------------------------------- flat fill / gaps
def test_flat_fill_only_while_connected_and_open(cal):
    b = M1CandleBuilder("GOLD", "428291", is_open=cal.is_open)
    t = ist(2026, 9, 21, 10, 0)
    b.set_connected(t)
    b.add_tick(Tick("428291", 70000, t + timedelta(seconds=5), last_qty=1))
    out = b.add_tick(Tick("428291", 70010, t + timedelta(minutes=3, seconds=5), last_qty=1))  # 10:01, 10:02 had no ticks
    assert [c.open_time.astimezone(IST).strftime("%H:%M") for c in out] == ["10:00", "10:01", "10:02"]
    assert out[1].source == "flat" and out[1].volume == 0 and out[1].close == 70000 and out[1].high == out[1].low == 70000
    # disconnected minutes are NOT flat-filled: they are gaps
    b2 = M1CandleBuilder("GOLD", "428291", is_open=cal.is_open)
    b2.set_connected(t)
    b2.add_tick(Tick("428291", 70000, t + timedelta(seconds=5), last_qty=1))
    b2.set_disconnected()
    b2.set_connected(t + timedelta(minutes=3))
    out2 = b2.add_tick(Tick("428291", 70010, t + timedelta(minutes=3, seconds=5), last_qty=1))
    assert [c.open_time.astimezone(IST).strftime("%H:%M") for c in out2] == ["10:00"]
    # closed market minutes are never flat filled
    b3 = M1CandleBuilder("GOLD", "428291", is_open=cal.is_open)
    tclose = ist(2026, 9, 21, 23, 28)
    b3.set_connected(tclose)
    b3.add_tick(Tick("428291", 70000, tclose + timedelta(seconds=5), last_qty=1))
    b3.add_tick(Tick("428291", 70001, tclose + timedelta(minutes=1, seconds=5), last_qty=1))
    flushed = b3.flush_at(ist(2026, 9, 22, 9, 0))
    assert [c.open_time.astimezone(IST).strftime("%H:%M") for c in flushed] == ["23:29"]


def _pipeline(cal, closed, gaps=None):
    return CandlePipeline("GOLD", "428291", "2026-10-05", Timeframe.M5, [Timeframe.M5, Timeframe.M15, Timeframe.H1, Timeframe.H4],
                          closed.append, calendar=cal, on_gap=(gaps.append if gaps is not None else None))


def test_pipeline_gap_suspends_dispatch_then_repair_resumes(cal):
    closed, gaps = [], []
    p = _pipeline(cal, closed, gaps)
    t = ist(2026, 9, 21, 10, 2)
    p.set_connected(t)
    for i in range(0, 12 * 60, 20):  # 10:02 .. 10:14 ticks
        p.add_tick(Tick("428291", 70000 + (i % 9), t + timedelta(seconds=i), last_qty=1))
    assert len(gaps) == 1 and gaps[0].timeframe is Timeframe.M5 and gaps[0].open_time == ist(2026, 9, 21, 10, 0)
    assert p.suspended and not p.continuity_ok
    assert closed == []  # the complete 10:05 bar is deferred, the gap bar is never dispatched
    broker = make_candles(1, start=ist(2026, 9, 21, 10, 0))[0]
    assert p.repair(broker)
    assert not p.suspended and p.continuity_ok and gaps[0].resolved
    assert [c.open_time.astimezone(IST).strftime("%H:%M") for c in closed] == ["10:00", "10:05"]
    assert closed[0].source == "dhan"
    p.add_tick(Tick("428291", 70000, ist(2026, 9, 21, 10, 15) + timedelta(seconds=1), last_qty=1))
    assert [(c.timeframe.value, c.open_time.astimezone(IST).strftime("%H:%M")) for c in closed] == [
        ("M5", "10:00"), ("M5", "10:05"), ("M5", "10:10"), ("M15", "10:00")]
    # a repair for the wrong bucket is refused
    assert not p.repair(make_candles(1, start=ist(2026, 9, 21, 10, 5))[0])


def test_pipeline_recovery_restores_uninterrupted_result(cal):
    """3-minute outage: recovery with historical M1 must yield identical M5/M15/H1 output."""
    start = ist(2026, 9, 21, 9, 0)
    m1_all = m1_series(60, start=start)  # 09:00 .. 09:59

    def ticks_for(c: Candle):
        return [Tick("428291", c.open, c.open_time + timedelta(seconds=1), last_qty=c.volume / 2),
                Tick("428291", c.high, c.open_time + timedelta(seconds=15), last_qty=0),
                Tick("428291", c.low, c.open_time + timedelta(seconds=30), last_qty=0),
                Tick("428291", c.close, c.open_time + timedelta(seconds=45), last_qty=c.volume / 2)]

    def run(outage: tuple[int, int] | None):
        closed = []
        p = _pipeline(cal, closed)
        p.set_connected(start)
        for i, c in enumerate(m1_all):
            if outage and outage[0] <= i < outage[1]:
                if i == outage[0]:
                    p.set_disconnected()
                continue
            if outage and i == outage[1]:
                # reconnect: recover the missed closed minutes from "historical" M1, then resume
                p.begin_recovery()
                p.set_connected(c.open_time)
                # ticks that arrive while recovery is in progress are buffered
                for t in ticks_for(c):
                    p.add_tick(t)
                rs = p.recovery_start()
                assert rs == m1_all[outage[0] - 1].open_time  # the minute left open at disconnect is re-fetched
                fed = p.recover_m1([x for x in m1_all[:outage[1]] if x.open_time >= rs])
                assert fed == outage[1] - outage[0] + 1
                p.end_recovery()
                continue
            for t in ticks_for(c):
                p.add_tick(t)
        p.flush_at(start + timedelta(minutes=61))
        return closed, p

    base, _ = run(None)
    rec, p = run((16, 19))  # disconnect at 09:16, reconnect at 09:19
    key = lambda c: (c.timeframe.value, c.open_time, c.open, c.high, c.low, c.close, round(c.volume, 6))  # noqa: E731
    assert [key(c) for c in rec] == [key(c) for c in base]
    assert p.continuity_ok and not p.gaps
    assert [c.timeframe.value for c in base].count("M5") == 12 and [c.timeframe.value for c in base].count("H1") == 1


def test_pipeline_duplicate_and_out_of_order_candle_delivery_is_harmless(cal):
    closed = []
    p = _pipeline(cal, closed)
    m1 = m1_series(15)  # 09:00 .. 09:14
    order = [0, 1, 1, 3, 2, 4, 4, 5, 6, 7, 8, 9, 9, 11, 10, 12, 13, 14, 14]
    for i in order:
        p.on_m1_closed(m1[i])
    p.flush_at(T0 + timedelta(minutes=16))
    m5 = [c for c in closed if c.timeframe is Timeframe.M5]
    assert [c.open_time for c in m5] == [T0, T0 + timedelta(minutes=5), T0 + timedelta(minutes=10)]
    for k, c in enumerate(m5):
        block = m1[k * 5:(k + 1) * 5]
        assert (c.open, c.high, c.low, c.close, c.volume) == (block[0].open, max(x.high for x in block), min(x.low for x in block),
                                                             block[-1].close, sum(x.volume for x in block))
    assert p.continuity_ok and not p.gaps
    # a late duplicate of an already-dispatched M1 is ignored, never re-opening a bar
    p.on_m1_closed(m1[2])
    assert len([c for c in closed if c.timeframe is Timeframe.M5]) == 3
