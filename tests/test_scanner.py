"""Broad-market scanner: universe from the instrument master, verified previous close, winners /
losers ranking, unchanged, missing reference, stale exclusion, multi-segment breadth, snapshots."""
from __future__ import annotations

import struct
from datetime import date, datetime, timedelta, timezone

import pytest

from aureon_mcx.broker.dhan.instruments import InstrumentMaster, parse_instrument_master_csv
from aureon_mcx.broker.dhan.live_feed import CODE_PREV_CLOSE, CODE_QUOTE, parse_packet
from aureon_mcx.config.yaml_models import ScannerConfig
from aureon_mcx.events import EventType, SystemEventBus
from aureon_mcx.scanner import REFERENCE_MISSING, REFERENCE_VERIFIED, InstrumentScanner, build_universe, partition
from tests.conftest import FIXTURES

TODAY = date(2026, 9, 21)
NOW = datetime(2026, 9, 21, 9, 0, tzinfo=timezone.utc)  # 14:30 IST Monday (market open)


def _header(code, length, segment, sec_id):
    return struct.pack("<BHBI", code, length, segment, sec_id)


def prev_close_packet(sec_id, prev_close, prev_oi=100, segment=5):
    return parse_packet(_header(CODE_PREV_CLOSE, 16, segment, sec_id) + struct.pack("<fI", prev_close, prev_oi))


def quote_packet(sec_id, ltp, ltt=1789962600, volume=1000, day_open=0.0, day_close=0.0, day_high=0.0, day_low=0.0, segment=5):
    body = struct.pack("<fhIfIIIffff", ltp, 1, ltt, ltp, volume, 10, 10, day_open, day_close, day_high, day_low)
    return parse_packet(_header(CODE_QUOTE, 50, segment, sec_id) + body)


@pytest.fixture
def master():
    records = parse_instrument_master_csv((FIXTURES / "instrument_master_detailed.csv").read_text())
    return InstrumentMaster(records=records, version="v", downloaded_at=NOW, source="fixture")


def _scanner(cfg=None, clock=None, events=None, repos=None, market_open=True):
    clock = clock or [NOW]
    return InstrumentScanner(cfg or ScannerConfig(), now=lambda: clock[0], events=events, repos=repos,
                             is_market_open=lambda ts: market_open), clock


def test_universe_from_instrument_master_front_month_only(master):
    uni = build_universe(master, ScannerConfig(), TODAY)
    ids = {r.base_name: r.security_id for r in uni["MCX_COMM"]}
    assert ids == {"GOLD": "428291", "GOLDM": "428292", "GOLDPETAL": "428293", "GOLDGUINEA": "428294", "SILVER": "429003",
                   "SILVERM": "429004", "SILVERMIC": "429005", "SILVER1000": "429006"}   # options, NSE equity, later expiries excluded
    all_exp = build_universe(master, ScannerConfig(nearest_expiry_only=False), TODAY)
    assert len(all_exp["MCX_COMM"]) == 12
    filtered = build_universe(master, ScannerConfig(include=["GOLD*"], exclude=["GOLDPETAL"]), TODAY)
    assert sorted(r.base_name for r in filtered["MCX_COMM"]) == ["GOLD", "GOLDGUINEA", "GOLDM"]
    expired = build_universe(master, ScannerConfig(), date(2026, 10, 10))
    assert {r.base_name: r.security_id for r in expired["MCX_COMM"]}["GOLD"] == "431102"  # OCT expired: DEC is the front month
    capped = build_universe(master, ScannerConfig(nearest_expiry_only=False, max_subscriptions_per_connection=3, max_connections=2), TODAY)
    assert len(capped["MCX_COMM"]) == 6   # never beyond the documented per-connection limit x connections
    assert partition([str(i) for i in range(7)], 3) == [["0", "1", "2"], ["3", "4", "5"], ["6"]]


def test_winners_losers_unchanged_missing_reference_and_stale(master):
    events = SystemEventBus(now=lambda: NOW)
    sc, clock = _scanner(ScannerConfig(stale_after_seconds=60, winners_limit=3, losers_limit=3), events=events)
    assert sc.build(master, TODAY) == 8 and sc.partitions == [sc.security_ids()]
    # previous close from Dhan's explicit previous-close packets (verified reference)
    for sid, pc in ((428291, 70000.0), (428292, 7000.0), (428293, 700.0), (429003, 84000.0), (429004, 84000.0), (429005, 84000.0)):
        sc.on_packet(prev_close_packet(sid, pc))
    sc.on_packet(quote_packet(428291, 70700.0, day_high=70800.0, day_low=69900.0, volume=5000))   # +1.0 %
    sc.on_packet(quote_packet(428292, 7210.0))                                                       # +3.0 %
    sc.on_packet(quote_packet(428293, 700.0))                                                        # unchanged
    sc.on_packet(quote_packet(429003, 82320.0))                                                      # -2.0 %
    sc.on_packet(quote_packet(429004, 83160.0))                                                      # -1.0 %
    sc.on_packet(quote_packet(429005, 85680.0))                                                      # +2.0 %
    sc.on_packet(quote_packet(428294, 5900.0))                                                       # GOLDGUINEA: no previous close -> MISSING
    q = sc.quotes["428294"]
    assert q.ltp == 5900.0 and q.previous_close is None and q.reference_status == REFERENCE_MISSING and q.change_pct is None
    assert sc.quotes["428291"].reference_status == REFERENCE_VERIFIED and sc.quotes["428291"].change == pytest.approx(700.0)
    winners, losers = sc.rankings()
    assert [(r.rank, r.quote.symbol, round(r.quote.change_pct, 2)) for r in winners] == [(1, "GOLDM", 3.0), (2, "SILVERMIC", 2.0), (3, "GOLD", 1.0)]
    assert [(r.rank, r.quote.symbol, round(r.quote.change_pct, 2)) for r in losers] == [(1, "SILVER", -2.0), (2, "SILVERM", -1.0), (3, "GOLDPETAL", 0.0)]
    b = sc.breadth()["overall"]
    assert (b["advancing"], b["declining"], b["unchanged"], b["stale"], b["unavailable"]) == (3, 2, 1, 0, 2)  # GOLDGUINEA + SILVER1000 unavailable
    assert b["average_pct_move"] == pytest.approx(0.5) and b["median_pct_move"] == pytest.approx(0.5)
    w = winners[0].to_dict(NOW, sc.stale_threshold)
    assert {"rank", "symbol", "display_symbol", "security_id", "segment", "expiry", "ltp", "previous_close", "change", "change_pct", "day_high",
            "day_low", "volume", "open_interest", "last_update", "stale"} <= set(w)
    assert w["display_symbol"] == "GOLDM OCT FUT" and w["segment"] == "MCX_COMM" and w["expiry"] == "2026-10-05" and w["stale"] is False
    # stale instruments are never today's winners / losers
    clock[0] = NOW + timedelta(seconds=90)
    sc.on_packet(quote_packet(428291, 70700.0))   # only GOLD is fresh
    winners, losers = sc.rankings()
    assert [r.quote.symbol for r in winners] == ["GOLD"] and [r.quote.symbol for r in losers] == ["GOLD"]
    assert sc.breadth()["overall"]["stale"] == 5
    assert sc.quotes["428292"].to_dict(clock[0], sc.stale_threshold)["stale"] is True
    # market closed: nothing is presented as a live ranking
    sc.is_market_open = lambda ts: False
    assert sc.rankings() == ([], []) and sc.leaderboard()["market_open"] is False


def test_quote_day_close_is_not_a_reference_unless_verified(master):
    sc, _ = _scanner(ScannerConfig())
    sc.build(master, TODAY)
    sc.on_packet(quote_packet(428291, 70700.0, day_close=70000.0))
    assert sc.quotes["428291"].reference_status == REFERENCE_MISSING and sc.quotes["428291"].change_pct is None
    assert sc.quotes["428291"].day_close_field == 70000.0
    sc2, _ = _scanner(ScannerConfig(previous_close_from_quote_day_close=True))
    sc2.build(master, TODAY)
    sc2.on_packet(quote_packet(428291, 70700.0, day_close=70000.0))
    assert sc2.quotes["428291"].reference_status == REFERENCE_VERIFIED and sc2.quotes["428291"].change_pct == pytest.approx(1.0)


def test_multi_segment_breadth_grouping(master):
    cfg = ScannerConfig(segments=["MCX_COMM", "NSE_EQ"], instrument_types=["FUTCOM", "EQUITY"], category_map={"GOLD": "bullion", "SILVER": "bullion", "HDFCBANK": "bank"})
    sc, _ = _scanner(cfg)
    assert sc.build(master, TODAY) == 9
    assert sc.quotes["1333"].segment == "NSE_EQ" and sc.quotes["1333"].instrument_type == "EQUITY" and sc.quotes["1333"].expiry is None
    sc.on_packet(prev_close_packet(1333, 1000.0, segment=1))
    sc.on_packet(quote_packet(1333, 990.0, segment=1))
    sc.on_packet(prev_close_packet(428291, 70000.0))
    sc.on_packet(quote_packet(428291, 70700.0))
    b = sc.breadth()
    assert b["by_segment"]["NSE_EQ"]["declining"] == 1 and b["by_segment"]["MCX_COMM"]["advancing"] == 1
    assert b["by_category"]["bank"]["declining"] == 1 and b["by_category"]["bullion"]["advancing"] == 1
    assert b["by_instrument_type"]["EQUITY"]["total"] == 1 and b["by_instrument_type"]["FUTCOM"]["total"] == 8
    status = sc.status()
    assert status["segments"] == {"MCX_COMM": 8, "NSE_EQ": 1} and status["verified_references"] == 2


def test_snapshots_digest_and_leader_change_events(master, repos):
    events = SystemEventBus(now=lambda: NOW)
    sc, clock = _scanner(ScannerConfig(snapshot_interval_seconds=60, digest_interval_seconds=60), events=events, repos=repos)
    sc.build(master, TODAY)
    for sid, pc in ((428291, 70000.0), (428292, 7000.0), (429003, 84000.0)):
        sc.on_packet(prev_close_packet(sid, pc))
    sc.on_packet(quote_packet(428291, 70700.0))
    sc.on_packet(quote_packet(428292, 7210.0))
    sc.on_packet(quote_packet(429003, 82320.0))
    r = sc.tick()
    assert r["snapshot"] == 3 and r["digest"] is True
    rows = repos.rank_snapshots.latest(10)
    assert {(x["symbol"], x["rank"]) for x in rows} == {("GOLDM", 1), ("GOLD", 2), ("SILVER", 3)}  # three eligible: all ranked once
    d = sc.digest()
    assert d["winners"][0]["symbol"] == "GOLDM OCT FUT" and d["losers"][0]["change_pct"] == -2.0 and d["advancers"] == 2 and d["decliners"] == 1
    assert [e.type for e in events.history] == [EventType.SCANNER_UPDATED]
    # within the interval: no new snapshot / digest; leader change is an event
    clock[0] = NOW + timedelta(seconds=30)
    sc.on_packet(quote_packet(428291, 73500.0))   # GOLD +5 % takes the lead
    r = sc.tick()
    assert r["snapshot"] == 0 and r["digest"] is False and r["leader_changed"] is True
    assert events.history[-1].type is EventType.SCANNER_LEADER_CHANGED and "GOLD OCT FUT" in events.history[-1].message
    clock[0] = NOW + timedelta(seconds=61)
    r = sc.tick()
    assert r["snapshot"] == 3 and r["digest"] is True and len(repos.rank_snapshots.latest(20)) == 6
