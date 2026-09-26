"""M1 trust model: a live M1 is admitted to analysis only when the feed remained trustworthy for
the ENTIRE minute. A stall (delivery frozen while the socket looks connected), a disconnect or a
resumed feed inside the minute makes it SUSPECT / PARTIAL: it never enters aggregation and the
exact broker M1 replaces it. Reconcile mode compares every trusted live M1 with the broker."""
from __future__ import annotations

import threading
from datetime import datetime, timedelta

import pytest

from aureon_mcx.health import HealthState
from aureon_mcx.market.candle import Candle
from aureon_mcx.market.candle_builder import (REASON_PARTIAL, REASON_RECONCILE, REASON_SILENT, REASON_SUSPECT, CandlePipeline,
                                              M1CandleBuilder, M1TrustState, ThreadAffinityError, Tick)
from aureon_mcx.market.sessions import SessionCalendar
from aureon_mcx.market.timeframe import Timeframe
from aureon_mcx.market.timeutil import IST
from aureon_mcx.metrics import Metrics
from tests.test_continuity import m1_series


def ist(h, m, s=0):
    return datetime(2026, 9, 21, h, m, s, tzinfo=IST)


@pytest.fixture
def cal(app_config):
    return SessionCalendar(app_config.sessions)


def tick(price, ts):
    return Tick("428291", price, ts, last_qty=1.0)


def _pipeline(cal, closed, pending, **kw):
    return CandlePipeline("GOLD", "428291", "e", Timeframe.M5, [Timeframe.M5], closed.append, calendar=cal,
                          on_pending=lambda m, r: pending.append((m, r)), metrics=Metrics(), **kw)


# ----------------------------------------------------------------- builder level
def test_full_healthy_minute_is_trusted(cal):
    b = M1CandleBuilder("GOLD", "428291", is_open=cal.is_open)
    b.set_connected(ist(10, 0))
    for s in (1, 20, 40, 59):
        assert b.add_tick(tick(70000 + s, ist(10, 0, s))) == []
    out = b.add_tick(tick(70100, ist(10, 1, 0)))
    assert len(out) == 1 and out[0].trusted
    rec = b.minute_trust(ist(10, 0))
    assert rec.state is M1TrustState.TRUSTED and rec.packets == 4 and not rec.continuity_lost
    assert rec.first_packet_at == ist(10, 0, 1) and rec.last_packet_at == ist(10, 0, 59)
    assert rec.socket_connected_at_open and rec.feed_generation == 1 and rec.stale_transitions == []
    assert b.minute_trust(ist(10, 1)).state is M1TrustState.OPEN_TRUSTED


def test_stall_after_first_tick_makes_minute_suspect(cal):
    """10:00:00 healthy, 10:00:04 last tick, delivery freezes, 10:01:02 flush -> NOT trusted."""
    b = M1CandleBuilder("GOLD", "428291", is_open=cal.is_open)
    b.set_connected(ist(10, 0))
    b.add_tick(tick(70000, ist(10, 0, 4)))
    assert b.minute_trust(ist(10, 0)).state is M1TrustState.OPEN_TRUSTED
    assert b.set_stale(ist(10, 0, 20)) is True and b.set_stale(ist(10, 0, 21)) is False  # idempotent
    rec = b.minute_trust(ist(10, 0))
    assert rec.state is M1TrustState.OPEN_SUSPECT and rec.continuity_lost and rec.reason == REASON_SUSPECT
    assert rec.stale_transitions == [ist(10, 0, 20)]
    out = b.flush_at(ist(10, 1, 2))
    assert len(out) == 1 and not out[0].trusted and out[0].candle.source == "partial"
    assert b.minute_trust(ist(10, 0)).state is M1TrustState.PARTIAL


def test_stall_after_many_ticks_still_untrusted_and_resume_minute_is_partial(cal):
    b = M1CandleBuilder("GOLD", "428291", is_open=cal.is_open)
    b.set_connected(ist(10, 0))
    for s in range(0, 45, 3):
        b.add_tick(tick(70000 + s, ist(10, 0, s)))
    b.set_stale(ist(10, 0, 58))
    # delivery resumes at 10:01:30: 10:00 closes untrusted, 10:01 is partial (its first 30 s were unobserved)
    out = b.add_tick(tick(70050, ist(10, 1, 30)))
    assert len(out) == 1 and not out[0].trusted and out[0].candle.open_time == ist(10, 0)
    assert b.coverage_start == ist(10, 1, 30) and not b.stale and b.feed_generation == 2
    rec = b.minute_trust(ist(10, 1))
    assert rec.state is M1TrustState.OPEN_SUSPECT and rec.reason == REASON_PARTIAL and rec.feed_generation == 2
    out2 = b.add_tick(tick(70060, ist(10, 2, 0)))
    assert len(out2) == 1 and not out2[0].trusted
    # 10:02 had coverage from its start -> trusted again
    out3 = b.add_tick(tick(70070, ist(10, 3, 0)))
    assert len(out3) == 1 and out3[0].trusted


# ---------------------------------------------------------------- pipeline level
def test_suspect_m1_never_reaches_analysis_until_broker_replaces_it(cal):
    closed, pending = [], []
    p = _pipeline(cal, closed, pending)
    t = ist(10, 0)
    p.set_connected(t)
    for i in range(0, 5 * 60, 15):  # 10:00 .. 10:04 healthy ticks (M5 bucket 10:00 fills)
        if t + timedelta(seconds=i) >= ist(10, 3, 30):
            break
        p.add_tick(tick(70000 + (i % 7), t + timedelta(seconds=i)))
    assert p.set_stale(ist(10, 3, 40))  # feed freezes inside 10:03
    p.flush_at(ist(10, 4, 2))           # 10:03 closes: suspect
    assert closed == [] and [(m, r) for m, r in pending] == [(ist(10, 3), REASON_SUSPECT)]
    assert p.minute_trust(ist(10, 3)).state is M1TrustState.AWAITING_BROKER
    p.flush_at(ist(10, 5, 2))           # 10:04 elapsed with no packet at all: silent
    assert pending[-1] == (ist(10, 4), REASON_SILENT)
    assert closed == [] and not p.continuity_ok  # the 10:00 M5 bar is NOT built from the suspect minute
    # broker verification delivers the exact bars -> they enter the same pipeline, the M5 closes
    broker = m1_series(5, start=t)[3:]
    verified, still = p.verify_m1(broker, now=ist(10, 5, 2))
    assert len(verified) == 2 and still == [] and p.continuity_ok
    assert p.minute_trust(ist(10, 3)).state is M1TrustState.VERIFIED
    m5 = [c for c in closed if c.timeframe is Timeframe.M5]
    assert len(m5) == 1 and m5[0].open_time == t
    assert m5[0].close == broker[-1].close  # verified broker data, not the frozen local bar
    assert p.metrics.get("suspect_m1_replaced") == 1 and p.metrics.get("silent_m1_replaced") == 1
    assert p.metrics.get("live_m1_verified") == 2 and p.metrics.get("live_m1_built") == 3


def test_reconcile_mode_confirms_or_rejects_live_m1(cal):
    closed, pending = [], []
    p = _pipeline(cal, closed, pending, reconcile_live_m1=True)
    t = ist(10, 0)
    p.set_connected(t)
    local = m1_series(3, start=t)
    for c in local:
        for price, sec in ((c.open, 1), (c.high, 15), (c.low, 30), (c.close, 45)):
            p.add_tick(Tick("428291", price, c.open_time + timedelta(seconds=sec), last_qty=c.volume / 4))
    p.add_tick(tick(70000, ist(10, 3, 1)))  # closes 10:02
    assert sorted(p.reconcile_queue) == [c.open_time for c in local]
    # broker agrees on the first two, differs on the third's close (and volume)
    broker = [Candle(**{**c.__dict__, "id": None}) for c in local]
    broker[2] = Candle(**{**local[2].__dict__, "close": local[2].close + 1.0, "volume": local[2].volume * 3, "id": None})
    results = p.reconcile_m1(broker)
    assert [r.matched for r in results] == [True, True, False]
    assert results[2].differences == ("close", "volume") and results[2].replaced_in_open_bucket  # 10:00 M5 bucket still open
    assert p.minute_trust(local[0].open_time).state is M1TrustState.VERIFIED
    rej = p.minute_trust(local[2].open_time)
    assert rej.state is M1TrustState.REJECTED and rej.reason == REASON_RECONCILE
    assert p.metrics.get("live_m1_verified") == 3 and p.metrics.get("live_m1_mismatches") == 1
    assert p.reconcile_queue == {}
    # the open bucket now carries the broker's bar, so the closed M5 is built from verified data
    assert p.primary_agg._bucket.constituents[local[2].open_time].close == local[2].close + 1.0
    p.add_tick(tick(70000, ist(10, 4, 1)))
    p.add_tick(tick(70000, ist(10, 5, 1)))
    m5 = [c for c in closed if c.timeframe is Timeframe.M5]
    assert len(m5) == 1 and m5[0].volume == sum(c.volume for c in local[:2]) + local[2].volume * 3 + 2
    # a mismatch on a minute whose bucket already closed is reported, never rewritten
    p.reconcile_queue[local[0].open_time] = local[0]
    late = p.reconcile_m1([Candle(**{**local[0].__dict__, "close": local[0].close + 5.0, "high": local[0].high + 5.0, "id": None})])
    assert len(late) == 1 and not late[0].matched and not late[0].replaced_in_open_bucket


# ------------------------------------------------------------- thread affinity
def test_pipeline_and_health_refuse_mutation_from_worker_threads(cal):
    closed, pending = [], []
    p = _pipeline(cal, closed, pending)
    h = HealthState()
    p.bind_to_current_thread()
    h.bind_to_current_thread()
    p.set_connected(ist(10, 0))  # owner thread: fine
    h.set("feed", "connected")
    errors: list[Exception] = []

    def worker():
        for fn in (lambda: p.add_tick(tick(1.0, ist(10, 0, 1))), lambda: p.flush_at(ist(10, 1, 2)), lambda: p.verify_m1([], ist(10, 1)),
                   lambda: p.recover_m1([]), lambda: p.set_stale(ist(10, 0, 5)), lambda: h.set("feed", "x"),
                   lambda: h.set_symbol_state("GOLD", "LIVE"), lambda: h.tick_seen("GOLD", ist(10, 0))):
            try:
                fn()
                errors.append(AssertionError("mutation from worker thread was accepted"))
            except ThreadAffinityError as exc:
                errors.append(exc)

    th = threading.Thread(target=worker)
    th.start()
    th.join()
    assert len(errors) == 8 and all(isinstance(e, ThreadAffinityError) for e in errors)
    assert p.m1.open_minute is None and h.components["feed"].status == "connected"  # nothing leaked through
    p.unbind_thread()
