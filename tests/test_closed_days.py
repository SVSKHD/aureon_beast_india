"""No-crash across market-closed periods: Friday close -> Saturday -> Sunday -> Monday open, and
trading day -> full holiday -> next trading day. A disconnected feed while the exchange is
closed creates no incidents, no gaps, no stale alerts and no crash; when the next session
opens, subscriptions / recovery resume cleanly and the symbol is LIVE again."""
from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone

from aureon_mcx.events import EventType
from aureon_mcx.market.timeutil import IST
from tests.test_app import FakeFeed, _app, m1_for, ticks_for

FRI = datetime(2026, 9, 25, 23, 25, tzinfo=IST)     # Friday 23:25 IST (US-DST period: closes 23:30)
MON = datetime(2026, 9, 28, 9, 0, tzinfo=IST)       # Monday 09:00 IST
THU = datetime(2026, 4, 2, 23, 25, tzinfo=IST)      # Thursday before Good Friday (3 Apr, full holiday)
SAT_AFTER = datetime(2026, 4, 4, 9, 0, tzinfo=IST)   # Saturday after the holiday: still closed
MON_AFTER = datetime(2026, 4, 6, 9, 0, tzinfo=IST)   # next trading day


def _run_multi_day(tmp_path, monkeypatch, start_open: datetime, closed_checkpoints: list[datetime], reopen: datetime):
    """Feed streams a few minutes before the close, drops while closed (reconnecting), and
    reconnects at the next open; the wall clock is stepped through the closed period."""
    from tests.test_app import FakeHistorical

    start_utc = start_open.astimezone(timezone.utc)
    app, http, hist, feeds, clock = _app(tmp_path, monkeypatch, symbols="GOLD", historical=lambda c: FakeHistorical(c, anchor=start_utc))
    clock[0] = start_utc
    app.startup()
    seen: dict = {}

    def feed_factory(cfg, on_tick, health):
        f = FakeFeed(on_tick, clock, minutes=8, ids=("428291",))

        async def run(stop):
            f.connected = True
            f.on_status("connected", {"reconnects": 0})
            await asyncio.sleep(0.3)
            base = clock[0].replace(second=0, microsecond=0)
            for c in m1_for("428291", base, base + timedelta(minutes=5)):  # the last five minutes before the close
                for t in ticks_for(c):
                    clock[0] = t.ts
                    on_tick(t)
                await f.wait_settled()
            await asyncio.sleep(0.3)
            seen["before_close"] = app.health.symbols["GOLD"].state
            # the exchange closes; the socket drops and keeps "reconnecting" all weekend
            f.connected = False
            f.on_status("reconnecting", {"delay": 300, "attempt": 1})
            for cp in closed_checkpoints:
                clock[0] = cp.astimezone(timezone.utc)
                await asyncio.sleep(0.45)  # housekeeping (0.2 s) + flush loop run at this wall clock
                seen.setdefault("closed_states", []).append((cp.isoformat(), app.health.symbols["GOLD"].state,
                                                             app.calendar.market_state(clock[0]).reason))
            # next session: reconnect and stream again
            clock[0] = reopen.astimezone(timezone.utc)
            f.connected = True
            f.reconnects = 1
            f.on_status("connected", {"reconnects": 1})
            await asyncio.sleep(0.4)
            base = clock[0].replace(second=0, microsecond=0)
            for c in m1_for("428291", base, base + timedelta(minutes=6)):
                for t in ticks_for(c):
                    clock[0] = t.ts
                    on_tick(t)
                await f.wait_settled()
            await asyncio.sleep(0.3)
            stop.set()

        f.run = run
        return f

    app._feed_factory = feed_factory
    asyncio.run(app.run())
    return app, seen


def _assert_stable(app, seen, closed_reasons: set[str]):
    p = app.runtimes["GOLD"].pipeline
    assert seen["before_close"] in ("LIVE", "RECOVERING_GAP")
    for iso, state, reason in seen["closed_states"]:
        assert reason in closed_reasons, (iso, reason)
        assert state != "STALE", (iso, state)   # no false stale alerts while closed
    assert app.task_failures == [] and app.crashes.recent == []     # the service never crashed
    assert p.unresolved_gaps == [] and p.pending_minutes() == []       # no false missing-candle incidents
    assert app.continuity.incidents.open_for("428291") == []
    assert app.health.symbols["GOLD"].state == "LIVE"                  # clean resume at the next open
    types = [e.type for e in app.events.history]
    assert EventType.MARKET_CLOSED in types and EventType.MARKET_OPENED in types
    assert not any(e.type is EventType.FEED_STALLED for e in app.events.history)
    return types


def test_weekend_friday_close_to_monday_open_is_stable(tmp_path, monkeypatch):
    checkpoints = [datetime(2026, 9, 25, 23, 40, tzinfo=IST), datetime(2026, 9, 26, 12, 0, tzinfo=IST), datetime(2026, 9, 27, 12, 0, tzinfo=IST),
                   datetime(2026, 9, 28, 8, 30, tzinfo=IST)]
    app, seen = _run_multi_day(tmp_path, monkeypatch, FRI, checkpoints, MON)  # streams 23:25-23:29 Friday, then the weekend
    _assert_stable(app, seen, {"OUTSIDE_TRADING_HOURS", "WEEKEND"})
    closed = [e for e in app.events.history if e.type is EventType.MARKET_CLOSED]
    assert any("weekend" in e.message for e in closed)
    opened = [e for e in app.events.history if e.type is EventType.MARKET_OPENED]
    assert opened and "MARKET OPEN" in opened[-1].message and opened[-1].payload["trading_date"] == "2026-09-28"
    assert app.health.symbols["GOLD"].reconnects == 1


def test_trading_day_full_holiday_next_trading_day_is_stable(tmp_path, monkeypatch):
    checkpoints = [datetime(2026, 4, 2, 23, 45, tzinfo=IST), datetime(2026, 4, 3, 12, 0, tzinfo=IST), SAT_AFTER, datetime(2026, 4, 5, 12, 0, tzinfo=IST)]
    app, seen = _run_multi_day(tmp_path, monkeypatch, THU, checkpoints, MON_AFTER)
    types = _assert_stable(app, seen, {"OUTSIDE_TRADING_HOURS", "FULL_HOLIDAY", "WEEKEND"})
    closed = [e for e in app.events.history if e.type is EventType.MARKET_CLOSED]
    assert any("Good Friday" in e.message for e in closed)
    assert any(e.payload.get("reason") == "FULL_HOLIDAY" and e.payload.get("holiday") == "Good Friday" for e in closed)
    assert types.count(EventType.MARKET_OPENED) >= 1
