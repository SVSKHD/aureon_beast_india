from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from aureon_mcx.health import HealthState


def test_symbol_health_states_and_status_line():
    h = HealthState()
    h.set("feed", "connected")
    h.set("observer", "live")
    sh = h.set_symbol_state("GOLD", "WARMING", "startup", security_id="428291", expiry="2026-10-05")
    assert not h.symbols["GOLD"].trusted and not h.ok
    h.candle_seen("GOLD/M5", datetime(2026, 9, 21, 9, 0, tzinfo=timezone.utc))
    h.tick_seen("GOLD", datetime(2026, 9, 21, 9, 1, tzinfo=timezone.utc))
    h.set_symbol_state("GOLD", "LIVE", "continuity restored", feed_connected=True, reconnects=2, unresolved_gaps=0)
    assert h.ok and h.symbols["GOLD"].trusted
    line = h.status_line()
    assert "GOLD[428291 exp 2026-10-05] state=LIVE feed=up reconnects=2 gaps=0" in line and "M5=14:30" in line
    snap = h.snapshot()["symbols"]["GOLD"]
    assert snap["last_closed"]["M5"].startswith("2026-09-21T09:00") and snap["last_tick_at"] is not None
    # a connected feed with an unresolved gap is NOT healthy
    h.set_symbol_state("GOLD", "ERROR", "data gap", unresolved_gaps=1)
    assert not h.ok and "state=ERROR" in h.status_line() and "gaps=1" in h.status_line()
    with pytest.raises(ValueError):
        h.set_symbol_state("GOLD", "BOGUS")
    for state in ("RECOVERING_GAP", "ROLLOVER_WARMING", "STALE"):
        h.set_symbol_state("GOLD", state)
        assert not h.ok
