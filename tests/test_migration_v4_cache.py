"""Migration v4 rebuilds legacy historical cache metadata from stored candles; v5 adds the
runtime resilience tables. Candle rows are never touched."""
from __future__ import annotations

import sqlite3
from datetime import datetime, timedelta

from aureon_mcx.market.sessions import SessionCalendar
from aureon_mcx.market.timeutil import IST, to_db
from aureon_mcx.storage.migrations import MIGRATIONS, SCHEMA_VERSION, _exec_script, apply_migrations
from tests.test_migrations import NOW, v1_connection


def ist(y, m, d, hh, mm=0):
    return datetime(y, m, d, hh, mm, tzinfo=IST)


def _to_v3(conn: sqlite3.Connection) -> None:
    for version, sql in MIGRATIONS[1:3]:
        _exec_script(conn, sql)
        conn.execute("INSERT INTO schema_version(version, applied_at) VALUES (?, ?)", (version, NOW))


def _legacy_range(conn, sid, tf, start, end, bars):
    conn.execute("INSERT INTO historical_cache_ranges(security_id, timeframe, range_start, range_end, bars, downloaded_at) VALUES (?,?,?,?,?,?)",
                 (sid, tf, to_db(start), to_db(end), bars, NOW))


def _candles(conn, sid, tf, opens):
    for t in opens:
        conn.execute("""INSERT INTO candles(symbol, security_id, expiry_date, timeframe, open_time, open, high, low, close, volume, created_at)
                        VALUES ('GOLD', ?, '2026-02-05', ?, ?, 1, 2, 0, 1, 1, ?)""", (sid, tf, to_db(t), NOW))


def opens(start, end, minutes=5):
    out, t = [], start
    while t < end:
        out.append(t)
        t += timedelta(minutes=minutes)
    return out


def test_v4_rebuilds_poisoned_cache_ranges_from_candles(app_config):
    conn = v1_connection()
    _to_v3(conn)
    start, end = ist(2026, 1, 14, 10), ist(2026, 1, 14, 12)
    # legacy semantics: the whole requested range was recorded although the broker returned
    # only 10:00-10:55 and 11:30-11:55 (the 11:00-11:25 bars are missing)
    _legacy_range(conn, "428291", "M5", start, end, 18)
    _candles(conn, "428291", "M5", opens(start, ist(2026, 1, 14, 11)) + opens(ist(2026, 1, 14, 11, 30), end))
    # a second legacy row that was completely empty (empty response cached as covered)
    _legacy_range(conn, "428291", "M15", ist(2026, 1, 15, 10), ist(2026, 1, 15, 11), 0)
    # a holiday range: nothing expected, legitimately empty, stays covered
    _legacy_range(conn, "429003", "M5", ist(2026, 1, 26, 10), ist(2026, 1, 26, 12), 0)
    candles_before = conn.execute("SELECT COUNT(*) FROM candles").fetchone()[0]

    cal = SessionCalendar(app_config.sessions)
    assert apply_migrations(conn, {"calendar": cal, "now": ist(2026, 12, 31, 23)}) == SCHEMA_VERSION
    rows = conn.execute("SELECT security_id, timeframe, range_start, range_end, bars FROM historical_cache_ranges ORDER BY security_id, timeframe, range_start").fetchall()
    got = [(r[0], r[1], r[2], r[3], r[4]) for r in rows]
    assert got == [
        ("428291", "M5", to_db(start), to_db(ist(2026, 1, 14, 11)), 12),
        ("428291", "M5", to_db(ist(2026, 1, 14, 11, 30)), to_db(end), 6),
        ("429003", "M5", to_db(ist(2026, 1, 26, 10)), to_db(ist(2026, 1, 26, 12)), 0),
    ]
    assert conn.execute("SELECT COUNT(*) FROM candles").fetchone()[0] == candles_before  # candles untouched
    log = conn.execute("SELECT version, step, metrics_json FROM migration_log WHERE version = 4").fetchone()
    assert log is not None and log[1] == "rebuild_cache_coverage"
    import json

    metrics = json.loads(log[2])
    assert metrics["legacy_cache_ranges_seen"] == 3 and metrics["verified_ranges_written"] == 3
    assert metrics["invalid_ranges_removed"] >= 1 and metrics["calendar_available"] is True
    # the hole is now downloadable again
    from aureon_mcx.storage.repositories import merge_intervals  # noqa: F401  (import sanity)


def test_v4_without_calendar_clears_metadata_only(app_config):
    conn = v1_connection()
    _to_v3(conn)
    start, end = ist(2026, 1, 14, 10), ist(2026, 1, 14, 12)
    _legacy_range(conn, "428291", "M5", start, end, 24)
    _candles(conn, "428291", "M5", opens(start, end))
    assert apply_migrations(conn, {}) == SCHEMA_VERSION
    assert conn.execute("SELECT COUNT(*) FROM historical_cache_ranges").fetchone()[0] == 0
    assert conn.execute("SELECT COUNT(*) FROM candles").fetchone()[0] == 24
    import json

    metrics = json.loads(conn.execute("SELECT metrics_json FROM migration_log WHERE version = 4").fetchone()[0])
    assert metrics == {"legacy_cache_ranges_seen": 1, "verified_ranges_written": 0, "invalid_ranges_removed": 1, "calendar_available": False,
                       "note": "no calendar available: cache metadata cleared, rebuilt by the next download"}


def test_v5_creates_runtime_tables(db):
    names = {r["name"] for r in db.query("SELECT name FROM sqlite_master WHERE type='table'")}
    for t in ("continuity_incidents", "crash_reports", "system_events", "market_rank_snapshots", "migration_log"):
        assert t in names
    assert db.migrate() == SCHEMA_VERSION  # idempotent
