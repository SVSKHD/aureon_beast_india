"""Schema migrations must consolidate inconsistent pre-v2 data without destroying
lifecycle state, and rebuild session_state (v3) without losing rows."""
from __future__ import annotations

import sqlite3

import pytest

from aureon_mcx.storage.migrations import MIGRATIONS, SCHEMA_VERSION, MigrationError, _exec_script, apply_migrations

NOW = "2026-01-14T10:00:00+00:00"


def v1_connection() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:", isolation_level=None)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    _exec_script(conn, MIGRATIONS[0][1])
    conn.execute("INSERT INTO schema_version(version, applied_at) VALUES (1, ?)", (NOW,))
    return conn


def add_candle(conn, sid="428291", minute=0):
    cur = conn.execute(
        """INSERT INTO candles(symbol, security_id, expiry_date, timeframe, open_time, open, high, low, close, volume, created_at)
           VALUES ('GOLD', ?, '2026-02-05', 'M5', ?, 1, 2, 0, 1, 1, ?)""",
        (sid, f"2026-01-14T{10 + minute // 60:02d}:{minute % 60:02d}:00+00:00", NOW),
    )
    return cur.lastrowid


def add_detection(conn, candle_id, kind="BREAKOUT"):
    cur = conn.execute(
        """INSERT INTO detections(candle_id, symbol, security_id, expiry_date, timeframe, open_time, family, kind, direction, price,
               label, payload_json, created_at) VALUES (?, 'GOLD', '428291', '2026-02-05', 'M5', ?, 'BREAKOUT', ?, 'BULLISH', 1, 'x', '{}', ?)""",
        (candle_id, NOW, kind, NOW),
    )
    return cur.lastrowid


def add_setup(conn, det_id, candle_id, state, updated="2026-01-14T10:00:00+00:00", closed=None):
    cur = conn.execute(
        """INSERT INTO setups(symbol, security_id, expiry_date, timeframe, family, direction, state, anchor_price, invalidation_price,
               origin_detection_id, origin_candle_id, created_open_time, updated_open_time, closed_at, created_at, updated_at)
           VALUES ('GOLD', '428291', '2026-02-05', 'M5', 'breakout', 'BULLISH', ?, 1, 0, ?, ?, ?, ?, ?, ?, ?)""",
        (state, det_id, candle_id, NOW, updated, closed, NOW, NOW),
    )
    return cur.lastrowid


def add_event(conn, setup_id, candle_id, to_state):
    conn.execute(
        """INSERT INTO setup_events(setup_id, candle_id, to_state, reason, evidence_json, open_time, created_at)
           VALUES (?, ?, ?, 'r', '{}', ?, ?)""",
        (setup_id, candle_id, to_state, NOW, NOW),
    )


def add_clearance(conn, setup_id, candle_id, cleared):
    conn.execute(
        """INSERT INTO clearances(setup_id, candle_id, open_time, cleared, policy_version, mtf_state, blockers_json, badges_json,
               fakeout_flags_json, evidence_json, created_at) VALUES (?, ?, ?, ?, 'v1', 'x', '[]', '[]', '[]', '{}', ?)""",
        (setup_id, candle_id, NOW, cleared, NOW),
    )


def add_ref(conn, setup_id, message_id, rendered):
    conn.execute(
        """INSERT INTO discord_message_refs(setup_id, channel_id, message_id, last_rendered_state_hash, last_rendered_at, created_at)
           VALUES (?, '1', ?, 'h', ?, ?)""",
        (setup_id, message_id, rendered, NOW),
    )


def test_v2_consolidates_duplicate_setups_keeping_lifecycle_state():
    conn = v1_connection()
    c = [add_candle(conn, minute=5 * i) for i in range(6)]
    det = add_detection(conn, c[0])
    # three setups for the same detection (v1 replay bug): the *terminal* one carries the real history
    stale = add_setup(conn, det, c[0], "OBSERVING", updated="2026-01-14T10:00:00+00:00")
    add_event(conn, stale, c[0], "OBSERVING")
    add_ref(conn, stale, "msg-old", "2026-01-14T10:01:00+00:00")
    rich = add_setup(conn, det, c[0], "INVALIDATED", updated="2026-01-14T10:20:00+00:00", closed="2026-01-14T10:20:00+00:00")
    for i, st in enumerate(["OBSERVING", "WATCH", "DEVELOPING", "INVALIDATED"]):
        add_event(conn, rich, c[i], st)
    add_clearance(conn, rich, c[2], 1)
    add_ref(conn, rich, "msg-rich", "2026-01-14T10:20:00+00:00")
    newest = add_setup(conn, det, c[0], "WATCH", updated="2026-01-14T10:25:00+00:00")  # later but not terminal
    add_event(conn, newest, c[0], "OBSERVING")
    add_event(conn, newest, c[5], "WATCH")
    add_clearance(conn, newest, c[2], 0)  # conflicts with the canonical's clearance on the same candle
    add_clearance(conn, newest, c[5], 1)  # unique to the loser: must be adopted, not destroyed
    # an unrelated single setup with an in-setup duplicate event (v1 replay)
    det2 = add_detection(conn, c[1], kind="OTHER")
    solo = add_setup(conn, det2, c[1], "WATCH")
    add_event(conn, solo, c[1], "OBSERVING")
    add_event(conn, solo, c[1], "OBSERVING")

    assert apply_migrations(conn) == SCHEMA_VERSION

    rows = conn.execute("SELECT id, state FROM setups WHERE origin_detection_id = ?", (det,)).fetchall()
    assert [(r["id"], r["state"]) for r in rows] == [(rich, "INVALIDATED")]  # terminal wins; nothing else survives
    events = conn.execute("SELECT candle_id, to_state FROM setup_events WHERE setup_id = ? ORDER BY id", (rich,)).fetchall()
    assert [(e["candle_id"], e["to_state"]) for e in events] == [(c[0], "OBSERVING"), (c[1], "WATCH"), (c[2], "DEVELOPING"),
                                                                  (c[3], "INVALIDATED"), (c[5], "WATCH")]
    assert conn.execute("SELECT COUNT(*) FROM setup_events WHERE setup_id IN (?, ?)", (stale, newest)).fetchone()[0] == 0
    clearances = conn.execute("SELECT candle_id, cleared FROM clearances WHERE setup_id = ? ORDER BY candle_id", (rich,)).fetchall()
    assert [(r["candle_id"], r["cleared"]) for r in clearances] == [(c[2], 1), (c[5], 1)]
    assert conn.execute("SELECT COUNT(*) FROM clearances").fetchone()[0] == 2
    ref = conn.execute("SELECT setup_id, message_id FROM discord_message_refs").fetchall()
    assert [(r["setup_id"], r["message_id"]) for r in ref] == [(rich, "msg-rich")]
    assert conn.execute("SELECT COUNT(*) FROM setup_events WHERE setup_id = ?", (solo,)).fetchone()[0] == 1
    # uniqueness is now enforced
    with pytest.raises(sqlite3.IntegrityError):
        add_setup(conn, det, c[0], "OBSERVING")
    with pytest.raises(sqlite3.IntegrityError):
        add_event(conn, rich, c[0], "OBSERVING")


def test_v2_prefers_latest_progress_and_richer_history_when_none_terminal():
    conn = v1_connection()
    c = [add_candle(conn, minute=5 * i) for i in range(3)]
    det = add_detection(conn, c[0])
    a = add_setup(conn, det, c[0], "OBSERVING", updated="2026-01-14T10:00:00+00:00")
    b = add_setup(conn, det, c[0], "DEVELOPING", updated="2026-01-14T10:10:00+00:00")
    add_event(conn, b, c[0], "OBSERVING")
    add_event(conn, b, c[2], "DEVELOPING")
    add_ref(conn, a, "msg-a", "2026-01-14T10:01:00+00:00")  # only the loser had a card: adopted by the canonical
    apply_migrations(conn)
    assert [r[0] for r in conn.execute("SELECT id FROM setups").fetchall()] == [b]
    assert conn.execute("SELECT setup_id, message_id FROM discord_message_refs").fetchone()[:] == (b, "msg-a")


def test_v2_refuses_ambiguous_duplicates_and_changes_nothing():
    conn = v1_connection()
    c = add_candle(conn)
    det = add_detection(conn, c)
    add_setup(conn, det, c, "COMPLETED", updated="2026-01-14T10:20:00+00:00", closed="2026-01-14T10:20:00+00:00")
    add_setup(conn, det, c, "INVALIDATED", updated="2026-01-14T10:20:00+00:00", closed="2026-01-14T10:20:00+00:00")
    with pytest.raises(MigrationError) as info:
        apply_migrations(conn)
    assert f"origin_detection_id={det}" in str(info.value) and "COMPLETED" in str(info.value)
    assert conn.execute("SELECT COUNT(*) FROM setups").fetchone()[0] == 2  # rolled back, nothing destroyed
    assert conn.execute("SELECT MAX(version) FROM schema_version").fetchone()[0] == 1


def test_v3_rebuilds_session_state_preserving_rows_and_ids():
    conn = v1_connection()
    # v1 could only hold one row per (symbol, day, session): a same-day rollover overwrote it
    for i, (sid, name) in enumerate([("428291", "ASIA"), ("428291", "LONDON"), ("431102", "NEW_YORK")]):
        conn.execute(
            """INSERT INTO session_state(id, symbol, security_id, session_date, session_name, session_group, trend, is_current, bars,
                   evidence_json, updated_at) VALUES (?, 'GOLD', ?, '2026-01-14', ?, 'global', 'UP', ?, ?, '{"k": 1}', ?)""",
            (10 + i, sid, name, 1 if i == 2 else 0, i + 1, NOW),
        )
    assert apply_migrations(conn) == SCHEMA_VERSION
    rows = conn.execute("SELECT id, security_id, session_name, is_current, bars, evidence_json FROM session_state ORDER BY id").fetchall()
    assert [tuple(r) for r in rows] == [(10, "428291", "ASIA", 0, 1, '{"k": 1}'), (11, "428291", "LONDON", 0, 2, '{"k": 1}'),
                                        (12, "431102", "NEW_YORK", 1, 3, '{"k": 1}')]
    # contract-aware uniqueness: same day + session for another contract is a new row, same contract is a conflict
    conn.execute("""INSERT INTO session_state(symbol, security_id, session_date, session_name, session_group, trend, updated_at)
                    VALUES ('GOLD', '440000', '2026-01-14', 'LONDON', 'global', 'UP', ?)""", (NOW,))
    with pytest.raises(sqlite3.IntegrityError):
        conn.execute("""INSERT INTO session_state(symbol, security_id, session_date, session_name, session_group, trend, updated_at)
                        VALUES ('GOLD', '428291', '2026-01-14', 'LONDON', 'global', 'UP', ?)""", (NOW,))
    assert conn.execute("SELECT seq FROM sqlite_sequence WHERE name = 'session_state'").fetchone()[0] >= 13
