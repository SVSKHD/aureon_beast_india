"""Schema migrations. Versioned via the `schema_version` table.

Write ordering / dependency rules are enforced with FOREIGN KEYs:
  candles  <- indicators, detections, setup_events, structure_pivots, market_day_frames
  detections <- setups(origin), feature_snapshots
  setups   <- setup_events, clearances, discord_message_refs
  feature_snapshots <- outcome_observations
Feature snapshots are immutable: an UPDATE trigger aborts any modification.
"""
from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timezone
from typing import Callable

MIGRATIONS: list[tuple[int, str]] = [
    (
        1,
        """
CREATE TABLE IF NOT EXISTS schema_version (
    version     INTEGER PRIMARY KEY,
    applied_at  TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS instruments (
    id                          INTEGER PRIMARY KEY AUTOINCREMENT,
    logical_symbol              TEXT NOT NULL,
    display_symbol              TEXT NOT NULL,
    security_id                 TEXT NOT NULL,
    exchange                    TEXT NOT NULL,
    exchange_segment            TEXT NOT NULL,
    instrument_type             TEXT NOT NULL,
    expiry_date                 TEXT NOT NULL,
    lot_size                    REAL,
    tick_size                   REAL,
    trading_symbol              TEXT,
    custom_symbol               TEXT,
    resolved_at                 TEXT NOT NULL,
    instrument_master_version   TEXT NOT NULL,
    is_active                   INTEGER NOT NULL DEFAULT 1,
    UNIQUE(logical_symbol, security_id, expiry_date)
);
CREATE INDEX IF NOT EXISTS ix_instruments_logical ON instruments(logical_symbol, is_active);

CREATE TABLE IF NOT EXISTS candles (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    symbol          TEXT NOT NULL,
    security_id     TEXT NOT NULL,
    expiry_date     TEXT NOT NULL,
    timeframe       TEXT NOT NULL,
    open_time       TEXT NOT NULL,
    open            REAL NOT NULL,
    high            REAL NOT NULL,
    low             REAL NOT NULL,
    close           REAL NOT NULL,
    volume          REAL NOT NULL DEFAULT 0,
    open_interest   REAL,
    source          TEXT NOT NULL DEFAULT 'dhan',
    is_closed       INTEGER NOT NULL DEFAULT 1,
    created_at      TEXT NOT NULL,
    UNIQUE(security_id, timeframe, open_time),
    CHECK (is_closed = 1)
);
CREATE INDEX IF NOT EXISTS ix_candles_lookup ON candles(symbol, security_id, timeframe, open_time);

CREATE TABLE IF NOT EXISTS historical_cache_ranges (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    security_id     TEXT NOT NULL,
    timeframe       TEXT NOT NULL,
    range_start     TEXT NOT NULL,
    range_end       TEXT NOT NULL,
    bars            INTEGER NOT NULL,
    downloaded_at   TEXT NOT NULL,
    UNIQUE(security_id, timeframe, range_start, range_end)
);

CREATE TABLE IF NOT EXISTS indicators (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    candle_id       INTEGER NOT NULL UNIQUE REFERENCES candles(id) ON DELETE CASCADE,
    symbol          TEXT NOT NULL,
    security_id     TEXT NOT NULL,
    expiry_date     TEXT NOT NULL,
    timeframe       TEXT NOT NULL,
    open_time       TEXT NOT NULL,
    ema_fast        REAL,
    ema_slow        REAL,
    ema_gap         REAL,
    ema_fast_slope  REAL,
    rsi             REAL,
    rsi_direction   TEXT,
    atr             REAL,
    volume          REAL,
    open_interest   REAL,
    warmed          INTEGER NOT NULL DEFAULT 0,
    ema_fast_period INTEGER NOT NULL,
    ema_slow_period INTEGER NOT NULL,
    rsi_period      INTEGER NOT NULL,
    atr_period      INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS ix_indicators_lookup ON indicators(security_id, timeframe, open_time);

CREATE TABLE IF NOT EXISTS structure_pivots (
    id                      INTEGER PRIMARY KEY AUTOINCREMENT,
    candle_id               INTEGER NOT NULL REFERENCES candles(id) ON DELETE CASCADE,
    confirmed_candle_id     INTEGER NOT NULL REFERENCES candles(id) ON DELETE CASCADE,
    symbol                  TEXT NOT NULL,
    security_id             TEXT NOT NULL,
    expiry_date             TEXT NOT NULL,
    timeframe               TEXT NOT NULL,
    kind                    TEXT NOT NULL,
    price                   REAL NOT NULL,
    label                   TEXT NOT NULL,
    pivot_open_time         TEXT NOT NULL,
    confirmed_at_open_time  TEXT NOT NULL,
    strength                INTEGER NOT NULL,
    UNIQUE(security_id, timeframe, kind, pivot_open_time)
);
CREATE INDEX IF NOT EXISTS ix_pivots_lookup ON structure_pivots(security_id, timeframe, confirmed_at_open_time);

CREATE TABLE IF NOT EXISTS market_day_frames (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    symbol          TEXT NOT NULL,
    security_id     TEXT NOT NULL,
    expiry_date     TEXT NOT NULL,
    timeframe       TEXT NOT NULL,
    trading_date    TEXT NOT NULL,
    open            REAL,
    high            REAL,
    low             REAL,
    close           REAL,
    bars            INTEGER NOT NULL DEFAULT 0,
    is_closed       INTEGER NOT NULL DEFAULT 0,
    last_candle_id  INTEGER REFERENCES candles(id) ON DELETE SET NULL,
    last_open_time  TEXT,
    structure_context   TEXT,
    structure_sequence  TEXT,
    updated_at      TEXT NOT NULL,
    UNIQUE(security_id, timeframe, trading_date)
);

CREATE TABLE IF NOT EXISTS detections (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    candle_id       INTEGER NOT NULL REFERENCES candles(id) ON DELETE CASCADE,
    symbol          TEXT NOT NULL,
    security_id     TEXT NOT NULL,
    expiry_date     TEXT NOT NULL,
    timeframe       TEXT NOT NULL,
    open_time       TEXT NOT NULL,
    family          TEXT NOT NULL,
    kind            TEXT NOT NULL,
    direction       TEXT NOT NULL,
    price           REAL NOT NULL,
    session         TEXT,
    label           TEXT NOT NULL,
    payload_json    TEXT NOT NULL,
    created_at      TEXT NOT NULL,
    UNIQUE(candle_id, family, kind, direction)
);
CREATE INDEX IF NOT EXISTS ix_detections_lookup ON detections(security_id, timeframe, open_time);

CREATE TABLE IF NOT EXISTS setups (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    symbol              TEXT NOT NULL,
    security_id         TEXT NOT NULL,
    expiry_date         TEXT NOT NULL,
    timeframe           TEXT NOT NULL,
    family              TEXT NOT NULL,
    direction           TEXT NOT NULL,
    state               TEXT NOT NULL,
    anchor_price        REAL NOT NULL,
    invalidation_price  REAL NOT NULL,
    origin_detection_id INTEGER NOT NULL REFERENCES detections(id) ON DELETE CASCADE,
    origin_candle_id    INTEGER NOT NULL REFERENCES candles(id) ON DELETE CASCADE,
    created_open_time   TEXT NOT NULL,
    updated_open_time   TEXT NOT NULL,
    context_json        TEXT NOT NULL DEFAULT '{}',
    closed_at           TEXT,
    created_at          TEXT NOT NULL,
    updated_at          TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS ix_setups_open ON setups(security_id, timeframe, state);

CREATE TABLE IF NOT EXISTS setup_events (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    setup_id        INTEGER NOT NULL REFERENCES setups(id) ON DELETE CASCADE,
    candle_id       INTEGER NOT NULL REFERENCES candles(id) ON DELETE CASCADE,
    detection_id    INTEGER REFERENCES detections(id) ON DELETE SET NULL,
    from_state      TEXT,
    to_state        TEXT NOT NULL,
    reason          TEXT NOT NULL,
    evidence_json   TEXT NOT NULL,
    open_time       TEXT NOT NULL,
    created_at      TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS ix_setup_events_setup ON setup_events(setup_id, id);

CREATE TABLE IF NOT EXISTS clearances (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    setup_id        INTEGER NOT NULL REFERENCES setups(id) ON DELETE CASCADE,
    candle_id       INTEGER NOT NULL REFERENCES candles(id) ON DELETE CASCADE,
    open_time       TEXT NOT NULL,
    cleared         INTEGER NOT NULL,
    policy_version  TEXT NOT NULL,
    mtf_state       TEXT NOT NULL,
    blockers_json   TEXT NOT NULL,
    badges_json     TEXT NOT NULL,
    fakeout_flags_json TEXT NOT NULL,
    evidence_json   TEXT NOT NULL,
    created_at      TEXT NOT NULL,
    UNIQUE(setup_id, candle_id)
);

CREATE TABLE IF NOT EXISTS symbol_state (
    symbol              TEXT PRIMARY KEY,
    security_id         TEXT NOT NULL,
    expiry_date         TEXT NOT NULL,
    present_trend       TEXT NOT NULL,
    mtf_json            TEXT NOT NULL,
    structure_json      TEXT NOT NULL,
    last_open_time      TEXT NOT NULL,
    updated_at          TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS session_state (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    symbol          TEXT NOT NULL,
    security_id     TEXT NOT NULL,
    session_date    TEXT NOT NULL,
    session_name    TEXT NOT NULL,
    session_group   TEXT NOT NULL,
    trend           TEXT NOT NULL,
    is_current      INTEGER NOT NULL DEFAULT 0,
    open            REAL,
    high            REAL,
    low             REAL,
    close           REAL,
    bars            INTEGER NOT NULL DEFAULT 0,
    opened_at       TEXT,
    closed_at       TEXT,
    last_open_time  TEXT,
    evidence_json   TEXT NOT NULL DEFAULT '{}',
    updated_at      TEXT NOT NULL,
    UNIQUE(symbol, session_date, session_name)
);

CREATE TABLE IF NOT EXISTS feature_snapshots (
    id                      INTEGER PRIMARY KEY AUTOINCREMENT,
    detection_id            INTEGER NOT NULL UNIQUE REFERENCES detections(id) ON DELETE CASCADE,
    candle_id               INTEGER NOT NULL REFERENCES candles(id) ON DELETE CASCADE,
    symbol                  TEXT NOT NULL,
    security_id             TEXT NOT NULL,
    expiry_date             TEXT NOT NULL,
    timeframe               TEXT NOT NULL,
    open_time               TEXT NOT NULL,
    direction               TEXT NOT NULL,
    setup_family            TEXT NOT NULL,
    reference_price         REAL NOT NULL,
    atr                     REAL,
    rsi                     REAL,
    ema_fast                REAL,
    ema_slow                REAL,
    present_trend           TEXT,
    mtf_state               TEXT,
    feature_schema_version  TEXT NOT NULL,
    features_json           TEXT NOT NULL,
    created_at              TEXT NOT NULL
);
CREATE TRIGGER IF NOT EXISTS trg_feature_snapshots_immutable
BEFORE UPDATE ON feature_snapshots
BEGIN
    SELECT RAISE(ABORT, 'feature_snapshots rows are immutable');
END;

CREATE TABLE IF NOT EXISTS outcome_observations (
    id                      INTEGER PRIMARY KEY AUTOINCREMENT,
    snapshot_id             INTEGER NOT NULL REFERENCES feature_snapshots(id) ON DELETE CASCADE,
    detection_id            INTEGER NOT NULL REFERENCES detections(id) ON DELETE CASCADE,
    horizon                 TEXT NOT NULL,
    horizon_bars            INTEGER NOT NULL,
    bars_observed           INTEGER NOT NULL,
    mfe                     REAL NOT NULL,
    mae                     REAL NOT NULL,
    mfe_atr                 REAL,
    mae_atr                 REAL,
    time_to_mfe_bars        INTEGER,
    time_to_mae_bars        INTEGER,
    bars_to_invalidation    INTEGER,
    bars_to_follow_through  INTEGER,
    final_move              REAL NOT NULL,
    label                   TEXT NOT NULL,
    label_version           TEXT NOT NULL,
    last_candle_open_time   TEXT NOT NULL,
    is_final                INTEGER NOT NULL DEFAULT 0,
    computed_at             TEXT NOT NULL,
    UNIQUE(snapshot_id, horizon)
);

CREATE TABLE IF NOT EXISTS discord_message_refs (
    id                          INTEGER PRIMARY KEY AUTOINCREMENT,
    setup_id                    INTEGER NOT NULL UNIQUE REFERENCES setups(id) ON DELETE CASCADE,
    channel_id                  TEXT NOT NULL,
    message_id                  TEXT NOT NULL,
    last_rendered_state_hash    TEXT NOT NULL,
    last_rendered_at            TEXT NOT NULL,
    created_at                  TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS model_registry (
    id                      INTEGER PRIMARY KEY AUTOINCREMENT,
    model_version           TEXT NOT NULL UNIQUE,
    feature_schema_version  TEXT NOT NULL,
    label_version           TEXT NOT NULL,
    training_start          TEXT NOT NULL,
    training_end            TEXT NOT NULL,
    sample_size             INTEGER NOT NULL,
    validation_metrics_json TEXT NOT NULL,
    instrument_scope        TEXT NOT NULL,
    created_at              TEXT NOT NULL
);
""",
    ),
    (
        2,
        """
-- Duplicate setups per origin detection are consolidated by _consolidate_duplicate_setups()
-- (run BEFORE this SQL): the canonical row keeps its lifecycle history, children are
-- repointed, and only then is uniqueness enforced.  Nothing is deleted blindly.
CREATE UNIQUE INDEX IF NOT EXISTS ux_setups_origin_detection ON setups(origin_detection_id);
CREATE UNIQUE INDEX IF NOT EXISTS ux_setup_events_transition ON setup_events(setup_id, candle_id, to_state);

CREATE TABLE IF NOT EXISTS monitor_subscriptions (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    setup_id        INTEGER NOT NULL REFERENCES setups(id) ON DELETE CASCADE,
    discord_user_id TEXT NOT NULL,
    created_at      TEXT NOT NULL,
    UNIQUE(setup_id, discord_user_id)
);

CREATE TABLE IF NOT EXISTS market_data_gaps (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    symbol              TEXT NOT NULL,
    security_id         TEXT NOT NULL,
    timeframe           TEXT NOT NULL,
    bucket_open_time    TEXT NOT NULL,
    expected            INTEGER NOT NULL,
    present             INTEGER NOT NULL,
    missing_json        TEXT NOT NULL,
    detected_at         TEXT NOT NULL,
    resolved_at         TEXT,
    resolution          TEXT,
    UNIQUE(security_id, timeframe, bucket_open_time)
);
""",
    ),
    (
        3,
        """
-- session_state is contract-aware: after a same-day rollover the new security_id gets
-- its own session rows instead of silently overwriting the old contract's.
-- Rows (and their ids) are preserved through a rebuild; SQLite cannot alter constraints in place.
CREATE TABLE session_state_v3 (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    symbol          TEXT NOT NULL,
    security_id     TEXT NOT NULL,
    session_date    TEXT NOT NULL,
    session_name    TEXT NOT NULL,
    session_group   TEXT NOT NULL,
    trend           TEXT NOT NULL,
    is_current      INTEGER NOT NULL DEFAULT 0,
    open            REAL,
    high            REAL,
    low             REAL,
    close           REAL,
    bars            INTEGER NOT NULL DEFAULT 0,
    opened_at       TEXT,
    closed_at       TEXT,
    last_open_time  TEXT,
    evidence_json   TEXT NOT NULL DEFAULT '{}',
    updated_at      TEXT NOT NULL,
    UNIQUE(symbol, security_id, session_date, session_name)
);
INSERT INTO session_state_v3(id, symbol, security_id, session_date, session_name, session_group, trend, is_current, open, high, low,
    close, bars, opened_at, closed_at, last_open_time, evidence_json, updated_at)
  SELECT id, symbol, security_id, session_date, session_name, session_group, trend, is_current, open, high, low,
    close, bars, opened_at, closed_at, last_open_time, evidence_json, updated_at FROM session_state ORDER BY id;
DROP TABLE session_state;
ALTER TABLE session_state_v3 RENAME TO session_state;
CREATE INDEX IF NOT EXISTS ix_session_state_lookup ON session_state(symbol, security_id, session_date);
""",
    ),
    (
        4,
        """
-- Legacy historical cache metadata (rows written under the old "requested range = cached"
-- semantics) is rebuilt by _rebuild_cache_coverage() BEFORE this SQL from the candles that
-- are actually stored (see migration_log for the metrics). Candle rows are never touched.
CREATE TABLE IF NOT EXISTS migration_log (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    version         INTEGER NOT NULL,
    step            TEXT NOT NULL,
    metrics_json    TEXT NOT NULL,
    applied_at      TEXT NOT NULL
);
""",
    ),
    (
        5,
        """
-- Runtime resilience tables: unresolved market-data incidents survive restarts, crashes are
-- durable, important system events are queryable, and scanner rankings can be studied later.
CREATE TABLE IF NOT EXISTS continuity_incidents (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    symbol              TEXT NOT NULL,
    security_id         TEXT NOT NULL,
    kind                TEXT NOT NULL,              -- pending_minute | gap
    timeframe           TEXT NOT NULL,
    open_time           TEXT NOT NULL,
    reason              TEXT NOT NULL,
    state               TEXT NOT NULL,              -- OPEN | RESOLVED | ABANDONED
    first_detected_at   TEXT NOT NULL,
    last_attempt_at     TEXT,
    next_retry_at       TEXT,
    attempt_count       INTEGER NOT NULL DEFAULT 0,
    last_error          TEXT,
    resolved_at         TEXT,
    resolution          TEXT,
    UNIQUE(security_id, kind, timeframe, open_time)
);
CREATE INDEX IF NOT EXISTS ix_continuity_incidents_open ON continuity_incidents(state, security_id);

CREATE TABLE IF NOT EXISTS crash_reports (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    crash_id            TEXT NOT NULL UNIQUE,
    timestamp           TEXT NOT NULL,
    component           TEXT NOT NULL,
    agent               TEXT,
    task                TEXT,
    exception_class     TEXT NOT NULL,
    message             TEXT NOT NULL,
    stack_trace         TEXT NOT NULL,
    symbol              TEXT,
    security_id         TEXT,
    app_version         TEXT NOT NULL,
    git_sha             TEXT,
    process_uptime_s    REAL NOT NULL,
    restart_number      INTEGER NOT NULL DEFAULT 0,
    recovery_result     TEXT NOT NULL DEFAULT 'pending',
    resolved_at         TEXT
);
CREATE INDEX IF NOT EXISTS ix_crash_reports_ts ON crash_reports(timestamp);

CREATE TABLE IF NOT EXISTS system_events (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    event_type      TEXT NOT NULL,
    severity        TEXT NOT NULL,
    dedupe_key      TEXT,
    agent           TEXT,
    symbol          TEXT,
    security_id     TEXT,
    message         TEXT NOT NULL,
    payload_json    TEXT NOT NULL DEFAULT '{}',
    created_at      TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS ix_system_events_created ON system_events(created_at);

CREATE TABLE IF NOT EXISTS market_rank_snapshots (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    snapshot_at     TEXT NOT NULL,
    segment         TEXT NOT NULL,
    category        TEXT,
    security_id     TEXT NOT NULL,
    symbol          TEXT NOT NULL,
    display_symbol  TEXT NOT NULL,
    rank            INTEGER NOT NULL,
    ltp             REAL,
    previous_close  REAL,
    change          REAL,
    change_pct      REAL,
    volume          REAL,
    open_interest   REAL,
    UNIQUE(snapshot_at, security_id)
);
CREATE INDEX IF NOT EXISTS ix_rank_snapshots_at ON market_rank_snapshots(snapshot_at);
""",
    ),
]

# Python-side steps that must run BEFORE a version's SQL (data consolidation that
# needs judgement SQL cannot express safely).
PRE_STEPS: dict[int, "Callable[[sqlite3.Connection, dict], None]"] = {}


def _ensure_migration_log(conn: sqlite3.Connection) -> None:
    conn.execute(
        """CREATE TABLE IF NOT EXISTS migration_log (
               id INTEGER PRIMARY KEY AUTOINCREMENT, version INTEGER NOT NULL, step TEXT NOT NULL,
               metrics_json TEXT NOT NULL, applied_at TEXT NOT NULL)"""
    )


def log_migration_step(conn: sqlite3.Connection, version: int, step: str, metrics: dict) -> None:
    """Durable record of what a data migration actually did (audited by tools.audit_database)."""
    _ensure_migration_log(conn)
    conn.execute("INSERT INTO migration_log(version, step, metrics_json, applied_at) VALUES (?,?,?,?)",
                 (version, step, json.dumps(metrics, default=str), datetime.now(tz=timezone.utc).isoformat()))


class MigrationError(RuntimeError):
    """A migration found data it cannot consolidate safely; nothing was changed."""


_TERMINAL = ("COMPLETED", "INVALIDATED")


def _consolidate_duplicate_setups(conn: sqlite3.Connection, context: dict | None = None) -> None:
    """v2: one setup per origin detection, without destroying lifecycle state.

    Canonical choice per origin_detection_id, in order: a terminal-state row (the
    lifecycle actually finished), then the latest ``updated_open_time`` (most
    progressed), then the richest event history, then the lowest id.  Two rows that
    tie on every criterion but disagree on state are ambiguous: the migration fails
    with a report instead of guessing.  Children (setup_events, clearances,
    discord_message_refs) are repointed to the canonical row, exact duplicates among
    them are dropped (oldest kept), and only then are the loser rows removed.
    """
    groups = conn.execute(
        "SELECT origin_detection_id FROM setups GROUP BY origin_detection_id HAVING COUNT(*) > 1"
    ).fetchall()
    ambiguous: list[str] = []
    metrics = {"duplicate_groups": len(groups), "setups_removed": 0, "events_repointed": 0, "clearances_repointed": 0}
    for (origin_id,) in [tuple(g) for g in groups]:
        rows = conn.execute(
            """SELECT s.id, s.state, s.updated_open_time, s.closed_at,
                      (SELECT COUNT(*) FROM setup_events e WHERE e.setup_id = s.id) AS events
               FROM setups s WHERE s.origin_detection_id = ? ORDER BY s.id""",
            (origin_id,),
        ).fetchall()

        def rank(r):
            return (1 if r["state"] in _TERMINAL else 0, r["updated_open_time"] or "", int(r["events"]), -int(r["id"]))

        ranked = sorted(rows, key=rank, reverse=True)
        best = ranked[0]
        ties = [r for r in ranked if rank(r)[:3] == rank(best)[:3]]
        if len({r["state"] for r in ties}) > 1:
            ambiguous.append(f"origin_detection_id={origin_id}: " + ", ".join(f"setup {r['id']}={r['state']}" for r in ties))
            continue
        canonical = int(best["id"])
        losers = [int(r["id"]) for r in rows if int(r["id"]) != canonical]
        marks = ",".join("?" for _ in losers)
        # lifecycle events: move, then drop exact duplicates (keep the oldest per transition)
        metrics["events_repointed"] += conn.execute(f"UPDATE setup_events SET setup_id = ? WHERE setup_id IN ({marks})",
                                                    (canonical, *losers)).rowcount
        conn.execute(
            """DELETE FROM setup_events WHERE setup_id = ? AND id NOT IN (
                   SELECT MIN(id) FROM setup_events WHERE setup_id = ? GROUP BY candle_id, to_state)""",
            (canonical, canonical),
        )
        # clearances: UNIQUE(setup_id, candle_id) - keep the canonical's own row when both exist
        conn.execute(
            f"""DELETE FROM clearances WHERE setup_id IN ({marks}) AND candle_id IN (
                    SELECT candle_id FROM clearances WHERE setup_id = ?)""",
            (*losers, canonical),
        )
        metrics["clearances_repointed"] += conn.execute(f"UPDATE clearances SET setup_id = ? WHERE setup_id IN ({marks})",
                                                        (canonical, *losers)).rowcount
        conn.execute(
            """DELETE FROM clearances WHERE setup_id = ? AND id NOT IN (
                   SELECT MIN(id) FROM clearances WHERE setup_id = ? GROUP BY candle_id)""",
            (canonical, canonical),
        )
        # discord card reference: UNIQUE(setup_id) - keep the canonical's, else adopt the newest loser's
        has_ref = conn.execute("SELECT 1 FROM discord_message_refs WHERE setup_id = ?", (canonical,)).fetchone()
        if has_ref is None:
            newest = conn.execute(
                f"SELECT id FROM discord_message_refs WHERE setup_id IN ({marks}) ORDER BY last_rendered_at DESC, id DESC LIMIT 1", losers
            ).fetchone()
            if newest is not None:
                conn.execute("UPDATE discord_message_refs SET setup_id = ? WHERE id = ?", (canonical, int(newest["id"])))
        conn.execute(f"DELETE FROM discord_message_refs WHERE setup_id IN ({marks})", losers)
        metrics["setups_removed"] += conn.execute(f"DELETE FROM setups WHERE id IN ({marks})", losers).rowcount
    if ambiguous:
        raise MigrationError("cannot consolidate duplicate setups (tied candidates disagree on state); "
                             "resolve manually before migrating:\n  " + "\n  ".join(ambiguous))
    # transitions that are still duplicated inside a single setup (v1 replays): keep the oldest
    metrics["duplicate_events_removed"] = conn.execute(
        "DELETE FROM setup_events WHERE id NOT IN (SELECT MIN(id) FROM setup_events GROUP BY setup_id, candle_id, to_state)"
    ).rowcount
    # A migration_log row for version 2 is the proof that the SAFE consolidation ran; a DB
    # migrated by the old blind DELETE has no such row (tools.audit_database reports it).
    log_migration_step(conn, 2, "consolidate_duplicate_setups", metrics)


PRE_STEPS[2] = _consolidate_duplicate_setups


def _rebuild_cache_coverage(conn: sqlite3.Connection, context: dict | None = None) -> None:
    """v4: rebuild historical_cache_ranges from the candles actually stored.

    Rows written before PR #3 meant "this range was REQUESTED"; a partial or empty broker
    response still suppressed later downloads. Every legacy row is replaced by the coverage
    that the stored candles prove (calendar-expected bars all present), computed with the
    same ``verified_coverage`` rule the live cache uses. Candle rows are never modified.
    Without a SessionCalendar in ``context`` nothing can be verified, so the metadata is
    cleared and rebuilt lazily by the next download (safer than trusting it).
    """
    from aureon_mcx.broker.dhan.historical import verified_coverage  # local import: avoids storage <-> broker cycle
    from aureon_mcx.market.timeframe import Timeframe
    from aureon_mcx.market.timeutil import from_db, to_db, utc_now
    from aureon_mcx.storage.repositories import merge_intervals, _row_to_candle

    ctx = context or {}
    calendar = ctx.get("calendar")
    now = ctx.get("now") or utc_now()
    rows = conn.execute("SELECT * FROM historical_cache_ranges ORDER BY security_id, timeframe, range_start").fetchall()
    metrics = {"legacy_cache_ranges_seen": len(rows), "verified_ranges_written": 0, "invalid_ranges_removed": 0,
               "calendar_available": calendar is not None}
    conn.execute("DELETE FROM historical_cache_ranges")
    if calendar is None:
        metrics["invalid_ranges_removed"] = len(rows)
        metrics["note"] = "no calendar available: cache metadata cleared, rebuilt by the next download"
        log_migration_step(conn, 4, "rebuild_cache_coverage", metrics)
        return
    by_key: dict[tuple[str, str], list[tuple]] = {}
    for r in rows:
        by_key.setdefault((r["security_id"], r["timeframe"]), []).append((from_db(r["range_start"]), from_db(r["range_end"])))
    for (security_id, tf_value), ranges in by_key.items():
        try:
            tf = Timeframe(tf_value)
        except ValueError:
            metrics["invalid_ranges_removed"] += len(ranges)
            continue
        verified: list[tuple] = []
        for start, end in merge_intervals(ranges):
            candles = [_row_to_candle(c) for c in conn.execute(
                "SELECT * FROM candles WHERE security_id = ? AND timeframe = ? AND open_time >= ? AND open_time < ? ORDER BY open_time",
                (security_id, tf_value, to_db(start), to_db(end))).fetchall()]
            verified += verified_coverage(candles, tf, start, end, calendar=calendar, now=now)
        merged = merge_intervals(verified)
        for s_, e_ in merged:
            bars = conn.execute(
                "SELECT COUNT(*) FROM candles WHERE security_id = ? AND timeframe = ? AND open_time >= ? AND open_time < ?",
                (security_id, tf_value, to_db(s_), to_db(e_))).fetchone()[0]
            conn.execute(
                """INSERT INTO historical_cache_ranges(security_id, timeframe, range_start, range_end, bars, downloaded_at)
                   VALUES (?,?,?,?,?,?)""",
                (security_id, tf_value, to_db(s_), to_db(e_), int(bars), to_db(now)))
        metrics["verified_ranges_written"] += len(merged)
        metrics["invalid_ranges_removed"] += max(0, len(ranges) - len(merged)) if merged else len(ranges)
    log_migration_step(conn, 4, "rebuild_cache_coverage", metrics)


PRE_STEPS[4] = _rebuild_cache_coverage

SCHEMA_VERSION = MIGRATIONS[-1][0]


def current_version(conn: sqlite3.Connection) -> int:
    row = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='schema_version'"
    ).fetchone()
    if row is None:
        return 0
    v = conn.execute("SELECT COALESCE(MAX(version), 0) FROM schema_version").fetchone()[0]
    return int(v or 0)


def apply_migrations(conn: sqlite3.Connection, context: dict | None = None) -> int:
    """Apply pending migrations. ``context`` may carry {"calendar": SessionCalendar, "now": datetime}
    for data migrations that need exchange-calendar judgement (v4 cache rebuild)."""
    version = current_version(conn)
    for target, sql in MIGRATIONS:
        if target <= version:
            continue
        conn.execute("BEGIN IMMEDIATE")
        try:
            pre = PRE_STEPS.get(target)
            if pre is not None:
                pre(conn, context or {})
            _exec_script(conn, sql)
            conn.execute(
                "INSERT INTO schema_version(version, applied_at) VALUES (?, ?)",
                (target, datetime.now(tz=timezone.utc).isoformat()),
            )
            conn.execute("COMMIT")
        except Exception:
            conn.execute("ROLLBACK")
            raise
        version = target
    return version


def _exec_script(conn: sqlite3.Connection, sql: str) -> None:
    # executescript() issues an implicit COMMIT; run statements individually to
    # keep the migration inside our explicit transaction.
    for stmt in _split_statements(sql):
        conn.execute(stmt)


def _split_statements(sql: str) -> list[str]:
    out: list[str] = []
    buf: list[str] = []
    in_trigger = False
    for line in sql.splitlines():
        stripped = line.strip()
        if not stripped:
            continue
        buf.append(line)
        upper = stripped.upper()
        if upper.startswith("CREATE TRIGGER"):
            in_trigger = True
        if in_trigger:
            if upper.startswith("END;"):
                out.append("\n".join(buf))
                buf = []
                in_trigger = False
            continue
        if stripped.endswith(";"):
            out.append("\n".join(buf))
            buf = []
    if buf:
        out.append("\n".join(buf))
    return [s for s in out if s.strip()]
