"""Read-only integrity audit of an Aureon SQLite database.

    python -m aureon_mcx.tools.audit_database [--db PATH] [--json]

Reports schema version, row counts, orphan rows, setup-lifecycle inconsistencies,
duplicate transitions, historical cache ranges, unresolved market gaps, session-state
consistency and snapshot / outcome integrity. It never modifies the database and never
tries to recreate data that an earlier (destructive) migration removed: when it cannot
prove that the safe v2 consolidation ran it reports ``MANUAL_REVIEW_REQUIRED``.
"""
from __future__ import annotations

import argparse
import json
import os
import sqlite3
import sys
from pathlib import Path
from typing import Any

MANUAL_REVIEW_REQUIRED = "MANUAL_REVIEW_REQUIRED"
OK = "OK"


def _count(conn: sqlite3.Connection, sql: str, params: tuple = ()) -> int:
    row = conn.execute(sql, params).fetchone()
    return int(row[0]) if row and row[0] is not None else 0


def _table_exists(conn: sqlite3.Connection, name: str) -> bool:
    return conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (name,)).fetchone() is not None


def _rows(conn: sqlite3.Connection, sql: str, params: tuple = (), limit: int = 20) -> list[dict[str, Any]]:
    cur = conn.execute(sql, params)
    cols = [d[0] for d in cur.description]
    return [dict(zip(cols, r)) for r in cur.fetchmany(limit)]


def audit(db_path: str | os.PathLike) -> dict[str, Any]:
    path = Path(db_path)
    if not path.exists():
        return {"database_path": str(path), "status": "MISSING", "findings": ["database file does not exist"]}
    conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
    try:
        return _audit(conn, path)
    finally:
        conn.close()


def _audit(conn: sqlite3.Connection, path: Path) -> dict[str, Any]:
    findings: list[str] = []
    report: dict[str, Any] = {"database_path": str(path)}
    if not _table_exists(conn, "schema_version"):
        report.update({"status": MANUAL_REVIEW_REQUIRED, "schema_version": 0, "findings": ["schema_version table missing"]})
        return report
    versions = _rows(conn, "SELECT version, applied_at FROM schema_version ORDER BY version", limit=100)
    report["schema_version"] = max((int(v["version"]) for v in versions), default=0)
    report["migrations"] = versions
    report["migration_log"] = _rows(conn, "SELECT version, step, metrics_json, applied_at FROM migration_log ORDER BY id", limit=100) \
        if _table_exists(conn, "migration_log") else []
    for entry in report["migration_log"]:
        try:
            entry["metrics"] = json.loads(entry.pop("metrics_json"))
        except (TypeError, ValueError):
            entry["metrics"] = {}

    # ---- counts
    counts = {}
    for t in ("instruments", "candles", "indicators", "detections", "setups", "setup_events", "clearances", "feature_snapshots",
              "outcome_observations", "session_state", "market_data_gaps", "historical_cache_ranges", "discord_message_refs",
              "monitor_subscriptions", "crash_reports", "system_events", "continuity_incidents"):
        counts[t] = _count(conn, f"SELECT COUNT(*) FROM {t}") if _table_exists(conn, t) else None
    report["counts"] = counts
    report["setup_count"] = counts["setups"]
    report["detection_count"] = counts["detections"]

    # ---- orphans (FKs are normally enforced, but a DB written with foreign_keys=OFF can still hold them)
    orphans = {
        "setup_events_without_setup": _count(conn, "SELECT COUNT(*) FROM setup_events e LEFT JOIN setups s ON s.id = e.setup_id WHERE s.id IS NULL"),
        "setup_events_without_candle": _count(conn, "SELECT COUNT(*) FROM setup_events e LEFT JOIN candles c ON c.id = e.candle_id WHERE c.id IS NULL"),
        "clearances_without_setup": _count(conn, "SELECT COUNT(*) FROM clearances x LEFT JOIN setups s ON s.id = x.setup_id WHERE s.id IS NULL"),
        "indicators_without_candle": _count(conn, "SELECT COUNT(*) FROM indicators i LEFT JOIN candles c ON c.id = i.candle_id WHERE c.id IS NULL"),
        "detections_without_candle": _count(conn, "SELECT COUNT(*) FROM detections d LEFT JOIN candles c ON c.id = d.candle_id WHERE c.id IS NULL"),
        "setups_without_origin_detection": _count(conn, "SELECT COUNT(*) FROM setups s LEFT JOIN detections d ON d.id = s.origin_detection_id WHERE d.id IS NULL"),
        "snapshots_without_detection": _count(conn, "SELECT COUNT(*) FROM feature_snapshots f LEFT JOIN detections d ON d.id = f.detection_id WHERE d.id IS NULL"),
        "outcomes_without_snapshot": _count(conn, "SELECT COUNT(*) FROM outcome_observations o LEFT JOIN feature_snapshots f ON f.id = o.snapshot_id WHERE f.id IS NULL"),
        "message_refs_without_setup": _count(conn, "SELECT COUNT(*) FROM discord_message_refs r LEFT JOIN setups s ON s.id = r.setup_id WHERE s.id IS NULL"),
    }
    if _table_exists(conn, "monitor_subscriptions"):
        orphans["monitor_subscriptions_without_setup"] = _count(
            conn, "SELECT COUNT(*) FROM monitor_subscriptions m LEFT JOIN setups s ON s.id = m.setup_id WHERE s.id IS NULL")
    report["orphan_rows"] = orphans
    for k, v in orphans.items():
        if v:
            findings.append(f"{v} orphan rows: {k}")

    # ---- setup lifecycle
    terminal = ("COMPLETED", "INVALIDATED")
    marks = ",".join("?" for _ in terminal)
    lifecycle = {
        "terminal_without_closed_at": _count(conn, f"SELECT COUNT(*) FROM setups WHERE state IN ({marks}) AND closed_at IS NULL", terminal),
        "open_with_closed_at": _count(conn, f"SELECT COUNT(*) FROM setups WHERE state NOT IN ({marks}) AND closed_at IS NOT NULL", terminal),
        "setups_without_events": _count(conn, "SELECT COUNT(*) FROM setups s WHERE NOT EXISTS (SELECT 1 FROM setup_events e WHERE e.setup_id = s.id)"),
        "state_differs_from_last_event": _count(
            conn,
            """SELECT COUNT(*) FROM setups s WHERE s.state != (
                   SELECT e.to_state FROM setup_events e WHERE e.setup_id = s.id ORDER BY e.open_time DESC, e.id DESC LIMIT 1)"""),
        "duplicate_setups_per_origin": _count(
            conn, "SELECT COUNT(*) FROM (SELECT origin_detection_id FROM setups GROUP BY origin_detection_id HAVING COUNT(*) > 1)"),
    }
    report["setup_lifecycle"] = lifecycle
    for k, v in lifecycle.items():
        if v:
            findings.append(f"setup lifecycle: {k}={v}")
    dup_transitions = _rows(conn, """SELECT setup_id, candle_id, to_state, COUNT(*) AS n FROM setup_events
                                     GROUP BY setup_id, candle_id, to_state HAVING n > 1""")
    report["duplicate_transitions"] = {"count": len(dup_transitions), "examples": dup_transitions[:10]}
    if dup_transitions:
        findings.append(f"{len(dup_transitions)} duplicated lifecycle transitions")

    # ---- historical cache ranges
    ranges = _rows(conn, """SELECT security_id, timeframe, range_start, range_end, bars FROM historical_cache_ranges
                            ORDER BY security_id, timeframe, range_start""", limit=10000)
    overlaps = 0
    prev: dict | None = None
    for r in ranges:
        if prev and (prev["security_id"], prev["timeframe"]) == (r["security_id"], r["timeframe"]) and r["range_start"] < prev["range_end"]:
            overlaps += 1
        prev = r
    report["historical_cache_ranges"] = {"count": len(ranges), "overlapping_rows": overlaps, "ranges": ranges[:50]}
    if overlaps:
        findings.append(f"{overlaps} overlapping historical cache ranges (legacy metadata; migration v4 rebuilds them)")
    if ranges and report["schema_version"] < 4:
        findings.append("historical cache metadata predates migration v4 (requested-range semantics); run the application to migrate")

    # ---- market data gaps / incidents
    report["unresolved_market_gaps"] = _rows(conn, """SELECT symbol, security_id, timeframe, bucket_open_time, expected, present, detected_at
                                                       FROM market_data_gaps WHERE resolved_at IS NULL ORDER BY bucket_open_time""", limit=50) \
        if _table_exists(conn, "market_data_gaps") else []
    if report["unresolved_market_gaps"]:
        findings.append(f"{len(report['unresolved_market_gaps'])} unresolved market-data gaps")
    if _table_exists(conn, "continuity_incidents"):
        report["open_continuity_incidents"] = _rows(conn, "SELECT symbol, security_id, kind, timeframe, open_time, reason, attempt_count, last_error "
                                                          "FROM continuity_incidents WHERE state = 'OPEN' ORDER BY open_time", limit=50)
        if report["open_continuity_incidents"]:
            findings.append(f"{len(report['open_continuity_incidents'])} open continuity incidents")

    # ---- session state
    session = {
        "rows": counts["session_state"],
        "multiple_current_per_group": _count(conn, """SELECT COUNT(*) FROM (SELECT symbol, security_id, session_group FROM session_state
                                                       WHERE is_current = 1 GROUP BY symbol, security_id, session_group HAVING COUNT(*) > 1)"""),
        "blank_security_id": _count(conn, "SELECT COUNT(*) FROM session_state WHERE security_id IS NULL OR security_id = ''"),
        "contract_aware_key": _has_index_on(conn, "session_state", ("symbol", "security_id", "session_date", "session_name")),
    }
    report["session_state"] = session
    if session["multiple_current_per_group"]:
        findings.append("session_state has several current rows for one (symbol, contract, group)")
    if not session["contract_aware_key"]:
        findings.append("session_state is not contract-aware (schema < v3)")

    # ---- snapshot / outcome integrity
    snap = {
        "snapshots": counts["feature_snapshots"],
        "snapshots_without_outcomes": _count(conn, """SELECT COUNT(*) FROM feature_snapshots f WHERE NOT EXISTS (
                                                       SELECT 1 FROM outcome_observations o WHERE o.snapshot_id = f.id)"""),
        "final_marker_without_horizons": _count(conn, """SELECT COUNT(*) FROM outcome_observations m WHERE m.horizon = '__all_final__' AND NOT EXISTS (
                                                          SELECT 1 FROM outcome_observations o WHERE o.snapshot_id = m.snapshot_id AND o.horizon != '__all_final__')"""),
        "final_rows_labelled_pending": _count(conn, "SELECT COUNT(*) FROM outcome_observations WHERE is_final = 1 AND label = 'PENDING'"),
        "duplicate_snapshot_per_detection": _count(conn, "SELECT COUNT(*) FROM (SELECT detection_id FROM feature_snapshots GROUP BY detection_id HAVING COUNT(*) > 1)"),
    }
    report["snapshot_outcome_integrity"] = snap
    for k in ("final_marker_without_horizons", "final_rows_labelled_pending", "duplicate_snapshot_per_detection"):
        if snap[k]:
            findings.append(f"outcome integrity: {k}={snap[k]}")

    # ---- migration history: was v2 the blind DELETE or the safe consolidation?
    v2_applied = any(int(v["version"]) == 2 for v in versions)
    v2_logged = any(int(e["version"]) == 2 for e in report["migration_log"])
    suspicious = v2_applied and not v2_logged and (counts["setups"] or 0) > 0
    report["migration_history"] = {
        "v2_applied": v2_applied, "v2_safe_consolidation_logged": v2_logged,
        "note": ("schema v2 was applied without a migration_log record: this database may have been migrated by the "
                 "blind duplicate DELETE (pre PR #3) or by the safe consolidation before it logged; permanently removed "
                 "setups cannot be recreated") if suspicious else None,
    }
    if suspicious:
        findings.append("migration history: v2 applied by a build that did not log the safe consolidation")
    report["findings"] = findings
    report["status"] = MANUAL_REVIEW_REQUIRED if (suspicious or any(orphans.values()) or dup_transitions
                                                  or lifecycle["duplicate_setups_per_origin"]) else OK
    return report


def _has_index_on(conn: sqlite3.Connection, table: str, columns: tuple[str, ...]) -> bool:
    for idx in conn.execute(f"PRAGMA index_list({table})").fetchall():
        name, unique = idx[1], idx[2]
        cols = tuple(r[2] for r in conn.execute(f"PRAGMA index_info({name})").fetchall())
        if unique and cols == columns:
            return True
    return False


def render_text(report: dict[str, Any]) -> str:
    lines = [f"database: {report.get('database_path')}", f"status: {report.get('status')}",
             f"schema_version: {report.get('schema_version')}"]
    counts = report.get("counts") or {}
    lines.append("counts: " + ", ".join(f"{k}={v}" for k, v in counts.items() if v is not None))
    for section in ("orphan_rows", "setup_lifecycle", "session_state", "snapshot_outcome_integrity"):
        data = report.get(section) or {}
        lines.append(f"{section}: " + ", ".join(f"{k}={v}" for k, v in data.items()))
    lines.append(f"duplicate_transitions: {report.get('duplicate_transitions', {}).get('count', 0)}")
    lines.append(f"historical_cache_ranges: {report.get('historical_cache_ranges', {}).get('count', 0)}")
    lines.append(f"unresolved_market_gaps: {len(report.get('unresolved_market_gaps') or [])}")
    hist = report.get("migration_history") or {}
    if hist.get("note"):
        lines.append(f"migration_history: {hist['note']}")
    for f in report.get("findings") or []:
        lines.append(f"  ! {f}")
    return "\n".join(lines)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Audit an Aureon SQLite database (read-only)")
    parser.add_argument("--db", default=os.environ.get("AUREON_LOCAL_DB_PATH", "data/aureon_mcx.db"))
    parser.add_argument("--json", action="store_true", help="print the report as JSON")
    args = parser.parse_args(argv)
    report = audit(args.db)
    print(json.dumps(report, indent=2, default=str) if args.json else render_text(report))
    return 0 if report.get("status") == OK else 1


if __name__ == "__main__":
    sys.exit(main())
