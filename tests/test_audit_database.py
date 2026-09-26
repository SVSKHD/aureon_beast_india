from __future__ import annotations

import json
import sqlite3

from aureon_mcx.storage.migrations import MIGRATIONS, _exec_script, apply_migrations
from aureon_mcx.tools.audit_database import MANUAL_REVIEW_REQUIRED, OK, audit, main, render_text
from tests.test_migrations import NOW, add_candle, add_detection, add_event, add_setup


def _file_db(tmp_path, name="audit.db"):
    path = tmp_path / name
    conn = sqlite3.connect(path, isolation_level=None)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    _exec_script(conn, MIGRATIONS[0][1])
    conn.execute("INSERT INTO schema_version(version, applied_at) VALUES (1, ?)", (NOW,))
    return path, conn


def test_audit_clean_database_is_ok(tmp_path):
    path, conn = _file_db(tmp_path)
    c = add_candle(conn)
    det = add_detection(conn, c)
    s = add_setup(conn, det, c, "WATCH")
    add_event(conn, s, c, "WATCH")
    apply_migrations(conn, {})
    conn.close()
    report = audit(path)
    assert report["status"] == OK and report["schema_version"] >= 5
    assert report["setup_count"] == 1 and report["detection_count"] == 1
    assert report["database_path"] == str(path)
    assert not any(report["orphan_rows"].values())
    assert report["migration_history"]["v2_safe_consolidation_logged"] is True
    assert report["session_state"]["contract_aware_key"] is True
    assert "status: OK" in render_text(report)


def test_audit_flags_blind_v2_history_and_inconsistencies(tmp_path):
    path, conn = _file_db(tmp_path)
    c = add_candle(conn)
    det = add_detection(conn, c)
    s = add_setup(conn, det, c, "COMPLETED")  # terminal without closed_at, state differs from last event
    add_event(conn, s, c, "OBSERVING")
    # simulate a database migrated by the OLD blind v2 (schema_version rows, no migration_log)
    for version, sql in MIGRATIONS[1:]:
        _exec_script(conn, sql)
        conn.execute("INSERT INTO schema_version(version, applied_at) VALUES (?, ?)", (version, NOW))
    conn.execute("PRAGMA foreign_keys = OFF")
    conn.execute("""INSERT INTO clearances(setup_id, candle_id, open_time, cleared, policy_version, mtf_state, blockers_json, badges_json,
                    fakeout_flags_json, evidence_json, created_at) VALUES (999, ?, ?, 1, 'v', 'x', '[]', '[]', '[]', '{}', ?)""", (c, NOW, NOW))
    conn.close()
    report = audit(path)
    assert report["status"] == MANUAL_REVIEW_REQUIRED
    assert report["migration_history"]["v2_applied"] and not report["migration_history"]["v2_safe_consolidation_logged"]
    assert report["orphan_rows"]["clearances_without_setup"] == 1
    assert report["setup_lifecycle"]["terminal_without_closed_at"] == 1
    assert report["setup_lifecycle"]["state_differs_from_last_event"] == 1
    assert any("blind" in f or "did not log" in f for f in report["findings"])


def test_audit_cli_json(tmp_path, capsys):
    path, conn = _file_db(tmp_path)
    apply_migrations(conn, {})
    conn.close()
    assert main(["--db", str(path), "--json"]) == 0
    out = json.loads(capsys.readouterr().out)
    assert out["status"] == OK and out["counts"]["setups"] == 0
    assert main(["--db", str(tmp_path / "missing.db")]) == 1
