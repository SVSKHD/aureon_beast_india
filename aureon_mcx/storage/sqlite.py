"""SQLite connection management.

Every connection sets:  journal_mode=WAL, foreign_keys=ON, busy_timeout=5000.
Transactions are short: one logical write per transaction.
"""
from __future__ import annotations

import sqlite3
import threading
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator

from .migrations import apply_migrations


class StorageError(RuntimeError):
    pass


class Database:
    def __init__(self, path: str | Path):
        self.path = str(path)
        if self.path != ":memory:":
            Path(self.path).parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.RLock()
        self._conn = sqlite3.connect(self.path, check_same_thread=False, isolation_level=None)
        self._conn.row_factory = sqlite3.Row
        self._apply_pragmas()

    def _apply_pragmas(self) -> None:
        cur = self._conn.cursor()
        cur.execute("PRAGMA journal_mode = WAL")
        cur.execute("PRAGMA foreign_keys = ON")
        cur.execute("PRAGMA busy_timeout = 5000")
        cur.close()

    def pragmas(self) -> dict[str, object]:
        cur = self._conn.cursor()
        out = {
            "journal_mode": cur.execute("PRAGMA journal_mode").fetchone()[0],
            "foreign_keys": cur.execute("PRAGMA foreign_keys").fetchone()[0],
            "busy_timeout": cur.execute("PRAGMA busy_timeout").fetchone()[0],
        }
        cur.close()
        return out

    @property
    def conn(self) -> sqlite3.Connection:
        return self._conn

    def migrate(self, context: dict | None = None) -> int:
        """Apply pending migrations; ``context`` (calendar, now) feeds data migrations."""
        with self._lock:
            return apply_migrations(self._conn, context)

    @contextmanager
    def transaction(self) -> Iterator[sqlite3.Connection]:
        """Short explicit transaction. Nested use re-enters the same transaction."""
        with self._lock:
            if self._conn.in_transaction:
                yield self._conn
                return
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                yield self._conn
            except Exception:
                self._conn.execute("ROLLBACK")
                raise
            else:
                self._conn.execute("COMMIT")

    def execute(self, sql: str, params: tuple | dict = ()) -> sqlite3.Cursor:
        with self._lock:
            return self._conn.execute(sql, params)

    def query(self, sql: str, params: tuple | dict = ()) -> list[sqlite3.Row]:
        with self._lock:
            cur = self._conn.execute(sql, params)
            rows = cur.fetchall()
            cur.close()
            return rows

    def query_one(self, sql: str, params: tuple | dict = ()) -> sqlite3.Row | None:
        rows = self.query(sql, params)
        return rows[0] if rows else None

    def close(self) -> None:
        with self._lock:
            self._conn.close()
