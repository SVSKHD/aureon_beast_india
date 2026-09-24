"""Repositories: the only code that writes SQL.

Rules:
  * One logical write per transaction (short transactions).
  * Dependent rows require their parent id (FK enforced; also checked here).
  * Every analytic row carries security_id, expiry_date, timeframe, open_time.
"""
from __future__ import annotations

import json
import sqlite3
from datetime import date, datetime
from typing import Any, Iterable

from aureon_mcx.confirmation.models import ClearanceResult
from aureon_mcx.detection.models import Detection, DetectionFamily, Direction
from aureon_mcx.indicators.models import IndicatorRow, RsiDirection
from aureon_mcx.market.candle import Candle
from aureon_mcx.market.timeframe import Timeframe
from aureon_mcx.market.timeutil import from_db, to_db, utc_now
from aureon_mcx.outcomes.models import FeatureSnapshot, OutcomeObservation
from aureon_mcx.setups.models import Setup, SetupEvent, SetupState
from aureon_mcx.structure.models import Pivot, PivotKind, StructureLabel

from .sqlite import Database, StorageError


class DependencyError(StorageError):
    """A dependent row was written before its parent exists."""


def _require_parent(value: int | None, what: str) -> int:
    if value is None:
        raise DependencyError(f"{what} must be persisted before dependent rows can be written")
    return int(value)


# ------------------------------------------------------------------ instruments
class InstrumentRepository:
    def __init__(self, db: Database):
        self.db = db

    def upsert_active(self, rec: dict[str, Any]) -> int:
        """Store a resolved instrument and mark it the only active one for its logical symbol."""
        now = to_db(utc_now())
        with self.db.transaction() as conn:
            conn.execute("UPDATE instruments SET is_active = 0 WHERE logical_symbol = ?", (rec["logical_symbol"],))
            conn.execute(
                """INSERT INTO instruments(logical_symbol, display_symbol, security_id, exchange, exchange_segment,
                      instrument_type, expiry_date, lot_size, tick_size, trading_symbol, custom_symbol,
                      resolved_at, instrument_master_version, is_active)
                   VALUES (:logical_symbol, :display_symbol, :security_id, :exchange, :exchange_segment,
                      :instrument_type, :expiry_date, :lot_size, :tick_size, :trading_symbol, :custom_symbol,
                      :resolved_at, :instrument_master_version, 1)
                   ON CONFLICT(logical_symbol, security_id, expiry_date) DO UPDATE SET
                      is_active = 1, resolved_at = excluded.resolved_at,
                      instrument_master_version = excluded.instrument_master_version,
                      display_symbol = excluded.display_symbol, lot_size = excluded.lot_size,
                      tick_size = excluded.tick_size, trading_symbol = excluded.trading_symbol,
                      custom_symbol = excluded.custom_symbol""",
                {**rec, "resolved_at": rec.get("resolved_at") or now},
            )
            row = conn.execute(
                "SELECT id FROM instruments WHERE logical_symbol = ? AND is_active = 1", (rec["logical_symbol"],)
            ).fetchone()
        return int(row["id"])

    def active(self, logical_symbol: str) -> sqlite3.Row | None:
        return self.db.query_one("SELECT * FROM instruments WHERE logical_symbol = ? AND is_active = 1", (logical_symbol,))

    def all_active(self) -> list[sqlite3.Row]:
        return self.db.query("SELECT * FROM instruments WHERE is_active = 1 ORDER BY logical_symbol")


# ---------------------------------------------------------------------- candles
def _row_to_candle(row: sqlite3.Row) -> Candle:
    return Candle(
        symbol=row["symbol"],
        security_id=row["security_id"],
        timeframe=Timeframe(row["timeframe"]),
        open_time=from_db(row["open_time"]),
        open=row["open"],
        high=row["high"],
        low=row["low"],
        close=row["close"],
        volume=row["volume"],
        open_interest=row["open_interest"],
        source=row["source"],
        is_closed=bool(row["is_closed"]),
        expiry_date=row["expiry_date"],
        id=row["id"],
    )


class CandleRepository:
    def __init__(self, db: Database):
        self.db = db

    def insert(self, candle: Candle) -> Candle:
        """Insert a CLOSED candle (idempotent on security_id/timeframe/open_time)."""
        if not candle.is_closed:
            raise StorageError("refusing to store an incomplete candle")
        now = to_db(utc_now())
        with self.db.transaction() as conn:
            conn.execute(
                """INSERT INTO candles(symbol, security_id, expiry_date, timeframe, open_time, open, high, low, close,
                       volume, open_interest, source, is_closed, created_at)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?,?,1,?)
                   ON CONFLICT(security_id, timeframe, open_time) DO UPDATE SET
                       open = excluded.open, high = excluded.high, low = excluded.low, close = excluded.close,
                       volume = excluded.volume, open_interest = excluded.open_interest""",
                (
                    candle.symbol, candle.security_id, candle.expiry_date, candle.timeframe.value,
                    to_db(candle.open_time), candle.open, candle.high, candle.low, candle.close,
                    candle.volume, candle.open_interest, candle.source, now,
                ),
            )
            row = conn.execute(
                "SELECT id FROM candles WHERE security_id = ? AND timeframe = ? AND open_time = ?",
                (candle.security_id, candle.timeframe.value, to_db(candle.open_time)),
            ).fetchone()
        return candle.with_id(int(row["id"]))

    def insert_many(self, candles: Iterable[Candle]) -> list[Candle]:
        return [self.insert(c) for c in candles]

    def get(self, candle_id: int) -> Candle | None:
        row = self.db.query_one("SELECT * FROM candles WHERE id = ?", (candle_id,))
        return _row_to_candle(row) if row else None

    def find(self, security_id: str, timeframe: Timeframe, open_time: datetime) -> Candle | None:
        row = self.db.query_one(
            "SELECT * FROM candles WHERE security_id = ? AND timeframe = ? AND open_time = ?",
            (security_id, timeframe.value, to_db(open_time)),
        )
        return _row_to_candle(row) if row else None

    def latest(self, security_id: str, timeframe: Timeframe, limit: int) -> list[Candle]:
        rows = self.db.query(
            "SELECT * FROM candles WHERE security_id = ? AND timeframe = ? ORDER BY open_time DESC LIMIT ?",
            (security_id, timeframe.value, limit),
        )
        return [_row_to_candle(r) for r in reversed(rows)]

    def range(self, security_id: str, timeframe: Timeframe, start: datetime, end: datetime) -> list[Candle]:
        rows = self.db.query(
            "SELECT * FROM candles WHERE security_id = ? AND timeframe = ? AND open_time >= ? AND open_time < ? ORDER BY open_time",
            (security_id, timeframe.value, to_db(start), to_db(end)),
        )
        return [_row_to_candle(r) for r in rows]

    def after(self, security_id: str, timeframe: Timeframe, open_time: datetime, limit: int) -> list[Candle]:
        """Candles strictly AFTER open_time (used by outcome observation; no leakage)."""
        rows = self.db.query(
            "SELECT * FROM candles WHERE security_id = ? AND timeframe = ? AND open_time > ? ORDER BY open_time LIMIT ?",
            (security_id, timeframe.value, to_db(open_time), limit),
        )
        return [_row_to_candle(r) for r in rows]

    def count(self, security_id: str, timeframe: Timeframe) -> int:
        row = self.db.query_one("SELECT COUNT(*) AS n FROM candles WHERE security_id = ? AND timeframe = ?", (security_id, timeframe.value))
        return int(row["n"]) if row else 0

    def last_open_time(self, security_id: str, timeframe: Timeframe) -> datetime | None:
        row = self.db.query_one(
            "SELECT MAX(open_time) AS t FROM candles WHERE security_id = ? AND timeframe = ?", (security_id, timeframe.value)
        )
        return from_db(row["t"]) if row and row["t"] else None


class HistoricalCacheRepository:
    def __init__(self, db: Database):
        self.db = db

    def is_cached(self, security_id: str, timeframe: Timeframe, start: datetime, end: datetime) -> bool:
        row = self.db.query_one(
            """SELECT 1 FROM historical_cache_ranges
               WHERE security_id = ? AND timeframe = ? AND range_start <= ? AND range_end >= ? LIMIT 1""",
            (security_id, timeframe.value, to_db(start), to_db(end)),
        )
        return row is not None

    def record(self, security_id: str, timeframe: Timeframe, start: datetime, end: datetime, bars: int) -> None:
        with self.db.transaction() as conn:
            conn.execute(
                """INSERT INTO historical_cache_ranges(security_id, timeframe, range_start, range_end, bars, downloaded_at)
                   VALUES (?,?,?,?,?,?)
                   ON CONFLICT(security_id, timeframe, range_start, range_end) DO UPDATE SET bars = excluded.bars,
                   downloaded_at = excluded.downloaded_at""",
                (security_id, timeframe.value, to_db(start), to_db(end), bars, to_db(utc_now())),
            )


# ------------------------------------------------------------------- indicators
class IndicatorRepository:
    def __init__(self, db: Database):
        self.db = db

    def insert(self, row: IndicatorRow) -> IndicatorRow:
        candle_id = _require_parent(row.candle_id, "candle")
        with self.db.transaction() as conn:
            conn.execute(
                """INSERT INTO indicators(candle_id, symbol, security_id, expiry_date, timeframe, open_time, ema_fast, ema_slow,
                       ema_gap, ema_fast_slope, rsi, rsi_direction, atr, volume, open_interest, warmed,
                       ema_fast_period, ema_slow_period, rsi_period, atr_period)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                   ON CONFLICT(candle_id) DO UPDATE SET ema_fast = excluded.ema_fast, ema_slow = excluded.ema_slow,
                       ema_gap = excluded.ema_gap, ema_fast_slope = excluded.ema_fast_slope, rsi = excluded.rsi,
                       rsi_direction = excluded.rsi_direction, atr = excluded.atr, volume = excluded.volume,
                       open_interest = excluded.open_interest, warmed = excluded.warmed""",
                (
                    candle_id, row.symbol, row.security_id, row.expiry_date, row.timeframe.value, to_db(row.open_time),
                    row.ema_fast, row.ema_slow, row.ema_gap, row.ema_fast_slope, row.rsi,
                    row.rsi_direction.value if row.rsi_direction else None, row.atr, row.volume, row.open_interest,
                    1 if row.warmed else 0, row.ema_fast_period, row.ema_slow_period, row.rsi_period, row.atr_period,
                ),
            )
            rid = conn.execute("SELECT id FROM indicators WHERE candle_id = ?", (candle_id,)).fetchone()["id"]
        return IndicatorRow(**{**row.__dict__, "id": int(rid)})

    def for_candles(self, candle_ids: list[int]) -> dict[int, IndicatorRow]:
        if not candle_ids:
            return {}
        q = ",".join("?" for _ in candle_ids)
        rows = self.db.query(f"SELECT * FROM indicators WHERE candle_id IN ({q})", tuple(candle_ids))
        return {int(r["candle_id"]): self._to_row(r) for r in rows}

    def latest(self, security_id: str, timeframe: Timeframe, limit: int) -> list[IndicatorRow]:
        rows = self.db.query(
            "SELECT * FROM indicators WHERE security_id = ? AND timeframe = ? ORDER BY open_time DESC LIMIT ?",
            (security_id, timeframe.value, limit),
        )
        return [self._to_row(r) for r in reversed(rows)]

    @staticmethod
    def _to_row(r: sqlite3.Row) -> IndicatorRow:
        return IndicatorRow(
            symbol=r["symbol"], security_id=r["security_id"], expiry_date=r["expiry_date"],
            timeframe=Timeframe(r["timeframe"]), open_time=from_db(r["open_time"]),
            ema_fast=r["ema_fast"], ema_slow=r["ema_slow"], ema_gap=r["ema_gap"], ema_fast_slope=r["ema_fast_slope"],
            rsi=r["rsi"], rsi_direction=RsiDirection(r["rsi_direction"]) if r["rsi_direction"] else None,
            atr=r["atr"], volume=r["volume"], open_interest=r["open_interest"],
            ema_fast_period=r["ema_fast_period"], ema_slow_period=r["ema_slow_period"],
            rsi_period=r["rsi_period"], atr_period=r["atr_period"], candle_id=r["candle_id"], id=r["id"],
        )


# --------------------------------------------------------------------- pivots
class PivotRepository:
    def __init__(self, db: Database):
        self.db = db

    def insert(self, p: Pivot) -> Pivot:
        cid = _require_parent(p.candle_id, "pivot candle")
        ccid = _require_parent(p.confirmed_candle_id, "confirming candle")
        with self.db.transaction() as conn:
            conn.execute(
                """INSERT INTO structure_pivots(candle_id, confirmed_candle_id, symbol, security_id, expiry_date, timeframe,
                       kind, price, label, pivot_open_time, confirmed_at_open_time, strength)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
                   ON CONFLICT(security_id, timeframe, kind, pivot_open_time) DO NOTHING""",
                (cid, ccid, p.symbol, p.security_id, p.expiry_date, p.timeframe.value, p.kind.value, p.price,
                 p.label.value, to_db(p.pivot_open_time), to_db(p.confirmed_at_open_time), p.strength),
            )
            row = conn.execute(
                "SELECT id FROM structure_pivots WHERE security_id = ? AND timeframe = ? AND kind = ? AND pivot_open_time = ?",
                (p.security_id, p.timeframe.value, p.kind.value, to_db(p.pivot_open_time)),
            ).fetchone()
        return Pivot(**{**p.__dict__, "id": int(row["id"])})

    def latest(self, security_id: str, timeframe: Timeframe, limit: int, confirmed_by: datetime | None = None) -> list[Pivot]:
        """Pivots ordered by pivot_open_time. `confirmed_by` hides pivots not yet confirmed at that bar."""
        if confirmed_by is None:
            rows = self.db.query(
                "SELECT * FROM structure_pivots WHERE security_id = ? AND timeframe = ? ORDER BY pivot_open_time DESC LIMIT ?",
                (security_id, timeframe.value, limit),
            )
        else:
            rows = self.db.query(
                """SELECT * FROM structure_pivots WHERE security_id = ? AND timeframe = ? AND confirmed_at_open_time <= ?
                   ORDER BY pivot_open_time DESC LIMIT ?""",
                (security_id, timeframe.value, to_db(confirmed_by), limit),
            )
        return [self._to(r) for r in reversed(rows)]

    @staticmethod
    def _to(r: sqlite3.Row) -> Pivot:
        return Pivot(
            symbol=r["symbol"], security_id=r["security_id"], expiry_date=r["expiry_date"], timeframe=Timeframe(r["timeframe"]),
            kind=PivotKind(r["kind"]), price=r["price"], label=StructureLabel(r["label"]),
            pivot_open_time=from_db(r["pivot_open_time"]), confirmed_at_open_time=from_db(r["confirmed_at_open_time"]),
            strength=r["strength"], candle_id=r["candle_id"], confirmed_candle_id=r["confirmed_candle_id"], id=r["id"],
        )


# ------------------------------------------------------------------ day frames
class DayFrameRepository:
    def __init__(self, db: Database):
        self.db = db

    def upsert(self, symbol: str, security_id: str, expiry_date: str, timeframe: Timeframe, trading_date: date,
               candle: Candle, is_closed: bool, structure_context: str | None = None,
               structure_sequence: str | None = None) -> None:
        cid = _require_parent(candle.id, "candle")
        now = to_db(utc_now())
        with self.db.transaction() as conn:
            existing = conn.execute(
                "SELECT * FROM market_day_frames WHERE security_id = ? AND timeframe = ? AND trading_date = ?",
                (security_id, timeframe.value, trading_date.isoformat()),
            ).fetchone()
            if existing is None:
                conn.execute(
                    """INSERT INTO market_day_frames(symbol, security_id, expiry_date, timeframe, trading_date, open, high, low, close,
                           bars, is_closed, last_candle_id, last_open_time, structure_context, structure_sequence, updated_at)
                       VALUES (?,?,?,?,?,?,?,?,?,1,?,?,?,?,?,?)""",
                    (symbol, security_id, expiry_date, timeframe.value, trading_date.isoformat(), candle.open, candle.high,
                     candle.low, candle.close, int(is_closed), cid, to_db(candle.open_time), structure_context,
                     structure_sequence, now),
                )
            else:
                if existing["is_closed"]:
                    return  # closed frames are frozen
                conn.execute(
                    """UPDATE market_day_frames SET high = MAX(high, ?), low = MIN(low, ?), close = ?, bars = bars + 1,
                           is_closed = ?, last_candle_id = ?, last_open_time = ?, structure_context = COALESCE(?, structure_context),
                           structure_sequence = COALESCE(?, structure_sequence), updated_at = ? WHERE id = ?""",
                    (candle.high, candle.low, candle.close, int(is_closed), cid, to_db(candle.open_time),
                     structure_context, structure_sequence, now, existing["id"]),
                )

    def close_frame(self, security_id: str, timeframe: Timeframe, trading_date: date) -> None:
        with self.db.transaction() as conn:
            conn.execute(
                "UPDATE market_day_frames SET is_closed = 1, updated_at = ? WHERE security_id = ? AND timeframe = ? AND trading_date = ?",
                (to_db(utc_now()), security_id, timeframe.value, trading_date.isoformat()),
            )

    def previous_closed(self, security_id: str, timeframe: Timeframe, before_date: date) -> sqlite3.Row | None:
        return self.db.query_one(
            """SELECT * FROM market_day_frames WHERE security_id = ? AND timeframe = ? AND trading_date < ? AND is_closed = 1
               ORDER BY trading_date DESC LIMIT 1""",
            (security_id, timeframe.value, before_date.isoformat()),
        )

    def get(self, security_id: str, timeframe: Timeframe, trading_date: date) -> sqlite3.Row | None:
        return self.db.query_one(
            "SELECT * FROM market_day_frames WHERE security_id = ? AND timeframe = ? AND trading_date = ?",
            (security_id, timeframe.value, trading_date.isoformat()),
        )


# ------------------------------------------------------------------ detections
class DetectionRepository:
    def __init__(self, db: Database):
        self.db = db

    def insert(self, d: Detection) -> Detection:
        cid = _require_parent(d.candle_id, "candle")
        with self.db.transaction() as conn:
            conn.execute(
                """INSERT INTO detections(candle_id, symbol, security_id, expiry_date, timeframe, open_time, family, kind, direction,
                       price, session, label, payload_json, created_at)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                   ON CONFLICT(candle_id, family, kind, direction) DO NOTHING""",
                (cid, d.symbol, d.security_id, d.expiry_date, d.timeframe.value, to_db(d.open_time), d.family.value, d.kind,
                 d.direction.value, d.price, d.session, d.label, d.payload_json, to_db(utc_now())),
            )
            row = conn.execute(
                "SELECT id FROM detections WHERE candle_id = ? AND family = ? AND kind = ? AND direction = ?",
                (cid, d.family.value, d.kind, d.direction.value),
            ).fetchone()
        d.id = int(row["id"])
        return d

    def get(self, detection_id: int) -> Detection | None:
        row = self.db.query_one("SELECT * FROM detections WHERE id = ?", (detection_id,))
        return self._to(row) if row else None

    def latest(self, security_id: str, timeframe: Timeframe, limit: int, since: datetime | None = None) -> list[Detection]:
        if since is None:
            rows = self.db.query(
                "SELECT * FROM detections WHERE security_id = ? AND timeframe = ? ORDER BY open_time DESC, id DESC LIMIT ?",
                (security_id, timeframe.value, limit),
            )
        else:
            rows = self.db.query(
                """SELECT * FROM detections WHERE security_id = ? AND timeframe = ? AND open_time >= ?
                   ORDER BY open_time DESC, id DESC LIMIT ?""",
                (security_id, timeframe.value, to_db(since), limit),
            )
        return [self._to(r) for r in reversed(rows)]

    def by_ids(self, ids: list[int]) -> list[Detection]:
        if not ids:
            return []
        q = ",".join("?" for _ in ids)
        return [self._to(r) for r in self.db.query(f"SELECT * FROM detections WHERE id IN ({q}) ORDER BY id", tuple(ids))]

    def without_snapshot(self, limit: int = 500) -> list[Detection]:
        rows = self.db.query(
            """SELECT d.* FROM detections d LEFT JOIN feature_snapshots s ON s.detection_id = d.id
               WHERE s.id IS NULL ORDER BY d.id LIMIT ?""",
            (limit,),
        )
        return [self._to(r) for r in rows]

    @staticmethod
    def _to(r: sqlite3.Row) -> Detection:
        return Detection(
            symbol=r["symbol"], security_id=r["security_id"], expiry_date=r["expiry_date"], timeframe=Timeframe(r["timeframe"]),
            open_time=from_db(r["open_time"]), family=DetectionFamily(r["family"]), kind=r["kind"],
            direction=Direction(r["direction"]), price=r["price"], label=r["label"], session=r["session"],
            payload=json.loads(r["payload_json"]), candle_id=r["candle_id"], id=r["id"],
        )


# ---------------------------------------------------------------------- setups
class SetupRepository:
    def __init__(self, db: Database):
        self.db = db

    def insert(self, s: Setup) -> Setup:
        _require_parent(s.origin_detection_id, "origin detection")
        _require_parent(s.origin_candle_id, "origin candle")
        now = to_db(utc_now())
        with self.db.transaction() as conn:
            cur = conn.execute(
                """INSERT INTO setups(symbol, security_id, expiry_date, timeframe, family, direction, state, anchor_price,
                       invalidation_price, origin_detection_id, origin_candle_id, created_open_time, updated_open_time,
                       context_json, closed_at, created_at, updated_at)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (s.symbol, s.security_id, s.expiry_date, s.timeframe.value, s.family, s.direction.value, s.state.value,
                 s.anchor_price, s.invalidation_price, s.origin_detection_id, s.origin_candle_id, to_db(s.created_open_time),
                 to_db(s.updated_open_time), s.context_json, to_db(s.closed_at), now, now),
            )
            s.id = int(cur.lastrowid)
        return s

    def update_state(self, s: Setup) -> None:
        sid = _require_parent(s.id, "setup")
        with self.db.transaction() as conn:
            conn.execute(
                """UPDATE setups SET state = ?, updated_open_time = ?, context_json = ?, closed_at = ?, updated_at = ?,
                       anchor_price = ?, invalidation_price = ? WHERE id = ?""",
                (s.state.value, to_db(s.updated_open_time), s.context_json, to_db(s.closed_at), to_db(utc_now()),
                 s.anchor_price, s.invalidation_price, sid),
            )

    def add_event(self, ev: SetupEvent) -> SetupEvent:
        _require_parent(ev.setup_id, "setup")
        _require_parent(ev.candle_id, "candle")
        with self.db.transaction() as conn:
            cur = conn.execute(
                """INSERT INTO setup_events(setup_id, candle_id, detection_id, from_state, to_state, reason, evidence_json,
                       open_time, created_at) VALUES (?,?,?,?,?,?,?,?,?)""",
                (ev.setup_id, ev.candle_id, ev.detection_id, ev.from_state.value if ev.from_state else None,
                 ev.to_state.value, ev.reason, ev.evidence_json, to_db(ev.open_time), to_db(utc_now())),
            )
            ev.id = int(cur.lastrowid)
        return ev

    def events(self, setup_id: int, limit: int | None = None) -> list[SetupEvent]:
        sql = "SELECT * FROM setup_events WHERE setup_id = ? ORDER BY id"
        rows = self.db.query(sql, (setup_id,))
        evs = [
            SetupEvent(
                setup_id=r["setup_id"], candle_id=r["candle_id"], detection_id=r["detection_id"],
                from_state=SetupState(r["from_state"]) if r["from_state"] else None, to_state=SetupState(r["to_state"]),
                reason=r["reason"], evidence=json.loads(r["evidence_json"]), open_time=from_db(r["open_time"]), id=r["id"],
            )
            for r in rows
        ]
        return evs[-limit:] if limit else evs

    def get(self, setup_id: int) -> Setup | None:
        row = self.db.query_one("SELECT * FROM setups WHERE id = ?", (setup_id,))
        return self._to(row) if row else None

    def open_for(self, security_id: str, timeframe: Timeframe) -> list[Setup]:
        rows = self.db.query(
            "SELECT * FROM setups WHERE security_id = ? AND timeframe = ? AND closed_at IS NULL ORDER BY id",
            (security_id, timeframe.value),
        )
        return [self._to(r) for r in rows]

    def latest(self, security_id: str, timeframe: Timeframe, limit: int) -> list[Setup]:
        rows = self.db.query(
            "SELECT * FROM setups WHERE security_id = ? AND timeframe = ? ORDER BY id DESC LIMIT ?",
            (security_id, timeframe.value, limit),
        )
        return [self._to(r) for r in reversed(rows)]

    @staticmethod
    def _to(r: sqlite3.Row) -> Setup:
        return Setup(
            symbol=r["symbol"], security_id=r["security_id"], expiry_date=r["expiry_date"], timeframe=Timeframe(r["timeframe"]),
            family=r["family"], direction=Direction(r["direction"]), state=SetupState(r["state"]), anchor_price=r["anchor_price"],
            invalidation_price=r["invalidation_price"], origin_detection_id=r["origin_detection_id"],
            origin_candle_id=r["origin_candle_id"], created_open_time=from_db(r["created_open_time"]),
            updated_open_time=from_db(r["updated_open_time"]), context=json.loads(r["context_json"]),
            closed_at=from_db(r["closed_at"]), id=r["id"],
        )


# ------------------------------------------------------------------ clearances
class ClearanceRepository:
    def __init__(self, db: Database):
        self.db = db

    def insert(self, c: ClearanceResult) -> ClearanceResult:
        _require_parent(c.setup_id, "setup")
        _require_parent(c.candle_id, "candle")
        f = c.to_json_fields()
        with self.db.transaction() as conn:
            conn.execute(
                """INSERT INTO clearances(setup_id, candle_id, open_time, cleared, policy_version, mtf_state, blockers_json,
                       badges_json, fakeout_flags_json, evidence_json, created_at) VALUES (?,?,?,?,?,?,?,?,?,?,?)
                   ON CONFLICT(setup_id, candle_id) DO UPDATE SET cleared = excluded.cleared, blockers_json = excluded.blockers_json,
                       badges_json = excluded.badges_json, fakeout_flags_json = excluded.fakeout_flags_json,
                       evidence_json = excluded.evidence_json, mtf_state = excluded.mtf_state, policy_version = excluded.policy_version""",
                (c.setup_id, c.candle_id, to_db(c.open_time), int(c.cleared), c.policy_version, c.mtf_state,
                 f["blockers_json"], f["badges_json"], f["fakeout_flags_json"], f["evidence_json"], to_db(utc_now())),
            )
            row = conn.execute("SELECT id FROM clearances WHERE setup_id = ? AND candle_id = ?", (c.setup_id, c.candle_id)).fetchone()
        c.id = int(row["id"])
        return c

    def latest_for_setup(self, setup_id: int) -> ClearanceResult | None:
        r = self.db.query_one("SELECT * FROM clearances WHERE setup_id = ? ORDER BY open_time DESC, id DESC LIMIT 1", (setup_id,))
        if r is None:
            return None
        return ClearanceResult(
            setup_id=r["setup_id"], candle_id=r["candle_id"], open_time=from_db(r["open_time"]), cleared=bool(r["cleared"]),
            policy_version=r["policy_version"], mtf_state=r["mtf_state"], blockers=json.loads(r["blockers_json"]),
            badges=json.loads(r["badges_json"]), fakeout_flags=json.loads(r["fakeout_flags_json"]),
            evidence=json.loads(r["evidence_json"]), id=r["id"],
        )


# ---------------------------------------------------------- symbol / session
class SymbolStateRepository:
    def __init__(self, db: Database):
        self.db = db

    def upsert(self, symbol: str, security_id: str, expiry_date: str, present_trend: str, mtf: dict[str, Any],
               structure: dict[str, Any], last_open_time: datetime) -> None:
        with self.db.transaction() as conn:
            conn.execute(
                """INSERT INTO symbol_state(symbol, security_id, expiry_date, present_trend, mtf_json, structure_json, last_open_time, updated_at)
                   VALUES (?,?,?,?,?,?,?,?)
                   ON CONFLICT(symbol) DO UPDATE SET security_id = excluded.security_id, expiry_date = excluded.expiry_date,
                       present_trend = excluded.present_trend, mtf_json = excluded.mtf_json, structure_json = excluded.structure_json,
                       last_open_time = excluded.last_open_time, updated_at = excluded.updated_at""",
                (symbol, security_id, expiry_date, present_trend, json.dumps(mtf, sort_keys=True, default=str),
                 json.dumps(structure, sort_keys=True, default=str), to_db(last_open_time), to_db(utc_now())),
            )

    def get(self, symbol: str) -> sqlite3.Row | None:
        return self.db.query_one("SELECT * FROM symbol_state WHERE symbol = ?", (symbol,))


class SessionStateRepository:
    def __init__(self, db: Database):
        self.db = db

    def upsert(self, rec: dict[str, Any]) -> None:
        with self.db.transaction() as conn:
            conn.execute(
                """INSERT INTO session_state(symbol, security_id, session_date, session_name, session_group, trend, is_current, open, high,
                       low, close, bars, opened_at, closed_at, last_open_time, evidence_json, updated_at)
                   VALUES (:symbol, :security_id, :session_date, :session_name, :session_group, :trend, :is_current, :open, :high,
                       :low, :close, :bars, :opened_at, :closed_at, :last_open_time, :evidence_json, :updated_at)
                   ON CONFLICT(symbol, session_date, session_name) DO UPDATE SET trend = excluded.trend, is_current = excluded.is_current,
                       open = excluded.open, high = excluded.high, low = excluded.low, close = excluded.close, bars = excluded.bars,
                       closed_at = excluded.closed_at, last_open_time = excluded.last_open_time, evidence_json = excluded.evidence_json,
                       updated_at = excluded.updated_at""",
                {**rec, "updated_at": to_db(utc_now())},
            )

    def clear_current(self, symbol: str, except_key: tuple[str, str] | None = None) -> None:
        with self.db.transaction() as conn:
            if except_key is None:
                conn.execute("UPDATE session_state SET is_current = 0 WHERE symbol = ?", (symbol,))
            else:
                conn.execute(
                    "UPDATE session_state SET is_current = 0 WHERE symbol = ? AND NOT (session_date = ? AND session_name = ?)",
                    (symbol, except_key[0], except_key[1]),
                )

    def for_symbol(self, symbol: str, session_date: str) -> list[sqlite3.Row]:
        return self.db.query(
            "SELECT * FROM session_state WHERE symbol = ? AND session_date = ? ORDER BY session_group, opened_at", (symbol, session_date)
        )

    def current(self, symbol: str) -> list[sqlite3.Row]:
        return self.db.query("SELECT * FROM session_state WHERE symbol = ? AND is_current = 1", (symbol,))

    def latest_closed(self, symbol: str, session_name: str) -> sqlite3.Row | None:
        return self.db.query_one(
            "SELECT * FROM session_state WHERE symbol = ? AND session_name = ? AND closed_at IS NOT NULL ORDER BY session_date DESC LIMIT 1",
            (symbol, session_name),
        )


# ------------------------------------------------------------------- outcomes
class SnapshotRepository:
    def __init__(self, db: Database):
        self.db = db

    def insert(self, s: FeatureSnapshot) -> FeatureSnapshot:
        _require_parent(s.detection_id, "detection")
        _require_parent(s.candle_id, "candle")
        with self.db.transaction() as conn:
            cur = conn.execute(
                """INSERT INTO feature_snapshots(detection_id, candle_id, symbol, security_id, expiry_date, timeframe, open_time, direction,
                       setup_family, reference_price, atr, rsi, ema_fast, ema_slow, present_trend, mtf_state, feature_schema_version,
                       features_json, created_at) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (s.detection_id, s.candle_id, s.symbol, s.security_id, s.expiry_date, s.timeframe.value, to_db(s.open_time),
                 s.direction, s.setup_family, s.reference_price, s.atr, s.rsi, s.ema_fast, s.ema_slow, s.present_trend,
                 s.mtf_state, s.feature_schema_version, s.features_json, to_db(utc_now())),
            )
            new_id = int(cur.lastrowid)
        return FeatureSnapshot(**{**s.__dict__, "id": new_id})

    def update(self, *_args, **_kwargs):  # application guard
        raise StorageError("feature_snapshots are immutable; updates are forbidden")

    def get(self, snapshot_id: int) -> FeatureSnapshot | None:
        r = self.db.query_one("SELECT * FROM feature_snapshots WHERE id = ?", (snapshot_id,))
        return self._to(r) if r else None

    def for_detection(self, detection_id: int) -> FeatureSnapshot | None:
        r = self.db.query_one("SELECT * FROM feature_snapshots WHERE detection_id = ?", (detection_id,))
        return self._to(r) if r else None

    def pending_outcomes(self, limit: int = 500) -> list[FeatureSnapshot]:
        """Snapshots that still have at least one non-final horizon (or none yet)."""
        rows = self.db.query(
            """SELECT s.* FROM feature_snapshots s
               WHERE NOT EXISTS (
                   SELECT 1 FROM outcome_observations o WHERE o.snapshot_id = s.id AND o.horizon = '__all_final__'
               ) ORDER BY s.id LIMIT ?""",
            (limit,),
        )
        return [self._to(r) for r in rows]

    def cohort(self, symbol: str, setup_family: str, direction: str, timeframe: Timeframe) -> list[FeatureSnapshot]:
        rows = self.db.query(
            "SELECT * FROM feature_snapshots WHERE symbol = ? AND setup_family = ? AND direction = ? AND timeframe = ? ORDER BY id",
            (symbol, setup_family, direction, timeframe.value),
        )
        return [self._to(r) for r in rows]

    @staticmethod
    def _to(r: sqlite3.Row) -> FeatureSnapshot:
        return FeatureSnapshot(
            detection_id=r["detection_id"], candle_id=r["candle_id"], symbol=r["symbol"], security_id=r["security_id"],
            expiry_date=r["expiry_date"], timeframe=Timeframe(r["timeframe"]), open_time=from_db(r["open_time"]),
            direction=r["direction"], setup_family=r["setup_family"], reference_price=r["reference_price"], atr=r["atr"],
            rsi=r["rsi"], ema_fast=r["ema_fast"], ema_slow=r["ema_slow"], present_trend=r["present_trend"], mtf_state=r["mtf_state"],
            feature_schema_version=r["feature_schema_version"], features=json.loads(r["features_json"]), id=r["id"],
        )


class OutcomeRepository:
    def __init__(self, db: Database):
        self.db = db

    def upsert(self, o: OutcomeObservation) -> OutcomeObservation:
        _require_parent(o.snapshot_id, "feature snapshot")
        _require_parent(o.detection_id, "detection")
        with self.db.transaction() as conn:
            existing = conn.execute(
                "SELECT id, is_final FROM outcome_observations WHERE snapshot_id = ? AND horizon = ?", (o.snapshot_id, o.horizon)
            ).fetchone()
            if existing is not None and existing["is_final"]:
                o.id = int(existing["id"])
                return o  # final observations are frozen
            conn.execute(
                """INSERT INTO outcome_observations(snapshot_id, detection_id, horizon, horizon_bars, bars_observed, mfe, mae, mfe_atr,
                       mae_atr, time_to_mfe_bars, time_to_mae_bars, bars_to_invalidation, bars_to_follow_through, final_move, label,
                       label_version, last_candle_open_time, is_final, computed_at)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                   ON CONFLICT(snapshot_id, horizon) DO UPDATE SET bars_observed = excluded.bars_observed, mfe = excluded.mfe,
                       mae = excluded.mae, mfe_atr = excluded.mfe_atr, mae_atr = excluded.mae_atr, time_to_mfe_bars = excluded.time_to_mfe_bars,
                       time_to_mae_bars = excluded.time_to_mae_bars, bars_to_invalidation = excluded.bars_to_invalidation,
                       bars_to_follow_through = excluded.bars_to_follow_through, final_move = excluded.final_move, label = excluded.label,
                       label_version = excluded.label_version, last_candle_open_time = excluded.last_candle_open_time,
                       is_final = excluded.is_final, computed_at = excluded.computed_at""",
                (o.snapshot_id, o.detection_id, o.horizon, o.horizon_bars, o.bars_observed, o.mfe, o.mae, o.mfe_atr, o.mae_atr,
                 o.time_to_mfe_bars, o.time_to_mae_bars, o.bars_to_invalidation, o.bars_to_follow_through, o.final_move, o.label,
                 o.label_version, to_db(o.last_candle_open_time), int(o.is_final), to_db(utc_now())),
            )
            row = conn.execute("SELECT id FROM outcome_observations WHERE snapshot_id = ? AND horizon = ?", (o.snapshot_id, o.horizon)).fetchone()
        o.id = int(row["id"])
        return o

    def mark_all_final(self, snapshot_id: int, detection_id: int, last_open_time: datetime) -> None:
        with self.db.transaction() as conn:
            conn.execute(
                """INSERT OR IGNORE INTO outcome_observations(snapshot_id, detection_id, horizon, horizon_bars, bars_observed, mfe, mae,
                       final_move, label, label_version, last_candle_open_time, is_final, computed_at)
                   VALUES (?,?,'__all_final__',0,0,0,0,0,'marker','marker',?,1,?)""",
                (snapshot_id, detection_id, to_db(last_open_time), to_db(utc_now())),
            )

    def for_snapshot(self, snapshot_id: int) -> list[OutcomeObservation]:
        rows = self.db.query(
            "SELECT * FROM outcome_observations WHERE snapshot_id = ? AND horizon != '__all_final__' ORDER BY horizon_bars", (snapshot_id,)
        )
        return [self._to(r) for r in rows]

    def cohort(self, snapshot_ids: list[int], horizon: str) -> list[OutcomeObservation]:
        if not snapshot_ids:
            return []
        q = ",".join("?" for _ in snapshot_ids)
        rows = self.db.query(
            f"SELECT * FROM outcome_observations WHERE horizon = ? AND is_final = 1 AND snapshot_id IN ({q})",
            (horizon, *snapshot_ids),
        )
        return [self._to(r) for r in rows]

    @staticmethod
    def _to(r: sqlite3.Row) -> OutcomeObservation:
        return OutcomeObservation(
            snapshot_id=r["snapshot_id"], detection_id=r["detection_id"], horizon=r["horizon"], horizon_bars=r["horizon_bars"],
            bars_observed=r["bars_observed"], mfe=r["mfe"], mae=r["mae"], mfe_atr=r["mfe_atr"], mae_atr=r["mae_atr"],
            time_to_mfe_bars=r["time_to_mfe_bars"], time_to_mae_bars=r["time_to_mae_bars"],
            bars_to_invalidation=r["bars_to_invalidation"], bars_to_follow_through=r["bars_to_follow_through"],
            final_move=r["final_move"], label=r["label"], label_version=r["label_version"],
            last_candle_open_time=from_db(r["last_candle_open_time"]), is_final=bool(r["is_final"]), id=r["id"],
        )


# ------------------------------------------------------------ discord refs
class MessageRefRepository:
    def __init__(self, db: Database):
        self.db = db

    def upsert(self, setup_id: int, channel_id: str, message_id: str, state_hash: str) -> None:
        _require_parent(setup_id, "setup")
        now = to_db(utc_now())
        with self.db.transaction() as conn:
            conn.execute(
                """INSERT INTO discord_message_refs(setup_id, channel_id, message_id, last_rendered_state_hash, last_rendered_at, created_at)
                   VALUES (?,?,?,?,?,?)
                   ON CONFLICT(setup_id) DO UPDATE SET channel_id = excluded.channel_id, message_id = excluded.message_id,
                       last_rendered_state_hash = excluded.last_rendered_state_hash, last_rendered_at = excluded.last_rendered_at""",
                (setup_id, str(channel_id), str(message_id), state_hash, now, now),
            )

    def get(self, setup_id: int) -> sqlite3.Row | None:
        return self.db.query_one("SELECT * FROM discord_message_refs WHERE setup_id = ?", (setup_id,))

    def all(self) -> list[sqlite3.Row]:
        return self.db.query("SELECT * FROM discord_message_refs ORDER BY setup_id")


# ----------------------------------------------------------------- aggregate
class Repositories:
    """Convenience bundle of all repositories over one Database."""

    def __init__(self, db: Database):
        self.db = db
        self.instruments = InstrumentRepository(db)
        self.candles = CandleRepository(db)
        self.historical_cache = HistoricalCacheRepository(db)
        self.indicators = IndicatorRepository(db)
        self.pivots = PivotRepository(db)
        self.day_frames = DayFrameRepository(db)
        self.detections = DetectionRepository(db)
        self.setups = SetupRepository(db)
        self.clearances = ClearanceRepository(db)
        self.symbol_state = SymbolStateRepository(db)
        self.session_state = SessionStateRepository(db)
        self.snapshots = SnapshotRepository(db)
        self.outcomes = OutcomeRepository(db)
        self.message_refs = MessageRefRepository(db)
