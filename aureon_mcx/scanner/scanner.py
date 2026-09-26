"""InstrumentScanner: incremental winners / losers / breadth over the enabled universe.

Every feed packet updates one in-memory InstrumentQuote (O(1)); rankings and breadth are
computed over the universe on demand (a few hundred instruments) and cached per second, so
the status API never scans historical rows. The previous close is a VERIFIED reference only
when the broker delivered it explicitly; otherwise `change_pct` is null and the instrument is
`reference_status: MISSING`. Stale instruments (no packet within `stale_after_seconds`) are
never ranked as today's winners / losers, and nothing is ranked while the market is closed.
"""
from __future__ import annotations

import logging
import statistics
from datetime import datetime, timedelta
from typing import Any, Callable

from aureon_mcx.broker.dhan.instruments import InstrumentMaster, InstrumentRecord
from aureon_mcx.broker.dhan.live_feed import CODE_FULL, CODE_PREV_CLOSE, CODE_QUOTE, CODE_TICKER, EXCHANGE_SEGMENT_CODES, FeedPacket
from aureon_mcx.config.yaml_models import ScannerConfig
from aureon_mcx.events import EventType, Severity, SystemEventBus
from aureon_mcx.market.timeutil import from_epoch, utc_now

from .models import REFERENCE_MISSING, REFERENCE_VERIFIED, InstrumentQuote, Ranked
from .universe import build_universe, partition

log = logging.getLogger("aureon.scanner")

SEGMENT_NAME_FOR_CODE = {v: k for k, v in EXCHANGE_SEGMENT_CODES.items()}


class InstrumentScanner:
    def __init__(self, cfg: ScannerConfig, now: Callable[[], datetime] = utc_now, events: SystemEventBus | None = None,
                 repos=None, metrics=None, is_market_open: Callable[[datetime], bool] | None = None):
        self.cfg = cfg
        self._now = now
        self.events = events
        self.repos = repos
        self.metrics = metrics
        self.is_market_open = is_market_open or (lambda ts: True)
        self.quotes: dict[str, InstrumentQuote] = {}
        self.universe: dict[str, list[InstrumentRecord]] = {}
        self.partitions: list[list[str]] = []
        self.built_at: datetime | None = None
        self.last_update: datetime | None = None
        self.updates = 0
        self.last_snapshot_at: datetime | None = None
        self.last_digest_at: datetime | None = None
        self._leaders: tuple[str | None, str | None] = (None, None)
        self._cache: tuple[datetime, dict[str, Any]] | None = None

    # ------------------------------------------------------------ universe
    def build(self, master: InstrumentMaster, today) -> int:
        self.universe = build_universe(master, self.cfg, today)
        ids: list[str] = []
        for segment, rows in self.universe.items():
            for rec in rows:
                q = self.quotes.get(rec.security_id)
                if q is None:
                    self.quotes[rec.security_id] = InstrumentQuote(
                        security_id=rec.security_id, symbol=rec.base_name, display_symbol=rec.display_name or rec.trading_symbol or rec.base_name,
                        segment=segment, instrument_type=rec.instrument_type, expiry=rec.expiry_date.isoformat() if rec.expiry_date else None,
                        category=self.cfg.category_map.get(rec.base_name))
                ids.append(rec.security_id)
        wanted = set(ids)
        for sid in [s for s in self.quotes if s not in wanted]:
            self.quotes.pop(sid, None)
        self.partitions = partition(ids, self.cfg.max_subscriptions_per_connection)
        self.built_at = self._now()
        self._cache = None
        return len(ids)

    @property
    def universe_size(self) -> int:
        return len(self.quotes)

    def security_ids(self) -> list[str]:
        return list(self.quotes)

    # ------------------------------------------------------------- packets
    def on_packet(self, p: FeedPacket) -> None:
        q = self.quotes.get(p.security_id)
        if q is None:
            return
        now = self._now()
        q.packets += 1
        q.last_update = now
        if p.code == CODE_PREV_CLOSE:
            if p.prev_close is not None and p.prev_close > 0:
                q.previous_close = float(p.prev_close)
                q.reference_status = REFERENCE_VERIFIED
            if p.prev_open_interest is not None:
                q.extra["prev_open_interest"] = float(p.prev_open_interest)
        elif p.code in (CODE_TICKER, CODE_QUOTE, CODE_FULL):
            if p.ltp is not None and p.ltp > 0:
                q.ltp = float(p.ltp)
            if p.ltt is not None:
                q.last_trade_at = from_epoch(p.ltt)
            if p.volume is not None:
                q.volume = float(p.volume)
            if p.day_open:
                q.day_open = float(p.day_open)
            if p.day_high:
                q.day_high = float(p.day_high)
            if p.day_low:
                q.day_low = float(p.day_low)
            if p.day_close is not None:
                q.day_close_field = float(p.day_close)
                if self.cfg.previous_close_from_quote_day_close and q.reference_status != REFERENCE_VERIFIED and p.day_close > 0:
                    q.previous_close = float(p.day_close)
                    q.reference_status = REFERENCE_VERIFIED
            if p.open_interest is not None:
                q.open_interest = float(p.open_interest)
        else:
            if p.open_interest is not None:
                q.open_interest = float(p.open_interest)
        self.last_update = now
        self.updates += 1
        self._cache = None

    # ------------------------------------------------------------ rankings
    @property
    def stale_threshold(self) -> timedelta:
        return timedelta(seconds=self.cfg.stale_after_seconds)

    def _eligible(self, now: datetime) -> list[InstrumentQuote]:
        thr = self.stale_threshold
        return [q for q in self.quotes.values() if q.change_pct is not None and not q.is_stale(now, thr)]

    def rankings(self, now: datetime | None = None) -> tuple[list[Ranked], list[Ranked]]:
        now = now or self._now()
        if not self.is_market_open(now):
            return [], []  # stale prices are never presented as today's live winners / losers
        elig = self._eligible(now)
        winners = sorted(elig, key=lambda q: (-(q.change_pct or 0.0), q.symbol))
        losers = sorted(elig, key=lambda q: ((q.change_pct or 0.0), q.symbol))
        w = [Ranked(i + 1, q) for i, q in enumerate(winners[: self.cfg.winners_limit])]
        l = [Ranked(i + 1, q) for i, q in enumerate(losers[: self.cfg.losers_limit])]
        return w, l

    def breadth(self, now: datetime | None = None) -> dict[str, Any]:
        now = now or self._now()
        thr = self.stale_threshold

        def bucket(quotes: list[InstrumentQuote]) -> dict[str, Any]:
            adv = dec = unch = stale = unavailable = 0
            moves: list[float] = []
            for q in quotes:
                if q.ltp is None or q.change_pct is None:
                    unavailable += 1
                    continue
                if q.is_stale(now, thr):
                    stale += 1
                    continue
                moves.append(q.change_pct)
                if q.change_pct > 0:
                    adv += 1
                elif q.change_pct < 0:
                    dec += 1
                else:
                    unch += 1
            return {"total": len(quotes), "advancing": adv, "declining": dec, "unchanged": unch, "stale": stale, "unavailable": unavailable,
                    "average_pct_move": round(statistics.fmean(moves), 4) if moves else None,
                    "median_pct_move": round(statistics.median(moves), 4) if moves else None}

        all_q = list(self.quotes.values())
        by = lambda key: {k: bucket([q for q in all_q if getattr(q, key) == k]) for k in sorted({getattr(q, key) for q in all_q if getattr(q, key)})}
        return {"overall": bucket(all_q), "by_segment": by("segment"), "by_category": by("category"), "by_instrument_type": by("instrument_type"),
                "market_open": self.is_market_open(now), "as_of": now.isoformat()}

    def leaderboard(self, now: datetime | None = None) -> dict[str, Any]:
        """Cached per-second projection for the API / Discord."""
        now = now or self._now()
        if self._cache is not None and (now - self._cache[0]) < timedelta(seconds=1):
            return self._cache[1]
        winners, losers = self.rankings(now)
        thr = self.stale_threshold
        data = {"universe": self.universe_size, "as_of": now.isoformat(), "market_open": self.is_market_open(now),
                "winners": [r.to_dict(now, thr) for r in winners], "losers": [r.to_dict(now, thr) for r in losers], "breadth": self.breadth(now)}
        self._cache = (now, data)
        return data

    def status(self, now: datetime | None = None) -> dict[str, Any]:
        now = now or self._now()
        b = self.breadth(now)["overall"]
        return {"enabled": self.cfg.enabled, "universe": self.universe_size, "segments": {k: len(v) for k, v in self.universe.items()},
                "partitions": [len(p) for p in self.partitions], "updates": self.updates,
                "last_update": self.last_update.isoformat() if self.last_update else None,
                "built_at": self.built_at.isoformat() if self.built_at else None, "advancers": b["advancing"], "decliners": b["declining"],
                "unchanged": b["unchanged"], "stale": b["stale"], "unavailable": b["unavailable"],
                "verified_references": sum(1 for q in self.quotes.values() if q.reference_status == REFERENCE_VERIFIED)}

    def quote_dicts(self, now: datetime | None = None) -> list[dict[str, Any]]:
        now = now or self._now()
        return [q.to_dict(now, self.stale_threshold) for q in sorted(self.quotes.values(), key=lambda q: (q.segment, q.symbol))]

    # ------------------------------------------------------- periodic work
    def tick(self, now: datetime | None = None) -> dict[str, Any]:
        """Periodic housekeeping: rank snapshots, digest cadence, leader-change events. Returns what happened."""
        now = now or self._now()
        result = {"snapshot": 0, "digest": False, "leader_changed": False}
        if not self.quotes:
            return result
        winners, losers = self.rankings(now)
        leaders = (winners[0].quote.security_id if winners else None, losers[0].quote.security_id if losers else None)
        if self.cfg.snapshot_interval_seconds and (self.last_snapshot_at is None or now - self.last_snapshot_at >= timedelta(seconds=self.cfg.snapshot_interval_seconds)):
            result["snapshot"] = self.write_snapshot(now)
        if leaders != self._leaders and any(leaders) and self._leaders != (None, None):
            result["leader_changed"] = True
            if self.events is not None:
                w = winners[0].quote if winners else None
                l = losers[0].quote if losers else None
                self.events.emit(EventType.SCANNER_LEADER_CHANGED,
                                 "leaderboard change: " + " · ".join(x for x in [
                                     f"top gainer {w.display_symbol} {w.change_pct:+.2f}%" if w else "", f"top loser {l.display_symbol} {l.change_pct:+.2f}%" if l else ""] if x),
                                 dedupe_key=f"scanner:leaders:{leaders[0]}:{leaders[1]}", agent="scanner_agent",
                                 winner=w.security_id if w else None, loser=l.security_id if l else None)
        self._leaders = leaders
        if self.last_digest_at is None or now - self.last_digest_at >= timedelta(seconds=self.cfg.digest_interval_seconds):
            if self.is_market_open(now) and (winners or losers):
                self.last_digest_at = now
                result["digest"] = True
                if self.events is not None:
                    b = self.breadth(now)["overall"]
                    self.events.emit(EventType.SCANNER_UPDATED, f"leaderboard digest: advancers {b['advancing']} | decliners {b['declining']} | "
                                     f"unchanged {b['unchanged']} (universe {self.universe_size})", agent="scanner_agent",
                                     dedupe_key=f"scanner:digest:{now.isoformat()}", digest=self.digest(now))
        if self.metrics is not None:
            self.metrics.inc("scanner_updates")
        return result

    def write_snapshot(self, now: datetime) -> int:
        if self.repos is None:
            return 0
        winners, losers = self.rankings(now)
        rows: list[dict[str, Any]] = []
        seen: set[str] = set()
        for ranked in list(winners) + [Ranked(-r.rank, r.quote) for r in losers]:
            q = ranked.quote
            if q.security_id in seen:
                continue
            seen.add(q.security_id)
            rows.append({"segment": q.segment, "category": q.category, "security_id": q.security_id, "symbol": q.symbol,
                         "display_symbol": q.display_symbol, "rank": ranked.rank, "ltp": q.ltp, "previous_close": q.previous_close,
                         "change": q.change, "change_pct": q.change_pct, "volume": q.volume, "open_interest": q.open_interest})
        if not rows:
            return 0  # nothing rankable (closed market / no verified references): try again next tick
        n = self.repos.rank_snapshots.insert_many(now, rows)
        self.last_snapshot_at = now
        return n

    def digest(self, now: datetime | None = None, top: int = 10) -> dict[str, Any]:
        now = now or self._now()
        winners, losers = self.rankings(now)
        b = self.breadth(now)["overall"]
        fmt = lambda r: {"rank": r.rank, "symbol": r.quote.display_symbol, "ltp": r.quote.ltp, "change_pct": round(r.quote.change_pct, 2)}
        return {"as_of": now.isoformat(), "winners": [fmt(r) for r in winners[:top]], "losers": [fmt(r) for r in losers[:top]],
                "advancers": b["advancing"], "decliners": b["declining"], "unchanged": b["unchanged"], "stale": b["stale"],
                "unavailable": b["unavailable"], "universe": self.universe_size}
