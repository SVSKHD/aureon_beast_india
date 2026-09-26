"""Application: the ordered startup flow, the supervised live run loop, recovery and rollover.

Startup (any failure in steps 1-9 aborts with a clear error):
 1 load config            2 validate Dhan credentials (never printed)
 3 instrument master      4/5 resolve GOLD / SILVER
 6 publish instrument metadata (log + instruments table + Discord status)
 7 SQLite WAL + migrations 8 historical M5 / M15 / H1 (H4 aggregated locally)
 9 warm indicators       10 connect WebSocket   11 subscribe   12 observer
13 Discord               14 service health

Runtime guarantees
  * every feed (re)connect triggers M1 gap recovery through the same pipeline before a
    symbol is LIVE; an unrecoverable gap suspends that symbol's analytics (ERROR);
  * contract rollover is staged: the new contract is fully warmed and seeded before the
    atomic switch; a failed warmup keeps the old contract running;
  * background tasks are supervised: feed / flush / housekeeping are restarted with
    backoff (repeated failure shuts the service down safely), Discord failure degrades
    health to discord=ERROR and retries while observation continues.
"""
from __future__ import annotations

import asyncio
import logging
import signal
import sys
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any, Awaitable, Callable

from aureon_mcx.broker.dhan import DhanApiError, DhanError, DhanInstrumentProvider, ResolvedContract, SymbolResolutionError, SymbolResolver
from aureon_mcx.broker.dhan.client import DhanHttpClient
from aureon_mcx.broker.dhan.errors import DhanCredentialsError
from aureon_mcx.broker.dhan.historical import CachedHistoricalProvider, DhanHistoricalProvider
from aureon_mcx.broker.dhan.live_feed import DhanLiveFeedProvider
from aureon_mcx.config import AppConfig, ConfigError, load_config
from aureon_mcx.agents import AgentRegistry
from aureon_mcx.continuity import KIND_GAP, KIND_PENDING_MINUTE, ContinuityService
from aureon_mcx.crash import CrashReporter
from aureon_mcx.events import EventType, Severity, SystemEvent, SystemEventBus
from aureon_mcx.health import HealthState
from aureon_mcx.logging_setup import configure_logging, kv
from aureon_mcx.market.candle import Candle
from aureon_mcx.market.candle_builder import REASON_SILENT, REASON_SUSPECT, CandlePipeline, GapRecord, Tick
from aureon_mcx.metrics import Metrics
from aureon_mcx.market.sessions import CalendarOutOfRange, SessionCalendar
from aureon_mcx.market.timeframe import Timeframe
from aureon_mcx.market.timeutil import IST, utc_now
from aureon_mcx.market.warmup import derive_h4, seed_pipeline, warmup_window
from aureon_mcx.observer import NullSink, PresentationSink, SymbolObserver
from aureon_mcx.outcomes import OutcomeService
from aureon_mcx.scanner import InstrumentScanner
from aureon_mcx.storage import Database, Repositories
from aureon_mcx.storage.parquet_archive import ParquetArchive
from aureon_mcx.version import short_sha, version_info

log = logging.getLogger("aureon.app")


class StartupError(RuntimeError):
    pass


@dataclass
class SymbolRuntime:
    """Everything that belongs to one active contract; swapped atomically on rollover."""

    contract: ResolvedContract
    observer: SymbolObserver
    history: dict[Timeframe, list[Candle]]
    pipeline: CandlePipeline | None = None


@dataclass
class TaskSpec:
    name: str
    factory: Callable[[], Awaitable[None]]
    critical: bool
    max_restarts: int = 5
    restarts: int = 0
    backoff: tuple[float, ...] = (1.0, 2.0, 5.0, 10.0, 30.0)
    agent: str = ""                     # agent id reported in crash reports / registry
    last_crash_id: str | None = None    # resolved once the restarted task is healthy again
    started_at: datetime | None = None


TASK_AGENTS = {"feed": "market_feed_agent", "flush": "aggregation_agent", "housekeeping": "health_agent", "discord": "discord_agent",
               "api": "api_agent", "scanner": "scanner_agent"}


class Application:
    def __init__(self, config_dir: str | None = None, env_file: str | None = None, *, http_factory: Callable[..., Any] | None = None,
                 instrument_provider_factory: Callable[..., Any] | None = None, historical_factory: Callable[..., Any] | None = None,
                 feed_factory: Callable[..., Any] | None = None, sink_factory: Callable[..., Any] | None = None,
                 now: Callable[[], datetime] = utc_now, validate_credentials_remotely: bool = True,
                 housekeeping_interval: float = 30.0, flush_interval: float = 1.0, feed_stall_seconds: float | None = None,
                 scanner_feed_factory: Callable[..., Any] | None = None, scanner_interval: float = 5.0):
        self._config_dir = config_dir
        self._env_file = env_file
        self._http_factory = http_factory or (lambda cfg: DhanHttpClient(cfg.env.DHAN_CLIENT_ID.get_secret_value(), cfg.env.DHAN_ACCESS_TOKEN.get_secret_value()))
        self._instrument_provider_factory = instrument_provider_factory or (lambda cfg, http: DhanInstrumentProvider(
            cfg.symbols.instrument_master.url, cfg.symbols.instrument_master.cache_path, cfg.symbols.instrument_master.refresh_hours, http))
        self._historical_factory = historical_factory or (lambda cfg, http: DhanHistoricalProvider(
            http, cfg.analysis.historical.max_days_per_request, calendar=self.calendar))
        self._feed_factory = feed_factory
        self._scanner_feed_factory = scanner_feed_factory  # tests inject fake scanner feeds
        self._sink_factory = sink_factory
        self._now = now
        self._validate_remote = validate_credentials_remotely
        self.housekeeping_interval = housekeeping_interval
        self.flush_interval = flush_interval
        self.feed_stall_seconds = feed_stall_seconds  # None -> analysis.historical.feed_stall_seconds
        self.task_backoff_scale = 1.0  # tests shrink restart backoffs
        self.cfg: AppConfig | None = None
        self.http = None
        self.resolver: SymbolResolver | None = None
        self.db: Database | None = None
        self.repos: Repositories | None = None
        self.calendar: SessionCalendar | None = None
        self.health = HealthState()
        self.sink: PresentationSink = NullSink()
        self.runtimes: dict[str, SymbolRuntime] = {}
        self.pending_runtimes: dict[str, SymbolRuntime] = {}  # security_id -> runtime prepared for rollover
        self.historical = None
        self.continuity: ContinuityService | None = None
        self.feed = None
        self.scanner: InstrumentScanner | None = None
        self.scanner_feeds: list = []
        self.scanner_interval = scanner_interval
        self.discord_client = None
        self.stop: asyncio.Event | None = None
        self.startup_log: list[str] = []
        self._tasks: dict[str, asyncio.Task] = {}
        self._specs: dict[str, TaskSpec] = {}
        self._loop: asyncio.AbstractEventLoop | None = None
        self.task_failures: list[tuple[str, str]] = []
        self._verify_inflight: dict[str, bool] = {}
        self._inflight: dict[tuple[str, str], bool] = {}  # (symbol, operation) -> running
        self._bg_tasks: set[asyncio.Task] = set()  # strong references: asyncio keeps only weak refs to tasks
        self.events = SystemEventBus(now=self._now)
        self.metrics = Metrics()
        self.agents = AgentRegistry(self.events, now=self._now)
        self.crashes = CrashReporter(None, self.events, now=self._now, started_at=self._now())
        self.started_at = self._now()
        self._market_state = None
        self.db_stats: dict[str, Any] = {}
        self._tick_times: deque[datetime] = deque(maxlen=5000)
        self.api = None
        from aureon_mcx.discord.ops import OpsChannel

        self.ops = OpsChannel(now=self._now, metrics=self.metrics)
        self.events.subscribe(self.ops.on_event)
        self._last_packet_at: datetime | None = None
        self._feed_stalled = False
        self._feed_generation = 0
        self.health.on_symbol_state = self._on_symbol_state_changed

    def _spawn(self, coro, name: str) -> asyncio.Task | None:
        if self._loop is None:
            coro.close()
            return None
        task = self._loop.create_task(coro, name=name)
        self._bg_tasks.add(task)
        task.add_done_callback(self._bg_tasks.discard)
        task.add_done_callback(self._bg_task_done)
        return task

    def _report_operation_error(self, sym: str, op: str, exc: BaseException) -> None:
        """An unexpected exception inside a continuity operation is a crash: reported durably,
        isolated to that operation (the incident schedule retries it), never swallowed."""
        rt = self.runtimes.get(sym)
        rep = self.crashes.report("continuity", exc, agent="continuity_agent", task=f"{op}:{sym}", symbol=sym,
                                  security_id=rt.contract.security_id if rt else None)
        self.metrics.inc("crashes")
        self.agents.error("continuity_agent", f"{op}:{sym}: {type(exc).__name__}: {exc}", state="DEGRADED", crash_id=rep.crash_id)

    def _bg_task_done(self, task: asyncio.Task) -> None:
        """A background operation (verify / repair / recover / reconcile / rollover) that raised is a
        crash: reported durably, isolated to that operation, retried by the scheduler."""
        if task.cancelled():
            return
        exc = task.exception()
        if exc is None:
            return
        name = task.get_name()
        sym = name.split(":", 1)[1] if ":" in name else None
        rt = self.runtimes.get(sym) if sym else None
        rep = self.crashes.report("continuity", exc, agent="continuity_agent", task=name, symbol=sym,
                                  security_id=rt.contract.security_id if rt else None)
        self.task_failures.append((name, type(exc).__name__))
        self.metrics.inc("crashes")
        self.agents.error("continuity_agent", f"{name}: {type(exc).__name__}: {exc}", state="DEGRADED", crash_id=rep.crash_id)
        for key in [k for k, v in self._inflight.items() if v and k[0] == sym]:
            self._inflight[key] = False

    # ---------------------------------------------------------- compat views
    @property
    def contracts(self) -> dict[str, ResolvedContract]:
        return {s: r.contract for s, r in self.runtimes.items()}

    @property
    def observers(self) -> dict[str, SymbolObserver]:
        return {s: r.observer for s, r in self.runtimes.items()}

    @property
    def history(self) -> dict[str, dict[Timeframe, list[Candle]]]:
        return {s: r.history for s, r in self.runtimes.items()}

    @property
    def pipelines(self) -> dict[str, CandlePipeline]:
        return {r.contract.security_id: r.pipeline for r in self.runtimes.values() if r.pipeline is not None}

    def _runtime_for_security(self, security_id: str) -> SymbolRuntime | None:
        rt = next((r for r in self.runtimes.values() if r.contract.security_id == security_id), None)
        if rt is None:
            rt = self.pending_runtimes.get(security_id)  # new contract being rolled in: never drop its ticks
        return rt

    # ------------------------------------------------------------------ steps
    def _step(self, n: int, text: str) -> None:
        self.startup_log.append(f"{n}:{text}")
        log.info("startup_step %s", kv(step=n, what=text))

    def startup(self) -> None:
        try:
            self._startup()
        except (ConfigError, DhanError, StartupError) as exc:
            log.error("startup_aborted %s", kv(error=type(exc).__name__, detail=str(exc)))
            raise StartupError(str(exc)) from exc

    def _startup(self) -> None:
        # 1 config
        self.cfg = cfg = load_config(self._config_dir, self._env_file)
        configure_logging(cfg.env.AUREON_LOG_LEVEL, secrets=[
            cfg.env.DHAN_ACCESS_TOKEN.get_secret_value() if cfg.env.DHAN_ACCESS_TOKEN else None,
            cfg.env.DHAN_CLIENT_ID.get_secret_value() if cfg.env.DHAN_CLIENT_ID else None,
            cfg.env.DISCORD_TOKEN.get_secret_value() if cfg.env.DISCORD_TOKEN else None])
        self._step(1, "config loaded")
        self.calendar = SessionCalendar(cfg.sessions)
        # never assume normal trading for a year nobody configured (fail closed)
        try:
            today = self.calendar.require_coverage(self._now())
        except CalendarOutOfRange as exc:
            self.health.set("calendar", "error", str(exc))
            raise StartupError(f"CALENDAR_OUT_OF_RANGE: {exc}") from exc
        self.health.set("calendar", "ok", f"{today.isoformat()} covered; years={self.calendar.years}; "
                        f"verified={'yes' if self.calendar.verified else 'NO'}")
        # 2 credentials (never printed)
        if not cfg.env.has_dhan_credentials:
            raise DhanCredentialsError("DHAN_CLIENT_ID / DHAN_ACCESS_TOKEN are not set")
        self.http = self._http_factory(cfg)
        self.http.require_credentials()
        if self._validate_remote:
            try:
                profile = self.http.get_json("/profile", context={"endpoint": "profile"})
            except DhanApiError as exc:
                raise DhanCredentialsError(f"Dhan rejected credentials (status={exc.status})") from exc
            status = (profile or {}).get("tokenValidity") if isinstance(profile, dict) else None
            log.info("dhan_credentials_valid %s", kv(token_validity=status))
        self._step(2, "dhan credentials validated")
        # 3 instrument master
        provider = self._instrument_provider_factory(cfg, self.http)
        self.resolver = SymbolResolver(cfg.symbols, provider, today=lambda: self._now().astimezone(IST).date())
        master = self.resolver.refresh(force=False)
        self._step(3, f"instrument master records={len(master.records)} version={master.version}")
        if cfg.scanner.enabled:
            self.scanner = InstrumentScanner(cfg.scanner, now=self._now, events=self.events, metrics=self.metrics,
                                             is_market_open=lambda ts: self.calendar.is_open(ts))
            n = self.scanner.build(master, self._now().astimezone(IST).date())
            log.info("scanner_universe_built %s", kv(instruments=n, partitions=len(self.scanner.partitions),
                                                     segments=",".join(f"{k}={len(v)}" for k, v in self.scanner.universe.items())))
        # 4/5 resolve
        contracts: dict[str, ResolvedContract] = {}
        for i, sym in enumerate(cfg.logical_symbols, start=4):
            contracts[sym] = self.resolver.resolve(sym)  # raises SymbolResolutionError -> abort
            self._step(min(i, 5), f"resolved {sym}")
        # 6 publish metadata (log now; DB row + Discord line once storage / Discord exist)
        for sym, c in contracts.items():
            log.info("symbol_resolved %s", kv(logical=sym, security_id=c.security_id, expiry=c.expiry_iso, display=c.display_symbol,
                                              lot_size=c.lot_size, tick_size=c.tick_size))
            self.health.set_symbol_state(sym, "WARMING", "startup", security_id=c.security_id, expiry=c.expiry_iso)
        self._step(6, "instrument metadata published")
        # 7 storage
        if cfg.env.AUREON_STORAGE_BACKEND != "sqlite":
            # DECISION: PostgreSQL backend is selectable in config but not implemented; fail closed.
            raise StartupError(f"storage backend {cfg.env.AUREON_STORAGE_BACKEND!r} is not implemented in this build")
        self.db = Database(cfg.env.AUREON_LOCAL_DB_PATH)
        before = self.db.query_one("SELECT COALESCE(MAX(version), 0) AS v FROM schema_version")["v"] \
            if self.db.query_one("SELECT name FROM sqlite_master WHERE type='table' AND name='schema_version'") else 0
        version = self.db.migrate({"calendar": self.calendar, "now": self._now()})
        self.repos = Repositories(self.db)
        self.crashes.repos = self.repos
        if self.scanner is not None:
            self.scanner.repos = self.repos
        self.events.subscribe(self._persist_event)
        self.agents.start("storage_agent", path=cfg.env.AUREON_LOCAL_DB_PATH, schema_version=version)
        if version != before:
            self.events.emit(EventType.DATABASE_MIGRATED, f"database migrated {before} -> {version}", agent="storage_agent",
                             from_version=int(before), to_version=version)
        for c in contracts.values():
            self.repos.instruments.upsert_active(c.as_row())
        self._step(7, f"sqlite ready schema_version={version} path={cfg.env.AUREON_LOCAL_DB_PATH}")
        # 8 / 9 historical + warm (per symbol, via the same routine used by rollover)
        archive = ParquetArchive(cfg.env.AUREON_PARQUET_DIR, cfg.env.AUREON_PARQUET_ARCHIVE)
        self.historical = self._historical_factory(cfg, self.http)
        self._cached = CachedHistoricalProvider(self.historical, self.repos, archive, calendar=self.calendar, now=self._now)
        self.continuity = ContinuityService(self.historical, self.repos, self.calendar, self.health, self._now, cfg.primary_timeframe,
                                            allow_zero_trade_fill=cfg.analysis.historical.allow_verified_zero_trade_fill,
                                            retries=cfg.analysis.historical.verification_retries,
                                            backoff=cfg.analysis.historical.verification_backoff_seconds)
        for sym, c in contracts.items():
            self.health.set_symbol_state(sym, "WARMING", f"preparing {c.security_id}", security_id=c.security_id, expiry=c.expiry_iso)
            self.runtimes[sym] = self._prepare_symbol(sym, c)
            self.runtimes[sym].observer.health = self.health
        self._restore_incidents()
        self._step(8, "historical context loaded")
        self._step(9, "indicators warmed")
        self.health.set("config", "ok")
        self.health.set("storage", "ok", cfg.env.AUREON_LOCAL_DB_PATH)
        self.health.set("resolver", "ok", ", ".join(f"{s}={c.security_id}" for s, c in contracts.items()))

    # ------------------------------------------------------ symbol preparation
    def _load_history(self, sym: str, c: ResolvedContract) -> dict[Timeframe, list[Candle]]:
        assert self.cfg is not None and self.calendar is not None
        cfg = self.cfg
        now = self._now()
        hist: dict[Timeframe, list[Candle]] = {}
        for tf in cfg.mtf_timeframes:
            if tf == Timeframe.H4:
                continue
            bars = cfg.analysis.historical.warmup_bars.get(tf, 200)
            start, end = warmup_window(bars, tf, now)
            candles = self._cached.load(sym, c.security_id, c.exchange_segment, c.instrument_type, c.expiry_iso, tf, start, end)
            hist[tf] = candles[-bars:]
            log.info("historical_warmup %s %s bars=%d", sym, tf.value, len(hist[tf]))
        if Timeframe.H4 in cfg.mtf_timeframes:
            hist[Timeframe.H4] = derive_h4(hist.get(Timeframe.H1, []), now=now, calendar=self.calendar)
            log.info("historical_warmup %s H4 bars=%d (aggregated from closed H1)", sym, len(hist[Timeframe.H4]))
        n = len(hist.get(cfg.primary_timeframe, []))
        if n < cfg.env.EMA_SLOW + 5:
            raise StartupError(f"insufficient {cfg.primary_timeframe.value} history for {sym} ({c.security_id}): {n} bars")
        return hist

    def _prepare_symbol(self, sym: str, c: ResolvedContract) -> SymbolRuntime:
        """Load history, warm a fresh observer and seed a pipeline for a contract. Raises on failure.

        May run in a worker thread (rollover): it only builds NEW, unpublished objects. The
        observer warms against a scratch HealthState; the caller attaches the shared health
        and binds the pipeline to the event loop when it registers the runtime."""
        assert self.cfg is not None and self.repos is not None and self.calendar is not None
        hist = self._load_history(sym, c)
        outcomes = OutcomeService(self.repos, self.cfg.analysis, self.calendar)
        obs = SymbolObserver(self.cfg, c, self.repos, self.calendar, outcomes, self.sink, HealthState())
        obs.warm(hist)
        rt = SymbolRuntime(contract=c, observer=obs, history=hist)
        rt.pipeline = self._make_pipeline(rt)
        return rt

    def _make_pipeline(self, rt: SymbolRuntime) -> CandlePipeline:
        assert self.cfg is not None
        sym = rt.contract.logical_symbol
        h = self.cfg.analysis.historical
        p = CandlePipeline(sym, rt.contract.security_id, rt.contract.expiry_iso, self.cfg.primary_timeframe, self.cfg.mtf_timeframes,
                           lambda candle, o=rt.observer: o.on_closed_candle(candle), calendar=self.calendar,
                           on_gap=lambda gap, s=sym: self._on_gap(s, gap), clock=self._now,
                           on_pending=lambda minute, reason, s=sym: self._on_pending(s, minute, reason),
                           reconcile_live_m1=h.reconcile_every_live_m1, reconcile_volume_tolerance=h.reconcile_volume_tolerance,
                           metrics=self.metrics)
        seed_pipeline(p, rt.history)
        return p

    def _persist_event(self, event: SystemEvent) -> None:
        if self.repos is None:
            return
        try:
            self.repos.events.insert(event.to_record())
        except Exception as exc:  # noqa: BLE001 - persistence must never break the producer
            log.warning("event_persist_failed %s", kv(type=event.type.value, error=type(exc).__name__))

    def _restore_incidents(self) -> None:
        """Unresolved incidents persisted by a previous run are re-queued (restart never forgets them)."""
        if self.continuity is None:
            return
        active = {rt.contract.security_id: sym for sym, rt in self.runtimes.items()}
        for inc in self.continuity.incidents.load(active):
            rt = self.runtimes.get(inc.symbol)
            if rt is None or rt.pipeline is None:
                continue
            if inc.kind == KIND_PENDING_MINUTE and self.continuity.minute_relevant(inc.open_time):
                rt.pipeline.pending_verification.setdefault(inc.open_time, inc.reason)
            elif inc.kind == KIND_PENDING_MINUTE:
                self.continuity.incidents.abandon(inc, "trading_day_over_at_restart")
            else:
                rt.pipeline.gaps.append(GapRecord(inc.timeframe, inc.open_time, 0, 0, (), inc.first_detected_at))
                rt.pipeline.suspended = True

    def _build_pipelines(self) -> None:
        for rt in self.runtimes.values():
            if rt.pipeline is None:
                rt.pipeline = self._make_pipeline(rt)

    # ------------------------------------------------------------ feed hooks
    def _on_tick(self, tick: Tick) -> None:
        now = self._now()
        self._last_packet_at = now
        self._tick_times.append(now)
        self.metrics.inc("ticks_received")
        self.agents.heartbeat("market_feed_agent", work=1)
        rt = self._runtime_for_security(tick.security_id)
        if rt is None or rt.pipeline is None:
            return
        self.health.tick_seen(rt.contract.logical_symbol, tick.ts)
        was_stale = rt.pipeline.feed_stale
        rt.pipeline.add_tick(tick)
        if was_stale and not rt.pipeline.feed_stale:
            self._on_feed_resumed(now)

    def _on_symbol_state_changed(self, symbol: str, old: str, new: str, detail: str) -> None:
        sh = self.health.symbols.get(symbol)
        sev = Severity.INFO if new in ("LIVE", "WARMING", "ROLLOVER_WARMING") else (Severity.ERROR if new == "ERROR" else Severity.WARNING)
        message = f"{symbol} {old} -> {new}" + (f": {detail}" if detail else "")
        if new == "LIVE" and old in ("RECOVERING_GAP", "STALE", "DEGRADED", "ERROR"):
            message = f"{symbol} recovered: {old} -> LIVE" + (f" ({detail})" if detail else "")
        self.events.emit(EventType.SYMBOL_STATE_CHANGED, message, severity=sev, dedupe_key=f"symbol:{symbol}:{new}", agent="continuity_agent",
                         symbol=symbol, security_id=sh.security_id if sh else None, old_state=old, new_state=new)

    def _on_feed_status(self, status: str, detail: dict) -> None:
        self.health.set("feed", status, str(detail))
        now = self._now()
        if status == "connected":
            self._feed_generation += 1
            self._feed_stalled = False
            self._last_packet_at = now
            self.metrics.inc("websocket_reconnects", int(detail.get("reconnects", 0)) - self.metrics.get("websocket_reconnects")
                             if int(detail.get("reconnects", 0)) > self.metrics.get("websocket_reconnects") else 0)
        for rt in self.runtimes.values():
            sh = self.health.symbol(rt.contract.logical_symbol)
            sh.reconnects = int(detail.get("reconnects", sh.reconnects)) if status == "connected" else sh.reconnects
            if status == "connected":
                sh.feed_connected = True
                if rt.pipeline is not None:
                    rt.pipeline.set_connected(now)
            else:
                sh.feed_connected = False
                if rt.pipeline is not None:
                    rt.pipeline.set_disconnected()
        gen = self._feed_generation
        if status == "connected":
            self.events.emit(EventType.FEED_CONNECTED, f"feed connected (generation {gen}, reconnects={detail.get('reconnects', 0)})",
                             dedupe_key=f"feed:connected:gen:{gen}", agent="market_feed_agent", generation=gen, **_safe(detail))
            self._spawn(self.recover_all(), name="recovery")
        elif status == "reconnecting":
            self.events.emit(EventType.FEED_RECONNECTING, f"feed reconnecting (attempt {detail.get('attempt')}, delay {detail.get('delay')}s)",
                             severity=Severity.WARNING, dedupe_key=f"feed:reconnecting:gen:{gen}", agent="market_feed_agent", generation=gen, **_safe(detail))
        elif status == "stopped":
            self.events.emit(EventType.FEED_DISCONNECTED, "feed stopped", severity=Severity.WARNING, dedupe_key=f"feed:stopped:gen:{gen}",
                             agent="market_feed_agent", generation=gen)

    def _feed_watchdog(self, now: datetime) -> None:
        """A connected socket that delivers nothing for `feed_stall_seconds` in open market is a
        STALLED feed: the open minute of every symbol becomes SUSPECT (broker-verified later)."""
        assert self.cfg is not None and self.calendar is not None
        if self._feed_stalled or not getattr(self.feed, "connected", False) or not self.calendar.is_open(now):
            return
        last = self._last_packet_at
        threshold = self.feed_stall_seconds if self.feed_stall_seconds is not None else self.cfg.analysis.historical.feed_stall_seconds
        if last is None or now - last < timedelta(seconds=threshold):
            return
        self._feed_stalled = True
        self.health.set("feed", "stale", f"no packet since {last.isoformat()}")
        for sym, rt in self.runtimes.items():
            if rt.pipeline is not None:
                rt.pipeline.set_stale(now)
            sh = self.health.symbol(sym)
            if sh.state == "LIVE":
                self.health.set_symbol_state(sym, "STALE", f"feed stalled: no packet since {last.isoformat()}")
        self.events.emit(EventType.FEED_STALLED, f"feed stalled: no packet since {last.isoformat()}", severity=Severity.WARNING,
                         dedupe_key=f"feed:stalled:gen:{self._feed_generation}", agent="market_feed_agent", last_packet_at=last.isoformat())

    def _on_feed_resumed(self, now: datetime) -> None:
        if not self._feed_stalled:
            return
        self._feed_stalled = False
        self.health.set("feed", "connected", "delivery resumed")
        self.events.clear_key(f"feed:stalled:gen:{self._feed_generation}")
        self.events.emit(EventType.FEED_RECOVERED, "feed delivery resumed; stalled minutes are broker-verified", agent="market_feed_agent",
                         dedupe_key=f"feed:recovered:gen:{self._feed_generation}")
        for sym in self.runtimes:
            self._settle_symbol_state(sym, True)

    def _on_gap(self, sym: str, gap: GapRecord) -> None:
        rt = self.runtimes.get(sym)
        if rt is None or self.continuity is None:
            return
        self.continuity.record_gap(rt.contract, gap)
        self.continuity.incidents.open(sym, rt.contract.security_id, KIND_GAP, gap.timeframe, gap.open_time, "gap_detected")
        self.health.set_symbol_state(sym, "ERROR", f"data gap {gap.timeframe.value} {gap.open_time.isoformat()} missing={len(gap.missing)}",
                                     unresolved_gaps=len(rt.pipeline.unresolved_gaps) if rt.pipeline else 1)
        self.events.emit(EventType.DATA_GAP, f"{sym} {gap.timeframe.value} {gap.open_time.isoformat()} gap: {gap.present}/{gap.expected} constituents",
                         severity=Severity.ERROR, dedupe_key=f"gap:{rt.contract.security_id}:{gap.timeframe.value}:{gap.open_time.isoformat()}",
                         agent="continuity_agent", symbol=sym, security_id=rt.contract.security_id, timeframe=gap.timeframe.value,
                         open_time=gap.open_time.isoformat(), missing=len(gap.missing))
        self._spawn(self.repair(sym), name=f"repair:{sym}")

    def _on_pending(self, sym: str, minute: datetime, reason: str) -> None:
        """A minute without trustworthy live coverage: verify it against the broker, never invent it."""
        rt = self.runtimes.get(sym)
        if rt is None or self.continuity is None:
            return
        self.continuity.incidents.open(sym, rt.contract.security_id, KIND_PENDING_MINUTE, Timeframe.M1, minute, reason)
        sh = self.health.symbol(sym)
        if sh.state in ("LIVE", "STALE"):
            state = "STALE" if reason in (REASON_SILENT, REASON_SUSPECT) else "RECOVERING_GAP"
            self.health.set_symbol_state(sym, state, f"minute {minute.isoformat()} needs broker verification ({reason})")
        self.events.emit(EventType.M1_PENDING, f"{sym} minute {minute.isoformat()} needs broker verification ({reason})",
                         severity=Severity.WARNING, dedupe_key=f"m1:pending:{rt.contract.security_id}:{minute.isoformat()}",
                         agent="continuity_agent", symbol=sym, security_id=rt.contract.security_id, minute=minute.isoformat(), reason=reason)
        self._continuity_tick()

    # ------------------------------------------------------- continuity ops
    # Every operation: worker thread fetches (immutable FetchResult) -> event loop applies.
    def _begin(self, sym: str, op: str) -> bool:
        if self._inflight.get((sym, op)):
            return False
        self._inflight[(sym, op)] = True
        return True

    def _end(self, sym: str, op: str) -> None:
        self._inflight[(sym, op)] = False

    async def verify(self, sym: str) -> bool:
        """One verification attempt for the symbol's pending minutes; the incident schedule decides the next one."""
        rt = self.runtimes.get(sym)
        if rt is None or rt.pipeline is None or self.continuity is None:
            return False
        if not self._begin(sym, "verify"):
            return False
        self._verify_inflight[sym] = True
        try:
            p = rt.pipeline
            window = self.continuity.verification_window(p)
            if window is None:
                return self._settle_symbol_state(sym, True)
            incs = [i for i in self.continuity.incidents.open_for(rt.contract.security_id, KIND_PENDING_MINUTE)
                    if i.open_time + timedelta(minutes=1) <= self._now()]
            self.continuity.incidents.attempt_started(incs)
            self.events.emit(EventType.M1_VERIFICATION_STARTED, f"{sym} verifying {len(incs)} minute(s) with the broker",
                             dedupe_key=f"m1:verify:{rt.contract.security_id}:{window[0].isoformat()}", agent="continuity_agent", symbol=sym,
                             security_id=rt.contract.security_id, minutes=len(incs))
            try:
                result = await asyncio.to_thread(self.continuity.fetch_m1, rt.contract, window[0], window[1])
            except Exception as exc:  # noqa: BLE001
                log.exception("verification_error %s", kv(symbol=sym, error=type(exc).__name__))
                self._report_operation_error(sym, "verify", exc)
                self.continuity.incidents.attempt_failed(incs, f"{type(exc).__name__}: {exc}")
                return self._settle_symbol_state(sym, True)
            verified, still = self.continuity.apply_verification(rt.contract, p, result)
            for m in verified:
                self.continuity.incidents.resolve(rt.contract.security_id, KIND_PENDING_MINUTE, Timeframe.M1, m, "broker_m1")
            if verified:
                self.events.emit(EventType.M1_VERIFIED, f"{sym} {len(verified)} minute(s) replaced by broker M1 ({verified[0].isoformat()}..)",
                                 agent="continuity_agent", symbol=sym, security_id=rt.contract.security_id, minutes=[m.isoformat() for m in verified])
            unresolved = [i for i in incs if i.open_time in still]
            if unresolved:
                error = result.error or "broker has not published the minute yet"
                self.continuity.incidents.attempt_failed(unresolved, error)
                self.events.emit(EventType.M1_VERIFICATION_FAILED, f"{sym} {len(unresolved)} minute(s) still unverified: {error[:80]}",
                                 severity=Severity.WARNING, dedupe_key=f"m1:unverified:{rt.contract.security_id}:{unresolved[0].open_time.isoformat()}",
                                 agent="continuity_agent", symbol=sym, security_id=rt.contract.security_id, attempts=unresolved[0].attempt_count)
            return self._settle_symbol_state(sym, True)
        finally:
            self._verify_inflight[sym] = False
            self._end(sym, "verify")

    async def reconcile(self, sym: str) -> int:
        """Reconcile mode: compare trusted live M1 bars with the broker's after the configured delay."""
        rt = self.runtimes.get(sym)
        if rt is None or rt.pipeline is None or self.continuity is None or self.cfg is None:
            return 0
        if not self._begin(sym, "reconcile"):
            return 0
        try:
            window = self.continuity.reconcile_window(rt.pipeline, self.cfg.analysis.historical.reconcile_delay_seconds)
            if window is None:
                return 0
            try:
                result = await asyncio.to_thread(self.continuity.fetch_m1, rt.contract, window[0], window[1])
            except Exception as exc:  # noqa: BLE001
                log.exception("reconcile_error %s", kv(symbol=sym, error=type(exc).__name__))
                self._report_operation_error(sym, "reconcile", exc)
                return 0
            if not result.ok:
                return 0
            results = rt.pipeline.reconcile_m1(list(result.candles))
            for r in results:
                if r.matched:
                    continue
                self.events.emit(EventType.M1_MISMATCH,
                                 f"{sym} live M1 {r.minute.isoformat()} differs from broker on {', '.join(r.differences)}"
                                 + (" (replaced in open bucket)" if r.replaced_in_open_bucket else " (bar already closed: reported only)"),
                                 severity=Severity.ERROR, agent="continuity_agent", symbol=sym, security_id=rt.contract.security_id,
                                 minute=r.minute.isoformat(), fields=list(r.differences), replaced=r.replaced_in_open_bucket,
                                 local={"o": r.local.open, "h": r.local.high, "l": r.local.low, "c": r.local.close, "v": r.local.volume},
                                 broker={"o": r.broker.open, "h": r.broker.high, "l": r.broker.low, "c": r.broker.close, "v": r.broker.volume} if r.broker else None)
            return len(results)
        finally:
            self._end(sym, "reconcile")

    async def recover_all(self) -> None:
        for sym in list(self.runtimes):
            await self.recover(sym)

    async def recover(self, sym: str) -> bool:
        rt = self.runtimes.get(sym)
        if rt is None or rt.pipeline is None or self.continuity is None:
            return False
        p = rt.pipeline
        if p.recovering:
            return False
        fallback = rt.history.get(self.cfg.primary_timeframe, [])[-1].close_time if rt.history.get(self.cfg.primary_timeframe) else None
        window = self.continuity.recovery_window(p, fallback)
        if window is None:
            return self._settle_symbol_state(sym, True)
        p.begin_recovery()
        self.health.set_symbol_state(sym, "RECOVERING_GAP", f"backfilling M1 from {window[0].isoformat()} to {window[1].isoformat()}")
        ok = False
        try:
            result = await asyncio.to_thread(self.continuity.fetch_m1, rt.contract, window[0], window[1], self.continuity.retries)
            outcome = self.continuity.apply_recovery(rt.contract, p, result)
            ok = outcome.ok
        except Exception as exc:  # noqa: BLE001
            log.exception("recovery_error %s", kv(symbol=sym, error=type(exc).__name__))
            self._report_operation_error(sym, "recover", exc)
        finally:
            p.end_recovery()
        return self._settle_symbol_state(sym, ok)

    async def repair(self, sym: str) -> bool:
        rt = self.runtimes.get(sym)
        if rt is None or rt.pipeline is None or self.continuity is None:
            return False
        if not self._begin(sym, "repair"):
            return False
        try:
            for gap in list(rt.pipeline.unresolved_gaps):
                inc = self.continuity.incidents.open(sym, rt.contract.security_id, KIND_GAP, gap.timeframe, gap.open_time, "gap_detected")
                self.continuity.incidents.attempt_started([inc])
                try:
                    result = await asyncio.to_thread(self.continuity.fetch_gap_candle, rt.contract, gap.timeframe, gap.open_time)
                except Exception as exc:  # noqa: BLE001
                    log.exception("repair_error %s", kv(symbol=sym, error=type(exc).__name__))
                    self._report_operation_error(sym, "repair", exc)
                    self.continuity.incidents.attempt_failed([inc], f"{type(exc).__name__}: {exc}")
                    break
                if gap.resolved:  # a late verified constituent completed the bucket meanwhile
                    self.continuity.incidents.resolve(rt.contract.security_id, KIND_GAP, gap.timeframe, gap.open_time, "constituents_verified")
                    continue
                if not self.continuity.apply_repair(rt.contract, rt.pipeline, gap, result):
                    self.continuity.incidents.attempt_failed([inc], result.error or "broker candle not available yet")
                    break
                self.continuity.incidents.resolve(rt.contract.security_id, KIND_GAP, gap.timeframe, gap.open_time, "broker_candle")
                self.events.emit(EventType.DATA_GAP_REPAIRED, f"{sym} {gap.timeframe.value} {gap.open_time.isoformat()} repaired with the broker candle",
                                 agent="continuity_agent", symbol=sym, security_id=rt.contract.security_id, timeframe=gap.timeframe.value,
                                 open_time=gap.open_time.isoformat())
            return self._settle_symbol_state(sym, True)
        finally:
            self._end(sym, "repair")

    def _continuity_tick(self) -> None:
        """Retry scheduler (called every second and on new incidents): due incidents are retried
        while the market is open and the minute is relevant; stale ones are ABANDONED (recorded)."""
        if self.continuity is None or self._loop is None or self.cfg is None:
            return
        now = self._now()
        tracker = self.continuity.incidents
        for sym, rt in list(self.runtimes.items()):
            if rt.pipeline is None:
                continue
            sid = rt.contract.security_id
            for inc in tracker.open_for(sid):
                if not self.continuity.minute_relevant(inc.open_time, now):
                    tracker.abandon(inc, "trading_day_over")
                    if inc.kind == KIND_PENDING_MINUTE:
                        rt.pipeline.pending_verification.pop(inc.open_time, None)
                    self.events.emit(EventType.M1_ABANDONED, f"{sym} {inc.kind} {inc.open_time.isoformat()} unresolved when its trading day ended "
                                     f"(attempts={inc.attempt_count}); kept on record, never filled", severity=Severity.ERROR,
                                     agent="continuity_agent", symbol=sym, security_id=sid, kind=inc.kind, open_time=inc.open_time.isoformat())
            if rt.pipeline.recovering or not self._retry_allowed(now):
                continue
            if tracker.due(now, sid, KIND_PENDING_MINUTE) and not self._inflight.get((sym, "verify")):
                self._spawn(self.verify(sym), name=f"verify:{sym}")
            if tracker.due(now, sid, KIND_GAP) and not self._inflight.get((sym, "repair")):
                self._spawn(self.repair(sym), name=f"repair:{sym}")
            if rt.pipeline.reconcile_live_m1 and rt.pipeline.reconcile_queue and not self._inflight.get((sym, "reconcile")):
                if self.continuity.reconcile_window(rt.pipeline, self.cfg.analysis.historical.reconcile_delay_seconds) is not None:
                    self._spawn(self.reconcile(sym), name=f"reconcile:{sym}")

    def _retry_allowed(self, now: datetime) -> bool:
        """Retries run while the calendar covers the date; incidents themselves are only kept for
        the current trading day (a minute from today's session is verifiable after the close,
        when the broker publishes its final bars), so nothing busy-loops on a closed market."""
        assert self.calendar is not None
        return self.calendar.covers(self.calendar.trading_date(now))

    def _sync_incidents(self, sym: str) -> None:
        """Incidents whose condition the pipeline no longer reports are RESOLVED (e.g. a pending
        minute delivered by reconnect recovery, a gap completed by late verified constituents)."""
        rt = self.runtimes.get(sym)
        if rt is None or rt.pipeline is None or self.continuity is None:
            return
        p, sid = rt.pipeline, rt.contract.security_id
        for inc in self.continuity.incidents.open_for(sid):
            if inc.kind == KIND_PENDING_MINUTE and inc.open_time not in p.pending_verification and inc.open_time in p._seen_m1:
                self.continuity.incidents.resolve(sid, KIND_PENDING_MINUTE, Timeframe.M1, inc.open_time, "broker_m1")
            elif inc.kind == KIND_GAP and not any(g.open_time == inc.open_time and g.timeframe is inc.timeframe and not g.resolved for g in p.gaps):
                self.continuity.incidents.resolve(sid, KIND_GAP, inc.timeframe, inc.open_time, "repaired")

    def _settle_symbol_state(self, sym: str, recovered: bool) -> bool:
        rt = self.runtimes[sym]
        p = rt.pipeline
        assert p is not None and self.cfg is not None and self.continuity is not None
        self._sync_incidents(sym)
        now = self._now()
        if not self.calendar.covers(self.calendar.trading_date(now)):
            # no calendar for this year: nothing can be trusted, no state is ever "LIVE" (fail closed)
            self.health.set_symbol_state(sym, "ERROR", f"CALENDAR_OUT_OF_RANGE: MCX calendar for {self.calendar.trading_date(now).year} not installed",
                                         unresolved_gaps=len(p.unresolved_gaps))
            return False
        gaps = len(p.unresolved_gaps)
        pending = p.pending_minutes()
        closed_pending = [m for m in pending if m + timedelta(minutes=1) <= now]
        if recovered and p.continuity_ok:
            partial = p.current_minute_partial(now)
            if partial is not None:
                # the minute in progress began before coverage did (mid-minute connect,
                # reconnect or rollover): its trust is unknown until the broker M1 replaces it
                self.health.set_symbol_state(sym, "RECOVERING_GAP", f"current minute {partial.isoformat()} is partial; "
                                             "awaiting broker verification", unresolved_gaps=0)
                return False
            if p.feed_stale:
                self.health.set_symbol_state(sym, "STALE", "feed stalled; awaiting delivery", unresolved_gaps=0)
                return False
            self.health.set_symbol_state(sym, "LIVE", "continuity verified", unresolved_gaps=0)
            return True
        if recovered and not gaps and pending and not closed_pending:
            # only the still-open (current) minute is pending: nothing to verify yet
            self.health.set_symbol_state(sym, "RECOVERING_GAP", f"current minute {pending[0].isoformat()} awaits broker verification",
                                         unresolved_gaps=0)
            return False
        if closed_pending and not gaps:
            detail = f"{len(closed_pending)} minute(s) unverified by broker; analytics suspended"
        else:
            detail = f"unresolved gaps={gaps}" if gaps else "recovery failed; analytics suspended"
        # incident age decides how loudly we complain; retries continue in every state
        oldest = self.continuity.incidents.oldest_open(rt.contract.security_id)
        age = oldest.age(now).total_seconds() if oldest is not None else None
        h = self.cfg.analysis.historical
        if not recovered:
            state = "ERROR"
        elif age is None or age < h.degraded_after_seconds:
            state = "RECOVERING_GAP"
        elif age < h.error_after_seconds:
            state = "DEGRADED"
        else:
            state = "ERROR"
        if oldest is not None:
            detail += f"; retrying (attempts={oldest.attempt_count}, next={oldest.next_retry_at.isoformat()})"
        self.health.set_symbol_state(sym, state, detail, unresolved_gaps=gaps + len(closed_pending))
        if state == "ERROR":
            self.sink.status(self.health.status_line())
        return False

    # -------------------------------------------------------------- live run
    async def run(self) -> None:
        assert self.cfg is not None and self.repos is not None
        cfg = self.cfg
        self.stop = asyncio.Event()
        self._loop = asyncio.get_running_loop()
        # every pipeline / health mutation happens on this thread; workers only fetch
        self.health.bind_to_current_thread()
        for rt in self.runtimes.values():
            if rt.pipeline is not None:
                rt.pipeline.bind_to_current_thread()
        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                self._loop.add_signal_handler(sig, self.stop.set)
            except (NotImplementedError, RuntimeError):  # pragma: no cover - Windows / nested loops
                pass
        # 13 (sink first so status lines have a destination) - Discord is presentation only
        if self._sink_factory is not None:
            self.sink, self.discord_client = self._sink_factory(cfg, self.repos)
        elif cfg.env.DISCORD_TOKEN and cfg.env.DISCORD_CHANNEL_ID:
            from aureon_mcx.discord.bot import DiscordSink, MonitorRegistry, create_client
            from aureon_mcx.discord.coalescer import UpdateCoalescer
            from aureon_mcx.discord.message_refs import MessageRefs

            from aureon_mcx.api.status import StatusProjection
            from aureon_mcx.discord.ops import OpsCommands

            refs = MessageRefs(self.repos)
            self.sink = DiscordSink(UpdateCoalescer(refs, cfg.analysis.discord.debounce_seconds))
            self.discord_client = create_client(cfg, self.repos, self.sink, refs, MonitorRegistry(self.repos), ops=self.ops,
                                                commands=OpsCommands(StatusProjection(self)))
        else:
            log.warning("discord_disabled %s", kv(reason="DISCORD_TOKEN / DISCORD_CHANNEL_ID not set; running headless"))
            self.sink = NullSink()
        for rt in self.runtimes.values():
            rt.observer.sink = self.sink
        # 10 / 11 websocket + subscriptions
        ids = [rt.contract.security_id for rt in self.runtimes.values()]
        if self._feed_factory is not None:
            self.feed = self._feed_factory(cfg, self._on_tick, self.health)
            if hasattr(self.feed, "on_status"):
                self.feed.on_status = self._on_feed_status
        else:
            self.feed = DhanLiveFeedProvider(cfg.env.DHAN_CLIENT_ID.get_secret_value(), cfg.env.DHAN_ACCESS_TOKEN.get_secret_value(),
                                             cfg.env.EXCHANGE_SEGMENT, self._on_tick, self._on_feed_status, closed_backoff=300.0,
                                             is_market_open=lambda: self.calendar.is_open(self._now()), now=self._now)
        await self.feed.subscribe(ids)
        self._build_pipelines()
        for rt in self.runtimes.values():
            if rt.pipeline is not None:
                rt.pipeline.bind_to_current_thread()
        vi = version_info()
        self.events.emit(EventType.APP_STARTED, "Aureon observer starting: " + ", ".join(f"{s}={c.security_id}" for s, c in self.contracts.items())
                         + f" (version {vi['app']}, git {vi['git_short']})", agent="health_agent", version=vi["app"], git_sha=vi["git_sha"])
        self.events.emit(EventType.DEPLOYMENT_INFO, f"deployed version {vi['app']} git {vi['git_short']}", agent="health_agent",
                         dedupe_key=f"deploy:{vi['git_sha']}", version=vi["app"], git_sha=vi["git_sha"], branch=vi["branch"], build=vi["build"])
        for agent_id in ("calendar_agent", "aggregation_agent", "continuity_agent", "analysis_agent", "setup_agent", "outcome_agent",
                         "rollover_agent", "health_agent"):
            self.agents.start(agent_id)
        self.agents.register("market_feed_agent", "Market feed", "Dhan WebSocket: subscriptions, packet timing, reconnects")
        self.agents.start("market_feed_agent", subscriptions=len(ids))
        self._market_watch(self._now())
        self._specs["feed"] = TaskSpec("feed", lambda: self.feed.run(self.stop), critical=True)
        self._start("feed")
        self._step(10, "websocket connecting")
        self._step(11, f"subscribed {','.join(ids)}")
        # 12 observer: wall-clock boundary flush
        self._specs["flush"] = TaskSpec("flush", self._flush_loop, critical=True)
        self._start("flush")
        self._step(12, "observer live")
        log.info("observer live")
        if self.discord_client is not None:
            self._specs["discord"] = TaskSpec("discord", lambda: self.discord_client.start(cfg.env.DISCORD_TOKEN.get_secret_value()),
                                              critical=False, max_restarts=1000, backoff=(5.0, 15.0, 30.0, 60.0))
            self._start("discord")
            self.health.set("discord", "starting")
            self.agents.start("discord_agent", mode="gateway")
            log.info("discord live")
        else:
            self.health.set("discord", "ok", "headless")
            self.agents.set_state("discord_agent", "STOPPED", "headless (no DISCORD_TOKEN / DISCORD_CHANNEL_ID)")
        self._step(13, "discord started")
        # 14 health + housekeeping
        self.health.set("observer", "live")
        self.health.set("feed", "connecting")
        self.sink.status("\n".join([f"{s} -> security_id {c.security_id} -> expiry {c.expiry_iso}" for s, c in self.contracts.items()]
                                   + [self.health.status_line()]))
        self._specs["housekeeping"] = TaskSpec("housekeeping", self._housekeeping_loop, critical=True)
        self._start("housekeeping")
        self._refresh_db_stats()
        await self._start_scanner(cfg)
        self._start_api(cfg)
        self._step(14, "service health published")
        log.info("service_health %s", kv(**{k: v["status"] for k, v in self.health.snapshot()["components"].items()}))
        await self._supervise()
        await self.shutdown()

    # ----------------------------------------------------------- projections
    def tick_rate(self) -> float | None:
        """Ticks per minute over the last minute (deep feed)."""
        if not self._tick_times:
            return None
        now = self._now()
        recent = sum(1 for t in self._tick_times if now - t <= timedelta(minutes=1))
        return float(recent)

    def _refresh_db_stats(self) -> None:
        """Cheap aggregate counts for the status API, refreshed by housekeeping (never per request)."""
        if self.repos is None:
            return
        try:
            rows = self.repos.db.query("SELECT state, COUNT(*) AS n FROM setups WHERE closed_at IS NULL GROUP BY state")
            backlog = self.repos.db.query_one(
                """SELECT COUNT(*) AS n FROM feature_snapshots s WHERE NOT EXISTS (
                       SELECT 1 FROM outcome_observations o WHERE o.snapshot_id = s.id AND o.horizon = '__all_final__')""")
            self.db_stats = {"setups_by_state": {r["state"]: int(r["n"]) for r in rows}, "outcome_backlog": int(backlog["n"]) if backlog else 0,
                             "refreshed_at": self._now().isoformat()}
        except Exception as exc:  # noqa: BLE001
            log.warning("db_stats_failed %s", kv(error=type(exc).__name__))

    # ------------------------------------------------------------------ api
    def _start_api(self, cfg: AppConfig) -> None:
        if not cfg.env.AUREON_API_ENABLED:
            self.agents.set_state("api_agent", "STOPPED", "AUREON_API_ENABLED=false")
            return
        from aureon_mcx.api import ApiServer

        self.api = ApiServer(self, cfg.env.AUREON_API_HOST, cfg.env.AUREON_API_PORT)
        self._specs["api"] = TaskSpec("api", lambda: self.api.run(self.stop), critical=False, max_restarts=1000, agent="api_agent",
                                      backoff=(2.0, 5.0, 10.0, 30.0))
        self._start("api")

    # --------------------------------------------------------------- scanner
    async def _start_scanner(self, cfg: AppConfig) -> None:
        """Tier-1 scanner: its own feed connection(s) (partitioned within Dhan's documented
        limits) deliver packets to the scanner only; the deep observer's feed is untouched."""
        if self.scanner is None:
            self.agents.set_state("scanner_agent", "STOPPED", "scanner disabled")
            return
        parts = self.scanner.partitions
        if not parts:
            self.agents.set_state("scanner_agent", "STOPPED", "empty universe")
            return
        for i, ids in enumerate(parts):
            if self._scanner_feed_factory is not None:
                feed = self._scanner_feed_factory(cfg, self.scanner.on_packet, i)
            else:
                feed = DhanLiveFeedProvider(cfg.env.DHAN_CLIENT_ID.get_secret_value(), cfg.env.DHAN_ACCESS_TOKEN.get_secret_value(),
                                            cfg.scanner.segments[0], lambda tick: None, lambda s, d, i=i: self._on_scanner_feed_status(i, s, d),
                                            mode=cfg.scanner.feed_mode, on_packet=self.scanner.on_packet, closed_backoff=300.0,
                                            is_market_open=lambda: self.calendar.is_open(self._now()), now=self._now)
            await feed.subscribe(ids)
            self.scanner_feeds.append(feed)
            name = f"scanner_feed_{i}"
            self._specs[name] = TaskSpec(name, (lambda f=feed: f.run(self.stop)), critical=False, max_restarts=1000, agent="scanner_agent",
                                         backoff=(2.0, 5.0, 10.0, 30.0, 60.0))
            self._start(name)
        self._specs["scanner"] = TaskSpec("scanner", self._scanner_loop, critical=False, max_restarts=1000, agent="scanner_agent")
        self._start("scanner")
        self.agents.start("scanner_agent", universe=self.scanner.universe_size, connections=len(parts))
        self.events.emit(EventType.SCANNER_UPDATED, f"scanner started: {self.scanner.universe_size} instruments over {len(parts)} feed connection(s)",
                         agent="scanner_agent", dedupe_key="scanner:started", universe=self.scanner.universe_size, connections=len(parts))

    def _on_scanner_feed_status(self, index: int, status: str, detail: dict) -> None:
        if self.scanner is None:
            return
        self.agents.heartbeat("scanner_agent", **{f"feed_{index}": status})
        if status == "connected":
            self.events.emit(EventType.FEED_CONNECTED, f"scanner feed {index} connected", dedupe_key=f"scanner:feed:{index}:connected:{detail.get('reconnects', 0)}",
                             agent="scanner_agent", connection=index)
        elif status == "reconnecting" and self.calendar is not None and self.calendar.is_open(self._now()):
            self.events.emit(EventType.FEED_RECONNECTING, f"scanner feed {index} reconnecting (attempt {detail.get('attempt')})", severity=Severity.WARNING,
                             dedupe_key=f"scanner:feed:{index}:reconnecting", agent="scanner_agent", connection=index)

    async def _scanner_loop(self) -> None:
        assert self.stop is not None and self.scanner is not None
        while not self.stop.is_set():
            now = self._now()
            self.scanner.tick(now)
            st = self.scanner.status(now)
            self.agents.heartbeat("scanner_agent", work=1, queue_depth=st["stale"] + st["unavailable"], universe=st["universe"],
                                  advancers=st["advancers"], decliners=st["decliners"], stale=st["stale"])
            try:
                await asyncio.wait_for(self.stop.wait(), timeout=self.scanner_interval)
            except asyncio.TimeoutError:
                pass

    # ----------------------------------------------------------- supervision
    def _start(self, name: str) -> None:
        spec = self._specs[name]
        spec.agent = spec.agent or TASK_AGENTS.get(name, name)
        spec.started_at = self._now()
        self._tasks[name] = asyncio.create_task(spec.factory(), name=name)

    async def _supervise(self) -> None:
        assert self.stop is not None
        stop_task = asyncio.create_task(self.stop.wait(), name="stop")
        try:
            while not self.stop.is_set():
                pending = {t for t in self._tasks.values() if not t.done()}
                done, _ = await asyncio.wait(pending | {stop_task}, return_when=asyncio.FIRST_COMPLETED)
                if stop_task in done:
                    break
                for t in done:
                    await self._handle_task_exit(t)
        finally:
            stop_task.cancel()

    async def _handle_task_exit(self, task: asyncio.Task) -> None:
        name = task.get_name()
        spec = self._specs.get(name)
        if task.cancelled():
            return
        stopping = self.stop is None or self.stop.is_set()
        exc = task.exception()
        if exc is None and (stopping or spec is None):
            return  # orderly exit at shutdown (or an unsupervised task): nothing to do
        agent = spec.agent or TASK_AGENTS.get(name, name)
        if exc is None:
            # A supervised loop returned while the application is still running.  A feed
            # that "finishes" is a silent feed; a flush loop that finishes never closes a
            # candle again.  Treat it exactly like a crash so it is restarted / escalated.
            exc = RuntimeError(f"task {name} returned while the application is running")
            self.task_failures.append((name, "TaskExited"))
            log.error("task_exited %s", kv(task=name, detail="returned while application running"))
            self.health.set(name, "error", "exited unexpectedly")
        else:
            self.task_failures.append((name, type(exc).__name__))
            log.error("task_failed %s", kv(task=name, error=type(exc).__name__, detail=str(exc)[:200]), exc_info=exc)
            if spec is None:
                return
            self.health.set(name, "error", f"{type(exc).__name__}: {str(exc)[:120]}")
        # detect -> report (durable) -> isolate -> restart -> recover
        rep = self.crashes.report(name, exc, agent=agent, task=name, restart_number=spec.restarts + 1)
        self.metrics.inc("crashes")
        spec.last_crash_id = rep.crash_id
        self.agents.error(agent, f"{type(exc).__name__}: {exc}", state="DEGRADED", crash_id=rep.crash_id)
        if spec.restarts >= spec.max_restarts:
            self.crashes.resolve(rep.crash_id, "failed")
            if spec.critical:
                log.critical("task_restart_limit %s", kv(task=name, restarts=spec.restarts))
                self.agents.set_state(agent, "FAILED", f"{name} failed {spec.restarts} times; shutting down")
                self.health.set("observer", "error", f"{name} failed repeatedly; shutting down")
                self.sink.status(self.health.status_line())
                self.events.emit(EventType.AGENT_FAILED, f"{agent} failed permanently ({name} crashed {spec.restarts + 1} times); shutting down safely",
                                 severity=Severity.CRITICAL, agent=agent, crash_id=rep.crash_id)
                assert self.stop is not None
                self.stop.set()
            else:
                self.agents.set_state(agent, "FAILED", f"{name} failed {spec.restarts} times; giving up (non-critical)")
            return
        delay = spec.backoff[min(spec.restarts, len(spec.backoff) - 1)] * self.task_backoff_scale
        spec.restarts += 1
        self.metrics.inc("restarts")
        self.agents.restarting(agent, spec.restarts, f"{type(exc).__name__}; restart {spec.restarts}/{spec.max_restarts} in {delay:.1f}s")
        self.sink.status(self.health.status_line())
        log.warning("task_restart %s", kv(task=name, delay=delay, attempt=spec.restarts))
        assert self.stop is not None
        try:
            await asyncio.wait_for(self.stop.wait(), timeout=delay)
            return
        except asyncio.TimeoutError:
            pass
        if name == "discord" and self.discord_client is not None and self.discord_client.is_closed():
            self.health.set("discord", "restarting")
        self._start(name)

    def _recovery_check(self) -> None:
        """A restarted task that has run cleanly for a while resolves its crash report (recovered)."""
        now = self._now()
        for name, spec in self._specs.items():
            task = self._tasks.get(name)
            if spec.last_crash_id is None or task is None or task.done() or spec.started_at is None:
                continue
            if (now - spec.started_at) >= timedelta(seconds=max(2.0, 5.0 * self.task_backoff_scale)):
                self.crashes.resolve(spec.last_crash_id, "success")
                spec.last_crash_id = None
                self.agents.set_state(spec.agent or TASK_AGENTS.get(name, name), "HEALTHY", f"{name} recovered after restart {spec.restarts}")
                if name != "discord":
                    self.health.set(name, "ok", f"recovered after restart {spec.restarts}")

    # ------------------------------------------------------------------ loops
    async def _flush_loop(self) -> None:
        assert self.stop is not None
        grace = timedelta(seconds=2)
        while not self.stop.is_set():
            now = self._now() - grace
            for p in self.pipelines.values():
                p.flush_at(now)  # exceptions propagate to the supervisor (never swallowed)
            self._feed_watchdog(self._now())
            self._continuity_tick()
            self._recovery_check()
            self.agents.heartbeat("aggregation_agent", queue_depth=sum(len(p.unresolved_gaps) for p in self.pipelines.values()))
            self.agents.heartbeat("continuity_agent", queue_depth=len(self.continuity.incidents.incidents) if self.continuity else 0)
            try:
                await asyncio.wait_for(self.stop.wait(), timeout=self.flush_interval)
            except asyncio.TimeoutError:
                pass

    def _market_watch(self, now: datetime) -> None:
        """Market state transitions (open / closed / session change / approaching close) as events."""
        assert self.calendar is not None
        ms = self.calendar.market_state(now)
        prev = self._market_state
        self._market_state = ms
        self.agents.heartbeat("calendar_agent", state=ms.state, reason=ms.reason, session=ms.session, trading_date=ms.trading_date.isoformat(),
                              next_open=ms.next_open.isoformat() if ms.next_open else None,
                              next_close=ms.next_close.isoformat() if ms.next_close else None, holiday=ms.holiday)
        if ms.reason == "CALENDAR_OUT_OF_RANGE":
            self.agents.set_state("calendar_agent", "ERROR", ms.detail)
        elif self.agents.get("calendar_agent").state == "ERROR":
            self.agents.set_state("calendar_agent", "HEALTHY", "calendar covers the trading date")
        if prev is None or (prev.state, prev.reason, prev.session, prev.trading_date) == (ms.state, ms.reason, ms.session, ms.trading_date):
            pass
        elif ms.is_open and not prev.is_open:
            label = "EVENING SESSION OPEN" if ms.session == "EVENING" else ("SPECIAL SESSION OPEN" if ms.session == "SPECIAL" else "MARKET OPEN")
            self.events.emit(EventType.MARKET_OPENED, f"MCX {label} - {ms.detail}", dedupe_key=f"market:open:{ms.trading_date}:{ms.session}",
                             agent="calendar_agent", **ms.to_dict())
        elif not ms.is_open and prev.is_open:
            label = {"EVENING_SESSION_CLOSED": "MORNING SESSION CLOSED", "OUTSIDE_TRADING_HOURS": "MARKET CLOSED"}.get(ms.reason, f"CLOSED - {ms.reason}")
            if prev.session == "EVENING" and ms.reason == "OUTSIDE_TRADING_HOURS":
                label = "EVENING SESSION CLOSED"
            self.events.emit(EventType.MARKET_CLOSED, f"MCX {label} - {ms.detail}" + (f"; next open {ms.next_open.isoformat()}" if ms.next_open else ""),
                             dedupe_key=f"market:closed:{ms.trading_date}:{ms.reason}:{prev.session}", agent="calendar_agent", **ms.to_dict())
        elif ms.is_open and prev.is_open and ms.session != prev.session:
            self.events.emit(EventType.MARKET_SESSION_CHANGED, f"MCX {ms.session} session - {ms.detail}", dedupe_key=f"market:session:{ms.trading_date}:{ms.session}",
                             agent="calendar_agent", **ms.to_dict())
        elif not ms.is_open and not prev.is_open and ms.reason != prev.reason:
            label = {"WEEKEND": "MCX CLOSED - weekend", "FULL_HOLIDAY": f"MCX CLOSED - {ms.holiday}", "MORNING_SESSION_CLOSED": "MCX MORNING SESSION CLOSED",
                     "SPECIAL_SESSION_PENDING": f"MCX CLOSED - {ms.holiday} (timings pending)"}.get(ms.reason, f"MCX CLOSED - {ms.reason}")
            self.events.emit(EventType.MARKET_CLOSED, f"{label}" + (f"; next open {ms.next_open.isoformat()}" if ms.next_open else ""),
                             dedupe_key=f"market:closed:{ms.trading_date}:{ms.reason}", agent="calendar_agent", **ms.to_dict())
        if ms.is_open and ms.closes_at is not None and timedelta(0) < ms.closes_at - now <= timedelta(minutes=10):
            self.events.emit(EventType.MARKET_CLOSING_SOON, f"MCX closes at {ms.closes_at.astimezone(IST).strftime('%H:%M')} IST",
                             dedupe_key=f"market:closing_soon:{ms.trading_date}:{ms.closes_at.isoformat()}", agent="calendar_agent")

    def _calendar_check(self) -> None:
        """Live mode: when the trading date rolls into a year without a calendar, fail closed
        (symbols ERROR, no gaps invented, loud event) instead of assuming normal hours."""
        assert self.calendar is not None
        try:
            self.calendar.require_coverage(self._now())
        except CalendarOutOfRange as exc:
            if self.health.components.get("calendar") is not None and self.health.components["calendar"].status == "error":
                return
            self.health.set("calendar", "error", str(exc))
            for sym in self.runtimes:
                self.health.set_symbol_state(sym, "ERROR", f"CALENDAR_OUT_OF_RANGE: {exc}")
            self.events.emit(EventType.CALENDAR_FAILURE, f"CALENDAR_OUT_OF_RANGE: {exc}; observation suspended", severity=Severity.CRITICAL,
                             dedupe_key=f"calendar:out_of_range:{exc.session_date.year}", agent="calendar_agent")
            self.sink.status(f"CALENDAR_OUT_OF_RANGE: {exc}")

    def _stale_check(self) -> None:
        assert self.cfg is not None and self.calendar is not None
        now = self._now()
        if not self.calendar.is_open(now):
            return
        limit = timedelta(seconds=2 * self.cfg.primary_timeframe.seconds + 60)
        # closed-market time never counts: right after the open the reference is the session start
        session_start, _ = self.calendar.trading_day_span(self.calendar.trading_date(now))
        for sym, rt in self.runtimes.items():
            sh = self.health.symbol(sym)
            if sh.state != "LIVE" or not sh.feed_connected or rt.pipeline is None:
                continue
            last = rt.pipeline.last_closed.get(self.cfg.primary_timeframe)
            if last is not None and now - max(last, session_start) > limit:
                self.health.set_symbol_state(sym, "STALE", f"no closed {self.cfg.primary_timeframe.value} since {last.isoformat()}")
                continue
            tick = sh.last_tick_at
            if tick is not None and now - max(tick, session_start) > timedelta(seconds=90):
                self.health.set_symbol_state(sym, "STALE", f"connected but no market data since {tick.isoformat()}")

    async def _housekeeping_loop(self) -> None:
        assert self.cfg is not None and self.stop is not None
        last_master = self._now()
        last_line = ""
        while not self.stop.is_set():
            try:
                await asyncio.wait_for(self.stop.wait(), timeout=self.housekeeping_interval)
            except asyncio.TimeoutError:
                pass
            if self.stop.is_set():
                break
            self._stale_check()
            self._calendar_check()
            self._market_watch(self._now())
            self._refresh_db_stats()
            self.agents.heartbeat("health_agent")
            self.agents.check_heartbeats()
            line = self.health.status_line()
            if line != last_line:
                last_line = line
                self.sink.status(line)
                log.info("service_health %s", kv(line=line.replace("\n", " | ")))
            if self._now() - last_master >= timedelta(hours=self.cfg.symbols.instrument_master.refresh_hours):
                last_master = self._now()
                await self.check_rollover()

    # --------------------------------------------------------------- rollover
    async def check_rollover(self) -> None:
        """Daily: refresh the instrument master; roll contracts through the staged process."""
        assert self.resolver is not None and self.cfg is not None and self.repos is not None
        try:
            await asyncio.to_thread(self.resolver.refresh, True)
        except DhanError as exc:
            log.warning("instrument_master_refresh_failed %s", kv(error=str(exc)))
            return
        for sym in list(self.runtimes):
            try:
                new = self.resolver.resolve(sym)
            except SymbolResolutionError as exc:
                log.error("rollover_resolution_failed %s", kv(logical=sym, reason=exc.reason))
                continue
            if new.security_id != self.runtimes[sym].contract.security_id:
                await self.roll_symbol(sym, new)

    async def roll_symbol(self, sym: str, new: ResolvedContract) -> bool:
        """Staged rollover: prepare (history, warm, seed) -> subscribe -> atomic switch -> unsubscribe old.
        Any failure keeps the old contract running untouched."""
        assert self.repos is not None
        old_rt = self.runtimes[sym]
        old = old_rt.contract
        log.warning("rollover_begin %s", kv(logical=sym, old_security_id=old.security_id, new_security_id=new.security_id, expiry=new.expiry_iso))
        self.health.set_symbol_state(sym, "ROLLOVER_WARMING", f"preparing {new.security_id}", security_id=old.security_id, expiry=old.expiry_iso)
        self.events.emit(EventType.ROLLOVER_STARTED, f"{sym} rollover: warming {new.security_id} (expiry {new.expiry_iso}) to replace {old.security_id}",
                         agent="rollover_agent", symbol=sym, security_id=new.security_id, old_security_id=old.security_id)
        try:
            new_rt = await asyncio.to_thread(self._prepare_symbol, sym, new)
            new_rt.observer.health = self.health           # warmed against a scratch HealthState in the worker
            assert new_rt.pipeline is not None
            new_rt.pipeline.bind_to_current_thread()
        except (StartupError, DhanError, Exception) as exc:  # noqa: BLE001
            log.error("rollover_failed %s", kv(logical=sym, new_security_id=new.security_id, error=type(exc).__name__, detail=str(exc)[:200]))
            self.health.set_symbol_state(sym, "LIVE" if old_rt.pipeline is not None and old_rt.pipeline.continuity_ok else "ERROR",
                                         f"rollover to {new.security_id} failed: {type(exc).__name__}; old contract retained",
                                         security_id=old.security_id, expiry=old.expiry_iso)
            self.sink.status(f"{sym} rollover to security_id {new.security_id} FAILED; keeping {old.security_id}")
            self.events.emit(EventType.ROLLOVER_FAILED, f"{sym} rollover to {new.security_id} failed: {type(exc).__name__}; keeping {old.security_id}",
                             severity=Severity.ERROR, agent="rollover_agent", symbol=sym, security_id=old.security_id)
            return False
        ind = new_rt.observer.indicators[self.cfg.primary_timeframe].latest
        if ind is None or not ind.warmed:
            log.error("rollover_not_warm %s", kv(logical=sym, new_security_id=new.security_id))
            self.health.set_symbol_state(sym, "LIVE", f"rollover to {new.security_id} aborted: indicators not warm", security_id=old.security_id)
            return False
        # Race-safe handover: the prepared runtime is registered under its security id BEFORE
        # subscribing, so a tick that arrives inside subscribe() is routed into its pipeline
        # instead of being discarded. Coverage starts now (the current minute is partial and
        # will be broker-verified like any other partial minute).
        feed_connected = bool(getattr(self.feed, "connected", False))
        if feed_connected:
            new_rt.pipeline.set_connected(self._now())
        self.pending_runtimes[new.security_id] = new_rt
        try:
            if self.feed is not None:
                await self.feed.subscribe([new.security_id])
        except Exception as exc:  # noqa: BLE001
            self.pending_runtimes.pop(new.security_id, None)
            log.error("rollover_subscribe_failed %s", kv(logical=sym, new_security_id=new.security_id, error=type(exc).__name__))
            self.health.set_symbol_state(sym, "LIVE" if old_rt.pipeline is not None and old_rt.pipeline.continuity_ok else "ERROR",
                                         f"rollover to {new.security_id} failed at subscribe; old contract retained",
                                         security_id=old.security_id, expiry=old.expiry_iso)
            return False
        self.repos.instruments.upsert_active(new.as_row())
        self.runtimes[sym] = new_rt          # atomic switch of observer / history / pipeline / contract
        self.pending_runtimes.pop(new.security_id, None)
        if self.feed is not None:
            await self.feed.unsubscribe([old.security_id])
        # NOT LIVE yet: continuity between the new contract's history and its live stream is unverified.
        self.health.set_symbol_state(sym, "RECOVERING_GAP", f"rolled from {old.security_id}; verifying live continuity",
                                     security_id=new.security_id, expiry=new.expiry_iso, unresolved_gaps=0, feed_connected=feed_connected)
        log.warning("rollover_switched %s", kv(logical=sym, security_id=new.security_id, expiry=new.expiry_iso))
        self.sink.status(f"{sym} rolled -> security_id {new.security_id} -> expiry {new.expiry_iso} (verifying continuity)")
        self.events.emit(EventType.ROLLOVER_SWITCHED, f"{sym} subscription switched {old.security_id} -> {new.security_id} (expiry {new.expiry_iso}); verifying continuity",
                         agent="rollover_agent", symbol=sym, security_id=new.security_id, old_security_id=old.security_id)
        if feed_connected:
            ok = await self.recover(sym)
        else:
            ok = False
            self.health.set_symbol_state(sym, "RECOVERING_GAP", "feed disconnected; continuity recovery runs on reconnect",
                                         security_id=new.security_id, expiry=new.expiry_iso)
        log.warning("rollover_complete %s", kv(logical=sym, security_id=new.security_id, expiry=new.expiry_iso,
                                                 state=self.health.symbol(sym).state))
        self.events.emit(EventType.ROLLOVER_COMPLETED, f"{sym} rollover complete: {new.security_id} state={self.health.symbol(sym).state}",
                         agent="rollover_agent", symbol=sym, security_id=new.security_id, state=self.health.symbol(sym).state)
        return ok

    # --------------------------------------------------------------- shutdown
    async def shutdown(self) -> None:
        log.info("shutdown_begin")
        if self.stop is not None:
            self.stop.set()
        self.events.emit(EventType.APP_STOPPED, "Aureon observer stopping", agent="health_agent")
        for a in self.agents.agents.values():
            if a.state not in ("FAILED", "STOPPED"):
                a.state = "STOPPED"
        for f in [self.feed] + list(self.scanner_feeds):
            if f is not None and hasattr(f, "disconnect"):
                try:
                    await f.disconnect()
                except Exception:  # noqa: BLE001
                    pass
        if self.discord_client is not None:
            try:
                await self.discord_client.close()
            except Exception:  # noqa: BLE001
                pass
        # let in-flight verification / recovery finish briefly, then cancel everything
        if self._bg_tasks:
            await asyncio.wait(set(self._bg_tasks), timeout=2.0)
        for t in list(self._tasks.values()) + list(self._bg_tasks):
            t.cancel()
        await asyncio.gather(*self._tasks.values(), *self._bg_tasks, return_exceptions=True)
        if self.db is not None:
            self.db.close()
        if self.http is not None and hasattr(self.http, "close"):
            self.http.close()
        log.info("shutdown_complete")


def _safe(detail: dict) -> dict:
    """Event payload from a feed status detail: primitives only, never credentials."""
    return {k: v for k, v in detail.items() if isinstance(v, (int, float, str, bool)) and k not in ("token", "client_id")}


def main(argv: list[str] | None = None) -> int:
    import argparse

    parser = argparse.ArgumentParser(description="Aureon MCX - research-first DhanHQ observer for MCX GOLD / SILVER")
    parser.add_argument("--config-dir", default=None)
    parser.add_argument("--env-file", default=None)
    args = parser.parse_args(argv)
    configure_logging("INFO")
    app = Application(args.config_dir, args.env_file)
    try:
        app.startup()
    except StartupError as exc:
        print(f"startup aborted: {exc}", file=sys.stderr)
        return 2
    try:
        asyncio.run(app.run())
    except KeyboardInterrupt:  # pragma: no cover
        pass
    return 0
