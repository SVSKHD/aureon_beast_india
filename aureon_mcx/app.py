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
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any, Awaitable, Callable

from aureon_mcx.broker.dhan import DhanApiError, DhanError, DhanInstrumentProvider, ResolvedContract, SymbolResolutionError, SymbolResolver
from aureon_mcx.broker.dhan.client import DhanHttpClient
from aureon_mcx.broker.dhan.errors import DhanCredentialsError
from aureon_mcx.broker.dhan.historical import CachedHistoricalProvider, DhanHistoricalProvider
from aureon_mcx.broker.dhan.live_feed import DhanLiveFeedProvider
from aureon_mcx.config import AppConfig, ConfigError, load_config
from aureon_mcx.continuity import ContinuityService
from aureon_mcx.health import HealthState
from aureon_mcx.logging_setup import configure_logging, kv
from aureon_mcx.market.candle import Candle
from aureon_mcx.market.candle_builder import CandlePipeline, GapRecord, Tick
from aureon_mcx.market.sessions import SessionCalendar
from aureon_mcx.market.timeframe import Timeframe
from aureon_mcx.market.timeutil import IST, utc_now
from aureon_mcx.market.warmup import derive_h4, seed_pipeline, warmup_window
from aureon_mcx.observer import NullSink, PresentationSink, SymbolObserver
from aureon_mcx.outcomes import OutcomeService
from aureon_mcx.storage import Database, Repositories
from aureon_mcx.storage.parquet_archive import ParquetArchive

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


class Application:
    def __init__(self, config_dir: str | None = None, env_file: str | None = None, *, http_factory: Callable[..., Any] | None = None,
                 instrument_provider_factory: Callable[..., Any] | None = None, historical_factory: Callable[..., Any] | None = None,
                 feed_factory: Callable[..., Any] | None = None, sink_factory: Callable[..., Any] | None = None,
                 now: Callable[[], datetime] = utc_now, validate_credentials_remotely: bool = True,
                 housekeeping_interval: float = 30.0):
        self._config_dir = config_dir
        self._env_file = env_file
        self._http_factory = http_factory or (lambda cfg: DhanHttpClient(cfg.env.DHAN_CLIENT_ID.get_secret_value(), cfg.env.DHAN_ACCESS_TOKEN.get_secret_value()))
        self._instrument_provider_factory = instrument_provider_factory or (lambda cfg, http: DhanInstrumentProvider(
            cfg.symbols.instrument_master.url, cfg.symbols.instrument_master.cache_path, cfg.symbols.instrument_master.refresh_hours, http))
        self._historical_factory = historical_factory or (lambda cfg, http: DhanHistoricalProvider(http, cfg.analysis.historical.max_days_per_request))
        self._feed_factory = feed_factory
        self._sink_factory = sink_factory
        self._now = now
        self._validate_remote = validate_credentials_remotely
        self.housekeeping_interval = housekeeping_interval
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
        self.historical = None
        self.continuity: ContinuityService | None = None
        self.feed = None
        self.discord_client = None
        self.stop: asyncio.Event | None = None
        self.startup_log: list[str] = []
        self._tasks: dict[str, asyncio.Task] = {}
        self._specs: dict[str, TaskSpec] = {}
        self._loop: asyncio.AbstractEventLoop | None = None
        self.task_failures: list[tuple[str, str]] = []
        self._verify_inflight: dict[str, bool] = {}
        self._bg_tasks: set[asyncio.Task] = set()  # strong references: asyncio keeps only weak refs to tasks

    def _spawn(self, coro, name: str) -> asyncio.Task | None:
        if self._loop is None:
            coro.close()
            return None
        task = self._loop.create_task(coro, name=name)
        self._bg_tasks.add(task)
        task.add_done_callback(self._bg_tasks.discard)
        return task

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
        return next((r for r in self.runtimes.values() if r.contract.security_id == security_id), None)

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
        version = self.db.migrate()
        self.repos = Repositories(self.db)
        for c in contracts.values():
            self.repos.instruments.upsert_active(c.as_row())
        self._step(7, f"sqlite ready schema_version={version} path={cfg.env.AUREON_LOCAL_DB_PATH}")
        # 8 / 9 historical + warm (per symbol, via the same routine used by rollover)
        archive = ParquetArchive(cfg.env.AUREON_PARQUET_DIR, cfg.env.AUREON_PARQUET_ARCHIVE)
        self.historical = self._historical_factory(cfg, self.http)
        self._cached = CachedHistoricalProvider(self.historical, self.repos, archive)
        self.continuity = ContinuityService(self.historical, self.repos, self.calendar, self.health, self._now, cfg.primary_timeframe,
                                            allow_zero_trade_fill=cfg.analysis.historical.allow_verified_zero_trade_fill,
                                            retries=cfg.analysis.historical.verification_retries)
        for sym, c in contracts.items():
            self.runtimes[sym] = self._prepare_symbol(sym, c, "WARMING")
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

    def _prepare_symbol(self, sym: str, c: ResolvedContract, state: str) -> SymbolRuntime:
        """Load history, warm a fresh observer and seed a pipeline for a contract. Raises on failure."""
        assert self.cfg is not None and self.repos is not None and self.calendar is not None
        self.health.set_symbol_state(sym, state, f"preparing {c.security_id}", security_id=c.security_id, expiry=c.expiry_iso)
        hist = self._load_history(sym, c)
        outcomes = OutcomeService(self.repos, self.cfg.analysis, self.calendar)
        obs = SymbolObserver(self.cfg, c, self.repos, self.calendar, outcomes, self.sink, self.health)
        obs.warm(hist)
        rt = SymbolRuntime(contract=c, observer=obs, history=hist)
        rt.pipeline = self._make_pipeline(rt)
        return rt

    def _make_pipeline(self, rt: SymbolRuntime) -> CandlePipeline:
        assert self.cfg is not None
        sym = rt.contract.logical_symbol
        p = CandlePipeline(sym, rt.contract.security_id, rt.contract.expiry_iso, self.cfg.primary_timeframe, self.cfg.mtf_timeframes,
                           lambda candle, o=rt.observer: o.on_closed_candle(candle), calendar=self.calendar,
                           on_gap=lambda gap, s=sym: self._on_gap(s, gap), clock=self._now,
                           on_pending=lambda minute, reason, s=sym: self._on_pending(s, minute, reason))
        seed_pipeline(p, rt.history)
        return p

    def _build_pipelines(self) -> None:
        for rt in self.runtimes.values():
            if rt.pipeline is None:
                rt.pipeline = self._make_pipeline(rt)

    # ------------------------------------------------------------ feed hooks
    def _on_tick(self, tick: Tick) -> None:
        rt = self._runtime_for_security(tick.security_id)
        if rt is None or rt.pipeline is None:
            return
        self.health.tick_seen(rt.contract.logical_symbol, tick.ts)
        rt.pipeline.add_tick(tick)

    def _on_feed_status(self, status: str, detail: dict) -> None:
        self.health.set("feed", status, str(detail))
        now = self._now()
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
        if status == "connected":
            self._spawn(self.recover_all(), name="recovery")

    def _on_gap(self, sym: str, gap: GapRecord) -> None:
        rt = self.runtimes.get(sym)
        if rt is None or self.continuity is None:
            return
        self.continuity.record_gap(rt.contract, gap)
        self.health.set_symbol_state(sym, "ERROR", f"data gap {gap.timeframe.value} {gap.open_time.isoformat()} missing={len(gap.missing)}",
                                     unresolved_gaps=len(rt.pipeline.unresolved_gaps) if rt.pipeline else 1)
        self._spawn(self.repair(sym), name=f"repair:{sym}")

    def _on_pending(self, sym: str, minute: datetime, reason: str) -> None:
        """A minute without trustworthy live coverage: verify it against the broker, never invent it."""
        rt = self.runtimes.get(sym)
        if rt is None:
            return
        sh = self.health.symbol(sym)
        if sh.state in ("LIVE", "STALE"):
            state = "STALE" if reason == "silent_feed" else "RECOVERING_GAP"
            self.health.set_symbol_state(sym, state, f"minute {minute.isoformat()} needs broker verification ({reason})")
        if rt.pipeline is not None and not rt.pipeline.recovering:
            self._spawn(self.verify(sym), name=f"verify:{sym}")

    async def verify(self, sym: str) -> bool:
        """Verify pending minutes with exact broker M1 bars (bounded retries, then ERROR)."""
        rt = self.runtimes.get(sym)
        if rt is None or rt.pipeline is None or self.continuity is None:
            return False
        if self._verify_inflight.get(sym):
            return False
        self._verify_inflight[sym] = True
        try:
            p = rt.pipeline
            retries = self.cfg.analysis.historical.verification_retries if self.cfg else 3
            for attempt in range(1, retries + 1):
                if not [m for m in p.pending_minutes() if m + timedelta(minutes=1) <= self._now()]:
                    break
                try:
                    verified, still = await asyncio.to_thread(self.continuity.verify_pending, rt.contract, p)
                except Exception as exc:  # noqa: BLE001
                    log.exception("verification_error %s", kv(symbol=sym, error=type(exc).__name__))
                    verified, still = 0, len(p.pending_minutes())
                if not [m for m in p.pending_minutes() if m + timedelta(minutes=1) <= self._now()]:
                    break
                if attempt < retries:
                    try:
                        await asyncio.wait_for(self.stop.wait(), timeout=0.5 * self.task_backoff_scale)
                        return False
                    except asyncio.TimeoutError:
                        pass
            return self._settle_symbol_state(sym, True)
        finally:
            self._verify_inflight[sym] = False

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
        p.begin_recovery()
        try:
            fallback = rt.history.get(self.cfg.primary_timeframe, [])[-1].close_time if rt.history.get(self.cfg.primary_timeframe) else None
            ok = await asyncio.to_thread(self.continuity.recover, rt.contract, p, fallback)
        except Exception as exc:  # noqa: BLE001
            log.exception("recovery_error %s", kv(symbol=sym, error=type(exc).__name__))
            ok = False
        finally:
            p.end_recovery()
        return self._settle_symbol_state(sym, ok)

    async def repair(self, sym: str) -> bool:
        rt = self.runtimes.get(sym)
        if rt is None or rt.pipeline is None or self.continuity is None:
            return False
        try:
            await asyncio.to_thread(self.continuity.repair, rt.contract, rt.pipeline)
        except Exception as exc:  # noqa: BLE001
            log.exception("repair_error %s", kv(symbol=sym, error=type(exc).__name__))
        return self._settle_symbol_state(sym, True)

    def _settle_symbol_state(self, sym: str, recovered: bool) -> bool:
        rt = self.runtimes[sym]
        p = rt.pipeline
        assert p is not None
        gaps = len(p.unresolved_gaps)
        pending = p.pending_minutes()
        closed_pending = [m for m in pending if m + timedelta(minutes=1) <= self._now()]
        if recovered and p.continuity_ok:
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
        self.health.set_symbol_state(sym, "ERROR", detail, unresolved_gaps=gaps + len(closed_pending))
        self.sink.status(self.health.status_line())
        return False

    # -------------------------------------------------------------- live run
    async def run(self) -> None:
        assert self.cfg is not None and self.repos is not None
        cfg = self.cfg
        self.stop = asyncio.Event()
        self._loop = asyncio.get_running_loop()
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

            refs = MessageRefs(self.repos)
            self.sink = DiscordSink(UpdateCoalescer(refs, cfg.analysis.discord.debounce_seconds))
            self.discord_client = create_client(cfg, self.repos, self.sink, refs, MonitorRegistry(self.repos))
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
                                             cfg.env.EXCHANGE_SEGMENT, self._on_tick, self._on_feed_status)
        await self.feed.subscribe(ids)
        self._build_pipelines()
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
            log.info("discord live")
        else:
            self.health.set("discord", "ok", "headless")
        self._step(13, "discord started")
        # 14 health + housekeeping
        self.health.set("observer", "live")
        self.health.set("feed", "connecting")
        self.sink.status("\n".join([f"{s} -> security_id {c.security_id} -> expiry {c.expiry_iso}" for s, c in self.contracts.items()]
                                   + [self.health.status_line()]))
        self._specs["housekeeping"] = TaskSpec("housekeeping", self._housekeeping_loop, critical=True)
        self._start("housekeeping")
        self._step(14, "service health published")
        log.info("service_health %s", kv(**{k: v["status"] for k, v in self.health.snapshot()["components"].items()}))
        await self._supervise()
        await self.shutdown()

    # ----------------------------------------------------------- supervision
    def _start(self, name: str) -> None:
        spec = self._specs[name]
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
        exc = None if task.cancelled() else task.exception()
        if exc is None:
            if self.stop is not None and not self.stop.is_set() and spec is not None:
                log.warning("task_exited %s", kv(task=name))
            return
        self.task_failures.append((name, type(exc).__name__))
        log.error("task_failed %s", kv(task=name, error=type(exc).__name__, detail=str(exc)[:200]), exc_info=exc)
        if spec is None:
            return
        self.health.set(name, "error", f"{type(exc).__name__}: {str(exc)[:120]}")
        if spec.restarts >= spec.max_restarts:
            if spec.critical:
                log.critical("task_restart_limit %s", kv(task=name, restarts=spec.restarts))
                self.health.set("observer", "error", f"{name} failed repeatedly; shutting down")
                self.sink.status(self.health.status_line())
                assert self.stop is not None
                self.stop.set()
            return
        delay = spec.backoff[min(spec.restarts, len(spec.backoff) - 1)] * self.task_backoff_scale
        spec.restarts += 1
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

    # ------------------------------------------------------------------ loops
    async def _flush_loop(self) -> None:
        assert self.stop is not None
        grace = timedelta(seconds=2)
        while not self.stop.is_set():
            now = self._now() - grace
            for p in self.pipelines.values():
                p.flush_at(now)  # exceptions propagate to the supervisor (never swallowed)
            try:
                await asyncio.wait_for(self.stop.wait(), timeout=1.0)
            except asyncio.TimeoutError:
                pass

    def _stale_check(self) -> None:
        assert self.cfg is not None and self.calendar is not None
        now = self._now()
        if not self.calendar.is_open(now):
            return
        limit = timedelta(seconds=2 * self.cfg.primary_timeframe.seconds + 60)
        for sym, rt in self.runtimes.items():
            sh = self.health.symbol(sym)
            if sh.state != "LIVE" or not sh.feed_connected or rt.pipeline is None:
                continue
            last = rt.pipeline.last_closed.get(self.cfg.primary_timeframe)
            if last is not None and now - last > limit:
                self.health.set_symbol_state(sym, "STALE", f"no closed {self.cfg.primary_timeframe.value} since {last.isoformat()}")
                continue
            tick = sh.last_tick_at
            if tick is not None and now - tick > timedelta(seconds=90):
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
        try:
            new_rt = await asyncio.to_thread(self._prepare_symbol, sym, new, "ROLLOVER_WARMING")
        except (StartupError, DhanError, Exception) as exc:  # noqa: BLE001
            log.error("rollover_failed %s", kv(logical=sym, new_security_id=new.security_id, error=type(exc).__name__, detail=str(exc)[:200]))
            self.health.set_symbol_state(sym, "LIVE" if old_rt.pipeline is not None and old_rt.pipeline.continuity_ok else "ERROR",
                                         f"rollover to {new.security_id} failed: {type(exc).__name__}; old contract retained",
                                         security_id=old.security_id, expiry=old.expiry_iso)
            self.sink.status(f"{sym} rollover to security_id {new.security_id} FAILED; keeping {old.security_id}")
            return False
        ind = new_rt.observer.indicators[self.cfg.primary_timeframe].latest
        if ind is None or not ind.warmed:
            log.error("rollover_not_warm %s", kv(logical=sym, new_security_id=new.security_id))
            self.health.set_symbol_state(sym, "LIVE", f"rollover to {new.security_id} aborted: indicators not warm", security_id=old.security_id)
            return False
        # subscribe the new contract first, then switch atomically, then drop the old one
        if self.feed is not None:
            await self.feed.subscribe([new.security_id])
        self.repos.instruments.upsert_active(new.as_row())
        if self.feed is not None and getattr(self.feed, "connected", False):
            new_rt.pipeline.set_connected(self._now())
        self.runtimes[sym] = new_rt
        if self.feed is not None:
            await self.feed.unsubscribe([old.security_id])
        self.health.set_symbol_state(sym, "LIVE", f"rolled from {old.security_id}", security_id=new.security_id, expiry=new.expiry_iso,
                                     unresolved_gaps=0)
        log.warning("rollover_complete %s", kv(logical=sym, security_id=new.security_id, expiry=new.expiry_iso))
        self.sink.status(f"{sym} rolled -> security_id {new.security_id} -> expiry {new.expiry_iso}")
        if getattr(self.feed, "connected", False):
            self._spawn(self.recover(sym), name=f"recovery:{sym}")
        return True

    # --------------------------------------------------------------- shutdown
    async def shutdown(self) -> None:
        log.info("shutdown_begin")
        if self.stop is not None:
            self.stop.set()
        if self.feed is not None and hasattr(self.feed, "disconnect"):
            try:
                await self.feed.disconnect()
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
