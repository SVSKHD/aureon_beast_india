"""Application: the ordered startup flow and the live run loop.

Startup (any failure in steps 1-9 aborts with a clear error):
 1 load config            2 validate Dhan credentials (never printed)
 3 instrument master      4/5 resolve GOLD / SILVER
 6 publish instrument metadata (log + instruments table + Discord status)
 7 SQLite WAL + migrations 8 historical M5 / M15 / H1 (H4 aggregated locally)
 9 warm indicators       10 connect WebSocket   11 subscribe   12 observer
13 Discord               14 service health
"""
from __future__ import annotations

import asyncio
import logging
import signal
import sys
from datetime import datetime, timedelta
from typing import Any, Callable

from aureon_mcx.broker.dhan import DhanApiError, DhanError, DhanInstrumentProvider, ResolvedContract, SymbolResolutionError, SymbolResolver
from aureon_mcx.broker.dhan.client import DhanHttpClient
from aureon_mcx.broker.dhan.errors import DhanCredentialsError
from aureon_mcx.broker.dhan.historical import CachedHistoricalProvider, DhanHistoricalProvider
from aureon_mcx.broker.dhan.live_feed import DhanLiveFeedProvider
from aureon_mcx.config import AppConfig, ConfigError, load_config
from aureon_mcx.health import HealthState
from aureon_mcx.logging_setup import configure_logging, kv
from aureon_mcx.market.candle import Candle
from aureon_mcx.market.candle_builder import CandlePipeline, Tick
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


class Application:
    def __init__(self, config_dir: str | None = None, env_file: str | None = None, *, http_factory: Callable[..., Any] | None = None,
                 instrument_provider_factory: Callable[..., Any] | None = None, historical_factory: Callable[..., Any] | None = None,
                 feed_factory: Callable[..., Any] | None = None, sink_factory: Callable[..., Any] | None = None,
                 now: Callable[[], datetime] = utc_now, validate_credentials_remotely: bool = True):
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
        self.cfg: AppConfig | None = None
        self.http = None
        self.resolver: SymbolResolver | None = None
        self.contracts: dict[str, ResolvedContract] = {}
        self.db: Database | None = None
        self.repos: Repositories | None = None
        self.calendar: SessionCalendar | None = None
        self.health = HealthState()
        self.sink: PresentationSink = NullSink()
        self.observers: dict[str, SymbolObserver] = {}
        self.pipelines: dict[str, CandlePipeline] = {}
        self.history: dict[str, dict[Timeframe, list[Candle]]] = {}
        self.feed = None
        self.discord_client = None
        self.stop: asyncio.Event | None = None  # created inside run()
        self.startup_log: list[str] = []

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
        for i, sym in enumerate(cfg.logical_symbols, start=4):
            self.contracts[sym] = self.resolver.resolve(sym)  # raises SymbolResolutionError -> abort
            self._step(min(i, 5), f"resolved {sym}")
        # 6 publish metadata (log now; DB row + Discord line once storage / Discord exist)
        for sym, c in self.contracts.items():
            log.info("symbol_resolved %s", kv(logical=sym, security_id=c.security_id, expiry=c.expiry_iso, display=c.display_symbol,
                                              lot_size=c.lot_size, tick_size=c.tick_size))
        self._step(6, "instrument metadata published")
        # 7 storage
        self.db = Database(cfg.env.AUREON_LOCAL_DB_PATH if cfg.env.AUREON_STORAGE_BACKEND == "sqlite" else cfg.env.AUREON_LOCAL_DB_PATH)
        if cfg.env.AUREON_STORAGE_BACKEND != "sqlite":
            # DECISION: PostgreSQL backend is selectable in config but not implemented; fail closed.
            raise StartupError(f"storage backend {cfg.env.AUREON_STORAGE_BACKEND!r} is not implemented in this build")
        version = self.db.migrate()
        self.repos = Repositories(self.db)
        for c in self.contracts.values():
            self.repos.instruments.upsert_active(c.as_row())
        self._step(7, f"sqlite ready schema_version={version} path={cfg.env.AUREON_LOCAL_DB_PATH}")
        # 8 historical
        archive = ParquetArchive(cfg.env.AUREON_PARQUET_DIR, cfg.env.AUREON_PARQUET_ARCHIVE)
        cached = CachedHistoricalProvider(self._historical_factory(cfg, self.http), self.repos, archive)
        now = self._now()
        for sym, c in self.contracts.items():
            hist: dict[Timeframe, list[Candle]] = {}
            for tf in cfg.mtf_timeframes:
                if tf == Timeframe.H4:
                    continue
                bars = cfg.analysis.historical.warmup_bars.get(tf, 200)
                start, end = warmup_window(bars, tf, now)
                candles = cached.load(sym, c.security_id, c.exchange_segment, c.instrument_type, c.expiry_iso, tf, start, end)
                hist[tf] = candles[-bars:]
                log.info("historical_warmup %s %s bars=%d", sym, tf.value, len(hist[tf]))
            if Timeframe.H4 in cfg.mtf_timeframes:
                hist[Timeframe.H4] = derive_h4(hist.get(Timeframe.H1, []), now=now)
                log.info("historical_warmup %s H4 bars=%d (aggregated from closed H1)", sym, len(hist[Timeframe.H4]))
            if len(hist.get(cfg.primary_timeframe, [])) < cfg.env.EMA_SLOW + 5:
                raise StartupError(f"insufficient {cfg.primary_timeframe.value} history for {sym}: {len(hist.get(cfg.primary_timeframe, []))} bars")
            self.history[sym] = hist
        self._step(8, "historical context loaded")
        # 9 warm indicators (observers)
        outcomes = OutcomeService(self.repos, cfg.analysis, self.calendar)
        for sym, c in self.contracts.items():
            obs = SymbolObserver(cfg, c, self.repos, self.calendar, outcomes, self.sink, self.health)
            obs.warm(self.history[sym])
            self.observers[sym] = obs
        self._step(9, "indicators warmed")
        self.health.set("config", "ok")
        self.health.set("storage", "ok", cfg.env.AUREON_LOCAL_DB_PATH)
        self.health.set("resolver", "ok", ", ".join(f"{s}={c.security_id}" for s, c in self.contracts.items()))

    # -------------------------------------------------------------- live run
    def _on_tick(self, tick: Tick) -> None:
        p = self.pipelines.get(tick.security_id)
        if p is not None:
            p.add_tick(tick)

    def _build_pipelines(self) -> None:
        assert self.cfg is not None
        for sym, c in self.contracts.items():
            obs = self.observers[sym]
            p = CandlePipeline(sym, c.security_id, c.expiry_iso, self.cfg.primary_timeframe, self.cfg.mtf_timeframes,
                               lambda candle, o=obs: o.on_closed_candle(candle))
            seed_pipeline(p, self.history[sym])
            self.pipelines[c.security_id] = p

    async def run(self) -> None:
        assert self.cfg is not None and self.repos is not None
        cfg = self.cfg
        self.stop = asyncio.Event()
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                loop.add_signal_handler(sig, self.stop.set)
            except (NotImplementedError, RuntimeError):  # pragma: no cover - Windows / nested loops
                pass
        # 13 (sink first so status lines have a destination) - Discord is presentation only
        tasks: list[asyncio.Task] = []
        if self._sink_factory is not None:
            self.sink, self.discord_client = self._sink_factory(cfg, self.repos)
        elif cfg.env.DISCORD_TOKEN and cfg.env.DISCORD_CHANNEL_ID:
            from aureon_mcx.discord.bot import DiscordSink, MonitorRegistry, create_client
            from aureon_mcx.discord.coalescer import UpdateCoalescer
            from aureon_mcx.discord.message_refs import MessageRefs

            refs = MessageRefs(self.repos)
            self.sink = DiscordSink(UpdateCoalescer(refs, cfg.analysis.discord.debounce_seconds))
            self.discord_client = create_client(cfg, self.repos, self.sink, refs, MonitorRegistry())
        else:
            log.warning("discord_disabled %s", kv(reason="DISCORD_TOKEN / DISCORD_CHANNEL_ID not set; running headless"))
            self.sink = NullSink()
        for obs in self.observers.values():
            obs.sink = self.sink
        # 10 / 11 websocket + subscriptions
        ids = [c.security_id for c in self.contracts.values()]
        if self._feed_factory is not None:
            self.feed = self._feed_factory(cfg, self._on_tick, self.health)
        else:
            self.feed = DhanLiveFeedProvider(cfg.env.DHAN_CLIENT_ID.get_secret_value(), cfg.env.DHAN_ACCESS_TOKEN.get_secret_value(),
                                             cfg.env.EXCHANGE_SEGMENT, self._on_tick, lambda s, d: self.health.set("feed", s, str(d)))
        await self.feed.subscribe(ids)
        self._build_pipelines()
        tasks.append(asyncio.create_task(self.feed.run(self.stop), name="feed"))
        self._step(10, "websocket connecting")
        self._step(11, f"subscribed {','.join(ids)}")
        # 12 observer: wall-clock boundary flush
        tasks.append(asyncio.create_task(self._flush_loop(), name="flush"))
        self._step(12, "observer live")
        log.info("observer live")
        if self.discord_client is not None:
            tasks.append(asyncio.create_task(self.discord_client.start(cfg.env.DISCORD_TOKEN.get_secret_value()), name="discord"))
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
        tasks.append(asyncio.create_task(self._housekeeping_loop(), name="housekeeping"))
        self._step(14, "service health published")
        log.info("service_health %s", kv(**{k: v["status"] for k, v in self.health.snapshot()["components"].items()}))
        await self.stop.wait()
        await self.shutdown(tasks)

    async def _flush_loop(self) -> None:
        grace = timedelta(seconds=2)
        while not self.stop.is_set():
            now = self._now() - grace
            for p in self.pipelines.values():
                try:
                    p.flush_at(now)
                except Exception as exc:  # noqa: BLE001
                    log.exception("flush_error %s", kv(error=type(exc).__name__))
            try:
                await asyncio.wait_for(self.stop.wait(), timeout=1.0)
            except asyncio.TimeoutError:
                pass

    async def _housekeeping_loop(self) -> None:
        assert self.cfg is not None
        last_master = self._now()
        while not self.stop.is_set():
            try:
                await asyncio.wait_for(self.stop.wait(), timeout=300.0)
            except asyncio.TimeoutError:
                pass
            if self.stop.is_set():
                break
            self.health.set("feed", "live" if getattr(self.feed, "connected", False) else "reconnecting")
            self.sink.status(self.health.status_line())
            log.info("service_health %s", kv(line=self.health.status_line()))
            if self._now() - last_master >= timedelta(hours=self.cfg.symbols.instrument_master.refresh_hours):
                last_master = self._now()
                await self.check_rollover()

    async def check_rollover(self) -> None:
        """Daily: refresh the instrument master and swap subscriptions when a contract rolled."""
        assert self.resolver is not None and self.cfg is not None and self.repos is not None
        try:
            self.resolver.refresh(force=True)
        except DhanError as exc:
            log.warning("instrument_master_refresh_failed %s", kv(error=str(exc)))
            return
        for sym in list(self.contracts):
            try:
                new = self.resolver.resolve(sym)
            except SymbolResolutionError as exc:
                log.error("rollover_resolution_failed %s", kv(logical=sym, reason=exc.reason))
                continue
            old = self.contracts[sym]
            if new.security_id == old.security_id:
                continue
            log.warning("rollover %s", kv(logical=sym, old_security_id=old.security_id, new_security_id=new.security_id, expiry=new.expiry_iso))
            self.repos.instruments.upsert_active(new.as_row())
            self.contracts[sym] = new
            outcomes = OutcomeService(self.repos, self.cfg.analysis, self.calendar)
            obs = SymbolObserver(self.cfg, new, self.repos, self.calendar, outcomes, self.sink, self.health)
            self.observers[sym] = obs
            self.history[sym] = {tf: [] for tf in self.cfg.mtf_timeframes}
            p = CandlePipeline(sym, new.security_id, new.expiry_iso, self.cfg.primary_timeframe, self.cfg.mtf_timeframes,
                               lambda candle, o=obs: o.on_closed_candle(candle))
            self.pipelines.pop(old.security_id, None)
            self.pipelines[new.security_id] = p
            if self.feed is not None:
                await self.feed.replace_subscription(old.security_id, new.security_id)
            self.sink.status(f"{sym} rolled -> security_id {new.security_id} -> expiry {new.expiry_iso}")

    async def shutdown(self, tasks: list[asyncio.Task]) -> None:
        log.info("shutdown_begin")
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
        for t in tasks:
            t.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
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
