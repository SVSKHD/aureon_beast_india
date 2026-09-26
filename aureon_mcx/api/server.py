"""FastAPI application + supervised Uvicorn server running under the observer's event loop.

READ-ONLY operational visibility. There is no execution endpoint of any kind. Handlers read
in-memory projections; the handful that touch the database run in the threadpool (plain
`def`), so nothing heavy ever runs on the event loop. Secrets are never serialised.
"""
from __future__ import annotations

import asyncio
import logging
from datetime import datetime
from typing import TYPE_CHECKING, Any

from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.responses import JSONResponse

from aureon_mcx.events import EventType
from aureon_mcx.logging_setup import kv
from aureon_mcx.market.timeutil import from_db

from .status import StatusProjection

if TYPE_CHECKING:  # pragma: no cover
    from aureon_mcx.app import Application

log = logging.getLogger("aureon.api")

FORBIDDEN_KEYS = ("token", "secret", "password", "authorization", "client_id", "access_token")


def _row(r) -> dict[str, Any]:
    return {k: r[k] for k in r.keys()}


def create_api(app: "Application") -> FastAPI:
    proj = StatusProjection(app)
    api = FastAPI(title="Aureon MCX status", version=proj.version()["app"], docs_url="/api/docs", redoc_url=None,
                  description="Read-only operational status of the Aureon MCX observer. No trading, no execution.")

    @api.middleware("http")
    async def count_requests(request: Request, call_next):
        app.metrics.inc("api_requests")
        app.agents.heartbeat("api_agent", work=1)
        return await call_next(request)

    @api.get("/api/v1/health")
    async def health():
        return proj.health()

    @api.get("/api/v1/status")
    async def status():
        return proj.status()

    @api.get("/api/v1/agents")
    async def agents():
        return {"overall": app.agents.overall(), "agents": proj.agents()}

    @api.get("/api/v1/agents/{agent_id}")
    async def agent(agent_id: str):
        a = proj.agent(agent_id)
        if a is None:
            raise HTTPException(status_code=404, detail=f"unknown agent {agent_id}")
        return a

    @api.get("/api/v1/symbols")
    async def symbols():
        return {"symbols": proj.symbols()}

    @api.get("/api/v1/symbols/{symbol}")
    async def symbol(symbol: str):
        s = proj.symbol(symbol.upper())
        if s is None:
            raise HTTPException(status_code=404, detail=f"unknown symbol {symbol}")
        return s

    @api.get("/api/v1/market")
    async def market():
        return proj.market()

    @api.get("/api/v1/market/winners")
    async def winners(limit: int = Query(default=20, ge=1, le=100)):
        sc = proj.scanner()
        return {"as_of": sc.get("as_of"), "market_open": sc.get("market_open"), "universe": sc.get("universe", 0), "winners": sc.get("winners", [])[:limit]}

    @api.get("/api/v1/market/losers")
    async def losers(limit: int = Query(default=20, ge=1, le=100)):
        sc = proj.scanner()
        return {"as_of": sc.get("as_of"), "market_open": sc.get("market_open"), "universe": sc.get("universe", 0), "losers": sc.get("losers", [])[:limit]}

    @api.get("/api/v1/market/breadth")
    async def breadth():
        return proj.breadth()

    @api.get("/api/v1/market/instruments")
    async def instruments(segment: str | None = None, limit: int = Query(default=500, ge=1, le=5000)):
        sc = app.scanner
        if sc is None:
            return {"instruments": []}
        rows = sc.quote_dicts(proj._now())
        if segment:
            rows = [r for r in rows if r["segment"] == segment.upper()]
        return {"count": len(rows), "instruments": rows[:limit]}

    @api.get("/api/v1/crashes")
    def crashes(limit: int = Query(default=50, ge=1, le=500), agent: str | None = None, resolved: bool | None = None, trace: bool = False):
        out = [r.to_dict(include_trace=trace) for r in reversed(app.crashes.recent)
               if (agent is None or r.agent == agent) and (resolved is None or (r.resolved_at is not None) == resolved)][:limit]
        if app.repos is not None and len(out) < limit:  # earlier runs live only in the database (threadpool)
            seen = {r["crash_id"] for r in out}
            for r in app.repos.crashes.recent(limit, agent=agent, resolved=resolved):
                if r["crash_id"] in seen:
                    continue
                d = _row(r)
                if not trace:
                    d["stack_trace"] = None
                out.append(d)
                if len(out) >= limit:
                    break
        return {"unresolved": len(app.crashes.unresolved()), "crashes": out}

    @api.get("/api/v1/events")
    def events(limit: int = Query(default=100, ge=1, le=1000), type: str | None = None):
        type_ = None
        if type:
            try:
                type_ = EventType(type.upper())
            except ValueError as exc:
                raise HTTPException(status_code=400, detail=f"unknown event type {type}") from exc
        out = [e.to_dict() for e in app.events.recent(limit, type_)]
        if app.repos is not None and len(out) < limit:
            seen = {(e["type"], e["created_at"], e["message"]) for e in out}
            for r in app.repos.events.recent(limit, type_.value if type_ else None):
                key = (r["event_type"], r["created_at"], r["message"])
                if key in seen:
                    continue
                d = _row(r)
                d["type"] = d.pop("event_type")
                out.append(d)
                if len(out) >= limit:
                    break
        return {"count": len(out), "events": out}

    @api.get("/api/v1/gaps")
    def gaps(limit: int = Query(default=100, ge=1, le=1000)):
        live = []
        for sid, p in app.pipelines.items():
            for g in p.gaps[-limit:]:
                live.append({"security_id": sid, "symbol": p.symbol, "timeframe": g.timeframe.value, "open_time": g.open_time.isoformat(),
                             "expected": g.expected, "present": g.present, "missing": [m.isoformat() for m in g.missing[:20]],
                             "detected_at": g.detected_at.isoformat(), "resolved_at": g.resolved_at.isoformat() if g.resolved_at else None})
        stored = [_row(r) for r in app.repos.gaps.unresolved()] if app.repos is not None else []
        incidents = app.continuity.incidents.snapshot() if app.continuity else {"open": [], "recent_closed": []}
        return {"unresolved": sum(1 for g in live if g["resolved_at"] is None), "gaps": live[-limit:], "stored_unresolved": stored[:limit],
                "incidents": incidents}

    @api.get("/api/v1/calendar")
    async def calendar():
        return proj.calendar()

    @api.get("/api/v1/metrics")
    async def metrics():
        return proj.metrics()

    @api.get("/api/v1/continuity")
    async def continuity():
        return proj.continuity()

    @api.exception_handler(Exception)
    async def unhandled(request: Request, exc: Exception):
        log.exception("api_error %s", kv(path=request.url.path, error=type(exc).__name__))
        return JSONResponse(status_code=500, content={"error": type(exc).__name__, "detail": "internal error (see logs)"})

    return api


class ApiServer:
    """Uvicorn as a supervised task under the observer's loop (single process, never workers)."""

    def __init__(self, app: "Application", host: str = "0.0.0.0", port: int = 1250):
        self.app = app
        self.host = host
        self.port = port
        self.fastapi = create_api(app)
        self.server = None
        self.started_at: datetime | None = None

    async def run(self, stop: asyncio.Event) -> None:
        import uvicorn

        config = uvicorn.Config(self.fastapi, host=self.host, port=self.port, log_level="warning", access_log=False, lifespan="off")
        server = uvicorn.Server(config)
        server.install_signal_handlers = lambda: None  # the application owns SIGINT / SIGTERM
        self.server = server

        async def _serve() -> None:
            try:
                await server.serve()
            except SystemExit as exc:  # uvicorn calls sys.exit on bind / startup failure: never let it stop the observer loop
                raise RuntimeError(f"uvicorn exited with code {exc.code} (bind or startup failure on {self.host}:{self.port})") from exc

        serve = asyncio.create_task(_serve(), name="api-serve")
        stop_task = asyncio.create_task(stop.wait(), name="api-stop")
        try:
            while True:
                done, _ = await asyncio.wait({serve, stop_task}, timeout=0.25, return_when=asyncio.FIRST_COMPLETED)
                if serve in done:
                    serve.result()  # bind failure etc. surfaces to the supervisor (restart with backoff)
                    if not stop.is_set():
                        raise RuntimeError(f"uvicorn server on {self.host}:{self.port} exited unexpectedly")
                    return
                if stop_task in done:
                    return
                if server.started and self.started_at is None:
                    self.started_at = self.app._now()
                    self.app.agents.start("api_agent", host=self.host, port=self.port)
                    self.app.events.emit(EventType.API_STARTED, f"status API listening on {self.host}:{self.port}", agent="api_agent",
                                         dedupe_key=f"api:started:{self.port}", host=self.host, port=self.port)
        finally:
            stop_task.cancel()
            server.should_exit = True
            try:
                await asyncio.wait_for(serve, timeout=5.0)
            except (asyncio.TimeoutError, asyncio.CancelledError, Exception):  # noqa: BLE001
                serve.cancel()
