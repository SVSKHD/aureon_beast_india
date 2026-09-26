"""StatusProjection: the one clean system report, built from in-memory runtime state.

Everything the dashboard needs is a projection of objects the application already keeps
up to date (health, agents, pipelines, scanner, incidents, crashes, metrics, calendar).
Nothing here runs a historical query; the few database counts (setups by state, outcome
backlog) are refreshed periodically by the application and cached. Secrets never appear.
"""
from __future__ import annotations

from datetime import datetime, timedelta
from typing import TYPE_CHECKING, Any

from aureon_mcx.market.timeutil import ensure_utc
from aureon_mcx.version import version_info

if TYPE_CHECKING:  # pragma: no cover
    from aureon_mcx.app import Application


class StatusProjection:
    def __init__(self, app: "Application"):
        self.app = app

    # ------------------------------------------------------------- helpers
    def _now(self) -> datetime:
        return ensure_utc(self.app._now())

    def overall(self) -> str:
        app = self.app
        agents = app.agents.overall()
        if agents in ("FAILED", "ERROR"):
            return agents
        symbols = [s.state for s in app.health.symbols.values()]
        if any(s == "ERROR" for s in symbols) or app.crashes.unresolved():
            return "DEGRADED" if agents != "FAILED" else agents
        if agents == "DEGRADED" or any(s not in ("LIVE",) for s in symbols) and self.market()["state"] == "OPEN":
            return "DEGRADED"
        return "HEALTHY" if agents in ("HEALTHY", "STOPPED") else agents

    # ------------------------------------------------------------ sections
    def version(self) -> dict[str, Any]:
        vi = version_info()
        now = self._now()
        return {"app": vi["app"], "name": vi["name"], "git_sha": vi["git_sha"], "git_short": vi["git_short"], "branch": vi["branch"],
                "build": vi["build"], "started_at": ensure_utc(self.app.started_at).isoformat(),
                "uptime_seconds": int(max(0.0, (now - ensure_utc(self.app.started_at)).total_seconds()))}

    def market(self) -> dict[str, Any]:
        cal = self.app.calendar
        if cal is None:
            return {"exchange": "MCX", "state": "UNKNOWN", "reason": "calendar not loaded"}
        ms = cal.market_state(self._now())
        d = ms.to_dict()
        d.update({"exchange": cal.config.calendar.exchange if cal.config.calendar else "MCX", "calendar_years": cal.years,
                  "opens_at": d.pop("opens_at"), "closes_at": d.pop("closes_at"), "day": cal.day_info(ms.trading_date)})
        return d

    def agents(self) -> list[dict[str, Any]]:
        return self.app.agents.snapshot()

    def agent(self, agent_id: str) -> dict[str, Any] | None:
        a = self.app.agents.agents.get(agent_id)
        return a.to_dict() if a else None

    def symbols(self) -> list[dict[str, Any]]:
        return [self.symbol(sym) for sym in sorted(self.app.runtimes)]

    def symbol(self, sym: str) -> dict[str, Any] | None:
        app = self.app
        rt = app.runtimes.get(sym)
        sh = app.health.symbols.get(sym)
        if rt is None or sh is None:
            return None
        now = self._now()
        p = rt.pipeline
        quote = app.scanner.quotes.get(rt.contract.security_id) if app.scanner is not None else None
        last_price = quote.ltp if quote and quote.ltp is not None else (p.m1._last_close if p is not None else None)
        pending = p.pending_minutes() if p else []
        incidents = app.continuity.incidents.open_for(rt.contract.security_id) if app.continuity else []
        cur_trust = p.minute_trust(now) if p is not None else None
        return {
            "symbol": sym, "security_id": rt.contract.security_id, "display_symbol": rt.contract.display_symbol, "expiry": rt.contract.expiry_iso,
            "state": sh.state, "detail": sh.detail, "analysis_enabled": sh.state == "LIVE", "last_price": last_price,
            "previous_close": quote.previous_close if quote else None, "reference_status": quote.reference_status if quote else None,
            "change": quote.change if quote else None, "change_pct": quote.change_pct if quote else None,
            "last_tick_at": sh.last_tick_at.isoformat() if sh.last_tick_at else None,
            "lag_seconds": int((now - ensure_utc(sh.last_tick_at)).total_seconds()) if sh.last_tick_at else None,
            "feed_connected": sh.feed_connected, "feed_stalled": bool(p.feed_stale) if p else None, "reconnects": sh.reconnects,
            "feed_generation": p.m1.feed_generation if p else None, "pending_verification": len(pending),
            "pending_minutes": [m.isoformat() for m in pending[:20]], "unresolved_gaps": len(p.unresolved_gaps) if p else sh.unresolved_gaps,
            "gaps": [{"timeframe": g.timeframe.value, "open_time": g.open_time.isoformat(), "expected": g.expected, "present": g.present}
                     for g in (p.unresolved_gaps if p else [])][:20],
            "open_incidents": [i.to_dict() for i in incidents][:20], "current_minute_trust": cur_trust.to_dict() if cur_trust else None,
            "last_closed": {tf: t.isoformat() for tf, t in sorted(sh.last_closed.items())},
            "closed_counts": {tf.value: n for tf, n in (p.closed_counts.items() if p else [])}, "updated_at": sh.updated_at.isoformat(),
        }

    def scanner(self) -> dict[str, Any]:
        sc = self.app.scanner
        if sc is None:
            return {"enabled": False, "universe": 0, "advancers": 0, "decliners": 0, "unchanged": 0, "stale": 0, "unavailable": 0, "winners": [], "losers": []}
        now = self._now()
        board = sc.leaderboard(now)
        st = sc.status(now)
        return {**st, "market_open": board["market_open"], "as_of": board["as_of"], "winners": board["winners"], "losers": board["losers"]}

    def breadth(self) -> dict[str, Any]:
        sc = self.app.scanner
        return sc.breadth(self._now()) if sc is not None else {"overall": None, "by_segment": {}, "by_category": {}, "by_instrument_type": {}}

    def continuity(self) -> dict[str, Any]:
        app = self.app
        pending = sum(len(p.pending_minutes()) for p in app.pipelines.values())
        gaps = sum(len(p.unresolved_gaps) for p in app.pipelines.values())
        inc = app.continuity.incidents.snapshot() if app.continuity else {"open": [], "recent_closed": []}
        return {"pending_minutes": pending, "unresolved_gaps": gaps, "open_incidents": len(inc["open"]), "incidents": inc["open"][:50],
                "recently_closed_incidents": inc["recent_closed"][-10:], "feed_stalled": app._feed_stalled,
                "last_packet_at": app._last_packet_at.isoformat() if app._last_packet_at else None}

    def crashes(self) -> dict[str, Any]:
        snap = self.app.crashes.snapshot()
        since = self._now() - timedelta(hours=24)
        snap["last_24h"] = self.app.crashes.count_since(since)
        return snap

    def discord(self) -> dict[str, Any]:
        a = self.app.agents.agents.get("discord_agent")
        comp = self.app.health.components.get("discord")
        ts = self.app.metrics.snapshot()["timestamps"]
        return {"state": a.state if a else "STOPPED", "component": comp.status if comp else None, "detail": comp.detail if comp else None,
                "last_message_at": ts.get("discord_last_message"), "messages": self.app.metrics.get("discord_messages"),
                "failures": self.app.metrics.get("discord_failures")}

    def metrics(self) -> dict[str, Any]:
        app = self.app
        m = app.metrics.snapshot()
        counters = dict(m["counters"])
        counters["pending_m1"] = sum(len(p.pending_minutes()) for p in app.pipelines.values())
        counters["aggregate_gaps"] = sum(len(p.unresolved_gaps) for p in app.pipelines.values())
        counters["observer_candles_processed"] = sum(rt.observer.closed_count for rt in app.runtimes.values())
        counters["restart_count"] = sum(spec.restarts for spec in app._specs.values())
        counters["crash_count"] = len(app.crashes.recent)
        feed = app.feed
        gauges = dict(m["gauges"])
        gauges["packets_received"] = int(getattr(feed, "packets", 0) or 0) + sum(int(getattr(f, "packets", 0) or 0) for f in app.scanner_feeds)
        gauges["packets_rejected"] = int(getattr(feed, "packets_rejected", 0) or 0) + sum(int(getattr(f, "packets_rejected", 0) or 0) for f in app.scanner_feeds)
        gauges["last_packet_at"] = app._last_packet_at.isoformat() if app._last_packet_at else None
        gauges["tick_rate_per_minute"] = app.tick_rate()
        gauges["setup_counts_by_state"] = dict(app.db_stats.get("setups_by_state", {}))
        gauges["outcome_backlog"] = app.db_stats.get("outcome_backlog")
        gauges["scanner_universe"] = app.scanner.universe_size if app.scanner else 0
        return {"counters": counters, "gauges": gauges, "timestamps": m["timestamps"], "as_of": self._now().isoformat()}

    def health(self) -> dict[str, Any]:
        app = self.app
        symbols = app.health.symbols
        trusted = [s for s in symbols.values() if s.trusted]
        market = self.market()
        ready = bool(symbols) and (len(trusted) == len(symbols) or market["state"] != "OPEN") and app.agents.overall() not in ("FAILED", "ERROR")
        return {"alive": True, "ready": ready, "status": self.overall(), "market": market["state"], "symbols": len(symbols), "trusted": len(trusted),
                "version": version_info()["app"], "git_sha": version_info()["git_short"], "as_of": self._now().isoformat()}

    def calendar(self) -> dict[str, Any]:
        cal = self.app.calendar
        if cal is None:
            return {}
        d = cal.describe()
        d["today"] = cal.day_info(cal.trading_date(self._now()))
        d["market"] = self.market()
        return d

    # ------------------------------------------------------------- report
    def status(self) -> dict[str, Any]:
        sc = self.scanner()
        cont = self.continuity()
        return {
            "status": self.overall(), "version": self.version(), "market": self.market(), "agents": self.agents(), "symbols": self.symbols(),
            "scanner": {"enabled": sc.get("enabled", False), "universe": sc.get("universe", 0), "advancers": sc.get("advancers", 0),
                        "decliners": sc.get("decliners", 0), "unchanged": sc.get("unchanged", 0), "stale": sc.get("stale", 0),
                        "unavailable": sc.get("unavailable", 0), "winners": sc.get("winners", [])[:10], "losers": sc.get("losers", [])[:10],
                        "as_of": sc.get("as_of")},
            "continuity": {"pending_minutes": cont["pending_minutes"], "unresolved_gaps": cont["unresolved_gaps"],
                           "open_incidents": cont["open_incidents"], "feed_stalled": cont["feed_stalled"]},
            "crashes": self.crashes(), "discord": self.discord(), "as_of": self._now().isoformat(),
        }
