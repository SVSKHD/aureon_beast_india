# Aureon MCX — DhanHQ GOLD / SILVER research observer

Research-first market-structure observer for **MCX GOLD and MCX SILVER futures** on
**DhanHQ API v2**. It is the MCX / Dhan sibling of the Aureon (MT5 / XAUUSD) system and
inherits its layering and safety principles.

> **This system never places orders.** It observes closed candles, stores every
> observation, and presents a calm, auditable view in Discord so a human can decide.
> `✅ CLEARED FOR REVIEW` means only that the configured research evidence agrees.
> It is not a trading recommendation.

## Non-negotiable principles

1. Market data and the indicator engine determine observations. Nothing else does.
2. Discord is presentation only. It renders stored rows; it never recomputes strategy state.
3. No automatic order placement. No order-placement code path exists in this build.
4. Raw bullish / bearish detections are never presented as tradeable (`REACTION DETECTED`, never `BUY`).
5. Ambiguous or missing evidence fails closed into `⛔ NOT CLEARED` / `WATCH`.
6. Every analytic row carries `security_id`, contract expiry, timeframe and the closed-candle `open_time`.
7. No future leakage: pivots confirm only after later closed candles; outcomes use only later closed candles.
8. Analysis runs only on closed candles, once per closed candle per timeframe. Never per tick.
9. The chart reads stored candles / indicators / detections / pivots. It cannot derive a signal.
10. Credentials come from the environment only, are `SecretStr`, and a log filter redacts them.

```
DHAN MARKET DATA -> CANDLE ENGINE -> INDICATORS -> DETECTION ENGINE
  -> STRUCTURE + MTF CONTEXT -> SETUP LIFECYCLE -> CONFIRMATION / CLEARANCE
  -> STORAGE -> DISCORD READ-ONLY PRESENTATION -> HUMAN DECISION
```

## Layout

```
main_aureon.py                 entry point
aureon_mcx/
  app.py                       ordered startup flow, live loop, rollover, shutdown
  observer.py                  closed-candle observer (pipeline orchestration)
  views.py                     SetupView (what Discord renders) + material state hash
  logging_setup.py             key=value logging, token-redacting filter
  config/                      env (.env) + YAML models + loader (pydantic, fail closed)
  broker/dhan/                 http client, instrument master, SymbolResolver, historical, live feed
  market/                      Candle, Timeframe, aggregation, tick->M1->M5 builder, sessions, warmup
  indicators/                  EMA20/EMA50/RSI14/ATR14 engine (persisted per closed candle)
  detection/                   EMA early/cross, wick, levels, liquidity, breakout, RSI events
  structure/                   confirmed pivots, HH/HL/LH/LL/EH/EL/SH/SL, dominance, sequence
  mtf/                         per-timeframe direction, MTF alignment, present + session trend
  setups/                      lifecycle state machine (explicit allowed transitions)
  confirmation/                INITIAL STRICT RESEARCH POLICY, blockers, fakeout flags
  outcomes/                    immutable snapshots, MFE/MAE horizons, labels, cohort, model stubs
  storage/                     SQLite (WAL/FK/busy_timeout), migrations, repositories, parquet
  discord/                     card builder, chart planner/renderer, coalescer, message refs, bot
  health/                      service health snapshot / status line
config/
  symbols.yaml sessions.yaml confirmation_policy.yaml analysis.yaml
tests/                         pytest suite (recorded fixtures, no network)
.env.example
```

## Setup

```bash
python -m venv .venv && . .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env            # fill DHAN_CLIENT_ID / DHAN_ACCESS_TOKEN / DISCORD_*
pytest                          # 240+ tests, no network
python main_aureon.py           # optional: --config-dir config --env-file .env
python -m aureon_mcx.tools.audit_database --db data/aureon_mcx.db --json   # read-only integrity audit
```

The process serves a read-only status API on `0.0.0.0:1250` (`AUREON_API_HOST` / `AUREON_API_PORT`,
`AUREON_API_ENABLED=false` disables it). `docker compose up` or the `Jenkinsfile` run the same
single process in a container with persistent volumes (see *Deployment* below).

`.env` keys (see `.env.example`): `BROKER`, `DHAN_CLIENT_ID`, `DHAN_ACCESS_TOKEN`, `SYMBOLS`,
`EXCHANGE_SEGMENT`, `PRIMARY_TIMEFRAME`, `EMA_FAST`, `EMA_SLOW`, `RSI_PERIOD`, `ATR_PERIOD`,
`SWING_STRENGTH`, `MTF`, `AUREON_STORAGE_BACKEND`, `AUREON_LOCAL_DB_PATH`, `AUREON_CONFIG_DIR`,
`AUREON_PARQUET_ARCHIVE`, `AUREON_API_ENABLED`, `AUREON_API_HOST`, `AUREON_API_PORT`, `AUREON_GIT_SHA`,
`DISCORD_TOKEN`, `DISCORD_CHANNEL_ID`, `DISCORD_GUILD_ID`.

YAML (all validated with pydantic, `extra=forbid`, startup aborts on any error):

| file | contents |
|---|---|
| `config/symbols.yaml` | instrument-master URL/cache, rollover window, per-symbol `underlying`, segment, `FUTCOM`, `contract_policy` enum |
| `config/sessions.yaml` | timezone, trading day, global sessions (ASIA/LONDON/NEW_YORK) and MCX local sessions, trend thresholds |
| `config/confirmation_policy.yaml` | `policy_version`, RSI threshold, every rule toggleable |
| `config/analysis.yaml` | EMA-approach thresholds, structure tolerance, wick rules, liquidity/breakout params, RSI levels, de-clutter limits, horizons, cohort minimum, historical warmup, verification retry schedule / degraded thresholds / feed stall / reconcile mode, Discord debounce, `execution.enabled=false` |
| `config/exchange_calendar.yaml` | official MCX calendar for one year (holidays, session closures, seasonal 23:30 close, special / pending special sessions); more years in `exchange_calendar_<YEAR>.yaml` or `calendars/*.yaml` |
| `config/scanner.yaml` | broad-market scanner universe (segments, instrument types, include / exclude globs, front-month only, Dhan feed limits, stale threshold, digest / snapshot cadence, category map) |

## Startup flow (`python main_aureon.py`)

1. load config → 2. validate Dhan credentials (`GET /v2/profile`, never printed) → 3. download / refresh
instrument master → 4. resolve GOLD → 5. resolve SILVER → 6. publish instrument metadata
(log + `instruments` table + Discord status line) → 7. SQLite WAL + migrations → 8. historical
M5 / M15 / H1 (H4 aggregated locally from closed H1) → 9. warm indicators / structure / setups
(no publishing) → 10. connect Dhan WebSocket → 11. subscribe resolved ids → 12. closed-candle
observer (tick → M1 → M5 → M15/H1/H4, wall-clock boundary flush) → 13. Discord → 14. health.

Any failure in steps 1–9 aborts with a clear error. SIGINT / SIGTERM shut down gracefully.
Example logs:

```
GOLD -> security_id 428291 -> expiry 2026-10-05
symbol_resolved logical=GOLD security_id=428291 expiry=2026-10-05 ...
historical_warmup GOLD M5 bars=600
observer live
discord live
```

## Market-data continuity (fail closed)

Aureon never analyses an incomplete or discontinuous candle stream.

* **Completeness.** Every aggregated bar (M1→M5, M5→M15/H1, H1→H4) tracks the expected
  source open-times for its bucket (calendar-aware: only open-market intervals count, the last
  bucket of the day is shorter). Duplicates never count twice, misaligned bars never count,
  out-of-order delivery is tolerated. A bucket whose boundary passes with constituents missing
  is `GAP_DETECTED`: it is never dispatched to the observer, the gap is recorded
  (`market_data_gaps`), the symbol goes to `ERROR` and later complete bars are held back until
  the gap is repaired with the exact broker candle.
* **M1 trust.** A live M1 is trusted only when tick coverage began at or before the minute's
  start (`coverage_start`, set on every connect / reconnect / rollover). A minute that began
  before coverage (startup at 10:02:30, a reconnect, a feed restart) and a minute in which a
  connected socket delivered no tick (`silent_feed`) are never analysed from ticks: they are
  queued for verification and replaced by the exact broker M1 (Dhan intraday, interval 1). A
  connected socket is not evidence that nothing traded. Until the broker candle arrives the
  symbol is `RECOVERING_GAP` (or `STALE` for silent minutes) and the M5 bucket that contains
  the minute is completed only from verified constituents (a `GAP_DETECTED` bucket is rebuilt
  once its late constituents are verified). If the broker has no candle for an open-market
  minute the symbol stays in `ERROR`; a zero-trade flat fill is possible only with
  `historical.allow_verified_zero_trade_fill: true` (default `false`) and only when the broker
  returned the neighbouring minutes. Minutes during a disconnect are gaps, never invented.
* **Reconnect recovery.** On every WebSocket connect (startup included) the closed M1 candles
  between the last processed minute (or the minute left open at disconnect) and now are fetched
  from the Dhan historical API and fed through the same pipeline in chronological order while
  live ticks are buffered, then buffered ticks are replayed. The symbol is `LIVE` only when
  continuity is verified; a failed recovery leaves it in `ERROR` with analytics suspended.
* **Exchange calendar.** `config/exchange_calendar.yaml` is the authoritative MCX calendar
  (year, full holidays, morning-closed days that open at 17:00, seasonal close periods for the
  23:30 IST US-daylight-time close vs 23:55 otherwise, special sessions). `config/sessions.yaml`
  gives the default session shape; the optional legacy `config/session_overrides.yaml` still
  applies first. `SessionCalendar` resolves override → special session → holiday → weekend →
  close period → default, and is the single source for "is the exchange open", trading date,
  session end, expected constituents and H4 buckets. The file records its SOURCE and
  `verified_against_official_circular`; a missing, malformed or unpopulated calendar fails startup.
* **Historical cache.** `historical_cache_ranges` stores *verified* coverage: intervals whose
  calendar-expected bars were all returned by the broker (merged on write). A partial, empty or
  truncated response leaves the missing sub-range uncovered and only that sub-range is
  re-requested next time; an unclosed bar is never covered; a holiday range is verified-empty.
* **Off-session prints.** A source bar inside a bucket but outside market hours is logged as
  `aggregation_extra_ignored` and never shapes the OHLC / volume / OI of a trusted bar.
* **H4 policy.** H4 bars are anchored to the configured trading-day start (09:00–13:00,
  13:00–17:00, 17:00–21:00, 21:00–close IST), never the 08:00 wall-clock bucket.

## Contract rollover (staged, race-safe)

When the resolver reports a new active security id: the new contract's M5/M15/H1 history is
loaded, H4 derived, minimum history validated, a fresh observer warmed and a pipeline seeded
(`ROLLOVER_WARMING`). The prepared runtime is registered under the new security id *before*
`subscribe`, so ticks that arrive during the subscription are routed into it rather than lost;
then observer / history / pipeline / active instrument switch atomically and the old id is
unsubscribed. The symbol is then `RECOVERING_GAP` until the new contract's history is verified
against its live stream (the current, partial minute is broker-verified like any other), and
only then `LIVE`. Any failure keeps the old contract running and reports `rollover ... FAILED`.
Session trend rows are contract-aware (`session_state` is unique per symbol, security id, date
and session), so a same-day rollover never overwrites the old contract's session.

## Health

`health.status_line()` (log + Discord status message) reports component states and, per
symbol: `LIVE`, `WARMING`, `RECOVERING_GAP`, `ROLLOVER_WARMING`, `STALE` or `ERROR`, with
security id, expiry, feed connection, reconnect count, unresolved gaps, last tick and the
latest closed M1/M5/M15/H1/H4. A connected feed with missing candles is not healthy. Background
tasks are supervised: feed / flush / housekeeping restart with backoff and repeated failure
shuts the service down; a supervised task that *returns* while the service is still running is
treated as a failure and restarted the same way; Discord failure degrades to `discord=error`
and retries while observation continues. A symbol with no tick for 90 s while the exchange is
open is `STALE`.

## M1 trust, feed stalls and persistent verification

Every live minute carries a trust record (`MinuteTrust`: first / last packet, packet count,
socket state at open, feed generation, stale transitions, continuity lost, reason) and a state:
`OPEN_TRUSTED` → `TRUSTED` (admitted to aggregation) or `OPEN_SUSPECT` → `PARTIAL` →
`AWAITING_BROKER` → `VERIFIED` (the broker's M1 replaced it). A connected socket that delivers
no packet for `historical.feed_stall_seconds` is a **stalled feed**: the open minute becomes
SUSPECT and coverage resumes only at the next packet, so the resume minute is PARTIAL. A
locally built M1 never enters analysis when continuity was lost inside the minute.

Unresolved minutes and gaps are **continuity incidents** (`continuity_incidents` table:
`first_detected_at`, `last_attempt_at`, `next_retry_at`, `attempt_count`, `last_error`). They
are retried on `historical.verification_backoff_seconds` (2, 5, 10, 20, 30, 60 s, then every
60 s) while the minute belongs to the current trading day, without a reconnect and without a
busy loop; a restart restores them. The symbol is `RECOVERING_GAP` → `DEGRADED`
(`degraded_after_seconds`) → `ERROR` (`error_after_seconds`) by incident age, `LIVE` again
with a recovery event when the broker delivers. Incidents are never deleted: they end
`RESOLVED` or `ABANDONED` (trading day over, recorded and announced).

`historical.reconcile_every_live_m1: true` (research validation) compares every trusted live
M1 with the broker's after `reconcile_delay_seconds`; mismatches are counted
(`live_m1_mismatches`), reported to Discord and replace the constituent while the primary
bucket is still open. Metrics: `live_m1_built`, `live_m1_verified`, `live_m1_mismatches`,
`partial_m1_replaced`, `silent_m1_replaced`, `suspect_m1_replaced`.

Threading contract: worker threads (`asyncio.to_thread`) only fetch Dhan data and return
immutable results; every `CandlePipeline` / `HealthState` / observer / Discord mutation runs
on the event loop (`ThreadAffinityError` otherwise).

## Market state and calendar coverage

`SessionCalendar.market_state(now)` reports `OPEN` or `CLOSED` with a reason (`WEEKEND`,
`FULL_HOLIDAY`, `MORNING_SESSION_CLOSED`, `EVENING_SESSION_CLOSED`, `OUTSIDE_TRADING_HOURS`,
`SPECIAL_SESSION_PENDING`, `CALENDAR_OUT_OF_RANGE`), the session (`MORNING` / `EVENING` /
`SPECIAL`) and the next open / close. While closed nothing is expected: no missing-candle
incidents, no stale alerts, no verification retries, feeds reconnect slowly, and the scanner
presents no "live" winners / losers. A trading date outside every configured calendar year is
`CALENDAR_OUT_OF_RANGE`: startup aborts and live mode fails closed (symbols `ERROR`,
`CALENDAR_FAILURE` event, "MCX calendar for 2027 not installed"). The shortened last bar of a
day (23:00 H1 closing 23:30 / 23:55, 23:15 / 23:45 M15) is closed at the exchange close
(`effective_close_time`), which Dhan normalisation and the cache use.

## Agents, crash reports and events

Major responsibilities are observable **agents** (`market_feed`, `continuity`, `calendar`,
`aggregation`, `scanner`, `analysis`, `setup`, `outcome`, `rollover`, `storage`, `discord`,
`api`, `health`) with one status model (`HEALTHY` / `DEGRADED` / `STALE` / `RECOVERING` /
`RESTARTING` / `ERROR` / `FAILED` / `STOPPED`, heartbeats, last success / error, restart and
work counts). Every unexpected failure becomes a durable **crash report** (`crash_reports`:
crash id, component, agent, task, exception class, redacted message and trace, symbol,
security id, version, git sha, uptime, restart number, recovery result) and the task is
restarted with backoff; a restarted task that runs cleanly resolves the report, repeated
critical failure ends in `FAILED` and a graceful shutdown. All operational changes flow
through the `SystemEventBus` (typed events with dedupe keys, persisted in `system_events`)
to the agent registry, the API and Discord.

## Status API (read-only, `:1250`)

`GET /api/v1/status` is the one clean report (status, version / git sha / uptime, market,
agents, symbols, scanner, continuity, crashes, discord); `/api/v1/health` is liveness plus a
`ready` flag (observation trustworthy); `/agents`, `/agents/{id}`, `/symbols`,
`/symbols/{symbol}`, `/market`, `/market/winners`, `/market/losers`, `/market/breadth`,
`/market/instruments`, `/crashes`, `/events`, `/gaps`, `/calendar`, `/metrics`,
`/continuity`. Handlers read in-memory projections (database counts are refreshed by
housekeeping), no endpoint exposes secrets, and nothing can place an order. Uvicorn runs as a
supervised task under the observer's event loop: one process, no workers; an API failure
degrades `api_agent` and never touches observation.

## Scanner (tier 1) and observer (tier 2)

The scanner subscribes to the whole enabled universe from Dhan's instrument master (MCX
commodity futures, front month per commodity by default; NSE segments when configured)
over its own feed connections partitioned within Dhan's documented limits, and keeps LTP /
verified previous close / % change / day range / volume / OI per instrument. Today's winners
and losers are ranked only from instruments with a VERIFIED previous close (Dhan's explicit
previous-close data; otherwise `change_pct: null`, `reference_status: MISSING`) that are not
stale, and only while the market is open. Breadth (advancers / decliners / unchanged / stale /
unavailable, average and median move) is reported overall and per segment / category /
instrument type; rankings are snapshotted to `market_rank_snapshots`. Only `SYMBOLS` receive
the full closed-candle observer.

## Discord as the operational surface

Beyond setup cards, Discord receives every meaningful state change rendered from the event
bus: startup / shutdown / deployed version, agent crash (`🚨 AUREON AGENT CRASH` with agent,
state, safe error, crash id, restart count, recovery), restart and recovery, feed connected /
disconnected / reconnecting / stalled / recovered, partial / silent / suspect minutes,
verification started / succeeded / failed, live-vs-broker mismatch, gap detected / repaired,
rollover stages, `MARKET OPEN` / `MARKET CLOSED` / session changes / holidays / approaching
close, and a periodic `🏆 TODAY'S WINNERS` / `📉 TODAY'S LOSERS` digest with breadth. Repeats
are deduplicated on the bus, bursts are batched, and the outbound rate is capped. Slash
commands `/status`, `/crashes`, `/market`, `/winners`, `/losers`, `/agents`, `/symbol` are
informational only.

## Deployment (Docker + Jenkins)

`Dockerfile`: python 3.12 slim, non-root user, `tini` for graceful SIGTERM, `EXPOSE 1250`,
health check on `/api/v1/health`, persistent volumes `/data/sqlite`, `/data/parquet`,
`/data/logs`; secrets are passed as environment variables at run time and never baked in.
`Jenkinsfile` stages: Checkout → Resolve Git SHA → Python syntax / static sanity → Install
dependencies → Run pytest → Build Docker image (`aureon-beast-india:${GIT_COMMIT}`) → Stop /
replace previous container (previous image kept as `:rollback`) → Start new container
(`aureon-beast-india`, `1250:1250`, host volumes under `/data/aureon`) → Health check (retry
loop, fails the build) → Deployment notification (Discord webhook: `✅ Aureon deployed` with
git / branch / tests / API / port / version, or `❌ Aureon deployment failed` with the stage).
Dhan / Discord credentials come from Jenkins credentials, are masked in logs, and a missing
Dhan credential refuses the deployment.

## Restart safety

Replaying the same history into the same SQLite file changes nothing: candles, indicators
(the first analysed values are authoritative), pivots, detections, setups (unique per
originating detection), lifecycle events (unique per setup / candle / target state),
clearances, snapshots, outcomes and day frames (derived from stored candles) are idempotent.
Monitor subscriptions are persisted in `monitor_subscriptions` and restored on startup.

Schema migrations (`schema_version`): v2 consolidates pre-v2 duplicate setups per origin
detection *before* enforcing uniqueness, keeping the canonical row (terminal state, then latest
progress, then richest history), repointing events / clearances / Discord card references and
refusing with a report when candidates are ambiguous; v3 rebuilds `session_state` with the
contract-aware key while preserving rows and ids. Outcome updates are filtered by contract in
SQL (`pending_outcomes(security_id, limit)`), so one contract's backlog cannot starve another.

## Discord card

Six full-width sections in fixed order — `1 · SETUP`, `2 · TREND`, `3 · MOMENTUM`,
`4 · CONFIRMATION EVIDENCE`, `5 · TRACE`, `6 · HISTORICAL REFERENCE` (only with enough outcome
data) — followed by an isolated **FINAL CHECK** (`⛔ NOT CLEARED` or `✅ CLEARED FOR REVIEW`,
"Manual review required."). The builder asserts FINAL CHECK is last and forbids trade-ready
wording. Cards are created once per setup and PATCHed only when the material state hash changes
(state, clearance, blockers, MTF, fakeout flags, new event), with a debounce window; unchanged
cards are never touched, including after a restart. A `Monitor` button subscribes a user to that
setup. There is no Execute button; `execute_request` refuses unless `execution.enabled` is true
and, even then, no order code path exists.

## Learning pipeline (phase 1 only)

Every directional detection freezes an immutable feature snapshot (`feature_schema_version`,
SQLite trigger + application guard). Outcomes are observed over 1 / 3 / 6 / 12 bars and the
session window from later closed candles only: direction-aware MFE / MAE (points and ATR
units), time-to-MFE/MAE, bars to invalidation / follow-through, and versioned labels
(`good_continuation`, `fakeout`, `wrong_direction`, `no_follow_through`, `ambiguous`,
`invalidated_setup`). Cohort facts are shown only above `MIN_COHORT_SAMPLE`. Model training is
deliberately not implemented; interfaces and the `model_registry` table exist (`outcomes/model_stub.py`).

## Section checklist (prompt sections 1–35)

| § | topic | implementation | tests |
|---|---|---|---|
| 0 | principles / layering | whole package; Discord reads `views.SetupView` + stored rows only | `test_discord.py`, `test_observer.py` |
| 1 | broker / market (Dhan v2, `MCX_COMM`, `FUTCOM`, no hard-coded ids) | `broker/dhan/*`, `config/symbols.yaml` | `test_symbol_resolver.py` |
| 2 | SymbolResolver (exact base-name, policy enum, rollover, fail closed, log shape) | `broker/dhan/symbol_resolver.py`, `instruments.py` | `test_symbol_resolver.py` |
| 3 | historical provider, Candle model, cache, timeframe roles, closed-only + complete aggregation | `broker/dhan/historical.py`, `market/candle.py`, `market/aggregation.py` | `test_market_data.py`, `test_continuity.py` |
| 4 | live feed WebSocket, reconnect/backoff/resubscribe + gap recovery, staged rollover, tick→M1→M5 | `broker/dhan/live_feed.py`, `market/candle_builder.py`, `continuity.py`, `app.py` | `test_live_feed.py`, `test_continuity.py`, `test_app.py` |
| 5 | SQLite WAL/FK/busy_timeout, migrations, tables, write ordering, parquet flag | `storage/*` | `test_storage.py` |
| 6 | indicators persisted per candle, configurable periods | `indicators/engine.py`, `storage/repositories.py` | `test_indicators.py` |
| 7 | early EMA vs actual cross, thresholds, payload, exact labels | `detection/ema.py` | `test_detection.py` |
| 8 | confirmed swing structure, labels, tolerance, dominance, sequence, `confirmed_at_open_time` | `structure/engine.py` | `test_structure.py` |
| 9 | chart structure rendering (labels above/below, one path per pivot kind) | `discord/chart_renderer.py` | `test_discord.py` |
| 10 | wick detection with explicit rules stored per detection | `detection/wick.py` | `test_detection.py` |
| 11 | liquidity levels (confirmed only), proximity / sweep / reclaim as observations | `detection/levels.py`, `detection/liquidity.py` | `test_detection.py` |
| 12 | breakout events + lifecycle state machine with `setup_events` per transition | `detection/breakout.py`, `setups/lifecycle.py` | `test_detection.py`, `test_lifecycle.py` |
| 13 | RSI value / direction, 30-50-70 events stored | `indicators/engine.py`, `detection/rsi_events.py` | `test_indicators.py`, `test_detection.py` |
| 14 | MTF reads, ALIGNED / AGAINST / CONFLICT / NO CONTEXT, early reversal, sideways non-directional | `mtf/context.py` | `test_mtf_trend.py` |
| 15 | present + session trend, exchange calendar with holidays / overrides | `mtf/trend.py`, `market/sessions.py`, `config/exchange_calendar.yaml` | `test_mtf_trend.py`, `test_calendar.py`, `test_continuity.py` |
| 16 | confirmation engine, INITIAL STRICT RESEARCH POLICY, `policy_version` | `confirmation/policy.py`, `config/confirmation_policy.yaml` | `test_confirmation.py` |
| 17 | counter-trend protection (`REACTION DETECTED` + ⚠ block, never bare BUY) | `confirmation/policy.py`, `discord/card_builder.py` | `test_confirmation.py`, `test_discord.py` |
| 18 | factual fakeout flags | `confirmation/fakeout.py` | `test_confirmation.py` |
| 19 | clearance with verbatim blockers, missing input = blocker | `confirmation/policy.py` | `test_confirmation.py` |
| 20 | card sections in order, FINAL CHECK last (assert + test) | `discord/card_builder.py` | `test_discord.py` |
| 21 | card visual rules (full width, separators, isolated final check) | `discord/card_builder.py` | `test_discord.py` |
| 22 | Monitor button; no auto-trade; Execute gated off and refusing | `discord/bot.py` | `test_discord.py` |
| 23 | chart from stored rows (candles, EMAs, labels, paths, markers, anchor, RSI, context) | `discord/chart_renderer.py` | `test_discord.py` |
| 24 | de-clutter limits (configurable) | `discord/chart_renderer.py`, `analysis.yaml` | `test_discord.py` |
| 25 | immutable feature snapshot, `feature_schema_version` | `outcomes/snapshot.py`, migration trigger | `test_outcomes.py`, `test_storage.py` |
| 26 | outcome observation MFE/MAE per horizon, closed future candles only | `outcomes/observation.py`, `outcomes/service.py` | `test_outcomes.py` |
| 27 | all outcomes labelled, versioned definitions | `outcomes/labels.py` | `test_outcomes.py` |
| 28 | model later: versions, registry table, stub interfaces, no training | `outcomes/model_stub.py`, `outcomes/versions.py` | — (stubs) |
| 29 | cohort facts, `MIN_COHORT_SAMPLE`, section 6 omitted below it | `outcomes/cohort.py`, `discord/card_builder.py` | `test_outcomes.py`, `test_discord.py` |
| 30 | Dhan API safety: env creds, retry/backoff, limiter, cache, redaction | `broker/dhan/client.py`, `logging_setup.py` | `test_dhan_client.py`, `test_config.py` |
| 31 | config (.env + YAML, pydantic, fail closed) | `config/*` | `test_config.py` |
| 32 | startup flow order, structured logs, abort on 1–9, graceful shutdown | `app.py`, `main_aureon.py` | `test_app.py` |
| 33 | Discord rate-limit safety (hash reconcile, debounce, no bulk refresh) | `discord/coalescer.py`, `discord/message_refs.py` | `test_discord.py` |
| 34 | test matrix | `tests/` (131 tests, recorded fixtures, CI on Python 3.11 / 3.12) | `.github/workflows/test.yml` |
| 35 | deliverables / commit-per-step | git history, this README | — |

## Not fully completed / explicit caveats

* **Live Dhan verification.** Endpoint payloads, the WebSocket binary layout, instrument-master
  column names and epoch semantics are implemented from the v2 documentation and validated only
  against recorded fixtures (no live network in tests). A first live run should confirm them;
  `historical.max_days_per_request` and `# DECISION` comments mark the assumptions.
* **`nearest_liquid` policy** uses the nearest non-expired standard contract outside the rollover
  window; the instrument master carries no liquidity data (see `# DECISION` in the resolver).
* **PostgreSQL backend** is selectable in config but not implemented; startup fails closed.
* **Live M15 / H1** are aggregated locally from closed, complete M5 candles; exact broker candles are
  used for warmup and for gap repair.
* **Money conversion** is not implemented (outcomes stay in points and ATR units); the
  `outcomes.contract_specs` config exists for a later explicit opt-in.
* **Health** is a structured log line plus the Discord status message; there is no HTTP endpoint.
* **Default branch.** GitHub's default branch must be switched to `main` in the repository
  settings (Repository Settings → Default branch → main); the code lives on `main` and the API
  used here cannot change it.
* **MCX 2026 calendar** encodes the official MCX trading-holiday list (operator-verified,
  `verified_against_official_circular: true`); the 08 Nov Diwali Muhurat session is a
  `pending_special_sessions` entry (treated as closed) until MCX publishes its hours.
* **Session-end candles.** `effective_close_time` and the recorded fixtures follow Dhan's
  documented convention (last bar stamped at its open, spanning to the exchange close); a live
  run should confirm it for the 23:00 H1 and 23:15 / 23:45 M15 bars.
* **Scanner previous close** relies on Dhan's explicit previous-close packet for quote / full
  subscriptions; if a live run shows it is not delivered for a segment, `change_pct` stays null
  (never fabricated) until `previous_close_from_quote_day_close` is verified and enabled.
* **Discord slash commands and outbound event embeds** are implemented against discord.py's
  app-command tree but, like the card runtime, are not exercised against a live gateway in tests.
* **Model training (§28)** is intentionally not implemented; only interfaces and the registry table exist.
* The Discord runtime (gateway login, message send / edit) is not exercised in tests; the card,
  chart planner / renderer, coalescer and message-ref logic are.
