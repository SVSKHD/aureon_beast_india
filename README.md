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
pytest                          # 100+ tests, no network
python main_aureon.py           # optional: --config-dir config --env-file .env
```

`.env` keys (see `.env.example`): `BROKER`, `DHAN_CLIENT_ID`, `DHAN_ACCESS_TOKEN`, `SYMBOLS`,
`EXCHANGE_SEGMENT`, `PRIMARY_TIMEFRAME`, `EMA_FAST`, `EMA_SLOW`, `RSI_PERIOD`, `ATR_PERIOD`,
`SWING_STRENGTH`, `MTF`, `AUREON_STORAGE_BACKEND`, `AUREON_LOCAL_DB_PATH`, `AUREON_CONFIG_DIR`,
`AUREON_PARQUET_ARCHIVE`, `DISCORD_TOKEN`, `DISCORD_CHANNEL_ID`, `DISCORD_GUILD_ID`.

YAML (all validated with pydantic, `extra=forbid`, startup aborts on any error):

| file | contents |
|---|---|
| `config/symbols.yaml` | instrument-master URL/cache, rollover window, per-symbol `underlying`, segment, `FUTCOM`, `contract_policy` enum |
| `config/sessions.yaml` | timezone, trading day, global sessions (ASIA/LONDON/NEW_YORK) and MCX local sessions, trend thresholds |
| `config/confirmation_policy.yaml` | `policy_version`, RSI threshold, every rule toggleable |
| `config/analysis.yaml` | EMA-approach thresholds, structure tolerance, wick rules, liquidity/breakout params, RSI levels, de-clutter limits, horizons, cohort minimum, historical warmup, Discord debounce, `execution.enabled=false` |

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
| 3 | historical provider, Candle model, cache, timeframe roles, closed-only aggregation | `broker/dhan/historical.py`, `market/candle.py`, `market/aggregation.py` | `test_market_data.py` |
| 4 | live feed WebSocket, reconnect/backoff/resubscribe, rollover swap, tick→M1→M5 | `broker/dhan/live_feed.py`, `market/candle_builder.py`, `app.py` | `test_live_feed.py`, `test_market_data.py`, `test_app.py` |
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
| 15 | present + session trend (configurable IST windows, `is_current`, `closed_at`) | `mtf/trend.py`, `market/sessions.py` | `test_mtf_trend.py`, `test_market_data.py` |
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
| 34 | test matrix | `tests/` (102 tests, recorded fixtures) | — |
| 35 | deliverables / commit-per-step | git history, this README | — |

## Not fully completed / explicit caveats

* **Live Dhan verification.** Endpoint payloads, the WebSocket binary layout, instrument-master
  column names and epoch semantics are implemented from the v2 documentation and validated only
  against recorded fixtures (no live network in tests). A first live run should confirm them;
  `historical.max_days_per_request` and `# DECISION` comments mark the assumptions.
* **`nearest_liquid` policy** uses the nearest non-expired standard contract outside the rollover
  window; the instrument master carries no liquidity data (see `# DECISION` in the resolver).
* **PostgreSQL backend** is selectable in config but not implemented; startup fails closed.
* **Live M15 / H1** are aggregated locally from closed M5 candles; exact broker candles are used
  for warmup only.
* **Money conversion** is not implemented (outcomes stay in points and ATR units); the
  `outcomes.contract_specs` config exists for a later explicit opt-in.
* **Health** is a log line plus the Discord status message; there is no HTTP endpoint.
* **Monitor subscriptions** live in memory and are lost on restart.
* **Model training (§28)** is intentionally not implemented; only interfaces and the registry table exist.
* The Discord runtime (gateway login, message send / edit) is not exercised in tests; the card,
  chart planner / renderer, coalescer and message-ref logic are.
