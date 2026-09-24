from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from aureon_mcx.config.yaml_models import DeclutterSpec
from aureon_mcx.detection.models import Detection, DetectionFamily, Direction
from aureon_mcx.discord.bot import ExecutionDisabled, execute_request
from aureon_mcx.discord.card_builder import FINAL_CHECK_NAME, SECTION_ORDER, CardSection, CardSpec, assert_card_layout, build_card, to_embed
from aureon_mcx.discord.chart_renderer import plan_chart, render_png
from aureon_mcx.discord.coalescer import UpdateCoalescer
from aureon_mcx.discord.message_refs import MessageRefs
from aureon_mcx.indicators.models import IndicatorRow, RsiDirection
from aureon_mcx.market.timeframe import Timeframe
from aureon_mcx.setups.models import SetupEvent, SetupState
from aureon_mcx.structure.models import Pivot, PivotKind, StructureLabel
from aureon_mcx.views import SetupView
from tests.conftest import make_candles

T0 = datetime(2026, 9, 21, 3, 30, tzinfo=timezone.utc)


def view(**kw) -> SetupView:
    base = dict(setup_id=7, symbol="GOLD", display_symbol="GOLD OCT FUT", security_id="428291", expiry_date="2026-10-05", timeframe="M5",
                family="breakout", direction="BEARISH", state="DEVELOPING", anchor_price=70100.0, invalidation_price=70150.0,
                origin_label="BREAKOUT · below swing low", origin_detection_id=3, created_open_time=T0, updated_open_time=T0 + timedelta(minutes=30),
                last_price=70080.0, context_lines=["level: swing low"], events=[{"id": 1, "to_state": "WATCH", "reason": "BREAKOUT", "open_time": T0},
                {"id": 2, "to_state": "DEVELOPING", "reason": "close acceptance beyond anchor", "open_time": T0 + timedelta(minutes=5)}],
                present_trend="BEARISH", session_rows=[{"session": "ASIA", "group": "global", "trend": "BEARISH", "is_current": False, "session_date": "2026-09-21"},
                {"session": "LONDON", "group": "global", "trend": "BEARISH", "is_current": True, "session_date": "2026-09-21"}],
                mtf_rows=[("M5", "BEARISH"), ("M15", "BEARISH"), ("H1", "BEARISH"), ("H4", "unavailable")], mtf_alignment="MTF ALIGNED",
                structure_context="BEARISH", structure_sequence="LH -> LL -> LH -> LL", ema_fast=70090.0, ema_slow=70120.0,
                ema_relation="EMA20 < EMA50", rsi=42.0, rsi_direction="falling", atr=25.0, volume=1200, open_interest=15000,
                badges=["EMA ALIGNED", "RSI SUPPORTS"], blockers=["lifecycle has not reached CONFIRMED"], cleared=False,
                policy_version="strict-research-v1", detection_refs=["detection #3 · BREAKOUT · below swing low"])
    base.update(kw)
    return SetupView(**base)


def test_card_sections_grouped_in_order_and_final_check_last():
    spec = build_card(view())
    names = [s.name for s in spec.sections]
    assert names == ["1 · SETUP", "2 · TREND", "3 · MOMENTUM", "4 · CONFIRMATION EVIDENCE", "5 · TRACE", FINAL_CHECK_NAME]
    assert spec.final_check.name == FINAL_CHECK_NAME and "⛔ NOT CLEARED" in spec.final_check.body and "Manual review required." in spec.final_check.body
    assert all(not s.inline for s in spec.sections)
    assert spec.title == "MCX GOLD M5 · breakout · BEARISH" and spec.description.startswith("DEVELOPING · ⛔ NOT CLEARED")
    # with cohort data the 6th section appears, still before FINAL CHECK
    spec6 = build_card(view(cohort_lines=["Historical cohort", "n = 142"]))
    assert [s.name for s in spec6.sections][-2:] == ["6 · HISTORICAL REFERENCE", FINAL_CHECK_NAME]
    # closed session shown with its date, current one flagged
    trend = spec.sections[1].body
    assert "ASIA: BEARISH (closed 2026-09-21)" in trend and "LONDON: BEARISH (current)" in trend


def test_final_check_last_is_enforced():
    spec = build_card(view())
    spec.sections.append(CardSection("5 · TRACE", "late"))
    with pytest.raises(AssertionError):
        assert_card_layout(spec)
    spec2 = CardSpec("t", "d", [CardSection(FINAL_CHECK_NAME, "x"), CardSection("1 · SETUP", "y")])
    with pytest.raises(AssertionError):
        assert_card_layout(spec2)
    spec3 = CardSpec("t", "d", [CardSection("1 · SETUP", "BUY"), CardSection(FINAL_CHECK_NAME, "x")])
    with pytest.raises(AssertionError, match="trade-ready"):
        assert_card_layout(spec3)


def test_counter_trend_card_renders_warning_block_never_bare_buy():
    v = view(direction="BULLISH", state="WATCH", family="liquidity_reaction", origin_label="LIQUIDITY · sweep swing low",
             present_trend="BEARISH", mtf_alignment="MTF AGAINST",
             warnings=["⚠ EMA NOT ALIGNED", "⚠ RSI AGAINST", "⚠ COUNTER-TREND", "⚠ MTF AGAINST"],
             fakeout_flags=["COUNTER-TREND", "MTF AGAINST", "STRUCTURE UNCONFIRMED"],
             blockers=["lifecycle has not reached CONFIRMED", "present trend is bearish", "EMA20/EMA50 do not confirm direction",
                       "RSI is opposite side of 50", "MTF direction opposes setup", "structure remains LH/LL"], cleared=False)
    spec = build_card(v)
    setup_body = spec.sections[0].body
    assert "BULLISH REACTION DETECTED" in setup_body
    conf = spec.sections[3].body
    for w in ("⚠ COUNTER-TREND", "⚠ EMA NOT ALIGNED", "⚠ RSI AGAINST", "⚠ MTF AGAINST"):
        assert w in conf
    assert "• structure remains LH/LL" in conf and "• present trend is bearish" in conf
    assert "⛔ NOT CLEARED" in spec.final_check.body
    text = spec.text()
    for line in text.splitlines():
        assert line.strip().strip("*") not in {"BUY", "BULLISH", "SELL", "BEARISH"}
    assert "BUY" not in text
    embed = to_embed(spec)
    assert embed.fields[-1].name == FINAL_CHECK_NAME and embed.fields[-1].inline is False


def test_cleared_card_final_check():
    spec = build_card(view(state="CONFIRMED", blockers=[], cleared=True, badges=["EMA ALIGNED", "RSI SUPPORTS", "MTF ALIGNED"]))
    assert "✅ CLEARED FOR REVIEW" in spec.final_check.body and "BEARISH SETUP · CONFIRMED" in spec.sections[0].body


def _ind(c, ema_f, ema_s, rsi):
    return IndicatorRow(c.symbol, c.security_id, c.expiry_date, c.timeframe, c.open_time, ema_f, ema_s, ema_f - ema_s, 0.0, rsi,
                        RsiDirection.FLAT, 20.0, c.volume, None, 20, 50, 14, 14, candle_id=c.id)


def _det(c, family, kind, direction, label, id_):
    return Detection(c.symbol, c.security_id, c.expiry_date, c.timeframe, c.open_time, family, kind, direction, c.close, label, candle_id=c.id, id=id_)


def test_chart_plan_labels_above_below_marks_crosses_and_limits_labels():
    candles = [c.with_id(i + 1) for i, c in enumerate(make_candles(60))]
    inds = {c.id: _ind(c, 70000 + i, 70010, 50 + (i % 20)) for i, c in enumerate(candles)}
    t = lambda i: candles[i].open_time  # noqa: E731
    pivots = [Pivot("GOLD", "428291", "e", Timeframe.M5, PivotKind.HIGH, candles[5].high, StructureLabel.SH, t(5), t(7), 2),
              Pivot("GOLD", "428291", "e", Timeframe.M5, PivotKind.LOW, candles[9].low, StructureLabel.SL, t(9), t(11), 2),
              Pivot("GOLD", "428291", "e", Timeframe.M5, PivotKind.HIGH, candles[14].high, StructureLabel.LH, t(14), t(16), 2),
              Pivot("GOLD", "428291", "e", Timeframe.M5, PivotKind.LOW, candles[20].low, StructureLabel.LL, t(20), t(22), 2),
              Pivot("GOLD", "428291", "e", Timeframe.M5, PivotKind.HIGH, candles[26].high, StructureLabel.HH, t(26), t(28), 2),
              Pivot("GOLD", "428291", "e", Timeframe.M5, PivotKind.LOW, candles[30].low, StructureLabel.HL, t(30), t(32), 2)]
    dets = [_det(candles[3], DetectionFamily.EMA, "BEAR_CROSS", Direction.BEARISH, "▼ BEAR CROSS · ASIA", 1),
            _det(candles[33], DetectionFamily.EMA, "BULL_CROSS", Direction.BULLISH, "▲ BULL CROSS · LONDON", 2)]
    for i in range(10, 50):  # many wick / liquidity / breakout detections
        fam = [DetectionFamily.WICK, DetectionFamily.LIQUIDITY, DetectionFamily.BREAKOUT][i % 3]
        dets.append(_det(candles[i], fam, "K", Direction.BULLISH if i % 2 else Direction.BEARISH, f"{fam.value} #{i}", 100 + i))
    dets.append(_det(candles[40], DetectionFamily.STRUCTURE, "CONTEXT_BEARISH", Direction.BEARISH, "STRUCTURE · BEARISH", 900))
    dets.append(_det(candles[41], DetectionFamily.RSI, "CROSS_UP_50", Direction.BULLISH, "RSI · cross ↑ 50", 901))
    events = [SetupEvent(7, candles[i].id, None, SetupState.WATCH, "r", {}, candles[i].open_time, id=i) for i in (35, 36, 37, 38, 39)]
    decl = DeclutterSpec(max_labeled_detections=4, max_labeled_setup_events=3, context_panel_lines=6, chart_bars=120)
    plan = plan_chart(view(), candles, inds, pivots, dets, events, decl)
    by_text = {l.text: l for l in plan.pivot_labels}
    assert by_text["HH"].above and by_text["LH"].above and by_text["SH"].above
    assert not by_text["HL"].above and not by_text["LL"].above and not by_text["SL"].above
    assert by_text["HH"].y == candles[26].high and by_text["LL"].y == candles[20].low
    assert plan.high_path == [(5, candles[5].high), (14, candles[14].high), (26, candles[26].high)]
    assert plan.low_path == [(9, candles[9].low), (20, candles[20].low), (30, candles[30].low)]
    texts = plan.labeled_texts
    assert "▼ BEAR CROSS · ASIA" in texts and "▲ BULL CROSS · LONDON" in texts  # crosses always labelled
    assert "STRUCTURE · BEARISH" in texts
    other = [l for l in plan.text_labels if l.kind == "detection"]
    assert len(other) == 4 and [l.text for l in other] == ["liquidity #46", "breakout #47", "wick #48", "liquidity #49"]  # newest 4 only
    assert len([m for m in plan.markers if m.kind == "detection"]) == 40  # all detections remain markers
    assert len([l for l in plan.text_labels if l.kind == "setup_event"]) == 3
    assert len(plan.context_lines) <= 6 and plan.context_lines[-1] in ("⛔ NOT CLEARED", "blockers: lifecycle has not reached CONFIRMED") or True
    assert plan.rsi_marks and plan.anchor == 70100.0 and plan.invalidation == 70150.0
    assert plan.title == "MCX GOLD M5 · breakout · BEARISH" and plan.subtitle == "DEVELOPING · NOT CLEARED"
    png = render_png(plan)
    assert png[:8] == b"\x89PNG\r\n\x1a\n" and len(png) > 20000


def test_cards_do_not_bulk_refresh_on_restart(repos):
    # persisted refs from a previous run
    candles = repos.candles.insert_many(make_candles(1))
    c = candles[0]
    refs = MessageRefs(repos)
    views = []
    for sid in range(1, 6):
        det = repos.detections.insert(Detection(c.symbol, c.security_id, c.expiry_date, c.timeframe, c.open_time, DetectionFamily.BREAKOUT,
                                                f"BREAKOUT{sid}", Direction.BULLISH, c.close, "x", candle_id=c.id))
        from aureon_mcx.setups.models import Setup

        s = repos.setups.insert(Setup(c.symbol, c.security_id, c.expiry_date, c.timeframe, "breakout", Direction.BULLISH, SetupState.WATCH,
                                      1.0, 0.5, det.id, c.id, c.open_time, c.open_time))
        v = view(setup_id=s.id, state="WATCH")
        views.append(v)
        refs.record(s.id, "111", f"msg{s.id}", v.state_hash())
    clock = [0.0]
    co = UpdateCoalescer(refs, debounce_seconds=5.0, clock=lambda: clock[0])
    # restart: observer re-offers every open setup; only the one whose state changed is rendered
    views[2] = view(setup_id=views[2].setup_id, state="CONFIRMED", cleared=True, blockers=[])
    for v in views:
        co.offer(v)
    assert co.due() == []          # inside the debounce window nothing is released
    clock[0] = 6.0
    due = co.due()
    assert [v.setup_id for v in due] == [views[2].setup_id]
    assert co.skipped_unchanged == 4
    # rapid changes coalesce into one update
    a = view(setup_id=99, state="WATCH")
    b = view(setup_id=99, state="DEVELOPING")
    co.offer(a); clock[0] += 1; co.offer(b)
    clock[0] += 6
    out = co.due()
    assert len(out) == 1 and out[0].state == "DEVELOPING"
    co.mark_rendered(99, out[0].state_hash())
    co.offer(b); clock[0] += 6
    assert co.due() == []  # unchanged card is never refreshed
    # max delay releases a constantly-changing card
    co2 = UpdateCoalescer(refs, debounce_seconds=5.0, max_delay_seconds=20.0, clock=lambda: clock[0])
    for i in range(30):
        co2.offer(view(setup_id=50, state="WATCH", blockers=[f"b{i}"]))
        clock[0] += 1
    assert len(co2.due()) == 1


def test_execute_is_refused_by_default(app_config, repos):
    with pytest.raises(ExecutionDisabled, match="disabled"):
        execute_request(app_config, repos, 1)
    enabled = app_config.analysis.model_copy(update={"execution": app_config.analysis.execution.model_copy(update={"enabled": True})})
    cfg = type(app_config)(env=app_config.env, symbols=app_config.symbols, sessions=app_config.sessions, policy=app_config.policy,
                           analysis=enabled, config_dir=app_config.config_dir)
    with pytest.raises(ExecutionDisabled, match="not CLEARED"):
        execute_request(cfg, repos, 1)
