from __future__ import annotations

from datetime import datetime, timedelta, timezone

from aureon_mcx.detection.models import Direction
from aureon_mcx.indicators.models import IndicatorRow, RsiDirection
from aureon_mcx.market.sessions import SessionCalendar
from aureon_mcx.market.timeframe import Timeframe
from aureon_mcx.mtf import MtfAlignment, SessionTrendTracker, TimeframeRead, TrendDirection, assess_mtf, classify_timeframe, present_trend
from aureon_mcx.structure.models import StructureContext
from tests.conftest import make_candles


def _ind(ema_fast, ema_slow, rsi, atr=10.0, tf=Timeframe.M5):
    return IndicatorRow("GOLD", "1", "e", tf, datetime(2026, 9, 21, tzinfo=timezone.utc), ema_fast, ema_slow, ema_fast - ema_slow, 0.0,
                        rsi, RsiDirection.FLAT, atr, 1.0, None, 20, 50, 14, 14)


def _reads(**kw) -> dict:
    out = {}
    for k, v in kw.items():
        tf = Timeframe(k)
        out[tf] = TimeframeRead(tf, TrendDirection(v) if v != "unavailable" else TrendDirection.UNAVAILABLE)
    return out


def test_classify_timeframe():
    assert classify_timeframe(Timeframe.M5, _ind(101, 100, 55), StructureContext.MIXED).direction is TrendDirection.BULLISH
    assert classify_timeframe(Timeframe.M5, _ind(99, 100, 45), StructureContext.BEARISH).direction is TrendDirection.BEARISH
    # EMA bullish but RSI below 50 -> sideways (fail closed)
    assert classify_timeframe(Timeframe.M5, _ind(101, 100, 45), StructureContext.MIXED).direction is TrendDirection.SIDEWAYS
    # structure opposes -> sideways
    assert classify_timeframe(Timeframe.M5, _ind(101, 100, 60), StructureContext.BEARISH).direction is TrendDirection.SIDEWAYS
    assert classify_timeframe(Timeframe.M5, _ind(101, 100, 60), StructureContext.BEARISH, require_structure=False).direction is TrendDirection.BULLISH
    assert classify_timeframe(Timeframe.H4, None, None).direction is TrendDirection.UNAVAILABLE
    r = classify_timeframe(Timeframe.M15, _ind(101, 100, 55), None)
    assert r.ema_relation == "EMA20 > EMA50" and r.to_dict()["direction"] == "BULLISH"


def test_assess_mtf_aligned_against_conflict_no_context():
    a = assess_mtf(_reads(M5="BEARISH", M15="BEARISH", H1="BEARISH", H4="unavailable"), Direction.BEARISH, Timeframe.M5)
    assert a.alignment is MtfAlignment.ALIGNED and a.consensus is TrendDirection.BEARISH
    # spec example: M5 bullish, M15 bearish, H1 bearish -> conflict / early reversal
    c = assess_mtf(_reads(M5="BULLISH", M15="BEARISH", H1="BEARISH"), Direction.BULLISH, Timeframe.M5)
    assert c.alignment is MtfAlignment.CONFLICT and c.early_reversal and "EARLY REVERSAL / WATCH" in c.notes
    # bullish reaction while every directional timeframe is bearish -> AGAINST
    g = assess_mtf(_reads(M5="BEARISH", M15="BEARISH", H1="BEARISH"), Direction.BULLISH, Timeframe.M5)
    assert g.alignment is MtfAlignment.AGAINST
    # higher timeframes disagree with one another -> CONFLICT
    x = assess_mtf(_reads(M5="BULLISH", M15="BULLISH", H1="BEARISH"), Direction.BULLISH, Timeframe.M5)
    assert x.alignment is MtfAlignment.CONFLICT and not x.early_reversal
    # sideways timeframes never count as agreement; no directional higher tf -> NO_CONTEXT
    n = assess_mtf(_reads(M5="BULLISH", M15="SIDEWAYS", H1="SIDEWAYS", H4="unavailable"), Direction.BULLISH, Timeframe.M5)
    assert n.alignment is MtfAlignment.NO_CONTEXT and "no directional higher-timeframe context" in n.notes
    # sideways M15 with bullish H1 -> aligned (sideways ignored)
    s = assess_mtf(_reads(M5="BULLISH", M15="SIDEWAYS", H1="BULLISH"), Direction.BULLISH, Timeframe.M5)
    assert s.alignment is MtfAlignment.ALIGNED
    assert s.display_rows()[0] == ("M5", "BULLISH")


def test_present_trend():
    up = make_candles(20, prices=[70000 + 10 * i for i in range(20)])
    assert present_trend(up, _ind(101, 100, 60, atr=10), StructureContext.MIXED, 6, 0.25).direction is TrendDirection.BULLISH
    assert present_trend(up, _ind(101, 100, 60, atr=10), StructureContext.BEARISH, 6, 0.25).direction is TrendDirection.SIDEWAYS
    down = make_candles(20, prices=[70000 - 10 * i for i in range(20)])
    assert present_trend(down, _ind(99, 100, 40, atr=10), StructureContext.BEARISH, 6, 0.25).direction is TrendDirection.BEARISH
    flat = make_candles(20, prices=[70000.0] * 20)
    assert present_trend(flat, _ind(101, 100, 60, atr=10), None, 6, 0.25).direction is TrendDirection.SIDEWAYS
    assert present_trend(up[:3], _ind(101, 100, 60), None, 6).direction is TrendDirection.UNAVAILABLE
    assert present_trend(up, None, None, 6).direction is TrendDirection.UNAVAILABLE


def test_session_trend_tracker_never_shows_closed_session_as_current(app_config):
    cal = SessionCalendar(app_config.sessions)
    tr = SessionTrendTracker(cal, min_move_atr=0.25)
    start = datetime(2026, 9, 21, 3, 30, tzinfo=timezone.utc)  # 09:00 IST, ASIA + MORNING
    n = int(4.5 * 12) + 3  # 09:00 .. past 13:30 IST
    candles = make_candles(n, start=start, prices=[70000 + 5 * i for i in range(n)])
    ind = _ind(101, 100, 60, atr=10)
    for c in candles:
        tr.update(c, ind)
    asia = tr.rows[("2026-09-21", "ASIA")]
    assert asia.is_current is False and asia.closed_at is not None and asia.trend is TrendDirection.BULLISH
    london = tr.rows[("2026-09-21", "LONDON")]
    assert london.is_current is True and london.closed_at is None
    assert tr.current("global").session_name == "LONDON"
    assert tr.current("mcx").session_name == "MORNING"
    assert tr.latest_closed("ASIA").session_date.isoformat() == "2026-09-21"
    rows = tr.display_rows()
    assert {r["session"] for r in rows if r["is_current"]} == {"LONDON", "MORNING"}
    rec = asia.to_record("GOLD", "1")
    assert rec["is_current"] == 0 and rec["session_date"] == "2026-09-21" and rec["closed_at"]
