from __future__ import annotations

from datetime import datetime, timezone

from aureon_mcx.detection import (
    BreakoutDetector, DetectionFamily, Direction, EmaDetector, Level, LevelSide, LiquidityDetector, RsiEventDetector,
    WickDetector, build_levels,
)
from aureon_mcx.detection.ema import LABEL_BEAR_CROSS, LABEL_BULL_CROSS, LABEL_EARLY_BEARISH, LABEL_EARLY_BULLISH
from aureon_mcx.indicators.models import IndicatorRow, RsiDirection
from aureon_mcx.market.candle import Candle
from aureon_mcx.market.timeframe import Timeframe
from aureon_mcx.structure.models import Pivot, PivotKind, StructureLabel
from tests.conftest import make_candles


def _ind(c: Candle, ema_fast, ema_slow, rsi=55.0, atr=10.0, rsi_dir=RsiDirection.FLAT) -> IndicatorRow:
    return IndicatorRow(symbol=c.symbol, security_id=c.security_id, expiry_date=c.expiry_date, timeframe=c.timeframe, open_time=c.open_time,
                        ema_fast=ema_fast, ema_slow=ema_slow, ema_gap=(ema_fast - ema_slow) if ema_fast is not None else None,
                        ema_fast_slope=0.0, rsi=rsi, rsi_direction=rsi_dir, atr=atr, volume=c.volume, open_interest=None,
                        ema_fast_period=20, ema_slow_period=50, rsi_period=14, atr_period=14, candle_id=c.id)


def _run_ema(gaps, atr=10.0, **kw):
    det = EmaDetector(**kw)
    candles = make_candles(len(gaps))
    out = []
    for c, g in zip(candles, gaps):
        out += det.update(c, _ind(c, 100 + g, 100, atr=atr), "ASIA")
    return out


def test_early_bullish_approach():
    # EMA20 below EMA50, gap shrinking, within 0.5*ATR (=5)
    dets = _run_ema([-12, -9, -6, -4, -3], approach_atr_fraction=0.5, min_shrinking_bars=2)
    kinds = [d.kind for d in dets]
    assert kinds == ["EARLY_BULLISH"]  # fires once per episode
    d = dets[0]
    assert d.label == LABEL_EARLY_BULLISH and d.direction is Direction.BULLISH and d.family is DetectionFamily.EMA
    assert d.payload["gap"] == -4 and d.payload["previous_gap"] == -6 and d.payload["ema20"] == 96 and d.payload["ema50"] == 100
    assert d.payload["session"] == "ASIA" and d.payload["timeframe"] == "M5" and "rsi" in d.payload and "atr" in d.payload


def test_early_bearish_approach():
    dets = _run_ema([12, 9, 6, 4, 3], approach_atr_fraction=0.5, min_shrinking_bars=2)
    assert [d.kind for d in dets] == ["EARLY_BEARISH"]
    assert dets[0].label == LABEL_EARLY_BEARISH and dets[0].direction is Direction.BEARISH


def test_bull_and_bear_cross():
    dets = _run_ema([-3, -1, 2, 4], approach_atr_fraction=0.1, min_shrinking_bars=5)
    assert [d.kind for d in dets] == ["BULL_CROSS"]
    assert dets[0].label == LABEL_BULL_CROSS.format(session="ASIA") == "▲ BULL CROSS · ASIA"
    dets = _run_ema([3, 1, 0, -2], approach_atr_fraction=0.1, min_shrinking_bars=5)
    assert [d.kind for d in dets] == ["BEAR_CROSS"]
    assert dets[0].label == "▼ BEAR CROSS · ASIA"
    # touching zero then going up: previous bar equal -> bull cross
    dets = _run_ema([-3, 0, 2], approach_atr_fraction=0.1, min_shrinking_bars=5)
    assert [d.kind for d in dets] == ["BULL_CROSS"]


def test_early_approach_is_never_a_cross():
    dets = _run_ema([-12, -9, -6, -4, -3, -2.5, -2], approach_atr_fraction=0.5, min_shrinking_bars=2)
    assert all(not d.is_ema_cross for d in dets)
    assert all("CROSS" not in d.label for d in dets)
    # approach then cross: two distinct events
    dets = _run_ema([-12, -9, -6, -4, -3, 1], approach_atr_fraction=0.5, min_shrinking_bars=2)
    assert [d.kind for d in dets] == ["EARLY_BULLISH", "BULL_CROSS"]


def test_early_thresholds_prevent_wobble():
    # gap shrinking but far from EMA50 relative to ATR -> nothing
    assert _run_ema([-40, -35, -30, -25], approach_atr_fraction=0.5, min_shrinking_bars=2, atr=10) == []
    # shrinking only one bar -> nothing with min_shrinking_bars=2
    assert _run_ema([-3, -2, -3, -2], approach_atr_fraction=0.5, min_shrinking_bars=2, atr=10) == []
    # no ATR -> fail closed
    assert _run_ema([-12, -9, -6, -4, -3], atr=None) == []


def test_wick_rules():
    c = make_candles(1)[0]
    # upper rejection: range 20, upper wick 15, body 2
    upper = Candle(**{**c.__dict__, "open": 70000, "close": 70002, "high": 70017, "low": 69997})
    det = WickDetector(0.6, 0.3, 0.8)
    d = det.update(upper, _ind(upper, 1, 1, atr=20.0), "LONDON")
    assert len(d) == 1 and d[0].kind == "UPPER_REJECTION" and d[0].label == "WICK · upper rejection" and d[0].direction is Direction.BEARISH
    assert d[0].payload["min_wick_range_fraction"] == 0.6 and d[0].payload["wick_fraction"] == 0.75
    lower = Candle(**{**c.__dict__, "open": 70002, "close": 70000, "high": 70003, "low": 69983})
    d = det.update(lower, _ind(lower, 1, 1, atr=20.0), None)
    assert [x.kind for x in d] == ["LOWER_REJECTION"] and d[0].label == "WICK · lower rejection"
    # ordinary tail: body too large -> not a wick signal
    normal = Candle(**{**c.__dict__, "open": 70000, "close": 70015, "high": 70020, "low": 69998})
    assert det.update(normal, _ind(normal, 1, 1, atr=20.0), None) == []
    # small range vs ATR -> nothing ; no ATR -> nothing
    assert det.update(upper, _ind(upper, 1, 1, atr=100.0), None) == []
    assert det.update(upper, _ind(upper, 1, 1, atr=None), None) == []


def test_liquidity_sweep_proximity_reclaim():
    c = make_candles(4)
    lvl_high = Level("swing_high", LevelSide.HIGH, 70100.0)
    lvl_low = Level("swing_low", LevelSide.LOW, 69900.0)
    det = LiquidityDetector(0.25)
    # bar 0: sweep of the high (high above, close back below) -> bearish reaction
    b0 = Candle(**{**c[0].__dict__, "open": 70080, "close": 70090, "high": 70110, "low": 70070})
    d = det.update(b0, _ind(b0, 1, 1, atr=40.0), "ASIA", [lvl_high, lvl_low])
    assert [(x.kind, x.direction) for x in d] == [("SWEEP", Direction.BEARISH)]
    assert d[0].label == "LIQUIDITY · sweep swing high" and d[0].payload["level_kind"] == "swing_high"
    # bar 1: proximity below the high (within 0.25*40 = 10)
    b1 = Candle(**{**c[1].__dict__, "open": 70085, "close": 70095, "high": 70098, "low": 70080})
    d = det.update(b1, _ind(b1, 1, 1, atr=40.0), "ASIA", [lvl_high, lvl_low])
    assert [(x.kind, x.direction) for x in d] == [("PROXIMITY", Direction.NEUTRAL)]
    # bar 2: close above the high
    b2 = Candle(**{**c[2].__dict__, "open": 70095, "close": 70120, "high": 70125, "low": 70090})
    assert det.update(b2, _ind(b2, 1, 1, atr=40.0), "ASIA", [lvl_high]) == []
    # bar 3: close back below -> bearish reclaim of the level
    b3 = Candle(**{**c[3].__dict__, "open": 70120, "close": 70090, "high": 70122, "low": 70085})
    d = det.update(b3, _ind(b3, 1, 1, atr=40.0), "ASIA", [lvl_high])
    assert [(x.kind, x.direction) for x in d] == [("RECLAIM", Direction.BEARISH)]
    # low-side sweep is a bullish REACTION, never a setup
    b4 = Candle(**{**c[0].__dict__, "open": 69920, "close": 69915, "high": 69930, "low": 69890})
    d = LiquidityDetector().update(b4, _ind(b4, 1, 1, atr=40.0), None, [lvl_low])
    assert d[0].kind == "SWEEP" and d[0].direction is Direction.BULLISH and d[0].family is DetectionFamily.LIQUIDITY


def test_build_levels_only_from_confirmed_inputs():
    t = datetime(2026, 9, 21, 4, 0, tzinfo=timezone.utc)
    piv = [Pivot("GOLD", "1", "e", Timeframe.M5, PivotKind.HIGH, 70100, StructureLabel.SH, t, t, 2),
           Pivot("GOLD", "1", "e", Timeframe.M5, PivotKind.HIGH, 70102, StructureLabel.EH, t, t, 2),
           Pivot("GOLD", "1", "e", Timeframe.M5, PivotKind.LOW, 69900, StructureLabel.SL, t, t, 2)]
    levels = build_levels(piv, 70300, 69700, None, None, equal_tolerance=5)
    kinds = sorted(l.kind for l in levels)
    assert kinds == ["equal_highs", "prev_day_high", "prev_day_low", "swing_high", "swing_high", "swing_low"]
    assert build_levels([], None, None, None, None, 1) == []


def test_breakout_sequence_and_second_failure():
    c = make_candles(8)
    lvl = Level("swing_high", LevelSide.HIGH, 70100.0)
    det = BreakoutDetector(acceptance_closes=2, retest_tolerance_atr=0.25)
    closes = [70080, 70120, 70130, 70125, 70090, 70115, 70085]
    lows = [70070, 70100, 70120, 70102, 70080, 70100, 70075]
    events = []
    for i, (cl, lo) in enumerate(zip(closes, lows)):
        b = Candle(**{**c[i].__dict__, "open": cl - 5, "close": cl, "high": cl + 10, "low": lo})
        events += [(d.kind, d.direction) for d in det.update(b, _ind(b, 1, 1, atr=20.0), "ASIA", [lvl])]
    assert events == [("BREAKOUT", Direction.BULLISH), ("ACCEPTANCE", Direction.BULLISH), ("RETEST", Direction.BULLISH),
                      ("FAILURE_INSIDE", Direction.BEARISH), ("RECLAIM", Direction.BULLISH), ("SECOND_FAILURE", Direction.BEARISH)]


def test_rsi_events():
    c = make_candles(6)
    det = RsiEventDetector([30, 50, 70])
    seq = [(45, RsiDirection.FLAT), (52, RsiDirection.RISING), (58, RsiDirection.RISING), (49, RsiDirection.FALLING),
           (72, RsiDirection.RISING), (68, RsiDirection.FALLING)]
    out = []
    for cd, (r, dr) in zip(c, seq):
        out += [d.kind for d in det.update(cd, _ind(cd, 1, 1, rsi=r, rsi_dir=dr), None)]
    assert out == ["CROSS_UP_50", "CROSS_DOWN_50", "TURN_DOWN", "CROSS_UP_50", "CROSS_UP_70", "TURN_UP", "CROSS_DOWN_70", "TURN_DOWN"]
