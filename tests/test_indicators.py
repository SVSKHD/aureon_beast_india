from __future__ import annotations

import math

import pytest

from aureon_mcx.indicators import IndicatorEngine, IndicatorParams, RsiDirection
from aureon_mcx.market.candle import Candle
from tests.conftest import make_candles


def _ref_ema(values, period):
    out = []
    ema = None
    for i, v in enumerate(values):
        if i + 1 < period:
            out.append(None)
            continue
        if ema is None:
            ema = sum(values[: period]) / period
        else:
            k = 2 / (period + 1)
            ema = (v - ema) * k + ema
        out.append(ema)
    return out


def test_ema_matches_reference_and_warmup():
    candles = make_candles(80)
    eng = IndicatorEngine(IndicatorParams(ema_fast=5, ema_slow=10, rsi_period=14, atr_period=14))
    rows = eng.warm(candles)
    closes = [c.close for c in candles]
    ref_fast = _ref_ema(closes, 5)
    ref_slow = _ref_ema(closes, 10)
    for r, f, s in zip(rows, ref_fast, ref_slow):
        assert (r.ema_fast is None) == (f is None)
        if f is not None:
            assert math.isclose(r.ema_fast, f, rel_tol=1e-12)
        if s is not None:
            assert math.isclose(r.ema_slow, s, rel_tol=1e-12)
            assert math.isclose(r.ema_gap, r.ema_fast - r.ema_slow, rel_tol=1e-12)
    assert rows[3].ema_fast is None and rows[4].ema_fast is not None
    assert rows[8].ema_slow is None and rows[9].ema_slow is not None
    assert rows[13].rsi is None and rows[14].rsi is not None
    assert rows[12].atr is None and rows[13].atr is not None
    assert not rows[13].warmed and rows[14].warmed


def test_rsi_bounds_and_direction():
    rising = make_candles(40, prices=[70000 + 10 * i for i in range(40)])
    eng = IndicatorEngine(IndicatorParams(ema_fast=5, ema_slow=10, rsi_period=14, atr_period=14))
    rows = eng.warm(rising)
    assert rows[-1].rsi == 100.0
    falling = make_candles(40, prices=[70000 - 10 * i for i in range(40)])
    rows = IndicatorEngine(IndicatorParams(ema_fast=5, ema_slow=10)).warm(falling)
    assert rows[-1].rsi == 0.0
    wobble = make_candles(60)
    rows = IndicatorEngine(IndicatorParams(ema_fast=5, ema_slow=10)).warm(wobble)
    for r in rows:
        if r.rsi is not None:
            assert 0.0 <= r.rsi <= 100.0
            assert r.rsi_direction in (RsiDirection.RISING, RsiDirection.FALLING, RsiDirection.FLAT)
    assert any(r.rsi_direction is RsiDirection.RISING for r in rows) and any(r.rsi_direction is RsiDirection.FALLING for r in rows)


def test_atr_positive_and_constant_range():
    candles = make_candles(30, prices=[70000.0] * 30)  # range always 10 (±5 wicks)
    rows = IndicatorEngine(IndicatorParams(ema_fast=5, ema_slow=10, atr_period=5)).warm(candles)
    assert all(math.isclose(r.atr, 10.0) for r in rows if r.atr is not None)


def test_engine_rejects_incomplete_or_out_of_order_candles():
    candles = make_candles(3)
    eng = IndicatorEngine(IndicatorParams())
    eng.update(candles[0])
    with pytest.raises(ValueError):
        eng.update(Candle(**{**candles[1].__dict__, "is_closed": False}))
    eng.update(candles[1])
    with pytest.raises(ValueError):
        eng.update(candles[0])


def test_indicators_persist_aligned_to_candle(repos):
    candles = repos.candles.insert_many(make_candles(60))
    eng = IndicatorEngine(IndicatorParams(ema_fast=5, ema_slow=10, rsi_period=14, atr_period=14))
    rows = [repos.indicators.insert(eng.update(c)) for c in candles]
    stored = repos.indicators.for_candles([c.id for c in candles])
    assert len(stored) == 60
    for c, r in zip(candles, rows):
        s = stored[c.id]
        assert s.open_time == c.open_time and s.candle_id == c.id
        assert s.ema_fast == r.ema_fast and s.rsi == r.rsi and s.atr == r.atr
        assert s.ema_fast_period == 5 and s.ema_slow_period == 10
    latest = repos.indicators.latest("428291", candles[0].timeframe, 5)
    assert [r.open_time for r in latest] == [c.open_time for c in candles[-5:]]
