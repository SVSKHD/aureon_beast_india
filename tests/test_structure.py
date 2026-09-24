from __future__ import annotations

import pytest

from aureon_mcx.market.candle import Candle
from aureon_mcx.structure import PivotKind, StructureContext, StructureLabel
from aureon_mcx.structure.engine import StructureEngine
from tests.conftest import make_candles


def _series(highs_lows):
    """Build candles from (high, low) pairs with open/close inside the range."""
    base = make_candles(len(highs_lows))
    out = []
    for c, (h, l) in zip(base, highs_lows):
        mid = (h + l) / 2
        out.append(Candle(**{**c.__dict__, "open": mid, "close": mid, "high": h, "low": l}))
    return out


def test_pivot_confirmed_only_after_strength_candles_close():
    # bar 2 is a swing high (10 > 8,9 and > 9,8); strength 2 -> confirmed at bar 4
    pairs = [(8, 1), (9, 2), (10, 3), (9, 2), (8, 1), (9, 2)]
    eng = StructureEngine(strength=2)
    cs = _series(pairs)
    exposed = {}
    for i, c in enumerate(cs):
        upd = eng.update(c)
        for p in upd.confirmed:
            exposed[i] = p
    assert list(exposed) == [4]  # never before the 2 later candles are closed
    p = exposed[4]
    assert p.kind is PivotKind.HIGH and p.price == 10 and p.label is StructureLabel.SH
    assert p.pivot_open_time == cs[2].open_time
    assert p.confirmed_at_open_time == cs[4].open_time
    assert p.confirmed_at_open_time > p.pivot_open_time


def test_labels_hh_hl_lh_ll_eh_el():
    # Uptrend: highs 10, 12 ; lows 5, 7  -> SH, HH ; SL, HL
    pairs = [(9, 6), (10, 5), (9, 6), (8, 6), (9, 7), (10, 6), (12, 7), (11, 8), (10, 7), (11, 8), (12, 9), (13, 8), (12, 9), (11, 10)]
    eng = StructureEngine(strength=1)
    labels = []
    for c in _series(pairs):
        labels += [(p.kind, p.label, p.price) for p in eng.update(c).confirmed]
    highs = [(l, pr) for k, l, pr in labels if k is PivotKind.HIGH]
    lows = [(l, pr) for k, l, pr in labels if k is PivotKind.LOW]
    assert highs[0] == (StructureLabel.SH, 10) and (StructureLabel.HH, 12) in highs
    assert lows[0] == (StructureLabel.SL, 5) and (StructureLabel.HL, 6) in lows or (StructureLabel.HL, 7) in lows

    # Downtrend: LH / LL
    pairs = [(10, 7), (12, 8), (10, 6), (9, 4), (10, 5), (8, 3), (9, 4), (7, 2), (8, 3)]
    eng = StructureEngine(strength=1)
    seq = []
    for c in _series(pairs):
        seq += [p.label for p in eng.update(c).confirmed]
    assert StructureLabel.LH in seq and StructureLabel.LL in seq
    assert eng.context() is StructureContext.BEARISH
    assert "LH" in eng.sequence() and "LL" in eng.sequence()

    # Equal high / equal low with ATR-fraction tolerance
    pairs = [(9, 4), (10, 5), (9, 4), (8, 3), (9, 4), (10.05, 5), (9, 4), (8, 3.02), (9, 4)]
    eng = StructureEngine(strength=1, equal_tolerance_mode="atr_fraction", equal_tolerance_value=0.1)
    seq = []
    for c in _series(pairs):
        seq += [p.label for p in eng.update(c, atr=1.0).confirmed]  # tol = 0.1
    assert StructureLabel.EH in seq and StructureLabel.EL in seq


def test_ticks_tolerance_and_no_atr_fails_closed():
    pairs = [(9, 4), (10, 5), (9, 4), (8, 3), (9, 4), (10.5, 5), (9, 4)]
    eng = StructureEngine(strength=1, equal_tolerance_mode="ticks", equal_tolerance_value=1, tick_size=1.0)
    seq = []
    for c in _series(pairs):
        seq += [p.label for p in eng.update(c).confirmed]
    assert StructureLabel.EH in seq
    eng2 = StructureEngine(strength=1, equal_tolerance_mode="atr_fraction", equal_tolerance_value=0.5)
    seq2 = []
    for c in _series(pairs):
        seq2 += [p.label for p in eng2.update(c, atr=None).confirmed]  # no ATR -> strict -> HH not EH
    assert StructureLabel.HH in seq2 and StructureLabel.EH not in seq2


def test_context_mixed_is_not_forced():
    pairs = [(9, 6), (10, 5), (9, 6), (8, 4), (9, 5), (12, 7), (11, 6), (10, 3), (11, 4)]
    eng = StructureEngine(strength=1)
    for c in _series(pairs):
        eng.update(c)
    assert eng.context() is StructureContext.MIXED
    assert eng.state_dict()["context"] == "MIXED"


def test_context_transition_reported():
    pairs = [(10, 7), (12, 8), (10, 6), (9, 4), (10, 5), (8, 3), (9, 4), (7, 2), (8, 3)]
    eng = StructureEngine(strength=1)
    changes = []
    for c in _series(pairs):
        u = eng.update(c)
        if u.context_changed:
            changes.append((u.previous_context, u.context))
    assert changes == [(StructureContext.MIXED, StructureContext.BEARISH)]


def test_engine_rejects_incomplete_candle():
    eng = StructureEngine()
    c = make_candles(1)[0]
    with pytest.raises(ValueError):
        eng.update(Candle(**{**c.__dict__, "is_closed": False}))
