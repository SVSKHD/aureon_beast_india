"""Formal, versioned outcome definitions (LABEL_VERSION = labels-v1).

good_continuation : follow-through reached, no invalidation within the horizon
fakeout           : favourable excursion (>= 0.5 * follow-through) then invalidation, or
                    follow-through reached and invalidation afterwards
wrong_direction   : invalidation reached with little favourable excursion first
no_follow_through : neither threshold reached and the move stayed small
ambiguous         : neither threshold reached but movement was not small
invalidated_setup : the linked setup lifecycle was INVALIDATED before follow-through
pending           : horizon not yet complete
"""
from __future__ import annotations

from .versions import LABEL_VERSION

GOOD_CONTINUATION = "good_continuation"
FAKEOUT = "fakeout"
WRONG_DIRECTION = "wrong_direction"
NO_FOLLOW_THROUGH = "no_follow_through"
AMBIGUOUS = "ambiguous"
INVALIDATED_SETUP = "invalidated_setup"
PENDING = "pending"

ALL_LABELS = (GOOD_CONTINUATION, FAKEOUT, WRONG_DIRECTION, NO_FOLLOW_THROUGH, AMBIGUOUS, INVALIDATED_SETUP, PENDING)


def label_outcome(is_final: bool, mfe_atr: float | None, mae_atr: float | None, final_move_atr: float | None,
                  bars_to_follow_through: int | None, bars_to_invalidation: int | None, setup_invalidated: bool = False,
                  follow_through_atr: float = 1.0) -> tuple[str, str]:
    if not is_final:
        return PENDING, LABEL_VERSION
    ft, inv = bars_to_follow_through, bars_to_invalidation
    invalidated_first = inv is not None and (ft is None or inv < ft)
    if ft is not None and not invalidated_first:
        return (FAKEOUT if inv is not None else GOOD_CONTINUATION), LABEL_VERSION
    if setup_invalidated and ft is None:
        return INVALIDATED_SETUP, LABEL_VERSION
    if invalidated_first:
        favourable = (mfe_atr or 0.0) >= 0.5 * follow_through_atr
        return (FAKEOUT if favourable else WRONG_DIRECTION), LABEL_VERSION
    small = abs(final_move_atr or 0.0) < 0.25 and (mfe_atr or 0.0) < 0.5 and (mae_atr or 0.0) < 0.5
    return (NO_FOLLOW_THROUGH if small else AMBIGUOUS), LABEL_VERSION
