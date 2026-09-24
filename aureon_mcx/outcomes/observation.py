"""Outcome observation over configured horizons from FUTURE closed candles only."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

from aureon_mcx.market.candle import Candle

from .labels import label_outcome
from .models import FeatureSnapshot, OutcomeObservation


@dataclass(frozen=True)
class Horizon:
    name: str
    bars: int | None  # None => session / end-of-window horizon


def horizons_from_config(bars: list[int], include_session: bool) -> list[Horizon]:
    out = [Horizon(f"{b}b", b) for b in bars]
    if include_session:
        out.append(Horizon("session", None))
    return out


def _window(snapshot: FeatureSnapshot, future: list[Candle], horizon: Horizon, session_end: datetime | None) -> tuple[list[Candle], bool]:
    fut = [c for c in future if c.is_closed and c.open_time > snapshot.open_time]
    fut.sort(key=lambda c: c.open_time)
    if horizon.bars is not None:
        return fut[: horizon.bars], len(fut) >= horizon.bars
    if session_end is None:
        return fut, False
    inside = [c for c in fut if c.open_time < session_end]
    final = any(c.open_time >= session_end for c in fut) or (bool(inside) and inside[-1].close_time >= session_end)
    return inside, final


def observe(snapshot: FeatureSnapshot, future: list[Candle], horizons: list[Horizon], follow_through_atr: float = 1.0,
            invalidation_atr: float = 1.0, session_end: datetime | None = None, session_closed: bool = False,
            setup_invalidated: bool = False) -> list[OutcomeObservation]:
    if snapshot.id is None:
        raise ValueError("snapshot must be persisted before outcomes are observed")
    long = snapshot.direction == "BULLISH"
    ref = snapshot.reference_price
    atr = snapshot.atr if snapshot.atr and snapshot.atr > 0 else None
    out: list[OutcomeObservation] = []
    for h in horizons:
        window, final = _window(snapshot, future, h, session_end)
        if h.bars is None and session_closed:
            final = True
        if not window:
            continue
        mfe = mae = 0.0
        t_mfe = t_mae = None
        inv_bar = ft_bar = None
        for i, c in enumerate(window, start=1):
            fav = (c.high - ref) if long else (ref - c.low)
            adv = (ref - c.low) if long else (c.high - ref)
            if fav > mfe:
                mfe, t_mfe = fav, i
            if adv > mae:
                mae, t_mae = adv, i
            if atr is not None:
                close_move = (c.close - ref) if long else (ref - c.close)
                if ft_bar is None and close_move >= follow_through_atr * atr:
                    ft_bar = i
                if inv_bar is None and close_move <= -invalidation_atr * atr:
                    inv_bar = i
        last = window[-1]
        final_move = (last.close - ref) if long else (ref - last.close)
        mfe_atr = mfe / atr if atr else None
        mae_atr = mae / atr if atr else None
        label, version = label_outcome(final, mfe_atr, mae_atr, (final_move / atr) if atr else None, ft_bar, inv_bar,
                                       setup_invalidated, follow_through_atr)
        out.append(OutcomeObservation(
            snapshot_id=snapshot.id, detection_id=snapshot.detection_id, horizon=h.name, horizon_bars=h.bars or len(window),
            bars_observed=len(window), mfe=mfe, mae=mae, mfe_atr=mfe_atr, mae_atr=mae_atr, time_to_mfe_bars=t_mfe,
            time_to_mae_bars=t_mae, bars_to_invalidation=inv_bar, bars_to_follow_through=ft_bar, final_move=final_move,
            label=label, label_version=version, last_candle_open_time=last.open_time, is_final=final,
        ))
    return out
