"""Historical cohort facts (no probabilities, no grades)."""
from __future__ import annotations

from dataclasses import dataclass
from statistics import median

from .labels import FAKEOUT, GOOD_CONTINUATION
from .models import OutcomeObservation


@dataclass(frozen=True)
class CohortStats:
    n: int
    horizon: str
    median_mfe_atr: float | None
    median_mae_atr: float | None
    continuation: int
    fakeout: int

    def lines(self) -> list[str]:
        fmt = lambda v: "n/a" if v is None else f"{v:.2f} ATR"  # noqa: E731
        return [
            "Historical cohort", f"n = {self.n}", "",
            f"Median favorable excursion: {fmt(self.median_mfe_atr)}", f"Median adverse excursion: {fmt(self.median_mae_atr)}", "",
            f"Continuation observed: {self.continuation} / {self.n}", f"Fakeout definition met: {self.fakeout} / {self.n}",
        ]


def cohort_stats(observations: list[OutcomeObservation], horizon: str, min_sample: int) -> CohortStats | None:
    finals = [o for o in observations if o.is_final and o.horizon == horizon]
    if len(finals) < min_sample:
        return None
    mfes = [o.mfe_atr for o in finals if o.mfe_atr is not None]
    maes = [o.mae_atr for o in finals if o.mae_atr is not None]
    return CohortStats(
        n=len(finals), horizon=horizon, median_mfe_atr=median(mfes) if mfes else None, median_mae_atr=median(maes) if maes else None,
        continuation=sum(1 for o in finals if o.label == GOOD_CONTINUATION), fakeout=sum(1 for o in finals if o.label == FAKEOUT),
    )
