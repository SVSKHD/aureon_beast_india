"""Server-side analysis chart from STORED rows only.

`plan_chart` is a pure function (testable without rendering) that decides what
gets drawn and which few items receive text labels. `render_png` draws the plan
with matplotlib. This module never imports the indicator or detection engines.
"""
from __future__ import annotations

import io
from dataclasses import dataclass, field
from datetime import datetime

from aureon_mcx.config.yaml_models import DeclutterSpec
from aureon_mcx.detection.models import Detection, DetectionFamily
from aureon_mcx.indicators.models import IndicatorRow
from aureon_mcx.market.candle import Candle
from aureon_mcx.market.timeutil import fmt_ist
from aureon_mcx.setups.models import SetupEvent
from aureon_mcx.structure.models import Pivot, PivotKind
from aureon_mcx.views import SetupView

ABOVE_LABELS = {"HH", "LH", "SH", "EH"}
BELOW_LABELS = {"HL", "LL", "SL", "EL"}


@dataclass(frozen=True)
class ChartLabel:
    x: int
    y: float
    text: str
    kind: str          # pivot | ema_cross | structure | detection | setup_event | anchor | invalidation
    above: bool
    direction: str = "NEUTRAL"


@dataclass(frozen=True)
class ChartMarker:
    x: int
    y: float
    kind: str
    direction: str
    above: bool


@dataclass
class ChartPlan:
    title: str
    subtitle: str
    candles: list[Candle]
    ema_fast: list[float | None]
    ema_slow: list[float | None]
    rsi: list[float | None]
    volume: list[float]
    pivot_labels: list[ChartLabel] = field(default_factory=list)
    high_path: list[tuple[int, float]] = field(default_factory=list)
    low_path: list[tuple[int, float]] = field(default_factory=list)
    markers: list[ChartMarker] = field(default_factory=list)
    text_labels: list[ChartLabel] = field(default_factory=list)
    rsi_marks: list[tuple[int, float, str]] = field(default_factory=list)
    anchor: float | None = None
    invalidation: float | None = None
    context_lines: list[str] = field(default_factory=list)

    @property
    def labeled_texts(self) -> list[str]:
        return [l.text for l in self.text_labels]


def _index(candles: list[Candle]) -> dict[datetime, int]:
    return {c.open_time: i for i, c in enumerate(candles)}


def plan_chart(view: SetupView, candles: list[Candle], indicators: dict[int, IndicatorRow], pivots: list[Pivot],
               detections: list[Detection], events: list[SetupEvent], declutter: DeclutterSpec) -> ChartPlan:
    candles = candles[-declutter.chart_bars:]
    idx = _index(candles)
    ema_f, ema_s, rsi, vol = [], [], [], []
    for c in candles:
        ind = indicators.get(c.id) if c.id is not None else None
        ema_f.append(ind.ema_fast if ind else None)
        ema_s.append(ind.ema_slow if ind else None)
        rsi.append(ind.rsi if ind else None)
        vol.append(c.volume)
    # matplotlib's default fonts have no emoji glyphs: the chart subtitle is plain text
    subtitle = f"{view.state} · {'CLEARED FOR REVIEW' if view.cleared else 'NOT CLEARED'}"
    plan = ChartPlan(title=view.title, subtitle=subtitle, candles=candles, ema_fast=ema_f, ema_slow=ema_s, rsi=rsi, volume=vol,
                     anchor=view.anchor_price, invalidation=view.invalidation_price)
    # structure: labels above (highs) / below (lows) + one path per pivot kind
    for p in sorted(pivots, key=lambda p: p.pivot_open_time):
        i = idx.get(p.pivot_open_time)
        if i is None:
            continue
        above = p.kind is PivotKind.HIGH
        assert (p.label.value in ABOVE_LABELS) == above
        plan.pivot_labels.append(ChartLabel(i, p.price, p.label.value, "pivot", above))
        (plan.high_path if above else plan.low_path).append((i, p.price))
    # detections: every one is a small marker; only EMA crosses, structure transitions and the newest N get text
    in_window = [d for d in sorted(detections, key=lambda d: (d.open_time, d.id or 0)) if d.open_time in idx]
    always = [d for d in in_window if d.is_ema_cross or d.family is DetectionFamily.STRUCTURE]
    others = [d for d in in_window if d not in always and d.family is not DetectionFamily.RSI]
    labeled_ids = {id(d) for d in always} | {id(d) for d in others[-declutter.max_labeled_detections:]} if declutter.max_labeled_detections else {id(d) for d in always}
    for d in in_window:
        i = idx[d.open_time]
        c = candles[i]
        if d.family is DetectionFamily.RSI:
            r = rsi[i]
            if r is not None:
                plan.rsi_marks.append((i, r, d.label))
            continue
        above = d.direction.value == "BEARISH" or d.kind in ("UPPER_REJECTION",)
        y = c.high if above else c.low
        kind = "ema_cross" if d.is_ema_cross else ("structure" if d.family is DetectionFamily.STRUCTURE else "detection")
        plan.markers.append(ChartMarker(i, y, kind, d.direction.value, above))
        if id(d) in labeled_ids:
            plan.text_labels.append(ChartLabel(i, y, d.label, kind, above, d.direction.value))
    # setup events: newest few
    ev_in = [e for e in events if e.open_time in idx]
    for e in ev_in[-declutter.max_labeled_setup_events:] if declutter.max_labeled_setup_events else []:
        i = idx[e.open_time]
        plan.text_labels.append(ChartLabel(i, candles[i].close, e.to_state.value, "setup_event", False, view.direction))
    # bounded market-context panel
    ctx = [f"present {view.present_trend} · structure {view.structure_context}", f"{view.structure_sequence or 'no confirmed swings'}",
           " ".join(f"{tf}:{d}" for tf, d in view.mtf_rows), view.mtf_alignment,
           f"{view.ema_relation} · RSI {view.rsi:.1f}" if view.rsi is not None else view.ema_relation,
           "flags: " + (", ".join(f for f in view.fakeout_flags if not f.startswith("·")) or "none"),
           "blockers: " + ("; ".join(view.blockers[:3]) + (" …" if len(view.blockers) > 3 else "") if view.blockers else "none"),
           "CLEARED FOR REVIEW" if view.cleared else "NOT CLEARED"]
    plan.context_lines = ctx[: declutter.context_panel_lines]
    return plan


def render_png(plan: ChartPlan) -> bytes:
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    from matplotlib.patches import Rectangle

    n = len(plan.candles)
    fig = plt.figure(figsize=(16, 11), dpi=110)
    gs = fig.add_gridspec(4, 1, height_ratios=[6, 1.2, 1.6, 1.4], hspace=0.08)
    ax = fig.add_subplot(gs[0])
    axv = fig.add_subplot(gs[1], sharex=ax)
    axr = fig.add_subplot(gs[2], sharex=ax)
    axc = fig.add_subplot(gs[3])
    up, dn = "#2ecc71", "#e74c3c"
    for i, c in enumerate(plan.candles):
        col = up if c.close >= c.open else dn
        ax.vlines(i, c.low, c.high, color=col, linewidth=0.8)
        ax.add_patch(Rectangle((i - 0.35, min(c.open, c.close)), 0.7, max(abs(c.close - c.open), 1e-9), color=col, linewidth=0))
    xs = list(range(n))
    ax.plot(xs, [v if v is not None else float("nan") for v in plan.ema_fast], color="#f1c40f", linewidth=1.2, label="EMA20")
    ax.plot(xs, [v if v is not None else float("nan") for v in plan.ema_slow], color="#3498db", linewidth=1.2, label="EMA50")
    if plan.high_path:
        ax.plot([x for x, _ in plan.high_path], [y for _, y in plan.high_path], color="#e67e22", linewidth=0.9, linestyle="--", alpha=0.8)
    if plan.low_path:
        ax.plot([x for x, _ in plan.low_path], [y for _, y in plan.low_path], color="#1abc9c", linewidth=0.9, linestyle="--", alpha=0.8)
    for l in plan.pivot_labels:
        ax.annotate(l.text, (l.x, l.y), xytext=(0, 9 if l.above else -12), textcoords="offset points", ha="center", fontsize=8,
                    color="#e67e22" if l.above else "#1abc9c", fontweight="bold")
    for m in plan.markers:
        color = {"ema_cross": "#9b59b6", "structure": "#e67e22", "detection": "#7f8c8d"}[m.kind]
        ax.plot(m.x, m.y, marker="v" if m.above else "^", color=color, markersize=6 if m.kind == "ema_cross" else 4, linestyle="none")
    for l in plan.text_labels:
        ax.annotate(l.text, (l.x, l.y), xytext=(0, 22 if l.above else -26), textcoords="offset points", ha="center", fontsize=7,
                    color="#9b59b6" if l.kind == "ema_cross" else "#34495e",
                    bbox=dict(boxstyle="round,pad=0.2", fc="white", ec="#bdc3c7", alpha=0.85))
    if plan.anchor is not None:
        ax.axhline(plan.anchor, color="#2c3e50", linewidth=0.9, linestyle="-.", label="anchor")
    if plan.invalidation is not None:
        ax.axhline(plan.invalidation, color="#c0392b", linewidth=0.9, linestyle=":", label="invalidation")
    ax.set_title(f"{plan.title}\n{plan.subtitle}", fontsize=13, loc="left")
    ax.legend(loc="upper left", fontsize=8, ncol=4)
    ax.grid(alpha=0.15)
    axv.bar(xs, plan.volume, color="#95a5a6", width=0.7)
    axv.set_ylabel("vol", fontsize=8)
    axr.plot(xs, [v if v is not None else float("nan") for v in plan.rsi], color="#8e44ad", linewidth=1.0)
    for lvl, col in ((70, "#e74c3c"), (50, "#7f8c8d"), (30, "#2ecc71")):
        axr.axhline(lvl, color=col, linewidth=0.6, linestyle="--")
    for x, y, text in plan.rsi_marks[-6:]:
        axr.plot(x, y, marker="o", color="#8e44ad", markersize=3)
    axr.set_ylim(0, 100)
    axr.set_ylabel("RSI14", fontsize=8)
    ticks = list(range(0, n, max(1, n // 8)))
    axr.set_xticks(ticks)
    axr.set_xticklabels([fmt_ist(plan.candles[i].open_time, "%d %b %H:%M") for i in ticks], fontsize=7)
    plt.setp(ax.get_xticklabels(), visible=False)
    plt.setp(axv.get_xticklabels(), visible=False)
    axc.axis("off")
    axc.text(0.01, 0.95, "\n".join(plan.context_lines), va="top", ha="left", fontsize=9, family="monospace", transform=axc.transAxes)
    buf = io.BytesIO()
    fig.savefig(buf, format="png", bbox_inches="tight")
    plt.close(fig)
    return buf.getvalue()


def render_chart_from_storage(repos, view: SetupView, declutter: DeclutterSpec, timeframe) -> bytes:
    """Load stored rows and render. The chart never derives a signal the observer did not store."""
    candles = repos.candles.latest(view.security_id, timeframe, declutter.chart_bars)
    if not candles:
        return b""
    indicators = repos.indicators.for_candles([c.id for c in candles if c.id is not None])
    pivots = repos.pivots.latest(view.security_id, timeframe, 60, confirmed_by=candles[-1].open_time)
    detections = repos.detections.latest(view.security_id, timeframe, 300, since=candles[0].open_time)
    events = repos.setups.events(view.setup_id)
    return render_png(plan_chart(view, candles, indicators, pivots, detections, events, declutter))
