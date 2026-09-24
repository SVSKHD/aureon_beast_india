"""Setup card: a small number of full-width sections, FINAL CHECK always last.

The builder consumes a SetupView only. No indicator or strategy recomputation.
"""
from __future__ import annotations

from dataclasses import dataclass, field

from aureon_mcx.market.timeutil import fmt_ist
from aureon_mcx.views import SetupView

FINAL_CHECK_NAME = "FINAL CHECK"
SECTION_ORDER = ["1 · SETUP", "2 · TREND", "3 · MOMENTUM", "4 · CONFIRMATION EVIDENCE", "5 · TRACE", "6 · HISTORICAL REFERENCE", FINAL_CHECK_NAME]
SEPARATOR = "─" * 28
TRADE_READY_WORDS = {"BUY", "SELL", "LONG", "SHORT", "ENTER", "EXECUTE NOW"}


@dataclass
class CardSection:
    name: str
    body: str
    inline: bool = False  # always full width


@dataclass
class CardSpec:
    title: str
    description: str
    sections: list[CardSection] = field(default_factory=list)
    color: int = 0x95A5A6
    footer: str = ""

    @property
    def final_check(self) -> CardSection:
        return self.sections[-1]

    def text(self) -> str:
        return "\n".join([self.title, self.description] + [f"{s.name}\n{s.body}" for s in self.sections] + [self.footer])


def _fmt(v: float | None, nd: int = 1) -> str:
    return "n/a" if v is None else f"{v:,.{nd}f}"


def _headline(view: SetupView) -> str:
    d = view.direction
    if view.state in ("CONFIRMED", "PULLBACK", "CONTINUATION"):
        return f"{d} SETUP · {view.state}"
    if view.is_terminal:
        return f"{d} SETUP · {view.state}"
    return f"{d} REACTION DETECTED"


def build_card(view: SetupView) -> CardSpec:
    sections: list[CardSection] = []
    # 1 · SETUP
    ev_lines = [f"• {e['to_state']} · {e['reason']} · {fmt_ist(e['open_time'], '%d %b %H:%M')}" for e in view.events[-4:]]
    setup_body = "\n".join([
        f"**{_headline(view)}**", "",
        f"State: **{view.state}**", f"Timeframe: {view.timeframe}", f"Anchor: {_fmt(view.anchor_price)}",
        f"Invalidation: {_fmt(view.invalidation_price)}", "Context: " + " · ".join(view.context_lines), "Events:"] + (ev_lines or ["• none"]))
    sections.append(CardSection(SECTION_ORDER[0], setup_body))
    # 2 · TREND
    sess = []
    for r in view.session_rows:
        tag = "current" if r["is_current"] else f"closed {r['session_date']}"
        sess.append(f"{r['session']}: {r['trend']} ({tag})")
    mtf = "\n".join(f"{tf:<4}{d}" for tf, d in view.mtf_rows)
    trend_body = "\n".join([f"Present trend: **{view.present_trend}**", "Session trend:"] + [f"• {s}" for s in (sess[-5:] or ["• none"])]
                           + ["", "```", mtf or "no timeframe reads", "```", f"Structure: {view.structure_context} · {view.structure_sequence or 'no confirmed swings'}"]
                           + (["⚠ EARLY REVERSAL / WATCH"] if view.early_reversal else []))
    sections.append(CardSection(SECTION_ORDER[1], trend_body))
    # 3 · MOMENTUM
    mom_body = "\n".join([
        f"EMA20: {_fmt(view.ema_fast)} · EMA50: {_fmt(view.ema_slow)} · {view.ema_relation}",
        f"Early EMA: {view.early_ema or 'none'}", f"Latest EMA cross: {view.latest_cross or 'none'}",
        f"RSI14: {_fmt(view.rsi)} ({view.rsi_direction or 'n/a'}) · ATR14: {_fmt(view.atr, 2)}",
        f"Volume: {_fmt(view.volume, 0)} · OI: {_fmt(view.open_interest, 0)}"])
    sections.append(CardSection(SECTION_ORDER[2], mom_body))
    # 4 · CONFIRMATION EVIDENCE
    conf_lines = ["Badges: " + (" · ".join(f"[{b}]" for b in view.badges) or "none"), f"MTF: {view.mtf_alignment}",
                  f"Structure: {view.structure_context}", "Fakeout flags:"] + [f"• {f}" for f in (view.fakeout_flags or ["none"])]
    if view.warnings:
        conf_lines += ["", *view.warnings]
    conf_lines += ["", "Blockers:"] + [f"• {b}" for b in (view.blockers or ["none"])]
    sections.append(CardSection(SECTION_ORDER[3], "\n".join(conf_lines)))
    # 5 · TRACE
    sections.append(CardSection(SECTION_ORDER[4], "\n".join(f"• {r}" for r in view.detection_refs) or "• none"))
    # 6 · HISTORICAL REFERENCE (only with enough outcome data)
    if view.cohort_lines:
        sections.append(CardSection(SECTION_ORDER[5], "\n".join(view.cohort_lines)))
    # FINAL CHECK - always last, visually isolated
    final_body = "\n".join([SEPARATOR, f"**{view.final_check}**", "Manual review required.", f"policy {view.policy_version}", SEPARATOR])
    sections.append(CardSection(FINAL_CHECK_NAME, final_body))
    spec = CardSpec(title=view.title, description=f"{view.subtitle}\n{view.display_symbol} · sec {view.security_id} · exp {view.expiry_date} · "
                    f"last close {_fmt(view.last_price)} @ {fmt_ist(view.updated_open_time)}",
                    sections=sections, color=0x2ECC71 if view.cleared else (0xE74C3C if view.blockers else 0x95A5A6),
                    footer="Observation only · no trade recommendation · analysis on closed candles")
    assert_card_layout(spec)
    return spec


def assert_card_layout(spec: CardSpec) -> None:
    """Enforced invariants: ordered full-width sections; FINAL CHECK is the last field."""
    names = [s.name for s in spec.sections]
    assert names[-1] == FINAL_CHECK_NAME, "FINAL CHECK must be the last full-width field"
    assert names.count(FINAL_CHECK_NAME) == 1
    order = [SECTION_ORDER.index(n) for n in names]
    assert order == sorted(order), f"sections out of order: {names}"
    assert all(not s.inline for s in spec.sections), "card sections must be full width"
    assert len(spec.sections) <= 7
    for s in spec.sections:
        for line in s.body.splitlines():
            assert line.strip().strip("*") not in TRADE_READY_WORDS, f"trade-ready wording is forbidden: {line!r}"


def to_embed(spec: CardSpec):
    import discord  # local import keeps the builder testable without a Discord runtime

    embed = discord.Embed(title=spec.title[:256], description=spec.description[:4096], color=spec.color)
    for s in spec.sections:
        embed.add_field(name=s.name[:256], value=s.body[:1024], inline=False)
    embed.set_footer(text=spec.footer[:2048])
    return embed
