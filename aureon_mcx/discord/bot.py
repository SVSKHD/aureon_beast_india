"""Discord bot: read-only presentation of stored setup views.

* `DiscordSink` receives SetupView objects from the observer and coalesces them.
* Cards are created once per setup and PATCHed only when the state hash changed.
* `Monitor` button subscribes a user to updates for that setup.
* No order placement exists. `execute_request` refuses unless execution.enabled
  is true, and even then there is no execution code path in this phase.
"""
from __future__ import annotations

import asyncio
import io
import logging
from typing import Any

from aureon_mcx.config import AppConfig
from aureon_mcx.logging_setup import kv
from aureon_mcx.storage.repositories import Repositories
from aureon_mcx.views import SetupView

from .card_builder import build_card, to_embed
from .chart_renderer import render_chart_from_storage
from .coalescer import UpdateCoalescer
from .message_refs import MessageRefs

log = logging.getLogger("aureon.discord")


class ExecutionDisabled(RuntimeError):
    pass


def execute_request(cfg: AppConfig, repos: Repositories, setup_id: int) -> dict[str, Any]:
    """Future-compatibility gate. Always re-reads storage; refuses when the flag is off."""
    if not cfg.analysis.execution.enabled:
        raise ExecutionDisabled("execution is disabled by config (execution.enabled=false); no auto-trade in this phase")
    setup = repos.setups.get(setup_id)
    clearance = repos.clearances.latest_for_setup(setup_id) if setup else None
    if setup is None or clearance is None or not clearance.cleared:
        raise ExecutionDisabled("setup is not CLEARED FOR REVIEW in storage; refusing")
    raise ExecutionDisabled("order placement is not implemented in this phase")


class DiscordSink:
    """PresentationSink implementation; safe to call from the event loop thread."""

    def __init__(self, coalescer: UpdateCoalescer):
        self.coalescer = coalescer
        self.status_text: str | None = None
        self.status_dirty = False

    def publish(self, view: SetupView) -> None:
        self.coalescer.offer(view)

    def status(self, text: str) -> None:
        self.status_text = text
        self.status_dirty = True


class MonitorRegistry:
    """Monitor subscriptions persisted in `monitor_subscriptions` (restored on startup)."""

    def __init__(self, repos: Repositories | None = None):
        self.repos = repos
        self.subscribers: dict[int, set[str]] = {}
        if repos is not None:
            for sid, users in repos.monitors.all().items():
                self.subscribers[sid] = set(users)

    def add(self, setup_id: int, user_id: int | str) -> bool:
        uid = str(user_id)
        s = self.subscribers.setdefault(setup_id, set())
        if uid in s:
            return False
        s.add(uid)
        if self.repos is not None:
            try:
                self.repos.monitors.add(setup_id, uid)
            except Exception as exc:  # noqa: BLE001 - e.g. setup row missing; keep the in-memory subscription
                log.warning("monitor_persist_failed %s", kv(setup_id=setup_id, error=type(exc).__name__))
        return True

    def mentions(self, setup_id: int) -> str:
        return " ".join(f"<@{u}>" for u in sorted(self.subscribers.get(setup_id, set())))


def create_client(cfg: AppConfig, repos: Repositories, sink: DiscordSink, refs: MessageRefs, monitors: MonitorRegistry):
    import discord

    intents = discord.Intents.default()

    class MonitorView(discord.ui.View):
        def __init__(self, setup_id: int):
            super().__init__(timeout=None)
            self.setup_id = setup_id
            btn = discord.ui.Button(label="Monitor", style=discord.ButtonStyle.secondary, custom_id=f"aureon:monitor:{setup_id}")
            btn.callback = self._monitor  # type: ignore[assignment]
            self.add_item(btn)
            # NOTE: no Execute button. Execution is absent in this phase (execution.enabled defaults to false).

        async def _monitor(self, interaction: discord.Interaction):
            added = monitors.add(self.setup_id, interaction.user.id)
            await interaction.response.send_message(
                f"{'Subscribed to' if added else 'Already monitoring'} setup #{self.setup_id} updates. Observation only.", ephemeral=True)

    class AureonClient(discord.Client):
        def __init__(self):
            super().__init__(intents=intents)
            self.channel_id = cfg.env.DISCORD_CHANNEL_ID
            self._status_message = None
            self._flush_task: asyncio.Task | None = None

        async def setup_hook(self) -> None:
            for row in repos.message_refs.all():
                self.add_view(MonitorView(int(row["setup_id"])), message_id=int(row["message_id"]))
            self._flush_task = asyncio.create_task(self._flush_loop())

        async def on_ready(self) -> None:
            log.info("discord live %s", kv(user=str(self.user), channel=self.channel_id))

        async def on_interaction(self, interaction: discord.Interaction) -> None:
            data = interaction.data or {}
            cid = str(data.get("custom_id", ""))
            if cid.startswith("aureon:monitor:") and not interaction.response.is_done():
                sid = int(cid.rsplit(":", 1)[1])
                added = monitors.add(sid, interaction.user.id)
                await interaction.response.send_message(
                    f"{'Subscribed to' if added else 'Already monitoring'} setup #{sid} updates. Observation only.", ephemeral=True)

        async def _channel(self):
            ch = self.get_channel(self.channel_id) if self.channel_id else None
            if ch is None and self.channel_id:
                ch = await self.fetch_channel(self.channel_id)
            return ch

        async def _flush_loop(self) -> None:
            await self.wait_until_ready()
            while not self.is_closed():
                try:
                    await self._flush_once()
                except Exception as exc:  # noqa: BLE001
                    log.exception("discord_flush_error %s", kv(error=type(exc).__name__))
                await asyncio.sleep(1.0)

        async def _flush_once(self) -> None:
            ch = await self._channel()
            if ch is None:
                return
            if sink.status_dirty and sink.status_text:
                sink.status_dirty = False
                try:
                    if self._status_message is None:
                        self._status_message = await ch.send(content=sink.status_text[:1900])
                    else:
                        await self._status_message.edit(content=sink.status_text[:1900])
                except discord.HTTPException as exc:
                    log.warning("discord_status_failed %s", kv(status=exc.status))
            for view in sink.coalescer.due():
                await self._render_and_send(ch, view)

        async def _render_and_send(self, ch, view: SetupView) -> None:
            spec = build_card(view)
            embed = to_embed(spec)
            # Chart rendering is off the event loop (to_thread; Database serialises access with
            # its own lock). A chart failure never loses the card update: send it without the chart.
            try:
                png = await asyncio.to_thread(render_chart_from_storage, repos, view, cfg.analysis.declutter, cfg.primary_timeframe)
            except Exception as exc:  # noqa: BLE001
                log.exception("discord_chart_failed %s", kv(setup_id=view.setup_id, error=type(exc).__name__))
                png = b""
                spec.footer = spec.footer + " · chart unavailable (render error logged)"
                embed = to_embed(spec)
            h = view.state_hash()
            ref = refs.message(view.setup_id)
            files = [discord.File(io.BytesIO(png), filename=f"setup_{view.setup_id}.png")] if png else []
            if files:
                embed.set_image(url=f"attachment://setup_{view.setup_id}.png")
            try:
                if ref is not None:
                    msg = await ch.fetch_message(int(ref[1]))
                    await msg.edit(embed=embed, attachments=files)
                    log.info("discord_card_patched %s", kv(setup_id=view.setup_id, state=view.state, cleared=view.cleared))
                else:
                    msg = await ch.send(embed=embed, files=files, view=MonitorView(view.setup_id))
                    log.info("discord_card_created %s", kv(setup_id=view.setup_id, state=view.state, cleared=view.cleared))
                refs.record(view.setup_id, ch.id, msg.id, h)
                sink.coalescer.mark_rendered(view.setup_id, h)
                mention = monitors.mentions(view.setup_id)
                if mention and ref is not None:
                    await ch.send(content=f"{mention} setup #{view.setup_id}: {view.state} · {view.final_check}", delete_after=600)
            except discord.NotFound:
                refs.forget(view.setup_id)  # message deleted by a human: recreate on the next material change
                sink.coalescer.rendered_hashes.pop(view.setup_id, None)
            except discord.HTTPException as exc:
                log.warning("discord_send_failed %s", kv(setup_id=view.setup_id, status=exc.status))

    return AureonClient()
