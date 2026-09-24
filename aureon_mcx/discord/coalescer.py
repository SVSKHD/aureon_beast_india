"""Rate-limit safety: debounce rapid changes and skip unchanged cards.

* `offer(view)` keeps only the newest view per setup.
* `due(now)` releases a setup once its debounce window elapsed AND its state
  hash differs from the last rendered hash (message refs). Unchanged cards are
  never re-rendered, including after a restart.
"""
from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Callable

from aureon_mcx.views import SetupView

from .message_refs import MessageRefs


@dataclass
class _Pending:
    view: SetupView
    first_offered: float
    last_offered: float


class UpdateCoalescer:
    def __init__(self, refs: MessageRefs, debounce_seconds: float = 5.0, max_delay_seconds: float = 30.0,
                 clock: Callable[[], float] = time.monotonic):
        self.refs = refs
        self.debounce = debounce_seconds
        self.max_delay = max_delay_seconds
        self.clock = clock
        self._pending: dict[int, _Pending] = {}
        self.skipped_unchanged = 0
        self.rendered_hashes: dict[int, str] = {}

    def offer(self, view: SetupView) -> None:
        now = self.clock()
        p = self._pending.get(view.setup_id)
        if p is None:
            self._pending[view.setup_id] = _Pending(view, now, now)
        else:
            p.view, p.last_offered = view, now

    def due(self, now: float | None = None) -> list[SetupView]:
        now = self.clock() if now is None else now
        out: list[SetupView] = []
        for sid, p in list(self._pending.items()):
            quiet = now - p.last_offered >= self.debounce
            too_old = now - p.first_offered >= self.max_delay
            if not (quiet or too_old):
                continue
            del self._pending[sid]
            h = p.view.state_hash()
            last = self.rendered_hashes.get(sid) or self.refs.last_hash(sid)
            if last == h:
                self.skipped_unchanged += 1
                continue
            out.append(p.view)
        return out

    def mark_rendered(self, setup_id: int, state_hash: str) -> None:
        self.rendered_hashes[setup_id] = state_hash

    @property
    def pending_count(self) -> int:
        return len(self._pending)
