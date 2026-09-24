"""Discord message references: one per setup, with the last rendered state hash."""
from __future__ import annotations

from aureon_mcx.storage.repositories import Repositories


class MessageRefs:
    def __init__(self, repos: Repositories):
        self.repos = repos

    def last_hash(self, setup_id: int) -> str | None:
        row = self.repos.message_refs.get(setup_id)
        return row["last_rendered_state_hash"] if row else None

    def message(self, setup_id: int) -> tuple[str, str] | None:
        row = self.repos.message_refs.get(setup_id)
        return (row["channel_id"], row["message_id"]) if row else None

    def record(self, setup_id: int, channel_id: str | int, message_id: str | int, state_hash: str) -> None:
        self.repos.message_refs.upsert(setup_id, str(channel_id), str(message_id), state_hash)

    def forget(self, setup_id: int) -> None:
        self.repos.message_refs.delete(setup_id)

    def all_hashes(self) -> dict[int, str]:
        return {int(r["setup_id"]): r["last_rendered_state_hash"] for r in self.repos.message_refs.all()}
