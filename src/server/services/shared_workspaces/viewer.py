"""How a shared workspace's thread reads to one member: who sent what, and a
turn paused for someone else's answer."""

from __future__ import annotations

import json
from typing import Any, Optional

from src.server.database.conversation.senders import thread_senders, turn_senders
from src.server.database.user import names_of
from src.server.models.conversation import (
    SharedThreadInfo,
    ThreadSender,
    WorkspaceThreadListItem,
)


def as_seen_by(
    status: Optional[str],
    interrupt_reason: Optional[str],
    sender: Optional[str],
    viewer: str,
) -> tuple[Optional[str], Optional[str]]:
    """A turn paused for its sender's answer reads to every other member as
    their turn still running, with no reason: only the sender is asked."""
    if status == "interrupted" and sender != viewer:
        return "running", None
    return status, interrupt_reason


async def name_thread_senders(items: list[WorkspaceThreadListItem], viewer: str) -> None:
    """Name each shared thread's starter and latest sender."""
    found = await thread_senders([i.thread_id for i in items])
    for item in items:
        row = found.get(item.thread_id) or {}
        item.run_status, item.interrupt_reason = as_seen_by(
            item.run_status, item.interrupt_reason, row.get("last_sender"), viewer
        )
        item.shared = SharedThreadInfo(
            created_by=ThreadSender(user_id=row["created_by"], name=row.get("created_by_name"))
            if row.get("created_by") else None,
            last_sender=ThreadSender(user_id=row["last_sender"], name=row.get("last_sender_name"))
            if row.get("last_sender") else None,
            shared_with_workspace=bool(row.get("shared_with_workspace")),
            others_sent=bool(row.get("others_sent")),
        )


async def name_turn_senders(thread_id: str, lines: list[Any]) -> None:
    """Put each replayed user message under the member who sent its turn."""
    senders = await turn_senders(thread_id)
    for line in lines:
        if line.event != "user_message":
            continue
        data = json.loads(line.data_json)
        sender = senders.get(data.get("turn_index"))
        if sender is not None:
            data["sender"] = sender
            line.data_json = json.dumps(data, ensure_ascii=False, default=str)


async def name_steer_senders(steers: list[dict]) -> list[dict]:
    """Waiting steers, each under the member who queued it, as the replay
    names a turn's sender: a stop by another member keeps them on the
    thread, where every member's view draws them."""
    names = await names_of(sorted({s["user_id"] for s in steers if s.get("user_id")}))
    for steer in steers:
        user_id = steer.pop("user_id", None)
        if user_id:
            steer["sender"] = {"user_id": user_id, "name": names.get(user_id)}
    return steers
