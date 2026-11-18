"""Whom a turn the server posts on a thread runs for, when its job names
no one."""

from __future__ import annotations

from src.server.database.conversation import threads_read


async def turn_owner(payload: dict, thread_id: str) -> tuple[str | None, str | None]:
    """The user and workspace the turn runs for: the payload's, else the
    thread's (no user for a shared workspace's thread, whose job is then
    dropped rather than run as nobody)."""
    user_id = payload.get("user_id") or await threads_read.thread_person(thread_id)
    workspace_id = payload.get("workspace_id")
    if not workspace_id:
        row = await threads_read.get_thread_by_id(thread_id)
        workspace_id = row.get("workspace_id") if row else None
    return (
        str(user_id) if user_id else None,
        str(workspace_id) if workspace_id else None,
    )
