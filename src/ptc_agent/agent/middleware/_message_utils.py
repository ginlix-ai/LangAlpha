"""Shared accessors for checkpoint messages (``BaseMessage`` objects or plain dicts).

Messages in LangGraph state can be either ``BaseMessage`` instances or plain
dicts (e.g. reconstructed skill markers), so field access must tolerate both.
"""

from __future__ import annotations

from typing import Any


def message_id(message: Any) -> str | None:
    """Framework-assigned id of a checkpoint message (object ``.id`` or dict ``id``)."""
    mid = getattr(message, "id", None)
    if mid is None and isinstance(message, dict):
        mid = message.get("id")
    return mid
