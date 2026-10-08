"""Shared accessors for checkpoint messages (``BaseMessage`` objects or plain dicts).

Messages in LangGraph state can be either ``BaseMessage`` instances or plain
dicts (e.g. reconstructed skill markers), so field access must tolerate both.
"""

from __future__ import annotations

from typing import Any

from langchain_core.messages import AIMessage, ToolMessage


def message_id(message: Any) -> str | None:
    """Framework-assigned id of a checkpoint message (object ``.id`` or dict ``id``)."""
    mid = getattr(message, "id", None)
    if mid is None and isinstance(message, dict):
        mid = message.get("id")
    return mid


def message_field(message: Any, name: str) -> Any:
    """A field of a checkpoint message in either typed or dict shape."""
    if isinstance(message, dict):
        return message.get(name)
    return getattr(message, name, None)


def is_tool_message(message: Any) -> bool:
    """True for a tool result in either typed (``ToolMessage``) or dict shape.

    The checkpoint reducer coerces every write to typed messages, but the
    orphan repair deletes what this misreads, so it does not trust that: a
    dict-shaped result read as something else would be dropped.
    """
    if isinstance(message, ToolMessage):
        return True
    if isinstance(message, dict):
        return message.get("role") == "tool" or message.get("type") == "tool"
    return False


def order_tool_results_first(messages: list) -> list:
    """Order each tool-result run so its ToolMessages precede any other content.

    A repair for history written before an attachment rode on the tool result. A
    visual Read used to resolve to a ``Command`` carrying
    ``[ToolMessage, HumanMessage]``, and a parallel batch interleaved that with
    the other calls' results as ``tool_result, text, image, tool_result``.
    Anthropic requires a user turn's tool_result blocks to come before any other
    content and 400s otherwise, and those messages are in checkpoints, so the bad
    order still replays on every turn of an existing thread.

    Nothing produces that shape any more; this exists for the threads that
    already carry it. Returns the original list when the order is already right.
    """
    out: list = []
    run: list = []
    in_run = False
    changed = False

    def flush() -> None:
        nonlocal changed
        if not run:
            return
        needs, seen_other = False, False
        for msg in run:
            if isinstance(msg, ToolMessage):
                if seen_other:
                    needs = True
                    break
            else:
                seen_other = True
        if needs:
            changed = True
            out.extend(m for m in run if isinstance(m, ToolMessage))
            out.extend(m for m in run if not isinstance(m, ToolMessage))
        else:
            out.extend(run)
        run.clear()

    for msg in messages:
        if isinstance(msg, AIMessage):
            flush()
            in_run = bool(getattr(msg, "tool_calls", None))
            out.append(msg)
        elif in_run:
            run.append(msg)
        else:
            out.append(msg)
    flush()

    return out if changed else messages
