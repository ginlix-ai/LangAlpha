"""Shared utility functions for middleware."""

from langchain_core.messages import SystemMessage
from langgraph.config import get_config


def run_ids() -> tuple[str | None, str]:
    """The thread of the graph run this is called in and which agent runs it,
    for a log line: ``main``, or a subagent's ``task:<id>``. A subagent runs
    on its parent's thread, so only the ``task:`` segment of its checkpoint
    namespace tells its lines apart; the rest names the node, which says
    nothing here. ``(None, "main")`` outside a run."""
    try:
        configurable = get_config().get("configurable") or {}
    except RuntimeError:
        return None, "main"
    segments = (configurable.get("checkpoint_ns") or "").split("|")
    return configurable.get("thread_id"), next((s for s in segments if s.startswith("task:")), "main")


def append_to_system_message(
    system_message: SystemMessage | None,
    text: str,
) -> SystemMessage:
    """Append text to a system message.

    Args:
        system_message: Existing system message or None.
        text: Text to add to the system message.

    Returns:
        New SystemMessage with the text appended.
    """
    new_content: list[str | dict[str, str]] = list(system_message.content_blocks) if system_message else []
    if new_content:
        text = f"\n\n{text}"
    new_content.append({"type": "text", "text": text})
    return SystemMessage(content=new_content)
