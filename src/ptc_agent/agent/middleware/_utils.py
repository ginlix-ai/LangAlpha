"""Shared utility functions for middleware."""

from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from typing import Any

from langchain_core.messages import SystemMessage
from langchain_core.runnables import RunnableConfig
from langchain_core.runnables.config import var_child_runnable_config
from langgraph.config import get_config
from langgraph.constants import TAG_NOSTREAM


def configurable(config: Mapping[str, Any] | None = None) -> dict[str, Any]:
    """``config``'s ``configurable``, or the current run's; empty outside a
    run, so a caller reads a key with no run to report into."""
    if config is None:
        try:
            config = get_config()
        except RuntimeError:
            return {}
    return config.get("configurable") or {}


def run_ids() -> tuple[str | None, str]:
    """The thread of the graph run this is called in and which agent runs it,
    for a log line: ``main``, or a subagent's ``task:<id>``. A subagent runs
    on its parent's thread, so only the ``task:`` segment of its checkpoint
    namespace tells its lines apart; the rest names the node, which says
    nothing here. ``(None, "main")`` outside a run."""
    current = configurable()
    segments = (current.get("checkpoint_ns") or "").split("|")
    return current.get("thread_id"), next((s for s in segments if s.startswith("task:")), "main")


def nostream(config: RunnableConfig) -> RunnableConfig:
    """``config`` with the tag that keeps what runs under it off the
    ``messages`` stream; the callbacks that bill and trace it still see it."""
    return {**config, "tags": [*(config.get("tags") or ()), TAG_NOSTREAM]}


@contextmanager
def unstreamed() -> Iterator[None]:
    """Tag the model calls made inside ``nostream``. The model node calls the
    model with the config it finds here, so the tag reaches the call."""
    token = var_child_runnable_config.set(nostream(var_child_runnable_config.get() or {}))
    try:
        yield
    finally:
        var_child_runnable_config.reset(token)


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
