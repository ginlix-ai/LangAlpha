"""A step another agent left waiting on the user, resumed by one that cannot run it.

Threads the retired Flash agent ran continue in Home. A step it left waiting
on the user (dispatching work, deleting a workspace) can name a tool the Chief
of Staff does not have. A step Claude Code left waiting runs in a node only
its own graph has, so a turn on another model cannot resume any of it. Either
is answered as cancelled before the thread
resumes.
"""

from __future__ import annotations

import logging
from collections.abc import Collection
from typing import Any

from langchain_core.messages import AIMessage, ToolMessage

from ptc_agent.agent.state import ensure_message_ids
from src.llms.claude_code import claude_replied_last

logger = logging.getLogger(__name__)

_CANCELLED = (
    "Not run: this chat changed agents before the user answered. Ask again if "
    "it is still wanted."
)


async def cancel_tool_calls_it_lacks(graph: Any, thread_id: str, *, by_name: bool = True) -> list[ToolMessage]:
    """Answer a waiting step the resuming agent cannot run, before a resume.

    ``by_name`` also answers the calls naming a tool the agent lacks, which
    only a thread Flash ran holds.

    Best effort: a failure leaves the step as it is, and the resume then
    answers the call as a tool the agent does not have. Returns the cancelled
    results, which the turn streams (see ``_cancel_tool_calls``).
    """
    try:
        return await _cancel_tool_calls(graph, thread_id, _tool_names(graph) if by_name else None)
    except Exception:
        logger.warning(
            f"[CHAT] Cancelling another agent's waiting step failed: "
            f"thread_id={thread_id}",
            exc_info=True,
        )
        return []


def _tool_names(graph: Any) -> frozenset[str]:
    """Every tool the graph can run, middleware's included.

    The model may be shown fewer on a turn; this is what the tool node holds.
    """
    return frozenset(graph.nodes["tools"].bound.tools_by_name)


async def _cancel_tool_calls(
    graph: Any,
    thread_id: str,
    tool_names: Collection[str] | None,
) -> list[ToolMessage]:
    """Answer every call of the waiting step that the graph cannot resume.

    Returns the cancellations, which the turn streams itself: the update lands
    before its stream opens, so the graph never emits them, and a card waiting
    on one would never settle. The finished results already streamed when
    their calls returned.
    """
    config = {"configurable": {"thread_id": thread_id}}
    state = await graph.aget_state(config)
    messages = state.values.get("messages") or []
    # This graph sees no step of Claude Code's graph waiting: it lacks the node.
    claude_waits = claude_replied_last(messages)
    if not state.interrupts and not claude_waits:
        return []
    last_ai = next((m for m in reversed(messages) if isinstance(m, AIMessage)), None)
    if last_ai is None or not last_ai.tool_calls:
        return []
    # A call that finished beside the waiting one is still a pending write of
    # the step, which an update drops, so its result is carried over.
    finished = [
        message
        for task in state.tasks
        if not task.interrupts and isinstance(task.result, dict)
        for message in _as_list(task.result.get("messages"))
        if isinstance(message, ToolMessage)
    ]
    answered = {
        m.tool_call_id for m in (*messages, *finished) if isinstance(m, ToolMessage)
    }
    unanswered = [call for call in last_ai.tool_calls if call["id"] not in answered]
    # By name, as the resume runs a call: the agents share some names (a
    # question, the workspace tool), and each resumes those itself.
    if not unanswered or not (
        claude_waits or (tool_names is not None and any(call["name"] not in tool_names for call in unanswered))
    ):
        return []
    # The update ends the step, so every call still waiting in it is answered,
    # or the next model call finds one without a result.
    cancelled = [
        ToolMessage(
            content=_CANCELLED,
            tool_call_id=call["id"],
            name=call["name"],
            status="error",
        )
        for call in unanswered
    ]
    await graph.aupdate_state(
        config,
        {"messages": ensure_message_ids([*finished, *cancelled])},
        as_node="tools",
    )
    logger.info(
        f"[CHAT] Cancelled {len(cancelled)} tool call(s) another agent left "
        f"waiting: thread_id={thread_id}"
    )
    return cancelled


def _as_list(value: Any) -> list:
    if value is None:
        return []
    return list(value) if isinstance(value, (list, tuple)) else [value]
