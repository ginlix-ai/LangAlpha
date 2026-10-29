"""When ``quiet`` may end a turn, and whether the turn changed any wakeups.

A turn a wakeup started has nobody waiting on a reply, so ending it silently
loses nothing; a user's message into it, even one sent while it was starting,
makes it a turn someone waits on, and quiet is refused there as on any user
turn. Messages from the Chief of Staff or another agent do not count: their
sender reads the thread, not a reply. A quiet turn also leaves the thread
read, so one that acted where the user would want to know, on an order or by
handing work to an analyst, is refused too, as is a quiet sent beside other
calls, which would end the turn before their results are read.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any, Literal, Optional

from langchain_core.messages import AIMessage, BaseMessage, HumanMessage, ToolMessage

from ptc_agent.agent.middleware.direct_mcp import METADATA_KEY as DIRECT_MCP_KEY
from ptc_agent.agent.transcript.classify import human_kind, is_run_boundary_message
from src.tools.wakeups.call import asks_change
from src.tools.wakeups.models import WakeupArtifact

TOOL_NAME = "schedule_wakeup"
_HANDOFF_TOOL = "delegate_to_analyst"

QuietRefusal = Literal["user_turn", "acted", "other_calls"]


def quiet_result(message: Any) -> WakeupArtifact | None:
    """The artifact of a ``schedule_wakeup`` result that accepted ``quiet``,
    or None for any other message."""
    if not isinstance(message, ToolMessage) or message.name != TOOL_NAME:
        return None
    artifact = message.artifact
    if isinstance(artifact, Mapping) and artifact.get("quiet") is True:
        return WakeupArtifact(quiet=True, changed=artifact.get("changed") is True)
    return None


def _user_steer(message: BaseMessage) -> bool:
    if not isinstance(message, HumanMessage) or human_kind(message) != "steering":
        return False
    delivered = (message.additional_kwargs or {}).get("steering_delivered")
    if not isinstance(delivered, Mapping):
        return False
    try:
        return int(delivered.get("count") or 0) > 0
    except (TypeError, ValueError):
        return False


def _last_boundary(messages: Sequence[BaseMessage]) -> int:
    for i in range(len(messages) - 1, -1, -1):
        if is_run_boundary_message(messages[i]):
            return i
    return -1


def _acted(message: BaseMessage) -> bool:
    """A result of a call on an order, answered or refused, or of a hand-off."""
    if not isinstance(message, ToolMessage):
        return False
    if message.name == _HANDOFF_TOOL:
        return True
    artifact = message.artifact
    stamp = artifact.get(DIRECT_MCP_KEY) if isinstance(artifact, Mapping) else None
    return isinstance(stamp, Mapping) and isinstance(stamp.get("order"), Mapping)


def _beside_other_calls(messages: Sequence[BaseMessage]) -> bool:
    """Whether the round this call is in also calls another tool, whose
    result the model has not read and which can itself be an order."""
    for message in reversed(messages):
        if isinstance(message, AIMessage):
            return any(c.get("name") != TOOL_NAME for c in message.tool_calls or ())
    return False


def quiet_refusal(
    messages: Sequence[BaseMessage], configurable: Mapping[str, Any]
) -> Optional[QuietRefusal]:
    """Why ``quiet`` may not end this turn, or None when it may."""
    if configurable.get("wake_turn") is not True:
        return "user_turn"
    start = _last_boundary(messages)
    turn = messages[start + 1 :]
    if any(_user_steer(m) for m in turn):
        return "user_turn"
    # Steers sent while the turn was starting land just ahead of its input,
    # after the previous turn's last reply.
    for message in reversed(messages[: max(start, 0)]):
        if isinstance(message, AIMessage):
            break
        if _user_steer(message):
            return "user_turn"
    if any(_acted(m) for m in turn):
        return "acted"
    if _beside_other_calls(turn):
        return "other_calls"
    return None


def changed_this_turn(messages: Sequence[BaseMessage], this_call_args: Any) -> bool:
    """Whether any call this turn asked to set or cancel a wakeup.

    Read from the calls, not from what they achieved, so the web, which sees
    only the calls, folds a quiet turn by the same rule.
    """
    if asks_change(this_call_args):
        return True
    for message in messages[_last_boundary(messages) + 1 :]:
        if not isinstance(message, AIMessage):
            continue
        for call in message.tool_calls or ():
            if call.get("name") == TOOL_NAME and asks_change(call.get("args")):
                return True
    return False
