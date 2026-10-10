"""The user's subagent switch: a row that says it moved, and a gate that holds it.

The switch can flip between turns or in the middle of one, and it must never
change what the model is bound to or the prompt it caches behind: dropping the
subagent tools or rewording the prompt would throw away the cached prefix on
every flip. So the bound tools and the prompt stay exactly as built, and the
switch reaches the model two other ways. The notice tells it, as a durable row
in history, whenever the switch now differs from the last value a row in view
announced. The gate refuses a launch at call time while the switch is off,
because a model that missed or ignored the row must still not start one.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable, Mapping, Sequence
from typing import Annotated, Any, NotRequired

from langchain.agents.middleware import AgentMiddleware
from langchain.agents.middleware.types import AgentState, PrivateStateAttr, ToolCallRequest
from langchain_core.messages import ToolMessage
from langgraph.types import Command

from ptc_agent.agent.middleware.runtime_context import (
    SUBAGENTS_ROW_KIND,
    DurableUpdate,
    build_update_message,
    last_stated,
    runtime_update_from_message,
)
from ptc_agent.agent.middleware.runtime_context.state import state_get

logger = logging.getLogger(__name__)

SUBAGENTS_SCHEMA_VERSION = 1

# The read sits in front of every model call, so a slow pool must not stall
# the turn: past this the switch is unknown, like any failed read.
_READ_TIMEOUT_S = 2.0

#: The tools that start or direct subagent work. ``TaskOutput`` is not one: it
#: only collects what already ran, and a report-back turn depends on it.
SUBAGENT_LAUNCH_TOOLS = frozenset({"Task", "RunWorkflow"})

SUBAGENTS_REFUSAL = (
    "Refused: the user has turned subagents off for this thread. Do this work "
    "yourself, or ask the user to turn subagents back on."
)

#: Whether subagents are allowed on the thread the agent was built for, or None
#: when the thread has no switch to read.
SubagentSwitchReader = Callable[[], Awaitable[bool | None]]


class SubagentSwitchState(AgentState):
    """``_subagents_trimmed`` is set once the window trims a notice (see
    ``compaction.window``), so that one was ever written outlives it."""

    _subagents_trimmed: Annotated[NotRequired[bool], PrivateStateAttr]


def last_announced(state: Any) -> bool | None:
    """The switch as the last notice the model can still read stated it.

    With none ever written the model has been told nothing, which is the
    default: subagents on. None when a compaction took every notice from view:
    the summary leaves them out but can keep what a refusal said, so what the
    model believes is unknown and the switch is restated, whichever side it is.
    """
    stated = last_stated(state, SUBAGENTS_ROW_KIND, "allowed")
    if stated is not None:
        return stated is not False
    if state_get(state, "_subagents_trimmed") or _has_notice(state_get(state, "messages") or ()):
        return None
    return True


def _has_notice(messages: Sequence[Any]) -> bool:
    for message in reversed(messages):
        row = runtime_update_from_message(message)
        if row is not None and row.kind == SUBAGENTS_ROW_KIND:
            return True
    return False


def subagents_window_carry(trimmed: Sequence[Any], state: Mapping[str, Any]) -> dict[str, Any]:
    """The window's carry (see ``compaction.window``) for the notices it
    trims, which ``last_announced`` would otherwise read as never written."""
    if state.get("_subagents_trimmed") or not _has_notice(trimmed):
        return {}
    return {"_subagents_trimmed": True}


class SubagentSwitchMiddleware(AgentMiddleware):
    """Announces the user's subagent switch in history and holds launches to it."""

    state_schema = SubagentSwitchState

    def __init__(self, read: SubagentSwitchReader) -> None:
        super().__init__()
        self._read = read

    async def _switch(self) -> bool | None:
        """The switch now, or None when it is unknown.

        Read fresh on every hook and never cached: a flip lands in Postgres
        from whichever worker served it. Unknown is no change to announce, and
        the gate lets the call through rather than block work on a failed read.
        """
        try:
            async with asyncio.timeout(_READ_TIMEOUT_S):
                return await self._read()
        except Exception:  # noqa: BLE001 - an unread switch is unknown, never a failed turn
            logger.warning("[SubagentSwitch] switch unread", exc_info=True)
            return None

    async def abefore_model(
        self, state: Any, runtime: Any = None
    ) -> dict[str, Any] | None:
        """A ``subagents_switched`` row when the switch moved since the last one.

        One hook covers every case: the first call of a turn, a flip mid-turn
        (the next call after it), a resumed interrupt, and a compaction that
        took every row from view.
        """
        allowed = await self._switch()
        if allowed is None or allowed == last_announced(state):
            return None
        row = DurableUpdate(
            kind=SUBAGENTS_ROW_KIND,
            schema_version=SUBAGENTS_SCHEMA_VERSION,
            # Every word is the template's, keyed on the provenance below.
            text="",
            provenance={"source": "user", "allowed": allowed},
        )
        # No ids: the Pregel path mints them for a row a hook returns.
        return {"messages": [build_update_message(row)]}

    # Async only, on purpose: the base sync ``wrap_tool_call`` raises, which is
    # the refusal a sync invocation needs, since the Task tool's sync body runs
    # a subagent and the switch can only be read asynchronously.
    async def awrap_tool_call(
        self,
        request: ToolCallRequest,
        handler: Callable[[ToolCallRequest], Awaitable[ToolMessage | Command]],
    ) -> ToolMessage | Command:
        name = request.tool_call.get("name", "")
        if name not in SUBAGENT_LAUNCH_TOOLS or await self._switch() is not False:
            return await handler(request)
        # The shape of the launch tools' own refusals: nothing started, so the
        # error status is the only signal the client's launch card settles on.
        return ToolMessage(
            content=SUBAGENTS_REFUSAL,
            tool_call_id=request.tool_call.get("id"),
            name=name,
            status="error",
        )
