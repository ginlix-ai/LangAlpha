"""Main-agent middleware for wakeups: ending a quiet wake turn, and telling the
agent which of its wakeups the user cancelled.

Both are main agent only. A subagent never has ``schedule_wakeup``, and a
cancel is news for the agent that set the wakeup.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Collection, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Annotated, Any, NotRequired, Protocol

from langchain.agents.middleware.types import (
    AgentMiddleware,
    AgentState,
    PrivateStateAttr,
    hook_config,
)

from ptc_agent.agent.middleware.repeat_guard import tool_rounds
from ptc_agent.agent.middleware.runtime_context.changes import UPDATE_SCHEMA_VERSION
from ptc_agent.agent.middleware.runtime_context.durable import (
    DurableUpdate,
    build_update_message,
    runtime_update_from_message,
)
from ptc_agent.agent.middleware.runtime_context.state import state_get
from ptc_agent.agent.middleware.runtime_context.turn import WAKEUP_CANCELLED_ROW_KIND
from src.tools.wakeups.quiet import quiet_result

logger = logging.getLogger(__name__)

# The read sits in front of every model call, so a slow pool must not stall
# the turn: past this the cancels wait for the next call.
_READ_TIMEOUT_S = 2.0


def _messages(state: Any) -> Sequence[Any]:
    messages = state.get("messages") if isinstance(state, dict) else None
    return messages or ()


# ---------------------------------------------------------------------------
# Quiet
# ---------------------------------------------------------------------------


def _quiet_round(messages: Sequence[Any]) -> bool:
    """Whether the last round of tool calls holds an accepted ``quiet``.

    Only a round with nothing after its results: anything there is something
    new to answer.
    """
    latest = next(tool_rounds(messages), None)
    return latest is not None and any(quiet_result(m) for m in latest[1])


class WakeupQuietGuardMiddleware(AgentMiddleware):
    """Ends the run after an accepted ``schedule_wakeup(quiet=...)``.

    Before the model rather than from the tool, as ``RepeatCallGuardMiddleware``
    does: a ``Command(goto=END)`` from a tool is one branch beside the tool
    node's own edge back to the model, which is called anyway. Placed ahead
    of the credit gate, steering and the context rows, so a model call that
    never happens takes in no steer or report and writes no row; what arrived
    after the quiet call waits for the next turn.
    """

    @hook_config(can_jump_to=["end"])
    async def abefore_model(self, state: Any, runtime: Any = None) -> dict[str, Any] | None:
        return {"jump_to": "end"} if _quiet_round(_messages(state)) else None

    @hook_config(can_jump_to=["end"])
    def before_model(self, state: Any, runtime: Any = None) -> dict[str, Any] | None:
        return {"jump_to": "end"} if _quiet_round(_messages(state)) else None


# ---------------------------------------------------------------------------
# The user's cancels
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class CancelGroup:
    """One row's worth of the user's cancels: the wakeups, who ended them
    (``user``, or ``setting_off`` when the user turned wakeups off), and the
    row's text."""

    wakeup_ids: tuple[str, ...]
    cancelled_by: str
    text: str


class CancelNotices(Protocol):
    """The user's cancels on the thread the agent was built for."""

    async def untold(self, told: Collection[str]) -> tuple[CancelGroup, ...]:
        """A group per canceller of the cancels ``told`` does not name."""
        ...


class WakeupCancelNoticeState(AgentState):
    """``_wakeup_cancels_trimmed`` is the wakeup ids named by the rows the
    window trimmed (see ``compaction.window``), so a cancel the agent was
    told of before a trim is not told again after it."""

    _wakeup_cancels_trimmed: Annotated[NotRequired[list[str]], PrivateStateAttr]


def _row_ids(messages: Sequence[Any]) -> set[str]:
    ids: set[str] = set()
    for message in messages:
        update = runtime_update_from_message(message)
        if update is None or update.kind != WAKEUP_CANCELLED_ROW_KIND:
            continue
        named = update.provenance.get("wakeup_ids") if isinstance(update.provenance, dict) else None
        ids.update(str(i) for i in named or ())
    return ids


def _told_ids(state: Any) -> set[str]:
    """Wakeup ids the agent's history has told it of: the rows in state,
    compacted ones too, and those the window trimmed."""
    trimmed = state_get(state, "_wakeup_cancels_trimmed") or ()
    return _row_ids(_messages(state)) | {str(i) for i in trimmed}


def wakeup_cancels_window_carry(
    trimmed: Sequence[Any], state: Mapping[str, Any]
) -> dict[str, Any]:
    """The window's carry (see ``compaction.window``) for the rows it trims,
    whose cancels would otherwise read as never told and be sent again."""
    held = [str(i) for i in state.get("_wakeup_cancels_trimmed") or ()]
    new = sorted(_row_ids(trimmed) - set(held))
    return {"_wakeup_cancels_trimmed": [*held, *new]} if new else {}


class WakeupCancelNoticeMiddleware(AgentMiddleware):
    """Writes a durable row for the wakeups the user cancelled that no row in
    the agent's history names yet.

    What the agent was told is read from its history, never stamped on the
    wakeup, so a rewind to a checkpoint before the row (a retry, a
    regenerate, an edited message) or a step lost before it was saved sends
    the row again. Bound whether or not wakeups are on, so the row that says
    the setting was turned off still arrives. One indexed query per model
    call, which fails open.
    """

    state_schema = WakeupCancelNoticeState

    def __init__(self, notices: CancelNotices) -> None:
        super().__init__()
        self._notices = notices

    # Async only, as ``SteeringMiddleware``'s is: the async agent never calls
    # the sync hook.
    async def abefore_model(self, state: Any, runtime: Any = None) -> dict[str, Any] | None:
        try:
            async with asyncio.timeout(_READ_TIMEOUT_S):
                groups = await self._notices.untold(_told_ids(state))
        except Exception:  # noqa: BLE001 - an unread cancel waits for the next call
            logger.warning("[Wakeups] cancel notice unread", exc_info=True)
            return None
        if not groups:
            return None
        now = datetime.now(UTC)
        return {
            "messages": [
                build_update_message(
                    DurableUpdate(
                        kind=WAKEUP_CANCELLED_ROW_KIND,
                        schema_version=UPDATE_SCHEMA_VERSION,
                        text=group.text,
                        provenance={
                            "source": "harness",
                            "wakeup_ids": list(group.wakeup_ids),
                            "cancelled_by": group.cancelled_by,
                        },
                        created_at=now,
                    )
                )
                for group in groups
            ]
        }


__all__ = [
    "CancelGroup",
    "CancelNotices",
    "WakeupCancelNoticeMiddleware",
    "WakeupCancelNoticeState",
    "WakeupQuietGuardMiddleware",
    "wakeup_cancels_window_carry",
]
