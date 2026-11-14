"""Hand-offs and new workspaces the user approved in advance.

The settings live in ``other_preference``, which the agent cannot write. New
workspaces are made with ``la``, whose approval reads its setting itself
(``cli_approval``). The hand-off's is read when the model proposes the call
and recorded against that call in the thread state, never when the tool runs:
a tool that paused re-runs from the top on resume, so a setting changed while
its card was pending would otherwise skip an answer the user already gave,
and a hand-off they declined would run.
"""

import logging
from typing import Annotated, Any

from langchain.agents.middleware import AgentMiddleware, AgentState
from langchain.agents.middleware.types import PrivateStateAttr
from typing_extensions import NotRequired

from ptc_agent.agent.pending_calls import pending_calls
from src.tools.secretary._commands import HANDOFF_TOOL_NAME

logger = logging.getLogger(__name__)

AUTO_APPROVE_HANDOFFS = "auto_approve_handoffs"
#: Lets ``la workspaces create`` run without asking in the app; the web's
#: Settings and the create dialog's "always" save it.
AUTO_APPROVE_WORKSPACE_CREATION = "auto_approve_workspace_creation"


async def standing_approval(user_id: str, key: str) -> bool:
    """Whether the user turned on the ``other_preference`` setting ``key``."""
    from src.server.database.user import get_user_preferences

    try:
        # Uncached: a read racing the write that turned it off could cache
        # the old yes.
        prefs = await get_user_preferences(user_id, cached=False)
    except Exception:
        # Asking is the default, so a setting that cannot be read asks
        # instead of failing the call.
        logger.warning(
            f"Could not read approval settings for user {user_id}; asking",
            exc_info=True,
        )
        return False
    other = (prefs or {}).get("other_preference") or {}
    return other.get(key) is True


class StandingApprovalState(AgentState):
    # Every model call proposing a hand-off rewrites this list, and only that
    # tool reads it, so an id left from an earlier model call can only meet a
    # tool that never looks.
    preapproved_calls: NotRequired[Annotated[list[str], PrivateStateAttr]]


class StandingApprovalMiddleware(AgentMiddleware):
    """Records which of a model call's hand-offs need no approval card."""

    state_schema = StandingApprovalState

    def __init__(self, user_id: str | None) -> None:
        super().__init__()
        self._user_id = user_id

    async def aafter_model(self, state: Any, runtime: Any) -> dict[str, Any] | None:
        if not self._user_id:
            return None
        _, calls = pending_calls(state.get("messages") or [])
        handoffs = [call["id"] for call in calls if call.get("name") == HANDOFF_TOOL_NAME]
        if not handoffs:
            return None
        approved = await standing_approval(self._user_id, AUTO_APPROVE_HANDOFFS)
        return {"preapproved_calls": handoffs if approved else []}


def preapproved(state: dict[str, Any], tool_call_id: str) -> bool:
    """Whether this call was approved in advance when the model proposed it."""
    return tool_call_id in (state.get("preapproved_calls") or ())
