"""Hand-offs and new workspaces the user approved in advance.

The setting lives in ``other_preference``, which the agent cannot write. It is
read when the model proposes the call and recorded against that call in the
thread state, never when the tool runs: a tool that paused re-runs from the top
on resume, so a setting changed while its card was pending would otherwise
skip an answer the user already gave, and a hand-off they declined would run.
"""

import logging
from typing import Annotated, Any

from langchain.agents.middleware import AgentMiddleware, AgentState
from langchain.agents.middleware.types import PrivateStateAttr
from langchain_core.messages import AIMessage
from typing_extensions import NotRequired

logger = logging.getLogger(__name__)

AUTO_APPROVE_HANDOFFS = "auto_approve_handoffs"
AUTO_APPROVE_WORKSPACE_CREATION = "auto_approve_workspace_creation"
AUTO_APPROVE_KEYS = (AUTO_APPROVE_HANDOFFS, AUTO_APPROVE_WORKSPACE_CREATION)


def _settings_for(call: dict[str, Any]) -> str | None:
    """The setting that, granted, lets ``call`` run without its card."""
    args = call.get("args")
    if not isinstance(args, dict):
        args = {}
    name = call.get("name")
    if name == "delegate_to_analyst":
        return AUTO_APPROVE_HANDOFFS
    if name == "manage_workspaces" and args.get("action") == "create":
        return AUTO_APPROVE_WORKSPACE_CREATION
    return None


async def _granted(user_id: str) -> set[str]:
    from src.server.database.user import get_user_preferences

    try:
        prefs = await get_user_preferences(user_id)
    except Exception:
        # Asking is the default, so a setting that cannot be read shows the
        # card instead of failing the turn.
        logger.warning(
            f"Could not read approval settings for user {user_id}; asking",
            exc_info=True,
        )
        return set()
    other = (prefs or {}).get("other_preference") or {}
    return {key for key in AUTO_APPROVE_KEYS if other.get(key) is True}


class StandingApprovalState(AgentState):
    # Every model call proposing an approvable call rewrites this list, and
    # only those tools read it, so an id left from an earlier model call can
    # only meet a tool that never looks.
    preapproved_calls: NotRequired[Annotated[list[str], PrivateStateAttr]]


class StandingApprovalMiddleware(AgentMiddleware):
    """Records which of a model call's tool calls need no approval card."""

    state_schema = StandingApprovalState

    def __init__(self, user_id: str | None) -> None:
        super().__init__()
        self._user_id = user_id

    async def aafter_model(self, state: Any, runtime: Any) -> dict[str, Any] | None:
        messages = state.get("messages") or []
        last = messages[-1] if messages else None
        if not isinstance(last, AIMessage) or not self._user_id:
            return None
        wanted = {
            call["id"]: needed
            for call in last.tool_calls
            if (needed := _settings_for(call))
        }
        if not wanted:
            return None
        granted = await _granted(self._user_id)
        return {
            "preapproved_calls": [
                call_id for call_id, needed in wanted.items() if needed in granted
            ]
        }


def preapproved(state: dict[str, Any], tool_call_id: str) -> bool:
    """Whether this call was approved in advance when the model proposed it."""
    return tool_call_id in (state.get("preapproved_calls") or ())
