"""What a secretary tool answers with, and the checks it runs before acting.

Every answer is a ``Command`` carrying one ``ToolMessage``, and an action that
changes the user's workspaces or threads pauses on ``interrupt()`` for the
user's approval, the same pattern the onboarding tools use.
"""

import json
import logging
from typing import Any

from langchain_core.messages import ToolMessage
from langgraph.types import Command, interrupt

try:
    from langchain.tools import InjectedToolCallId
except ImportError:
    from langchain_core.tools import InjectedToolCallId

logger = logging.getLogger(__name__)

__all__ = [
    "InjectedToolCallId",
    "decline_command",
    "decline_routing_command",
    "error_command",
    "hitl_confirm",
    "success_command",
    "verify_thread_owner",
    "verify_workspace_owner",
]


def hitl_confirm(
    action_type: str, payload: dict[str, Any]
) -> tuple[bool, dict]:
    """Pause the graph for user confirmation via interrupt().

    Args:
        action_type: The action type string (e.g. "create_workspace")
        payload: Additional data to include in the action request

    Returns:
        Tuple of (approved, response_dict)
    """
    response = interrupt(
        {"action_requests": [{"type": action_type, **payload}]}
    )

    approved = False
    if isinstance(response, dict):
        decisions = response.get("decisions", [])
        if decisions and decisions[0].get("type") == "approve":
            approved = True

    return approved, response if isinstance(response, dict) else {}


def decline_command(message: str, tool_call_id: str) -> Command:
    """Return a Command for a declined HITL action."""
    return Command(
        update={
            "messages": [
                ToolMessage(content=message, tool_call_id=tool_call_id),
            ],
        }
    )


def decline_routing_command(
    declined: str, response: dict, tool_call_id: str
) -> Command:
    """Decline a hand-off, or the workspace for one, without freeing the work.

    A bare decline read to the model as leave to do the work itself, though
    the user declined where it would go, not that it should be done here.
    """
    from src.server.models.chat import rejection_feedback

    decisions = response.get("decisions") or [{}]
    feedback = rejection_feedback(decisions[0].get("message"))
    said = f', saying: "{feedback}"' if feedback else ""
    return decline_command(
        f"User declined {declined}{said}. Unless they said to do the work "
        "here, do not do it yourself instead: ask them where they want it.",
        tool_call_id,
    )


def success_command(
    data: dict[str, Any], tool_call_id: str, artifact: dict[str, Any] | None = None
) -> Command:
    """Return a Command with JSON-serialized success data."""
    return Command(
        update={
            "messages": [
                ToolMessage(
                    content=json.dumps(data),
                    tool_call_id=tool_call_id,
                    artifact=artifact,
                ),
            ],
        }
    )


def error_command(error: str, tool_call_id: str, **fields: Any) -> Command:
    """Return a Command with a JSON error response, plus any ``fields`` that
    say what the failed action was."""
    return Command(
        update={
            "messages": [
                ToolMessage(
                    content=json.dumps({"success": False, "error": error, **fields}),
                    tool_call_id=tool_call_id,
                ),
            ],
        }
    )


async def verify_workspace_owner(
    workspace_id: str, user_id: str, tool_call_id: str
) -> Command | None:
    """Return error Command if user doesn't own workspace, else None."""
    from src.server.database.workspace import get_workspace

    ws = await get_workspace(workspace_id)
    if not ws or str(ws.get("user_id")) != user_id:
        return error_command("workspace not found", tool_call_id)
    return None


async def verify_thread_owner(
    thread_id: str, user_id: str, tool_call_id: str
) -> Command | None:
    """Return error Command if user doesn't own thread, else None."""
    from src.server.database.conversation.threads_read import get_thread_owner_id

    try:
        owner_id = await get_thread_owner_id(thread_id)
        if owner_id != user_id:
            return error_command(
                "thread not found or not owned by user", tool_call_id
            )
    except Exception as e:
        logger.error(f"Failed to verify thread ownership: {e}")
        return error_command("thread not found", tool_call_id)
    return None
