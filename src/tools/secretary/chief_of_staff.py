"""The Chief of Staff's hand-off tool: how Home's agent gives a workspace's analyst work.

Each workspace has its own agent, its analyst, and the Chief of Staff in Home
hands work to them rather than doing it in their folders. A hand-off always
names its workspace: a new one is created first with ``la workspaces create``,
which asks the user in the app unless they let it create workspaces without
asking, so the user agrees to the workspace before any work is handed to it.
Listing, reading, stopping and deleting workspaces and threads go through
``la`` too, from the agent's Bash.
"""

from typing import Annotated

from langchain.tools import InjectedState
from langchain_core.runnables import RunnableConfig
from langchain_core.tools import tool
from langgraph.types import Command

from src.llms.reasoning import DelegatedEffort
from src.tools.secretary._commands import (
    HANDOFF_TOOL_NAME,
    InjectedToolCallId,
    error_command,
)
from src.tools.secretary.approvals import preapproved
from src.tools.secretary.dispatch import dispatch


@tool(HANDOFF_TOOL_NAME)
async def delegate_to_analyst(
    question: str,
    config: RunnableConfig,
    state: Annotated[dict, InjectedState],
    workspace_id: str | None = None,
    thread_id: str | None = None,
    report_back: bool = True,
    model: str | None = None,
    effort: DelegatedEffort | None = None,
    tool_call_id: Annotated[str, InjectedToolCallId] = "",
) -> Command:
    """Hand work to a workspace's analyst, who does it there in the background unless the user declines.

    Needs workspace_id or thread_id: a workspace that does not exist yet is
    created with `la workspaces create` first.

    Args:
        question: The task, written for the analyst, who does not see this conversation.
        workspace_id: The workspace whose analyst starts a new thread. Ignored with thread_id.
        thread_id: An analyst's thread to continue. While it is mid-turn, the analyst takes the task in before its next step, or as its next turn if that one ends first.
        report_back: Leave True to be told when the analyst finishes, is stopped or fails.
            False only when the user asks for no report, or will follow the work in that
            workspace themselves.
        model: A model name from `la models list`. Pass one only when the user asks for a
            model or a skill's procedure names one; it stays the thread's model for later
            turns. Omitted, the turn runs the thread's own model, or the user's default on
            a new thread. A thread mid-turn takes it only into a turn already running that
            model, and otherwise refuses it until that turn ends.
        effort: How much work the analyst puts into this turn, read as Task's effort
            is. Omitted, the thread's own effort, else the user's setting. A level the
            model lacks runs at the next one below, and the result says which. For this
            turn only; a thread mid-turn refuses it until that turn ends.

    Returns:
        The analyst's thread, still running; its report_back field says whether a report-back is coming.
    """
    if not workspace_id and not thread_id:
        return error_command(
            "No workspace named. Pass workspace_id, or thread_id to continue "
            "a thread. For work no workspace covers, create one with "
            "`la workspaces create` first, then hand off to it.",
            tool_call_id,
        )
    return await dispatch(
        question, config, workspace_id, thread_id, report_back, tool_call_id,
        preapproved=preapproved(state, tool_call_id), model=model, effort=effort,
    )


CHIEF_OF_STAFF_TOOLS = [delegate_to_analyst]
