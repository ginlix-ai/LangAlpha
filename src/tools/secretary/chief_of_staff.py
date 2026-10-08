"""The Chief of Staff's tools: how Home's agent runs the user's workspaces.

Each workspace has its own agent, its analyst, and the Chief of Staff in Home
hands work to them rather than doing it in their folders. A hand-off always
names its workspace: a new one is created first, under its own approval or
one the user gave in advance, so the user agrees to the workspace before any
work is handed to it. It lists, creates and deletes workspaces but does not
stop them: Home runs on the computer a stop would take down. A delete is the
user's to confirm on its own card, never approved in advance. ``agent_output``
is how it reads a thread, so its ``manage_threads`` has no second way to.
"""

from typing import Annotated

from langchain.tools import InjectedState
from langchain_core.runnables import RunnableConfig
from langchain_core.tools import tool
from langgraph.types import Command

from src.tools.secretary._commands import InjectedToolCallId, error_command
from src.tools.secretary.approvals import preapproved
from src.tools.secretary.dispatch import dispatch
from src.tools.secretary.tools import (
    _workspaces_delete,
    agent_output,
    threads_delete,
    threads_list,
    threads_stop,
    workspaces_create,
    workspaces_list,
)


@tool("manage_workspaces")
async def chief_of_staff_workspaces(
    action: str,
    config: RunnableConfig,
    state: Annotated[dict, InjectedState],
    name: str | None = None,
    description: str | None = None,
    workspace_id: str | None = None,
    tool_call_id: Annotated[str, InjectedToolCallId] = "",
) -> Command:
    """List the user's workspaces, or create or delete one once the user confirms.

    Args:
        action: "list", "create" or "delete".
        name: Name for the new workspace; required for "create".
        description: What the new workspace is for; optional.
        workspace_id: The workspace to delete; required for "delete". Home cannot be deleted.

    Returns:
        One short row per workspace, with its id, and its folder (dir_name) when it is on
        this computer; for "create", the new id.
    """
    user_id = config.get("configurable", {}).get("user_id")
    if not user_id:
        return error_command("user_id not found in config", tool_call_id)
    if action == "list":
        from src.server.database.home_workspace import get_flash_workspace_id

        return await workspaces_list(
            user_id, tool_call_id, home_id=get_flash_workspace_id(user_id)
        )
    if action == "create":
        return await workspaces_create(
            user_id, name, description, tool_call_id,
            preapproved=preapproved(state, tool_call_id),
        )
    if action == "delete":
        return await _workspaces_delete(user_id, workspace_id, tool_call_id)
    return error_command(
        f"Unknown action: {action}. Use list, create or delete.", tool_call_id
    )


@tool("delegate_to_analyst")
async def delegate_to_analyst(
    question: str,
    config: RunnableConfig,
    state: Annotated[dict, InjectedState],
    workspace_id: str | None = None,
    thread_id: str | None = None,
    report_back: bool = True,
    tool_call_id: Annotated[str, InjectedToolCallId] = "",
) -> Command:
    """Hand work to a workspace's analyst, who does it there in the background unless the user declines.

    Needs workspace_id or thread_id: a workspace that does not exist yet is
    created with manage_workspaces first.

    Args:
        question: The task, written for the analyst, who does not see this conversation.
        workspace_id: The workspace whose analyst starts a new thread. Ignored with thread_id.
        thread_id: An analyst's thread to continue.
        report_back: True to be told when the analyst finishes, is stopped or fails, so
            you can relay the outcome; False when the user will read it in the workspace
            themselves.

    Returns:
        The analyst's thread, still running; its report_back field says whether a report-back is coming.
    """
    if not workspace_id and not thread_id:
        return error_command(
            "No workspace named. Pass workspace_id, or thread_id to continue "
            "a thread. For work no workspace covers, create one with "
            'manage_workspaces(action="create") first, then hand off to it.',
            tool_call_id,
        )
    return await dispatch(
        question, config, workspace_id, thread_id, report_back, tool_call_id,
        preapproved=preapproved(state, tool_call_id),
    )


@tool("manage_threads")
async def chief_of_staff_threads(
    action: str,
    config: RunnableConfig,
    workspace_id: str | None = None,
    thread_id: str | None = None,
    run_id: str | None = None,
    tool_call_id: Annotated[str, InjectedToolCallId] = "",
) -> Command:
    """List the user's threads, stop the turn one is running, or delete one once the user confirms.

    Args:
        action: "list", "stop" or "delete". "stop" ends the turn within a few seconds;
            the thread and what it produced so far stay.
        workspace_id: For "list", only this workspace's threads; all of them without it.
        thread_id: The thread to stop or delete; required for both.
        run_id: For "stop", the run_id from the hand-off's delegate_to_analyst result, so
            the stop never ends a turn started there since; omit it to stop whatever the
            thread is running.
    """
    configurable = config.get("configurable", {})
    user_id = configurable.get("user_id")
    if not user_id:
        return error_command("user_id not found in config", tool_call_id)
    if action == "list":
        return await threads_list(user_id, workspace_id, tool_call_id)
    if action == "stop":
        return await threads_stop(
            user_id,
            thread_id,
            configurable.get("thread_id"),
            tool_call_id,
            run_id=run_id,
        )
    if action == "delete":
        return await threads_delete(user_id, thread_id, tool_call_id)
    return error_command(
        f"Unknown action: {action}. Use list, stop or delete.", tool_call_id
    )


CHIEF_OF_STAFF_TOOLS = [
    chief_of_staff_workspaces,
    delegate_to_analyst,
    agent_output,
    chief_of_staff_threads,
]
