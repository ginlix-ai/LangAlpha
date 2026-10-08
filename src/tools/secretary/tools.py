"""Flash's secretary tools: its workspaces, its hand-offs, and their threads.

The workspace and thread actions are shared with the Chief of Staff, whose own
tools are in ``chief_of_staff``; a hand-off runs through ``dispatch``.
"""

import json
import logging
from typing import Annotated

from langchain.tools import InjectedState
from langchain_core.messages import ToolMessage
from langchain_core.runnables import RunnableConfig
from langchain_core.tools import tool
from langgraph.types import Command

from src.tools.secretary._commands import (
    InjectedToolCallId,
    decline_command,
    decline_routing_command,
    error_command,
    hitl_confirm,
    success_command,
    verify_thread_owner,
    verify_workspace_owner,
)
from src.tools.secretary.approvals import preapproved
from src.tools.secretary.dispatch import dispatch
from src.utils.nested import without_keys

logger = logging.getLogger(__name__)

# The tools take the user from the run config, so the owner's user_id in a
# listed row is nothing the model needs or passes back. The row's own ids, which
# it does pass back, stay.
_OWNER_ID = frozenset({"user_id"})

_STOPPING = (
    "Stopping; the turn ends within a few seconds. If it was a hand-off with "
    "a report-back, that report will say how far it got: tell the user it is "
    "stopping and do not read the thread now."
)

# A workspace as a listing shows it: what a hand-off is routed by, and the
# status a stop or a "what is running" answer reads. A listing comes before
# every hand-off, so the row's settings, artifacts and machine bindings stay out.
_LISTED_WORKSPACE_FIELDS = (
    "workspace_id",
    "name",
    "description",
    "dir_name",
    "status",
    "created_at",
    "last_activity_at",
)


def _timezone(configurable: dict) -> str:
    """The user's zone, which a run's label states its start time in."""
    return configurable.get("timezone") or "UTC"


async def _get_thread_output(
    user_id: str,
    thread_id: str,
    tool_call_id: str,
    turns: int = 1,
    timezone: str = "UTC",
) -> Command:
    """Verify ownership and extract thread output.

    Shared by agent_output tool and manage_threads(action="get_output").
    ``turns`` bounds how many recent turns are returned (1 = latest only).
    """
    from src.tools.secretary.utils import extract_text_from_thread

    if err := await verify_thread_owner(thread_id, user_id, tool_call_id):
        return err

    try:
        result = await extract_text_from_thread(thread_id, turns, timezone=timezone)
    except Exception as e:
        logger.error(f"Failed to extract text from thread {thread_id}: {e}")
        return error_command("failed to retrieve thread output", tool_call_id)

    return success_command(result, tool_call_id)


# ---------------------------------------------------------------------------
# Tool 1: manage_workspaces
# ---------------------------------------------------------------------------


@tool("manage_workspaces")
async def manage_workspaces(
    action: str,
    config: RunnableConfig,
    state: Annotated[dict, InjectedState],
    name: str | None = None,
    description: str | None = None,
    workspace_id: str | None = None,
    tool_call_id: Annotated[str, InjectedToolCallId] = "",
) -> Command:
    """Manage user workspaces: list, create, or delete.

    Args:
        action: One of "list", "create", "delete"
        name: Workspace name (required for "create")
        description: Workspace description (optional, for "create")
        workspace_id: Workspace ID (required for "delete")
    """
    configurable = config.get("configurable", {})
    user_id = configurable.get("user_id")
    if not user_id:
        return error_command("user_id not found in config", tool_call_id)

    if action == "list":
        return await workspaces_list(user_id, tool_call_id)
    elif action == "create":
        return await workspaces_create(
            user_id, name, description, tool_call_id,
            preapproved=preapproved(state, tool_call_id),
        )
    elif action == "delete":
        return await _workspaces_delete(user_id, workspace_id, tool_call_id)
    else:
        return error_command(
            f"Unknown action: {action}. Use list, create, or delete.",
            tool_call_id,
        )


async def workspaces_list(
    user_id: str, tool_call_id: str, *, home_id: str | None = None
) -> Command:
    """List workspaces for the user.

    With ``home_id``, the Chief of Staff's listing: a row names its folder only
    when the workspace is on Home's computer, the one whose folders it reads.
    """
    try:
        from src.server.database.workspace import get_workspace, get_workspaces_for_user

        # Pinned, then most recently used: past the 20th the list stops, and
        # the workspace a hand-off is for is far likelier to be recent than
        # early in the user's custom order.
        workspaces, total = await get_workspaces_for_user(
            user_id=user_id, limit=20, sort_by="activity"
        )
        if home_id is not None:
            from src.tools.secretary.activity import folder_on

            home = await get_workspace(home_id)
            computer_id = home.get("computer_id") if home else None
            workspaces = [
                {**ws, "dir_name": folder_on(ws, computer_id)} for ws in workspaces
            ]
        content = json.dumps(
            {
                "success": True,
                "workspaces": [
                    {field: ws.get(field) for field in _LISTED_WORKSPACE_FIELDS}
                    for ws in workspaces
                ],
                "total": total,
            },
            default=str,
        )
    except Exception as e:
        logger.error(f"Failed to list workspaces: {e}")
        content = json.dumps({"success": False, "error": "failed to list workspaces"})

    return Command(
        update={
            "messages": [
                ToolMessage(content=content, tool_call_id=tool_call_id),
            ],
        }
    )


async def workspaces_create(
    user_id: str,
    name: str | None,
    description: str | None,
    tool_call_id: str,
    preapproved: bool = False,
) -> Command:
    """Create a new workspace, once the user confirms unless ``preapproved``."""
    if not name:
        return error_command(
            "name is required for create action", tool_call_id
        )

    if not preapproved:
        approved, response = hitl_confirm(
            "create_workspace",
            {"workspace_name": name, "workspace_description": description or ""},
        )

        if not approved:
            return decline_routing_command(
                "workspace creation", response, tool_call_id
            )

    from src.server.database.workspace_names import (
        WorkspaceNameInvalid,
        WorkspaceNameTaken,
    )

    try:
        from src.server.services.workspace_manager import WorkspaceManager

        workspace_manager = WorkspaceManager.get_instance()
        workspace = await workspace_manager.create_workspace(
            user_id=user_id,
            name=name,
            description=description,
        )

        result = {
            "success": True,
            "workspace_id": str(workspace["workspace_id"]),
            "workspace_name": workspace["name"],
        }
        if preapproved:
            # The card for it is drawn from this result.
            result["preapproved"] = True
            result["workspace_description"] = description or ""
        return success_command(result, tool_call_id)
    except WorkspaceNameTaken as e:
        if e.workspace_id is None:
            # The holder was renamed or deleted before it could be named.
            hint = "Try again, or create this one under a different name."
        else:
            hint = (
                f"Its workspace_id is {e.workspace_id}: use that workspace, "
                "or create this one under a different name."
            )
        return error_command(f"{e} {hint}", tool_call_id)
    except WorkspaceNameInvalid as e:
        return error_command(str(e), tool_call_id)
    except Exception as e:
        logger.error(f"Failed to create workspace: {e}")
        return error_command("failed to create workspace", tool_call_id)


async def _workspaces_delete(
    user_id: str, workspace_id: str | None, tool_call_id: str
) -> Command:
    """Delete a workspace with HITL confirmation."""
    if not workspace_id:
        return error_command(
            "workspace_id is required for delete action", tool_call_id
        )

    from src.server.database.home_workspace import is_flash_row
    from src.server.database.workspace import get_workspace

    workspace = await get_workspace(workspace_id)
    if not workspace or str(workspace.get("user_id")) != user_id:
        return error_command("workspace not found", tool_call_id)
    # Refused before the card, so the user is never asked to approve a delete
    # that cannot happen.
    if is_flash_row(workspace):
        return error_command("Home cannot be deleted.", tool_call_id)

    # The card shows the name: an id alone cannot tell the user which
    # workspace they are approving the loss of.
    approved, _ = hitl_confirm(
        "delete_workspace",
        {"workspace_id": workspace_id, "workspace_name": workspace.get("name")},
    )

    if not approved:
        return decline_command(
            "User declined workspace deletion.", tool_call_id
        )

    from src.server.database.workspace import WorkspaceBusyError
    from src.server.services.workspace_manager import WorkspaceManager

    try:
        workspace_manager = WorkspaceManager.get_instance()
        await workspace_manager.delete_workspace(workspace_id)
        return success_command(
            {"success": True, "workspace_id": workspace_id},
            tool_call_id,
        )
    except (WorkspaceBusyError, ValueError) as e:
        # Refusals the user should hear: active work, or Home/flash.
        return error_command(str(e), tool_call_id)
    except Exception as e:
        logger.error(f"Failed to delete workspace: {e}")
        return error_command("failed to delete workspace", tool_call_id)


# ---------------------------------------------------------------------------
# Tool 2: ptc_agent
# ---------------------------------------------------------------------------


@tool("ptc_agent")
async def ptc_agent(
    question: str,
    config: RunnableConfig,
    state: Annotated[dict, InjectedState],
    workspace_id: str | None = None,
    thread_id: str | None = None,
    report_back: bool = True,
    tool_call_id: Annotated[str, InjectedToolCallId] = "",
) -> Command:
    """Dispatch a research question to a PTC agent.

    Two modes:
    - New thread: pass workspace_id (or omit to auto-create a workspace).
    - Continue thread: pass thread_id to send a follow-up message.

    The PTC agent runs asynchronously — use agent_output to check results.

    Args:
        question: The research question or follow-up message
        workspace_id: Workspace to create a new thread in. Ignored if thread_id is set.
        thread_id: Existing thread to continue. Overrides workspace_id.
        report_back: If True, flash will automatically summarize results when PTC completes.
            Set to False when the user wants to check results themselves.
    """
    return await dispatch(
        question, config, workspace_id, thread_id, report_back, tool_call_id,
        preapproved=preapproved(state, tool_call_id),
    )


# ---------------------------------------------------------------------------
# Tool 3: agent_output
# ---------------------------------------------------------------------------


@tool("agent_output")
async def agent_output(
    thread_id: str,
    config: RunnableConfig,
    turns: int = 1,
    tool_call_id: Annotated[str, InjectedToolCallId] = "",
) -> Command:
    """Read what a thread's agent wrote, in any of the user's workspaces, while it runs or after.

    Args:
        thread_id: The thread to read.
        turns: How many of the most-recent turns to return. Default 1 (only the
            latest turn's output). Pass a larger N for the last N turns, or 0
            for recent history (up to the 50 most recent turns); multiple turns
            are separated by '---'. A turn still streaming returns only that
            live turn.

    Returns:
        The agent's reply text, not its tool calls, each run under a line with its start time, how it ended and whether it was a report-back, and whether the thread is still running.
    """
    configurable = config.get("configurable", {})
    user_id = configurable.get("user_id")
    if not user_id:
        return error_command("user_id not found in config", tool_call_id)

    return await _get_thread_output(
        user_id, thread_id, tool_call_id, turns, _timezone(configurable)
    )


# ---------------------------------------------------------------------------
# Tool 4: manage_threads
# ---------------------------------------------------------------------------


@tool("manage_threads")
async def manage_threads(
    action: str,
    config: RunnableConfig,
    workspace_id: str | None = None,
    thread_id: str | None = None,
    turns: int = 1,
    tool_call_id: Annotated[str, InjectedToolCallId] = "",
) -> Command:
    """Manage conversation threads: list, get output, or delete.

    Args:
        action: One of "list", "get_output", "delete"
        workspace_id: Optional workspace ID to filter threads (for "list")
        thread_id: Thread ID (required for "get_output" and "delete")
        turns: For "get_output", how many recent turns to return. Default 1
            (latest only); N for the last N turns; 0 for recent history (up to
            the 50 most recent turns).
    """
    configurable = config.get("configurable", {})
    user_id = configurable.get("user_id")
    if not user_id:
        return error_command("user_id not found in config", tool_call_id)

    if action == "list":
        return await threads_list(user_id, workspace_id, tool_call_id)
    elif action == "get_output":
        return await _threads_get_output(
            user_id, thread_id, tool_call_id, turns, _timezone(configurable)
        )
    elif action == "delete":
        return await threads_delete(user_id, thread_id, tool_call_id)
    else:
        return error_command(
            f"Unknown action: {action}. Use list, get_output, or delete.",
            tool_call_id,
        )


async def threads_list(
    user_id: str, workspace_id: str | None, tool_call_id: str
) -> Command:
    """List threads, optionally filtered by workspace."""
    try:
        if workspace_id:
            if err := await verify_workspace_owner(workspace_id, user_id, tool_call_id):
                return err

            from src.server.database.conversation.threads_read import get_workspace_threads

            threads, total = await get_workspace_threads(
                workspace_id=workspace_id, limit=20
            )
        else:
            from src.server.database.conversation.threads_read import get_threads_for_user

            threads, total = await get_threads_for_user(
                user_id=user_id, limit=20
            )

        content = json.dumps(
            {
                "success": True,
                "threads": without_keys(threads, _OWNER_ID),
                "total": total,
            },
            default=str,
        )
    except Exception as e:
        logger.error(f"Failed to list threads: {e}")
        content = json.dumps({"success": False, "error": "failed to list threads"})

    return Command(
        update={
            "messages": [
                ToolMessage(content=content, tool_call_id=tool_call_id),
            ],
        }
    )


async def _threads_get_output(
    user_id: str,
    thread_id: str | None,
    tool_call_id: str,
    turns: int = 1,
    timezone: str = "UTC",
) -> Command:
    """Get output from a specific thread."""
    if not thread_id:
        return error_command(
            "thread_id is required for get_output action", tool_call_id
        )

    return await _get_thread_output(user_id, thread_id, tool_call_id, turns, timezone)


async def threads_stop(
    user_id: str,
    thread_id: str | None,
    own_thread_id: str | None,
    tool_call_id: str,
    run_id: str | None = None,
) -> Command:
    """Stop the turn ``run_id`` names on a thread, else whichever it is running.

    The same cancel as ``POST /cancel``, so the run ends through the finalize
    CAS and a hand-off with a report-back still reports that it was stopped.
    A run_id keeps a stop meant for a hand-off from ending a turn started in
    that thread after the hand-off finished.
    """
    from src.server.utils.pg_sanitize import normalize_uuid

    if not thread_id:
        return error_command("thread_id is required for stop", tool_call_id)
    thread_id = normalize_uuid(thread_id)
    if thread_id is None:
        return error_command("thread not found or not owned by user", tool_call_id)
    if thread_id == normalize_uuid(own_thread_id):
        return error_command(
            "That is this conversation; end your turn instead.", tool_call_id
        )
    # An empty run_id reads as omitted, as it does for POST /cancel.
    if run_id:
        run_id = normalize_uuid(run_id)
        if run_id is None:
            return error_command(
                "run_id is not a run id: pass the one delegate_to_analyst "
                "returned, or leave it out.",
                tool_call_id,
            )
    if err := await verify_thread_owner(thread_id, user_id, tool_call_id):
        return err

    from src.server.services.cancel_dispatch import cancel_workflow

    try:
        outcome = await cancel_workflow(thread_id, run_id=run_id or None)
    except Exception:
        # cancel_workflow logs the cause and raises it as a bare 500.
        return error_command("stop_failed", tool_call_id)
    if outcome["cancelled"]:
        # Said here, where the model decides what to do next: told only in the
        # prompt, it read the thread straight away and reported the stop twice.
        return success_command(
            {"success": True, "cancelled": True, "message": _STOPPING},
            tool_call_id,
        )
    # The stop ran and found nothing of that run to end; ``state`` says why.
    # The model named the thread to stop; echoing its id back invites it into
    # the reply.
    return success_command(
        {"success": True, **without_keys(outcome, {"thread_id"})}, tool_call_id
    )


async def threads_delete(
    user_id: str, thread_id: str | None, tool_call_id: str
) -> Command:
    """Delete a thread with HITL confirmation."""
    if not thread_id:
        return error_command(
            "thread_id is required for delete action", tool_call_id
        )

    if err := await verify_thread_owner(thread_id, user_id, tool_call_id):
        return err

    approved, _ = hitl_confirm(
        "delete_thread",
        {"thread_id": thread_id},
    )

    if not approved:
        return decline_command(
            "User declined thread deletion.", tool_call_id
        )

    try:
        from src.server.database.conversation.threads_read import get_thread_by_id
        from src.server.database.conversation.threads_write import delete_thread
        from src.server.services.thread_mutation import (
            MutationConflict,
            MutationUnavailable,
            ThreadMutationRunner,
        )

        # The row is gone after the delete, and the prune needs its workspace.
        try:
            thread_row = await get_thread_by_id(thread_id)
        except Exception:
            thread_row = None

        # Guarded delete, same fence as the HTTP endpoint (v4 2.4a): an
        # unfenced delete here would cascade away a live run's ledger rows
        # out from under a writer on any worker.
        try:
            async with ThreadMutationRunner.get_instance().exclusive(
                thread_id, "delete"
            ) as mutation:
                await delete_thread(thread_id, conn=mutation.conn)
        except MutationConflict as e:
            detail = e.detail if isinstance(e.detail, dict) else {}
            return error_command(
                detail.get("message")
                or "Thread is busy (a run or mutation is in progress); "
                "stop it first, then retry the delete.",
                tool_call_id,
            )
        except MutationUnavailable:
            return error_command(
                "Thread deletion is temporarily unavailable; retry shortly.",
                tool_call_id,
            )

        # Invalidate thread existence cache (matches HTTP delete endpoint)
        try:
            from src.server.database.conversation.threads_write import thread_exists_key
            from src.utils.cache.redis_cache import get_cache_client
            cache = get_cache_client()
            if cache.enabled and cache.client:
                await cache.client.delete(thread_exists_key(thread_id))
        except Exception:
            pass

        # As the HTTP endpoint does: a bring-up skips a workspace whose live
        # threads look unchanged, so it would not prune this one's dirs.
        if thread_row:
            from src.server.services.workspace_manager import WorkspaceManager

            manager = WorkspaceManager.current()
            if manager is not None:
                manager.prune_thread_dirs_soon(str(thread_row["workspace_id"]))

        return success_command(
            {"success": True, "thread_id": thread_id},
            tool_call_id,
        )
    except Exception as e:
        logger.error(f"Failed to delete thread: {e}")
        return error_command("failed to delete thread", tool_call_id)
