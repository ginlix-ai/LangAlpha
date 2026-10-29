"""The workspace and thread actions behind the Chief of Staff's tools.

The tools themselves are in ``chief_of_staff``; a hand-off runs through
``dispatch``.
"""

import asyncio
import json
import logging

from langchain_core.messages import ToolMessage
from langgraph.types import Command

from ptc_agent.agent.limits import pending_read
from src.server.contracts.status import is_live
from src.tools.secretary._commands import (
    decline_command,
    decline_routing_command,
    error_command,
    hitl_confirm,
    success_command,
    verify_thread_owner,
    verify_workspace_owner,
)
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


async def workspaces_list(user_id: str, tool_call_id: str) -> Command:
    """List workspaces for the user.

    A row names its folder, which sits at the root of its computer, and its
    computer, with whether that is the user's own, only when the user has
    more than one; on another computer than Home's, the folder's whole path
    too, since calls there take that machine's own paths.
    """
    try:
        from src.server.database.workspace import get_workspaces_for_user
        from src.tools.secretary.utils import read_computer_placement

        # Every workspace, since one left off the list is one the agent tells
        # the user does not exist. Pinned, then most recently used: the one a
        # hand-off is for is far likelier to be recent than early in the
        # user's custom order.
        (workspaces, _), placement = await asyncio.gather(
            get_workspaces_for_user(user_id=user_id, limit=None, sort_by="activity"),
            read_computer_placement(user_id),
        )
        rows = []
        for ws in workspaces:
            row = {field: ws.get(field) for field in _LISTED_WORKSPACE_FIELDS}
            if path := placement.path(ws):
                row["path"] = path
            if placement.computers:
                row["computer"] = placement.computer(ws)
            if machine := placement.machine(ws):
                row["machine"] = machine
            rows.append(row)
        content = json.dumps({"success": True, "workspaces": rows}, default=str)
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
    from src.server.services.computer_errors import ComputerPreparingError

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
    except ComputerPreparingError:
        return error_command(
            "The user's computer is still being prepared, so no workspace can "
            "be created on it yet. Tell the user to try again once it is ready.",
            tool_call_id,
        )
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

    from src.server.database.home_workspace import is_home_row
    from src.server.database.workspace import get_workspace

    workspace = await get_workspace(workspace_id)
    if not workspace or str(workspace.get("user_id")) != user_id:
        return error_command("workspace not found", tool_call_id)
    # Refused before the card, so the user is never asked to approve a delete
    # that cannot happen.
    if is_home_row(workspace):
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
        # Refusals the user should hear: active work, or Home.
        return error_command(str(e), tool_call_id)
    except Exception as e:
        logger.error(f"Failed to delete workspace: {e}")
        return error_command("failed to delete workspace", tool_call_id)


async def threads_list(
    user_id: str,
    workspace_id: str | None,
    tool_call_id: str,
    own_thread_id: str | None = None,
) -> Command:
    """List threads, optionally filtered by workspace.

    A listing that shows another thread mid-run is marked for the loop guard,
    as a read of that thread is. The caller's own thread is always running,
    so it does not count.
    """
    from src.server.utils.pg_sanitize import normalize_uuid

    artifact = None
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
        own = normalize_uuid(own_thread_id) if own_thread_id else None
        if any(
            is_live(row.get("current_status"))
            and normalize_uuid(row.get("conversation_thread_id")) != own
            for row in threads
        ):
            artifact = pending_read()
    except Exception as e:
        logger.error(f"Failed to list threads: {e}")
        content = json.dumps({"success": False, "error": "failed to list threads"})

    return Command(
        update={
            "messages": [
                ToolMessage(
                    content=content, tool_call_id=tool_call_id, artifact=artifact
                ),
            ],
        }
    )


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
        outcome = await cancel_workflow(
            thread_id, run_id=run_id or None, by_user=False
        )
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
        {"success": True, **without_keys(outcome, {"thread_id", "returned_steers"})}, tool_call_id
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
        from src.server.services.commands.control import delete_thread_ending_commands
        from src.server.services.thread_mutation import (
            MutationConflict,
            MutationUnavailable,
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
            await delete_thread_ending_commands(thread_id)
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
            from src.server.services.workspace_manager import prune_thread_dirs_soon

            prune_thread_dirs_soon(str(thread_row["workspace_id"]))

        return success_command(
            {"success": True, "thread_id": thread_id},
            tool_call_id,
        )
    except Exception as e:
        logger.error(f"Failed to delete thread: {e}")
        return error_command("failed to delete thread", tool_call_id)
