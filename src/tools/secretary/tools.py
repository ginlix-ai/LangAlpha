"""
Secretary tools: workspace management, PTC dispatch, agent monitoring, thread management.

These tools use interrupt() to pause the graph and wait for user approval
via the frontend, following the same HITL pattern as onboarding tools.
"""

import json
import logging
import os
import uuid
from typing import Annotated, Any

from langchain_core.messages import ToolMessage
from langchain_core.runnables import RunnableConfig
from langchain_core.tools import tool
from langgraph.types import Command, interrupt

try:
    from langchain.tools import InjectedToolCallId
except ImportError:
    from langchain_core.tools import InjectedToolCallId

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Shared HITL helper
# ---------------------------------------------------------------------------


def _hitl_confirm(
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


def _decline_command(message: str, tool_call_id: str) -> Command:
    """Return a Command for a declined HITL action."""
    return Command(
        update={
            "messages": [
                ToolMessage(content=message, tool_call_id=tool_call_id),
            ],
        }
    )


def _success_command(data: dict[str, Any], tool_call_id: str) -> Command:
    """Return a Command with JSON-serialized success data."""
    return Command(
        update={
            "messages": [
                ToolMessage(
                    content=json.dumps(data), tool_call_id=tool_call_id
                ),
            ],
        }
    )


def _error_command(error: str, tool_call_id: str) -> Command:
    """Return a Command with a JSON error response."""
    return Command(
        update={
            "messages": [
                ToolMessage(
                    content=json.dumps({"success": False, "error": error}),
                    tool_call_id=tool_call_id,
                ),
            ],
        }
    )


# ---------------------------------------------------------------------------
# Shared ownership verification helpers
# ---------------------------------------------------------------------------


async def _verify_workspace_owner(
    workspace_id: str, user_id: str, tool_call_id: str
) -> Command | None:
    """Return error Command if user doesn't own workspace, else None."""
    from src.server.database.workspace import get_workspace

    ws = await get_workspace(workspace_id)
    if not ws or str(ws.get("user_id")) != user_id:
        return _error_command("workspace not found", tool_call_id)
    return None


async def _cleanup_auto_created_workspace(workspace_id: str) -> None:
    """Best-effort delete of a just-created, provably-unused workspace."""
    try:
        from src.server.services.workspace_manager import WorkspaceManager

        await WorkspaceManager.get_instance().delete_workspace(workspace_id)
    except Exception as cleanup_err:
        logger.warning(
            f"Failed to delete auto-created workspace {workspace_id} "
            f"after failed dispatch: {cleanup_err}"
        )


async def _resolve_workspace_name(
    workspace_id: str | None, user_id: str
) -> str | None:
    """Display name for a workspace OWNED by ``user_id`` (else None).

    Ownership-scoped so the new-thread dispatch HITL card can't surface another
    user's workspace name before the ownership check runs.
    """
    if not workspace_id:
        return None
    from src.server.database.workspace import get_workspace

    try:
        ws = await get_workspace(workspace_id)
        if not ws or str(ws.get("user_id")) != user_id:
            return None
        return ws.get("name")
    except Exception as e:
        logger.warning(f"Failed to resolve workspace name for {workspace_id}: {e}")
        return None


async def _verify_thread_owner(
    thread_id: str, user_id: str, tool_call_id: str
) -> Command | None:
    """Return error Command if user doesn't own thread, else None."""
    from src.server.database.conversation import get_thread_owner_id

    try:
        owner_id = await get_thread_owner_id(thread_id)
        if owner_id != user_id:
            return _error_command(
                "thread not found or not owned by user", tool_call_id
            )
    except Exception as e:
        logger.error(f"Failed to verify thread ownership: {e}")
        return _error_command("thread not found", tool_call_id)
    return None


async def _get_thread_output(
    user_id: str, thread_id: str, tool_call_id: str, turns: int = 1
) -> Command:
    """Verify ownership and extract thread output.

    Shared by agent_output tool and manage_threads(action="get_output").
    ``turns`` bounds how many recent turns are returned (1 = latest only).
    """
    from src.tools.secretary.utils import extract_text_from_thread

    if err := await _verify_thread_owner(thread_id, user_id, tool_call_id):
        return err

    try:
        result = await extract_text_from_thread(thread_id, turns)
    except Exception as e:
        logger.error(f"Failed to extract text from thread {thread_id}: {e}")
        return _error_command("failed to retrieve thread output", tool_call_id)

    return _success_command(result, tool_call_id)


# ---------------------------------------------------------------------------
# Tool 1: manage_workspaces
# ---------------------------------------------------------------------------


@tool("manage_workspaces")
async def manage_workspaces(
    action: str,
    config: RunnableConfig,
    name: str | None = None,
    description: str | None = None,
    workspace_id: str | None = None,
    tool_call_id: Annotated[str, InjectedToolCallId] = "",
) -> Command:
    """Manage user workspaces: list, create, delete, or stop.

    Args:
        action: One of "list", "create", "delete", "stop"
        name: Workspace name (required for "create")
        description: Workspace description (optional, for "create")
        workspace_id: Workspace ID (required for "delete" and "stop")
    """
    configurable = config.get("configurable", {})
    user_id = configurable.get("user_id")
    if not user_id:
        return _error_command("user_id not found in config", tool_call_id)

    if action == "list":
        return await _workspaces_list(user_id, tool_call_id)
    elif action == "create":
        return await _workspaces_create(
            user_id, name, description, tool_call_id
        )
    elif action == "delete":
        return await _workspaces_delete(user_id, workspace_id, tool_call_id)
    elif action == "stop":
        return await _workspaces_stop(user_id, workspace_id, tool_call_id)
    else:
        return _error_command(
            f"Unknown action: {action}. Use list, create, delete, or stop.",
            tool_call_id,
        )


async def _workspaces_list(user_id: str, tool_call_id: str) -> Command:
    """List workspaces for the user."""
    try:
        from src.server.database.workspace import get_workspaces_for_user

        workspaces, total = await get_workspaces_for_user(
            user_id=user_id, limit=20
        )
        content = json.dumps(
            {"success": True, "workspaces": workspaces, "total": total},
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


async def _workspaces_create(
    user_id: str,
    name: str | None,
    description: str | None,
    tool_call_id: str,
) -> Command:
    """Create a new workspace with HITL confirmation."""
    if not name:
        return _error_command(
            "name is required for create action", tool_call_id
        )

    approved, _ = _hitl_confirm(
        "create_workspace",
        {"workspace_name": name, "workspace_description": description or ""},
    )

    if not approved:
        return _decline_command(
            "User declined workspace creation.", tool_call_id
        )

    try:
        from src.server.services.workspace_manager import WorkspaceManager

        workspace_manager = WorkspaceManager.get_instance()
        workspace = await workspace_manager.create_workspace(
            user_id=user_id,
            name=name,
            description=description,
        )

        workspace_id = str(workspace["workspace_id"])
        return _success_command(
            {
                "success": True,
                "workspace_id": workspace_id,
                "workspace_name": name,
            },
            tool_call_id,
        )
    except Exception as e:
        logger.error(f"Failed to create workspace: {e}")
        return _error_command("failed to create workspace", tool_call_id)


async def _workspaces_delete(
    user_id: str, workspace_id: str | None, tool_call_id: str
) -> Command:
    """Delete a workspace with HITL confirmation."""
    if not workspace_id:
        return _error_command(
            "workspace_id is required for delete action", tool_call_id
        )

    if err := await _verify_workspace_owner(workspace_id, user_id, tool_call_id):
        return err

    approved, _ = _hitl_confirm(
        "delete_workspace",
        {"workspace_id": workspace_id},
    )

    if not approved:
        return _decline_command(
            "User declined workspace deletion.", tool_call_id
        )

    try:
        from src.server.services.workspace_manager import WorkspaceManager

        workspace_manager = WorkspaceManager.get_instance()
        await workspace_manager.delete_workspace(workspace_id)
        return _success_command(
            {"success": True, "workspace_id": workspace_id},
            tool_call_id,
        )
    except Exception as e:
        logger.error(f"Failed to delete workspace: {e}")
        return _error_command("failed to delete workspace", tool_call_id)


async def _workspaces_stop(
    user_id: str, workspace_id: str | None, tool_call_id: str
) -> Command:
    """Stop a workspace with HITL confirmation."""
    if not workspace_id:
        return _error_command(
            "workspace_id is required for stop action", tool_call_id
        )

    if err := await _verify_workspace_owner(workspace_id, user_id, tool_call_id):
        return err

    approved, _ = _hitl_confirm(
        "stop_workspace",
        {"workspace_id": workspace_id},
    )

    if not approved:
        return _decline_command(
            "User declined workspace stop.", tool_call_id
        )

    try:
        from src.server.services.workspace_manager import WorkspaceManager

        workspace_manager = WorkspaceManager.get_instance()
        await workspace_manager.stop_workspace(workspace_id)
        return _success_command(
            {"success": True, "workspace_id": workspace_id},
            tool_call_id,
        )
    except Exception as e:
        logger.error(f"Failed to stop workspace: {e}")
        return _error_command("failed to stop workspace", tool_call_id)


# ---------------------------------------------------------------------------
# Tool 2: ptc_agent
# ---------------------------------------------------------------------------


@tool("ptc_agent")
async def ptc_agent(
    question: str,
    config: RunnableConfig,
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
    import aiohttp

    configurable = config.get("configurable", {})
    user_id = configurable.get("user_id")
    if not user_id:
        return _error_command("user_id not found in config", tool_call_id)

    # A PTC dispatch is delivered by POSTing to the internal /messages endpoint
    # with ``X-Dispatch: background``. That header is only honoured when the
    # request also carries a matching ``INTERNAL_SERVICE_TOKEN`` (see
    # ``is_internal`` in src/server/app/threads.py). When the token is unset the
    # endpoint silently downgrades to a *foreground* SSE stream: it runs the
    # full PTC workflow synchronously (minutes, many LLM calls) and returns
    # ``text/event-stream`` -- which this tool cannot parse as the JSON dispatch
    # ack, so the user sees ``dispatch_failed`` while model credits are burned
    # for a result that is never delivered. A dispatch that can never be
    # honoured as background is a deployment misconfiguration; fail loud here
    # instead of firing a doomed, credit-burning request.
    if not os.environ.get("INTERNAL_SERVICE_TOKEN", "").strip():
        logger.error(
            "PTC dispatch aborted: INTERNAL_SERVICE_TOKEN is not set. Without "
            "it the background dispatch cannot be authenticated and the "
            "workflow would run in the foreground, burning credits with no "
            "result. Set INTERNAL_SERVICE_TOKEN on the backend service to "
            "enable dispatch."
        )
        return _error_command("internal_service_token_missing", tool_call_id)

    is_continuation = thread_id is not None

    # Resolve workspace_id from existing thread or create/verify workspace
    if is_continuation:
        from src.server.database.conversation import get_thread_by_id
        from src.server.utils.pg_sanitize import normalize_uuid

        # Normalize once so the owner check and the fetch bind the same canonical
        # UUID. get_thread_owner_id normalizes internally; get_thread_by_id binds
        # raw, so a non-canonical id (e.g. urn:uuid:...) would pass the owner
        # check yet 22P02 on the fetch. None means not a UUID -> not found.
        normalized_id = normalize_uuid(thread_id)
        if normalized_id is None:
            return _error_command(
                "thread not found or not owned by user", tool_call_id
            )
        thread_id = normalized_id

        # Ownership lives on workspaces.user_id (conversation_threads has no
        # user_id column), so verify via the JOIN helper rather than reading
        # a user_id off the thread row, which is always None.
        if err := await _verify_thread_owner(thread_id, user_id, tool_call_id):
            return err
        thread = await get_thread_by_id(thread_id)
        workspace_id = str(thread["workspace_id"])
        workspace_name = await _resolve_workspace_name(workspace_id, user_id)
    else:
        # New thread: surface the existing workspace's real name; when
        # auto-creating (no workspace_id) use the planned name (question snippet).
        if workspace_id:
            workspace_name = await _resolve_workspace_name(workspace_id, user_id)
        else:
            workspace_name = question[:50].strip()

    approved, response = _hitl_confirm(
        "ptc_agent",
        {
            "workspace_id": workspace_id,
            "workspace_name": workspace_name,
            "thread_id": thread_id,
            "question": question,
            "report_back": report_back,
            "tool_call_id": tool_call_id,
        },
    )

    if not approved:
        return _decline_command(
            "User declined PTC agent dispatch.", tool_call_id
        )

    # Apply user overrides from HITL decision (e.g. toggling report_back)
    decisions = response.get("decisions", [])
    if decisions:
        overrides = decisions[0].get("overrides", {})
        if "report_back" in overrides:
            report_back = overrides["report_back"]

    auto_created_workspace = False
    if not is_continuation:
        # Create workspace or verify ownership
        if workspace_id is None:
            from src.server.handlers.chat.report_back import check_dispatch_capacity

            # Advisory cap check BEFORE provisioning: a dispatch reserve() is
            # certain to reject must not spin up a sandbox it would orphan.
            # reserve() below remains the atomic authority.
            cap_err = await check_dispatch_capacity(
                configurable.get("thread_id") if report_back else None, user_id
            )
            if cap_err is not None:
                return _error_command(cap_err, tool_call_id)
            try:
                from src.server.services.workspace_manager import WorkspaceManager

                workspace_manager = WorkspaceManager.get_instance()
                workspace = await workspace_manager.create_workspace(
                    user_id=user_id,
                    name=workspace_name or "Research",
                    description=f"Auto-created for: {question[:100]}",
                )
                workspace_id = str(workspace["workspace_id"])
                auto_created_workspace = True
            except Exception as e:
                logger.error(f"Failed to create workspace for PTC dispatch: {e}")
                return _error_command("workspace_creation_failed", tool_call_id)
        else:
            if err := await _verify_workspace_owner(workspace_id, user_id, tool_call_id):
                return err

        # New thread
        thread_id = str(uuid.uuid4())

    # reserve() takes a cap slot + records the PTC origin, rolling back on any
    # non-committed exit; a no-op when flash_thread_id is None (report_back off).
    # ``slot.wired`` (not the request flag) is echoed as report_back so we never
    # promise a report-back the completion gate would drop.
    from src.server.handlers.chat.report_back import reserve

    flash_thread_id = configurable.get("thread_id") if report_back else None
    flash_workspace_id = configurable.get("workspace_id")

    # Dispatch via internal HTTP call.
    # X-Dispatch: background tells the endpoint to run the PTC workflow in a
    # background asyncio task and return JSON immediately, avoiding the
    # generator-cancelled-on-client-disconnect race.
    self_base_url = os.environ.get("GINLIXFLOW_BASE_URL", "http://localhost:8000")
    service_token = os.environ.get("INTERNAL_SERVICE_TOKEN", "")

    async with reserve(
        flash_thread_id, thread_id, workspace_id, flash_workspace_id, user_id
    ) as slot:
        # Cap rejection or a fail-closed origin write — abort (reserve rolls back).
        if slot.error is not None:
            # No HTTP was sent, so a workspace auto-created above is provably
            # unused — delete it rather than leak its sandbox (the pre-check
            # narrows this to the pre-check/reserve race).
            if auto_created_workspace:
                await _cleanup_auto_created_workspace(workspace_id)
            return _error_command(slot.error, tool_call_id)
        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    f"{self_base_url}/api/v1/threads/{thread_id}/messages",
                    json={
                        "messages": [{"role": "user", "content": question}],
                        "agent_mode": "ptc",
                        "workspace_id": workspace_id,
                    },
                    headers={
                        "X-Service-Token": service_token,
                        "X-User-Id": user_id,
                        "X-Dispatch": "background",
                    },
                    timeout=aiohttp.ClientTimeout(connect=10, sock_read=30),
                ) as resp:
                    if resp.status >= 400:
                        # An error status proves the endpoint exited before
                        # scheduling the run (every raise path precedes its
                        # create_task), so an auto-created workspace is still
                        # provably unused. The timeout/connection/body branches
                        # below deliberately keep it: the run may have started.
                        if auto_created_workspace:
                            await _cleanup_auto_created_workspace(workspace_id)
                        return _error_command("dispatch_failed", tool_call_id)
                    body = await resp.json()
                    if not body.get("status") == "dispatched":
                        return _error_command("dispatch_failed", tool_call_id)
        except (aiohttp.ClientError, ValueError) as e:
            logger.error(f"PTC dispatch HTTP error: {e}")
            return _error_command("dispatch_failed", tool_call_id)
        except TimeoutError:
            logger.error("PTC dispatch timed out")
            return _error_command("dispatch_timeout", tool_call_id)

        # The run is durably started — keep the reservation. Any earlier return
        # exits the CM without commit() and rolls it back.
        slot.commit()
        return _success_command(
            {
                "success": True,
                "workspace_id": workspace_id,
                "thread_id": thread_id,
                "status": "dispatched",
                "report_back": slot.wired,
            },
            tool_call_id,
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
    """Retrieve the text output of a running or completed PTC agent thread.

    Use this to check on the progress or results of a dispatched PTC agent.

    Args:
        thread_id: The thread ID to retrieve output from
        turns: How many of the most-recent turns to return. Default 1 (only the
            latest turn's output). Pass a larger N for the last N turns, or 0
            for recent history (up to the 50 most recent turns); multiple turns
            are separated by '---'. A turn still streaming returns only that
            live turn.
    """
    configurable = config.get("configurable", {})
    user_id = configurable.get("user_id")
    if not user_id:
        return _error_command("user_id not found in config", tool_call_id)

    return await _get_thread_output(user_id, thread_id, tool_call_id, turns)


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
        return _error_command("user_id not found in config", tool_call_id)

    if action == "list":
        return await _threads_list(user_id, workspace_id, tool_call_id)
    elif action == "get_output":
        return await _threads_get_output(user_id, thread_id, tool_call_id, turns)
    elif action == "delete":
        return await _threads_delete(user_id, thread_id, tool_call_id)
    else:
        return _error_command(
            f"Unknown action: {action}. Use list, get_output, or delete.",
            tool_call_id,
        )


async def _threads_list(
    user_id: str, workspace_id: str | None, tool_call_id: str
) -> Command:
    """List threads, optionally filtered by workspace."""
    try:
        if workspace_id:
            if err := await _verify_workspace_owner(workspace_id, user_id, tool_call_id):
                return err

            from src.server.database.conversation import get_workspace_threads

            threads, total = await get_workspace_threads(
                workspace_id=workspace_id, limit=20
            )
        else:
            from src.server.database.conversation import get_threads_for_user

            threads, total = await get_threads_for_user(
                user_id=user_id, limit=20
            )

        content = json.dumps(
            {"success": True, "threads": threads, "total": total},
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
    user_id: str, thread_id: str | None, tool_call_id: str, turns: int = 1
) -> Command:
    """Get output from a specific thread."""
    if not thread_id:
        return _error_command(
            "thread_id is required for get_output action", tool_call_id
        )

    return await _get_thread_output(user_id, thread_id, tool_call_id, turns)


async def _threads_delete(
    user_id: str, thread_id: str | None, tool_call_id: str
) -> Command:
    """Delete a thread with HITL confirmation."""
    if not thread_id:
        return _error_command(
            "thread_id is required for delete action", tool_call_id
        )

    if err := await _verify_thread_owner(thread_id, user_id, tool_call_id):
        return err

    approved, _ = _hitl_confirm(
        "delete_thread",
        {"thread_id": thread_id},
    )

    if not approved:
        return _decline_command(
            "User declined thread deletion.", tool_call_id
        )

    try:
        from src.server.database.conversation import delete_thread

        await delete_thread(thread_id)

        # Invalidate thread existence cache (matches HTTP delete endpoint)
        try:
            from src.server.database.conversation import thread_exists_key
            from src.utils.cache.redis_cache import get_cache_client
            cache = get_cache_client()
            if cache.enabled and cache.client:
                await cache.client.delete(thread_exists_key(thread_id))
        except Exception:
            pass

        return _success_command(
            {"success": True, "thread_id": thread_id},
            tool_call_id,
        )
    except Exception as e:
        logger.error(f"Failed to delete thread: {e}")
        return _error_command("failed to delete thread", tool_call_id)
