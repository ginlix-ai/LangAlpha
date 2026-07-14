"""
Secretary tools: workspace management, PTC dispatch, agent monitoring, thread management.

These tools use interrupt() to pause the graph and wait for user approval
via the frontend, following the same HITL pattern as onboarding tools.
"""

import asyncio
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


_DISPATCH_CONFIRM_GRACE_S = 6.0
_DISPATCH_CONFIRM_POLL_S = 0.5


async def _confirm_dispatch_admission(
    thread_id: str, expected_gen: str | None
) -> bool:
    """Probe the endpoint's admission marker after an ambiguous dispatch
    exchange. The dispatched branch writes its WorkflowTracker blob — stamped
    with the POST's dispatch generation — BEFORE scheduling the run and
    replying, aborts if that write fails, and every rejection path exits
    before it. The marker is a POSITIVE-ONLY oracle: True means the marker
    provably belongs to THIS dispatch — an exact generation match when the
    dispatch carries one, or any blob when it doesn't (callers pass
    ``expected_gen=None`` only for a thread id minted by this very call, so
    nobody else can have triggered a marker on it). False settles NOTHING:
    a foreign blob may be a stale terminal our own admission is about to
    replace (so it keeps polling to the deadline, never returns early), and
    no finite absence proves a delivered, still-processing request won't
    admit later. The caller must treat False as unproven and retain. 2.4
    removes the WorkflowTracker; this probe moves to the durable attempt
    row then.
    """
    from src.server.services.workflow_tracker import WorkflowTracker
    from src.utils.cache.redis_cache import get_cache_client

    cache = get_cache_client()
    if not (cache.enabled and cache.client):
        return False
    key = f"{WorkflowTracker.STATUS_PREFIX}{thread_id}"
    loop = asyncio.get_running_loop()
    deadline = loop.time() + _DISPATCH_CONFIRM_GRACE_S
    while True:
        try:
            blob = await cache.get_strict(key)
        except Exception:
            blob = None
        if isinstance(blob, dict):
            if expected_gen is None:
                return True
            marker_gen = (blob.get("metadata") or {}).get("origin_dispatch_gen")
            if marker_gen == expected_gen:
                return True
        if loop.time() >= deadline:
            return False
        await asyncio.sleep(_DISPATCH_CONFIRM_POLL_S)


def _unknown_dispatch_command(
    error: str, thread_id: str, workspace_id: str | None, tool_call_id: str
) -> Command:
    """Ambiguous dispatch outcome: the reservation is retained and the run may
    already be live on ``thread_id`` — surface that id so the model checks
    agent_output before re-dispatching (a blind retry would occupy a second
    cap slot and can produce a duplicate report-back)."""
    return Command(
        update={
            "messages": [
                ToolMessage(
                    content=json.dumps(
                        {
                            "success": False,
                            "error": error,
                            "outcome": "unknown_retained",
                            "thread_id": thread_id,
                            "workspace_id": workspace_id,
                            "note": (
                                "Dispatch outcome unknown — the analysis may "
                                "already be running on this thread. Check "
                                "agent_output with this thread_id before "
                                "re-dispatching."
                            ),
                        }
                    ),
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

    # With auth enabled, the endpoint rejects an unauthenticated background
    # dispatch (403); abort before any side effect (HITL prompt, workspace
    # creation, cap reservation) so the user gets the specific error instead.
    from src.config.settings import background_dispatch_requires_token

    if background_dispatch_requires_token():
        logger.error(
            "PTC dispatch aborted: INTERNAL_SERVICE_TOKEN is not set, so the "
            "background dispatch cannot be authenticated. Set it on the "
            "backend service to enable dispatch."
        )
        return _error_command("internal_service_token_missing", tool_call_id)

    is_continuation = thread_id is not None

    # Resolve workspace_id from existing thread or create/verify workspace
    if is_continuation:
        from src.server.database.conversation import get_thread_by_id
        from src.server.utils.pg_sanitize import normalize_uuid

        # Normalize once so the owner check and every downstream bind use the
        # same canonical UUID (get_thread_owner_id and get_thread_by_id also
        # normalize internally). None means not a UUID -> not found.
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
        # ``rejected``: a definitive non-scheduling proof was observed — a
        # cancellation arriving during the subsequent best-effort cleanup must
        # then NOT commit. ``ambiguous_error``: delivery unproven either way;
        # settled by the admission-marker reconciliation below the try.
        rejected = False
        ambiguous_error: str | None = None
        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    f"{self_base_url}/api/v1/threads/{thread_id}/messages",
                    json={
                        "messages": [{"role": "user", "content": question}],
                        "agent_mode": "ptc",
                        "workspace_id": workspace_id,
                        # Ordering hint for finalize hooks: only a WIRED
                        # report-back binds the run to this flash thread's
                        # serialization chain.
                        "origin_flash_thread_id": (
                            flash_thread_id if slot.wired else None
                        ),
                        # Incarnation token minted by reserve(): fences this
                        # dispatch's terminal teardowns to its own generation.
                        "origin_dispatch_gen": (
                            slot.dispatch_gen if slot.wired else None
                        ),
                    },
                    headers={
                        "X-Service-Token": service_token,
                        "X-User-Id": user_id,
                        "X-Dispatch": "background",
                    },
                    timeout=aiohttp.ClientTimeout(connect=10, sock_read=30),
                    allow_redirects=False,
                ) as resp:
                    if resp.status >= 400:
                        # An error status proves the endpoint exited before
                        # scheduling the run (every raise path precedes its
                        # create_task), so an auto-created workspace is still
                        # provably unused.
                        rejected = True
                        if auto_created_workspace:
                            await _cleanup_auto_created_workspace(workspace_id)
                        return _error_command("dispatch_failed", tool_call_id)
                    if resp.status != 200:
                        # Not the endpoint's reply (it answers exactly 200;
                        # redirects are disabled): some other hop spoke —
                        # whether the handler ran is settled below.
                        ambiguous_error = "dispatch_failed"
                    else:
                        # The endpoint's exact success status IS the
                        # scheduling proof (it replies only after its
                        # create_task) — commit BEFORE touching the body, so
                        # a lost/truncated body can't roll back a reservation
                        # whose run is already live (the run's report-back
                        # would find no origin and be dropped).
                        slot.commit()
                        try:
                            body = await resp.json()
                        except (aiohttp.ClientError, ValueError, TimeoutError):
                            logger.warning(
                                "PTC dispatch response body lost after "
                                "success status 200; treating as dispatched"
                            )
                            body = {"status": "dispatched"}
                        if (
                            not isinstance(body, dict)
                            or body.get("status") != "dispatched"
                        ):
                            # A 200 carrying a contradictory body: the status
                            # proof stands (never roll back), but don't claim
                            # success — reconcile below.
                            ambiguous_error = "dispatch_failed"
        except asyncio.CancelledError:
            # Cancellation mid-exchange (flash turn cancelled, worker
            # shutdown) is as ambiguous as a lost response: the endpoint may
            # already have scheduled the run, so commit before propagating —
            # UNLESS the outcome was already definitively rejected and the
            # cancel merely landed during the best-effort cleanup.
            if not rejected:
                slot.commit()
            raise
        except (aiohttp.ClientConnectorError, aiohttp.InvalidURL) as e:
            # The request provably never reached the endpoint — rolling the
            # reservation back is safe and an auto-created workspace is
            # provably unused. (A cancel during this cleanup propagates
            # uncommitted — the rollback is exactly what's wanted.)
            logger.error(f"PTC dispatch connection failed: {e}")
            if auto_created_workspace:
                await _cleanup_auto_created_workspace(workspace_id)
            return _error_command("dispatch_failed", tool_call_id)
        except (aiohttp.ClientError, ValueError) as e:
            logger.error(f"PTC dispatch HTTP error: {e}")
            ambiguous_error = "dispatch_failed"
        except TimeoutError:
            logger.error("PTC dispatch timed out")
            ambiguous_error = "dispatch_timeout"

        if ambiguous_error is not None:
            # Settle the unknown against the endpoint's admission marker,
            # scoped to THIS dispatch's generation (the endpoint stamps the
            # POST's origin_dispatch_gen into the marker and refuses to
            # schedule if the write fails). The oracle is POSITIVE-ONLY:
            # confirmation upgrades to plain success; anything less retains
            # the reservation as unknown (TTL-bounded, orphan-reaped once
            # the origin lapses). Reconciliation NEVER rolls back — the only
            # sound rollback receipts are a definitive HTTP status (the
            # >=400 branch) or a provably-undelivered connection, both
            # handled above. In particular a continuation must not roll back
            # on a foreign/absent marker: our own admission may stamp
            # moments later, and destroying the provisional origin then
            # orphans a LIVE run's report-back (the retained-but-409'd
            # alternative merely wedges one cap slot until the origin TTL).
            if slot.wired:
                expected_gen = slot.dispatch_gen
            elif not is_continuation:
                # Unwired fresh pair: the thread id was minted by this call,
                # so ANY marker on it can only be our own admission.
                expected_gen = None
            else:
                # Unwired continuation: no identity to match — a marker
                # proves only that SOME run held the thread. Unprovable;
                # retain without probing.
                slot.commit()
                return _unknown_dispatch_command(
                    ambiguous_error, thread_id, workspace_id, tool_call_id
                )
            try:
                confirmed = await _confirm_dispatch_admission(
                    thread_id, expected_gen
                )
            except asyncio.CancelledError:
                # Cancelled mid-probe: still unknown — retain.
                slot.commit()
                raise
            slot.commit()
            if confirmed:
                # The lost reply was a real acceptance of THIS request.
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
            return _unknown_dispatch_command(
                ambiguous_error, thread_id, workspace_id, tool_call_id
            )

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
