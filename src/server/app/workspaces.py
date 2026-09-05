"""
Workspace Management API Router.

Provides CRUD endpoints for managing workspaces, where each workspace
has a dedicated Daytona sandbox (1:1 mapping).

Endpoints:
- POST /api/v1/workspaces - Create workspace
- GET /api/v1/workspaces - List workspaces
- GET /api/v1/workspaces/{workspace_id} - Get workspace details
- PUT /api/v1/workspaces/{workspace_id} - Update workspace
- POST /api/v1/workspaces/{workspace_id}/start - Start stopped workspace
- POST /api/v1/workspaces/{workspace_id}/stop - Stop running workspace
- DELETE /api/v1/workspaces/{workspace_id} - Delete workspace
"""

import asyncio
import contextlib
import json
import logging
import time
from typing import Literal

from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import Response, StreamingResponse

from src.server.utils.api import CurrentUserId, require_workspace_owner
from src.server.dependencies.usage_limits import (
    ALWAYS_ON_QUOTA,
    SPEC_QUOTAS,
    WorkspaceLimitCheck,
    assert_always_on_allowed,
    assert_spec_allowed,
    get_capacity_status,
)
from src.server.database.workspace import (
    get_workspace as db_get_workspace,
    get_workspaces_for_user,
    update_workspace as db_update_workspace,
    get_or_create_flash_workspace,
    batch_update_sort_order,
)
from src.server.models.workspace import (
    WorkspaceActionResponse,
    WorkspaceAlwaysOnRequest,
    WorkspaceCreate,
    WorkspaceListResponse,
    WorkspaceQuotaResponse,
    WorkspaceReorderRequest,
    WorkspaceResponse,
    WorkspaceSpecRequest,
    WorkspaceUpdate,
)
from ptc_agent.core.sandbox.runtime import SandboxGoneError, SandboxTransientError
from src.server.utils.error_sanitization import sandbox_unreachable_detail
from src.server.models.workspace_refresh import WorkspaceRefreshResponse
from src.server.services.user_skills import sandbox_skill_sync_params
from src.server.services.workspace_manager import WorkspaceManager
from src.server.services.workspace_status_pubsub import subscribe_to_status

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1/workspaces", tags=["Workspaces"])


@contextlib.asynccontextmanager
async def _workspace_action_errors(action: str, workspace_id: str):
    """Shared error mapping for workspace action routes.

    Re-raises HTTPException so ``require_workspace_owner``'s 403/404 pass
    through, maps ValueError→404 and RuntimeError→400, and turns anything
    else into a logged 500.
    """
    try:
        yield
    except HTTPException:
        raise
    except (SandboxGoneError, SandboxTransientError):
        # Before the RuntimeError arm, which these subclass. An unreachable
        # sandbox is not a bad request: letting it fall through answered 400
        # with the provider's raw text, which both loses the 503 the file panel
        # keys on and ships request URLs and SDK bodies to the client. Re-raise
        # for the app-level handler that owns the wording and the sanitizing.
        raise
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except RuntimeError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.exception(f"Error {action} workspace {workspace_id}: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to {action} workspace")


def _workspace_to_response(workspace: dict) -> WorkspaceResponse:
    """Convert workspace dict to response model."""
    return WorkspaceResponse(
        workspace_id=str(workspace["workspace_id"]),
        user_id=workspace["user_id"],
        name=workspace["name"],
        description=workspace.get("description"),
        sandbox_id=workspace.get("sandbox_id"),
        status=workspace["status"],
        created_at=workspace["created_at"],
        updated_at=workspace["updated_at"],
        last_activity_at=workspace.get("last_activity_at"),
        stopped_at=workspace.get("stopped_at"),
        config=workspace.get("config"),
        is_pinned=workspace.get("is_pinned", False),
        sort_order=workspace.get("sort_order", 0),
        resource_tier=workspace.get("resource_tier", "standard"),
        is_always_on=workspace.get("is_always_on", False),
    )


@router.post("", response_model=WorkspaceResponse, status_code=201)
async def create_workspace(
    request: WorkspaceCreate,
    x_user_id: WorkspaceLimitCheck,
):
    """
    Create a new workspace with dedicated sandbox.

    This creates a new Daytona sandbox for the workspace. The operation
    may take 30-60 seconds as the sandbox needs to be initialized.

    Args:
        request: Workspace creation request
        x_user_id: User ID from header

    Returns:
        Created workspace details
    """
    try:
        manager = WorkspaceManager.get_instance()
        workspace = await manager.create_workspace(
            user_id=x_user_id,
            name=request.name,
            description=request.description,
            config=request.config,
        )

        logger.info(
            f"Created workspace {workspace['workspace_id']} for user {x_user_id}"
        )
        return _workspace_to_response(workspace)

    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.exception(f"Error creating workspace: {e}")
        raise HTTPException(status_code=500, detail="Failed to create workspace")


@router.post("/flash", response_model=WorkspaceResponse)
async def get_flash_workspace(
    x_user_id: CurrentUserId,
):
    """
    Get or create the shared flash workspace for this user.

    Uses a deterministic UUID so the same user always gets the same workspace.
    Idempotent — safe to call on every app load.

    Returns:
        Flash workspace details
    """
    try:
        workspace = await get_or_create_flash_workspace(x_user_id)
        return _workspace_to_response(workspace)
    except Exception as e:
        logger.exception(f"Error ensuring flash workspace: {e}")
        raise HTTPException(status_code=500, detail="Failed to ensure flash workspace")


@router.post("/reorder", status_code=204)
async def reorder_workspaces(
    request: WorkspaceReorderRequest,
    x_user_id: CurrentUserId,
):
    """
    Batch-update workspace sort order.

    Accepts a list of workspace_id + sort_order pairs and updates them
    in a single query. Only workspaces owned by the requesting user
    are affected.
    """
    try:
        items = [(str(item.workspace_id), item.sort_order) for item in request.items]
        await batch_update_sort_order(user_id=x_user_id, items=items)
    except Exception as e:
        logger.exception(f"Error reordering workspaces: {e}")
        raise HTTPException(status_code=500, detail="Failed to reorder workspaces")


@router.get("", response_model=WorkspaceListResponse)
async def list_workspaces(
    x_user_id: CurrentUserId,
    limit: int = Query(20, ge=1, le=100, description="Maximum results"),
    offset: int = Query(0, ge=0, description="Number to skip"),
    sort_by: Literal["activity", "name", "custom"] = Query(
        "custom", description="Sort mode: activity, name, or custom"
    ),
    include_flash: bool = Query(False, description="Include flash workspaces in results"),
):
    """
    List workspaces for a user.

    Args:
        x_user_id: User ID from header
        limit: Maximum number of results (1-100)
        offset: Number of results to skip
        sort_by: Sort mode — 'activity' (updated_at), 'name' (alphabetical), 'custom' (sort_order)
        include_flash: Whether to include flash workspaces (default false)

    Returns:
        Paginated list of workspaces
    """
    try:
        workspaces, total = await get_workspaces_for_user(
            user_id=x_user_id,
            limit=limit,
            offset=offset,
            sort_by=sort_by,
            include_flash=include_flash,
        )

        return WorkspaceListResponse(
            workspaces=[_workspace_to_response(w) for w in workspaces],
            total=total,
            limit=limit,
            offset=offset,
        )

    except Exception as e:
        logger.exception(f"Error listing workspaces: {e}")
        raise HTTPException(status_code=500, detail="Failed to list workspaces")


# Declared before the `/{workspace_id}` routes so `/quota` isn't captured as an id.
@router.get("/quota", response_model=WorkspaceQuotaResponse)
async def get_workspace_quota(x_user_id: CurrentUserId):
    """Per-capability count quotas for the change-spec / always-on UI.

    Platform mode only — every field is null in OSS mode, so the UI just omits the
    remaining-count hint. Fails open: a capability the platform can't report comes
    back null rather than erroring the whole call.
    """
    performance, maximum, always_on = await asyncio.gather(
        get_capacity_status(x_user_id, SPEC_QUOTAS["performance"]),
        get_capacity_status(x_user_id, SPEC_QUOTAS["max"]),
        get_capacity_status(x_user_id, ALWAYS_ON_QUOTA),
    )
    return WorkspaceQuotaResponse(
        performance=performance,
        max=maximum,
        always_on=always_on,
    )


_SSE_HEADERS = {
    "Cache-Control": "no-cache",
    "X-Accel-Buffering": "no",
    "Connection": "keep-alive",
}
_EVENTS_KEEPALIVE_S = 30.0
_EVENTS_MAX_DURATION_S = 600.0
_EVENTS_TERMINAL = {"running", "error", "deleted"}


def _sse_status_event(
    workspace_id: str, status: str, sandbox_state: str | None = None
) -> str:
    data = {"workspace_id": workspace_id, "status": status}
    if sandbox_state:
        data["sandbox_state"] = sandbox_state
    return f"event: status\ndata: {json.dumps(data)}\n\n"


@router.get("/{workspace_id}/events")
async def workspace_status_events(workspace_id: str, x_user_id: CurrentUserId):
    """Push workspace lifecycle status changes to the client via SSE.

    Replaces the previous 4-second interval polling on the frontend. The
    handler emits the current status immediately, then one ``status``
    event per transition (stopped → starting → running). Closes once the
    status is terminal (``running``, ``error``, ``deleted``), or after a
    600 s hard cap. A ``: ping\\n\\n`` keepalive is sent every 30 s.

    Falls back to DB polling at the keepalive interval when Redis pub/sub
    is unavailable so the FE never needs an interval-polling sidecar.
    """
    workspace = await db_get_workspace(workspace_id)
    require_workspace_owner(workspace, user_id=x_user_id)

    initial_status = workspace["status"]

    async def event_generator():
        started = time.monotonic()
        last_status = initial_status
        last_sandbox_state: str | None = None

        async def reconcile() -> tuple[str | None, bool]:
            """Re-read the authoritative DB row and diff against last_status.

            Returns ``(event_to_yield_or_None, should_close)``. Closes the
            stream when the workspace is gone or has reached a terminal status.
            Single source of the read → diff → emit logic shared by the
            post-subscribe read, the keepalive tick, the pub/sub hint confirm,
            and the Redis-disabled poll.
            """
            nonlocal last_status
            ws = await db_get_workspace(workspace_id)
            if ws is None:
                return None, True
            if ws["status"] == last_status:
                return None, False
            last_status = ws["status"]
            return (
                _sse_status_event(workspace_id, last_status),
                last_status in _EVENTS_TERMINAL,
            )

        yield _sse_status_event(workspace_id, last_status)
        if last_status in _EVENTS_TERMINAL:
            return

        while time.monotonic() - started < _EVENTS_MAX_DURATION_S:
            subscription_broke = False
            async with subscribe_to_status(workspace_id) as wait_for_notify:
                if wait_for_notify is not None:
                    # Always re-read after subscribing — the publish for the
                    # current status could have fired between the initial DB
                    # read above and the SUBSCRIBE.
                    event, close = await reconcile()
                    if event:
                        yield event
                    if close:
                        return

                    while time.monotonic() - started < _EVENTS_MAX_DURATION_S:
                        kind, payload = await wait_for_notify(
                            _EVENTS_KEEPALIVE_S
                        )
                        if kind == "error":
                            # Broken pub/sub connection: abandon it — a broken
                            # wait returns immediately, so looping on it would
                            # busy-spin DB reads. The paced cycle below owns
                            # liveness until the resubscribe.
                            subscription_broke = True
                            break
                        if payload is None:
                            # Keepalive tick or a missed message — re-read to
                            # be safe, then send keepalive.
                            event, close = await reconcile()
                            if event:
                                yield event
                            if close:
                                return
                            yield ": ping\n\n"
                            continue
                        # Forward a sandbox sub-state refinement (e.g.
                        # 'archived') immediately. It's a non-terminal hint
                        # during the 'starting' phase — it can't close the
                        # stream and isn't persisted in the DB, so it's emitted
                        # directly without a DB re-read. Lets the FE escalate to
                        # the slow-restore spinner even when a background warm
                        # (not this client's chat) owns the start.
                        sandbox_state = payload.get("sandbox_state")
                        hinted = payload.get("status")
                        # Pair the refinement with the payload's own status, not
                        # the cached last_status. A publish can carry
                        # {status:'starting', sandbox_state:'archived'} while
                        # reconcile() still has last_status on 'stopped' (the
                        # 'starting' publish raced ahead of its commit).
                        # Emitting 'stopped' here makes the FE drop the archived
                        # hint, and the follow-up plain 'starting' (from
                        # reconcile) carries no refinement — the spinner is lost.
                        event_status = (
                            hinted if isinstance(hinted, str) else last_status
                        )
                        if (
                            sandbox_state
                            and sandbox_state != last_sandbox_state
                            and event_status not in _EVENTS_TERMINAL
                        ):
                            # Guard on event_status (what we emit), not
                            # last_status — this is a non-terminal hint, so it
                            # must never carry a terminal status. reconcile()
                            # owns terminal transitions.
                            last_sandbox_state = sandbox_state
                            yield _sse_status_event(
                                workspace_id,
                                event_status,
                                sandbox_state=sandbox_state,
                            )
                        # Treat the status hint as advisory only. A publish can
                        # race ahead of its transaction commit (or be spurious),
                        # and a terminal status closes the stream — so confirm
                        # against the authoritative DB row (via reconcile)
                        # before trusting it.
                        if not hinted or hinted == last_status:
                            continue
                        event, close = await reconcile()
                        if event:
                            yield event
                        if close:
                            return
                    if not subscription_broke:
                        break  # duration cap reached while subscribed

            # No subscription (Redis disabled / subscribe failed / broke): one
            # keepalive-paced DB cycle, then retry the subscribe — attempts are
            # thereby paced at the keepalive interval, never a tight loop.
            remaining = _EVENTS_MAX_DURATION_S - (time.monotonic() - started)
            if remaining <= 0:
                break
            await asyncio.sleep(min(_EVENTS_KEEPALIVE_S, remaining))
            event, close = await reconcile()
            if event:
                yield event
            if close:
                return
            yield ": ping\n\n"

        yield "event: timeout\ndata: {}\n\n"

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers=_SSE_HEADERS,
    )


@router.get("/{workspace_id}", response_model=WorkspaceResponse)
async def get_workspace(workspace_id: str, x_user_id: CurrentUserId):
    """
    Get workspace details.

    Args:
        workspace_id: Workspace UUID
        x_user_id: Authenticated user ID

    Returns:
        Workspace details
    """
    try:
        workspace = await db_get_workspace(workspace_id)
        require_workspace_owner(workspace, user_id=x_user_id)

        return _workspace_to_response(workspace)

    except HTTPException:
        raise
    except Exception as e:
        logger.exception(f"Error getting workspace {workspace_id}: {e}")
        raise HTTPException(status_code=500, detail="Failed to get workspace")


@router.put("/{workspace_id}", response_model=WorkspaceResponse)
async def update_workspace(
    workspace_id: str,
    request: WorkspaceUpdate,
    x_user_id: CurrentUserId,
):
    """
    Update workspace metadata.

    Args:
        workspace_id: Workspace UUID
        request: Update request with new values
        x_user_id: Authenticated user ID

    Returns:
        Updated workspace details
    """
    try:
        # Check workspace exists and ownership
        workspace = await db_get_workspace(workspace_id)
        require_workspace_owner(workspace, user_id=x_user_id)

        # Update workspace
        updated = await db_update_workspace(
            workspace_id=workspace_id,
            name=request.name,
            description=request.description,
            config=request.config,
            is_pinned=request.is_pinned,
        )

        if not updated:
            raise HTTPException(status_code=404, detail="Workspace not found")

        return _workspace_to_response(updated)

    except HTTPException:
        raise
    except Exception as e:
        logger.exception(f"Error updating workspace {workspace_id}: {e}")
        raise HTTPException(status_code=500, detail="Failed to update workspace")


# Strong references to in-flight warm tasks. asyncio holds only *weak*
# references to tasks, so a fire-and-forget create_task whose handle goes out
# of scope can be garbage-collected mid-flight — which, after the row was
# claimed to 'starting', would wedge the workspace. Hold a strong ref until
# the task finishes (discarded in the done callback below).
_warm_tasks: set[asyncio.Task] = set()


def _log_warm_task_exception(workspace_id: str, task: asyncio.Task) -> None:
    """Surface background warm-task failures (silent create_task is dangerous)."""
    if task.cancelled():
        return
    exc = task.exception()
    if exc is not None:
        logger.exception(
            "Background warm task for workspace %s failed: %s",
            workspace_id,
            exc,
            exc_info=exc,
        )


def _schedule_warm_restart(
    manager: WorkspaceManager, workspace_id: str, user_id: str
) -> None:
    """Start a workspace in the background, tracked for clean shutdown drain.

    ``get_session_for_workspace`` claims the row as 'starting' and brings the
    sandbox up (reconnect / recover / recreate), re-asserting the workspace's
    tier and always-on at start time. Registered in ``_warm_tasks`` so
    ``drain_warm_tasks`` can cancel and await it on shutdown. Deduped against
    the chat path via the manager's per-workspace locks.
    """
    task = asyncio.create_task(
        manager.get_session_for_workspace(workspace_id, user_id=user_id),
        name=f"warm-workspace-{workspace_id}",
    )
    _warm_tasks.add(task)
    task.add_done_callback(_warm_tasks.discard)
    task.add_done_callback(
        lambda t, wid=workspace_id: _log_warm_task_exception(wid, t)
    )


async def drain_warm_tasks() -> None:
    """Cancel and await in-flight warm tasks on shutdown.

    A warm task cancelled mid-Phase-2 triggers the CancelledError revert in
    WorkspaceManager.get_session_for_workspace, which resets the row from
    'starting' back to 'stopped'. Without this drain the event loop tears the
    tasks down abruptly during shutdown and the revert may not land, leaving
    rows wedged in 'starting' until the next process reaps them. Call from the
    app lifespan shutdown BEFORE WorkspaceManager.shutdown().
    """
    if not _warm_tasks:
        return
    tasks = list(_warm_tasks)
    logger.info("Draining %d in-flight warm task(s) on shutdown", len(tasks))
    for task in tasks:
        task.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)


@router.post("/{workspace_id}/start", response_model=WorkspaceActionResponse)
async def start_workspace(
    workspace_id: str,
    x_user_id: CurrentUserId,
    lazy: bool = Query(
        False,
        description=(
            "If true, schedule the restart in the background and return 202 "
            "immediately with status='starting'. If false (default), block "
            "until the session is ready and return status='running'."
        ),
    ),
):
    """
    Start a stopped workspace.

    This restarts the Daytona sandbox, which is much faster than creating
    a new one (~5 seconds vs ~60 seconds).

    With ``lazy=true``, the endpoint returns 202 immediately and continues
    the restart in a background task — used by the proactive warm-on-entry
    UI flow to avoid blocking the request for stopped-hot (~5s) or
    archived (~60-300s) restores.

    Args:
        workspace_id: Workspace UUID
        lazy: When true, return 202 + 'starting' immediately and continue
            the restart in the background. Default false (blocking).

    Returns:
        Action result
    """
    async with _workspace_action_errors("start", workspace_id):
        manager = WorkspaceManager.get_instance()

        # Get workspace to check status
        workspace = await db_get_workspace(workspace_id)
        if not workspace:
            raise HTTPException(status_code=404, detail="Workspace not found")

        require_workspace_owner(workspace, user_id=x_user_id)

        if workspace["status"] == "running":
            return WorkspaceActionResponse(
                workspace_id=workspace_id,
                status="running",
                message="Workspace is already running",
            )

        if workspace["status"] == "starting":
            return WorkspaceActionResponse(
                workspace_id=workspace_id,
                status="starting",
                message="Workspace is already starting",
            )

        if workspace["status"] != "stopped":
            raise HTTPException(
                status_code=400,
                detail=f"Cannot start workspace in '{workspace['status']}' state",
            )

        if lazy:
            # Schedule the restart and return 202 immediately. The chat path's
            # own get_session_for_workspace call will dedupe against this via
            # the existing _observed_lock and _pending_lazy_sync primitives.
            _schedule_warm_restart(manager, workspace_id, x_user_id)
            logger.info(f"Scheduled warm restart for workspace {workspace_id}")
            payload = WorkspaceActionResponse(
                workspace_id=workspace_id,
                status="starting",
                message="Workspace warm initiated",
            )
            return Response(
                status_code=202,
                content=payload.model_dump_json(),
                media_type="application/json",
            )

        # Existing blocking path
        await manager.get_session_for_workspace(workspace_id, user_id=x_user_id)

        logger.info(f"Started workspace {workspace_id}")
        return WorkspaceActionResponse(
            workspace_id=workspace_id,
            status="running",
            message="Workspace started successfully",
        )


@router.post("/{workspace_id}/stop", response_model=WorkspaceActionResponse)
async def stop_workspace(workspace_id: str, x_user_id: CurrentUserId):
    """
    Stop a running workspace.

    This stops the Daytona sandbox but preserves all data. The workspace
    can be quickly restarted later.

    Args:
        workspace_id: Workspace UUID
        x_user_id: Authenticated user ID

    Returns:
        Action result
    """
    async with _workspace_action_errors("stop", workspace_id):
        workspace = await db_get_workspace(workspace_id)
        require_workspace_owner(workspace, user_id=x_user_id)

        manager = WorkspaceManager.get_instance()
        workspace = await manager.stop_workspace(workspace_id)

        logger.info(f"Stopped workspace {workspace_id}")
        return WorkspaceActionResponse(
            workspace_id=workspace_id,
            status="stopped",
            message="Workspace stopped successfully",
        )


@router.post("/{workspace_id}/archive", response_model=WorkspaceActionResponse)
async def archive_workspace(
    workspace_id: str,
    x_user_id: CurrentUserId,
):
    """
    Archive a stopped workspace (moves sandbox to object storage).

    The workspace must be in 'stopped' state. Archived sandboxes take longer
    to start (~60-300s) but use no compute resources.

    Args:
        workspace_id: Workspace UUID

    Returns:
        Action result
    """
    async with _workspace_action_errors("archive", workspace_id):
        workspace = await db_get_workspace(workspace_id)
        require_workspace_owner(workspace, user_id=x_user_id)

        manager = WorkspaceManager.get_instance()
        await manager.archive_workspace(workspace_id)

        logger.info(f"Archived workspace {workspace_id}")
        return WorkspaceActionResponse(
            workspace_id=workspace_id,
            status="stopped",
            message="Workspace archived successfully",
        )


@router.post("/{workspace_id}/spec", response_model=WorkspaceResponse)
async def set_workspace_spec(
    workspace_id: str,
    request: WorkspaceSpecRequest,
    x_user_id: CurrentUserId,
):
    """
    Change a workspace's sandbox spec tier (standard, performance, max).

    Gated by the platform entitlement layer (403 if the tier is not on the
    user's plan, 429 if the per-tier count quota is exhausted); both are
    no-ops in OSS mode. Re-applying the workspace's current tier skips the
    count check. Sizing lives in per-tier snapshots, so a tier change recreates
    the sandbox (files persisted to the DB and restored) rather than resizing.

    Args:
        workspace_id: Workspace UUID
        request: Target spec tier
        x_user_id: Authenticated user ID

    Returns:
        Updated workspace details
    """
    async with _workspace_action_errors("set spec for", workspace_id):
        workspace = await db_get_workspace(workspace_id)
        require_workspace_owner(workspace, user_id=x_user_id)

        current_tier = workspace.get("resource_tier", "standard")
        await assert_spec_allowed(x_user_id, request.tier, current_tier=current_tier)

        manager = WorkspaceManager.get_instance()
        updated = await manager.set_workspace_spec(
            workspace_id, request.tier, user_id=x_user_id
        )

        logger.info(
            f"Set workspace {workspace_id} spec to {request.tier!r} for user {x_user_id}"
        )
        return _workspace_to_response(updated)


@router.post("/{workspace_id}/always-on", response_model=WorkspaceResponse)
async def set_workspace_always_on(
    workspace_id: str,
    request: WorkspaceAlwaysOnRequest,
    x_user_id: CurrentUserId,
):
    """
    Toggle a workspace's always-on flag (disables sandbox auto-stop).

    Enabling is gated by the platform entitlement layer (403 if always-on is
    not on the user's plan, 429 if the count quota is exhausted); both are
    no-ops in OSS mode. Disabling is never gated, and re-enabling a workspace
    that is already always-on consumes no new slot so it skips the gate (an
    idempotent retry at the quota limit must not 429).

    Args:
        workspace_id: Workspace UUID
        request: Whether to keep the sandbox always-on
        x_user_id: Authenticated user ID

    Returns:
        Updated workspace details
    """
    async with _workspace_action_errors("set always-on for", workspace_id):
        workspace = await db_get_workspace(workspace_id)
        require_workspace_owner(workspace, user_id=x_user_id)

        if request.enabled and not workspace.get("is_always_on"):
            await assert_always_on_allowed(x_user_id)

        manager = WorkspaceManager.get_instance()
        updated = await manager.set_workspace_always_on(
            workspace_id, request.enabled, user_id=x_user_id
        )

        # Always-on means the sandbox should be up 24/7, so enabling it on a
        # stopped workspace starts it now rather than deferring to the next
        # turn. The flag is already persisted, so the warm start brings the
        # sandbox up with auto-stop disabled (_restart_workspace / _recover_
        # sandbox re-assert it). The heavy restore (~5-300s) runs in the
        # background; the row goes to 'starting' and the status stream reports
        # 'running' when ready. (Running workspaces already had auto-stop
        # disabled in set_workspace_always_on; no start needed.)
        if request.enabled and (updated or {}).get("status") == "stopped":
            _schedule_warm_restart(manager, workspace_id, x_user_id)
            logger.info(
                f"Always-on enabled: scheduled warm start for {workspace_id}"
            )
            updated = {**updated, "status": "starting"}

        logger.info(
            f"Set workspace {workspace_id} always-on to {request.enabled} "
            f"for user {x_user_id}"
        )
        return _workspace_to_response(updated)


@router.post(
    "/{workspace_id}/duplicate", response_model=WorkspaceResponse, status_code=201
)
async def duplicate_workspace(
    workspace_id: str,
    x_user_id: WorkspaceLimitCheck,
):
    """
    Duplicate a workspace, copying its files into a fresh sandbox.

    Counts against the active-workspace quota via the create-capacity
    dependency. The copy carries the source's resource tier but always
    resets always-on to false. Flash workspaces cannot be duplicated.

    Args:
        workspace_id: Source workspace UUID
        x_user_id: User ID (capacity-checked)

    Returns:
        The newly created workspace
    """
    async with _workspace_action_errors("duplicate", workspace_id):
        workspace = await db_get_workspace(workspace_id)
        require_workspace_owner(workspace, user_id=x_user_id)

        manager = WorkspaceManager.get_instance()
        new_workspace = await manager.duplicate_workspace(workspace_id, x_user_id)

        logger.info(
            f"Duplicated workspace {workspace_id} to "
            f"{new_workspace['workspace_id']} for user {x_user_id}"
        )
        return _workspace_to_response(new_workspace)


@router.post("/{workspace_id}/refresh", response_model=WorkspaceRefreshResponse)
async def refresh_workspace(
    workspace_id: str,
    x_user_id: CurrentUserId,
):
    """Refresh sandbox skills + tool modules.

    Intended for long-lived/reconnected sandboxes where tool module generation
    is skipped during reconnect.
    """

    manager = WorkspaceManager.get_instance()
    workspace = await db_get_workspace(workspace_id)
    require_workspace_owner(workspace, user_id=x_user_id)

    try:
        session = await manager.get_session_for_workspace(
            workspace_id, user_id=x_user_id
        )
    except (SandboxGoneError, SandboxTransientError):
        # Same reasoning as _workspace_action_errors above: re-raise for the
        # app-level handler that owns both the wording and the sanitizing,
        # rather than spelling a fourth variant of this 503 here.
        raise
    except Exception as e:
        # Everything else still answers 503, but never with the raw text: this
        # is the same call _acquire_sandbox makes, and its exceptions quote
        # provider URLs and sandbox ids.
        raise HTTPException(status_code=503, detail=sandbox_unreachable_detail(e))

    sandbox = getattr(session, "sandbox", None)
    if sandbox is None:
        raise HTTPException(status_code=503, detail="Sandbox not available")

    skill_dirs = (
        manager.config.skills.local_skill_dirs_with_sandbox()
        if manager.config.skills.enabled
        else None
    )

    try:
        user_skill_params = await sandbox_skill_sync_params(
            x_user_id,
            manager.config.skills.sandbox_skills_base,
            workspace_id=workspace_id,
        )
        result = await sandbox.sync_sandbox_assets(
            skill_dirs=skill_dirs,
            reusing_sandbox=True,
            force_refresh=True,
            **user_skill_params,
        )
    except Exception as e:
        logger.exception(f"Refresh failed for workspace {workspace_id}: {e}")
        raise HTTPException(status_code=500, detail="Failed to refresh sandbox assets")

    servers: list[str] = []
    try:
        if getattr(session, "mcp_registry", None) is not None:
            servers = list(session.mcp_registry.connectors.keys())
    except Exception:
        servers = []

    return WorkspaceRefreshResponse(
        workspace_id=workspace_id,
        status="ok",
        message="Sandbox refreshed",
        refreshed_tools=bool(
            set(result.refreshed_modules)
            & {"mcp_servers", "data_client", "tool_modules"}
        ),
        skills_uploaded="skills" in result.refreshed_modules,
        servers=servers,
        details={"refreshed_modules": result.refreshed_modules},
    )


@router.delete("/{workspace_id}", status_code=204)
async def delete_workspace(workspace_id: str, x_user_id: CurrentUserId):
    """
    Delete a workspace and its sandbox.

    This permanently deletes the workspace and its associated Daytona
    sandbox. All data will be lost.

    Args:
        workspace_id: Workspace UUID
        x_user_id: Authenticated user ID
    """
    try:
        # Guard: prevent deletion of flash workspaces
        workspace = await db_get_workspace(workspace_id)
        if workspace and workspace.get("status") == "flash":
            raise HTTPException(
                status_code=400,
                detail="Cannot delete flash workspace",
            )
        require_workspace_owner(workspace, user_id=x_user_id)

        manager = WorkspaceManager.get_instance()
        await manager.delete_workspace(workspace_id)

        # Invalidate existence cache
        from src.server.database.conversation import ws_exists_key
        from src.utils.cache.redis_cache import get_cache_client

        cache = get_cache_client()
        if cache.enabled and cache.client:
            try:
                await cache.client.delete(ws_exists_key(workspace_id))
            except Exception:
                pass

        logger.info(f"Deleted workspace {workspace_id}")
        # Return 204 No Content

    except HTTPException:
        raise
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        logger.exception(f"Error deleting workspace {workspace_id}: {e}")
        raise HTTPException(status_code=500, detail="Failed to delete workspace")
