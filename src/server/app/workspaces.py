"""
Workspace Management API Router.

Provides CRUD endpoints for managing workspaces. A workspace is a project; the
machine it runs on is a ``computers`` row, so the lifecycle routes here are
aliases that run the same transition through the project-addressed manager.

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
import logging
from typing import Literal

from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import Response, StreamingResponse

from src.server.utils.api import CurrentUserId, require_workspace_owner
from src.server.dependencies.usage_limits import (
    ALWAYS_ON_QUOTA,
    SPEC_QUOTAS,
    assert_always_on_allowed,
    assert_spec_allowed,
    get_capacity_status,
)
from src.server.app.background_starts import schedule_start
from src.server.app.status_stream import (
    SSE_HEADERS,
    sse_status_event,
    status_event_stream,
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


# ---------------------------------------------------------------------------
# Lifecycle aliases: workspace in, computer out
# ---------------------------------------------------------------------------
#
# The lifecycle routes below are aliases for the ones under
# ``/api/v1/computers``. They stay for a full release because the only caller of
# stop/start/archive is an interpolated template in the sandbox settings panel,
# invisible to a grep, so removing them would break the UI silently. Each alias
# calls the project-addressed manager method, which resolves the machine and
# runs the same transition the computer route runs, so there is one
# implementation while both paths are live rather than two that can drift.


def _workspace_to_response(workspace: dict) -> WorkspaceResponse:
    """Convert workspace dict to response model."""
    computer_id = workspace.get("computer_id")
    return WorkspaceResponse(
        workspace_id=str(workspace["workspace_id"]),
        user_id=workspace["user_id"],
        name=workspace["name"],
        description=workspace.get("description"),
        sandbox_id=workspace.get("sandbox_id"),
        computer_id=str(computer_id) if computer_id else None,
        dir_name=workspace.get("dir_name"),
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
        files_restore_incomplete=bool(workspace.get("files_restore_incomplete")),
    )


@router.post("", response_model=WorkspaceResponse, status_code=201)
async def create_workspace(
    request: WorkspaceCreate,
    x_user_id: CurrentUserId,
):
    """
    Create a workspace on the caller's computer.

    Returns as soon as the row is written: the workspace is bound to the
    user's computer and owns a folder on it, and the machine itself is brought
    up by the first turn (or an explicit start), so nothing here waits on a
    sandbox. A user with no computer yet gets one.

    Not capacity-checked: the plan meters computers, and this route allocates
    none.

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


@router.get("/{workspace_id}/events")
async def workspace_status_events(workspace_id: str, x_user_id: CurrentUserId):
    """Push workspace lifecycle status changes to the client via SSE.

    Replaces the previous 4-second interval polling on the frontend. The
    handler emits the current status immediately, then one ``status``
    event per transition (stopped → starting → running). Closes once the
    status is terminal (``running``, ``error``, ``deleted``), or after a
    600 s hard cap. A ``: ping\\n\\n`` keepalive is sent every 30 s.

    Subscribes on the machine's channel, because that is where every writer
    publishes; a workspace with no machine keeps its own channel. Each frame
    names both ids, so a client holding only a workspace id learns the computer
    it runs on from the stream itself.
    """
    workspace = await db_get_workspace(workspace_id)
    require_workspace_owner(workspace, user_id=x_user_id)

    computer_id = (
        str(workspace["computer_id"]) if workspace.get("computer_id") else None
    )

    def _frame(
        status: str, sandbox_state: str | None = None, error: str | None = None
    ) -> str:
        return sse_status_event(
            {"workspace_id": workspace_id, "computer_id": computer_id},
            status,
            sandbox_state,
            error,
        )

    async def _read_status() -> str | None:
        nonlocal computer_id
        ws = await db_get_workspace(workspace_id)
        if ws is None:
            return None
        # A workspace can be bound to a machine after the stream opened, so the
        # next resubscribe picks up the channel the writers moved to.
        computer_id = str(ws["computer_id"]) if ws.get("computer_id") else None
        return ws["status"]

    return StreamingResponse(
        status_event_stream(
            initial_status=workspace["status"],
            read_status=_read_status,
            subscribe=lambda: subscribe_to_status(
                workspace_id, computer_id=computer_id
            ),
            frame=_frame,
        ),
        media_type="text/event-stream",
        headers=SSE_HEADERS,
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


def _schedule_warm_restart(
    manager: WorkspaceManager, workspace_id: str, user_id: str
) -> None:
    """Start a workspace in the background, keyed by the project.

    ``get_session_for_workspace`` claims the row as 'starting' and brings the
    machine up, re-asserting the workspace's tier and always-on at start time.
    The key is the project, not the machine: starting the machine alone would
    attach whichever live sibling it finds and leave this project's folder
    uncreated. The chat path dedupes against this via the manager's
    per-workspace locks.
    """
    schedule_start(
        f"workspace:{workspace_id}",
        lambda: manager.get_session_for_workspace(workspace_id, user_id=user_id),
    )


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
    Start a workspace and materialise it on its machine.

    This restarts the sandbox, which is much faster than creating a new one
    (~5 seconds vs ~60 seconds). A workspace whose machine is already running
    still goes through an acquisition rather than answering from its row: a
    project created or duplicated while the machine was up is born running
    with nothing of its own on disk.

    With ``lazy=true``, the endpoint returns 202 immediately and continues
    in a background task - used by the proactive warm-on-entry UI flow to
    avoid blocking the request for stopped-hot (~5s) or archived
    (~60-300s) restores.

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

        def _warm_in_background() -> Response:
            """202 now, the attach in a background task.

            Workspace-keyed, not machine-keyed: starting the machine alone
            attaches whichever live sibling it finds, and this project would
            still have no folder when the caller came back."""
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

        if workspace["status"] == "running":
            # A project created or duplicated while its machine was running is
            # born running with no folder of its own: the folder, its files and
            # its tool overlay are materialised by the first attach. Answering
            # here without one sends the caller to a path nothing has created.
            if lazy:
                return _warm_in_background()
            await manager.get_session_for_workspace(workspace_id, user_id=x_user_id)
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
            # The chat path's own get_session_for_workspace call dedupes against
            # this via the existing _observed_lock and the machine record's
            # pending_lazy_sync flag.
            return _warm_in_background()

        # Blocking path. An acquisition for this workspace brings the same
        # machine up as the computer route would and materialises this project
        # on it.
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

        await WorkspaceManager.get_instance().stop_workspace(workspace_id)

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

        await WorkspaceManager.get_instance().archive_workspace(workspace_id)

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

        updated = await WorkspaceManager.get_instance().set_workspace_spec(
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

        updated = await WorkspaceManager.get_instance().set_workspace_always_on(
            workspace_id, request.enabled
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
            # Workspace-keyed: starting the machine alone attaches whichever
            # sibling it finds, which would leave this project's folder
            # uncreated on a machine its own row calls running.
            _schedule_warm_restart(
                WorkspaceManager.get_instance(), workspace_id, x_user_id
            )
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
    x_user_id: CurrentUserId,
):
    """
    Duplicate a workspace, copying its files into a new project.

    The copy lands on the same computer as every other project of this user,
    so it takes that machine's tier and always-on rather than the source's,
    allocates nothing, and returns without waiting on a sandbox. Its files are
    restored the first time the machine starts. Flash workspaces cannot be
    duplicated.

    Args:
        workspace_id: Source workspace UUID
        x_user_id: User ID from header

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

    # The manager owns the sync, including the folder and the v3 root owner a
    # layout migration needs, and stamps what the machine observed. A lazy
    # acquisition can leave this route as the machine's first sync, so it has
    # to be the same call the acquisition path makes.
    try:
        result = await manager.refresh_project_assets(
            workspace_id, x_user_id, sandbox
        )
    except Exception as e:
        logger.exception(f"Refresh failed for workspace {workspace_id}: {e}")
        raise HTTPException(status_code=500, detail="Failed to refresh sandbox assets")
    if result is None:
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
    Delete a workspace and its project data.

    This permanently deletes the workspace and its folder. The computer and
    sibling workspaces remain available.

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
