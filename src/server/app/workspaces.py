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

import logging
from typing import Literal

from fastapi import APIRouter, HTTPException, Query

from src.server.utils.api import CurrentUserId, require_workspace_owner
from src.server.dependencies.usage_limits import WorkspaceLimitCheck
from src.server.database.workspace import (
    get_workspace as db_get_workspace,
    get_workspaces_for_user,
    update_workspace as db_update_workspace,
    get_or_create_flash_workspace,
    batch_update_sort_order,
)
from src.server.models.workspace import (
    WorkspaceActionResponse,
    WorkspaceCreate,
    WorkspaceListResponse,
    WorkspaceReorderRequest,
    WorkspaceResponse,
    WorkspaceUpdate,
)
from src.server.models.workspace_refresh import WorkspaceRefreshResponse
from src.server.services.workspace_manager import WorkspaceManager

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1/workspaces", tags=["Workspaces"])


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


@router.post("/{workspace_id}/start", response_model=WorkspaceActionResponse)
async def start_workspace(
    workspace_id: str,
    x_user_id: CurrentUserId,
):
    """
    Start a stopped workspace.

    This restarts the Daytona sandbox, which is much faster than creating
    a new one (~5 seconds vs ~60 seconds).

    Args:
        workspace_id: Workspace UUID

    Returns:
        Action result
    """
    try:
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

        # Start by getting session (triggers restart)
        await manager.get_session_for_workspace(workspace_id, user_id=x_user_id)

        logger.info(f"Started workspace {workspace_id}")
        return WorkspaceActionResponse(
            workspace_id=workspace_id,
            status="running",
            message="Workspace started successfully",
        )

    except HTTPException:
        raise
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except RuntimeError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.exception(f"Error starting workspace {workspace_id}: {e}")
        raise HTTPException(status_code=500, detail="Failed to start workspace")


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
    try:
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

    except HTTPException:
        raise
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except RuntimeError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.exception(f"Error stopping workspace {workspace_id}: {e}")
        raise HTTPException(status_code=500, detail="Failed to stop workspace")


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
    try:
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

    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except RuntimeError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.exception(f"Error archiving workspace {workspace_id}: {e}")
        raise HTTPException(status_code=500, detail="Failed to archive workspace")


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
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"Sandbox not available: {e}")

    sandbox = getattr(session, "sandbox", None)
    if sandbox is None:
        raise HTTPException(status_code=503, detail="Sandbox not available")

    skill_dirs = (
        manager.config.skills.local_skill_dirs_with_sandbox()
        if manager.config.skills.enabled
        else None
    )

    try:
        result = await sandbox.sync_sandbox_assets(
            skill_dirs=skill_dirs,
            reusing_sandbox=True,
            force_refresh=True,
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
