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
from typing import Optional

from fastapi import APIRouter, Header, HTTPException, Query

from src.server.database.workspace_db import (
    get_workspace as db_get_workspace,
    get_workspaces_for_user,
    update_workspace as db_update_workspace,
)
from src.server.models.workspace import (
    WorkspaceActionResponse,
    WorkspaceCreate,
    WorkspaceListResponse,
    WorkspaceResponse,
    WorkspaceUpdate,
)
from src.server.models.workspace_refresh import WorkspaceRefreshResponse
from src.server.services.workspace_manager import WorkspaceManager

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1/workspaces", tags=["Workspaces"])


def _require_workspace_owner(workspace: dict | None, *, user_id: str, workspace_id: str) -> None:
    if not workspace:
        raise HTTPException(status_code=404, detail="Workspace not found")
    if workspace.get("user_id") != user_id:
        raise HTTPException(status_code=403, detail="Forbidden")


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
    )


@router.post("", response_model=WorkspaceResponse, status_code=201)
async def create_workspace(
    request: WorkspaceCreate,
    x_user_id: str = Header(..., alias="X-User-Id", description="User ID"),
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

        logger.info(f"Created workspace {workspace['workspace_id']} for user {x_user_id}")
        return _workspace_to_response(workspace)

    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.exception(f"Error creating workspace: {e}")
        raise HTTPException(status_code=500, detail="Failed to create workspace")


@router.get("", response_model=WorkspaceListResponse)
async def list_workspaces(
    x_user_id: str = Header(..., alias="X-User-Id", description="User ID"),
    limit: int = Query(20, ge=1, le=100, description="Maximum results"),
    offset: int = Query(0, ge=0, description="Number to skip"),
):
    """
    List workspaces for a user.

    Args:
        x_user_id: User ID from header
        limit: Maximum number of results (1-100)
        offset: Number of results to skip

    Returns:
        Paginated list of workspaces
    """
    try:
        workspaces, total = await get_workspaces_for_user(
            user_id=x_user_id,
            limit=limit,
            offset=offset,
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
async def get_workspace(workspace_id: str):
    """
    Get workspace details.

    Args:
        workspace_id: Workspace UUID

    Returns:
        Workspace details
    """
    try:
        workspace = await db_get_workspace(workspace_id)
        if not workspace:
            raise HTTPException(status_code=404, detail="Workspace not found")

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
):
    """
    Update workspace metadata.

    Args:
        workspace_id: Workspace UUID
        request: Update request with new values

    Returns:
        Updated workspace details
    """
    try:
        # Check workspace exists
        workspace = await db_get_workspace(workspace_id)
        if not workspace:
            raise HTTPException(status_code=404, detail="Workspace not found")

        # Update workspace
        updated = await db_update_workspace(
            workspace_id=workspace_id,
            name=request.name,
            description=request.description,
            config=request.config,
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
    x_user_id: str = Header(..., alias="X-User-Id", description="User ID"),
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

        _require_workspace_owner(workspace, user_id=x_user_id, workspace_id=workspace_id)

        if workspace["status"] == "running":
            return WorkspaceActionResponse(
                workspace_id=workspace_id,
                status="running",
                message="Workspace is already running",
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
async def stop_workspace(workspace_id: str):
    """
    Stop a running workspace.

    This stops the Daytona sandbox but preserves all data. The workspace
    can be quickly restarted later.

    Args:
        workspace_id: Workspace UUID

    Returns:
        Action result
    """
    try:
        manager = WorkspaceManager.get_instance()
        workspace = await manager.stop_workspace(workspace_id)

        logger.info(f"Stopped workspace {workspace_id}")
        return WorkspaceActionResponse(
            workspace_id=workspace_id,
            status="stopped",
            message="Workspace stopped successfully",
        )

    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except RuntimeError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.exception(f"Error stopping workspace {workspace_id}: {e}")
        raise HTTPException(status_code=500, detail="Failed to stop workspace")


@router.post("/{workspace_id}/refresh", response_model=WorkspaceRefreshResponse)
async def refresh_workspace(
    workspace_id: str,
    x_user_id: str = Header(..., alias="X-User-Id", description="User ID"),
):
    """Refresh sandbox skills + tool modules.

    Intended for long-lived/reconnected sandboxes where tool module generation
    is skipped during reconnect.
    """

    manager = WorkspaceManager.get_instance()
    workspace = await db_get_workspace(workspace_id)
    _require_workspace_owner(workspace, user_id=x_user_id, workspace_id=workspace_id)

    try:
        session = await manager.get_session_for_workspace(workspace_id, user_id=x_user_id)
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"Sandbox not available: {e}")

    sandbox = getattr(session, "sandbox", None)
    if sandbox is None:
        raise HTTPException(status_code=503, detail="Sandbox not available")

    refreshed_tools = False
    skills_uploaded = False

    try:
        result = await sandbox.refresh_tools()
        refreshed_tools = bool(result.get("success", True))
    except Exception as e:
        logger.exception(f"Refresh tools failed for workspace {workspace_id}: {e}")
        raise HTTPException(status_code=500, detail="Failed to refresh tools")

    # Sync skills by default (manifest-based; usually no-op)
    try:
        if manager.config.skills.enabled:
            skill_dirs = manager.config.skills.local_skill_dirs_with_sandbox()
            if skill_dirs:
                skills_uploaded = await sandbox.sync_skills(
                    skill_dirs,
                    reusing_sandbox=True,
                )
    except Exception as e:
        logger.warning(f"Skills sync failed during refresh: {e}")

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
        refreshed_tools=refreshed_tools,
        skills_uploaded=skills_uploaded,
        servers=servers,
        details={"tools": result},
    )


@router.delete("/{workspace_id}", status_code=204)
async def delete_workspace(workspace_id: str):
    """
    Delete a workspace and its sandbox.

    This permanently deletes the workspace and its associated Daytona
    sandbox. All data will be lost.

    Args:
        workspace_id: Workspace UUID
    """
    try:
        manager = WorkspaceManager.get_instance()
        await manager.delete_workspace(workspace_id)

        logger.info(f"Deleted workspace {workspace_id}")
        # Return 204 No Content

    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        logger.exception(f"Error deleting workspace {workspace_id}: {e}")
        raise HTTPException(status_code=500, detail="Failed to delete workspace")
