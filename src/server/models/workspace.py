"""
Request and response models for Workspace management API.

Workspaces provide isolated environments for PTC agents, with each workspace
having a dedicated Daytona sandbox (1:1 mapping).
"""

import uuid
from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Literal, Optional

from pydantic import BaseModel, ConfigDict, Field


class WorkspaceStatus(str, Enum):
    """Workspace lifecycle states."""

    CREATING = "creating"
    STARTING = "starting"
    RUNNING = "running"
    STOPPING = "stopping"
    STOPPED = "stopped"
    ERROR = "error"
    DELETED = "deleted"
    FLASH = "flash"


class WorkspaceCreate(BaseModel):
    """Request model for creating a new workspace."""

    name: str = Field(
        ...,
        min_length=1,
        max_length=255,
        description="Workspace name",
    )
    description: Optional[str] = Field(
        None,
        max_length=1000,
        description="Optional workspace description",
    )
    config: Optional[Dict[str, Any]] = Field(
        None,
        description="Optional configuration settings",
    )


class WorkspaceUpdate(BaseModel):
    """Request model for updating workspace metadata."""

    name: Optional[str] = Field(
        None,
        min_length=1,
        max_length=255,
        description="New workspace name",
    )
    description: Optional[str] = Field(
        None,
        max_length=1000,
        description="New workspace description",
    )
    config: Optional[Dict[str, Any]] = Field(
        None,
        description="New configuration settings (replaces existing)",
    )
    is_pinned: Optional[bool] = Field(
        None,
        description="Pin workspace to top of gallery",
    )


class WorkspaceSpecRequest(BaseModel):
    """Request model for changing a workspace's sandbox spec tier."""

    tier: Literal["standard", "performance", "max"] = Field(
        description="Target sandbox spec preset",
    )


class WorkspaceAlwaysOnRequest(BaseModel):
    """Request model for toggling a workspace's always-on flag."""

    enabled: bool = Field(description="Whether to keep the sandbox always-on")


class WorkspaceCapacity(BaseModel):
    """Count-quota status for one elevated capability (platform mode only)."""

    used: int = Field(description="How many of this capability the user occupies")
    limit: int = Field(description="Plan ceiling for this capability; -1 = unlimited")


class WorkspaceQuotaResponse(BaseModel):
    """Per-capability count quotas for the change-spec / always-on UI.

    Each field is null when the quota does not apply — OSS mode, the platform is
    unreachable, or it reports no count for that capability.
    """

    performance: Optional[WorkspaceCapacity] = None
    max: Optional[WorkspaceCapacity] = None
    always_on: Optional[WorkspaceCapacity] = None


class WorkspaceResponse(BaseModel):
    """Response model for workspace details."""

    workspace_id: str = Field(description="Unique workspace identifier")
    user_id: str = Field(description="Owner user ID")
    name: str = Field(description="Workspace name")
    description: Optional[str] = Field(None, description="Workspace description")
    sandbox_id: Optional[str] = Field(
        None,
        description="Daytona sandbox ID (null if not yet created)",
    )
    status: str = Field(
        description=(
            "Workspace status: creating, starting, running, stopping, stopped, "
            "error, deleted, flash"
        )
    )
    created_at: datetime = Field(description="Creation timestamp")
    updated_at: datetime = Field(description="Last update timestamp")
    last_activity_at: Optional[datetime] = Field(
        None,
        description="Last agent activity timestamp",
    )
    stopped_at: Optional[datetime] = Field(
        None,
        description="When workspace was stopped (if status=stopped)",
    )
    config: Optional[Dict[str, Any]] = Field(
        None,
        description="Configuration settings",
    )
    is_pinned: bool = Field(False, description="Whether workspace is pinned to top")
    sort_order: int = Field(0, description="Manual sort order within pin group")
    resource_tier: str = Field(
        "standard",
        description="Sandbox spec preset: standard, performance, max",
    )
    is_always_on: bool = Field(
        False,
        description="Whether auto-stop is disabled (always-on sandbox)",
    )

    model_config = ConfigDict(from_attributes=True)


class WorkspaceReorderItem(BaseModel):
    """Single item in a reorder request."""

    workspace_id: uuid.UUID = Field(description="Workspace identifier")
    sort_order: int = Field(ge=0, description="New sort order value")


class WorkspaceReorderRequest(BaseModel):
    """Request model for batch reordering workspaces."""

    items: List[WorkspaceReorderItem] = Field(
        ...,
        min_length=1,
        max_length=500,
        description="List of workspace ID + sort_order pairs",
    )


class WorkspaceListResponse(BaseModel):
    """Response model for paginated workspace list."""

    workspaces: List[WorkspaceResponse] = Field(
        default_factory=list,
        description="List of workspaces",
    )
    total: int = Field(0, description="Total number of workspaces")
    limit: int = Field(description="Page size")
    offset: int = Field(description="Number of items skipped")


class WorkspaceActionResponse(BaseModel):
    """Response model for workspace actions (start, stop)."""

    workspace_id: str = Field(description="Workspace identifier")
    status: str = Field(description="New workspace status")
    message: str = Field(description="Action result message")
