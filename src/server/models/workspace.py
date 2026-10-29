"""
Request and response models for Workspace management API.

A workspace is a project the agent works in; the isolated environment it runs
in is a computer (``models/computer.py``), named here by ``computer_id``. The
sandbox-lifecycle fields on ``WorkspaceResponse`` read the write-through shadow
of that computer's row.
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
    # A user's Home before its first computer.
    UNBOUND = "unbound"


#: While the sandbox is not serving, file routes answer from the mirror rows.
MIRROR_SERVED_STATUSES = frozenset(
    {WorkspaceStatus.STOPPED, WorkspaceStatus.STOPPING, WorkspaceStatus.STARTING}
)


def served_from_mirror(status: str | None) -> bool:
    return status in MIRROR_SERVED_STATUSES


class WorkspaceCreate(BaseModel):
    """Request model for creating a new workspace."""

    name: str = Field(
        ...,
        min_length=1,
        max_length=80,
        description="Workspace name; unique per user, and the name of its folder",
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
    computer_id: Optional[uuid.UUID] = Field(
        None,
        description=(
            "One of the caller's computers to create the workspace on; their "
            "primary computer when absent"
        ),
    )


class WorkspaceUpdate(BaseModel):
    """Request model for updating workspace metadata."""

    name: Optional[str] = Field(
        None,
        min_length=1,
        max_length=80,
        description="New workspace name; unique per user, and the name of its folder",
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

    tier: Literal["standard", "performance", "max", "xlarge"] = Field(
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
    xlarge: Optional[WorkspaceCapacity] = None
    always_on: Optional[WorkspaceCapacity] = None
    computers: Optional[WorkspaceCapacity] = Field(
        None,
        description=(
            "Computers the user holds against the plan's cap, stopped ones "
            "included; a machine its owner brings is never counted"
        ),
    )


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
    computer_id: Optional[str] = Field(
        None,
        description=(
            "The computer this workspace runs on (null for a Home before its "
            "first computer, or one not bound to a computer)"
        ),
    )
    dir_name: Optional[str] = Field(
        None,
        description=(
            "The folder this workspace owns on its computer; null for a "
            "workspace with no computer"
        ),
    )
    previous_dir_names: list[str] = Field(
        default_factory=list,
        description=(
            "Folders this workspace was renamed out of, newest first; a path "
            "under one of them still means this workspace's folder"
        ),
    )
    root_dir: Optional[str] = Field(
        None,
        description=(
            "The root of the computer this workspace is on, where its folder "
            "sits and where the agent's absolute paths start; given on the "
            "detail and the list, null on the answer to a change"
        ),
    )
    previous_root_dirs: list[str] = Field(
        default_factory=list,
        description=(
            "Roots of the computers this workspace moved off, newest first; "
            "a path under one of them through its folder still means this "
            "workspace's folder"
        ),
    )
    status: str = Field(
        description=(
            "Workspace status: creating, starting, running, stopping, stopped, "
            "error, deleted; 'unbound' for the user's Home before its first computer"
        )
    )
    is_home: bool = Field(
        description=(
            "Whether this is the user's Home, where work in no particular "
            "workspace runs; tell Home by this, not by status"
        ),
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
        description=(
            "Sandbox spec preset: standard (Small), performance (Medium), "
            "max (Large), xlarge (Extra Large)"
        ),
    )
    is_always_on: bool = Field(
        False,
        description="Whether auto-stop is disabled (always-on sandbox)",
    )
    files_restore_incomplete: bool = Field(
        False,
        description=(
            "Whether a restore left files unrecovered, so a file the workspace "
            "records may be absent from its folder"
        ),
    )
    files_restore_offered_at: Optional[datetime] = Field(
        None,
        description=(
            "When the folder was found gone from its owner's machine while a "
            "backup holds it. Set, the restore waits on the owner's choice "
            "(POST files/restore-choice) and no backup of it runs"
        ),
    )
    files_backed_up_at: Optional[datetime] = Field(
        None, description="When the last complete backup of the folder ran"
    )

    model_config = ConfigDict(from_attributes=True)


class FilesRestoreChoice(BaseModel):
    restore: bool = Field(
        description=(
            "True lays the backup back into the folder; False keeps the folder "
            "as it is, and the backup then follows it"
        )
    )


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
