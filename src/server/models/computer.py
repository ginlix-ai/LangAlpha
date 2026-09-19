"""Request and response models for the Computer management API.

A computer is one isolated execution environment addressed on its own, so the
lifecycle fields a workspace used to carry (status, resource tier, always-on)
are read and written here. ``provider_ref`` is the vendor's id for the machine
and is only ever handed to the owner.
"""

from datetime import datetime
from enum import StrEnum
from typing import Any, Dict, List, Literal, Optional

from pydantic import BaseModel, ConfigDict, Field


class ComputerStatus(StrEnum):
    """Computer lifecycle states: the workspace set minus ``flash``.

    StrEnum, not ``(str, Enum)``: a member of the latter renders as
    ``ComputerStatus.RUNNING`` in an f-string while comparing equal to
    ``"running"``, so every log line and message built from one had to route
    around it. Here a member is its value everywhere a string is wanted.
    """

    CREATING = "creating"
    STARTING = "starting"
    RUNNING = "running"
    STOPPING = "stopping"
    STOPPED = "stopped"
    ERROR = "error"
    DELETED = "deleted"


# A machine is claimable for start only from a settled down state. ``creating``
# is one: 046 copies a workspace's status onto the machine it backfills, so a
# create that bound a sandbox and then crashed leaves a startable machine
# sitting at ``creating``.
CLAIMABLE_FOR_START = (ComputerStatus.STOPPED, ComputerStatus.CREATING)


class ComputerCreate(BaseModel):
    """Request model for creating a computer."""

    name: Optional[str] = Field(
        None,
        min_length=1,
        max_length=255,
        description="User-facing computer name; a default is used when absent",
    )
    resource_tier: Literal["standard", "performance", "max"] = Field(
        "standard",
        description="Spec preset the computer is created at",
    )


class ComputerSpecRequest(BaseModel):
    """Request model for changing a computer's spec tier."""

    tier: Literal["standard", "performance", "max"] = Field(
        description="Target spec preset",
    )


class ComputerAlwaysOnRequest(BaseModel):
    """Request model for toggling a computer's always-on flag."""

    enabled: bool = Field(description="Whether to keep the computer always-on")


class ComputerResponse(BaseModel):
    """Response model for computer details."""

    computer_id: str = Field(description="Unique computer identifier")
    user_id: str = Field(description="Owner user ID")
    kind: str = Field(description="Execution backend: daytona, docker")
    name: str = Field(description="User-facing computer name")
    status: str = Field(
        description=(
            "Computer status: creating, starting, running, stopping, stopped, "
            "error, deleted"
        )
    )
    resource_tier: str = Field(
        "standard",
        description="Spec preset: standard, performance, max",
    )
    is_always_on: bool = Field(
        False,
        description="Whether auto-stop is disabled (always-on computer)",
    )
    is_primary: bool = Field(
        False,
        description="Whether this is the user's default computer",
    )
    root_dir: str = Field(description="Filesystem root of the computer's home")
    workspace_count: int = Field(
        0,
        description="Live projects on this computer, which its stop takes down together",
    )
    layout_version: Optional[int] = Field(
        None,
        description=(
            "Filesystem layout the machine last reported. Null until one has "
            "been observed, which is not the same as version zero."
        ),
    )
    provider_ref: Optional[str] = Field(
        None,
        description=(
            "Vendor identifier for the running machine. Present only for the "
            "owner; null when the computer has never been provisioned."
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
        description="When the computer was stopped (if status=stopped)",
    )
    config: Optional[Dict[str, Any]] = Field(
        None,
        description="Configuration settings",
    )

    model_config = ConfigDict(from_attributes=True)


class ComputerListResponse(BaseModel):
    """Response model for a user's computer list."""

    computers: List[ComputerResponse] = Field(
        default_factory=list,
        description="The user's computers, primary first",
    )
    total: int = Field(0, description="Total number of computers")


class ComputerActionResponse(BaseModel):
    """Response model for computer actions (start, stop, archive)."""

    computer_id: str = Field(description="Computer identifier")
    status: str = Field(description="New computer status")
    message: str = Field(description="Action result message")


class ComputerSessionResponse(BaseModel):
    """What a computer is, plus what the answering worker holds for it.

    ``status`` and ``provider_ref`` are the cross-worker truth from Postgres.
    ``ready`` and ``session_provider_ref`` describe only the worker that served
    this request, because a runtime session is process-local execution context;
    a client must never read them as liveness.
    """

    computer_id: str = Field(description="Computer identifier")
    status: str = Field(description="Computer status, from Postgres")
    provider_ref: Optional[str] = Field(
        None, description="Vendor identifier for the machine, from Postgres"
    )
    ready: bool = Field(
        False,
        description="This worker holds a session with a runtime attached",
    )
    session_provider_ref: Optional[str] = Field(
        None,
        description="What this worker's session is bound to, if it holds one",
    )
