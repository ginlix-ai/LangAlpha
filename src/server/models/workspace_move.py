"""Request and response models for moving a workspace to another computer.

A move is accepted with a 202 and runs after the response, so its outcome is
read back from ``GET /workspaces/{id}/move``. A refusal before acceptance and
a failure after carry the same ``{code, message, files}`` shape, as a spec
change's do.
"""

from datetime import datetime
from typing import List, Literal, Optional

from pydantic import BaseModel, Field

from src.server.models.computer import UnsavedFileOut

MovePhase = Literal[
    "preparing", "backing_up", "staged", "committed", "done", "failed", "cancelled"
]

MoveRefusalCode = Literal[
    "not_found",
    "not_movable",
    "same_computer",
    "target_unavailable",
    "not_primary",
    "turn_active",
    "command_running",
    "move_in_progress",
    "not_cancellable",
    "no_backup",
]

MoveErrorCode = Literal[
    "source_unavailable",
    "backup_incomplete",
    "busy",
    "no_room",
    "target_unavailable",
    "not_found",
    "not_movable",
    "interrupted",
    "unknown",
]


class WorkspaceMoveRequest(BaseModel):
    computer_id: str = Field(description="The computer to move the workspace to")
    from_backup: bool = Field(
        False,
        description=(
            "Lay the workspace on the target from its last backup without "
            "reaching the computer it is on, for a machine its owner brought "
            "that is gone. The folder there is left as it is"
        ),
    )


class WorkspaceMoveError(BaseModel):
    """Why a move refused or failed, in a shape a client can map to copy."""

    code: MoveErrorCode | MoveRefusalCode
    message: str = Field(description="A sentence the user can be shown as is")
    files: List[UnsavedFileOut] = Field(
        default_factory=list,
        description="For backup_incomplete: the files the backup could not save",
    )


class WorkspaceMoveOut(BaseModel):
    move_id: str
    workspace_id: str
    source_computer_id: str
    target_computer_id: str
    make_primary: bool
    from_backup: bool = False
    phase: MovePhase
    error: Optional[WorkspaceMoveError] = None
    created_at: datetime
    committed_at: Optional[datetime] = None
    finished_at: Optional[datetime] = None


class MakePrimaryResponse(BaseModel):
    """Either the flip landed now, or Home has to move first and ``move`` runs it."""

    computer_id: str
    is_primary: bool
    move: Optional[WorkspaceMoveOut] = None
