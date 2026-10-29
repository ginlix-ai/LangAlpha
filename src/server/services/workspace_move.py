"""The one mapping from a workspace move's refusal or failure to what the client is told.

Apart from the computer manager, as a spec change's mapping is, so a router
reading a move on every poll needs none of the runner.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any, Dict, Mapping, Optional

from ptc_agent.core.sandbox.providers import may_be_primary, provider_class
from src.server.database import workspace_move as db
from src.server.database.workspace_move import MoveRefused
from src.server.models.workspace_move import WorkspaceMoveError, WorkspaceMoveOut
from src.server.services.computer_errors import ComputerBusyError
from src.server.services.persistence.sync_result import BackupIncomplete
from src.server.services.spec_change import unsaved_files_out

logger = logging.getLogger(__name__)

MOVE_MESSAGES: Dict[str, str] = {
    "not_found": "Workspace not found.",
    "not_movable": "This workspace can't be moved.",
    "same_computer": "This workspace is already on that computer.",
    "target_unavailable": "That computer can't take workspaces right now.",
    "not_primary": (
        "A computer you added yourself can't be primary, since Home lives on "
        "the primary."
    ),
    "turn_active": (
        "This workspace has work running. Wait for it to finish or stop it, "
        "then move it."
    ),
    "command_running": (
        "A command is still running on this computer. Wait for it to end or "
        "stop it in its chat, then move the workspace."
    ),
    "move_in_progress": (
        "A workspace is already moving. Wait for that move to finish, then try again."
    ),
    "not_cancellable": "This move is too far along to cancel.",
    "no_backup": "This workspace has no backup to restore from.",
    "source_unavailable": (
        "The computer this workspace is on couldn't start, so its files "
        "couldn't be copied. Try again once it is running."
    ),
    "busy": "The computer is busy. Try the move again in a moment.",
    "no_room": (
        "The computer this workspace is on doesn't have enough free disk to "
        "move it. Free some space there, then try again."
    ),
    "interrupted": (
        "The move was interrupted before it finished. The workspace stays "
        "where it was. Try again."
    ),
    "unknown": "The move failed. The workspace stays on its current computer.",
}


class MoveSourceUnavailable(RuntimeError):
    """The computer the workspace is on could not be brought up to copy it from."""


class MoveNoRoom(RuntimeError):
    """A machine its owner brought could not rename the folder aside and has
    no room to copy it, and the folder is never cut down to its backup there."""


def move_error_detail(code: str) -> Dict[str, Any]:
    """The ``{code, message, files}`` a refusal answers with and a failure records."""
    return WorkspaceMoveError(code=code, message=MOVE_MESSAGES[code]).model_dump(
        mode="json"
    )


def move_error(exc: BaseException) -> Dict[str, Any]:
    """Map a failed move to the code the client switches on; detail stays in the log."""
    if isinstance(exc, asyncio.CancelledError):
        code = "interrupted"
    elif isinstance(exc, MoveSourceUnavailable):
        code = "source_unavailable"
    elif isinstance(exc, MoveNoRoom):
        code = "no_room"
    elif isinstance(exc, BackupIncomplete):
        return WorkspaceMoveError(
            code="backup_incomplete",
            message=exc.user_message,
            files=unsaved_files_out(exc.unsaved),
        ).model_dump(mode="json")
    elif isinstance(exc, ComputerBusyError):
        code = "busy"
    elif isinstance(exc, MoveRefused) and exc.code in MOVE_MESSAGES:
        code = exc.code
    else:
        code = "unknown"
    return move_error_detail(code)


async def claim_workspace_move(
    *,
    user_id: str,
    workspace_id: str,
    target_computer_id: str,
    make_primary: bool = False,
    from_backup: bool = False,
) -> Dict[str, Any]:
    """Record a move (``database.workspace_move.claim_workspace_move``), only
    a computer the providers let hold Home taking it as the primary, and only
    one the server cannot rebuild left behind by a move from the backup."""
    return await db.claim_workspace_move(
        user_id=user_id,
        workspace_id=workspace_id,
        target_computer_id=target_computer_id,
        make_primary=make_primary,
        from_backup=from_backup,
        may_be_primary=may_be_primary,
        may_be_left=lambda kind: not provider_class(kind).recreatable,
    )


async def set_primary_computer(user_id: str, computer_id: str) -> bool:
    """``database.workspace_move.set_primary_computer``, by the same rule."""
    return await db.set_primary_computer(
        user_id, computer_id, may_be_primary=may_be_primary
    )


def move_from_row(row: Optional[Mapping[str, Any]]) -> Optional[WorkspaceMoveOut]:
    """The move as a client reads it, a runner lost before staging read as failed.

    Staleness is projected on the database clock by every read of the row,
    so this answers what the next claim or sweep would decide. A stale move
    past staging is still in progress: recovery finishes it.
    """
    if row is None:
        return None
    phase = row["phase"]
    error = row.get("error")
    if row.get("stale") and phase in ("preparing", "backing_up"):
        phase, error = "failed", {"code": "interrupted"}
    if error is not None:
        code = error.get("code") if isinstance(error, Mapping) else None
        # A backup that left files unsaved words its own sentence, naming them.
        if code not in MOVE_MESSAGES and not (
            code == "backup_incomplete" and error.get("message")
        ):
            code = "unknown"
        # A row written by the sweep carries only the code.
        error = {
            "code": code,
            "message": error.get("message") or MOVE_MESSAGES[code],
            "files": error.get("files") or [],
        }
    return WorkspaceMoveOut(
        move_id=str(row["move_id"]),
        workspace_id=str(row["workspace_id"]),
        source_computer_id=str(row["source_computer_id"]),
        target_computer_id=str(row["target_computer_id"]),
        make_primary=bool(row["make_primary"]),
        from_backup=bool(row.get("from_backup")),
        phase=phase,
        error=error,
        created_at=row["created_at"],
        committed_at=row.get("committed_at"),
        finished_at=row.get("finished_at"),
    )
