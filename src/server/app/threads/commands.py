"""A thread's background commands: the Bash and ExecuteCode jobs handed off
past the tool's wait, running or ended, and the user's stop."""

from typing import List, Optional

from fastapi import HTTPException, Query

from src.server.database.runs import command_runs as cr_db
from src.server.models.command import (
    CommandOut,
    CommandsResponse,
    CommandStatus,
    CommandStopResponse,
)
from src.server.services.commands import control
from src.server.utils import api as auth_api
from src.server.utils.api import CurrentUserId

from ._deps import router


@router.get("/{thread_id}/commands", response_model=CommandsResponse)
async def list_commands(
    thread_id: str,
    x_user_id: CurrentUserId,
    status: Optional[List[CommandStatus]] = Query(default=None),
    limit: int = Query(default=50, ge=1, le=200),
):
    """The thread's commands: every running one, then the ``limit`` newest
    started of those that ended; ``status`` (repeatable) keeps only those
    statuses."""
    await auth_api.require_thread_owner(thread_id, x_user_id)
    rows = await cr_db.list_for_thread(thread_id, statuses=status, limit=limit)
    return CommandsResponse(
        thread_id=thread_id, commands=[CommandOut.model_validate(r) for r in rows]
    )


@router.post(
    "/{thread_id}/commands/{command_id}/stop", response_model=CommandStopResponse
)
async def stop_thread_command(
    thread_id: str, command_id: str, x_user_id: CurrentUserId
):
    """Stop a running command. The agent is told at the thread's next turn.

    ``stopped`` is False for one that had already ended; the row in the
    answer says how. 404 when the command is not this thread's."""
    await auth_api.require_thread_owner(thread_id, x_user_id)
    result = await control.stop_thread_command(thread_id, command_id, by="user")
    if result.outcome == "not_found":
        raise HTTPException(status_code=404, detail="Command not found")
    return CommandStopResponse(
        stopped=result.outcome == "stopped",
        command=CommandOut.model_validate(result.row),
    )
