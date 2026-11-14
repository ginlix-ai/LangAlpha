"""Where a turn a request would start runs, and who sent a thread its turn.

A computer's token reaches all of the user's computers and Home; where a turn
would run decides only whether it waits for the user's OK first
(``dependencies.cli_approval``), which it does on any computer but the
token's own. Who sent a thread its current turn is the loop guard's
(``dependencies.principal``).
"""

from __future__ import annotations

from dataclasses import dataclass

from src.server.database.computer import get_home_computer
from src.server.database.home_workspace import (
    get_home_workspace_id,
    is_home_id,
    is_home_row,
)
from src.server.database.pool import get_db_connection
from src.server.database.sql_fences import FENCE_NOT_DELETED
from src.server.utils.pg_sanitize import normalize_uuid


@dataclass(frozen=True)
class Place:
    """The workspace a turn runs in and the computer it runs on, with the
    names the app shows of them."""

    workspace_id: str
    workspace_name: str | None
    computer_id: str | None
    computer_name: str | None
    in_home: bool


# The workspace and the live computer it is bound to; ``{source}`` is either
# the workspace row itself or a thread joined to it.
_PLACE_SQL = f"""
    SELECT w.workspace_id, w.user_id, w.status, w.name, c.computer_id, c.name
    FROM {{source}}
    LEFT JOIN computers c ON c.computer_id = w.computer_id AND c.{FENCE_NOT_DELETED}
    WHERE {{key}} = %s::uuid AND w.user_id = %s AND w.status <> 'deleted'
"""
_BY_WORKSPACE = _PLACE_SQL.format(source="workspaces w", key="w.workspace_id")
_BY_THREAD = _PLACE_SQL.format(
    source=(
        "conversation_threads t JOIN workspaces w ON w.workspace_id = t.workspace_id"
    ),
    key="t.conversation_thread_id",
)


async def turn_place(
    user_id: str, *, workspace_id: str | None = None, thread_id: str | None = None
) -> Place | None:
    """Where a turn runs in the thread's workspace when one is named, else
    the workspace, else the user's Home; None for one the user does not have.

    Home, and a workspace on no computer yet, run on Home's computer
    (``get_home_computer``). Home's id is known without its row, which its
    first turn creates, so it never reads as one the user lacks.
    """
    if thread_id is None and (workspace_id is None or is_home_id(user_id, workspace_id)):
        return await _home_place(user_id)
    resource_id = normalize_uuid(thread_id if thread_id is not None else workspace_id)
    if resource_id is None:
        return None
    async with get_db_connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                _BY_THREAD if thread_id is not None else _BY_WORKSPACE, (resource_id, user_id)
            )
            row = await cur.fetchone()
    if row is None:
        return None
    found_id, owner, status, name, computer_id, computer_name = row
    if is_home_row({"user_id": owner, "workspace_id": found_id, "status": status}):
        return await _home_place(user_id)
    if computer_id is None:
        home = await _home_place(user_id)
        return Place(str(found_id), name, home.computer_id, home.computer_name, False)
    return Place(str(found_id), name, str(computer_id), computer_name, False)


async def _home_place(user_id: str) -> Place:
    computer = await get_home_computer(user_id)
    return Place(
        get_home_workspace_id(user_id),
        None,
        str(computer["computer_id"]) if computer else None,
        computer.get("name") if computer else None,
        True,
    )


async def turn_sender(user_id: str, thread_id: str) -> str | None:
    """The thread whose agent sent ``thread_id`` its latest turn: a hand-off
    from the Chief of Staff, or a send through ``la``; None for a turn a
    person, a schedule or the server started."""
    async with get_db_connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                """
                SELECT q.metadata->'origin'->>'type', q.metadata->'origin'->>'agent',
                       q.metadata->'origin'->>'id'
                FROM conversation_queries q
                JOIN conversation_threads t ON t.conversation_thread_id = q.conversation_thread_id
                JOIN workspaces w ON w.workspace_id = t.workspace_id
                WHERE q.conversation_thread_id = %s::uuid AND w.user_id = %s
                ORDER BY q.turn_index DESC LIMIT 1
                """,
                (thread_id, user_id),
            )
            row = await cur.fetchone()
    if row is None or row[0] != "agent" or row[1] not in ("cli", "chief_of_staff"):
        return None
    return str(row[2]) if row[2] else None
