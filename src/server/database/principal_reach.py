"""Which computer a resource a route names sits on, for a computer's token.

A computer's token reaches only what its own computer holds, since separate
computers are the isolation boundary between workspaces, and never Home,
whose turns run the Chief of Staff, which reaches every computer. A lookup
answers ``MISSING`` for a resource the user does not have, so the route
answers its own 404, ``HOME`` for one in the user's Home whatever computer
Home sits on, and None for one on no computer.
"""

from __future__ import annotations

import enum
from typing import Any, Final, Literal

from src.server.database.home_workspace import get_home_workspace_id, is_home_row
from src.server.database.pool import get_db_connection


class Missing(enum.Enum):
    MISSING = "missing"


class InHome(enum.Enum):
    HOME = "home"


MISSING: Final = Missing.MISSING
HOME: Final = InHome.HOME

#: What a reach lookup answers: the computer, or one of the sentinels above.
Reach = str | None | Missing | InHome

ReachKind = Literal["workspace", "thread", "automation"]


def workspace_or_home(user_id: str, workspace_id: Any) -> str:
    """The workspace a turn or an automation naming ``workspace_id`` runs in:
    the user's Home when it names none. ``automation_workspace_sql`` is the
    same rule in SQL."""
    return str(workspace_id) if workspace_id else get_home_workspace_id(user_id)


def automation_workspace_sql(alias: str) -> str:
    """The workspace the automation row ``alias`` runs in, as SQL taking one
    positional parameter, the user's Home id (``workspace_or_home``)."""
    return f"COALESCE({alias}.workspace_id, %s::uuid)"


# Each answers the computer, then the workspace and its status, which say
# whether it is Home (``is_home_row``).
_COMPUTER_OF: Final[dict[ReachKind, str]] = {
    "workspace": (
        "SELECT computer_id, workspace_id, status FROM workspaces"
        " WHERE workspace_id = %s::uuid AND user_id = %s"
    ),
    "thread": (
        "SELECT w.computer_id, w.workspace_id, w.status FROM conversation_threads t"
        " JOIN workspaces w ON w.workspace_id = t.workspace_id"
        " WHERE t.conversation_thread_id = %s::uuid AND w.user_id = %s"
    ),
    # Home's row may not exist yet, so the workspace is the automation's own.
    "automation": (
        f"SELECT w.computer_id, {automation_workspace_sql('a')}, w.status"
        " FROM automations a"
        f" LEFT JOIN workspaces w ON w.workspace_id = {automation_workspace_sql('a')}"
        " WHERE a.automation_id = %s::uuid AND a.user_id = %s"
    ),
}


async def computer_of(kind: ReachKind, user_id: str, resource_id: str) -> Reach:
    params: tuple[str, ...] = (resource_id, user_id)
    if kind == "automation":
        home_id = get_home_workspace_id(user_id)
        params = (home_id, home_id, *params)
    async with get_db_connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(_COMPUTER_OF[kind], params)
            row = await cur.fetchone()
    if row is None:
        return MISSING
    computer_id, workspace_id, status = row
    if is_home_row({"user_id": user_id, "workspace_id": workspace_id, "status": status}):
        return HOME
    return str(computer_id) if computer_id is not None else None


async def automation_thread(user_id: str, automation_id: str) -> str | None:
    """The thread the automation's runs send into; None for one that starts
    a thread each run, or that the user does not have."""
    async with get_db_connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                "SELECT conversation_thread_id FROM automations"
                " WHERE automation_id = %s::uuid AND user_id = %s",
                (automation_id, user_id),
            )
            row = await cur.fetchone()
    return str(row[0]) if row is not None and row[0] is not None else None
