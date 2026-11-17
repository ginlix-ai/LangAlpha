"""Home's stand-in: where a Home turn runs while the machine Home lives on can't be reached.

A hidden workspace row at a fixed id (``home_workspace.get_home_stand_in_id``)
on a cloud computer the user already has; nothing is ever built for it. Where
it may stand is one SQL rule here, read alike by a turn's placement and by the
sweeps that release Home's parked work, so work is released only onto the
computer a Home turn would run on. Two workers placing it at once converge on
the fixed id.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any, Dict, Mapping, Optional

from psycopg.rows import dict_row
from psycopg.types.json import Json

from ptc_agent.core.sandbox.providers import RECREATABLE_KINDS
from src.server.database.home_away import OWED_SQL, left_on_stand_in, left_sql
from src.server.database.home_workspace import (
    HOME_STAND_IN_KEY,
    get_home_stand_in_id,
    home_id_sql,
    stand_in_id_sql,
)
from src.server.database.pool import get_db_connection
from src.server.database.sql_fences import computer_cols, takes_a_workspace_sql
from src.server.database.user_lock import lock_user_writes
from src.server.database.workspace_folders import (
    folder_allocation,
    release_former_folder,
    takes_name_folders,
)
from src.server.database.workspace_names import HOME_FOLDER, HOME_STAND_IN_FOLDER

logger = logging.getLogger(__name__)

# The kinds the stand-in stands on: those the deployment builds, never a
# machine the user brought, which may be the one that is away.
_KINDS = ", ".join(f"'{kind}'" for kind in RECREATABLE_KINDS)

# A read of what the user keeps on a computer leaves the stand-in out by its
# mark: it is placed again on the next need, so it alone keeps nothing.
EXCLUDE_STAND_IN = f"NOT COALESCE(config, '{{}}'::jsonb) ? '{HOME_STAND_IN_KEY}'"

# The stand-in's computer columns, beside its own of the same names.
_HOST = "host__"


def _owed_sql(user_id_sql: str) -> str:
    return (
        "EXISTS (SELECT 1 FROM home_away"
        f" WHERE home_away.user_id = {user_id_sql} AND {OWED_SQL})"
    )


def _left_sql(user_id_sql: str, computer_id_sql: str) -> str:
    return (
        "EXISTS (SELECT 1 FROM home_away"
        f" WHERE home_away.user_id = {user_id_sql} AND {left_sql(computer_id_sql)})"
    )


def kept_sql(stand_in: str, computer: str) -> str:
    """Whether stand-in row ``stand_in`` stays on computer row ``computer``,
    its own: while it takes a workspace, so the stand-in's files are where
    the next turn on the cloud looks, and while files Home is owed, or files
    the last hand-back left, are there, however that computer fares, since
    they are nowhere else and a folder no row names would go unwatched."""
    user_id = f"{stand_in}.user_id"
    return (
        f"({stand_in}.status <> 'deleted' AND {computer}.status <> 'deleted'"
        f" AND ({takes_a_workspace_sql(computer)}"
        f" OR {_owed_sql(user_id)} OR {_left_sql(user_id, f'{computer}.computer_id')}))"
    )


def _turn_host_sql(host: str) -> str:
    """Whether computer row ``host`` is where a Home turn in the stand-in runs
    now: the computer the stand-in stays on (``kept_sql``) if it takes a
    workspace, else, with none kept, any of its user's of the kinds built that
    takes one. Kept on a computer that takes none (failed, with owed or left
    files on it), no computer is: Home's turns and parked work wait for Home's
    machine, or for the user to start or remove that computer, and the
    hand-back's own retries see to owed files."""
    return (
        f"({host}.kind IN ({_KINDS}) AND {takes_a_workspace_sql(host)}"
        " AND NOT EXISTS (SELECT 1 FROM workspaces kept_s"
        " JOIN computers kept_c ON kept_c.computer_id = kept_s.computer_id"
        f" WHERE kept_s.workspace_id = {stand_in_id_sql(f'{host}.user_id')}"
        f" AND kept_c.computer_id <> {host}.computer_id"
        f" AND {kept_sql('kept_s', 'kept_c')}))"
    )


def home_stand_in_could_run_sql(ws: str) -> str:
    """Whether workspace row ``ws`` is its user's Home and a Home turn could
    run in the stand-in now (``stand_in_computer``), so Home's work need not
    wait for the machine Home lives on."""
    return (
        f"{ws}.workspace_id = {home_id_sql(f'{ws}.user_id')}"
        " AND EXISTS (SELECT 1 FROM computers host"
        f" WHERE host.user_id = {ws}.user_id AND {_turn_host_sql('host')})"
    )


async def stand_in_computer(user_id: str, *, conn=None) -> Optional[Dict[str, Any]]:
    """The computer a Home turn in the stand-in runs on now
    (``_turn_host_sql``): the one it stands on, else the one it would be
    placed on, a running one first, as the turn need not wait for a start,
    then the most recently used, then the oldest. None when there is none. A
    read: nothing is placed, built or started."""
    async with get_db_connection(conn) as c, c.cursor(row_factory=dict_row) as cur:
        await cur.execute(
            f"""
            SELECT {computer_cols("host")}
            FROM computers host
            WHERE host.user_id = %(user)s AND {_turn_host_sql("host")}
            ORDER BY (host.status = 'running') DESC,
                     host.last_activity_at DESC NULLS LAST,
                     host.created_at, host.computer_id
            LIMIT 1
            """,
            {"user": user_id},
        )
        row = await cur.fetchone()
    return dict(row) if row else None


async def stand_in_needed(user_id: str, *, conn=None) -> bool:
    """Whether the primary is a machine of the user's own, which can be away,
    so the cloud computer Home's stand-in stands on is kept for it when its
    last workspace goes, rather than retired with nowhere left to stand."""
    async with get_db_connection(conn) as c, c.cursor() as cur:
        await cur.execute(
            f"""
            SELECT 1 FROM computers
            WHERE user_id = %s AND is_primary AND status <> 'deleted'
              AND kind NOT IN ({_KINDS})
            """,
            (user_id,),
        )
        return await cur.fetchone() is not None


@dataclass(frozen=True, slots=True)
class StandIn:
    """Home's stand-in as it stands: a live row on a live computer."""

    row: Dict[str, Any]
    computer: Dict[str, Any]
    #: It stays on this computer (``kept_sql``).
    kept: bool
    #: A Home turn in it runs on this computer now (``_turn_host_sql``).
    runs: bool
    #: Files Home is owed are in it.
    owed: bool
    #: The last hand-back left files in it, too large, unreadable, or all of
    #: them when it gave up (``home_away.left_on_stand_in``).
    left: bool


async def read_stand_in(user_id: str, *, conn=None) -> Optional[StandIn]:
    """The user's stand-in, its computer and what keeps it there, in one
    read; None when it stands nowhere."""
    from src.server.database.workspace import _ws_cols

    async with get_db_connection(conn) as c, c.cursor(row_factory=dict_row) as cur:
        await cur.execute(
            f"""
            SELECT {_ws_cols("s")}, {computer_cols("c", named=_HOST)},
                   {kept_sql("s", "c")} AS stand_in_kept,
                   {_turn_host_sql("c")} AS stand_in_runs,
                   {_owed_sql("s.user_id")} AS stand_in_owed,
                   (SELECT last_handback FROM home_away
                    WHERE home_away.user_id = s.user_id) AS stand_in_last_handback
            FROM workspaces s
            JOIN computers c ON c.computer_id = s.computer_id
            WHERE s.workspace_id = %(id)s AND s.user_id = %(user)s
              AND s.status <> 'deleted' AND c.status <> 'deleted'
            """,
            {"id": get_home_stand_in_id(user_id), "user": user_id},
        )
        record = await cur.fetchone()
    if record is None:
        return None
    row: Dict[str, Any] = {}
    computer: Dict[str, Any] = {}
    for name, value in record.items():
        if name.startswith(_HOST):
            computer[name[len(_HOST) :]] = value
        elif not name.startswith("stand_in_"):
            row[name] = value
    return StandIn(
        row=row,
        computer=computer,
        kept=record["stand_in_kept"],
        runs=record["stand_in_runs"],
        owed=record["stand_in_owed"],
        left=left_on_stand_in(
            record["stand_in_last_handback"], str(computer["computer_id"])
        )
        is not None,
    )


async def stand_in_on(computer: Mapping[str, Any]) -> Optional[StandIn]:
    """Home's stand-in if it stands on ``computer``, whose owner it serves."""
    user_id = str(computer.get("user_id") or "")
    if not user_id:
        return None
    stand_in = await read_stand_in(user_id)
    if stand_in is None or str(stand_in.computer["computer_id"]) != str(
        computer["computer_id"]
    ):
        return None
    return stand_in


async def place_home_stand_in(user_id: str) -> Optional[StandIn]:
    """Home's stand-in where a Home turn runs now, placed there unless it
    already stands on a computer that keeps it (``kept_sql``).

    Inserted already bound, as a workspace created on a computer is, never
    ``unbound``: an unbound row at another id is what the Home fold takes in.
    Placed again, its row or computer gone or that computer failed or being
    retired with nothing owed, on ``stand_in_computer``, in a folder no
    workspace can take (``HOME_STAND_IN_FOLDER``). Its tool selection is
    Home's, read through (``home_workspace.acting_for``), never a copy.
    The loser of two workers placing it at once reads the winner's. None when
    it stands nowhere and no computer takes it; the caller asks ``runs``
    before a turn goes there.
    """
    from src.server.database.computer import get_computer

    current = await read_stand_in(user_id)
    if current is not None and current.kept:
        return current
    host = await stand_in_computer(user_id)
    if host is None:
        return current
    computer_id = str(host["computer_id"])
    workspace_id = get_home_stand_in_id(user_id)
    async with (
        folder_allocation(computer_id) as owned,
        owned.cursor(row_factory=dict_row) as cur,
    ):
        await lock_user_writes(cur, user_id)
        # A turn's START holds this row FOR SHARE until its away mark
        # commits (``mark_away``): waiting here, the move below reads that
        # mark in a snapshot of its own and keeps the stand-in for the turn.
        # After the user's lock, which orders placements among themselves,
        # and after the computer's row: a computer's status write takes it,
        # then its workspaces' rows (``shadowed_write``), so taking them the
        # other way round could deadlock with one.
        await cur.execute(
            "SELECT 1 FROM computers WHERE computer_id = %s FOR SHARE", (computer_id,)
        )
        await cur.execute(
            "SELECT 1 FROM workspaces WHERE workspace_id = %s FOR UPDATE",
            (workspace_id,),
        )
        computer = await get_computer(computer_id, conn=owned)
        # No workspace's folder starts with a dot and no other user's stand-in
        # is on this computer, so the folder is free. On a computer not on the
        # folder layout yet, a placeholder, which the first settle after the
        # move renames (``FolderRow.target``).
        dir_name = HOME_STAND_IN_FOLDER
        if computer is not None and not takes_name_folders(computer):
            dir_name = f"{HOME_STAND_IN_FOLDER}-{workspace_id[:8]}"
        await cur.execute(
            f"""
            WITH comp AS (
                SELECT computer_id, status, resource_tier, is_always_on,
                       provider_ref, platform_secret_version
                FROM computers host
                WHERE computer_id = %(computer_id)s AND user_id = %(user_id)s
                  AND {_turn_host_sql("host")}
                FOR SHARE
            )
            INSERT INTO workspaces (
                workspace_id, user_id, name, config, computer_id, dir_name,
                status, resource_tier, is_always_on, sandbox_id,
                platform_secret_version
            )
            SELECT %(workspace_id)s::uuid, %(user_id)s, %(name)s, %(config)s,
                   comp.computer_id, %(dir_name)s, comp.status,
                   comp.resource_tier, comp.is_always_on, comp.provider_ref,
                   comp.platform_secret_version
            FROM comp
            ON CONFLICT (workspace_id) DO UPDATE SET
                computer_id = EXCLUDED.computer_id,
                dir_name = EXCLUDED.dir_name,
                status = EXCLUDED.status,
                resource_tier = EXCLUDED.resource_tier,
                is_always_on = EXCLUDED.is_always_on,
                sandbox_id = EXCLUDED.sandbox_id,
                platform_secret_version = EXCLUDED.platform_secret_version,
                config = EXCLUDED.config,
                previous_dir_names = '{{}}',
                updated_at = NOW()
            WHERE workspaces.user_id = EXCLUDED.user_id
              AND NOT EXISTS (
                  SELECT 1 FROM computers c
                  WHERE c.computer_id = workspaces.computer_id
                    AND {kept_sql("workspaces", "c")}
              )
            RETURNING dir_name
            """,
            {
                "computer_id": computer_id,
                "workspace_id": workspace_id,
                "user_id": user_id,
                "name": HOME_FOLDER,
                "config": Json({HOME_STAND_IN_KEY: True}),
                "dir_name": dir_name,
            },
        )
        placed = await cur.fetchone() is not None
        if placed:
            await release_former_folder(
                cur, computer_id=computer_id, workspace_id=workspace_id, folder=dir_name
            )
    if placed:
        logger.info(
            f"Placed Home's stand-in {workspace_id} for user {user_id} on computer "
            f"{computer_id} as {dir_name}"
        )
    # Placed here, or by another worker first, or ``computer_id`` stopped
    # taking it meanwhile: what stands now.
    return await read_stand_in(user_id)
