"""Moving a computer to a new root folder on its machine, and pointing it at
a machine paired again.

A computer's root was fixed at create until a reinstall could choose another,
so each workspace on it spells its old files under the old root. The move
keeps that root in each workspace's ``previous_root_dirs``, as a move to
another computer does, and owes each backed-up workspace a restore into its
folder under the new root, which starts empty.
"""

from __future__ import annotations

from typing import Any, Dict, Optional

from psycopg.rows import dict_row
from psycopg.types.json import Json

from src.server.database.pool import get_db_connection
from src.server.database.sql_fences import COMPUTER_COLS, FENCE_NOT_DELETED
from src.server.database.workspace_move import PREVIOUS_ROOT_DIRS_MAX

# A machine is repaired only while nothing runs on it: a start claims it from
# one of these, and its claim would read the row the repair is rewriting.
_REPAIRABLE = ["stopped", "creating"]


async def move_computer_root(
    computer_id: str, *, old_root: str, new_root: str, conn=None
) -> Optional[Dict[str, Any]]:
    """Point the computer at *new_root*, fenced on the root and status it was
    decided on; None when either moved meanwhile.

    One transaction, so no reader sees the new root with workspaces that do
    not yet remember the old one. A workspace with no backup is not flagged,
    since a restore would bring nothing and the flag only holds its backups
    back, and neither is one on a computer whose backup is off: the reinstall
    tells its owner the files stay in the old folder. The epoch moves for
    every workspace, so each worker attaches its folder anew under the new
    root.
    """
    async with get_db_connection(conn) as owned, owned.transaction():
        async with owned.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                f"""
                UPDATE computers
                SET root_dir = %(new)s, start_failure = NULL, updated_at = NOW()
                WHERE computer_id = %(computer_id)s
                  AND root_dir = %(old)s
                  AND status = ANY(%(repairable)s)
                  AND {FENCE_NOT_DELETED}
                RETURNING {COMPUTER_COLS}
                """,
                {
                    "computer_id": computer_id,
                    "old": old_root,
                    "new": new_root,
                    "repairable": _REPAIRABLE,
                },
            )
            row = await cur.fetchone()
            if row is None:
                return None
            await _owe_restores(
                cur,
                computer_id,
                old_root=old_root,
                backed_up=row.get("files_backup") is not False,
            )
    return dict(row)


async def _owe_restores(
    cur, computer_id: str, *, old_root: Optional[str], backed_up: bool = True
) -> None:
    """Owe each of the computer's workspaces with a backup a restore into its
    folder, which starts empty, and move every workspace's epoch so each
    worker attaches its folder anew. *old_root*, a root the computer left,
    is kept in each workspace's ``previous_root_dirs``. *backed_up* False owes
    none, and drops one the old folder still owed: the last backup of a
    computer whose backup is off is not laid unasked, and the new folder,
    missing the files that backup holds, is offered to its owner instead."""
    old = old_root.rstrip("/") if old_root else None
    await cur.execute(
        """
        WITH backed AS (
            SELECT w.workspace_id,
                   %(backed_up)s AND EXISTS (
                       SELECT 1 FROM workspace_files f
                       WHERE f.workspace_id = w.workspace_id
                   ) AS held
            FROM workspaces w
            WHERE w.computer_id = %(computer_id)s AND w.status <> 'deleted'
        )
        UPDATE workspaces w
        SET previous_root_dirs = CASE WHEN %(old)s::text IS NULL
                THEN w.previous_root_dirs
                ELSE (
                    ARRAY[%(old)s::text]
                    || array_remove(w.previous_root_dirs, %(old)s::text)
                )[1:%(cap)s] END,
            files_epoch = gen_random_uuid(),
            files_restore_incomplete_at = CASE WHEN b.held
                THEN COALESCE(w.files_restore_incomplete_at, NOW())
                WHEN NOT %(backed_up)s THEN NULL
                ELSE w.files_restore_incomplete_at END,
            files_restore_offered_at = CASE WHEN b.held
                THEN NULL ELSE w.files_restore_offered_at END
        FROM backed b
        WHERE w.workspace_id = b.workspace_id
        """,
        {
            "computer_id": computer_id,
            "old": old,
            "cap": PREVIOUS_ROOT_DIRS_MAX,
            "backed_up": backed_up,
        },
    )


async def repoint_computer_machine(
    computer_id: str,
    *,
    provider_config: Dict[str, Any],
    expected_provider_config: Dict[str, Any],
    moved: bool = False,
    conn=None,
) -> Optional[Dict[str, Any]]:
    """Point the computer at the machine as it was paired again, and forget
    the failed start that sent its owner to do it.

    Fenced on the target it was decided on and on the computer being down;
    None when either moved meanwhile. *moved*, a machine that holds nothing
    of the computer, owes its workspaces their restores in the same
    transaction, as a move to a new folder does, and as there, only while
    the computer's backup is on.
    """
    async with get_db_connection(conn) as owned, owned.transaction():
        async with owned.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                f"""
                UPDATE computers
                SET provider_config = %(config)s::jsonb,
                    start_failure = NULL,
                    updated_at = NOW()
                WHERE computer_id = %(computer_id)s
                  AND provider_config = %(expected)s::jsonb
                  AND status = ANY(%(repairable)s)
                  AND {FENCE_NOT_DELETED}
                RETURNING {COMPUTER_COLS}
                """,
                {
                    "computer_id": computer_id,
                    "config": Json(provider_config),
                    "expected": Json(expected_provider_config),
                    "repairable": _REPAIRABLE,
                },
            )
            row = await cur.fetchone()
            if row is not None and moved:
                await _owe_restores(
                    cur,
                    computer_id,
                    old_root=None,
                    backed_up=row.get("files_backup") is not False,
                )
    return dict(row) if row else None
