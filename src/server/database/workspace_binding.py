"""Where workspaces sit on their computers, in narrow reads.

Separate from ``workspace.py`` because every caller on the hot paths (turn
start, backup, restore, skill reconcile, a tool call on another computer)
wants a few of these facts and none of the wide JSONB columns a full row
carries.
"""

from __future__ import annotations

import logging
from collections.abc import Sequence
from typing import Any, Dict, Optional

from psycopg.rows import dict_row

from src.server.database.pool import get_db_connection
from src.server.utils.pg_sanitize import normalize_uuid

logger = logging.getLogger(__name__)

# Empty folder names are the computer root rather than a sibling directory, so
# they are not siblings of anything and never enter the denial list.
_BINDING_SQL = """
    SELECT
        w.computer_id,
        w.dir_name,
        w.status,
        w.layout_origin,
        w.previous_dir_names,
        ARRAY(
            SELECT s.dir_name
            FROM workspaces s
            WHERE s.computer_id = w.computer_id
              AND s.workspace_id <> w.workspace_id
              AND s.status <> 'deleted'
              AND s.dir_name IS NOT NULL
              AND s.dir_name <> ''
            ORDER BY s.created_at
        ) AS sibling_dir_names
    FROM workspaces w
    WHERE w.workspace_id = %s
"""


async def get_project_binding(
    workspace_id: str, *, conn=None
) -> Optional[Dict[str, Any]]:
    """The workspace's computer, its current and former folders there, and its siblings'.

    Raises on a read failure rather than answering None: the caller cannot tell
    a failed read from an unbound project, and treating the second as the first
    hands it the whole computer.
    """
    normalized = normalize_uuid(workspace_id)
    if normalized is None:
        return None

    try:
        async with get_db_connection(conn) as owned:
            async with owned.cursor(row_factory=dict_row) as cur:
                await cur.execute(_BINDING_SQL, (normalized,))
                row = await cur.fetchone()
    except Exception as e:
        logger.error(f"Error reading the binding for workspace {workspace_id}: {e}")
        raise
    if row is None:
        return None
    return {
        "computer_id": row["computer_id"],
        "dir_name": row["dir_name"],
        "status": row["status"],
        "layout_origin": row.get("layout_origin"),
        "previous_dir_names": tuple(row.get("previous_dir_names") or ()),
        "sibling_dir_names": tuple(row["sibling_dir_names"] or ()),
    }


async def get_computer_folders(computer_id: str, *, conn=None) -> dict[str, str]:
    """The live workspaces on a computer as {folder: workspace_id}, oldest
    first, for a caller at its root, where each folder is a workspace's own
    and none is the caller's."""
    normalized = normalize_uuid(computer_id)
    if normalized is None:
        return {}
    async with get_db_connection(conn) as owned:
        async with owned.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                """
                SELECT dir_name, workspace_id FROM workspaces
                WHERE computer_id = %s
                  AND status <> 'deleted'
                  AND dir_name IS NOT NULL
                  AND dir_name <> ''
                ORDER BY created_at
                """,
                (normalized,),
            )
            return {row["dir_name"]: str(row["workspace_id"]) for row in await cur.fetchall()}


async def get_computers_with_folders(
    computer_ids: Sequence[str], *, conn=None
) -> set[str]:
    """Which of ``computer_ids`` hold a live workspace folder, in one read."""
    normalized = [n for n in map(normalize_uuid, computer_ids) if n is not None]
    if not normalized:
        return set()
    async with get_db_connection(conn) as owned:
        async with owned.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                """
                SELECT DISTINCT computer_id FROM workspaces
                WHERE computer_id = ANY(%s::uuid[])
                  AND status <> 'deleted'
                  AND dir_name IS NOT NULL
                  AND dir_name <> ''
                """,
                (normalized,),
            )
            return {str(row["computer_id"]) for row in await cur.fetchall()}
