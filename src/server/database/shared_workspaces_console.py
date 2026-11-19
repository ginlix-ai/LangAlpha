"""The organization console's listing of its shared workspaces, a page at a
time, newest first."""

from __future__ import annotations

from base64 import urlsafe_b64decode, urlsafe_b64encode
from datetime import datetime
from typing import Any, Optional
from uuid import UUID

from psycopg.rows import dict_row

from src.server.database.pool import get_db_connection
from src.server.database.runs.lifecycle import live_runs_exist
from src.server.database.shared_workspaces import SHARED_COLS
from src.server.utils.pg_sanitize import normalize_uuid

CONSOLE_PAGE_SIZE = 25
_CONSOLE_MAX_PAGE = 100


def decode_console_cursor(cursor: Optional[str]) -> Optional[tuple[datetime, str]]:
    """The ``(created_at, workspace_id)`` keyset a cursor names. Malformed
    raises, so a bad cursor is a 400 rather than the first page again."""
    if not cursor:
        return None
    try:
        raw = urlsafe_b64decode(cursor + "=" * (-len(cursor) % 4)).decode()
        created, _, workspace_id = raw.partition("|")
        stamp = datetime.fromisoformat(created)
        if stamp.tzinfo is None:
            raise ValueError("naive timestamp")
        return stamp, str(UUID(workspace_id))
    except (ValueError, UnicodeDecodeError) as exc:
        raise ValueError("malformed cursor") from exc


def _console_cursor(row: dict[str, Any]) -> str:
    raw = f"{row['created_at'].isoformat()}|{row['workspace_id']}"
    return urlsafe_b64encode(raw.encode()).decode().rstrip("=")


async def list_for_org(
    org_id: str,
    *,
    cursor: Optional[str] = None,
    limit: int = CONSOLE_PAGE_SIZE,
    workspace_id: Optional[str] = None,
) -> tuple[list[dict[str, Any]], Optional[str]]:
    """One page of the organization's live shared workspaces, newest first,
    each with its members, its computer's state and whether a turn runs in
    it now, and the cursor for the
    next page (None on the last). ``workspace_id`` keeps to that one."""
    limit = max(1, min(int(limit or CONSOLE_PAGE_SIZE), _CONSOLE_MAX_PAGE))
    params: dict[str, Any] = {"org": org_id, "limit": limit + 1}
    after = ""
    keyset = decode_console_cursor(cursor)
    if keyset is not None:
        after = "AND (s.created_at, s.workspace_id) < (%(after_at)s, %(after_id)s::uuid)"
        params["after_at"], params["after_id"] = keyset
    if workspace_id is not None:
        workspace_id = normalize_uuid(workspace_id)
        if workspace_id is None:
            return [], None
        after += " AND s.workspace_id = %(only)s::uuid"
        params["only"] = workspace_id
    async with get_db_connection() as conn, conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(
            f"""
            SELECT {SHARED_COLS}, s.created_at, w.name, w.description,
                   cu.name AS created_by_name, cu.email AS created_by_email,
                   c.status AS computer_status,
                   {live_runs_exist('w.workspace_id')} AS turn_running,
                   GREATEST(
                       w.updated_at, c.last_activity_at,
                       (SELECT max(t.updated_at) FROM conversation_threads t
                         WHERE t.workspace_id = w.workspace_id)
                   ) AS last_activity_at
              FROM shared_workspaces s
              JOIN workspaces w ON w.workspace_id = s.workspace_id
              LEFT JOIN computers c ON c.computer_id = w.computer_id
              LEFT JOIN users cu ON cu.user_id = s.created_by
             WHERE s.org_id = %(org)s AND w.status <> 'deleted' {after}
             ORDER BY s.created_at DESC, s.workspace_id DESC
             LIMIT %(limit)s
            """,
            params,
        )
        rows = [dict(r) for r in await cur.fetchall()]
        more = len(rows) > limit
        rows = rows[:limit]
        if rows:
            await cur.execute(
                """
                SELECT m.workspace_id::text AS workspace_id, m.user_id, m.role,
                       m.added_at, u.name, u.email
                  FROM shared_workspace_members m
                  JOIN users u ON u.user_id = m.user_id
                 WHERE m.workspace_id = ANY(%s::uuid[])
                 ORDER BY m.role, m.added_at, m.user_id
                """,
                ([r["workspace_id"] for r in rows],),
            )
            by_workspace: dict[str, list[dict[str, Any]]] = {}
            for m in await cur.fetchall():
                by_workspace.setdefault(m.pop("workspace_id"), []).append(dict(m))
            for r in rows:
                r["members"] = by_workspace.get(r["workspace_id"], [])
    return rows, _console_cursor(rows[-1]) if more else None
