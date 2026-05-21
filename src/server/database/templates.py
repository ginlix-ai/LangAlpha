"""Database access layer for template_entries.

Templates are upper-layer applications grouping workspaces under a shared
dashboard / schema. Each entry binds 1:1 to a workspace via CASCADE FK.

This module mirrors the style of src/server/database/workspace.py (raw psycopg3
queries, optional connection reuse, dict_row factory).
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any

from psycopg.rows import dict_row
from psycopg.types.json import Json

from src.server.database.conversation import get_db_connection

logger = logging.getLogger(__name__)


# Columns to return from SELECT/INSERT/UPDATE operations.
# Centralized so we don't drift between queries.
_RETURN_COLS = """
    entry_id, user_id, template_id, workspace_id, entry_key, display_name,
    status, progress, summary, payload, error_message, params,
    created_at, updated_at, completed_at
"""


# =============================================================================
# Create
# =============================================================================


async def create_entry(
    user_id: str,
    template_id: str,
    workspace_id: str,
    entry_key: str,
    display_name: str | None = None,
    params: dict[str, Any] | None = None,
    conn=None,
) -> dict[str, Any]:
    """Insert a new template entry. Status defaults to 'pending'.

    Raises psycopg.errors.UniqueViolation if (user_id, template_id, entry_key)
    or workspace_id already exists. Caller should catch and translate.
    """
    params_json = Json(params or {})

    async def _execute(cur):
        await cur.execute(
            f"""
            INSERT INTO template_entries
                (user_id, template_id, workspace_id, entry_key, display_name, params)
            VALUES (%s, %s, %s, %s, %s, %s)
            RETURNING {_RETURN_COLS}
            """,
            (user_id, template_id, workspace_id, entry_key, display_name, params_json),
        )
        return await cur.fetchone()

    try:
        if conn:
            async with conn.cursor(row_factory=dict_row) as cur:
                result = await _execute(cur)
        else:
            async with get_db_connection() as conn:
                async with conn.cursor(row_factory=dict_row) as cur:
                    result = await _execute(cur)

        logger.info(
            "Created template entry: %s (template=%s, workspace=%s)",
            result["entry_id"], template_id, workspace_id,
        )
        return dict(result)
    except Exception as e:
        logger.error(
            "Error creating template entry (template=%s, key=%s): %s",
            template_id, entry_key, e,
        )
        raise


# =============================================================================
# Read
# =============================================================================


async def get_entry(entry_id: str, conn=None) -> dict[str, Any] | None:
    """Get a single entry by id."""
    async def _execute(cur):
        await cur.execute(
            f"SELECT {_RETURN_COLS} FROM template_entries WHERE entry_id = %s",
            (entry_id,),
        )
        return await cur.fetchone()

    if conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            result = await _execute(cur)
    else:
        async with get_db_connection() as conn:
            async with conn.cursor(row_factory=dict_row) as cur:
                result = await _execute(cur)

    return dict(result) if result else None


async def get_entry_by_workspace(
    workspace_id: str, conn=None
) -> dict[str, Any] | None:
    """Look up entry by its bound workspace_id (UNIQUE)."""
    async def _execute(cur):
        await cur.execute(
            f"SELECT {_RETURN_COLS} FROM template_entries WHERE workspace_id = %s",
            (workspace_id,),
        )
        return await cur.fetchone()

    if conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            result = await _execute(cur)
    else:
        async with get_db_connection() as conn:
            async with conn.cursor(row_factory=dict_row) as cur:
                result = await _execute(cur)

    return dict(result) if result else None


async def list_entries(
    user_id: str,
    template_id: str,
    status: str | None = None,
    limit: int = 100,
    offset: int = 0,
    conn=None,
) -> tuple[list[dict[str, Any]], int]:
    """List entries for (user, template), optionally filtered by status.

    Returns (rows, total_count).
    """
    where_extra = ""
    extra_params: list[Any] = []
    if status:
        where_extra = "AND status = %s"
        extra_params.append(status)

    async def _execute(cur):
        # Total count
        await cur.execute(
            f"""
            SELECT COUNT(*) AS total FROM template_entries
            WHERE user_id = %s AND template_id = %s {where_extra}
            """,
            (user_id, template_id, *extra_params),
        )
        total_row = await cur.fetchone()
        total = total_row["total"] if total_row else 0

        # Rows
        await cur.execute(
            f"""
            SELECT {_RETURN_COLS} FROM template_entries
            WHERE user_id = %s AND template_id = %s {where_extra}
            ORDER BY updated_at DESC
            LIMIT %s OFFSET %s
            """,
            (user_id, template_id, *extra_params, limit, offset),
        )
        rows = await cur.fetchall()
        return [dict(r) for r in rows], total

    if conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            return await _execute(cur)
    else:
        async with get_db_connection() as conn:
            async with conn.cursor(row_factory=dict_row) as cur:
                return await _execute(cur)


# =============================================================================
# Update
# =============================================================================


async def update_progress(
    entry_id: str,
    progress: dict[str, Any],
    status: str | None = None,
    conn=None,
) -> dict[str, Any] | None:
    """Update progress (and optionally status) — used during analysis runs."""
    now = datetime.now(timezone.utc)

    async def _execute(cur):
        if status:
            await cur.execute(
                f"""
                UPDATE template_entries
                SET progress = %s, status = %s, updated_at = %s
                WHERE entry_id = %s
                RETURNING {_RETURN_COLS}
                """,
                (Json(progress), status, now, entry_id),
            )
        else:
            await cur.execute(
                f"""
                UPDATE template_entries
                SET progress = %s, updated_at = %s
                WHERE entry_id = %s
                RETURNING {_RETURN_COLS}
                """,
                (Json(progress), now, entry_id),
            )
        return await cur.fetchone()

    if conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            result = await _execute(cur)
    else:
        async with get_db_connection() as conn:
            async with conn.cursor(row_factory=dict_row) as cur:
                result = await _execute(cur)

    return dict(result) if result else None


async def finalize_entry(
    entry_id: str,
    status: str,
    summary: dict[str, Any] | None = None,
    payload: dict[str, Any] | None = None,
    error_message: str | None = None,
    conn=None,
) -> dict[str, Any] | None:
    """Finalize an entry after analysis completes (status = completed/failed).

    On success: status='completed', writes summary + payload, sets completed_at.
    On failure: status='failed', writes error_message.
    """
    if status not in ("completed", "failed"):
        raise ValueError(f"finalize_entry status must be completed/failed, got {status!r}")

    now = datetime.now(timezone.utc)

    async def _execute(cur):
        # ``::jsonb`` cast on the placeholder is required so COALESCE can
        # match the column type — psycopg sends Json as 'json', not 'jsonb'.
        await cur.execute(
            f"""
            UPDATE template_entries
            SET status = %s,
                summary = COALESCE(%s::jsonb, summary),
                payload = COALESCE(%s::jsonb, payload),
                error_message = %s,
                completed_at = %s,
                updated_at = %s
            WHERE entry_id = %s
            RETURNING {_RETURN_COLS}
            """,
            (
                status,
                Json(summary) if summary is not None else None,
                Json(payload) if payload is not None else None,
                error_message,
                now,
                now,
                entry_id,
            ),
        )
        return await cur.fetchone()

    if conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            result = await _execute(cur)
    else:
        async with get_db_connection() as conn:
            async with conn.cursor(row_factory=dict_row) as cur:
                result = await _execute(cur)

    if result:
        logger.info(
            "Finalized entry %s status=%s template=%s",
            entry_id, status, result["template_id"],
        )
    return dict(result) if result else None


async def reset_for_rerun(entry_id: str, conn=None) -> dict[str, Any] | None:
    """Reset an entry to 'pending' for a rerun. Keeps payload as last result."""
    now = datetime.now(timezone.utc)

    async def _execute(cur):
        await cur.execute(
            f"""
            UPDATE template_entries
            SET status = 'pending',
                progress = '{{}}'::jsonb,
                error_message = NULL,
                updated_at = %s
            WHERE entry_id = %s
            RETURNING {_RETURN_COLS}
            """,
            (now, entry_id),
        )
        return await cur.fetchone()

    if conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            result = await _execute(cur)
    else:
        async with get_db_connection() as conn:
            async with conn.cursor(row_factory=dict_row) as cur:
                result = await _execute(cur)

    return dict(result) if result else None


# =============================================================================
# Delete (rarely used directly — workspace CASCADE will usually handle this)
# =============================================================================


async def delete_entry(entry_id: str, conn=None) -> bool:
    """Hard delete an entry. Note: deleting the workspace will CASCADE delete
    the entry automatically, so this is only for "remove from dashboard but
    keep workspace" semantics (not exposed in v1)."""
    async def _execute(cur):
        await cur.execute(
            "DELETE FROM template_entries WHERE entry_id = %s RETURNING entry_id",
            (entry_id,),
        )
        return await cur.fetchone()

    if conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            result = await _execute(cur)
    else:
        async with get_db_connection() as conn:
            async with conn.cursor(row_factory=dict_row) as cur:
                result = await _execute(cur)

    return result is not None
