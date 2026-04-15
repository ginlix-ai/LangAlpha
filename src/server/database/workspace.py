"""
Database utility functions for workspace management.

Provides functions for creating, retrieving, and managing workspaces in PostgreSQL.
Each workspace has a 1:1 mapping with a Daytona sandbox.
"""

import logging
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

from psycopg.rows import dict_row

from src.server.database.conversation import get_db_connection

logger = logging.getLogger(__name__)

# Deterministic namespace for flash workspace UUIDs
FLASH_WORKSPACE_NAMESPACE = uuid.UUID("f1a50000-0000-5000-e000-f1a500000000")


def get_flash_workspace_id(user_id: str) -> str:
    """Deterministic UUID v5 — same user always gets the same flash workspace ID."""
    return str(uuid.uuid5(FLASH_WORKSPACE_NAMESPACE, user_id))


async def get_or_create_flash_workspace(
    user_id: str, conn=None
) -> Dict[str, Any]:
    """
    Upsert the user's shared flash workspace. No lookup needed — ID is computed.

    Uses deterministic UUID v5 so the same user always maps to the same workspace.
    INSERT ... ON CONFLICT DO UPDATE makes this idempotent and race-condition-free.
    """
    from psycopg.types.json import Json

    workspace_id = get_flash_workspace_id(user_id)
    config_json = Json({"flash_mode": True})

    async def _execute(cur):
        await cur.execute(
            """
            INSERT INTO workspaces (workspace_id, user_id, name, description, config, status, is_pinned)
            VALUES (%s, %s, %s, %s, %s, %s, TRUE)
            ON CONFLICT (workspace_id) DO UPDATE SET updated_at = NOW(), is_pinned = TRUE
            RETURNING workspace_id, user_id, name, description, sandbox_id,
                      status, created_at, updated_at, last_activity_at, stopped_at, config, artifacts,
                      is_pinned, sort_order
            """,
            (workspace_id, user_id, "Flash", "Flash mode conversations", config_json, "flash"),
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

        logger.info(f"Upserted flash workspace: {workspace_id} for user: {user_id}")
        return dict(result)

    except Exception as e:
        logger.error(f"Error upserting flash workspace for user {user_id}: {e}")
        raise


# =============================================================================
# Workspace CRUD Operations
# =============================================================================


async def create_workspace(
    user_id: str,
    name: str,
    description: Optional[str] = None,
    config: Optional[Dict[str, Any]] = None,
    conn=None,
    workspace_id: Optional[str] = None,
    status: str = "creating",
) -> Dict[str, Any]:
    """
    Create a new workspace entry.

    Args:
        user_id: User ID who owns the workspace
        name: Workspace name
        description: Optional workspace description
        config: Optional configuration as JSON
        conn: Optional database connection to reuse
        workspace_id: Optional specific workspace ID (UUID). If None, auto-generated.
        status: Initial status (default: "creating", use "flash" for flash workspaces)

    Returns:
        Created workspace record as dict
    """
    from psycopg.types.json import Json

    try:
        config_json = Json(config) if config else Json({})

        async def _execute(cur):
            if workspace_id:
                # Use specific workspace_id (for flash mode: workspace_id = thread_id)
                await cur.execute(
                    """
                    INSERT INTO workspaces (workspace_id, user_id, name, description, config, status)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    RETURNING workspace_id, user_id, name, description, sandbox_id,
                              status, created_at, updated_at, last_activity_at, stopped_at, config, artifacts,
                              is_pinned, sort_order
                    """,
                    (workspace_id, user_id, name, description, config_json, status),
                )
            else:
                # Auto-generate workspace_id
                await cur.execute(
                    """
                    INSERT INTO workspaces (user_id, name, description, config, status)
                    VALUES (%s, %s, %s, %s, %s)
                    RETURNING workspace_id, user_id, name, description, sandbox_id,
                              status, created_at, updated_at, last_activity_at, stopped_at, config, artifacts,
                              is_pinned, sort_order
                    """,
                    (user_id, name, description, config_json, status),
                )
            return await cur.fetchone()

        if conn:
            async with conn.cursor(row_factory=dict_row) as cur:
                result = await _execute(cur)
        else:
            async with get_db_connection() as conn:
                async with conn.cursor(row_factory=dict_row) as cur:
                    result = await _execute(cur)

        logger.info(f"Created workspace: {result['workspace_id']} for user: {user_id}")
        return dict(result)

    except Exception as e:
        logger.error(f"Error creating workspace for user {user_id}: {e}")
        raise


async def get_workspace(
    workspace_id: str,
    conn=None,
) -> Optional[Dict[str, Any]]:
    """
    Get a workspace by ID.

    Args:
        workspace_id: Workspace UUID
        conn: Optional database connection to reuse

    Returns:
        Workspace record as dict, or None if not found
    """
    try:
        async def _execute(cur):
            await cur.execute(
                """
                SELECT workspace_id, user_id, name, description, sandbox_id,
                       status, created_at, updated_at, last_activity_at, stopped_at, config, artifacts,
                       is_pinned, sort_order
                FROM workspaces
                WHERE workspace_id = %s AND status != 'deleted'
                """,
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

        if result:
            return dict(result)
        return None

    except Exception as e:
        logger.error(f"Error getting workspace {workspace_id}: {e}")
        raise


async def get_workspaces_for_user(
    user_id: str,
    limit: int = 20,
    offset: int = 0,
    include_deleted: bool = False,
    sort_by: str = "custom",
    include_flash: bool = False,
    conn=None,
) -> Tuple[List[Dict[str, Any]], int]:
    """
    Get all workspaces for a user with pagination.

    Args:
        user_id: User ID
        limit: Maximum number of results
        offset: Number of results to skip
        include_deleted: Whether to include deleted workspaces
        sort_by: Sort mode
        include_flash: Whether to include flash workspaces in results
        conn: Optional database connection to reuse

    Returns:
        Tuple of (list of workspace dicts, total count)
    """
    try:
        status_filter = "" if include_deleted else "AND status != 'deleted'"
        # Exclude flash workspaces from gallery listings unless explicitly requested
        flash_filter = "" if include_flash else "AND status != 'flash'"

        async def _execute(cur):
            # Get total count
            await cur.execute(
                f"""
                SELECT COUNT(*) as total
                FROM workspaces
                WHERE user_id = %s {status_filter} {flash_filter}
                """,
                (user_id,),
            )
            count_result = await cur.fetchone()
            total = count_result["total"] if count_result else 0

            # Build ORDER BY based on sort mode
            if sort_by == "activity":
                order_clause = "is_pinned DESC, COALESCE(last_activity_at, updated_at) DESC"
            elif sort_by == "name":
                order_clause = "is_pinned DESC, name ASC"
            else:
                # 'custom' — manual sort order, then recency
                order_clause = "is_pinned DESC, sort_order ASC, updated_at DESC"

            # Get paginated results
            await cur.execute(
                f"""
                SELECT workspace_id, user_id, name, description, sandbox_id,
                       status, created_at, updated_at, last_activity_at, stopped_at, config, artifacts,
                       is_pinned, sort_order
                FROM workspaces
                WHERE user_id = %s {status_filter} {flash_filter}
                ORDER BY {order_clause}
                LIMIT %s OFFSET %s
                """,
                (user_id, limit, offset),
            )
            results = await cur.fetchall()
            return [dict(r) for r in results], total

        if conn:
            async with conn.cursor(row_factory=dict_row) as cur:
                return await _execute(cur)
        else:
            async with get_db_connection() as conn:
                async with conn.cursor(row_factory=dict_row) as cur:
                    return await _execute(cur)

    except Exception as e:
        logger.error(f"Error getting workspaces for user {user_id}: {e}")
        raise


async def update_workspace(
    workspace_id: str,
    name: Optional[str] = None,
    description: Optional[str] = None,
    config: Optional[Dict[str, Any]] = None,
    is_pinned: Optional[bool] = None,
    conn=None,
) -> Optional[Dict[str, Any]]:
    """
    Update workspace metadata.

    Args:
        workspace_id: Workspace UUID
        name: Optional new name
        description: Optional new description
        config: Optional new config (replaces existing)
        conn: Optional database connection to reuse

    Returns:
        Updated workspace record, or None if not found
    """
    from psycopg.types.json import Json

    try:
        # Build dynamic update query
        updates = []
        params = []

        if name is not None:
            updates.append("name = %s")
            params.append(name)

        if description is not None:
            updates.append("description = %s")
            params.append(description)

        if config is not None:
            updates.append("config = %s")
            params.append(Json(config))

        if is_pinned is not None:
            updates.append("is_pinned = %s")
            params.append(is_pinned)

        if not updates:
            # Nothing to update, just return current state
            return await get_workspace(workspace_id, conn=conn)

        updates.append("updated_at = %s")
        params.append(datetime.now(timezone.utc))
        params.append(workspace_id)

        update_clause = ", ".join(updates)

        async def _execute(cur):
            await cur.execute(
                f"""
                UPDATE workspaces
                SET {update_clause}
                WHERE workspace_id = %s AND status != 'deleted'
                RETURNING workspace_id, user_id, name, description, sandbox_id,
                          status, created_at, updated_at, last_activity_at, stopped_at, config, artifacts,
                          is_pinned, sort_order
                """,
                params,
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
            logger.debug(f"Updated workspace: {workspace_id}")
            return dict(result)
        return None

    except Exception as e:
        logger.error(f"Error updating workspace {workspace_id}: {e}")
        raise


async def update_workspace_status(
    workspace_id: str,
    status: str,
    sandbox_id: Optional[str] = None,
    conn=None,
) -> Optional[Dict[str, Any]]:
    """
    Update workspace status and optionally sandbox_id.

    Args:
        workspace_id: Workspace UUID
        status: New status (creating, running, stopping, stopped, error, deleted)
        sandbox_id: Optional sandbox ID to set
        conn: Optional database connection to reuse

    Returns:
        Updated workspace record, or None if not found
    """
    try:
        now = datetime.now(timezone.utc)

        # Build update based on status
        if sandbox_id is not None:
            if status == "stopped":
                query = """
                    UPDATE workspaces
                    SET status = %s, sandbox_id = %s, updated_at = %s, stopped_at = %s
                    WHERE workspace_id = %s
                    RETURNING workspace_id, user_id, name, description, sandbox_id,
                              status, created_at, updated_at, last_activity_at, stopped_at, config, artifacts,
                              is_pinned, sort_order
                """
                params = (status, sandbox_id, now, now, workspace_id)
            else:
                query = """
                    UPDATE workspaces
                    SET status = %s, sandbox_id = %s, updated_at = %s
                    WHERE workspace_id = %s
                    RETURNING workspace_id, user_id, name, description, sandbox_id,
                              status, created_at, updated_at, last_activity_at, stopped_at, config, artifacts,
                              is_pinned, sort_order
                """
                params = (status, sandbox_id, now, workspace_id)
        else:
            if status == "stopped":
                query = """
                    UPDATE workspaces
                    SET status = %s, updated_at = %s, stopped_at = %s
                    WHERE workspace_id = %s
                    RETURNING workspace_id, user_id, name, description, sandbox_id,
                              status, created_at, updated_at, last_activity_at, stopped_at, config, artifacts,
                              is_pinned, sort_order
                """
                params = (status, now, now, workspace_id)
            else:
                query = """
                    UPDATE workspaces
                    SET status = %s, updated_at = %s
                    WHERE workspace_id = %s
                    RETURNING workspace_id, user_id, name, description, sandbox_id,
                              status, created_at, updated_at, last_activity_at, stopped_at, config, artifacts,
                              is_pinned, sort_order
                """
                params = (status, now, workspace_id)

        async def _execute(cur):
            await cur.execute(query, params)
            return await cur.fetchone()

        if conn:
            async with conn.cursor(row_factory=dict_row) as cur:
                result = await _execute(cur)
        else:
            async with get_db_connection() as conn:
                async with conn.cursor(row_factory=dict_row) as cur:
                    result = await _execute(cur)

        if result:
            logger.debug(f"Updated workspace {workspace_id} status to: {status}")
            return dict(result)
        return None

    except Exception as e:
        logger.error(f"Error updating workspace {workspace_id} status: {e}")
        raise


async def update_workspace_activity(
    workspace_id: str,
    conn=None,
) -> bool:
    """
    Update workspace last_activity_at timestamp (conditional).

    Only writes if the last update was > 60 seconds ago, avoiding a full
    UPDATE on every message.  Process-safe via SQL WHERE clause.

    Args:
        workspace_id: Workspace UUID
        conn: Optional database connection to reuse

    Returns:
        True if the row was updated, False if skipped (within cooldown)
    """
    try:
        now = datetime.now(timezone.utc)

        async def _execute(cur):
            await cur.execute(
                """
                UPDATE workspaces
                SET last_activity_at = %s, updated_at = %s
                WHERE workspace_id = %s
                  AND status != 'deleted'
                  AND (last_activity_at IS NULL
                       OR last_activity_at < %s - INTERVAL '60 seconds')
                """,
                (now, now, workspace_id, now),
            )
            return cur.rowcount > 0

        if conn:
            async with conn.cursor(row_factory=dict_row) as cur:
                result = await _execute(cur)
        else:
            async with get_db_connection() as conn:
                async with conn.cursor(row_factory=dict_row) as cur:
                    result = await _execute(cur)

        return result

    except Exception as e:
        logger.error(f"Error updating workspace {workspace_id} activity: {e}")
        raise


async def delete_workspace(
    workspace_id: str,
    hard_delete: bool = False,
    conn=None,
) -> bool:
    """
    Delete a workspace (soft delete by default).

    Args:
        workspace_id: Workspace UUID
        hard_delete: If True, permanently delete the record
        conn: Optional database connection to reuse

    Returns:
        True if deleted, False if not found
    """
    try:
        async def _execute(cur):
            if hard_delete:
                await cur.execute(
                    """
                    DELETE FROM workspaces
                    WHERE workspace_id = %s
                    RETURNING workspace_id
                    """,
                    (workspace_id,),
                )
            else:
                await cur.execute(
                    """
                    UPDATE workspaces
                    SET status = 'deleted', updated_at = %s
                    WHERE workspace_id = %s AND status != 'deleted'
                    RETURNING workspace_id
                    """,
                    (datetime.now(timezone.utc), workspace_id),
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
                f"{'Hard' if hard_delete else 'Soft'} deleted workspace: {workspace_id}"
            )
            return True
        return False

    except Exception as e:
        logger.error(f"Error deleting workspace {workspace_id}: {e}")
        raise


async def batch_update_sort_order(
    user_id: str,
    items: List[Tuple[str, int]],
    conn=None,
) -> None:
    """
    Batch-update sort_order for multiple workspaces in a single query.

    Args:
        user_id: User ID (for ownership check)
        items: List of (workspace_id, sort_order) tuples
        conn: Optional database connection to reuse
    """
    if not items:
        return

    try:
        async def _execute(cur):
            # Build VALUES list for the update
            values_parts = []
            params: list = []
            for ws_id, order in items:
                values_parts.append("(%s, %s)")
                params.extend([ws_id, order])
            values_sql = ", ".join(values_parts)
            params.append(user_id)

            await cur.execute(
                f"""
                UPDATE workspaces w
                SET sort_order = v.new_order, updated_at = NOW()
                FROM (VALUES {values_sql}) AS v(wid, new_order)
                WHERE w.workspace_id = v.wid::uuid AND w.user_id = %s
                """,
                params,
            )
            return cur.rowcount

        if conn:
            async with conn.cursor(row_factory=dict_row) as cur:
                updated = await _execute(cur)
        else:
            async with get_db_connection() as conn:
                async with conn.cursor(row_factory=dict_row) as cur:
                    updated = await _execute(cur)

        if updated == 0:
            logger.warning(f"batch_update_sort_order: 0/{len(items)} rows updated for user {user_id}")
        else:
            logger.info(f"Batch-updated sort_order for {updated}/{len(items)} workspaces (user {user_id})")

    except Exception as e:
        logger.error(f"Error batch-updating sort_order for user {user_id}: {e}")
        raise


async def get_workspaces_by_status(
    status: str,
    limit: int = 100,
    conn=None,
) -> List[Dict[str, Any]]:
    """
    Get workspaces by status (for cleanup tasks).

    Args:
        status: Status to filter by
        limit: Maximum number of results
        conn: Optional database connection to reuse

    Returns:
        List of workspace dicts
    """
    try:
        async def _execute(cur):
            await cur.execute(
                """
                SELECT workspace_id, user_id, name, description, sandbox_id,
                       status, created_at, updated_at, last_activity_at, stopped_at, config, artifacts,
                       is_pinned, sort_order
                FROM workspaces
                WHERE status = %s
                ORDER BY last_activity_at ASC NULLS FIRST
                LIMIT %s
                """,
                (status, limit),
            )
            results = await cur.fetchall()
            return [dict(r) for r in results]

        if conn:
            async with conn.cursor(row_factory=dict_row) as cur:
                return await _execute(cur)
        else:
            async with get_db_connection() as conn:
                async with conn.cursor(row_factory=dict_row) as cur:
                    return await _execute(cur)

    except Exception as e:
        logger.error(f"Error getting workspaces by status {status}: {e}")
        raise


# ---------------------------------------------------------------------------
# Preview server command persistence
# ---------------------------------------------------------------------------


async def save_preview_command(
    workspace_id: str, port: int, command: str
) -> None:
    """Store a preview server command in ``artifacts.preview_servers``."""
    try:
        async with get_db_connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    """
                    UPDATE workspaces
                    SET artifacts = jsonb_set(
                        COALESCE(artifacts, '{}'::jsonb),
                        '{preview_servers}',
                        COALESCE(artifacts->'preview_servers', '{}'::jsonb)
                            || jsonb_build_object(%s::text, %s::text),
                        true
                    )
                    WHERE workspace_id = %s
                    """,
                    (str(port), command, workspace_id),
                )
    except Exception:
        logger.debug("Failed to persist preview command", exc_info=True)


async def get_preview_command(
    workspace_id: str, port: int
) -> Optional[str]:
    """Read a preview server command from ``artifacts.preview_servers``."""
    try:
        async with get_db_connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    "SELECT artifacts->'preview_servers'->>%s FROM workspaces WHERE workspace_id = %s",
                    (str(port), workspace_id),
                )
                row = await cur.fetchone()
                return row[0] if row else None
    except Exception:
        logger.debug("Failed to read preview command", exc_info=True)
        return None
