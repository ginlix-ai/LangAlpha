"""
Database utility functions for workspace file persistence.

Provides functions for syncing workspace files between Daytona sandboxes
and PostgreSQL for offline access and disaster recovery.
"""

import logging
from datetime import datetime
from typing import Any, Dict, List, Optional

from psycopg.rows import dict_row

from src.server.database.conversation import get_db_connection

logger = logging.getLogger(__name__)


# =============================================================================
# Workspace File Operations
# =============================================================================


async def bulk_upsert_files(
    workspace_id: str,
    files: List[Dict[str, Any]],
    *,
    conn=None,
) -> int:
    """
    Bulk insert or update workspace files.

    Uses executemany with ON CONFLICT for efficient batch upserts.
    Sub-batches at 50 rows per executemany call to limit memory.

    Args:
        workspace_id: Workspace UUID
        files: List of dicts with keys: file_path, file_name, file_size,
               content_hash, content_text, content_binary, mime_type,
               is_binary, permissions, sandbox_modified_at
        conn: Optional database connection to reuse

    Returns:
        Number of files written
    """
    if not files:
        return 0

    sql = """
        INSERT INTO workspace_files (
            workspace_id, file_path, file_name, file_size, content_hash,
            content_text, content_binary, mime_type, is_binary, permissions,
            sandbox_modified_at
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (workspace_id, file_path) DO UPDATE SET
            file_name = EXCLUDED.file_name,
            file_size = EXCLUDED.file_size,
            content_hash = EXCLUDED.content_hash,
            content_text = EXCLUDED.content_text,
            content_binary = EXCLUDED.content_binary,
            mime_type = EXCLUDED.mime_type,
            is_binary = EXCLUDED.is_binary,
            permissions = EXCLUDED.permissions,
            sandbox_modified_at = EXCLUDED.sandbox_modified_at,
            updated_at = NOW()
    """

    params_list = [
        (
            workspace_id,
            f["file_path"],
            f["file_name"],
            f["file_size"],
            f.get("content_hash"),
            f.get("content_text"),
            f.get("content_binary"),
            f.get("mime_type"),
            f.get("is_binary", False),
            f.get("permissions"),
            f.get("sandbox_modified_at"),
        )
        for f in files
    ]

    sub_batch_size = 50
    count = 0

    try:

        async def _execute(c):
            nonlocal count
            async with c.transaction():
                async with c.cursor() as cur:
                    for i in range(0, len(params_list), sub_batch_size):
                        batch = params_list[i : i + sub_batch_size]
                        await cur.executemany(sql, batch)
                        count += len(batch)

        if conn:
            await _execute(conn)
        else:
            async with get_db_connection() as c:
                await _execute(c)

        logger.debug(
            f"Bulk upserted {count} files for workspace {workspace_id}"
        )
        return count

    except Exception as e:
        logger.error(
            f"Error bulk upserting files for workspace {workspace_id}: {e}"
        )
        raise


async def upsert_file(
    workspace_id: str,
    file_path: str,
    file_name: str,
    file_size: int,
    content_hash: Optional[str] = None,
    content_text: Optional[str] = None,
    content_binary: Optional[bytes] = None,
    mime_type: Optional[str] = None,
    is_binary: bool = False,
    permissions: Optional[str] = None,
    sandbox_modified_at: Optional[datetime] = None,
    *,
    conn=None,
) -> None:
    """Insert or update a single workspace file. Delegates to bulk_upsert_files."""
    await bulk_upsert_files(
        workspace_id,
        [
            {
                "file_path": file_path,
                "file_name": file_name,
                "file_size": file_size,
                "content_hash": content_hash,
                "content_text": content_text,
                "content_binary": content_binary,
                "mime_type": mime_type,
                "is_binary": is_binary,
                "permissions": permissions,
                "sandbox_modified_at": sandbox_modified_at,
            }
        ],
        conn=conn,
    )


async def get_files_for_workspace(
    workspace_id: str,
    *,
    include_content: bool = False,
    conn=None,
) -> List[Dict[str, Any]]:
    """
    Get all files for a workspace.

    By default returns metadata only (no content_text/content_binary) for
    efficient listing. Set include_content=True to include file contents.

    Args:
        workspace_id: Workspace UUID
        include_content: Whether to include content_text and content_binary
        conn: Optional database connection to reuse

    Returns:
        List of file records as dicts, ordered by file_path ASC
    """
    try:
        if include_content:
            columns = """
                workspace_file_id, workspace_id, file_path, file_name,
                file_size, content_hash, content_text, content_binary,
                mime_type, is_binary, permissions, sandbox_modified_at,
                created_at, updated_at
            """
        else:
            columns = """
                workspace_file_id, workspace_id, file_path, file_name,
                file_size, content_hash, mime_type, is_binary, permissions,
                sandbox_modified_at, created_at, updated_at
            """

        async def _execute(cur):
            await cur.execute(
                f"""
                SELECT {columns}
                FROM workspace_files
                WHERE workspace_id = %s
                ORDER BY file_path ASC
                """,
                (workspace_id,),
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
        logger.error(f"Error getting files for workspace {workspace_id}: {e}")
        raise


async def get_file(
    workspace_id: str,
    file_path: str,
    *,
    include_content: bool = True,
    conn=None,
) -> Optional[Dict[str, Any]]:
    """
    Get a single file by workspace_id and file_path.

    Args:
        workspace_id: Workspace UUID
        file_path: Full path of the file within the workspace
        include_content: Whether to include content_text and content_binary (default True)
        conn: Optional database connection to reuse

    Returns:
        File record as dict, or None if not found
    """
    try:
        if include_content:
            columns = """
                workspace_file_id, workspace_id, file_path, file_name,
                file_size, content_hash, content_text, content_binary,
                mime_type, is_binary, permissions, sandbox_modified_at,
                created_at, updated_at
            """
        else:
            columns = """
                workspace_file_id, workspace_id, file_path, file_name,
                file_size, content_hash, mime_type, is_binary, permissions,
                sandbox_modified_at, created_at, updated_at
            """

        async def _execute(cur):
            await cur.execute(
                f"""
                SELECT {columns}
                FROM workspace_files
                WHERE workspace_id = %s AND file_path = %s
                """,
                (workspace_id, file_path),
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
        logger.error(
            f"Error getting file {file_path} for workspace {workspace_id}: {e}"
        )
        raise


async def get_file_hashes(
    workspace_id: str,
    *,
    conn=None,
) -> Dict[str, str]:
    """
    Get content hashes for all files in a workspace.

    Useful for diffing local sandbox state against stored state to determine
    which files need to be synced.

    Args:
        workspace_id: Workspace UUID
        conn: Optional database connection to reuse

    Returns:
        Dict mapping file_path to content_hash
    """
    try:

        async def _execute(cur):
            await cur.execute(
                """
                SELECT file_path, content_hash
                FROM workspace_files
                WHERE workspace_id = %s
                """,
                (workspace_id,),
            )
            results = await cur.fetchall()
            return {row["file_path"]: row["content_hash"] for row in results}

        if conn:
            async with conn.cursor(row_factory=dict_row) as cur:
                return await _execute(cur)
        else:
            async with get_db_connection() as conn:
                async with conn.cursor(row_factory=dict_row) as cur:
                    return await _execute(cur)

    except Exception as e:
        logger.error(f"Error getting file hashes for workspace {workspace_id}: {e}")
        raise


async def get_file_metadata_for_sync(
    workspace_id: str,
    *,
    conn=None,
) -> Dict[str, Dict[str, Any]]:
    """
    Get file metadata for incremental sync comparison.

    Returns dict mapping file_path to {content_hash, file_size, mtime_epoch}.
    Used to pre-filter unchanged files before downloading.
    """
    try:

        async def _execute(cur):
            await cur.execute(
                """
                SELECT file_path, content_hash, file_size,
                       EXTRACT(EPOCH FROM sandbox_modified_at) AS mtime_epoch
                FROM workspace_files
                WHERE workspace_id = %s
                """,
                (workspace_id,),
            )
            results = await cur.fetchall()
            return {
                row["file_path"]: {
                    "content_hash": row["content_hash"],
                    "file_size": row["file_size"],
                    "mtime_epoch": float(row["mtime_epoch"])
                    if row["mtime_epoch"] is not None
                    else None,
                }
                for row in results
            }

        if conn:
            async with conn.cursor(row_factory=dict_row) as cur:
                return await _execute(cur)
        else:
            async with get_db_connection() as conn:
                async with conn.cursor(row_factory=dict_row) as cur:
                    return await _execute(cur)

    except Exception as e:
        logger.error(f"Error getting file metadata for workspace {workspace_id}: {e}")
        raise


async def bulk_update_file_mtimes(
    workspace_id: str,
    updates: List[tuple],
    *,
    conn=None,
) -> int:
    """
    Bulk update sandbox_modified_at for multiple files.

    Args:
        workspace_id: Workspace UUID
        updates: List of (file_path, sandbox_modified_at) tuples
        conn: Optional database connection to reuse

    Returns:
        Number of rows updated
    """
    if not updates:
        return 0

    sql = """
        UPDATE workspace_files
        SET sandbox_modified_at = %s, updated_at = NOW()
        WHERE workspace_id = %s AND file_path = %s
    """

    params_list = [
        (mtime, workspace_id, fpath) for fpath, mtime in updates
    ]

    try:

        async def _execute(c):
            async with c.transaction():
                async with c.cursor() as cur:
                    await cur.executemany(sql, params_list)

        if conn:
            await _execute(conn)
        else:
            async with get_db_connection() as c:
                await _execute(c)

        logger.debug(
            f"Bulk updated mtimes for {len(updates)} files in workspace {workspace_id}"
        )
        return len(updates)

    except Exception as e:
        logger.warning(
            f"Error bulk updating mtimes for workspace {workspace_id}: {e}"
        )
        return 0


async def update_file_mtime(
    workspace_id: str,
    file_path: str,
    sandbox_modified_at: datetime,
) -> None:
    """Update only the sandbox_modified_at for a file. Delegates to bulk_update_file_mtimes."""
    await bulk_update_file_mtimes(
        workspace_id, [(file_path, sandbox_modified_at)]
    )


async def delete_removed_files(
    workspace_id: str,
    active_paths: set,
    *,
    conn=None,
) -> int:
    """
    Delete files that are no longer present in the sandbox.

    Removes all workspace_files rows whose file_path is NOT in active_paths.
    If active_paths is empty, deletes all files (equivalent to delete_all_files).

    Args:
        workspace_id: Workspace UUID
        active_paths: Set of file paths that still exist in the sandbox
        conn: Optional database connection to reuse

    Returns:
        Number of deleted rows
    """
    try:
        if not active_paths:
            return await delete_all_files(workspace_id, conn=conn)

        paths_list = list(active_paths)

        async def _execute(cur):
            await cur.execute(
                """
                DELETE FROM workspace_files
                WHERE workspace_id = %s AND NOT (file_path = ANY(%s))
                """,
                (workspace_id, paths_list),
            )
            return cur.rowcount

        if conn:
            async with conn.cursor(row_factory=dict_row) as cur:
                deleted = await _execute(cur)
        else:
            async with get_db_connection() as conn:
                async with conn.cursor(row_factory=dict_row) as cur:
                    deleted = await _execute(cur)

        if deleted:
            logger.info(f"Deleted {deleted} removed files for workspace {workspace_id}")
        return deleted

    except Exception as e:
        logger.error(f"Error deleting removed files for workspace {workspace_id}: {e}")
        raise


async def delete_all_files(
    workspace_id: str,
    *,
    conn=None,
) -> int:
    """
    Delete all files for a workspace.

    Args:
        workspace_id: Workspace UUID
        conn: Optional database connection to reuse

    Returns:
        Number of deleted rows
    """
    try:

        async def _execute(cur):
            await cur.execute(
                """
                DELETE FROM workspace_files
                WHERE workspace_id = %s
                """,
                (workspace_id,),
            )
            return cur.rowcount

        if conn:
            async with conn.cursor(row_factory=dict_row) as cur:
                deleted = await _execute(cur)
        else:
            async with get_db_connection() as conn:
                async with conn.cursor(row_factory=dict_row) as cur:
                    deleted = await _execute(cur)

        if deleted:
            logger.info(f"Deleted all {deleted} files for workspace {workspace_id}")
        return deleted

    except Exception as e:
        logger.error(f"Error deleting all files for workspace {workspace_id}: {e}")
        raise


async def get_workspace_total_size(
    workspace_id: str,
    *,
    conn=None,
) -> int:
    """
    Get total file size for all files in a workspace.

    Args:
        workspace_id: Workspace UUID
        conn: Optional database connection to reuse

    Returns:
        Total size in bytes
    """
    try:

        async def _execute(cur):
            await cur.execute(
                """
                SELECT COALESCE(SUM(file_size), 0) AS total_size
                FROM workspace_files
                WHERE workspace_id = %s
                """,
                (workspace_id,),
            )
            result = await cur.fetchone()
            return result["total_size"]

        if conn:
            async with conn.cursor(row_factory=dict_row) as cur:
                return await _execute(cur)
        else:
            async with get_db_connection() as conn:
                async with conn.cursor(row_factory=dict_row) as cur:
                    return await _execute(cur)

    except Exception as e:
        logger.error(f"Error getting total size for workspace {workspace_id}: {e}")
        raise
