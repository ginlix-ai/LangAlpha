"""
Database utility functions for workspace file persistence.

Provides functions for syncing workspace files between Daytona sandboxes
and PostgreSQL for offline access and disaster recovery.
"""

import logging
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

from psycopg.errors import LockNotAvailable
from psycopg.rows import dict_row

from src.server.database.pool import get_db_connection
from src.server.database.session_lock import release_session_lock
from src.server.utils.pg_sanitize import strip_pg_nul_str

logger = logging.getLogger(__name__)

_EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)

# Namespace prefix for the per-workspace manifest-sync lock, so the key space
# is this module's alone.
_SYNC_LOCK_NS = "WSFILES_SYNC"
# Longer than any healthy sync, so a waiter only gives up when the holder is
# wedged; a bounded wait keeps a stuck holder from pinning every later sync's
# pool slot behind it.
SYNC_LOCK_WAIT = "120s"


class WorkspaceSyncBusy(Exception):
    """Another sync held the workspace lock for the whole wait."""


def datetime_to_micros(value: datetime | None) -> int | None:
    """Exact integer microseconds since the epoch, or None."""
    if value is None:
        return None
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return (value - _EPOCH) // timedelta(microseconds=1)


def micros_to_datetime(micros: int) -> datetime:
    return _EPOCH + timedelta(microseconds=micros)


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
               content_hash, content_text, content_binary, blob_sha256,
               pack_sha256, pack_offset, mime_type, is_binary, permissions,
               sandbox_modified_at, kind ('file' default, 'dir', 'symlink'),
               symlink_target
        conn: Optional database connection to reuse

    Returns:
        Number of files written
    """
    if not files:
        return 0

    sql = """
        INSERT INTO workspace_files (
            workspace_id, file_path, file_name, file_size, content_hash,
            content_text, content_binary, blob_sha256, pack_sha256, pack_offset,
            mime_type, is_binary, permissions, sandbox_modified_at, kind,
            symlink_target
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (workspace_id, file_path) DO UPDATE SET
            file_name = EXCLUDED.file_name,
            file_size = EXCLUDED.file_size,
            content_hash = EXCLUDED.content_hash,
            content_text = EXCLUDED.content_text,
            content_binary = EXCLUDED.content_binary,
            blob_sha256 = EXCLUDED.blob_sha256,
            pack_sha256 = EXCLUDED.pack_sha256,
            pack_offset = EXCLUDED.pack_offset,
            mime_type = EXCLUDED.mime_type,
            is_binary = EXCLUDED.is_binary,
            permissions = EXCLUDED.permissions,
            sandbox_modified_at = EXCLUDED.sandbox_modified_at,
            kind = EXCLUDED.kind,
            symlink_target = EXCLUDED.symlink_target,
            updated_at = NOW()
    """

    # Strip NUL bytes from any string bound to a TEXT/VARCHAR column. Sandbox
    # files (and the find listings that name them) can carry `\x00` which
    # Postgres rejects with `cannot contain NUL`, killing the whole 50-row
    # transaction. content_binary is BYTEA and accepts NUL fine.
    params_list = [
        (
            workspace_id,
            strip_pg_nul_str(f["file_path"]),
            strip_pg_nul_str(f["file_name"]),
            f["file_size"],
            f.get("content_hash"),
            strip_pg_nul_str(f.get("content_text")),
            f.get("content_binary"),
            f.get("blob_sha256"),
            f.get("pack_sha256"),
            f.get("pack_offset"),
            strip_pg_nul_str(f.get("mime_type")),
            f.get("is_binary", False),
            strip_pg_nul_str(f.get("permissions")),
            f.get("sandbox_modified_at"),
            f.get("kind", "file"),
            strip_pg_nul_str(f.get("symlink_target")),
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


async def get_files_for_workspace(
    workspace_id: str,
    *,
    include_content: bool = False,
    all_kinds: bool = False,
    conn=None,
) -> List[Dict[str, Any]]:
    """
    Get all files for a workspace.

    By default returns metadata only (no content_text/content_binary) for
    efficient listing. Set include_content=True to include file contents.

    Only ``kind = 'file'`` rows are returned unless ``all_kinds`` is set:
    directory and symlink rows exist for restore, and every other reader
    (file tree, serving, redaction) is written for files. Restore is the one
    caller that asks for everything.

    Args:
        workspace_id: Workspace UUID
        include_content: Whether to include content_text and content_binary
        all_kinds: Include directory and symlink rows
        conn: Optional database connection to reuse

    Returns:
        List of file records as dicts, ordered by file_path ASC
    """
    try:
        if include_content:
            columns = """
                workspace_file_id, workspace_id, file_path, file_name,
                file_size, content_hash, content_text, content_binary,
                blob_sha256, pack_sha256, pack_offset, mime_type, is_binary, permissions,
                sandbox_modified_at, created_at, updated_at,
                kind, symlink_target
            """
        else:
            columns = """
                workspace_file_id, workspace_id, file_path, file_name,
                file_size, content_hash, mime_type, is_binary, permissions,
                sandbox_modified_at, created_at, updated_at,
                kind, symlink_target
            """

        kind_filter = "" if all_kinds else "AND kind = 'file'"

        async def _execute(cur):
            await cur.execute(
                f"""
                SELECT {columns}
                FROM workspace_files
                WHERE workspace_id = %s {kind_filter}
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
                blob_sha256, pack_sha256, pack_offset, mime_type, is_binary, permissions,
                sandbox_modified_at, created_at, updated_at,
                kind, symlink_target
            """
        else:
            columns = """
                workspace_file_id, workspace_id, file_path, file_name,
                file_size, content_hash, mime_type, is_binary, permissions,
                sandbox_modified_at, created_at, updated_at,
                kind, symlink_target
            """

        async def _execute(cur):
            await cur.execute(
                f"""
                SELECT {columns}
                FROM workspace_files
                WHERE workspace_id = %s AND file_path = %s AND kind = 'file'
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


async def get_file_metadata_for_sync(
    workspace_id: str,
    *,
    conn=None,
) -> Dict[str, Dict[str, Any]]:
    """
    Get file metadata for incremental sync comparison.

    Returns dict mapping file_path to {content_hash, file_size, mtime_epoch,
    mtime_ns, kind, permissions, symlink_target, blob_sha256, is_binary,
    mime_type}. Every kind is
    included: the sync diff has to see directory and symlink rows to know
    whether they still exist.

    ``mtime_ns`` is derived from the stored microsecond timestamp, so it
    equals the nanosecond value the sandbox reports only when the file's mtime
    was itself set from this row (restore does that) or the filesystem keeps
    no finer precision. Either way an exact match means unchanged.
    """
    try:

        async def _execute(cur):
            await cur.execute(
                """
                SELECT file_path, content_hash, file_size, kind, permissions,
                       symlink_target, blob_sha256, pack_sha256, pack_offset,
                       is_binary, mime_type, sandbox_modified_at
                FROM workspace_files
                WHERE workspace_id = %s
                """,
                (workspace_id,),
            )
            results = await cur.fetchall()
            out = {}
            for row in results:
                modified = row["sandbox_modified_at"]
                # Integer microseconds straight off the datetime: a float
                # epoch drops the last digit on about one row in twenty,
                # which read as "changed" and rewrote the row every sync.
                micros = datetime_to_micros(modified)
                out[row["file_path"]] = {
                    "content_hash": row["content_hash"],
                    "file_size": row["file_size"],
                    "mtime_epoch": micros / 1_000_000 if micros is not None else None,
                    "mtime_ns": micros * 1000 if micros is not None else None,
                    "kind": row["kind"],
                    "permissions": row["permissions"],
                    "symlink_target": row["symlink_target"],
                    "blob_sha256": row["blob_sha256"],
                    "pack_sha256": row["pack_sha256"],
                    "pack_offset": row["pack_offset"],
                    "is_binary": row["is_binary"],
                    "mime_type": row["mime_type"],
                }
            return out

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


async def bulk_update_file_stamps(
    workspace_id: str,
    updates: List[tuple],
    *,
    conn=None,
) -> int:
    """
    Bulk update the mtime and mode of files whose bytes did not change.

    Args:
        workspace_id: Workspace UUID
        updates: List of (file_path, sandbox_modified_at, permissions) tuples
        conn: Optional database connection to reuse

    Returns:
        Number of rows updated
    """
    if not updates:
        return 0

    sql = """
        UPDATE workspace_files
        SET sandbox_modified_at = %s, permissions = %s, updated_at = NOW()
        WHERE workspace_id = %s AND file_path = %s
    """

    params_list = [
        (mtime, permissions, workspace_id, fpath)
        for fpath, mtime, permissions in updates
    ]

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


async def manifest_clock(*, conn=None) -> datetime:
    """The database's own ``now()``: the reference every ``updated_at`` uses."""
    async with get_db_connection(conn) as c:
        async with c.cursor() as cur:
            await cur.execute("SELECT now()")
            row = await cur.fetchone()
    return row[0]


@asynccontextmanager
async def workspace_sync_lock(workspace_id: str):
    """Serialize manifest syncs for one workspace, and yield the session holding it.

    Session-level rather than transaction-level because a sync is not one
    statement: it reads the manifest, walks the sandbox, moves bytes, and only
    then writes. Two overlapping syncs that each read before either wrote let
    the older pack's unconditional upsert land last, so the fence has to span
    the read and the write together. The connection is yielded because the
    sync's own reads and writes belong on this session rather than on a second
    pool slot checked out underneath it.
    """
    key = f"{_SYNC_LOCK_NS}:{workspace_id}"
    async with get_db_connection() as conn:
        try:
            # SET LOCAL scopes the timeout to this transaction; the session
            # lock itself outlives the commit.
            async with conn.transaction():
                async with conn.cursor() as cur:
                    await cur.execute(f"SET LOCAL lock_timeout = '{SYNC_LOCK_WAIT}'")
                    await cur.execute(
                        "SELECT pg_advisory_lock(hashtextextended(%s, 0))", (key,)
                    )
        except LockNotAvailable as e:
            raise WorkspaceSyncBusy(
                f"workspace {workspace_id} sync lock held for over {SYNC_LOCK_WAIT}"
            ) from e
        except BaseException:
            # A cancellation or failure while the grant may already have
            # landed: the transaction rolls back but a session lock does
            # not, so the session is released the way a held one is rather
            # than handed to the pool with the lock still on it.
            await release_session_lock(conn, key)
            raise
        try:
            yield conn
        finally:
            await release_session_lock(conn, key)


async def delete_removed_files(
    workspace_id: str,
    active_paths: set,
    *,
    untouched_since: datetime,
    conn=None,
) -> int:
    """
    Delete files that are no longer present in the sandbox.

    Removes all workspace_files rows whose file_path is NOT in active_paths.

    ``untouched_since`` fences the prune to rows nobody has written since the
    caller's scan began: two syncs may overlap (a post-turn backup and a stop,
    say), and the older scan must not delete a row the newer one just added.
    It is required rather than optional because an unfenced prune against an
    empty ``active_paths`` erases the whole manifest.

    Args:
        workspace_id: Workspace UUID
        active_paths: Set of file paths that still exist in the sandbox
        untouched_since: Only delete rows with ``updated_at`` before this
        conn: Optional database connection to reuse

    Returns:
        Number of deleted rows
    """
    try:
        paths_list = list(active_paths)

        async def _execute(cur):
            await cur.execute(
                """
                DELETE FROM workspace_files
                WHERE workspace_id = %s
                  AND NOT (file_path = ANY(%s::text[]))
                  AND (%s::timestamptz IS NULL OR updated_at < %s::timestamptz)
                """,
                (workspace_id, paths_list, untouched_since, untouched_since),
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


async def delete_file_rows(
    workspace_id: str, paths: list[str], *, conn=None
) -> int:
    """Delete the named manifest rows. Unlike ``delete_removed_files`` this
    is unfenced: the caller has positive evidence each path is gone."""
    if not paths:
        return 0

    async def _execute(cur):
        await cur.execute(
            """
            DELETE FROM workspace_files
            WHERE workspace_id = %s AND file_path = ANY(%s::text[])
            """,
            (workspace_id, paths),
        )
        return cur.rowcount

    if conn:
        async with conn.cursor() as cur:
            return await _execute(cur)
    async with get_db_connection() as conn:
        async with conn.cursor() as cur:
            return await _execute(cur)


async def copy_workspace_files(
    source_id: str,
    dest_id: str,
    *,
    conn=None,
) -> int:
    """
    Copy all files from one workspace to another.

    Inserts fresh rows for dest_id mirroring every file under source_id.
    Primary keys and created_at/updated_at use their DB defaults (new uuid,
    NOW()); all content columns are carried over.

    ``blob_sha256`` is copied rather than dereferenced: the fork points at the
    same content-addressed objects, so forking a large workspace no longer
    duplicates its TOASTed bytes. Shared pointers are also why per-workspace
    object deletion is unsafe by construction.

    Args:
        source_id: Workspace UUID to copy files from
        dest_id: Workspace UUID to copy files into
        conn: Optional database connection to reuse

    Returns:
        Number of rows inserted
    """
    try:

        async def _execute(cur):
            await cur.execute(
                """
                INSERT INTO workspace_files (
                    workspace_id, file_path, file_name, file_size,
                    content_hash, content_text, content_binary, blob_sha256,
                    pack_sha256, pack_offset, mime_type, is_binary, permissions,
                    sandbox_modified_at, kind, symlink_target
                )
                SELECT
                    %s, file_path, file_name, file_size,
                    content_hash, content_text, content_binary, blob_sha256,
                    pack_sha256, pack_offset, mime_type, is_binary, permissions,
                    sandbox_modified_at, kind, symlink_target
                FROM workspace_files
                WHERE workspace_id = %s
                """,
                (dest_id, source_id),
            )
            return cur.rowcount

        if conn:
            async with conn.cursor(row_factory=dict_row) as cur:
                copied = await _execute(cur)
        else:
            async with get_db_connection() as conn:
                async with conn.cursor(row_factory=dict_row) as cur:
                    copied = await _execute(cur)

        if copied:
            logger.info(
                f"Copied {copied} files from workspace {source_id} to {dest_id}"
            )
        return copied

    except Exception as e:
        logger.error(
            f"Error copying files from workspace {source_id} to {dest_id}: {e}"
        )
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
