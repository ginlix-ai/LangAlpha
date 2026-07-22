"""Read models over conversation_threads: lookups, listings, auth metadata."""

import logging
from typing import Optional, List, Dict, Any, Tuple

from psycopg.rows import dict_row

from src.server.database import pool
from src.server.database.conversation import _sql
from src.server.utils.pg_sanitize import normalize_uuid

logger = logging.getLogger(__name__)


def _like_escape(value: str) -> str:
    """Escape LIKE wildcards so caller-supplied prefixes match literally.

    Use with `ESCAPE '\\'` in the LIKE clause so `_` and `%` in the prefix
    (e.g. `market_view`) bind to themselves instead of matching any character.
    """
    return value.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")


async def lookup_thread_by_external_id(
    platform: str, external_id: str, user_id: str
) -> Optional[str]:
    """Look up thread_id by platform + external_id, scoped to user's workspaces.

    Returns the conversation_thread_id if found, None otherwise.
    """
    try:
        async with pool.get_db_connection() as conn:
            async with conn.cursor(row_factory=dict_row) as cur:
                await cur.execute(
                    """
                    SELECT ct.conversation_thread_id
                    FROM conversation_threads ct
                    JOIN workspaces w ON ct.workspace_id = w.workspace_id
                    WHERE ct.platform = %s
                      AND ct.external_id = %s
                      AND w.user_id = %s
                    ORDER BY ct.updated_at DESC
                    LIMIT 1
                """,
                    (platform, external_id, user_id),
                )
                result = await cur.fetchone()
                if result:
                    thread_id = str(result["conversation_thread_id"])
                    logger.info(
                        f"[conversation_db] lookup_thread_by_external_id "
                        f"platform={platform} external_id={external_id} -> {thread_id}"
                    )
                    return thread_id
                return None
    except Exception as e:
        logger.error(f"Error looking up thread by external_id: {e}")
        return None


async def get_thread_checkpoint_id(conversation_thread_id: str) -> str | None:
    """Get the latest checkpoint ID stored for a thread."""
    try:
        async with pool.get_db_connection() as conn:
            async with conn.cursor(row_factory=dict_row) as cur:
                await cur.execute(
                    "SELECT latest_checkpoint_id FROM conversation_threads WHERE conversation_thread_id = %s",
                    (conversation_thread_id,),
                )
                row = await cur.fetchone()
                return row["latest_checkpoint_id"] if row else None
    except Exception as e:
        logger.error(f"Error getting thread checkpoint_id: {e}")
        return None


async def get_workspace_threads(
    workspace_id: str,
    limit: int = 20,
    offset: int = 0,
    sort_by: str = "updated_at",
    sort_order: str = "desc",
    platform_prefix: Optional[str] = None,
) -> Tuple[List[Dict[str, Any]], int]:
    """Get threads for a workspace with pagination.

    `platform_prefix`: if set, restricts to rows where `platform` LIKE
    '<prefix>%' (e.g. "market_view" matches "market_view:AAPL" and any future
    "market_view:*" suffixes). Sargable on Postgres btree, but after the
    workspace_id filter this is a tiny scan in practice.
    """
    # Validate sort parameters
    valid_sort_fields = ["created_at", "updated_at", "thread_index"]
    if sort_by not in valid_sort_fields:
        sort_by = "updated_at"

    if sort_order.lower() not in ["asc", "desc"]:
        sort_order = "desc"

    where_extra = ""
    extra_params: List[Any] = []
    if platform_prefix:
        where_extra = " AND platform LIKE %s ESCAPE '\\'"
        extra_params.append(f"{_like_escape(platform_prefix)}%")

    try:
        async with pool.get_db_connection() as conn:
            async with conn.cursor(row_factory=dict_row) as cur:
                # Get total count
                await cur.execute(
                    f"""
                    SELECT COUNT(*) as total
                    FROM conversation_threads
                    WHERE workspace_id = %s{where_extra}
                """,
                    (workspace_id, *extra_params),
                )

                total_result = await cur.fetchone()
                total_count = total_result["total"]

                # Get threads
                query = f"""
                    SELECT
                        conversation_thread_id, workspace_id, current_status, msg_type, thread_index,
                        title, platform, is_shared, created_at, updated_at
                    FROM conversation_threads
                    WHERE workspace_id = %s{where_extra}
                    ORDER BY {sort_by} {sort_order.upper()}
                    LIMIT %s OFFSET %s
                """
                await cur.execute(query, (workspace_id, *extra_params, limit, offset))

                threads = await cur.fetchall()
                return [dict(row) for row in threads], total_count

    except Exception as e:
        logger.error(f"Error getting threads for workspace: {e}")
        raise


async def get_threads_for_user(
    user_id: str,
    limit: int = 20,
    offset: int = 0,
    sort_by: str = "updated_at",
    sort_order: str = "desc",
    platform_prefix: Optional[str] = None,
) -> Tuple[List[Dict[str, Any]], int]:
    """Get all threads for a user across all workspaces.

    `platform_prefix`: optional prefix match on `platform` (e.g. "market_view").
    """
    sort_fields = {
        "created_at": "t.created_at",
        "updated_at": "t.updated_at",
        "thread_index": "t.thread_index",
    }
    if sort_by not in sort_fields:
        sort_by = "updated_at"

    if sort_order.lower() not in ["asc", "desc"]:
        sort_order = "desc"

    order_by = sort_fields[sort_by]

    where_extra = ""
    extra_params: List[Any] = []
    if platform_prefix:
        where_extra = " AND t.platform LIKE %s ESCAPE '\\'"
        extra_params.append(f"{_like_escape(platform_prefix)}%")

    try:
        async with pool.get_db_connection() as conn:
            async with conn.cursor(row_factory=dict_row) as cur:
                await cur.execute(
                    f"""
                    SELECT COUNT(*) as total
                    FROM conversation_threads t
                    JOIN workspaces w ON t.workspace_id = w.workspace_id
                    WHERE w.user_id = %s AND w.status != 'deleted'{where_extra}
                    """,
                    (user_id, *extra_params),
                )
                total_result = await cur.fetchone()
                total_count = total_result["total"] if total_result else 0

                query = f"""
                    SELECT
                        t.conversation_thread_id, t.workspace_id, t.current_status, t.msg_type, t.thread_index,
                        t.title, t.platform, t.is_shared, t.created_at, t.updated_at,
                        fq.content AS first_query_content
                    FROM conversation_threads t
                    JOIN workspaces w ON t.workspace_id = w.workspace_id
                    LEFT JOIN LATERAL (
                        SELECT q.content
                        FROM conversation_queries q
                        WHERE q.conversation_thread_id = t.conversation_thread_id
                        ORDER BY q.turn_index ASC
                        LIMIT 1
                    ) fq ON TRUE
                    WHERE w.user_id = %s AND w.status != 'deleted'{where_extra}
                    ORDER BY {order_by} {sort_order.upper()}
                    LIMIT %s OFFSET %s
                """
                await cur.execute(query, (user_id, *extra_params, limit, offset))
                threads = await cur.fetchall()
                return [dict(row) for row in threads], total_count

    except Exception as e:
        logger.error(f"Error getting threads for user: {e}")
        raise


async def get_thread_with_summary(
    conversation_thread_id: str,
) -> Optional[Dict[str, Any]]:
    """Get thread with enriched summary data (pair count, costs, etc.)."""
    try:
        async with pool.get_db_connection() as conn:
            async with conn.cursor(row_factory=dict_row) as cur:
                # Get thread basic info
                await cur.execute(
                    """
                    SELECT conversation_thread_id, workspace_id, current_status, thread_index, created_at, updated_at
                    FROM conversation_threads
                    WHERE conversation_thread_id = %s
                """,
                    (conversation_thread_id,),
                )

                thread = await cur.fetchone()
                if not thread:
                    return None

                thread = dict(thread)

                # Aggregates: pair/time/error over the settled attempt per
                # turn (superseded attempts must not inflate them), but cost
                # over ALL usage rows — spend on failed attempts is real.
                await cur.execute(
                    f"""
                    SELECT
                        COUNT(q.turn_index) as pair_count,
                        (SELECT COALESCE(SUM((u.token_usage->>'total_cost')::float), 0)
                         FROM conversation_usages u
                         WHERE u.conversation_thread_id = %s) as total_cost,
                        COALESCE(SUM(r.execution_time), 0) as total_execution_time,
                        MAX(q.type) as last_query_type,
                        BOOL_OR(COALESCE(array_length(r.errors, 1), 0) > 0) as has_errors
                    FROM conversation_queries q
                    LEFT JOIN ({_sql._SETTLED_ATTEMPTS}) r ON q.turn_index = r.turn_index
                    WHERE q.conversation_thread_id = %s
                """,
                    (
                        conversation_thread_id,
                        conversation_thread_id,
                        conversation_thread_id,
                    ),
                )

                stats = await cur.fetchone()
                if stats:
                    thread.update(dict(stats))

                return thread

    except Exception as e:
        logger.error(f"Error getting thread with summary: {e}")
        raise


async def get_thread_by_id(conversation_thread_id: str) -> Optional[Dict[str, Any]]:
    """
    Get thread by ID.

    Args:
        conversation_thread_id: Thread ID

    Returns:
        Thread dict or None if not found
    """
    conversation_thread_id = normalize_uuid(conversation_thread_id)
    if conversation_thread_id is None:
        return None

    try:
        async with pool.get_db_connection() as conn:
            async with conn.cursor(row_factory=dict_row) as cur:
                await cur.execute(
                    """
                    SELECT conversation_thread_id, workspace_id, current_status,
                           msg_type, thread_index, title,
                           share_token, is_shared, share_permissions, shared_at,
                           created_at, updated_at
                    FROM conversation_threads
                    WHERE conversation_thread_id = %s
                """,
                    (conversation_thread_id,),
                )

                result = await cur.fetchone()
                return dict(result) if result else None

    except Exception as e:
        logger.error(f"Error getting thread by id: {e}")
        raise


async def get_thread_owner_id(thread_id: str) -> Optional[str]:
    """Return the user_id that owns the thread's workspace, or None if not found.

    Delegates to ``get_thread_auth_meta`` (the superset query) to avoid a
    near-duplicate JOIN; UUID normalization / not-found handling live there.
    """
    meta = await get_thread_auth_meta(thread_id)
    return meta["user_id"] if meta else None


async def get_thread_auth_meta(thread_id: str) -> Optional[Dict[str, Any]]:
    """Owner ``user_id`` + ``is_shared`` + ``msg_type`` in one query.

    Lets ``/status`` authorize the caller, read share state, and pick the
    report-back read model (flash watch set vs PTC task outbox) from a
    single round-trip. Returns ``None`` if the thread doesn't exist.
    """
    # Same UUID normalization as get_thread_owner_id: a non-UUID id can't match
    # the column, so treat it as not-found (clean 404) rather than risk a 500.
    thread_id = normalize_uuid(thread_id)
    if thread_id is None:
        return None
    try:
        async with pool.get_db_connection() as conn:
            async with conn.cursor(row_factory=dict_row) as cur:
                await cur.execute(
                    """
                    SELECT w.user_id, t.is_shared, t.msg_type
                    FROM conversation_threads t
                    JOIN workspaces w ON w.workspace_id = t.workspace_id
                    WHERE t.conversation_thread_id = %s
                    """,
                    (thread_id,),
                )
                result = await cur.fetchone()
                return dict(result) if result else None
    except Exception as e:
        logger.error(f"Error getting thread auth meta: {e}")
        raise


async def get_thread_by_share_token(share_token: str) -> Optional[Dict[str, Any]]:
    """
    Get a shared thread by its public share token.

    Returns thread info + workspace_id + workspace name only if is_shared = TRUE.
    """
    try:
        async with pool.get_db_connection() as conn:
            async with conn.cursor(row_factory=dict_row) as cur:
                await cur.execute(
                    """
                    SELECT
                        t.conversation_thread_id,
                        t.workspace_id,
                        t.current_status,
                        t.msg_type,
                        t.title,
                        t.share_token,
                        t.is_shared,
                        t.share_permissions,
                        t.shared_at,
                        t.created_at,
                        t.updated_at,
                        w.name AS workspace_name
                    FROM conversation_threads t
                    JOIN workspaces w ON w.workspace_id = t.workspace_id
                    WHERE t.share_token = %s AND t.is_shared = TRUE
                """,
                    (share_token,),
                )

                result = await cur.fetchone()
                return dict(result) if result else None

    except Exception as e:
        logger.error(f"Error getting thread by share token: {e}")
        raise
