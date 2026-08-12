"""conversation_queries: turn allocation and the idempotent user-message write."""

import logging
from datetime import datetime, timezone
from typing import Optional, List, Dict, Any, Tuple

from psycopg.rows import dict_row

from src.server.database import pool
from src.server.database.conversation import errors
from src.server.utils.pg_sanitize import SafeJson, normalize_uuid, strip_pg_nul_str

logger = logging.getLogger(__name__)


async def get_latest_turn_index(conversation_thread_id: str) -> Optional[int]:
    """Highest persisted turn_index for a thread; None when it has no turns.

    Read failures also return None — callers treat it as "no signal", so a DB
    blip degrades to the pre-signal behavior instead of failing the status read.
    """
    conversation_thread_id = normalize_uuid(conversation_thread_id)
    if not conversation_thread_id:
        return None
    try:
        async with pool.get_db_connection() as conn:
            async with conn.cursor(row_factory=dict_row) as cur:
                await cur.execute(
                    """
                    SELECT MAX(turn_index) AS latest_turn_index
                    FROM conversation_queries
                    WHERE conversation_thread_id = %s
                """,
                    (conversation_thread_id,),
                )
                result = await cur.fetchone()
                return result["latest_turn_index"] if result else None
    except Exception as e:
        logger.error(f"Error reading latest turn index: {e}")
        return None


async def create_query(
    conversation_query_id: str,
    conversation_thread_id: str,
    turn_index: int,
    content: str,
    query_type: str,
    feedback_action: Optional[str] = None,
    metadata: Optional[Dict[str, Any]] = None,
    created_at: Optional[datetime] = None,
    conn=None,
    idempotent: bool = True,
) -> Dict[str, Any]:
    """
    Create a query entry.

    Args:
        conversation_query_id: Query ID
        conversation_thread_id: Thread ID
        turn_index: Turn index
        content: Query content
        query_type: Query type
        feedback_action: Optional feedback action
        metadata: Optional metadata
        created_at: Optional timestamp
        conn: Optional database connection to reuse
        idempotent: If True, use ON CONFLICT DO UPDATE for safe retries
    """
    if created_at is None:
        created_at = datetime.now(timezone.utc)

    # Sanitize user-typed input — `content` is a TEXT column, `metadata` is JSONB.
    # User can paste anything (binary copy/paste, terminal escapes), so adversarial
    # NUL bytes are realistic here. Strip once so all code paths below see clean data.
    content = strip_pg_nul_str(content)

    params = (
        conversation_query_id,
        conversation_thread_id,
        turn_index,
        content,
        query_type,
        feedback_action,
        SafeJson(metadata or {}),
        created_at,
    )

    async def _insert(cur) -> Dict[str, Any]:
        if idempotent:
            # Idempotent: ON CONFLICT DO UPDATE for safe retries. The WHERE
            # clause gates the UPDATE on content equality so a legitimate
            # retry-of-same-content (HITL resume, network retry) succeeds
            # silently while a concurrent different-content INSERT collision
            # (worker race that bypassed the in-process admission lock)
            # produces no RETURNING row — we surface QueryConflictError
            # instead of letting ON CONFLICT DO UPDATE silently overwrite
            # the loser's row.
            await cur.execute(
                """
                INSERT INTO conversation_queries (
                    conversation_query_id, conversation_thread_id, turn_index, content, type,
                    feedback_action, metadata, created_at
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (conversation_thread_id, turn_index) DO UPDATE
                SET content = EXCLUDED.content,
                    type = EXCLUDED.type,
                    feedback_action = EXCLUDED.feedback_action,
                    metadata = EXCLUDED.metadata,
                    created_at = EXCLUDED.created_at
                WHERE conversation_queries.content IS NOT DISTINCT FROM EXCLUDED.content
                RETURNING conversation_query_id, conversation_thread_id, turn_index, content, type,
                          feedback_action, metadata, created_at
            """,
                params,
            )
            result = await cur.fetchone()
            if result is None:
                await cur.execute(
                    "SELECT content FROM conversation_queries "
                    "WHERE conversation_thread_id = %s AND turn_index = %s",
                    (conversation_thread_id, turn_index),
                )
                existing = await cur.fetchone()
                raise errors.QueryConflictError(
                    thread_id=conversation_thread_id,
                    turn_index=turn_index,
                    existing_content=(existing or {}).get("content"),
                )
        else:
            # Non-idempotent: fail on conflict
            await cur.execute(
                """
                INSERT INTO conversation_queries (
                    conversation_query_id, conversation_thread_id, turn_index, content, type,
                    feedback_action, metadata, created_at
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING conversation_query_id, conversation_thread_id, turn_index, content, type,
                          feedback_action, metadata, created_at
            """,
                params,
            )
            result = await cur.fetchone()
        logger.debug(
            f"[conversation_db] create_query query_id={conversation_query_id} thread_id={conversation_thread_id} turn_index={turn_index} type={query_type}"
        )
        return dict(result)

    try:
        if conn:
            async with conn.cursor(row_factory=dict_row) as cur:
                return await _insert(cur)
        async with pool.get_db_connection() as new_conn:
            async with new_conn.cursor(row_factory=dict_row) as cur:
                return await _insert(cur)
    except Exception as e:
        logger.error(f"Error creating query: {e}")
        raise


async def get_queries_for_thread(
    conversation_thread_id: str, limit: Optional[int] = None, offset: int = 0
) -> Tuple[List[Dict[str, Any]], int]:
    """Get queries for a thread."""
    try:
        async with pool.get_db_connection() as conn:
            async with conn.cursor(row_factory=dict_row) as cur:
                # Get total count
                await cur.execute(
                    """
                    SELECT COUNT(*) as total
                    FROM conversation_queries
                    WHERE conversation_thread_id = %s
                """,
                    (conversation_thread_id,),
                )

                total_result = await cur.fetchone()
                total_count = total_result["total"]

                # Get queries
                if limit:
                    await cur.execute(
                        """
                        SELECT
                            conversation_query_id, conversation_thread_id, turn_index, content, type,
                            feedback_action, metadata, created_at
                        FROM conversation_queries
                        WHERE conversation_thread_id = %s
                        ORDER BY turn_index ASC
                        LIMIT %s OFFSET %s
                    """,
                        (conversation_thread_id, limit, offset),
                    )
                else:
                    await cur.execute(
                        """
                        SELECT
                            conversation_query_id, conversation_thread_id, turn_index, content, type,
                            feedback_action, metadata, created_at
                        FROM conversation_queries
                        WHERE conversation_thread_id = %s
                        ORDER BY turn_index ASC
                    """,
                        (conversation_thread_id,),
                    )

                queries = await cur.fetchall()
                return [dict(row) for row in queries], total_count

    except Exception as e:
        logger.error(f"Error getting queries for thread: {e}")
        raise
