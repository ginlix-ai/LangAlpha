"""conversation_feedbacks: per-turn thumbs and comments."""

import logging

from psycopg.rows import dict_row

from src.server.database import pool
from src.server.database.conversation import _sql

logger = logging.getLogger(__name__)


async def upsert_feedback(
    conversation_thread_id: str,
    turn_index: int,
    user_id: str,
    rating: str,
    issue_categories: list | None = None,
    comment: str | None = None,
    consent_human_review: bool = False,
    conn=None,
) -> dict:
    """Upsert a feedback rating for a conversation response.

    Resolves conversation_response_id from (thread_id, turn_index).
    Uses ON CONFLICT to update if feedback already exists for this response+user.
    """

    async def _execute(conn):
        async with conn.cursor(row_factory=dict_row) as cur:
            # Resolve response_id — the turn's latest settled attempt (what
            # the user actually saw); never an in_progress slot.
            await cur.execute(
                """
                SELECT conversation_response_id
                FROM conversation_responses
                WHERE conversation_thread_id = %s AND turn_index = %s
                    AND status <> 'in_progress'
                ORDER BY attempt_no DESC
                LIMIT 1
            """,
                (conversation_thread_id, turn_index),
            )
            row = await cur.fetchone()
            if not row:
                return None

            response_id = str(row["conversation_response_id"])
            review_status = (
                "pending" if consent_human_review and rating == "thumbs_down" else None
            )

            await cur.execute(
                """
                INSERT INTO conversation_feedback (
                    conversation_response_id, user_id, rating,
                    issue_categories, comment,
                    consent_human_review, review_status
                ) VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (conversation_response_id, user_id) DO UPDATE SET
                    rating = EXCLUDED.rating,
                    issue_categories = EXCLUDED.issue_categories,
                    comment = EXCLUDED.comment,
                    consent_human_review = EXCLUDED.consent_human_review,
                    review_status = EXCLUDED.review_status
                RETURNING *
            """,
                (
                    response_id,
                    user_id,
                    rating,
                    issue_categories,
                    comment,
                    consent_human_review,
                    review_status,
                ),
            )
            result = await cur.fetchone()
            return {
                **result,
                "turn_index": turn_index,
            }

    try:
        if conn:
            return await _execute(conn)
        else:
            async with pool.get_db_connection() as conn_new:
                return await _execute(conn_new)
    except Exception as e:
        logger.error(f"Error upserting feedback: {e}")
        raise


async def get_feedback_for_thread(
    conversation_thread_id: str,
    user_id: str,
    conn=None,
) -> list:
    """Get all feedback for a thread by a specific user.

    JOINs to conversation_responses to derive turn_index.
    """

    async def _execute(conn):
        async with conn.cursor(row_factory=dict_row) as cur:
            # Join through the settled attempt per turn so feedback left on a
            # superseded attempt doesn't resurface on the visible turn.
            await cur.execute(
                f"""
                SELECT f.*, r.turn_index
                FROM conversation_feedback f
                JOIN ({_sql._SETTLED_ATTEMPTS}) r
                    ON f.conversation_response_id = r.conversation_response_id
                WHERE f.user_id = %s
                ORDER BY r.turn_index
            """,
                (conversation_thread_id, user_id),
            )
            return await cur.fetchall()

    try:
        if conn:
            return await _execute(conn)
        else:
            async with pool.get_db_connection() as conn_new:
                return await _execute(conn_new)
    except Exception as e:
        logger.error(f"Error getting feedback for thread: {e}")
        raise


async def delete_feedback(
    conversation_thread_id: str,
    turn_index: int,
    user_id: str,
    conn=None,
) -> bool:
    """Delete feedback for a specific response by a specific user.

    Resolves conversation_response_id from (thread_id, turn_index).
    """

    async def _execute(conn):
        async with conn.cursor(row_factory=dict_row) as cur:
            # Target only the turn's latest settled attempt — mirroring the
            # upsert resolution, so delete undoes exactly what upsert wrote.
            await cur.execute(
                """
                DELETE FROM conversation_feedback f
                USING (
                    SELECT conversation_response_id
                    FROM conversation_responses
                    WHERE conversation_thread_id = %s AND turn_index = %s
                        AND status <> 'in_progress'
                    ORDER BY attempt_no DESC
                    LIMIT 1
                ) r
                WHERE f.conversation_response_id = r.conversation_response_id
                    AND f.user_id = %s
            """,
                (conversation_thread_id, turn_index, user_id),
            )
            return cur.rowcount > 0

    try:
        if conn:
            return await _execute(conn)
        else:
            async with pool.get_db_connection() as conn_new:
                return await _execute(conn_new)
    except Exception as e:
        logger.error(f"Error deleting feedback: {e}")
        raise
