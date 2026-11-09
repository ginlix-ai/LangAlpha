"""conversation_feedbacks: per-turn thumbs and comments."""

import logging

from psycopg.rows import dict_row

from src.server.database import pool
from src.server.database.conversation import _sql

logger = logging.getLogger(__name__)

_ANY = object()


class RatingChanged(Exception):
    """The rating no longer has the report it was read with: another request
    changed it in between."""


async def upsert_feedback(
    conversation_thread_id: str,
    turn_index: int,
    user_id: str,
    rating: str,
    issue_categories: list | None = None,
    comment: str | None = None,
    consent_human_review: bool = False,
    conn=None,
    report_id_read: object = _ANY,
) -> dict:
    """Upsert a feedback rating for a conversation response.

    Resolves conversation_response_id from (thread_id, turn_index).
    Uses ON CONFLICT to update if feedback already exists for this response+user.
    With ``report_id_read`` (None for no rating or none with a report), an
    existing rating is changed only while its report is still that one, and
    ``RatingChanged`` is raised otherwise: a rating that left a thumbs-down
    must be one whose report was withdrawn.
    """
    guarded = report_id_read is not _ANY

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

            # A rating that becomes a thumbs-down gets a new report id, and
            # with it a new report to the team; one that stays a thumbs-down
            # keeps its report, and a thumbs-up has none.
            await cur.execute(
                """
                INSERT INTO conversation_feedback (
                    conversation_response_id, user_id, rating,
                    issue_categories, comment,
                    consent_human_review, review_status, report_id
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s,
                    CASE WHEN %s = 'thumbs_down' THEN gen_random_uuid() END
                )
                ON CONFLICT (conversation_response_id, user_id) DO UPDATE SET
                    rating = EXCLUDED.rating,
                    -- A thumbs-down the team already has keeps what it was
                    -- sent with: sent again, its report is a duplicate the
                    -- platform keeps as it was, and more goes as a note or a
                    -- review from the sent dialog.
                    issue_categories = CASE WHEN conversation_feedback.reported_at IS NOT NULL AND EXCLUDED.rating = 'thumbs_down' THEN conversation_feedback.issue_categories
                        ELSE EXCLUDED.issue_categories END,
                    comment = CASE WHEN conversation_feedback.reported_at IS NOT NULL AND EXCLUDED.rating = 'thumbs_down' THEN conversation_feedback.comment
                        ELSE EXCLUDED.comment END,
                    consent_human_review = CASE WHEN conversation_feedback.reported_at IS NOT NULL AND EXCLUDED.rating = 'thumbs_down' THEN conversation_feedback.consent_human_review
                        ELSE EXCLUDED.consent_human_review END,
                    review_status = CASE WHEN conversation_feedback.reported_at IS NOT NULL AND EXCLUDED.rating = 'thumbs_down' THEN conversation_feedback.review_status
                        ELSE EXCLUDED.review_status END,
                    report_id = CASE
                        WHEN EXCLUDED.rating <> 'thumbs_down' THEN NULL
                        WHEN conversation_feedback.rating = 'thumbs_down'
                            THEN COALESCE(conversation_feedback.report_id, EXCLUDED.report_id)
                        ELSE EXCLUDED.report_id
                    END,
                    reported_at = CASE
                        WHEN EXCLUDED.rating = 'thumbs_down'
                            AND conversation_feedback.rating = 'thumbs_down'
                            THEN conversation_feedback.reported_at
                    END,
                    report_follow_ups = CASE
                        WHEN EXCLUDED.rating = 'thumbs_down'
                            AND conversation_feedback.rating = 'thumbs_down'
                            THEN conversation_feedback.report_follow_ups
                        ELSE '[]'::jsonb
                    END,
                    updated_at = NOW()
            """
                + (
                    " WHERE conversation_feedback.report_id IS NOT DISTINCT FROM %s::uuid"
                    if guarded
                    else ""
                )
                + " RETURNING *",
                (
                    response_id,
                    user_id,
                    rating,
                    issue_categories,
                    comment,
                    consent_human_review,
                    review_status,
                    rating,
                    *((report_id_read,) if guarded else ()),
                ),
            )
            result = await cur.fetchone()
            if result is None:
                raise RatingChanged
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


async def get_feedback_for_turn(
    conversation_thread_id: str, turn_index: int, user_id: str
) -> dict | None:
    """The user's rating of a turn, as ``get_feedback_for_thread`` reads it:
    on the turn's settled attempt, with its turn index."""
    async with pool.get_db_connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                f"""
                SELECT f.*, r.turn_index
                FROM conversation_feedback f
                JOIN ({_sql._SETTLED_ATTEMPTS}) r
                    ON f.conversation_response_id = r.conversation_response_id
                WHERE f.user_id = %s AND r.turn_index = %s
            """,
                (conversation_thread_id, user_id, turn_index),
            )
            return await cur.fetchone()


async def get_feedback_by_id(conversation_feedback_id: str) -> dict | None:
    async with pool.get_db_connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                "SELECT * FROM conversation_feedback WHERE conversation_feedback_id = %s",
                (conversation_feedback_id,),
            )
            return await cur.fetchone()


async def delete_feedback_by_id(conversation_feedback_id: str, report_id: str | None) -> bool:
    """Delete the rating read earlier in the request, not whichever attempt
    the turn resolves to now: a retry settling meanwhile moves the turn. Only
    while it still has the report it was read with, the one just withdrawn."""
    async with pool.get_db_connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                "DELETE FROM conversation_feedback WHERE conversation_feedback_id = %s "
                "AND report_id IS NOT DISTINCT FROM %s::uuid",
                (conversation_feedback_id, report_id),
            )
            return cur.rowcount > 0


# Each update below is guarded on the report id: a rating removed, or changed
# to a thumbs-up, while its report was being sent no longer stands for that
# report, and finds nothing to update.


async def mark_feedback_reported(conversation_feedback_id: str, report_id: str) -> dict | None:
    """The platform accepted the thumbs-down's report; keeps the first time."""
    return await _update_feedback(
        conversation_feedback_id,
        report_id,
        "reported_at = COALESCE(reported_at, NOW())",
        (),
    )


async def add_feedback_follow_up(
    conversation_feedback_id: str, report_id: str, note_id: str, note: str
) -> dict | None:
    """A note the platform posted under the thumbs-down's report, once per
    ``note_id``: a retry of a note already kept changes nothing."""
    return await _update_feedback(
        conversation_feedback_id,
        report_id,
        "report_follow_ups = CASE"
        " WHEN report_follow_ups @> jsonb_build_array(jsonb_build_object('id', %s::text))"
        " THEN report_follow_ups"
        " ELSE report_follow_ups || jsonb_build_array("
        "jsonb_build_object('id', %s::text, 'note', %s::text, 'at', NOW())) END",
        (note_id, note_id, note),
    )


async def request_feedback_review(conversation_feedback_id: str, report_id: str) -> dict | None:
    """The user asked for a human review after rating."""
    return await _update_feedback(
        conversation_feedback_id,
        report_id,
        "consent_human_review = TRUE, review_status = COALESCE(review_status, 'pending')",
        (),
    )


async def _update_feedback(
    conversation_feedback_id: str, report_id: str, assignments: str, params: tuple
):
    async with pool.get_db_connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                f"UPDATE conversation_feedback SET {assignments}, updated_at = NOW() "
                "WHERE conversation_feedback_id = %s AND report_id = %s RETURNING *",
                (*params, conversation_feedback_id, report_id),
            )
            return await cur.fetchone()
