"""conversation_usages: the write-once per-run usage record."""

import logging
from typing import Optional, Dict, Any

from psycopg import AsyncConnection
from psycopg.rows import dict_row
from psycopg.types.json import Json

from src.server.database import pool
from src.server.utils.pg_sanitize import normalize_uuid

logger = logging.getLogger(__name__)


async def create_usage_record(
    usage_data: Dict[str, Any], conn: Optional[AsyncConnection] = None
) -> bool:
    """
    Create a usage record in conversation_usages table.

    Args:
        usage_data: Usage data dict with structure:
            {
                "conversation_usage_id": str,
                "conversation_response_id": str,
                "user_id": str,
                "conversation_thread_id": str,
                "workspace_id": str,
                "msg_type": str,
                "status": str,
                "token_usage": dict (JSONB),
                "infrastructure_usage": dict (JSONB, optional),
                "token_credits": float,
                "infrastructure_credits": float,
                "total_credits": float,
                "created_at": datetime
            }
        conn: Optional connection (for transactions)

    Returns:
        True if successful

    Raises:
        psycopg.Error: On database errors
    """

    async def _create(cur):
        await cur.execute(
            """
            INSERT INTO conversation_usages (
                conversation_usage_id,
                conversation_response_id,
                user_id,
                conversation_thread_id,
                workspace_id,
                msg_type,
                status,
                token_usage,
                infrastructure_usage,
                token_credits,
                infrastructure_credits,
                total_credits,
                is_byok,
                created_at
            ) VALUES (
                %(conversation_usage_id)s,
                %(conversation_response_id)s,
                %(user_id)s,
                %(conversation_thread_id)s,
                %(workspace_id)s,
                %(msg_type)s,
                %(status)s,
                %(token_usage)s,
                %(infrastructure_usage)s,
                %(token_credits)s,
                %(infrastructure_credits)s,
                %(total_credits)s,
                %(is_byok)s,
                %(created_at)s
            )
        """,
            {
                "conversation_usage_id": usage_data["conversation_usage_id"],
                "conversation_response_id": usage_data["conversation_response_id"],
                "user_id": usage_data["user_id"],
                "conversation_thread_id": usage_data["conversation_thread_id"],
                "workspace_id": usage_data["workspace_id"],
                "msg_type": usage_data.get("msg_type", "ptc"),
                "status": usage_data.get("status", "completed"),
                "token_usage": Json(usage_data.get("token_usage")),
                "infrastructure_usage": Json(usage_data.get("infrastructure_usage")),
                "token_credits": usage_data["token_credits"],
                "infrastructure_credits": usage_data["infrastructure_credits"],
                "total_credits": usage_data["total_credits"],
                "is_byok": usage_data.get("is_byok", False),
                "created_at": usage_data["created_at"],
            },
        )

    if conn:
        async with conn.cursor() as cur:
            await _create(cur)
    else:
        async with pool.get_db_connection() as conn:
            async with conn.cursor() as cur:
                await _create(cur)

    return True


async def get_thread_token_totals(
    conversation_thread_id: str, live_run_id: Optional[str] = None
) -> Optional[Dict[str, int]]:
    """Input and output tokens over every usage row of a thread.

    Every model call the thread made counts: the main agent's and its
    subagents', on every attempt, including turns an edit or regenerate later
    cut. The main row of ``live_run_id``, the run the caller reports live, is
    left out even when that run settles mid-read, because the client counts
    that run from its stream. None on a failed read, which costs the totals
    rather than the status they ride on.
    """
    conversation_thread_id = normalize_uuid(conversation_thread_id)
    if not conversation_thread_id:
        return None
    try:
        async with pool.get_db_connection() as conn:
            async with conn.cursor(row_factory=dict_row) as cur:
                await cur.execute(
                    """
                    SELECT
                        COALESCE(SUM((m.value->>'input_tokens')::bigint), 0)
                            AS input_tokens,
                        COALESCE(SUM((m.value->>'output_tokens')::bigint), 0)
                            AS output_tokens
                    FROM conversation_usages u,
                         jsonb_each(u.token_usage->'by_model') AS m
                    WHERE u.conversation_thread_id = %(thread_id)s
                      AND jsonb_typeof(u.token_usage->'by_model') = 'object'
                      AND (
                          u.msg_type = 'task'
                          OR %(live_run_id)s::uuid IS NULL
                          OR u.conversation_response_id <> %(live_run_id)s::uuid
                      )
                    """,
                    {
                        "thread_id": conversation_thread_id,
                        "live_run_id": normalize_uuid(live_run_id),
                    },
                )
                row = await cur.fetchone()
                return {
                    "input_tokens": int(row["input_tokens"]),
                    "output_tokens": int(row["output_tokens"]),
                }
    except Exception as e:
        logger.error(f"Error reading thread token totals: {e}")
        return None
