"""conversation_usages: the write-once per-run usage record."""

import logging
from typing import Optional, Dict, Any

from psycopg import AsyncConnection
from psycopg.types.json import Json

from src.server.database import pool

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
