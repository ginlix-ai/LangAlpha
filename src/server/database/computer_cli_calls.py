"""The record of what a computer's agent did through the ``la`` CLI (migration 100)."""

from __future__ import annotations

from datetime import datetime
from typing import Any

from psycopg.types.json import Json

from src.server.database.pool import get_db_connection


async def record(
    *,
    computer_id: str,
    user_id: str,
    thread_id: str | None,
    call_id: str | None,
    method: str,
    route: str,
    targets: dict[str, Any],
    status: int,
) -> None:
    async with get_db_connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                """
                INSERT INTO computer_cli_calls
                    (computer_id, user_id, thread_id, call_id, method, route, targets, status)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (computer_id, user_id, thread_id, call_id, method, route, Json(targets), status),
            )


async def prune(before: datetime, limit: int) -> int:
    """Delete up to ``limit`` rows older than ``before``. Every worker's
    sweep runs it, so each takes only rows no other pass holds, and a bounded
    delete keeps one pass short."""
    async with get_db_connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                """
                DELETE FROM computer_cli_calls WHERE id IN (
                    SELECT id FROM computer_cli_calls WHERE created_at < %s
                    ORDER BY created_at LIMIT %s
                    FOR UPDATE SKIP LOCKED
                )
                """,
                (before, limit),
            )
            return cur.rowcount
