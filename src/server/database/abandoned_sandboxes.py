"""Sandboxes a create gave up on, kept until the provider has deleted them."""

from __future__ import annotations

from datetime import timedelta
from typing import Any

from psycopg.rows import dict_row

from src.server.database.pool import get_db_connection


async def record(kind: str, ref: str, reason: str) -> None:
    """Note *ref* for the sweep. A name reported twice keeps its first sighting,
    which is what its absence is measured from."""
    async with get_db_connection() as conn:
        await conn.execute(
            """
            INSERT INTO abandoned_sandboxes (kind, ref, reason)
            VALUES (%s, %s, %s)
            ON CONFLICT (kind, ref) DO UPDATE SET reason = EXCLUDED.reason
            """,
            (kind, ref, reason),
        )


async def claim_due(*, retry_after: timedelta, limit: int) -> list[dict[str, Any]]:
    """Take up to ``limit`` rows no worker tried within ``retry_after``,
    marking each tried so no other worker takes it meanwhile."""
    async with get_db_connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                """
                WITH due AS (
                    SELECT kind, ref FROM abandoned_sandboxes
                    WHERE attempted_at IS NULL
                       OR attempted_at < NOW() - %(retry_after)s
                    ORDER BY attempted_at ASC NULLS FIRST, created_at ASC
                    LIMIT %(limit)s
                    FOR UPDATE SKIP LOCKED
                )
                UPDATE abandoned_sandboxes AS a
                   SET attempted_at = NOW(), attempts = a.attempts + 1
                  FROM due
                 WHERE a.kind = due.kind AND a.ref = due.ref
                RETURNING a.kind, a.ref, a.reason, a.attempts,
                          NOW() - a.created_at AS age
                """,
                {"retry_after": retry_after, "limit": limit},
            )
            return list(await cur.fetchall())


async def drop(kind: str, ref: str) -> None:
    """Forget a sandbox the provider no longer has."""
    async with get_db_connection() as conn:
        await conn.execute(
            "DELETE FROM abandoned_sandboxes WHERE kind = %s AND ref = %s", (kind, ref)
        )


async def note_attempt(kind: str, ref: str, error: str | None) -> None:
    """Record why the last try left the row, or None when it only waits out
    the absence window."""
    async with get_db_connection() as conn:
        await conn.execute(
            "UPDATE abandoned_sandboxes SET last_error = %s WHERE kind = %s AND ref = %s",
            (error, kind, ref),
        )
