"""Secrets the migration mints in Postgres so every worker signs with one key."""

from __future__ import annotations

from src.server.database.pool import get_db_connection


async def get_server_key(name: str) -> bytes:
    """The named key, or ``LookupError`` when the migration that seeds it has not run."""
    async with get_db_connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute("SELECT secret FROM server_keys WHERE name = %s", (name,))
            row = await cur.fetchone()
    if not row or not row[0]:
        raise LookupError(f"server key {name!r} is not provisioned")
    return bytes(row[0])
