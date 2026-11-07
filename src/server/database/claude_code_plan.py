"""The Claude plan behind each Claude Code credential (``claude_code_plan``, migration 111).

One row per user and credential: ``SAVED`` for the setup token the user
saved or stored, wherever it runs, and ``computer_credential`` for a
computer's own token. ``state`` is what the CLI's reports said, merged by
the caller under the row's lock (``update_plan``), so how one report moves
it is decided in one place. ``generation`` fences the writes: it moves each
time the credential's token is replaced or removed, and a write opened on
an older one is dropped, so a run still going on the old token never writes
that account's state onto the new one.
"""

from __future__ import annotations

from collections.abc import Callable
from datetime import datetime
from typing import Any

from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

from src.server.database.pool import get_db_connection

#: The setup token the user saved or stored, wherever a turn runs on it.
SAVED = "saved"


def computer_credential(computer_id: str) -> str:
    """The credential of a computer's own token."""
    return f"computer:{computer_id}"


def plan_credential(signed_in: str, computer_id: str | None) -> str | None:
    """The row of the token a turn signs in with: ``saved``, the setup token
    the user saved, or ``machine``, the computer's own, which only a turn on
    a computer has."""
    if signed_in == "saved":
        return SAVED
    return computer_credential(computer_id) if computer_id else None


async def update_plan(
    user_id: str, credential: str, generation: int, seen_at: datetime, change: Callable[[dict[str, Any]], dict[str, Any]],
) -> bool:
    """Replace the credential's state with ``change(state)``, under the row's
    lock, where its generation is still ``generation``. False where it moved:
    the report was the replaced token's. Only a run that opened on no row
    (generation 0) makes one, so a row deleted since (``drop_plan``) stays
    gone for a run that opened on it; one such run may make a removed
    computer's row again, which no one reads, since its computer is gone."""
    async with get_db_connection() as conn, conn.transaction(), conn.cursor() as cur:
        if generation == 0:
            await cur.execute(
                "INSERT INTO claude_code_plan (user_id, credential, updated_at) VALUES (%s, %s, %s) ON CONFLICT DO NOTHING",
                (user_id, credential, seen_at),
            )
        await cur.execute(
            "SELECT generation, state FROM claude_code_plan WHERE user_id = %s AND credential = %s FOR UPDATE",
            (user_id, credential),
        )
        if (row := await cur.fetchone()) is None:
            return False
        current, state = row
        if current != generation:
            return False
        await cur.execute(
            "UPDATE claude_code_plan SET state = %s, updated_at = GREATEST(updated_at, %s) "
            "WHERE user_id = %s AND credential = %s",
            (Jsonb(change(state)), seen_at, user_id, credential),
        )
    return True


async def change_plans(user_id: str, change: Callable[[dict[str, Any]], dict[str, Any]], *, conn=None) -> None:
    """Replace the state of each of the user's rows with ``change(state)``,
    under its lock, whatever its generation: a change the user made holds
    for every credential's."""
    async with get_db_connection(conn) as conn, conn.transaction(), conn.cursor() as cur:
        await cur.execute("SELECT credential, state FROM claude_code_plan WHERE user_id = %s FOR UPDATE", (user_id,))
        for credential, state in await cur.fetchall():
            await cur.execute(
                "UPDATE claude_code_plan SET state = %s WHERE user_id = %s AND credential = %s",
                (Jsonb(change(state)), user_id, credential),
            )


async def get_plans(user_id: str, credentials: list[str] | None = None) -> dict[str, dict[str, Any]]:
    """The user's rows by credential, only ``credentials`` where given."""
    query = "SELECT * FROM claude_code_plan WHERE user_id = %s"
    params: tuple[Any, ...] = (user_id,)
    if credentials is not None:
        query, params = f"{query} AND credential = ANY(%s)", (user_id, credentials)
    async with get_db_connection() as conn, conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(query, params)
        return {row["credential"]: row for row in await cur.fetchall()}


async def drop_plan(user_id: str, credential: str) -> None:
    """Delete the credential's row: its computer was removed, and its id is
    never used again."""
    async with get_db_connection() as conn, conn.cursor() as cur:
        await cur.execute("DELETE FROM claude_code_plan WHERE user_id = %s AND credential = %s", (user_id, credential))


async def forget_plan(user_id: str, credential: str, *, token_digest: str | None = None, conn=None) -> None:
    """Drop what the credential's reports said and move its generation: the
    token on it now may be another Claude account's. ``token_digest`` names
    the token that now signs in on it, so saving the same one again keeps its
    state; without one (a removal) the state always goes."""
    async with get_db_connection(conn) as conn, conn.cursor() as cur:
        await cur.execute(
            """
            INSERT INTO claude_code_plan AS p (user_id, credential, generation, token_digest)
            VALUES (%s, %s, 1, %s)
            ON CONFLICT (user_id, credential) DO UPDATE SET
                generation = p.generation + 1, state = '{}', token_digest = EXCLUDED.token_digest, updated_at = NOW()
            WHERE EXCLUDED.token_digest IS NULL OR p.token_digest IS DISTINCT FROM EXCLUDED.token_digest
            """,
            (user_id, credential, token_digest),
        )
