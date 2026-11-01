"""Stored digests of the tokens a computer's ``la`` CLI presents."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any

from psycopg.rows import dict_row

from src.server.database.pool import get_db_connection
from src.server.database.sql_fences import FENCE_BINDABLE


@dataclass(frozen=True)
class TokenRow:
    #: When the current token runs out; None with no token.
    expires_at: datetime | None = None
    #: The sandbox that took it: None until a write lands, since one can
    #: fail after its mint.
    held_by: str | None = None


async def current(computer_id: str) -> TokenRow:
    async with get_db_connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                "SELECT expires_at, held_by FROM computer_cli_tokens WHERE computer_id = %s",
                (computer_id,),
            )
            row = await cur.fetchone()
    if row is None:
        return TokenRow()
    return TokenRow(row["expires_at"], row["held_by"])


async def save_token(
    computer_id: str,
    user_id: str,
    digest: bytes,
    expires_at: datetime,
    seen: datetime | None,
) -> bool:
    """Make ``digest`` the current token, held by no sandbox yet, if the
    current one is still the one read (``seen``, None for no row); the one it
    replaces stays valid until its own expiry.

    The compare keeps two mints that read the same row from rotating the
    token a sandbox holds out of both slots before either is written. False
    when it lost that race, or the computer is not its user's or is leaving
    service: the share lock orders the save against a stop, which revokes
    after it. A row deleted since it was read is inserted afresh, which only
    a computer serving again since can take, through that fence: a token it
    would have been minted anyway.
    """
    async with get_db_connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                f"""
                WITH serving AS (
                    SELECT computer_id, user_id FROM computers
                    WHERE computer_id = %(computer_id)s AND user_id = %(user_id)s
                      AND {FENCE_BINDABLE}
                    FOR SHARE
                )
                INSERT INTO computer_cli_tokens AS t
                    (computer_id, user_id, token_sha256, expires_at, updated_at)
                SELECT computer_id, user_id, %(digest)s, %(expires_at)s, NOW()
                FROM serving
                ON CONFLICT (computer_id) DO UPDATE
                SET prev_token_sha256 = t.token_sha256,
                    prev_expires_at = t.expires_at,
                    token_sha256 = EXCLUDED.token_sha256,
                    expires_at = EXCLUDED.expires_at,
                    held_by = NULL,
                    user_id = EXCLUDED.user_id,
                    updated_at = NOW()
                WHERE t.expires_at IS NOT DISTINCT FROM %(seen)s::timestamptz
                RETURNING t.computer_id
                """,
                {
                    "computer_id": computer_id,
                    "user_id": user_id,
                    "digest": digest,
                    "expires_at": expires_at,
                    "seen": seen,
                },
            )
            return await cur.fetchone() is not None


async def mark_held(computer_id: str, sandbox_id: str, expires_at: datetime) -> None:
    """Record that ``sandbox_id`` took the token expiring then, and only that
    one: a token minted since is held by no sandbox yet."""
    async with get_db_connection() as conn:
        await conn.execute(
            "UPDATE computer_cli_tokens SET held_by = %s"
            " WHERE computer_id = %s AND expires_at = %s",
            (sandbox_id, computer_id, expires_at),
        )


async def load_token(computer_id: str) -> dict[str, Any] | None:
    """The computer's token digests and owner.

    Joined to the computer under the fence ``save_token`` mints behind, so one
    stopping, stopped, deleted or changed hands authenticates nothing whether
    or not its row is gone.
    """
    async with get_db_connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                f"""
                SELECT t.user_id, t.token_sha256, t.expires_at,
                       t.prev_token_sha256, t.prev_expires_at
                FROM computer_cli_tokens t
                JOIN computers c ON c.computer_id = t.computer_id
                WHERE t.computer_id = %s AND c.user_id = t.user_id
                  AND c.{FENCE_BINDABLE}
                """,
                (computer_id,),
            )
            return await cur.fetchone()


async def delete_token(computer_id: str) -> None:
    async with get_db_connection() as conn:
        await conn.execute(
            "DELETE FROM computer_cli_tokens WHERE computer_id = %s", (computer_id,)
        )


async def claim_due(
    *,
    refresh_below: timedelta,
    retry_after: timedelta,
    settling: timedelta,
    limit: int,
) -> list[dict[str, Any]]:
    """Take up to ``limit`` running computers whose sandbox holds no token
    with more than ``refresh_below`` left, marking each tried so no worker
    takes it again for ``retry_after``.

    A token minted within ``settling`` is left alone: held by no sandbox
    until its own write lands, it would read as owed one, and a mint now
    would push it out before that write is recorded.

    Only a computer with a row: the first token is written by the acquire
    that runs the turn, and a sandbox no turn has used needs none. Each row
    carries what the idle reaper judges the computer by.
    """
    async with get_db_connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                """
                WITH due AS (
                    SELECT t.computer_id
                    FROM computer_cli_tokens t
                    JOIN computers c ON c.computer_id = t.computer_id
                    WHERE c.status = 'running' AND c.provider_ref IS NOT NULL
                      AND (t.expires_at < NOW() + %(refresh_below)s
                           OR t.held_by IS DISTINCT FROM c.provider_ref)
                      AND (t.attempted_at IS NULL
                           OR t.attempted_at < NOW() - %(retry_after)s)
                      AND t.updated_at < NOW() - %(settling)s
                    ORDER BY t.attempted_at ASC NULLS FIRST, t.expires_at ASC
                    LIMIT %(limit)s
                    FOR UPDATE OF t SKIP LOCKED
                ),
                claimed AS (
                    UPDATE computer_cli_tokens AS t SET attempted_at = NOW()
                    FROM due WHERE t.computer_id = due.computer_id
                    RETURNING t.computer_id
                )
                SELECT c.computer_id, c.user_id, c.provider_ref, c.kind,
                       c.provider_config, c.root_dir, c.is_always_on,
                       COALESCE(c.last_activity_at, c.updated_at) AS last_activity
                FROM claimed JOIN computers c USING (computer_id)
                """,
                {
                    "refresh_below": refresh_below,
                    "retry_after": retry_after,
                    "settling": settling,
                    "limit": limit,
                },
            )
            return [dict(row) for row in await cur.fetchall()]
