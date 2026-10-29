"""Stored digests of the tokens a computer's file mount presents, and what
the mount serves with them."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any

from psycopg.rows import dict_row

from src.server.database.pool import get_db_connection
from src.server.database.sql_fences import FENCE_BINDABLE


async def save_token(
    computer_id: str, user_id: str, digest: bytes, expires_at: datetime
) -> bool:
    """Make ``digest`` the current token, held by no sandbox yet; the one it
    replaces stays valid until its own expiry, so a daemon mid-request keeps
    working, and one serving with it still serves. A daemon whose token was
    already the previous one (the current one never published) has just lost
    it, so the row stops saying it serves.

    False when the computer is not its user's or is leaving service. The
    share lock on the computer orders the save against a stop: one saved
    first is revoked after it, and one waiting on it finds the computer
    stopping.
    """
    async with get_db_connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                f"""
                WITH serving AS (
                    SELECT computer_id, user_id FROM computers
                    WHERE computer_id = %s AND user_id = %s AND {FENCE_BINDABLE}
                    FOR SHARE
                )
                INSERT INTO livefs_tokens AS t
                    (computer_id, user_id, token_sha256, expires_at, updated_at)
                SELECT computer_id, user_id, %s, %s, NOW() FROM serving
                ON CONFLICT (computer_id) DO UPDATE
                    SET prev_token_sha256 = t.token_sha256,
                        prev_expires_at = t.expires_at,
                        token_sha256 = EXCLUDED.token_sha256,
                        expires_at = EXCLUDED.expires_at,
                        held_by = NULL,
                        served_by = CASE WHEN t.held_by IS NULL THEN NULL
                                         ELSE t.served_by END,
                        user_id = EXCLUDED.user_id,
                        updated_at = NOW()
                RETURNING computer_id
                """,
                (computer_id, user_id, digest, expires_at),
            )
            return await cur.fetchone() is not None


async def load_token(computer_id: str) -> dict[str, Any] | None:
    """The computer's token digests, with the owner and root it serves.

    Joined to the computer under the fence ``save_token`` mints behind, so one
    stopping, stopped, deleted or changed hands authenticates nothing whether
    or not its row is gone: a stop whose revoke failed, or a start reverted to
    stopped, leaves no token serving.
    """
    async with get_db_connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                f"""
                SELECT t.user_id, t.token_sha256, t.expires_at,
                       t.prev_token_sha256, t.prev_expires_at, c.root_dir
                FROM livefs_tokens t
                JOIN computers c ON c.computer_id = t.computer_id
                WHERE t.computer_id = %s AND c.user_id = t.user_id
                  AND c.{FENCE_BINDABLE}
                """,
                (computer_id,),
            )
            return await cur.fetchone()


@dataclass(frozen=True)
class Served:
    """What a start answered serving with: the daemon's code and the server
    address it dials."""

    code: str
    url: str


@dataclass(frozen=True)
class TokenRow:
    #: When the current token runs out; None with no token.
    expires_at: datetime | None = None
    #: The sandbox that took it: None until a publish lands, since one can
    #: fail after its mint.
    held_by: str | None = None
    #: The sandbox whose daemon the last start answered serving, with what,
    #: and until when the token that daemon holds is good. A mint leaves
    #: them, since the daemon serves on with the token it has.
    served_by: str | None = None
    served: Served | None = None
    served_until: datetime | None = None


#: The columns ``token_row`` reads.
TOKEN_COLUMNS = (
    "expires_at, held_by, served_by, served_code, served_url, served_until"
)


def token_row(row: dict[str, Any] | None) -> TokenRow:
    """A row of ``TOKEN_COLUMNS``, or None for no row."""
    if row is None or row["expires_at"] is None:
        return TokenRow()
    expires_at, held_by = row["expires_at"], row["held_by"]
    code, url = row["served_code"], row["served_url"]
    if row["served_by"] is None or code is None or url is None:
        return TokenRow(expires_at, held_by)
    return TokenRow(
        expires_at, held_by, row["served_by"], Served(code, url), row["served_until"]
    )


async def current_token(computer_id: str) -> TokenRow:
    async with get_db_connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                f"SELECT {TOKEN_COLUMNS} FROM livefs_tokens WHERE computer_id = %s",
                (computer_id,),
            )
            return token_row(await cur.fetchone())


_SET_SERVED = """
    served_by = CASE WHEN %(code)s::text IS NULL THEN NULL ELSE %(sandbox_id)s::text END,
    served_code = %(code)s::text, served_url = %(url)s::text,
    served_until = CASE WHEN %(code)s::text IS NULL THEN NULL ELSE expires_at END
"""


def _params(
    computer_id: str, sandbox_id: str | None, expires_at: Any, served: Served | None
) -> dict[str, Any]:
    return {
        "computer_id": computer_id,
        "sandbox_id": sandbox_id,
        "expires_at": expires_at,
        "code": served.code if served is not None else None,
        "url": served.url if served is not None else None,
    }


async def mark_held(
    computer_id: str,
    sandbox_id: str | None,
    expires_at: datetime,
    served: Served | None,
) -> None:
    """Record that ``sandbox_id`` took the token expiring then, and only
    that one (a token minted since is held by no sandbox yet), with what
    its start answered: serving with ``served``, or (None) down."""
    async with get_db_connection() as conn:
        await conn.execute(
            f"UPDATE livefs_tokens SET held_by = %(sandbox_id)s::text, {_SET_SERVED}"
            " WHERE computer_id = %(computer_id)s AND expires_at = %(expires_at)s",
            _params(computer_id, sandbox_id, expires_at, served),
        )


async def mark_served(
    computer_id: str,
    sandbox_id: str | None,
    expires_at: datetime | None,
    served: Served | None,
) -> None:
    """Record what a start on ``sandbox_id`` answered with the token it
    already held; nothing once that token was replaced or revoked."""
    async with get_db_connection() as conn:
        await conn.execute(
            f"UPDATE livefs_tokens SET {_SET_SERVED}"
            " WHERE computer_id = %(computer_id)s AND held_by = %(sandbox_id)s::text"
            " AND expires_at = %(expires_at)s",
            _params(computer_id, sandbox_id, expires_at, served),
        )


async def mark_down(computer_id: str, sandbox_id: str) -> None:
    """Record that no daemon serves on ``sandbox_id``, which restarted: a
    boot leaves no mount, and no error a command would meet."""
    async with get_db_connection() as conn:
        await conn.execute(
            "UPDATE livefs_tokens SET served_by = NULL"
            " WHERE computer_id = %s AND served_by = %s",
            (computer_id, sandbox_id),
        )


async def delete_token(computer_id: str) -> None:
    async with get_db_connection() as conn:
        await conn.execute(
            "DELETE FROM livefs_tokens WHERE computer_id = %s", (computer_id,)
        )
