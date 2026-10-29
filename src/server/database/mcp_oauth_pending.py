"""What a pending sign-in saves at its callback, held encrypted until then.

The start and the callback of one sign-in land on any worker, minutes apart,
so the payload lives here and nowhere in process memory. Rows are keyed by a
digest of the OAuth state: the state is the callback's whole credential, and
the table holding it must not be able to replay one.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
from dataclasses import dataclass
from typing import Any

from psycopg.rows import dict_row

from src.server.database.encryption import get_encryption_key
from src.server.database.pool import get_db_connection
from src.server.database.sql_fences import advisory_key

logger = logging.getLogger(__name__)

MAX_PENDING_PER_USER = 3


def state_digest(state: str) -> bytes:
    return hashlib.sha256(state.encode()).digest()


@dataclass(frozen=True)
class PendingRow:
    user_id: str
    kind: str
    server_name: str
    payload: dict[str, Any]


async def store_pending(
    state: str,
    *,
    user_id: str,
    kind: str,
    server_name: str,
    payload: dict[str, Any],
    ttl_seconds: int,
) -> None:
    """Park a payload for the callback of ``state``.

    A user keeps at most ``MAX_PENDING_PER_USER`` sign-ins parked, the newest:
    a payload can be a whole plugin package, and a new server name sidesteps
    the supersession that retires an older sign-in of the same server. One
    pushed out reads as expired at its callback, which already says so. The
    lock makes the count hold against two starts at once.
    """
    key = get_encryption_key()
    # Off the event loop, and before the lock: a plugin's package is up to
    # 64 MiB of base64 inside it.
    text = await asyncio.to_thread(json.dumps, payload)
    async with get_db_connection() as conn:
        async with conn.transaction():
            async with conn.cursor() as cur:
                await cur.execute(
                    "SELECT pg_advisory_xact_lock(%s)",
                    (advisory_key("mcp_oauth_pending", user_id),),
                )
                await cur.execute(
                    """
                    WITH evicted AS (
                        DELETE FROM mcp_oauth_pending
                        WHERE user_id = %(user_id)s
                          AND state_sha256 NOT IN (
                              SELECT state_sha256 FROM mcp_oauth_pending
                              WHERE user_id = %(user_id)s AND expires_at > NOW()
                              ORDER BY created_at DESC
                              LIMIT %(kept)s
                          )
                    )
                    INSERT INTO mcp_oauth_pending
                        (state_sha256, user_id, kind, server_name, payload,
                         expires_at)
                    VALUES (%(state)s, %(user_id)s, %(kind)s, %(server_name)s,
                            pgp_sym_encrypt(%(payload)s, %(key)s),
                            NOW() + make_interval(secs => %(ttl)s))
                    """,
                    {
                        "state": state_digest(state),
                        "user_id": user_id,
                        "kind": kind,
                        "server_name": server_name,
                        "payload": text,
                        "key": key,
                        "ttl": ttl_seconds,
                        "kept": MAX_PENDING_PER_USER - 1,
                    },
                )


async def claim_pending(state: str, user_id: str) -> PendingRow | None:
    """Take the payload for ``state``, once. None when it is gone or expired.

    The delete is the claim, so two callbacks for one state cannot both get
    the payload, and an expired one is removed without being decrypted.
    """
    key = get_encryption_key()
    async with get_db_connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                """
                DELETE FROM mcp_oauth_pending
                WHERE state_sha256 = %s AND user_id = %s
                RETURNING kind, server_name,
                    CASE WHEN expires_at > NOW()
                         THEN pgp_sym_decrypt(payload, %s) END AS payload
                """,
                (state_digest(state), user_id, key),
            )
            row = await cur.fetchone()
    if row is None or row["payload"] is None:
        return None
    return PendingRow(
        user_id=user_id,
        kind=row["kind"],
        server_name=row["server_name"],
        payload=await asyncio.to_thread(json.loads, row["payload"]),
    )


async def drop_pending(state: str) -> None:
    """Delete the payload of a sign-in that can no longer reach its callback."""
    async with get_db_connection() as conn:
        await conn.execute(
            "DELETE FROM mcp_oauth_pending WHERE state_sha256 = %s",
            (state_digest(state),),
        )


async def delete_expired_pending() -> int:
    """Delete sign-ins past their expiry; returns how many were removed.

    A sign-in the user walked away from has no callback to delete its own row,
    and the payload may hold secret values, so the refresh sweeper clears them.
    """
    async with get_db_connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute("DELETE FROM mcp_oauth_pending WHERE expires_at < NOW()")
            return cur.rowcount
