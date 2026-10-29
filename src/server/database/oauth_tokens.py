"""
Database CRUD for user OAuth tokens (encrypted at rest).

Stores access/refresh tokens for OAuth-connected providers (e.g. Codex).
Same pgp_sym_encrypt/decrypt pattern as api_keys.py. The writers keep the
``has_any_oauth_token`` cache current themselves.
"""

import logging
from contextlib import asynccontextmanager
from datetime import datetime
from typing import Any, Dict, Optional

from psycopg.rows import dict_row

from src.server.database.pool import get_db_connection
from src.server.database.encryption import get_encryption_key
from src.server.database.sql_fences import advisory_key

logger = logging.getLogger(__name__)


# A flag counts only while it is newer than the row's last write. Code from
# before the flag existed (an older worker mid-deploy, or a rollback) saves
# fresh tokens without clearing it, and that save is what moves updated_at.
_LIVE_FLAG = """
    CASE WHEN needs_reauth_at >= updated_at THEN needs_reauth_at END AS needs_reauth_at,
    CASE WHEN needs_reauth_at >= updated_at THEN last_error_code END AS last_error_code
"""


@asynccontextmanager
async def connection_change(user_id: str, provider: str):
    """Hold one connection's writer lock, so a sign-in's save and a disconnect
    queue rather than interleave: a disconnect landing between a save's claim
    and its write would delete nothing, and the write would bring the
    connection back after it. The lock is Postgres's and lasts the
    transaction, so unlike a timed lease it cannot lapse under a slow write.

    Writes made inside pass the yielded connection (``conn=``); the cached
    ``has_any_oauth_token`` is dropped once they commit.
    """
    async with get_db_connection() as conn, conn.transaction():
        await conn.execute(
            "SELECT pg_advisory_xact_lock(%s)", (advisory_key("OAUTH", user_id, provider),)
        )
        yield conn
    await invalidate_oauth_active_cache(user_id)


@asynccontextmanager
async def _connection(conn):
    if conn is not None:
        yield conn
        return
    async with get_db_connection() as own:
        yield own


async def upsert_oauth_tokens(
    user_id: str,
    provider: str,
    access_token: str,
    refresh_token: str,
    account_id: str,
    email: Optional[str] = None,
    plan_type: Optional[str] = None,
    expires_at: Optional[datetime] = None,
    conn=None,
) -> None:
    """Insert or update OAuth tokens for a provider (encrypted).

    Fresh tokens are a working connection, so this also clears a needs-reauth
    flag: a connect and a successful refresh both come through here.
    """
    enc_key = get_encryption_key()
    async with _connection(conn) as db, db.cursor() as cur:
        await cur.execute(
            """
            INSERT INTO user_oauth_tokens
                (user_id, provider, access_token, refresh_token,
                 account_id, email, plan_type, expires_at, created_at, updated_at)
            VALUES
                (%s, %s, pgp_sym_encrypt(%s, %s), pgp_sym_encrypt(%s, %s),
                 %s, %s, %s, %s, NOW(), NOW())
            ON CONFLICT (user_id, provider) DO UPDATE SET
                access_token  = EXCLUDED.access_token,
                refresh_token = EXCLUDED.refresh_token,
                account_id    = EXCLUDED.account_id,
                email         = EXCLUDED.email,
                plan_type     = EXCLUDED.plan_type,
                expires_at    = EXCLUDED.expires_at,
                needs_reauth_at = NULL,
                last_error_code = NULL,
                updated_at    = NOW()
            """,
            (
                user_id, provider,
                access_token, enc_key,
                refresh_token, enc_key,
                account_id, email, plan_type, expires_at,
            ),
        )
        logger.debug(
            f"[oauth_tokens] upsert user_id={user_id} provider={provider}"
        )
    # A lock holder drops it once its transaction commits.
    if conn is None:
        await invalidate_oauth_active_cache(user_id)


async def replace_refreshed_tokens(
    user_id: str,
    provider: str,
    *,
    spent_refresh_token: str,
    access_token: str,
    refresh_token: str,
    account_id: str,
    email: Optional[str],
    plan_type: Optional[str],
    expires_at: Optional[datetime],
) -> bool:
    """Store a refresh's tokens over the ones it spent; False when the row moved on.

    Conditioned on ``spent_refresh_token``, the one the refresh used, still being
    stored: a user who disconnected while it ran must not find the connection
    back, nor one who reconnected find the old account over the new one.
    """
    enc_key = get_encryption_key()
    async with get_db_connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                """
                UPDATE user_oauth_tokens
                SET access_token  = pgp_sym_encrypt(%s, %s),
                    refresh_token = pgp_sym_encrypt(%s, %s),
                    account_id    = %s,
                    email         = %s,
                    plan_type     = %s,
                    expires_at    = %s,
                    needs_reauth_at = NULL,
                    last_error_code = NULL,
                    updated_at    = NOW()
                WHERE user_id = %s AND provider = %s
                  AND pgp_sym_decrypt(refresh_token, %s) = %s
                """,
                (
                    access_token, enc_key,
                    refresh_token, enc_key,
                    account_id, email, plan_type, expires_at,
                    user_id, provider, enc_key, spent_refresh_token,
                ),
            )
            replaced = cur.rowcount > 0
    if replaced:
        await invalidate_oauth_active_cache(user_id)
    return replaced


async def get_oauth_tokens(
    user_id: str, provider: str
) -> Optional[Dict[str, Any]]:
    """
    Get decrypted OAuth tokens for a provider.

    Returns:
        {access_token, refresh_token, account_id, email, plan_type, expires_at,
        needs_reauth_at, last_error_code} or None if not found.
    """
    enc_key = get_encryption_key()
    async with get_db_connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                """
                SELECT
                    pgp_sym_decrypt(access_token, %s)  AS access_token,
                    pgp_sym_decrypt(refresh_token, %s)  AS refresh_token,
                    account_id, email, plan_type, expires_at,"""
                + _LIVE_FLAG
                + """
                FROM user_oauth_tokens
                WHERE user_id = %s AND provider = %s
                """,
                (enc_key, enc_key, user_id, provider),
            )
            row = await cur.fetchone()
            if row:
                # pgp_sym_decrypt returns text but psycopg3 may yield
                # bytes/memoryview for BYTEA columns — ensure str
                row["access_token"] = str(row["access_token"]) if row["access_token"] else ""
                row["refresh_token"] = str(row["refresh_token"]) if row["refresh_token"] else ""
            return row


async def delete_oauth_tokens(user_id: str, provider: str, conn=None) -> None:
    """Remove OAuth tokens for a provider."""
    async with _connection(conn) as db, db.cursor() as cur:
        await cur.execute(
            "DELETE FROM user_oauth_tokens WHERE user_id = %s AND provider = %s",
            (user_id, provider),
        )
        logger.info(f"[oauth_tokens] deleted user_id={user_id} provider={provider}")
    # A lock holder drops it once its transaction commits.
    if conn is None:
        await invalidate_oauth_active_cache(user_id)


async def get_oauth_connection(
    user_id: str, provider: str
) -> Optional[Dict[str, Any]]:
    """The stored connection without its tokens (no decryption), or None.

    Returns:
        {account_id, email, plan_type, expires_at, needs_reauth_at,
        last_error_code}
    """
    async with get_db_connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                """
                SELECT account_id, email, plan_type, expires_at,"""
                + _LIVE_FLAG
                + """
                FROM user_oauth_tokens
                WHERE user_id = %s AND provider = %s
                """,
                (user_id, provider),
            )
            return await cur.fetchone()


async def mark_oauth_needs_reauth(
    user_id: str, provider: str, *, refresh_token: str, error_code: str
) -> bool:
    """Flag the connection as needing sign-in; False when the row moved on.

    Conditioned on ``refresh_token`` still being the stored one. A refresh
    that failed because another request had already rotated the token (Codex
    refresh tokens are single use), or because the user reconnected while it
    ran, must not flag the working connection that replaced it.
    """
    enc_key = get_encryption_key()
    async with get_db_connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                """
                UPDATE user_oauth_tokens
                SET needs_reauth_at = CASE WHEN needs_reauth_at >= updated_at
                                           THEN needs_reauth_at ELSE NOW() END,
                    last_error_code = %s
                WHERE user_id = %s AND provider = %s
                  AND pgp_sym_decrypt(refresh_token, %s) = %s
                """,
                (error_code, user_id, provider, enc_key, refresh_token),
            )
            flagged = cur.rowcount > 0
    logger.info(
        f"[oauth_tokens] needs_reauth user_id={user_id} provider={provider} "
        f"code={error_code} applied={flagged}"
    )
    if flagged:
        await invalidate_oauth_active_cache(user_id)
    return flagged


async def flagged_oauth_connection(user_id: str) -> Optional[Dict[str, Any]]:
    """The user's most recently flagged connection, ``{provider,
    last_error_code}``, or None when none needs signing in again."""
    async with get_db_connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                """
                SELECT provider, last_error_code FROM user_oauth_tokens
                WHERE user_id = %s AND needs_reauth_at >= updated_at
                ORDER BY needs_reauth_at DESC LIMIT 1
                """,
                (user_id,),
            )
            return await cur.fetchone()


async def expire_oauth_access_token(user_id: str, provider: str) -> bool:
    """Mark the stored access token as spent, so the next use refreshes it.

    For an access token the provider refused before its stated expiry. The
    refresh then decides whether the connection is gone, from the token
    endpoint's own answer. Not conditioned on which token was refused: if it
    was already replaced, the cost is one early refresh, made under the same
    lock as any other.
    """
    async with get_db_connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                """
                UPDATE user_oauth_tokens
                SET expires_at = LEAST(expires_at, NOW())
                WHERE user_id = %s AND provider = %s
                  AND NOT COALESCE(needs_reauth_at >= updated_at, FALSE)
                  AND expires_at > NOW()
                """,
                (user_id, provider),
            )
            return cur.rowcount > 0


_OAUTH_ACTIVE_TTL = 86400  # 24h — freshness via explicit invalidation


def _active_key(user_id: str) -> str:
    # v2 since a flagged connection stopped counting: a v1 entry says yes for it.
    return f"oauth_active:v2:{user_id}"


async def has_any_oauth_token(user_id: str) -> bool:
    """Quick check: does the user have at least one working OAuth connection?

    A connection that needs signing in is not one: it cannot pay for any turn,
    so the user is gated as one with no own credential until they reconnect.

    Result is cached in Redis for up to ``_OAUTH_ACTIVE_TTL`` seconds.
    Every writer here that changes the answer (upsert, delete, flag)
    invalidates it.
    """
    from src.utils.cache.redis_cache import get_cache_client

    cache_key = _active_key(user_id)
    cache = get_cache_client()
    if cache.enabled and cache.client:
        try:
            cached = await cache.client.get(cache_key)
            if cached is not None:
                return cached == b"1"
        except Exception:
            pass  # Redis down — fall through to DB

    async with get_db_connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                "SELECT 1 FROM user_oauth_tokens WHERE user_id = %s "
                "AND NOT COALESCE(needs_reauth_at >= updated_at, FALSE) LIMIT 1",
                (user_id,),
            )
            result = (await cur.fetchone()) is not None

    if cache.enabled and cache.client:
        try:
            await cache.client.set(cache_key, b"1" if result else b"0", ex=_OAUTH_ACTIVE_TTL)
        except Exception:
            pass

    return result


async def invalidate_oauth_active_cache(user_id: str) -> None:
    """Delete the cached ``has_any_oauth_token`` result so the next call hits
    the DB. Fails open: a write that landed is never reported as failed
    because Redis was not there to hear of it."""
    from src.utils.cache.redis_cache import get_cache_client

    try:
        cache = get_cache_client()
        if cache.enabled and cache.client:
            # The v1 key too, for workers still on the code that reads it.
            await cache.client.delete(_active_key(user_id), f"oauth_active:{user_id}")
    except Exception:
        logger.debug("[oauth_tokens] oauth_active invalidation failed", exc_info=True)
