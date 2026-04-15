"""
Database CRUD for user OAuth tokens (encrypted at rest).

Stores access/refresh tokens for OAuth-connected providers (e.g. Codex).
Same pgp_sym_encrypt/decrypt pattern as api_keys.py.
"""

import logging
from datetime import datetime
from typing import Any, Dict, Optional

from psycopg.rows import dict_row

from src.server.database.conversation import get_db_connection
from src.server.database.encryption import get_encryption_key

logger = logging.getLogger(__name__)


async def upsert_oauth_tokens(
    user_id: str,
    provider: str,
    access_token: str,
    refresh_token: str,
    account_id: str,
    email: Optional[str] = None,
    plan_type: Optional[str] = None,
    expires_at: Optional[datetime] = None,
) -> None:
    """Insert or update OAuth tokens for a provider (encrypted)."""
    enc_key = get_encryption_key()
    async with get_db_connection() as conn:
        async with conn.cursor() as cur:
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


async def get_oauth_tokens(
    user_id: str, provider: str
) -> Optional[Dict[str, Any]]:
    """
    Get decrypted OAuth tokens for a provider.

    Returns:
        {access_token, refresh_token, account_id, email, plan_type, expires_at}
        or None if not found.
    """
    enc_key = get_encryption_key()
    async with get_db_connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                """
                SELECT
                    pgp_sym_decrypt(access_token, %s)  AS access_token,
                    pgp_sym_decrypt(refresh_token, %s)  AS refresh_token,
                    account_id, email, plan_type, expires_at
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


async def delete_oauth_tokens(user_id: str, provider: str) -> None:
    """Remove OAuth tokens for a provider."""
    async with get_db_connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                "DELETE FROM user_oauth_tokens WHERE user_id = %s AND provider = %s",
                (user_id, provider),
            )
            logger.info(
                f"[oauth_tokens] deleted user_id={user_id} provider={provider}"
            )


async def get_oauth_status(
    user_id: str, provider: str
) -> Dict[str, Any]:
    """
    Quick status check (no decryption).

    Returns:
        {connected: bool, account_id, email, plan_type}
    """
    async with get_db_connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                """
                SELECT account_id, email, plan_type
                FROM user_oauth_tokens
                WHERE user_id = %s AND provider = %s
                """,
                (user_id, provider),
            )
            row = await cur.fetchone()
            if row:
                return {
                    "connected": True,
                    "account_id": row["account_id"],
                    "email": row["email"],
                    "plan_type": row["plan_type"],
                }
            return {"connected": False, "account_id": None, "email": None, "plan_type": None}


_OAUTH_ACTIVE_TTL = 86400  # 24h — freshness via explicit invalidation


async def has_any_oauth_token(user_id: str) -> bool:
    """Quick check: does the user have at least one OAuth token row?

    Result is cached in Redis for up to ``_OAUTH_ACTIVE_TTL`` seconds.
    The cache is explicitly invalidated by ``invalidate_oauth_active_cache``
    whenever tokens are written/deleted.
    """
    from src.utils.cache.redis_cache import get_cache_client

    cache_key = f"oauth_active:{user_id}"
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
                "SELECT 1 FROM user_oauth_tokens WHERE user_id = %s LIMIT 1",
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
    """Delete the cached ``has_any_oauth_token`` result so the next call hits the DB."""
    from src.utils.cache.redis_cache import get_cache_client

    cache = get_cache_client()
    if cache.enabled and cache.client:
        try:
            await cache.client.delete(f"oauth_active:{user_id}")
        except Exception:
            pass
