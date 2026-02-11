"""
Database CRUD for user API keys (BYOK support).

Normalized schema: one row per (user_id, provider) in user_api_keys,
plus a byok_enabled boolean on the users table.

All API keys are encrypted at rest using pgcrypto (pgp_sym_encrypt/decrypt).
Encryption is transparent to callers — functions accept and return plaintext strings.
"""

import logging
import os
from typing import Any, Dict, Optional

from psycopg.rows import dict_row

from src.server.database.conversation import get_db_connection

logger = logging.getLogger(__name__)


def _get_encryption_key() -> str:
    """Return the symmetric encryption key for API key storage."""
    key = os.getenv("BYOK_ENCRYPTION_KEY")
    if not key:
        raise RuntimeError(
            "BYOK_ENCRYPTION_KEY environment variable is not set. "
            "Required for encrypting user API keys at rest."
        )
    return key


async def get_user_api_keys(user_id: str) -> Dict[str, Any]:
    """
    Get user's BYOK configuration: toggle + all provider keys (decrypted).

    Returns:
        { byok_enabled: bool, keys: { provider: api_key_plaintext, ... } }
    """
    enc_key = _get_encryption_key()
    async with get_db_connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            # Fetch byok toggle from users table
            await cur.execute(
                "SELECT byok_enabled FROM users WHERE user_id = %s",
                (user_id,),
            )
            user_row = await cur.fetchone()
            byok_enabled = bool(user_row["byok_enabled"]) if user_row else False

            # Fetch all provider keys (decrypted)
            await cur.execute(
                "SELECT provider, pgp_sym_decrypt(api_key, %s) AS api_key "
                "FROM user_api_keys WHERE user_id = %s ORDER BY provider",
                (enc_key, user_id),
            )
            rows = await cur.fetchall()
            keys = {row["provider"]: row["api_key"] for row in rows}

            return {"byok_enabled": byok_enabled, "keys": keys}


async def set_byok_enabled(user_id: str, enabled: bool) -> bool:
    """
    Set the global BYOK toggle on the users table.

    Returns:
        The new byok_enabled value.
    """
    async with get_db_connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                "UPDATE users SET byok_enabled = %s, updated_at = NOW() WHERE user_id = %s RETURNING byok_enabled",
                (enabled, user_id),
            )
            result = await cur.fetchone()
            logger.info(f"[api_keys_db] set_byok_enabled user_id={user_id} enabled={enabled}")
            return bool(result["byok_enabled"]) if result else False


async def upsert_api_key(user_id: str, provider: str, api_key: str) -> None:
    """
    Insert or update a single provider key (encrypted).
    """
    enc_key = _get_encryption_key()
    async with get_db_connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                """
                INSERT INTO user_api_keys (user_id, provider, api_key, created_at, updated_at)
                VALUES (%s, %s, pgp_sym_encrypt(%s, %s), NOW(), NOW())
                ON CONFLICT (user_id, provider) DO UPDATE
                SET api_key = EXCLUDED.api_key,
                    updated_at = NOW()
                """,
                (user_id, provider, api_key, enc_key),
            )
            logger.info(f"[api_keys_db] upsert_key user_id={user_id} provider={provider}")


async def delete_api_key(user_id: str, provider: str) -> None:
    """
    Remove one provider key.
    """
    async with get_db_connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                "DELETE FROM user_api_keys WHERE user_id = %s AND provider = %s",
                (user_id, provider),
            )
            logger.info(f"[api_keys_db] delete_key user_id={user_id} provider={provider}")


async def get_key_for_provider(user_id: str, provider: str) -> Optional[str]:
    """
    Quick lookup: return the decrypted API key for a specific provider, or None.
    """
    enc_key = _get_encryption_key()
    async with get_db_connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                "SELECT pgp_sym_decrypt(api_key, %s) AS api_key "
                "FROM user_api_keys WHERE user_id = %s AND provider = %s",
                (enc_key, user_id, provider),
            )
            row = await cur.fetchone()
            return row["api_key"] if row else None


async def is_byok_active(user_id: str) -> bool:
    """
    Quick check: is BYOK enabled AND does the user have at least one key?
    """
    async with get_db_connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                """
                SELECT 1 FROM users u
                WHERE u.user_id = %s
                  AND u.byok_enabled = TRUE
                  AND EXISTS (
                      SELECT 1 FROM user_api_keys k WHERE k.user_id = u.user_id
                  )
                LIMIT 1
                """,
                (user_id,),
            )
            return (await cur.fetchone()) is not None


async def get_byok_key_for_provider(user_id: str, provider: str) -> Optional[str]:
    """
    Combined query: return the decrypted API key only if BYOK is enabled.

    Returns None if BYOK is disabled OR no key exists for this provider.
    Saves a round-trip vs calling is_byok_active() + get_key_for_provider() separately.
    """
    enc_key = _get_encryption_key()
    async with get_db_connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                """
                SELECT pgp_sym_decrypt(k.api_key, %s) AS api_key
                FROM user_api_keys k
                JOIN users u ON u.user_id = k.user_id
                WHERE k.user_id = %s AND k.provider = %s AND u.byok_enabled = TRUE
                """,
                (enc_key, user_id, provider),
            )
            row = await cur.fetchone()
            return row["api_key"] if row else None
