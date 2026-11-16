"""Database access for each user's Firecrawl account key (``user_firecrawl_accounts``).

The key is encrypted at rest with pgcrypto, under the key the vault uses, and
only the egress relay ever reads it back.
"""

from psycopg.rows import dict_row

from src.server.database.encryption import get_encryption_key
from src.server.database.pool import get_db_connection


async def get_account_key(user_id: str) -> str | None:
    """The user's decrypted Firecrawl key, or None before one is provisioned."""
    enc_key = get_encryption_key()
    async with get_db_connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                "SELECT pgp_sym_decrypt(api_key, %s) AS api_key "
                "FROM user_firecrawl_accounts WHERE user_id = %s",
                (enc_key, user_id),
            )
            row = await cur.fetchone()
            return row["api_key"] if row else None


async def save_account(
    user_id: str, *, api_key: str, contact_email: str, gateway_status: str | None
) -> str:
    """Store a provisioned account unless the user already has one, and answer the key held.

    Firecrawl returns the same key for the same email, so two workers
    provisioning at once agree anyway; answering the stored row keeps every
    caller on the one key held.
    """
    enc_key = get_encryption_key()
    async with get_db_connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                """
                INSERT INTO user_firecrawl_accounts
                    (user_id, api_key, contact_email, gateway_status)
                VALUES (%s, pgp_sym_encrypt(%s, %s), %s, %s)
                ON CONFLICT (user_id) DO NOTHING
                """,
                (user_id, api_key, enc_key, contact_email, gateway_status),
            )
            await cur.execute(
                "SELECT pgp_sym_decrypt(api_key, %s) AS api_key "
                "FROM user_firecrawl_accounts WHERE user_id = %s",
                (enc_key, user_id),
            )
            row = await cur.fetchone()
            return row["api_key"]


async def replace_account_key(user_id: str, *, rejected: str, api_key: str) -> str | None:
    """Store ``api_key`` in place of ``rejected``, and answer the key held.

    Only while ``rejected`` is still the stored key, so a slow worker cannot
    write over a newer key with the one it fetched; two workers saving the
    same replacement both answer it. None when the row is gone.
    """
    enc_key = get_encryption_key()
    async with get_db_connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                """
                UPDATE user_firecrawl_accounts
                SET api_key = pgp_sym_encrypt(%s, %s), updated_at = NOW()
                WHERE user_id = %s AND pgp_sym_decrypt(api_key, %s) = %s
                """,
                (api_key, enc_key, user_id, enc_key, rejected),
            )
            await cur.execute(
                "SELECT pgp_sym_decrypt(api_key, %s) AS api_key "
                "FROM user_firecrawl_accounts WHERE user_id = %s",
                (enc_key, user_id),
            )
            row = await cur.fetchone()
            return row["api_key"] if row else None
