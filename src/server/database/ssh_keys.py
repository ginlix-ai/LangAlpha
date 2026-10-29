"""Database access for each user's SSH client key (``user_ssh_keys``).

The private half is encrypted at rest with pgcrypto, under the key the vault
uses; the public half and its fingerprint are stored plain, since they are
what the user is shown.
"""

from typing import Any, Dict, Optional

from psycopg.rows import dict_row

from src.server.database.encryption import get_encryption_key
from src.server.database.pool import get_db_connection


async def get_user_ssh_public_key(user_id: str) -> Optional[Dict[str, Any]]:
    """The user's public key and fingerprint, or None before one is minted. No decryption."""
    async with get_db_connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                "SELECT public_key, fingerprint, created_at "
                "FROM user_ssh_keys WHERE user_id = %s",
                (user_id,),
            )
            return await cur.fetchone()


async def insert_user_ssh_key(
    user_id: str, *, public_key: str, fingerprint: str, private_key: str
) -> Dict[str, Any]:
    """Store a minted key unless the user already holds one, and answer the one held.

    Two workers minting for the same user at once both insert; the loser's key
    is dropped by the conflict and it answers the winner's, so the user only
    ever sees one key.
    """
    enc_key = get_encryption_key()
    async with get_db_connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                """
                INSERT INTO user_ssh_keys
                    (user_id, public_key, fingerprint, private_key)
                VALUES (%s, %s, %s, pgp_sym_encrypt(%s, %s))
                ON CONFLICT (user_id) DO NOTHING
                """,
                (user_id, public_key, fingerprint, private_key, enc_key),
            )
            await cur.execute(
                "SELECT public_key, fingerprint, created_at "
                "FROM user_ssh_keys WHERE user_id = %s",
                (user_id,),
            )
            return await cur.fetchone()


async def get_user_ssh_private_key(user_id: str) -> Optional[str]:
    """The user's decrypted private key, or None when they never minted one."""
    enc_key = get_encryption_key()
    async with get_db_connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                "SELECT pgp_sym_decrypt(private_key, %s) AS private_key "
                "FROM user_ssh_keys WHERE user_id = %s",
                (enc_key, user_id),
            )
            row = await cur.fetchone()
            return row["private_key"] if row else None
