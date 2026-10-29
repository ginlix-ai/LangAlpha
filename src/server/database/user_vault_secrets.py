"""
Database CRUD for the vault: a user's secrets, shared by all of their workspaces.

Values are encrypted at rest with pgcrypto (pgp_sym_encrypt/decrypt).
Encryption is transparent to callers: functions accept and return plaintext.
"""

import logging
from collections.abc import Collection
from typing import Any

from psycopg.rows import dict_row

from src.server.database.encryption import (
    encryption_configured,
    get_encryption_key as _get_encryption_key,
)
from src.server.database.pool import get_db_connection
from src.server.database.user_lock import lock_user_writes

logger = logging.getLogger(__name__)

MAX_SECRETS_PER_USER = 50


class SecretExists(ValueError):
    """The account already holds a secret by this name."""


class SecretLimitReached(ValueError):
    """The account already holds ``MAX_SECRETS_PER_USER`` secrets."""


async def get_user_secrets(user_id: str) -> list[dict[str, Any]]:
    """List all secrets for a user (decrypted server-side for masking)."""
    enc_key = _get_encryption_key()
    async with get_db_connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                """
                SELECT user_vault_secret_id, name, description,
                       pgp_sym_decrypt(value, %s) AS plaintext,
                       created_at, updated_at
                FROM user_vault_secrets
                WHERE user_id = %s
                ORDER BY name
                """,
                (enc_key, user_id),
            )
            rows = await cur.fetchall()
            return [
                {
                    "user_vault_secret_id": str(r["user_vault_secret_id"]),
                    "name": r["name"],
                    "description": r["description"] or "",
                    "masked_value": _mask(r["plaintext"]),
                    "created_at": r["created_at"].isoformat(),
                    "updated_at": r["updated_at"].isoformat(),
                }
                for r in rows
            ]


async def reveal_user_secret(user_id: str, name: str) -> str | None:
    """Return the plaintext value of a single secret, or None if not found."""
    enc_key = _get_encryption_key()
    async with get_db_connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                """
                SELECT pgp_sym_decrypt(value, %s) AS plaintext
                FROM user_vault_secrets
                WHERE user_id = %s AND name = %s
                """,
                (enc_key, user_id, name),
            )
            row = await cur.fetchone()
            return row["plaintext"] if row else None


async def get_user_secrets_decrypted(
    user_id: str, names: Collection[str] | None = None
) -> dict[str, str]:
    """Return {name: plaintext_value} for sandbox injection, or only ``names``.

    The one secret set every consumer reads: the sandbox push decides what a
    server can authenticate with, the redactor what gets scrubbed from output,
    and the two must agree. Each row decrypted is a full S2K derivation, so a
    caller that knows which names it needs (the relay, per request) names
    them rather than paying for the whole vault.
    """
    only = "" if names is None else " AND name = ANY(%s)"
    scope = (user_id,) if names is None else (user_id, list(names))
    async with get_db_connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            # Without the key, "no secrets" is true only when none is stored.
            # A restored database or a key dropped from the config still holds
            # rows, and reading them as absent would leave the redactor
            # scrubbing nothing, so those fail on the missing key instead.
            if not encryption_configured():
                await cur.execute(
                    f"SELECT 1 FROM user_vault_secrets WHERE user_id = %s{only} LIMIT 1",
                    scope,
                )
                if await cur.fetchone() is None:
                    return {}
            enc_key = _get_encryption_key()
            await cur.execute(
                f"""
                SELECT name, pgp_sym_decrypt(value, %s) AS plaintext
                FROM user_vault_secrets
                WHERE user_id = %s{only}
                """,
                (enc_key, *scope),
            )
            rows = await cur.fetchall()
            return {r["name"]: r["plaintext"] for r in rows}


# Over the ciphertext, which every write re-salts, so noticing a change costs
# no decrypt. An empty vault reads as ''.
_VAULT_FINGERPRINT = (
    "COALESCE(md5(string_agg(name || ':' || md5(value), ',' ORDER BY name)), '')"
)


async def get_user_vault_snapshot(user_id: str) -> tuple[dict[str, str], str]:
    """The whole vault decrypted, with the fingerprint of the rows it came from.

    One statement reads both, so they describe one committed state: a writer
    whose fingerprint still matches :func:`get_user_vault_fingerprint` after
    it published knows what it wrote is the vault.
    """
    if not encryption_configured():  # {} when nothing is stored, else its error
        return await get_user_secrets_decrypted(user_id), ""
    params = {"key": _get_encryption_key(), "user_id": user_id}
    async with get_db_connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                f"""
                SELECT fp.fingerprint, v.name,
                       pgp_sym_decrypt(v.value, %(key)s) AS plaintext
                FROM (SELECT {_VAULT_FINGERPRINT} AS fingerprint
                      FROM user_vault_secrets WHERE user_id = %(user_id)s) fp
                LEFT JOIN user_vault_secrets v ON v.user_id = %(user_id)s
                """,
                params,
            )
            rows = await cur.fetchall()
    secrets = {r["name"]: r["plaintext"] for r in rows if r["name"] is not None}
    return secrets, rows[0]["fingerprint"]


async def get_user_vault_fingerprint(user_id: str) -> str:
    """Moves on every committed create, value write or delete; a description edit leaves it."""
    async with get_db_connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                f"SELECT {_VAULT_FINGERPRINT} AS fingerprint "
                "FROM user_vault_secrets WHERE user_id = %s",
                (user_id,),
            )
            return (await cur.fetchone())["fingerprint"]


async def get_user_secret_names(user_id: str) -> set[str]:
    """Return the set of secret names for a user. No decryption."""
    async with get_db_connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                "SELECT name FROM user_vault_secrets WHERE user_id = %s",
                (user_id,),
            )
            rows = await cur.fetchall()
            return {r["name"] for r in rows}


async def create_user_secret(
    user_id: str, name: str, value: str, description: str = "", *, conn=None
) -> None:
    """Insert a new secret (encrypted). Raises ValueError on duplicate or limit."""
    enc_key = _get_encryption_key()
    async with get_db_connection(conn) as conn:
        async with conn.transaction():
            async with conn.cursor(row_factory=dict_row) as cur:
                # Serialize concurrent creates for the same user
                await lock_user_writes(cur, user_id)
                await cur.execute(
                    "SELECT COUNT(*) AS cnt FROM user_vault_secrets "
                    "WHERE user_id = %s",
                    (user_id,),
                )
                row = await cur.fetchone()
                if row["cnt"] >= MAX_SECRETS_PER_USER:
                    raise SecretLimitReached(
                        f"Maximum of {MAX_SECRETS_PER_USER} secrets per user reached"
                    )

                await cur.execute(
                    """
                    INSERT INTO user_vault_secrets
                        (user_id, name, value, description, created_at, updated_at)
                    VALUES (%s, %s, pgp_sym_encrypt(%s, %s), %s, NOW(), NOW())
                    ON CONFLICT (user_id, name) DO NOTHING
                    RETURNING user_vault_secret_id
                    """,
                    (user_id, name, value, enc_key, description),
                )
                inserted = await cur.fetchone()
                if not inserted:
                    raise SecretExists(f"Secret with name {name!r} already exists")
                logger.info(
                    f"[user_vault_db] create_secret user_id={user_id} name={name}"
                )


async def update_user_secret(
    user_id: str,
    name: str,
    *,
    value: str | None = None,
    description: str | None = None,
) -> bool:
    """Partial update of a secret. Returns True if row was found."""
    if value is None and description is None:
        return True  # nothing to update

    enc_key = _get_encryption_key()
    async with get_db_connection() as conn:
        async with conn.cursor() as cur:
            parts: list[str] = []
            params: list[Any] = []
            if value is not None:
                parts.append("value = pgp_sym_encrypt(%s, %s)")
                params.extend([value, enc_key])
            if description is not None:
                parts.append("description = %s")
                params.append(description)
            parts.append("updated_at = NOW()")
            params.extend([user_id, name])

            await cur.execute(
                f"UPDATE user_vault_secrets SET {', '.join(parts)} "
                "WHERE user_id = %s AND name = %s",
                params,
            )
            if cur.rowcount == 0:
                return False
            logger.info(
                f"[user_vault_db] update_secret user_id={user_id} name={name}"
            )
            return True


async def delete_user_secret(user_id: str, name: str) -> bool:
    """Delete a secret by name. Returns True if row existed."""
    async with get_db_connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                "DELETE FROM user_vault_secrets WHERE user_id = %s AND name = %s",
                (user_id, name),
            )
            if cur.rowcount == 0:
                return False
            logger.info(
                f"[user_vault_db] delete_secret user_id={user_id} name={name}"
            )
            return True


def _mask(value: str) -> str:
    """Mask a secret value for display: show first 3 and last 4 chars."""
    if len(value) <= 8:
        return "••••••••"
    return value[:3] + "••••" + value[-4:]
