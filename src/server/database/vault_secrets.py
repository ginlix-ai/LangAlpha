"""
Database CRUD for per-workspace vault secrets.

Secrets are encrypted at rest using pgcrypto (pgp_sym_encrypt/decrypt).
Encryption is transparent to callers — functions accept and return plaintext.
"""

import logging
from typing import Any

from psycopg.rows import dict_row

from src.server.database.conversation import get_db_connection
from src.server.database.encryption import get_encryption_key as _get_encryption_key

logger = logging.getLogger(__name__)

# Hard limit on secrets per workspace
MAX_SECRETS_PER_WORKSPACE = 20


async def get_workspace_secrets(workspace_id: str) -> list[dict[str, Any]]:
    """List all secrets for a workspace (decrypted server-side for masking)."""
    enc_key = _get_encryption_key()
    async with get_db_connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                """
                SELECT workspace_vault_secret_id, name, description,
                       pgp_sym_decrypt(value, %s) AS plaintext,
                       created_at, updated_at
                FROM workspace_vault_secrets
                WHERE workspace_id = %s
                ORDER BY name
                """,
                (enc_key, workspace_id),
            )
            rows = await cur.fetchall()
            return [
                {
                    "workspace_vault_secret_id": str(r["workspace_vault_secret_id"]),
                    "name": r["name"],
                    "description": r["description"] or "",
                    "masked_value": _mask(r["plaintext"]),
                    "created_at": r["created_at"].isoformat(),
                    "updated_at": r["updated_at"].isoformat(),
                }
                for r in rows
            ]


async def reveal_secret(workspace_id: str, name: str) -> str | None:
    """Return the plaintext value of a single secret, or None if not found."""
    enc_key = _get_encryption_key()
    async with get_db_connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                """
                SELECT pgp_sym_decrypt(value, %s) AS plaintext
                FROM workspace_vault_secrets
                WHERE workspace_id = %s AND name = %s
                """,
                (enc_key, workspace_id, name),
            )
            row = await cur.fetchone()
            return row["plaintext"] if row else None


async def get_workspace_secrets_decrypted(workspace_id: str) -> dict[str, str]:
    """Return {name: plaintext_value} for sandbox injection."""
    enc_key = _get_encryption_key()
    async with get_db_connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                """
                SELECT name, pgp_sym_decrypt(value, %s) AS plaintext
                FROM workspace_vault_secrets
                WHERE workspace_id = %s
                """,
                (enc_key, workspace_id),
            )
            rows = await cur.fetchall()
            return {r["name"]: r["plaintext"] for r in rows}


async def get_workspace_secret_names(workspace_id: str) -> set[str]:
    """Return the set of secret names for a workspace. No decryption.

    Used by the vault-blueprints endpoint to compute which declared credentials
    are not yet set without paying the pgcrypto cost of the masking query.
    """
    async with get_db_connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                "SELECT name FROM workspace_vault_secrets WHERE workspace_id = %s",
                (workspace_id,),
            )
            rows = await cur.fetchall()
            return {r["name"] for r in rows}


async def create_secret(
    workspace_id: str, name: str, value: str, description: str = ""
) -> None:
    """Insert a new secret (encrypted). Raises ValueError on duplicate or limit."""
    enc_key = _get_encryption_key()
    async with get_db_connection() as conn:
        async with conn.transaction():
            async with conn.cursor(row_factory=dict_row) as cur:
                # Serialize concurrent creates for the same workspace
                await cur.execute(
                    "SELECT pg_advisory_xact_lock(hashtext(%s))",
                    (workspace_id,),
                )
                await cur.execute(
                    "SELECT COUNT(*) AS cnt FROM workspace_vault_secrets WHERE workspace_id = %s",
                    (workspace_id,),
                )
                row = await cur.fetchone()
                if row["cnt"] >= MAX_SECRETS_PER_WORKSPACE:
                    raise ValueError(
                        f"Maximum of {MAX_SECRETS_PER_WORKSPACE} secrets per workspace reached"
                    )

                await cur.execute(
                    """
                    INSERT INTO workspace_vault_secrets
                        (workspace_id, name, value, description, created_at, updated_at)
                    VALUES (%s, %s, pgp_sym_encrypt(%s, %s), %s, NOW(), NOW())
                    ON CONFLICT (workspace_id, name) DO NOTHING
                    RETURNING workspace_vault_secret_id
                    """,
                    (workspace_id, name, value, enc_key, description),
                )
                inserted = await cur.fetchone()
                if not inserted:
                    raise ValueError(
                        f"Secret with name {name!r} already exists in this workspace"
                    )
                logger.info(
                    f"[vault_db] create_secret workspace_id={workspace_id} name={name}"
                )


async def update_secret(
    workspace_id: str,
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
            params.extend([workspace_id, name])

            await cur.execute(
                f"UPDATE workspace_vault_secrets SET {', '.join(parts)} "
                "WHERE workspace_id = %s AND name = %s",
                params,
            )
            if cur.rowcount == 0:
                return False
            logger.info(
                f"[vault_db] update_secret workspace_id={workspace_id} name={name}"
            )
            return True


async def delete_secret(workspace_id: str, name: str) -> bool:
    """Delete a secret by name. Returns True if row existed."""
    async with get_db_connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                "DELETE FROM workspace_vault_secrets WHERE workspace_id = %s AND name = %s",
                (workspace_id, name),
            )
            if cur.rowcount == 0:
                return False
            logger.info(
                f"[vault_db] delete_secret workspace_id={workspace_id} name={name}"
            )
            return True


def _mask(value: str) -> str:
    """Mask a secret value for display: show first 3 and last 4 chars."""
    if len(value) <= 8:
        return "••••••••"
    return value[:3] + "••••" + value[-4:]
