"""
Database CRUD for vault secrets, workspace- and user-scoped.

Both tiers are the same table shape under the same pgcrypto-at-rest scheme
(pgp_sym_encrypt/decrypt), so the SQL lives here once and is parameterized by a
``_VaultTier`` descriptor; ``user_vault_secrets`` holds the user tier and its
own public names. Encryption is transparent to callers — functions accept and
return plaintext.
"""

import logging
from dataclasses import dataclass
from typing import Any

from psycopg.rows import dict_row

from src.server.database.pool import get_db_connection
from src.server.database.encryption import encryption_configured, get_encryption_key as _get_encryption_key

logger = logging.getLogger(__name__)

# Hard limit on secrets per workspace
MAX_SECRETS_PER_WORKSPACE = 20

# SQL identifiers can never be bound parameters, so a tier's table/column names
# are f-string interpolated into the statements below. These allowlists are what
# makes that safe: a tier is only constructible from names that exist here.
_ALLOWED_TABLES = frozenset({"workspace_vault_secrets", "user_vault_secrets"})
_ALLOWED_COLUMNS = frozenset(
    {
        "workspace_id",
        "user_id",
        "workspace_vault_secret_id",
        "user_vault_secret_id",
    }
)


@dataclass(frozen=True)
class _VaultTier:
    """The identifiers and limits that distinguish one vault tier from another."""

    table: str
    owner_col: str
    id_col: str
    max_secrets: int
    label: str
    log_prefix: str
    # Trailing phrase on the duplicate-name error; the tiers word it differently.
    duplicate_suffix: str = ""

    def __post_init__(self) -> None:
        if self.table not in _ALLOWED_TABLES:
            raise ValueError(f"Unknown vault table: {self.table!r}")
        for col in (self.owner_col, self.id_col):
            if col not in _ALLOWED_COLUMNS:
                raise ValueError(f"Unknown vault column: {col!r}")


WORKSPACE_TIER = _VaultTier(
    table="workspace_vault_secrets",
    owner_col="workspace_id",
    id_col="workspace_vault_secret_id",
    max_secrets=MAX_SECRETS_PER_WORKSPACE,
    label="workspace",
    log_prefix="[vault_db]",
    duplicate_suffix=" in this workspace",
)


# ---------------------------------------------------------------------------
# Tier-parameterized implementations
# ---------------------------------------------------------------------------


async def _list(tier: _VaultTier, owner_id: str) -> list[dict[str, Any]]:
    enc_key = _get_encryption_key()
    async with get_db_connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                f"""
                SELECT {tier.id_col}, name, description,
                       pgp_sym_decrypt(value, %s) AS plaintext,
                       created_at, updated_at
                FROM {tier.table}
                WHERE {tier.owner_col} = %s
                ORDER BY name
                """,
                (enc_key, owner_id),
            )
            rows = await cur.fetchall()
            return [
                {
                    tier.id_col: str(r[tier.id_col]),
                    "name": r["name"],
                    "description": r["description"] or "",
                    "masked_value": _mask(r["plaintext"]),
                    "created_at": r["created_at"].isoformat(),
                    "updated_at": r["updated_at"].isoformat(),
                }
                for r in rows
            ]


async def _reveal(tier: _VaultTier, owner_id: str, name: str) -> str | None:
    enc_key = _get_encryption_key()
    async with get_db_connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                f"""
                SELECT pgp_sym_decrypt(value, %s) AS plaintext
                FROM {tier.table}
                WHERE {tier.owner_col} = %s AND name = %s
                """,
                (enc_key, owner_id, name),
            )
            row = await cur.fetchone()
            return row["plaintext"] if row else None


async def _decrypted(tier: _VaultTier, owner_id: str) -> dict[str, str]:
    enc_key = _get_encryption_key()
    async with get_db_connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                f"""
                SELECT name, pgp_sym_decrypt(value, %s) AS plaintext
                FROM {tier.table}
                WHERE {tier.owner_col} = %s
                """,
                (enc_key, owner_id),
            )
            rows = await cur.fetchall()
            return {r["name"]: r["plaintext"] for r in rows}


async def _names(tier: _VaultTier, owner_id: str) -> set[str]:
    async with get_db_connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                f"SELECT name FROM {tier.table} WHERE {tier.owner_col} = %s",
                (owner_id,),
            )
            rows = await cur.fetchall()
            return {r["name"] for r in rows}


async def _create(
    tier: _VaultTier,
    owner_id: str,
    name: str,
    value: str,
    description: str = "",
    *,
    conn=None,
) -> None:
    enc_key = _get_encryption_key()
    async with get_db_connection(conn) as conn:
        async with conn.transaction():
            async with conn.cursor(row_factory=dict_row) as cur:
                # Serialize concurrent creates for the same owner
                await cur.execute(
                    "SELECT pg_advisory_xact_lock(hashtext(%s))",
                    (owner_id,),
                )
                await cur.execute(
                    f"SELECT COUNT(*) AS cnt FROM {tier.table} "
                    f"WHERE {tier.owner_col} = %s",
                    (owner_id,),
                )
                row = await cur.fetchone()
                if row["cnt"] >= tier.max_secrets:
                    raise ValueError(
                        f"Maximum of {tier.max_secrets} secrets "
                        f"per {tier.label} reached"
                    )

                await cur.execute(
                    f"""
                    INSERT INTO {tier.table}
                        ({tier.owner_col}, name, value, description, created_at, updated_at)
                    VALUES (%s, %s, pgp_sym_encrypt(%s, %s), %s, NOW(), NOW())
                    ON CONFLICT ({tier.owner_col}, name) DO NOTHING
                    RETURNING {tier.id_col}
                    """,
                    (owner_id, name, value, enc_key, description),
                )
                inserted = await cur.fetchone()
                if not inserted:
                    raise ValueError(
                        f"Secret with name {name!r} already exists"
                        f"{tier.duplicate_suffix}"
                    )
                logger.info(
                    f"{tier.log_prefix} create_secret "
                    f"{tier.owner_col}={owner_id} name={name}"
                )


async def _update(
    tier: _VaultTier,
    owner_id: str,
    name: str,
    *,
    value: str | None = None,
    description: str | None = None,
) -> bool:
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
            params.extend([owner_id, name])

            await cur.execute(
                f"UPDATE {tier.table} SET {', '.join(parts)} "
                f"WHERE {tier.owner_col} = %s AND name = %s",
                params,
            )
            if cur.rowcount == 0:
                return False
            logger.info(
                f"{tier.log_prefix} update_secret "
                f"{tier.owner_col}={owner_id} name={name}"
            )
            return True


async def _delete(tier: _VaultTier, owner_id: str, name: str) -> bool:
    async with get_db_connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                f"DELETE FROM {tier.table} WHERE {tier.owner_col} = %s AND name = %s",
                (owner_id, name),
            )
            if cur.rowcount == 0:
                return False
            logger.info(
                f"{tier.log_prefix} delete_secret "
                f"{tier.owner_col}={owner_id} name={name}"
            )
            return True


def _mask(value: str) -> str:
    """Mask a secret value for display: show first 3 and last 4 chars."""
    if len(value) <= 8:
        return "••••••••"
    return value[:3] + "••••" + value[-4:]


# ---------------------------------------------------------------------------
# Workspace tier — public API
# ---------------------------------------------------------------------------


async def get_workspace_secrets(workspace_id: str) -> list[dict[str, Any]]:
    """List all secrets for a workspace (decrypted server-side for masking)."""
    return await _list(WORKSPACE_TIER, workspace_id)


async def reveal_secret(workspace_id: str, name: str) -> str | None:
    """Return the plaintext value of a single secret, or None if not found."""
    return await _reveal(WORKSPACE_TIER, workspace_id, name)


async def get_workspace_secrets_decrypted(workspace_id: str) -> dict[str, str]:
    """Return {name: plaintext_value} for sandbox injection."""
    return await _decrypted(WORKSPACE_TIER, workspace_id)


async def get_workspace_secret_names(workspace_id: str) -> set[str]:
    """Return the set of secret names for a workspace. No decryption.

    Used by the vault-blueprints endpoint to compute which declared credentials
    are not yet set without paying the pgcrypto cost of the masking query.
    """
    return await _names(WORKSPACE_TIER, workspace_id)


async def create_secret(
    workspace_id: str, name: str, value: str, description: str = "", *, conn=None
) -> None:
    """Insert a new secret (encrypted). Raises ValueError on duplicate or limit."""
    await _create(WORKSPACE_TIER, workspace_id, name, value, description, conn=conn)


async def update_secret(
    workspace_id: str,
    name: str,
    *,
    value: str | None = None,
    description: str | None = None,
) -> bool:
    """Partial update of a secret. Returns True if row was found."""
    return await _update(
        WORKSPACE_TIER, workspace_id, name, value=value, description=description
    )


async def delete_secret(workspace_id: str, name: str) -> bool:
    """Delete a secret by name. Returns True if row existed."""
    return await _delete(WORKSPACE_TIER, workspace_id, name)


# ---------------------------------------------------------------------------
# Both tiers — the merge rule
# ---------------------------------------------------------------------------


async def get_effective_secrets(
    workspace_id: str, user_id: str | None = None
) -> dict[str, str]:
    """The secret set a workspace actually sees: the owner's user-level secrets
    shadowed by the workspace's own.

    The one definition of the merge, because its two consumers must agree — the
    sandbox push decides what a server can authenticate with, the redactor
    decides what gets scrubbed out of files. A redactor answering
    workspace-only leaves an inherited server's credential in the clear.

    ``user_id`` is read off the workspace row when omitted; callers that
    already hold the owner should pass it.
    """
    # A deployment without the encryption key cannot have written any vault
    # secret (every write encrypts with it), so "no secrets" is the true
    # answer here — unlike a lookup failure, which propagates.
    if not encryption_configured():
        return {}

    # Imported here: the user tier's module is built on this one's internals,
    # so a top-level import would close the cycle.
    from src.server.database.user_vault_secrets import get_user_secrets_decrypted

    if user_id is None:
        from src.server.database.workspace import get_workspace

        workspace = await get_workspace(workspace_id)
        user_id = (workspace or {}).get("user_id")

    secrets = await _decrypted(WORKSPACE_TIER, workspace_id)
    if not user_id:
        return secrets
    user_secrets = await get_user_secrets_decrypted(user_id)
    return {**user_secrets, **secrets} if user_secrets else secrets
