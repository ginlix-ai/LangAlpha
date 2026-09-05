"""
Database CRUD for user-level vault secrets.

Same pgcrypto-at-rest pattern as workspace_vault_secrets — the SQL is shared,
parameterized by the tier descriptor below. User secrets are merged with
workspace secrets at sandbox push, workspace winning on name collision.
"""

from typing import Any

from src.server.database.vault_secrets import (
    _VaultTier,
    _create,
    _decrypted,
    _delete,
    _list,
    _names,
    _reveal,
    _update,
)

MAX_SECRETS_PER_USER = 50

USER_TIER = _VaultTier(
    table="user_vault_secrets",
    owner_col="user_id",
    id_col="user_vault_secret_id",
    max_secrets=MAX_SECRETS_PER_USER,
    label="user",
    log_prefix="[user_vault_db]",
)


async def get_user_secrets(user_id: str) -> list[dict[str, Any]]:
    """List all secrets for a user (decrypted server-side for masking)."""
    return await _list(USER_TIER, user_id)


async def reveal_user_secret(user_id: str, name: str) -> str | None:
    """Return the plaintext value of a single secret, or None if not found."""
    return await _reveal(USER_TIER, user_id, name)


async def get_user_secrets_decrypted(user_id: str) -> dict[str, str]:
    """Return {name: plaintext_value} for sandbox injection."""
    return await _decrypted(USER_TIER, user_id)


async def get_user_secret_names(user_id: str) -> set[str]:
    """Return the set of secret names for a user. No decryption."""
    return await _names(USER_TIER, user_id)


async def create_user_secret(
    user_id: str, name: str, value: str, description: str = "", *, conn=None
) -> None:
    """Insert a new secret (encrypted). Raises ValueError on duplicate or limit."""
    await _create(USER_TIER, user_id, name, value, description, conn=conn)


async def update_user_secret(
    user_id: str,
    name: str,
    *,
    value: str | None = None,
    description: str | None = None,
) -> bool:
    """Partial update of a secret. Returns True if row was found."""
    return await _update(
        USER_TIER, user_id, name, value=value, description=description
    )


async def delete_user_secret(user_id: str, name: str) -> bool:
    """Delete a secret by name. Returns True if row existed."""
    return await _delete(USER_TIER, user_id, name)
