"""Vault Secrets API Router (Plugins backing store).

CRUD for per-user encrypted secrets, the one vault every workspace of the user
resolves ``${vault:NAME}`` refs against. Convergence after a mutation is
``services/vault_invalidation``.

Endpoints:
- GET    /api/v1/mcp/vault/secrets
- GET    /api/v1/mcp/vault/blueprints
- POST   /api/v1/mcp/vault/secrets
- PUT    /api/v1/mcp/vault/secrets/{name}
- GET    /api/v1/mcp/vault/secrets/{name}/reveal
- DELETE /api/v1/mcp/vault/secrets/{name}
"""

from __future__ import annotations

import logging

from fastapi import APIRouter, HTTPException

from src.server.database.user_vault_secrets import (
    MAX_SECRETS_PER_USER,
    SecretExists,
    SecretLimitReached,
    create_user_secret,
    delete_user_secret,
    get_user_secrets,
    reveal_user_secret,
    update_user_secret,
)
from src.server.models.vault import CreateSecretRequest, UpdateSecretRequest
from src.server.services.vault_invalidation import after_secret_change
from src.server.utils.api import CurrentUserId, handle_api_exceptions

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1/mcp", tags=["User Vault Secrets"])


def _collect_config_blueprints() -> dict[str, dict]:
    """Vault blueprints declared by the enabled builtin MCP servers, by name.

    First-declaration wins on metadata; duplicate blueprint names across
    servers are treated as aliases: the second server's description/docs_url/
    regex are discarded, but its name is appended to `sources` so the UI can
    show which integrations share the credential.
    """
    # Lazy import to avoid circular dependency between `setup` module and router
    # registration. `setup.agent_config` is populated in `lifespan()` at startup.
    from src.server.app import setup

    collected: dict[str, dict] = {}
    if setup.agent_config is None:
        # Startup race: request landed before lifespan completed.
        return collected
    for server in setup.agent_config.mcp.servers:
        if not server.enabled:
            continue
        for bp in server.vault_blueprints:
            entry = collected.get(bp.name)
            if entry is None:
                collected[bp.name] = {
                    "name": bp.name,
                    "label": bp.label,
                    "description": bp.description,
                    "docs_url": bp.docs_url,
                    "regex": bp.regex,
                    "sources": [server.name],
                }
            else:
                entry["sources"].append(server.name)
    return collected


@router.get("/vault/secrets")
@handle_api_exceptions("list user vault secrets", logger)
async def list_secrets(user_id: CurrentUserId):
    secrets = await get_user_secrets(user_id)
    return {
        "secrets": secrets,
        "remaining_slots": max(0, MAX_SECRETS_PER_USER - len(secrets)),
    }


@router.get("/vault/blueprints")
@handle_api_exceptions("list user vault blueprints", logger)
async def list_blueprints(user_id: CurrentUserId):
    """The 'recommended but not yet set' credential list.

    Config blueprints (builtin MCP servers) plus every enabled plugin's
    declared ``ai.langalpha`` secrets, minus what the vault already holds.
    First declaration wins on metadata; later declarers just extend
    ``sources``.
    """
    from src.server.database.plugins import list_plugins
    from src.server.database.user_vault_secrets import get_user_secret_names
    from src.server.services.plugins.extension import NAMESPACE, parse_extension
    from src.server.services.plugins.manifest import manifest_extension

    existing_names = await get_user_secret_names(user_id)
    remaining_slots = max(0, MAX_SECRETS_PER_USER - len(existing_names))

    collected = _collect_config_blueprints()
    for plugin in await list_plugins(user_id):
        if not plugin["enabled"]:
            continue
        extension = parse_extension(
            manifest_extension(plugin.get("manifest") or {}, NAMESPACE)
        )
        for secret in extension.secrets:
            sources = sorted({b.server for b in secret.bind}) or [plugin["name"]]
            entry = collected.get(secret.name)
            if entry is None:
                collected[secret.name] = {
                    "name": secret.name,
                    "label": secret.label,
                    "description": secret.description,
                    "docs_url": secret.docs_url,
                    "regex": secret.regex,
                    "sources": sources,
                    "plugin_name": plugin["name"],
                }
            else:
                entry["sources"] = sorted(set(entry["sources"]) | set(sources))

    blueprints = [
        bp for name, bp in collected.items() if name not in existing_names
    ]
    return {"blueprints": blueprints, "remaining_slots": remaining_slots}


@router.post("/vault/secrets", status_code=201)
@handle_api_exceptions("create user vault secret", logger)
async def create_secret(body: CreateSecretRequest, user_id: CurrentUserId):
    try:
        await create_user_secret(user_id, body.name, body.value, body.description)
    except SecretExists as e:
        raise HTTPException(
            status_code=409,
            detail={"code": "secret_exists", "message": str(e), "name": body.name},
        ) from e
    except SecretLimitReached as e:
        raise HTTPException(
            status_code=409,
            detail={
                "code": "secret_limit",
                "message": str(e),
                "max": MAX_SECRETS_PER_USER,
            },
        ) from e

    await after_secret_change(user_id, body.name)
    return {"name": body.name}


@router.put("/vault/secrets/{name}")
@handle_api_exceptions("update user vault secret", logger)
async def update_secret(name: str, body: UpdateSecretRequest, user_id: CurrentUserId):
    found = await update_user_secret(
        user_id, name, value=body.value, description=body.description
    )
    if not found:
        raise HTTPException(status_code=404, detail="Secret not found")

    await after_secret_change(user_id, name, value_changed=body.value is not None)
    return {"name": name}


@router.get("/vault/secrets/{name}/reveal")
@handle_api_exceptions("reveal user vault secret", logger)
async def reveal_secret(name: str, user_id: CurrentUserId):
    value = await reveal_user_secret(user_id, name)
    if value is None:
        raise HTTPException(status_code=404, detail="Secret not found")
    return {"value": value}


@router.delete("/vault/secrets/{name}")
@handle_api_exceptions("delete user vault secret", logger)
async def delete_secret(name: str, user_id: CurrentUserId):
    found = await delete_user_secret(user_id, name)
    if not found:
        raise HTTPException(status_code=404, detail="Secret not found")

    await after_secret_change(user_id, name)
    return {"ok": True}
