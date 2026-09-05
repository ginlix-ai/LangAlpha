"""Workspace Vault Secrets API Router.

CRUD for per-workspace encrypted secrets. Convergence after a mutation (the
sandbox push and the MCP cache invalidation) is ``services/vault_invalidation``,
shared with the user tier.

Endpoints:
- GET    /api/v1/workspaces/{workspace_id}/vault/secrets
- POST   /api/v1/workspaces/{workspace_id}/vault/secrets
- PUT    /api/v1/workspaces/{workspace_id}/vault/secrets/{name}
- GET    /api/v1/workspaces/{workspace_id}/vault/secrets/{name}/reveal
- DELETE /api/v1/workspaces/{workspace_id}/vault/secrets/{name}
- GET    /api/v1/workspaces/{workspace_id}/vault/blueprints
"""

from __future__ import annotations

import logging

from fastapi import APIRouter, HTTPException

from src.server.database.vault_secrets import (
    MAX_SECRETS_PER_WORKSPACE,
    create_secret as create_secret_db,
    delete_secret,
    get_workspace_secret_names,
    get_workspace_secrets,
    reveal_secret as reveal_secret_db,
    update_secret,
)
from src.server.database.workspace import get_workspace as db_get_workspace
from src.server.models.vault import CreateSecretRequest, UpdateSecretRequest
from src.server.services.vault_invalidation import WORKSPACE_TIER, after_secret_change
from src.server.utils.api import CurrentUserId, handle_api_exceptions, require_workspace_owner

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1/workspaces", tags=["Vault Secrets"])


def _collect_config_blueprints() -> dict[str, dict]:
    """Vault blueprints declared by the enabled builtin MCP servers, by name.

    First-declaration wins on metadata; duplicate blueprint names across
    servers are treated as aliases: the second server's description/docs_url/
    regex are discarded, but its name is appended to `sources` so the UI can
    show which integrations share the credential.

    Shared with the user tier, which layers each enabled plugin's declared
    secrets on top of this same dict.
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


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------

@router.get("/{workspace_id}/vault/secrets")
@handle_api_exceptions("list vault secrets", logger)
async def list_secrets(workspace_id: str, user_id: CurrentUserId):
    workspace = await db_get_workspace(workspace_id)
    require_workspace_owner(workspace, user_id=user_id)
    secrets = await get_workspace_secrets(workspace_id)
    return {"secrets": secrets}


@router.post("/{workspace_id}/vault/secrets", status_code=201)
@handle_api_exceptions("create vault secret", logger)
async def create_secret(
    workspace_id: str, body: CreateSecretRequest, user_id: CurrentUserId,
):
    workspace = await db_get_workspace(workspace_id)
    require_workspace_owner(workspace, user_id=user_id)

    try:
        await create_secret_db(workspace_id, body.name, body.value, body.description)
    except ValueError as e:
        raise HTTPException(status_code=409, detail=str(e))

    await after_secret_change(
        WORKSPACE_TIER, workspace_id, body.name, user_id=user_id
    )
    return {"name": body.name}


@router.put("/{workspace_id}/vault/secrets/{name}")
@handle_api_exceptions("update vault secret", logger)
async def update_secret_endpoint(
    workspace_id: str, name: str, body: UpdateSecretRequest, user_id: CurrentUserId,
):
    workspace = await db_get_workspace(workspace_id)
    require_workspace_owner(workspace, user_id=user_id)

    found = await update_secret(
        workspace_id, name, value=body.value, description=body.description,
    )
    if not found:
        raise HTTPException(status_code=404, detail="Secret not found")

    await after_secret_change(
        WORKSPACE_TIER, workspace_id, name,
        user_id=user_id,
        value_changed=body.value is not None,
    )
    return {"name": name}


@router.get("/{workspace_id}/vault/secrets/{name}/reveal")
@handle_api_exceptions("reveal vault secret", logger)
async def reveal_secret_endpoint(
    workspace_id: str, name: str, user_id: CurrentUserId,
):
    workspace = await db_get_workspace(workspace_id)
    require_workspace_owner(workspace, user_id=user_id)

    value = await reveal_secret_db(workspace_id, name)
    if value is None:
        raise HTTPException(status_code=404, detail="Secret not found")
    return {"value": value}


@router.delete("/{workspace_id}/vault/secrets/{name}")
@handle_api_exceptions("delete vault secret", logger)
async def delete_secret_endpoint(
    workspace_id: str, name: str, user_id: CurrentUserId,
):
    workspace = await db_get_workspace(workspace_id)
    require_workspace_owner(workspace, user_id=user_id)

    found = await delete_secret(workspace_id, name)
    if not found:
        raise HTTPException(status_code=404, detail="Secret not found")

    await after_secret_change(WORKSPACE_TIER, workspace_id, name, user_id=user_id)
    return {"ok": True}


@router.get("/{workspace_id}/vault/blueprints")
@handle_api_exceptions("list vault blueprints", logger)
async def list_blueprints(workspace_id: str, user_id: CurrentUserId):
    """Return the 'recommended but not yet set' credential blueprints.

    Blueprints are declared inline on each MCP server entry in agent_config.yaml
    (`vault_blueprints:` block). This endpoint walks all enabled servers, dedupes
    by name, and subtracts credentials the workspace already has.

    Note: agent_config is loaded once at server startup. Changes to
    agent_config.yaml require a server restart to take effect here.
    """
    workspace = await db_get_workspace(workspace_id)
    require_workspace_owner(workspace, user_id=user_id)

    existing_names = await get_workspace_secret_names(workspace_id)
    remaining_slots = max(0, MAX_SECRETS_PER_WORKSPACE - len(existing_names))
    collected = _collect_config_blueprints()

    blueprints = [bp for name, bp in collected.items() if name not in existing_names]
    return {"blueprints": blueprints, "remaining_slots": remaining_slots}
