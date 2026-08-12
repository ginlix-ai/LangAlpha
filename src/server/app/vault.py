"""Workspace Vault Secrets API Router.

CRUD for per-workspace encrypted secrets. On every mutation, secrets are
pushed to the running sandbox (if any) so code can use them immediately.

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
import re

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field, field_validator

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
from src.server.services.workspace_manager import WorkspaceManager
from src.server.utils.api import CurrentUserId, handle_api_exceptions, require_workspace_owner

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1/workspaces", tags=["Vault Secrets"])

_NAME_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]{0,63}$")


# ---------------------------------------------------------------------------
# Request / response models
# ---------------------------------------------------------------------------

class CreateSecretRequest(BaseModel):
    name: str = Field(..., min_length=1, max_length=64)
    value: str = Field(..., min_length=1, max_length=4096)
    description: str = Field("", max_length=256)

    @field_validator("name")
    @classmethod
    def validate_name(cls, v: str) -> str:
        if not _NAME_RE.match(v):
            raise ValueError(
                "Name must be 1-64 characters: letters, digits, underscores; "
                "must start with a letter or underscore"
            )
        return v


class UpdateSecretRequest(BaseModel):
    value: str | None = Field(None, min_length=1, max_length=4096)
    description: str | None = Field(None, max_length=256)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

async def _push_to_sandbox(workspace_id: str) -> None:
    """Push vault secrets to the running sandbox (best-effort)."""
    try:
        wm = WorkspaceManager.get_instance()
        await wm.push_vault_secrets(workspace_id)
    except Exception:
        logger.warning(
            f"[vault] Failed to push secrets to sandbox for workspace {workspace_id}",
            exc_info=True,
        )


async def _invalidate_mcp_for_secret(
    workspace_id: str, user_id: str, secret_name: str
) -> None:
    """Best-effort MCP cache invalidation when a secret's VALUE changes.

    The MCP discovery fingerprint hashes ``${vault:NAME}`` ref strings, never
    values, so a vault mutation alone can't churn any config hash. When the
    changed secret is referenced by a workspace MCP server we bump
    ``mcp_config_version`` (live sessions re-resolve on next acquire), purge
    discovery snapshots of referencing servers whose discovery runs WITH
    secrets (their cached ``tools/list`` may depend on the credential), and
    schedule a proactive apply so a ``needs_secret``/``pending`` server comes
    alive without waiting for the user's next message.
    """
    try:
        import json

        from ptc_agent.core.mcp_sanitize import (
            discovery_should_use_secrets,
            vault_refs,
        )

        from src.server.database import mcp_servers as mcp_db
        from src.server.services.mcp_config import workspace_row_to_server_config

        referencing: list[dict] = []
        for row in await mcp_db.list_workspace_servers(workspace_id):
            if row.get("source") != "workspace" or not row.get("config"):
                continue
            if secret_name in vault_refs(json.dumps(row["config"])):
                referencing.append(row)
        if not referencing:
            return

        purge: list[str] = []
        for row in referencing:
            try:
                server = workspace_row_to_server_config(row)
            except Exception:
                continue
            if discovery_should_use_secrets(server):
                purge.append(row["name"])

        # Purge + bump in ONE transaction: a partial purge with an un-bumped
        # version would let live sessions skip re-resolution against the
        # half-purged cache.
        if purge:
            await mcp_db.delete_tool_schemas_and_bump(workspace_id, purge)
        else:
            await mcp_db.bump_workspace_mcp_version(workspace_id)

        from src.server.app.mcp_servers import _schedule_proactive_apply

        _schedule_proactive_apply(workspace_id, user_id)
        logger.info(
            f"[vault] secret {secret_name!r} change invalidated MCP config for "
            f"workspace {workspace_id} ({len(referencing)} referencing server(s))"
        )
    except Exception:
        logger.warning(
            f"[vault] MCP invalidation failed for workspace {workspace_id}",
            exc_info=True,
        )


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

    await _push_to_sandbox(workspace_id)
    await _invalidate_mcp_for_secret(workspace_id, user_id, body.name)
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

    await _push_to_sandbox(workspace_id)
    if body.value is not None:  # description-only edits can't affect discovery
        await _invalidate_mcp_for_secret(workspace_id, user_id, name)
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

    await _push_to_sandbox(workspace_id)
    await _invalidate_mcp_for_secret(workspace_id, user_id, name)
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

    # Lazy import to avoid circular dependency between `setup` module and router
    # registration. `setup.agent_config` is populated in `lifespan()` at startup.
    from src.server.app import setup

    existing_names = await get_workspace_secret_names(workspace_id)
    remaining_slots = max(0, MAX_SECRETS_PER_WORKSPACE - len(existing_names))

    if setup.agent_config is None:
        # Startup race: request landed before lifespan completed.
        return {"blueprints": [], "remaining_slots": remaining_slots}

    # First-declaration wins on metadata; duplicate blueprint names across
    # servers are treated as aliases: the second server's description/docs_url/
    # regex are discarded, but its name is appended to `sources` so the UI can
    # show which integrations share the credential.
    collected: dict[str, dict] = {}
    for server in setup.agent_config.mcp.servers:
        if not server.enabled:
            continue
        for bp in server.vault_blueprints:
            existing = collected.get(bp.name)
            if existing is None:
                collected[bp.name] = {
                    "name": bp.name,
                    "label": bp.label,
                    "description": bp.description,
                    "docs_url": bp.docs_url,
                    "regex": bp.regex,
                    "sources": [server.name],
                }
            else:
                existing["sources"].append(server.name)

    blueprints = [bp for name, bp in collected.items() if name not in existing_names]
    return {"blueprints": blueprints, "remaining_slots": remaining_slots}
