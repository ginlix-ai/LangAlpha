"""Workspace Vault Secrets API Router.

CRUD for per-workspace encrypted secrets. On every mutation, secrets are
pushed to the running sandbox (if any) so code can use them immediately.

Endpoints:
- GET    /api/v1/workspaces/{workspace_id}/vault/secrets
- POST   /api/v1/workspaces/{workspace_id}/vault/secrets
- PUT    /api/v1/workspaces/{workspace_id}/vault/secrets/{name}
- GET    /api/v1/workspaces/{workspace_id}/vault/secrets/{name}/reveal
- DELETE /api/v1/workspaces/{workspace_id}/vault/secrets/{name}
"""

from __future__ import annotations

import logging
import re

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field, field_validator

from src.server.database.vault_secrets import (
    create_secret as create_secret_db,
    delete_secret,
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
    return {"ok": True}
