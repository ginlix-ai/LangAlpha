"""Read-only API for the agent's long-term memory (LangGraph store)."""

from __future__ import annotations

import logging
from typing import Any

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel

from src.server.app import setup
from src.server.app._store_helpers import (
    aget,
    coerce_str,
    paginate_namespace,
    require_store,
    validate_key,
)
from src.server.database.workspace import get_workspace as db_get_workspace
from src.server.utils.api import CurrentUserId, require_workspace_owner

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1/memory", tags=["Memory"])


class MemoryEntry(BaseModel):
    key: str
    size: int
    created_at: str | None = None
    modified_at: str | None = None


class MemoryListResponse(BaseModel):
    tier: str  # "user" | "workspace"
    entries: list[MemoryEntry]
    truncated: bool = False


class MemoryReadResponse(BaseModel):
    tier: str
    key: str
    content: str
    encoding: str
    created_at: str | None = None
    modified_at: str | None = None


def _value_to_entry(key: str, value: Any) -> MemoryEntry:
    if not isinstance(value, dict):
        return MemoryEntry(key=key, size=0)
    content = value.get("content")
    size = len(content) if isinstance(content, str) else 0
    return MemoryEntry(
        key=key,
        size=size,
        created_at=coerce_str(value.get("created_at")) or None,
        modified_at=coerce_str(value.get("modified_at")) or None,
    )


def _value_to_read(tier: str, key: str, value: Any) -> MemoryReadResponse:
    if not isinstance(value, dict):
        # Store corruption — return empty instead of 500.
        logger.warning("memory entry has non-dict value", extra={"key": key})
        return MemoryReadResponse(tier=tier, key=key, content="", encoding="utf-8")
    return MemoryReadResponse(
        tier=tier,
        key=key,
        content=coerce_str(value.get("content")),
        encoding=coerce_str(value.get("encoding"), "utf-8") or "utf-8",
        created_at=coerce_str(value.get("created_at")) or None,
        modified_at=coerce_str(value.get("modified_at")) or None,
    )


# --- User tier ---------------------------------------------------------------


@router.get("/user", response_model=MemoryListResponse)
async def list_user_memory(user_id: CurrentUserId) -> MemoryListResponse:
    """List all user-tier memory entries for the caller."""
    store = require_store(setup.store)
    namespace = (user_id, "memory")
    entries, truncated = await paginate_namespace(store, namespace, _value_to_entry)
    return MemoryListResponse(tier="user", entries=entries, truncated=truncated)


@router.get("/user/read", response_model=MemoryReadResponse)
async def read_user_memory(
    user_id: CurrentUserId,
    key: str = Query(..., description="Key relative to the user memory root"),
) -> MemoryReadResponse:
    """Read one user-tier memory file by its key."""
    validate_key(key)
    store = require_store(setup.store)
    item = await aget(store, (user_id, "memory"), key)
    if item is None:
        raise HTTPException(status_code=404, detail="Memory entry not found")
    return _value_to_read("user", key, item.value)


# --- Workspace tier ----------------------------------------------------------


@router.get("/workspaces/{workspace_id}", response_model=MemoryListResponse)
async def list_workspace_memory(
    workspace_id: str,
    user_id: CurrentUserId,
) -> MemoryListResponse:
    """List all workspace-tier memory entries for the caller's workspace."""
    workspace = await db_get_workspace(workspace_id)
    require_workspace_owner(workspace, user_id=user_id)
    store = require_store(setup.store)
    namespace = (user_id, "workspaces", workspace_id, "memory")
    entries, truncated = await paginate_namespace(store, namespace, _value_to_entry)
    return MemoryListResponse(
        tier="workspace", entries=entries, truncated=truncated
    )


@router.get("/workspaces/{workspace_id}/read", response_model=MemoryReadResponse)
async def read_workspace_memory(
    workspace_id: str,
    user_id: CurrentUserId,
    key: str = Query(..., description="Key relative to the workspace memory root"),
) -> MemoryReadResponse:
    """Read one workspace-tier memory file by its key."""
    validate_key(key)
    workspace = await db_get_workspace(workspace_id)
    require_workspace_owner(workspace, user_id=user_id)
    store = require_store(setup.store)
    namespace = (user_id, "workspaces", workspace_id, "memory")
    item = await aget(store, namespace, key)
    if item is None:
        raise HTTPException(status_code=404, detail="Memory entry not found")
    return _value_to_read("workspace", key, item.value)
