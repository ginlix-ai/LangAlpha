"""Read-only API for the agent's long-term memory (LangGraph store)."""

from __future__ import annotations

import asyncio
import logging
from typing import Any

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel

from ptc_agent.agent.backends import (
    InvalidMemoryKeyError,
    validate_memory_key,
)
from src.server.app import setup
from src.server.database.workspace import get_workspace as db_get_workspace
from src.server.utils.api import CurrentUserId, require_workspace_owner

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1/memory", tags=["Memory"])

_MAX_LIST_LIMIT = 500
_STORE_OP_TIMEOUT_S = 2.0


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


def _require_store() -> Any:
    if setup.store is None:
        raise HTTPException(
            status_code=503,
            detail="Memory store is not configured on this server",
        )
    return setup.store


def _coerce_str(value: Any, default: str = "") -> str:
    return value if isinstance(value, str) else default


def _value_to_entry(key: str, value: Any) -> MemoryEntry:
    if not isinstance(value, dict):
        return MemoryEntry(key=key, size=0)
    content = value.get("content")
    size = len(content) if isinstance(content, str) else 0
    return MemoryEntry(
        key=key,
        size=size,
        created_at=_coerce_str(value.get("created_at")) or None,
        modified_at=_coerce_str(value.get("modified_at")) or None,
    )


def _value_to_read(tier: str, key: str, value: Any) -> MemoryReadResponse:
    if not isinstance(value, dict):
        # Store corruption — return empty instead of 500.
        logger.warning("memory entry has non-dict value", key=key)
        return MemoryReadResponse(tier=tier, key=key, content="", encoding="utf-8")
    return MemoryReadResponse(
        tier=tier,
        key=key,
        content=_coerce_str(value.get("content")),
        encoding=_coerce_str(value.get("encoding"), "utf-8") or "utf-8",
        created_at=_coerce_str(value.get("created_at")) or None,
        modified_at=_coerce_str(value.get("modified_at")) or None,
    )


def _validate_key(key: str) -> None:
    """Share the backend's rules so every accepted key can be round-tripped."""
    try:
        validate_memory_key(key)
    except InvalidMemoryKeyError as exc:
        raise HTTPException(status_code=400, detail=f"Invalid memory key: {exc}") from exc


async def _asearch(
    store: Any, namespace: tuple[str, ...], *, limit: int, offset: int
) -> list[Any]:
    try:
        return await asyncio.wait_for(
            store.asearch(namespace, limit=limit, offset=offset),
            timeout=_STORE_OP_TIMEOUT_S,
        )
    except asyncio.TimeoutError as exc:
        raise HTTPException(
            status_code=504,
            detail="Memory store timed out. Retry shortly.",
        ) from exc


async def _aget(store: Any, namespace: tuple[str, ...], key: str) -> Any:
    try:
        return await asyncio.wait_for(
            store.aget(namespace, key),
            timeout=_STORE_OP_TIMEOUT_S,
        )
    except asyncio.TimeoutError as exc:
        raise HTTPException(
            status_code=504,
            detail="Memory store timed out. Retry shortly.",
        ) from exc


async def _list_namespace(
    store: Any, namespace: tuple[str, ...]
) -> tuple[list[MemoryEntry], bool]:
    entries: list[MemoryEntry] = []
    offset = 0
    page = 100
    truncated = False
    while len(entries) < _MAX_LIST_LIMIT:
        results = await _asearch(store, namespace, limit=page, offset=offset)
        if not results:
            break
        for item in results:
            entries.append(_value_to_entry(item.key, item.value))
        if len(results) < page:
            break
        offset += page
    if len(entries) >= _MAX_LIST_LIMIT:
        # Peek past the cap in case the page boundary hid the extra items.
        extra = await _asearch(store, namespace, limit=1, offset=_MAX_LIST_LIMIT)
        truncated = bool(extra)
    return entries[:_MAX_LIST_LIMIT], truncated


# --- User tier ---------------------------------------------------------------


@router.get("/user", response_model=MemoryListResponse)
async def list_user_memory(user_id: CurrentUserId) -> MemoryListResponse:
    """List all user-tier memory entries for the caller."""
    store = _require_store()
    namespace = (user_id, "memory")
    entries, truncated = await _list_namespace(store, namespace)
    return MemoryListResponse(tier="user", entries=entries, truncated=truncated)


@router.get("/user/read", response_model=MemoryReadResponse)
async def read_user_memory(
    user_id: CurrentUserId,
    key: str = Query(..., description="Key relative to the user memory root"),
) -> MemoryReadResponse:
    """Read one user-tier memory file by its key."""
    _validate_key(key)
    store = _require_store()
    item = await _aget(store, (user_id, "memory"), key)
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
    store = _require_store()
    namespace = (user_id, "workspaces", workspace_id, "memory")
    entries, truncated = await _list_namespace(store, namespace)
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
    _validate_key(key)
    workspace = await db_get_workspace(workspace_id)
    require_workspace_owner(workspace, user_id=user_id)
    store = _require_store()
    namespace = (user_id, "workspaces", workspace_id, "memory")
    item = await _aget(store, namespace, key)
    if item is None:
        raise HTTPException(status_code=404, detail="Memory entry not found")
    return _value_to_read("workspace", key, item.value)
