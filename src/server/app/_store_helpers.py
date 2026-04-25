"""Shared helpers for routers that read and write the agent long-term store.

memory.py (user/workspace memory) and memo.py (user-managed documents) share
the same BaseStore wire protocol, auth + timeout story, and list-pagination
shape. Anything that isn't tied to a specific response model lives here so
adding a third tier in the future doesn't fork the pattern.
"""

from __future__ import annotations

import asyncio
from typing import Any, Callable, TypeVar

from fastapi import HTTPException

from ptc_agent.agent.backends import (
    InvalidStoreKeyError,
    validate_store_key,
)

# Matches MemoryContextMiddleware's per-aget budget. Utility store reads stay
# responsive even when the pool is slow.
STORE_OP_TIMEOUT_S = 2.0

# Caps a single list response so a runaway namespace can't pull unbounded rows
# back through the API. Increase per-router if a caller needs more.
MAX_LIST_LIMIT = 500


def require_store(store: Any) -> Any:
    """503 when the store isn't wired — local MemorySaver-mode dev or failed startup."""
    if store is None:
        raise HTTPException(
            status_code=503,
            detail="Long-term store is not configured on this server",
        )
    return store


def coerce_str(value: Any, default: str = "") -> str:
    return value if isinstance(value, str) else default


def validate_key(key: str) -> None:
    """Raise HTTP 400 with the backend's rule text when a key fails the path-safe rules."""
    try:
        validate_store_key(key)
    except InvalidStoreKeyError as exc:
        raise HTTPException(status_code=400, detail=f"Invalid key: {exc}") from exc


async def asearch(
    store: Any, namespace: tuple[str, ...], *, limit: int, offset: int
) -> list[Any]:
    """``store.asearch`` with a bounded timeout. 504 if the store is slow.

    Note on prefix semantics: ``AsyncPostgresStore`` does *string* prefix
    matching on ``".".join(namespace)``, so an exactly-equal tuple is not
    enough — sibling tiers must avoid being string prefixes of each other.
    See ``langgraph.store.postgres.base._namespace_to_text``.
    """
    try:
        return await asyncio.wait_for(
            store.asearch(namespace, limit=limit, offset=offset),
            timeout=STORE_OP_TIMEOUT_S,
        )
    except asyncio.TimeoutError as exc:
        raise HTTPException(
            status_code=504,
            detail="Long-term store timed out. Retry shortly.",
        ) from exc


async def aget(store: Any, namespace: tuple[str, ...], key: str) -> Any:
    """store.aget with a bounded timeout. 504 if the store is slow."""
    try:
        return await asyncio.wait_for(
            store.aget(namespace, key),
            timeout=STORE_OP_TIMEOUT_S,
        )
    except asyncio.TimeoutError as exc:
        raise HTTPException(
            status_code=504,
            detail="Long-term store timed out. Retry shortly.",
        ) from exc


async def aput(
    store: Any, namespace: tuple[str, ...], key: str, value: Any
) -> None:
    """store.aput with a bounded timeout. 504 if the store is slow."""
    try:
        await asyncio.wait_for(
            store.aput(namespace, key, value),
            timeout=STORE_OP_TIMEOUT_S,
        )
    except asyncio.TimeoutError as exc:
        raise HTTPException(
            status_code=504,
            detail="Long-term store timed out. Retry shortly.",
        ) from exc


async def adelete(store: Any, namespace: tuple[str, ...], key: str) -> None:
    """store.adelete with a bounded timeout. 504 if the store is slow."""
    try:
        await asyncio.wait_for(
            store.adelete(namespace, key),
            timeout=STORE_OP_TIMEOUT_S,
        )
    except asyncio.TimeoutError as exc:
        raise HTTPException(
            status_code=504,
            detail="Long-term store timed out. Retry shortly.",
        ) from exc


E = TypeVar("E")


async def paginate_namespace(
    store: Any,
    namespace: tuple[str, ...],
    value_to_entry: Callable[[str, Any], E],
    *,
    max_list_limit: int = MAX_LIST_LIMIT,
    page: int = 100,
) -> tuple[list[E], bool]:
    """Page through a namespace, mapping each store item to an entry.

    Returns (entries, truncated) where truncated indicates the namespace had
    more than ``max_list_limit`` items. Callers supply ``value_to_entry`` so
    memory and memo can shape their own response rows independently.
    """
    entries: list[E] = []
    offset = 0
    while len(entries) < max_list_limit:
        results = await asearch(store, namespace, limit=page, offset=offset)
        if not results:
            break
        for item in results:
            entries.append(value_to_entry(item.key, item.value))
        if len(results) < page:
            break
        offset += page
    truncated = False
    if len(entries) >= max_list_limit:
        extra = await asearch(store, namespace, limit=1, offset=max_list_limit)
        truncated = bool(extra)
    return entries[:max_list_limit], truncated
