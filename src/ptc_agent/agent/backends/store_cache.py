"""Per-request store cache shared between memory/memo middleware + backends.

Background
----------
On every model call the agent runs ``MemoryContextMiddleware`` (two ``aget``
calls — user + workspace ``memory.md``) and ``MemoAwarenessMiddleware`` (one
``aget`` for the memo catalog row). A 3-tool-call turn pays 9 store reads
before the model body runs, on data that is identical round-to-round in 99%
of turns. This cache deduplicates those reads to one set per turn.

Lifecycle
---------
A fresh cache is constructed per ``PTCAgent.create_agent`` call. Because one
agent is built per request (see ``agent.py`` invariant), the cache is
request-scoped: it never bridges users or turns. Writes through
``StoreBackend.awrite_text`` / ``aedit_text`` invalidate the affected
key so the next middleware read sees the fresh value within the same turn.

Bounds
------
Per turn the cache holds at most ~5 entries (two memory tiers + memo catalog
+ a couple of agent-side memory writes). No eviction policy is needed at
this size; the cache is dropped with the agent instance.
"""

from __future__ import annotations

import structlog
from typing import Any

from langgraph.store.base import BaseStore

logger = structlog.get_logger(__name__)


class RequestScopedStoreCache:
    """Tiny ``aget`` cache shared by middleware + backend within one agent.

    Only ``aget`` lookups are cached. ``asearch`` and write paths bypass the
    cache deliberately: the search-results payload is heavy and rarely
    repeats, and writes go straight to the store followed by an
    ``invalidate`` so future reads observe the new value.

    Not safe to share across event loops or asyncio tasks that outlive the
    agent run; the agent runs single-threaded on one loop.
    """

    __slots__ = ("_cache",)

    def __init__(self) -> None:
        self._cache: dict[tuple[tuple[str, ...], str], Any] = {}

    async def aget(
        self,
        store: BaseStore,
        namespace: tuple[str, ...],
        key: str,
    ) -> Any:
        """Return the cached ``Item`` (or ``None``) for the key, fetching once."""
        ck = (namespace, key)
        if ck in self._cache:
            return self._cache[ck]
        item = await store.aget(namespace, key)
        self._cache[ck] = item
        return item

    def invalidate(
        self,
        namespace: tuple[str, ...],
        key: str | None = None,
    ) -> None:
        """Drop one entry, or every entry under ``namespace`` when ``key`` is None."""
        if key is None:
            stale = [ck for ck in self._cache if ck[0] == namespace]
            for ck in stale:
                self._cache.pop(ck, None)
        else:
            self._cache.pop((namespace, key), None)

    def __len__(self) -> int:
        return len(self._cache)
