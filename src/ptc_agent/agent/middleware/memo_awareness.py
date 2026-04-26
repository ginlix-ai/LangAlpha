"""Middleware that injects a tiny memo-index awareness block into the system prompt.

The memo feature lives under ``.agents/user/memo/`` and is user-managed: the
agent has read-only access. Unlike memory (agent-written), injecting every
memo title into every turn is noisy and leaks potentially private contents.

Instead we inject a single ~80-byte block advertising how many memos exist and
where they live. The agent follows up on demand via ``read_file`` / ``glob``.
The block is appended AFTER the prompt-cache breakpoint, mirroring
``MemoryContextMiddleware``.
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable

import structlog
from langchain.agents.middleware.types import AgentMiddleware, ModelRequest, ModelResponse
from langchain_core.messages import SystemMessage
from langgraph.store.base import BaseStore

from ptc_agent.agent.backends.store_cache import RequestScopedStoreCache

logger = structlog.get_logger(__name__)

# A memo-count lookup on every model call must not stall the turn.
_DEFAULT_TIMEOUT_S = 2.0

# Cap the display value so the injected string stays constant-size regardless
# of how many memos a user has — also keeps the prompt stable for caching.
_COUNT_DISPLAY_CAP = 500
_COUNT_QUERY_LIMIT = _COUNT_DISPLAY_CAP + 1  # 501 — "500+" when we see this many

NamespaceFactory = Callable[[], tuple[str, ...]]


def _append_content_block(system_message: SystemMessage | None, text: str) -> SystemMessage:
    new_content: list[dict[str, str]] = (
        list(system_message.content_blocks) if system_message else []
    )
    prefix = "\n\n" if new_content else ""
    new_content.append({"type": "text", "text": f"{prefix}{text}"})
    return SystemMessage(content_blocks=new_content)


class MemoAwarenessMiddleware(AgentMiddleware):
    """Inject a ``<memo-index count="N" path="..."/>`` block when memos exist.

    The block is appended as a new content part on the system message, same
    placement pattern used by :class:`MemoryContextMiddleware` — after the
    prompt-cache breakpoint so its value can change per turn without
    invalidating the cached prefix.

    Notes:
        The result is memoized per-namespace on the instance. One
        ``MemoAwarenessMiddleware`` lives for one ``PTCAgent.create_agent``
        call (≈ one request), so the memo collapses K model calls in a turn
        into one count compute — protecting the slow path (memo-less users
        whose catalog row doesn't exist) from K postgres ``asearch`` round-
        trips. Memo writes by the user are rare mid-turn, and the agent
        itself has read-only access, so a single stable count per request
        is correct.
    """

    def __init__(
        self,
        *,
        store: BaseStore,
        user_namespace_factory: NamespaceFactory,
        timeout_s: float = _DEFAULT_TIMEOUT_S,
        display_path: str = ".agents/user/memo/",
        index_key: str = "memo.md",
        cache: RequestScopedStoreCache | None = None,
    ) -> None:
        self._store = store
        self._user_namespace_factory = user_namespace_factory
        self._timeout_s = timeout_s
        self._display_path = display_path
        self._index_key = index_key
        # Optional shared cache. The catalog row is the only reason this
        # middleware hits the store on the fast path; caching it across the
        # K model calls in a turn collapses K reads into 1.
        self._cache = cache
        # Per-request count memo. Bounded by the namespaces this instance
        # ever sees within its lifetime (always 1 in production — the
        # factory captures one user_id at agent-creation time).
        self._count_cache: dict[tuple[str, ...], int] = {}

    async def _count_memos(self, namespace: tuple[str, ...]) -> int:
        """Count memos in the namespace, capping at the query limit.

        Fast path: read the count directly off memo.md's value, which the
        rebuild path stores so we don't need a full-row asearch on the model
        critical path. memo.md is rebuilt on every upload/write/delete/regen,
        so its row already reflects the current namespace size.

        Slow path (catalog missing or malformed): one bounded asearch. This
        runs at most once per namespace lifetime — first turn before any
        upload, or if the catalog row was wiped.

        Both paths are memoized on ``self._count_cache`` so the K model
        calls in a turn pay one compute, not K. Stable for the request
        because memos are user-managed (agent is read-only) and out-of-band
        writes mid-turn are rare; the worst case is a one-turn-stale count.
        """
        cached = self._count_cache.get(namespace)
        if cached is not None:
            return cached
        catalog = await (
            self._cache.aget(self._store, namespace, self._index_key)
            if self._cache is not None
            else self._store.aget(namespace, self._index_key)
        )
        if catalog is not None and isinstance(catalog.value, dict):
            count = catalog.value.get("memo_count")
            if isinstance(count, int) and count >= 0:
                self._count_cache[namespace] = count
                return count
        # Fallback: enumerate. Bounded by _COUNT_QUERY_LIMIT to keep the cost
        # capped even when the namespace is large. The inner ``wait_for``
        # mirrors ``_store_helpers.asearch`` and is defense-in-depth: the
        # outer ``awrap_model_call`` already wraps this whole coroutine, but
        # bounding the asearch directly keeps the slow path safe if a future
        # refactor reroutes around the outer guard.
        results = await asyncio.wait_for(
            self._store.asearch(namespace, limit=_COUNT_QUERY_LIMIT, offset=0),
            timeout=self._timeout_s,
        )
        count = (
            0 if not results
            else sum(1 for item in results if item.key != self._index_key)
        )
        self._count_cache[namespace] = count
        return count

    @staticmethod
    def _format_count(count: int) -> str | None:
        """Return display string for the given count, or None to skip injection."""
        if count <= 0:
            return None
        if count >= _COUNT_QUERY_LIMIT:
            return f"{_COUNT_DISPLAY_CAP}+"
        return str(count)

    def wrap_model_call(
        self,
        request: ModelRequest,
        handler: Callable[[ModelRequest], ModelResponse],
    ) -> ModelResponse:
        return handler(request)

    async def awrap_model_call(
        self,
        request: ModelRequest,
        handler: Callable[[ModelRequest], Awaitable[ModelResponse]],
    ) -> ModelResponse:
        try:
            namespace = self._user_namespace_factory()
        except Exception:
            logger.exception("memo namespace resolution failed")
            return await handler(request)

        try:
            count = await asyncio.wait_for(
                self._count_memos(namespace), timeout=self._timeout_s
            )
        except asyncio.TimeoutError:
            logger.warning(
                "memo count read timed out; skipping injection",
                namespace=namespace,
                timeout_s=self._timeout_s,
            )
            return await handler(request)
        except Exception:
            logger.exception(
                "memo count store.asearch failed; skipping injection",
                namespace=namespace,
            )
            return await handler(request)

        count_str = self._format_count(count)
        if count_str is None:
            return await handler(request)

        block = f'<memo-index count="{count_str}" path="{self._display_path}"/>'
        new_system_message = _append_content_block(request.system_message, block)
        modified_request = request.override(system_message=new_system_message)
        return await handler(modified_request)
