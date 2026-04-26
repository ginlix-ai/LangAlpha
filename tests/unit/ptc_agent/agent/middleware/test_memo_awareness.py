"""Unit tests for ``MemoAwarenessMiddleware``.

Confirms the middleware appends a tiny ``<memo-index count="N" path="..."/>``
block to the system message when memos exist, skips injection cleanly when
there are none or when the store call fails, and caps the displayed count at
``500+`` once we hit the query cap.
"""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime

import pytest
from langchain_core.messages import SystemMessage
from langgraph.store.memory import InMemoryStore

from ptc_agent.agent.middleware.memo_awareness import MemoAwarenessMiddleware


class _FakeStore:
    """Minimal async-store stand-in exposing ``aget`` + ``asearch`` for middleware.

    Used by tests that need to control the result / raise / stall in ways
    ``InMemoryStore`` doesn't support (its methods are read-only slots).
    The middleware reads ``memo.md.memo_count`` first via ``aget`` and only
    falls back to ``asearch`` when the catalog row is missing or malformed —
    so tests can drive either path.
    """

    def __init__(self, *, asearch_impl, aget_impl=None) -> None:
        self._asearch_impl = asearch_impl
        self._aget_impl = aget_impl
        self.calls: list[dict] = []
        self.aget_calls: list[dict] = []

    async def asearch(self, namespace_prefix, *, query=None, filter=None,
                      limit=10, offset=0, refresh_ttl=None):
        self.calls.append({
            "namespace_prefix": namespace_prefix,
            "limit": limit,
            "offset": offset,
        })
        return await self._asearch_impl(
            namespace_prefix,
            query=query,
            filter=filter,
            limit=limit,
            offset=offset,
            refresh_ttl=refresh_ttl,
        )

    async def aget(self, namespace, key):
        self.aget_calls.append({"namespace": namespace, "key": key})
        if self._aget_impl is None:
            # Default behavior: catalog row missing — middleware falls back
            # to the asearch path, which is what most legacy tests exercise.
            return None
        return await self._aget_impl(namespace, key)


def _now() -> str:
    return datetime.now(UTC).isoformat()


def _memo_value(content: str, *, original_filename: str = "doc.md") -> dict:
    now = _now()
    return {
        "content": content,
        "encoding": "utf-8",
        "original_filename": original_filename,
        "key": original_filename,
        "created_at": now,
        "modified_at": now,
    }


class _FakeRequest:
    """Minimal stand-in for ``ModelRequest`` exposing what the middleware uses."""

    def __init__(self, system_message: SystemMessage | None = None) -> None:
        self.system_message = system_message
        self.overrode_with: SystemMessage | None = None

    def override(self, *, system_message: SystemMessage) -> "_FakeRequest":
        clone = _FakeRequest(system_message=system_message)
        clone.overrode_with = system_message
        self.overrode_with = system_message
        return clone


async def _capture_handler(request: _FakeRequest) -> _FakeRequest:
    """Echo handler — just returns whatever was passed in."""
    return request


@pytest.fixture
def store():
    return InMemoryStore()


@pytest.fixture
def middleware(store):
    return MemoAwarenessMiddleware(
        store=store,
        user_namespace_factory=lambda: ("user_abc", "memos"),
    )


def _last_text_block(message: SystemMessage) -> str:
    return message.content_blocks[-1]["text"]


class TestInjection:
    @pytest.mark.asyncio
    async def test_single_memo_injects_count_block(self, store, middleware):
        await store.aput(
            ("user_abc", "memos"), "q1-thesis.md", _memo_value("hello")
        )
        request = _FakeRequest(system_message=SystemMessage(content="base"))
        result = await middleware.awrap_model_call(request, _capture_handler)
        appended = _last_text_block(result.system_message)
        assert '<memo-index count="1" path=".agents/user/memo/"/>' in appended

    @pytest.mark.asyncio
    async def test_multiple_memos_uses_accurate_count(self, store, middleware):
        await store.aput(("user_abc", "memos"), "a.md", _memo_value("a"))
        await store.aput(("user_abc", "memos"), "b.md", _memo_value("b"))
        await store.aput(("user_abc", "memos"), "c.md", _memo_value("c"))
        request = _FakeRequest(system_message=SystemMessage(content="base"))
        result = await middleware.awrap_model_call(request, _capture_handler)
        appended = _last_text_block(result.system_message)
        assert 'count="3"' in appended

    @pytest.mark.asyncio
    async def test_display_path_respected(self, store):
        mw = MemoAwarenessMiddleware(
            store=store,
            user_namespace_factory=lambda: ("user_abc", "memos"),
            display_path=".agents/user/memo/",
        )
        await store.aput(("user_abc", "memos"), "x.md", _memo_value("x"))
        request = _FakeRequest(system_message=SystemMessage(content="base"))
        result = await mw.awrap_model_call(request, _capture_handler)
        assert 'path=".agents/user/memo/"' in _last_text_block(result.system_message)

    @pytest.mark.asyncio
    async def test_zero_memos_skips_injection(self, store, middleware):
        request = _FakeRequest(system_message=SystemMessage(content="base"))
        result = await middleware.awrap_model_call(request, _capture_handler)
        # Handler received the original request unmodified — override never called.
        assert request.overrode_with is None
        # And the returned system message is the untouched original.
        assert result.system_message is not None
        # No memo-index block should appear anywhere.
        for block in result.system_message.content_blocks:
            assert "memo-index" not in block.get("text", "")

    @pytest.mark.asyncio
    async def test_memo_md_excluded_from_count(self, store, middleware):
        """memo.md sits in the namespace once rebuild_memo_index runs, but it
        is the catalog itself — counting it would add a phantom +1 to every
        injection."""
        await store.aput(
            ("user_abc", "memos"),
            "memo.md",
            {"content": "# Memos\n\n_0 memos_", "encoding": "utf-8"},
        )
        await store.aput(("user_abc", "memos"), "q1.md", _memo_value("hello"))
        request = _FakeRequest(system_message=SystemMessage(content="base"))
        result = await middleware.awrap_model_call(request, _capture_handler)
        appended = _last_text_block(result.system_message)
        assert 'count="1"' in appended

    @pytest.mark.asyncio
    async def test_only_memo_md_means_zero(self, store, middleware):
        """If the catalog is the only key in the namespace (e.g. all memos
        deleted but rebuild ran), the count must be zero and no block
        injects."""
        await store.aput(
            ("user_abc", "memos"),
            "memo.md",
            {"content": "# Memos", "encoding": "utf-8"},
        )
        request = _FakeRequest(system_message=SystemMessage(content="base"))
        await middleware.awrap_model_call(request, _capture_handler)
        assert request.overrode_with is None


class _FakeItem:
    """Minimal stand-in for a langgraph store Item — exposes only ``.key``."""

    def __init__(self, key: str) -> None:
        self.key = key


class TestCountCap:
    @pytest.mark.asyncio
    async def test_over_limit_displays_500_plus(self):
        fake_results = [_FakeItem(f"memo-{i}.md") for i in range(501)]

        async def _asearch(namespace_prefix, **_):
            return fake_results

        fake_store = _FakeStore(asearch_impl=_asearch)
        mw = MemoAwarenessMiddleware(
            store=fake_store,
            user_namespace_factory=lambda: ("user_abc", "memos"),
        )

        request = _FakeRequest(system_message=SystemMessage(content="base"))
        result = await mw.awrap_model_call(request, _capture_handler)
        appended = _last_text_block(result.system_message)
        assert 'count="500+"' in appended
        # Sanity: we asked for exactly 501.
        assert len(fake_store.calls) == 1
        assert fake_store.calls[0]["limit"] == 501
        assert fake_store.calls[0]["offset"] == 0


class TestFailureModes:
    @pytest.mark.asyncio
    async def test_asearch_timeout_skips_injection(self):
        async def _slow_asearch(namespace_prefix, **_):
            await asyncio.sleep(1.0)
            return []

        fake_store = _FakeStore(asearch_impl=_slow_asearch)
        mw = MemoAwarenessMiddleware(
            store=fake_store,
            user_namespace_factory=lambda: ("user_abc", "memos"),
            timeout_s=0.01,
        )

        request = _FakeRequest(system_message=SystemMessage(content="base"))
        result = await mw.awrap_model_call(request, _capture_handler)
        # Handler got the original unmodified request.
        assert request.overrode_with is None
        assert result is request

    @pytest.mark.asyncio
    async def test_asearch_error_skips_injection(self):
        async def _boom_asearch(namespace_prefix, **_):
            raise RuntimeError("store unavailable")

        fake_store = _FakeStore(asearch_impl=_boom_asearch)
        mw = MemoAwarenessMiddleware(
            store=fake_store,
            user_namespace_factory=lambda: ("user_abc", "memos"),
        )

        request = _FakeRequest(system_message=SystemMessage(content="base"))
        result = await mw.awrap_model_call(request, _capture_handler)
        # No injection — handler proceeds with the original request.
        assert request.overrode_with is None
        assert result is request

    @pytest.mark.asyncio
    async def test_namespace_factory_error_skips_injection(self, store):
        def bad_factory() -> tuple[str, ...]:
            raise RuntimeError("no identity")

        mw = MemoAwarenessMiddleware(
            store=store,
            user_namespace_factory=bad_factory,
        )
        request = _FakeRequest(system_message=SystemMessage(content="base"))
        result = await mw.awrap_model_call(request, _capture_handler)
        assert request.overrode_with is None
        assert result is request


class TestCacheDeduplication:
    """When wired with a RequestScopedStoreCache the middleware reads memo.md
    exactly once across multiple model calls in the same turn."""

    @pytest.mark.asyncio
    async def test_repeated_calls_with_cache_hit_store_once(self):
        from ptc_agent.agent.backends.store_cache import RequestScopedStoreCache

        class _Item:
            def __init__(self, value: dict) -> None:
                self.value = value

        async def _aget(namespace, key):
            # memo.md fast path: returns a value carrying memo_count so the
            # middleware short-circuits the asearch fallback.
            if key == "memo.md":
                return _Item({"memo_count": 4})
            return None

        async def _asearch(namespace_prefix, **_):
            # Should not be reached on the fast path.
            raise AssertionError("asearch must not run when memo.md has memo_count")

        fake_store = _FakeStore(asearch_impl=_asearch, aget_impl=_aget)

        cache = RequestScopedStoreCache()
        mw = MemoAwarenessMiddleware(
            store=fake_store,
            user_namespace_factory=lambda: ("user_abc", "memos"),
            cache=cache,
        )

        for _ in range(3):
            request = _FakeRequest(system_message=SystemMessage(content="base"))
            result = await mw.awrap_model_call(request, _capture_handler)
            appended = _last_text_block(result.system_message)
            assert 'count="4"' in appended

        # Three model calls, but only one store.aget round-trip — the cache
        # served the next two from memory.
        assert len(fake_store.aget_calls) == 1


class TestCountMemoization:
    """Regression: ``_count_memos`` memoizes its result per-instance so the K
    model calls in a turn collapse to one compute. Without this, memo-less
    users (catalog row missing) paid K postgres ``asearch`` round-trips per
    turn — flagged in the PR #176 follow-up review."""

    @pytest.mark.asyncio
    async def test_slow_path_asearch_runs_once_for_memo_less_namespace(self):
        # No catalog row → middleware falls through to asearch every time
        # without memoization.
        async def _empty_asearch(namespace_prefix, **_):
            return []

        fake_store = _FakeStore(asearch_impl=_empty_asearch)
        mw = MemoAwarenessMiddleware(
            store=fake_store,
            user_namespace_factory=lambda: ("user_abc", "memos"),
        )

        # Drive the awareness path by directly invoking _count_memos so we
        # measure store calls without dragging the whole request flow in.
        for _ in range(5):
            assert await mw._count_memos(("user_abc", "memos")) == 0

        # 5 invocations, one asearch — the rest came from the per-namespace
        # memo on the middleware.
        assert len(fake_store.calls) == 1

    @pytest.mark.asyncio
    async def test_fast_path_memoizes_count_without_recache(self):
        # Catalog row exists but the middleware was constructed without a
        # ``RequestScopedStoreCache``; the count cache should still collapse
        # repeat aget calls to one.
        class _Item:
            def __init__(self, value: dict) -> None:
                self.value = value

        async def _aget(namespace, key):
            if key == "memo.md":
                return _Item({"memo_count": 7})
            return None

        async def _asearch(namespace_prefix, **_):
            raise AssertionError("asearch must not run when catalog has memo_count")

        fake_store = _FakeStore(asearch_impl=_asearch, aget_impl=_aget)
        mw = MemoAwarenessMiddleware(
            store=fake_store,
            user_namespace_factory=lambda: ("user_abc", "memos"),
        )

        for _ in range(3):
            assert await mw._count_memos(("user_abc", "memos")) == 7

        # Three invocations, one aget — the count cache served the rest.
        assert len(fake_store.aget_calls) == 1

    @pytest.mark.asyncio
    async def test_slow_path_asearch_has_inner_timeout_guard(self):
        # Defense-in-depth: bypass awrap_model_call's outer wait_for and call
        # _count_memos directly, asserting the asearch is bounded internally.
        # If a future refactor drops the outer guard, this still keeps the
        # slow path from hanging the agent.
        async def _slow_asearch(namespace_prefix, **_):
            await asyncio.sleep(1.0)
            return []

        fake_store = _FakeStore(asearch_impl=_slow_asearch)
        mw = MemoAwarenessMiddleware(
            store=fake_store,
            user_namespace_factory=lambda: ("user_abc", "memos"),
            timeout_s=0.01,
        )

        with pytest.raises(asyncio.TimeoutError):
            await mw._count_memos(("user_abc", "memos"))

    @pytest.mark.asyncio
    async def test_different_namespaces_have_independent_cache_entries(self):
        # The factory returns one namespace per agent in production, but the
        # cache key is the namespace tuple — verify it doesn't cross-pollute
        # if a future caller routes multiple namespaces through one instance.
        seen: list[tuple[str, ...]] = []

        async def _asearch(namespace_prefix, **_):
            seen.append(namespace_prefix)
            return []

        fake_store = _FakeStore(asearch_impl=_asearch)
        mw = MemoAwarenessMiddleware(
            store=fake_store,
            user_namespace_factory=lambda: ("user_abc", "memos"),
        )

        await mw._count_memos(("user_abc", "memos"))
        await mw._count_memos(("user_xyz", "memos"))
        await mw._count_memos(("user_abc", "memos"))  # cached
        await mw._count_memos(("user_xyz", "memos"))  # cached

        assert seen == [("user_abc", "memos"), ("user_xyz", "memos")]
