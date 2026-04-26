"""Unit tests for ``RequestScopedStoreCache``.

Confirms the cache deduplicates ``aget`` calls within a single agent run,
that ``invalidate`` drops the matching entry so a follow-up read goes back
to the store, and that wildcards (``key=None``) clear all entries under a
namespace without affecting siblings.
"""

from __future__ import annotations

import pytest

from ptc_agent.agent.backends.store_cache import RequestScopedStoreCache


class _CountingStore:
    """Minimal stand-in that records every aget so we can assert call counts."""

    def __init__(self, values: dict[tuple[tuple[str, ...], str], object]) -> None:
        self._values = values
        self.calls: list[tuple[tuple[str, ...], str]] = []

    async def aget(self, namespace: tuple[str, ...], key: str) -> object | None:
        self.calls.append((namespace, key))
        return self._values.get((namespace, key))


@pytest.mark.asyncio
async def test_first_aget_hits_store_second_returns_cached() -> None:
    ns = ("user1", "memory")
    store = _CountingStore({(ns, "memory.md"): "v1"})
    cache = RequestScopedStoreCache()

    a = await cache.aget(store, ns, "memory.md")
    b = await cache.aget(store, ns, "memory.md")

    assert a == "v1"
    assert b == "v1"
    assert len(store.calls) == 1


@pytest.mark.asyncio
async def test_invalidate_specific_key_only_clears_that_entry() -> None:
    ns = ("user1", "memory")
    store = _CountingStore({(ns, "memory.md"): "v1", (ns, "other.md"): "x"})
    cache = RequestScopedStoreCache()

    await cache.aget(store, ns, "memory.md")
    await cache.aget(store, ns, "other.md")
    assert len(store.calls) == 2

    cache.invalidate(ns, "memory.md")

    # memory.md goes back to the store; other.md stays cached.
    await cache.aget(store, ns, "memory.md")
    await cache.aget(store, ns, "other.md")
    assert len(store.calls) == 3


@pytest.mark.asyncio
async def test_invalidate_namespace_drops_every_key_in_namespace() -> None:
    ns = ("user1", "memory")
    other_ns = ("user2", "memory")
    store = _CountingStore({
        (ns, "memory.md"): "v1",
        (ns, "doc.md"): "v2",
        (other_ns, "memory.md"): "v3",
    })
    cache = RequestScopedStoreCache()

    await cache.aget(store, ns, "memory.md")
    await cache.aget(store, ns, "doc.md")
    await cache.aget(store, other_ns, "memory.md")
    assert len(store.calls) == 3

    cache.invalidate(ns)  # key=None → namespace-wide drop

    # ns entries refetch; other_ns sibling is untouched.
    await cache.aget(store, ns, "memory.md")
    await cache.aget(store, ns, "doc.md")
    await cache.aget(store, other_ns, "memory.md")
    assert len(store.calls) == 5


@pytest.mark.asyncio
async def test_none_value_is_cached_so_missing_key_lookup_doesnt_repeat() -> None:
    """Caching ``None`` (key absent) saves the second negative lookup."""
    ns = ("user1", "memory")
    store = _CountingStore(values={})
    cache = RequestScopedStoreCache()

    a = await cache.aget(store, ns, "memory.md")
    b = await cache.aget(store, ns, "memory.md")

    assert a is None
    assert b is None
    assert len(store.calls) == 1


@pytest.mark.asyncio
async def test_separate_namespaces_do_not_collide() -> None:
    a_ns = ("user1", "memory")
    b_ns = ("user1", "memos")
    store = _CountingStore({(a_ns, "x.md"): "memory", (b_ns, "x.md"): "memo"})
    cache = RequestScopedStoreCache()

    a = await cache.aget(store, a_ns, "x.md")
    b = await cache.aget(store, b_ns, "x.md")

    assert a == "memory"
    assert b == "memo"
    assert len(store.calls) == 2
