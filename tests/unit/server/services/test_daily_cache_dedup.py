"""Single-flight serialization on DailyCacheService's sync full-fetch path.

Regression guard: a burst of concurrent requests for the same daily series
(cache missing, or a watermark-stale envelope just discarded) must coalesce
onto ONE upstream fetch. The full-fetch path serializes on the same per-key
lock as _delta_refresh; the leader fetches and fills the cache, followers
re-read the fill — otherwise N concurrent readers each fire their own blocking
upstream fetch (thundering herd at the open before the provider publishes
today's daily bar).
"""

from __future__ import annotations

import asyncio

import pytest

from src.server.services.cache import daily_cache_service as dcs
from src.server.services.cache.daily_cache_service import DailyCacheService

_WATERMARK_MS = 1_700_000_000_000  # arbitrary fixed epoch-ms for the stub bar


class _StubCache:
    """In-memory cache: async get/set over a dict, matching RedisCacheClient."""

    def __init__(self) -> None:
        self.store: dict[str, dict] = {}

    async def get(self, key: str):
        return self.store.get(key)

    async def set(self, key: str, value: dict, ttl: int | None = None) -> None:
        self.store[key] = value


class _GatedProvider:
    """Provider stub whose daily fetch blocks on a gate, counting calls."""

    def __init__(self, gate: asyncio.Event) -> None:
        self._gate = gate
        self.calls = 0
        self.source_names = ["stub"]

    async def get_daily_with_source(self, symbol, from_date, to_date, is_index, user_id):
        self.calls += 1
        await self._gate.wait()
        return [{"time": _WATERMARK_MS, "close": 1.0}], "stub", False


@pytest.fixture
def fresh_service(monkeypatch):
    """Reset the singleton so in-flight/lock state doesn't leak across tests."""
    DailyCacheService._instance = None
    gate = asyncio.Event()
    provider = _GatedProvider(gate)
    cache = _StubCache()

    async def _get_provider():
        return provider

    monkeypatch.setattr(dcs, "get_cache_client", lambda: cache)
    monkeypatch.setattr(dcs, "get_market_data_provider", _get_provider)
    yield DailyCacheService(), provider, cache, gate
    DailyCacheService._instance = None


@pytest.mark.asyncio
async def test_concurrent_cold_reads_coalesce_to_one_fetch(fresh_service):
    svc, provider, _cache, gate = fresh_service

    tasks = [asyncio.create_task(svc.get_stock_daily("AAPL")) for _ in range(5)]
    # Let the leader grab the lock and the rest queue behind it on the gate.
    await asyncio.sleep(0.02)
    gate.set()
    results = await asyncio.gather(*tasks)

    assert provider.calls == 1  # single upstream fetch for the whole burst
    assert all(r.data == [{"time": _WATERMARK_MS, "close": 1.0}] for r in results)
    # Exactly one leader fetched (cached=False); the rest served the fill.
    assert sum(r.cached is False for r in results) == 1
    assert sum(r.cached is True for r in results) == 4


@pytest.mark.asyncio
async def test_distinct_symbols_do_not_coalesce(fresh_service):
    svc, provider, _cache, gate = fresh_service
    gate.set()  # no need to block; just verify per-key separation

    await asyncio.gather(svc.get_stock_daily("AAPL"), svc.get_stock_daily("MSFT"))

    assert provider.calls == 2


@pytest.mark.asyncio
async def test_leader_cancellation_does_not_poison_follower(fresh_service):
    # A waiter disconnecting (cancel) must not poison others: cancelling the
    # lock leader just releases the lock. The follower becomes the new leader
    # and fetches — it still resolves with data (never a CancelledError).
    svc, provider, _cache, gate = fresh_service

    t1 = asyncio.create_task(svc.get_stock_daily("AAPL"))  # becomes leader
    t2 = asyncio.create_task(svc.get_stock_daily("AAPL"))  # waits on the lock
    await asyncio.sleep(0.02)

    t1.cancel()
    with pytest.raises(asyncio.CancelledError):
        await t1

    gate.set()
    result = await t2  # follower still resolves with data, not poisoned
    assert result.data == [{"time": _WATERMARK_MS, "close": 1.0}]
    # Leader's upstream fetch was abandoned on cancel, so the follower re-fetched.
    assert provider.calls == 2


@pytest.mark.asyncio
async def test_second_identical_call_serves_cache(fresh_service):
    # After the leader fills the cache, an identical follow-up serves the fill
    # (fresh fetched_at → not stale) instead of re-fetching.
    svc, provider, _cache, gate = fresh_service
    gate.set()

    first = await svc.get_stock_daily("AAPL")
    second = await svc.get_stock_daily("AAPL")

    assert provider.calls == 1
    assert first.cached is False and second.cached is True
