"""RedisCacheClient TTL safety (set) and batched read alignment (mget).

The safety-net contract: an omitted TTL must never mint an immortal key —
immortality is opt-in via ttl=PERSIST; junk TTLs clamp to SAFETY_TTL.
"""

from __future__ import annotations

import json

import pytest

from src.utils.cache.redis_cache import PERSIST, SAFETY_TTL, RedisCacheClient


class _FakeRedis:
    """Records set/setex calls; mget serves a canned key→raw-bytes map."""

    def __init__(self, store: dict[str, bytes] | None = None):
        self.store = store or {}
        self.set_calls: list[tuple[str, str]] = []
        self.setex_calls: list[tuple[str, int, str]] = []

    async def set(self, key, value):
        self.set_calls.append((key, value))

    async def setex(self, key, ttl, value):
        self.setex_calls.append((key, ttl, value))

    async def mget(self, keys):
        return [self.store.get(k) for k in keys]


@pytest.fixture
def cache():
    client = RedisCacheClient(url="redis://unit-test-never-connects:6379/0")
    client.enabled = True
    client.client = _FakeRedis()
    return client


@pytest.mark.asyncio
async def test_set_without_ttl_gets_safety_ttl(cache):
    assert await cache.set("k", {"a": 1}) is True
    assert cache.client.setex_calls == [("k", SAFETY_TTL, json.dumps({"a": 1}))]
    assert cache.client.set_calls == []


@pytest.mark.asyncio
@pytest.mark.parametrize("bad_ttl", [0, -5])
async def test_non_positive_ttl_clamps_to_safety_ttl(cache, bad_ttl):
    # PERSIST (-1) is the one deliberate exception; other junk clamps.
    if bad_ttl == PERSIST:
        pytest.skip("PERSIST covered separately")
    await cache.set("k", "v", ttl=bad_ttl)
    (_, ttl, _), = cache.client.setex_calls
    assert ttl == SAFETY_TTL


@pytest.mark.asyncio
async def test_persist_is_the_only_immortal_path(cache):
    await cache.set("k", "v", ttl=PERSIST)
    assert cache.client.set_calls == [("k", json.dumps("v"))]
    assert cache.client.setex_calls == []


@pytest.mark.asyncio
async def test_explicit_ttl_passes_through(cache):
    await cache.set("k", "v", ttl=300)
    (_, ttl, _), = cache.client.setex_calls
    assert ttl == 300


@pytest.mark.asyncio
async def test_set_unserializable_returns_false_without_write(cache):
    assert await cache.set("k", object()) is False
    assert cache.client.setex_calls == [] and cache.client.set_calls == []


@pytest.mark.asyncio
async def test_mget_aligns_hits_misses_and_bad_json(cache):
    cache.client.store = {
        "a": json.dumps({"x": 1}).encode(),
        "c": b"{not json",
    }
    out = await cache.mget(["a", "b", "c"])
    assert out == [{"x": 1}, None, None]  # bad JSON degrades to a miss


@pytest.mark.asyncio
async def test_mget_empty_and_disabled(cache):
    assert await cache.mget([]) == []
    cache.enabled = False
    assert await cache.mget(["a", "b"]) == [None, None]


@pytest.mark.asyncio
async def test_mget_client_error_degrades_to_all_misses(cache):
    class _Boom(_FakeRedis):
        async def mget(self, keys):
            raise ConnectionError("redis down")

    cache.client = _Boom()
    assert await cache.mget(["a", "b"]) == [None, None]


# ---------------------------------------------------------------------------
# get_strict — fail-loud read for outbox executors
# ---------------------------------------------------------------------------


class _StrictRedis:
    def __init__(self, store: dict[str, bytes] | None = None, boom: bool = False):
        self.store = store or {}
        self.boom = boom

    async def get(self, key):
        if self.boom:
            raise ConnectionError("redis down")
        return self.store.get(key)


@pytest.mark.asyncio
async def test_get_strict_returns_none_only_when_confirmed_absent(cache):
    cache.client = _StrictRedis({"k": json.dumps({"a": 1}).encode()})
    assert await cache.get_strict("k") == {"a": 1}
    assert await cache.get_strict("missing") is None


@pytest.mark.asyncio
async def test_get_strict_raises_on_transport_failure(cache):
    cache.client = _StrictRedis(boom=True)
    with pytest.raises(ConnectionError):
        await cache.get_strict("k")


@pytest.mark.asyncio
async def test_get_strict_raises_when_disabled_or_uninitialized(cache):
    # `enabled` flips off at runtime on a failed startup connect — strict
    # readers must treat that identically to a blip (raise), never as
    # "confirmed absent" (a drainer would ack a dropped effect).
    cache.enabled = False
    with pytest.raises(RuntimeError):
        await cache.get_strict("k")
    cache.enabled = True
    cache.client = None
    with pytest.raises(RuntimeError):
        await cache.get_strict("k")
