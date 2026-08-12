"""Unit tests for RedisCacheClient.acquire_lock / release_lock."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest

from src.utils.cache.redis_cache import RedisCacheClient


def _make_client() -> RedisCacheClient:
    client = RedisCacheClient.__new__(RedisCacheClient)
    client.enabled = True
    client.stats = {"hits": 0, "misses": 0, "sets": 0, "deletes": 0, "errors": 0}
    client.client = None
    return client


@pytest.mark.asyncio
async def test_acquire_lock_acquired():
    cache = _make_client()
    redis_mock = MagicMock()
    redis_mock.set = AsyncMock(return_value=True)  # SET NX succeeded
    cache.client = redis_mock

    assert await cache.acquire_lock("news:lock:k", "tok", 1000) is True
    redis_mock.set.assert_awaited_once_with("news:lock:k", "tok", nx=True, px=1000)


@pytest.mark.asyncio
async def test_acquire_lock_contended_returns_false():
    cache = _make_client()
    redis_mock = MagicMock()
    redis_mock.set = AsyncMock(return_value=None)  # key already held
    cache.client = redis_mock

    assert await cache.acquire_lock("news:lock:k", "tok", 1000) is False


@pytest.mark.asyncio
async def test_acquire_lock_redis_error_returns_none():
    cache = _make_client()
    redis_mock = MagicMock()
    redis_mock.set = AsyncMock(side_effect=RuntimeError("down"))
    cache.client = redis_mock

    assert await cache.acquire_lock("news:lock:k", "tok", 1000) is None
    assert cache.stats["errors"] == 1


@pytest.mark.asyncio
async def test_acquire_lock_disabled_returns_none():
    cache = _make_client()
    cache.enabled = False
    assert await cache.acquire_lock("news:lock:k", "tok", 1000) is None


@pytest.mark.asyncio
async def test_release_lock_compare_and_deletes():
    cache = _make_client()
    redis_mock = MagicMock()
    redis_mock.eval = AsyncMock(return_value=1)
    cache.client = redis_mock

    await cache.release_lock("news:lock:k", "tok")
    args = redis_mock.eval.await_args.args
    # eval(script, numkeys, key, token)
    assert args[1] == 1
    assert args[2] == "news:lock:k"
    assert args[3] == "tok"


@pytest.mark.asyncio
async def test_release_lock_disabled_is_noop():
    cache = _make_client()
    cache.enabled = False
    await cache.release_lock("news:lock:k", "tok")  # must not raise
