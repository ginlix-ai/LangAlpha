"""Unit tests for RedisCacheClient.scan_keys (non-blocking SCAN helper)."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from src.utils.cache.redis_cache import RedisCacheClient


def _make_client() -> RedisCacheClient:
    client = RedisCacheClient.__new__(RedisCacheClient)
    client.enabled = True
    client.stats = {"hits": 0, "misses": 0, "sets": 0, "deletes": 0, "errors": 0}
    client.client = None
    return client


@pytest.mark.asyncio
async def test_scan_keys_returns_decoded_matches():
    cache = _make_client()

    async def fake_scan_iter(match=None, count=None):
        assert match == "news:*"
        for k in (b"news:general:20", b"news:tickertick:general:50"):
            yield k

    redis_mock = MagicMock()
    redis_mock.scan_iter = fake_scan_iter
    cache.client = redis_mock

    keys = await cache.scan_keys("news:*")
    assert keys == ["news:general:20", "news:tickertick:general:50"]


@pytest.mark.asyncio
async def test_scan_keys_disabled_returns_empty():
    cache = _make_client()
    cache.enabled = False
    assert await cache.scan_keys("news:*") == []


@pytest.mark.asyncio
async def test_scan_keys_swallows_errors():
    cache = _make_client()

    def boom(match=None, count=None):
        raise RuntimeError("redis down")

    redis_mock = MagicMock()
    redis_mock.scan_iter = boom
    cache.client = redis_mock

    assert await cache.scan_keys("news:*") == []
    assert cache.stats["errors"] == 1
