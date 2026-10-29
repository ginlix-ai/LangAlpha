"""The connect log names the server without the AUTH password the URL carries."""

from __future__ import annotations

import logging

import pytest

from src.utils.cache import redis_cache
from src.utils.cache.redis_cache import RedisCacheClient


class _Pinged:
    def __init__(self, **_):
        pass

    async def ping(self):
        return True


@pytest.mark.asyncio
async def test_connect_log_names_the_server_not_the_password(monkeypatch, caplog):
    monkeypatch.setattr(redis_cache.redis, "Redis", _Pinged)
    client = RedisCacheClient(url="rediss://:s3cret-token@cache.example.com:6380/2")
    client.enabled = True

    with caplog.at_level(logging.INFO, logger=redis_cache.__name__):
        await client.connect()

    assert "s3cret-token" not in caplog.text
    assert "Redis cache connected: rediss://cache.example.com:6380/2 " in caplog.text
