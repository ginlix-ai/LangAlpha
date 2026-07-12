"""``_assert_stream_transport_ready`` — chat admission preflight (I6).

Every chat consumer (first connect included) tails the Redis event stream,
so admission must be refused when that transport cannot serve: configured
away (memory backend / storage disabled — permanent) or unreachable
(PING failure — retryable, before anything durable happens).
"""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi import HTTPException

from src.server.app.threads import _assert_stream_transport_ready


def _manager(backend="redis", enable_storage=True):
    m = MagicMock()
    m.event_storage_backend = backend
    m.enable_storage = enable_storage
    return m


def _patches(manager, cache=None):
    from src.server.services.background_task_manager import BackgroundTaskManager

    if cache is None:
        cache = MagicMock()
        cache.enabled = True
        cache.client.ping = AsyncMock(return_value=True)
    return (
        patch.object(
            BackgroundTaskManager,
            "get_instance",
            classmethod(lambda cls: manager),
        ),
        patch("src.utils.cache.redis_cache.get_cache_client", return_value=cache),
    )


@pytest.mark.asyncio
async def test_redis_backend_with_reachable_transport_admits():
    p1, p2 = _patches(_manager())
    with p1, p2:
        await _assert_stream_transport_ready()  # no raise


@pytest.mark.asyncio
async def test_memory_backend_is_refused():
    """No Redis event storage: streams would 200 and never deliver a byte."""
    p1, p2 = _patches(_manager(backend="memory"))
    with p1, p2:
        with pytest.raises(HTTPException) as exc:
            await _assert_stream_transport_ready()
    assert exc.value.status_code == 503
    assert exc.value.detail["code"] == "transport_unavailable"


@pytest.mark.asyncio
async def test_disabled_storage_is_refused():
    p1, p2 = _patches(_manager(enable_storage=False))
    with p1, p2:
        with pytest.raises(HTTPException) as exc:
            await _assert_stream_transport_ready()
    assert exc.value.status_code == 503


@pytest.mark.asyncio
async def test_redis_backend_with_ping_failure_is_503_with_retry_after():
    cache = MagicMock()
    cache.enabled = True
    cache.client.ping = AsyncMock(side_effect=ConnectionError("down"))
    p1, p2 = _patches(_manager(), cache=cache)
    with p1, p2:
        with pytest.raises(HTTPException) as exc:
            await _assert_stream_transport_ready()
    assert exc.value.status_code == 503
    assert exc.value.headers.get("Retry-After") == "3"
