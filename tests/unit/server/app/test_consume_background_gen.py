"""Crash-path cleanup for `_consume_background_gen`.

When a dispatched background generator raises, the except branch tears down the
report-back watch keyed by the *PTC* thread id. Regression: the FLASH_DISPATCH
site (a report-back run) used the flash thread id as the origin key, so a
report-back run that crashed before its terminal handler fired left the durable
watch/pointer alive until TTL and `/status` kept reporting a stale pending run.
"""

from unittest.mock import AsyncMock, patch

import pytest

from src.server.app.threads import _consume_background_gen


async def _crashing_gen():
    raise RuntimeError("kaboom")
    yield  # unreachable — marks this as an async generator


class _FakeClient:
    def __init__(self):
        self.publish = AsyncMock()
        self.xadd = AsyncMock()


class _FakeCache:
    def __init__(self, origin_map):
        self.enabled = True
        self.client = _FakeClient()
        self._origin = origin_map

    async def get(self, key):
        return self._origin.get(key)


def _patched(cache, clear):
    return (
        patch(
            "src.utils.cache.redis_cache.get_cache_client", return_value=cache
        ),
        patch(
            "src.server.handlers.chat.ptc_workflow.clear_flash_report_back", clear
        ),
    )


@pytest.mark.asyncio
async def test_report_back_crash_clears_watch_via_ptc_thread_id():
    # report-back run: thread_id is the flash thread, but the origin lives under
    # the completed PTC thread named by report_back_ptc_thread_id.
    cache = _FakeCache({"ptc_origin:ptc-1": {"flash_thread_id": "flash-1"}})
    clear = AsyncMock()
    p1, p2 = _patched(cache, clear)
    with p1, p2:
        ok = await _consume_background_gen(
            _crashing_gen(),
            "FLASH_DISPATCH",
            "flash-1",
            "run-1",
            report_back_ptc_thread_id="ptc-1",
        )
    assert ok is False
    clear.assert_awaited_once_with(cache, "ptc-1", "flash-1")
    cache.client.publish.assert_awaited_once()
    assert cache.client.publish.call_args[0][0] == "thread:wake:flash-1"


@pytest.mark.asyncio
async def test_ordinary_flash_dispatch_crash_preserves_watch():
    # No report_back id: the origin lookup uses the flash thread id, misses, and
    # leaves a still-running dispatched PTC's keys intact for reload recovery.
    cache = _FakeCache({})  # ptc_origin:flash-1 absent
    clear = AsyncMock()
    p1, p2 = _patched(cache, clear)
    with p1, p2:
        ok = await _consume_background_gen(
            _crashing_gen(), "FLASH_DISPATCH", "flash-1", "run-1"
        )
    assert ok is False
    clear.assert_not_awaited()
    cache.client.publish.assert_not_awaited()


@pytest.mark.asyncio
async def test_ptc_dispatch_crash_clears_via_thread_id():
    # PTC_DISPATCH: thread_id IS the ptc thread, so the default origin key hits.
    cache = _FakeCache({"ptc_origin:ptc-9": {"flash_thread_id": "flash-9"}})
    clear = AsyncMock()
    p1, p2 = _patched(cache, clear)
    with p1, p2:
        ok = await _consume_background_gen(
            _crashing_gen(), "PTC_DISPATCH", "ptc-9", "run-9"
        )
    assert ok is False
    clear.assert_awaited_once_with(cache, "ptc-9", "flash-9")
    assert cache.client.publish.call_args[0][0] == "thread:wake:flash-9"
