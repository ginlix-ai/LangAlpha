"""Tests for BackgroundTaskManager._buffer_event_redis pipeline rewrite.

Verifies:
  - Happy path calls the atomic pipeline helper exactly once per event
  - Pipeline failure triggers in-memory fallback (CRITICAL regression — a Redis
    blip must not drop SSE events that the frontend is about to replay)
  - Redis disabled path writes directly to in-memory without touching Redis
  - Event ID parsing handles both numeric and malformed SSE headers
"""

from __future__ import annotations

from datetime import datetime
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.server.services.background_task_manager import (
    BackgroundTaskManager,
    TaskInfo,
    TaskStatus,
)


def _make_btm(backend: str = "redis", fallback: bool = True) -> BackgroundTaskManager:
    with patch("src.server.services.background_task_manager.get_max_concurrent_workflows", return_value=10), \
         patch("src.server.services.background_task_manager.get_workflow_result_ttl", return_value=3600), \
         patch("src.server.services.background_task_manager.get_abandoned_workflow_timeout", return_value=3600), \
         patch("src.server.services.background_task_manager.get_cleanup_interval", return_value=60), \
         patch("src.server.services.background_task_manager.is_intermediate_storage_enabled", return_value=False), \
         patch("src.server.services.background_task_manager.get_max_stored_messages_per_agent", return_value=1000), \
         patch("src.server.services.background_task_manager.get_event_storage_backend", return_value=backend), \
         patch("src.server.services.background_task_manager.is_event_storage_fallback_enabled", return_value=fallback), \
         patch("src.server.services.background_task_manager.get_redis_ttl_workflow_events", return_value=86400):
        btm = BackgroundTaskManager()
    return btm


def _register_task(btm: BackgroundTaskManager, thread_id: str = "thread-1") -> TaskInfo:
    task_info = TaskInfo(
        thread_id=thread_id,
        status=TaskStatus.RUNNING,
        created_at=datetime.now(),
        started_at=datetime.now(),
    )
    btm.tasks[thread_id] = task_info
    return task_info


class TestBufferEventRedisHappyPath:

    @pytest.mark.asyncio
    async def test_single_pipeline_call_per_event(self):
        """Happy path: one event → exactly one pipelined_event_buffer call."""
        btm = _make_btm()
        _register_task(btm)

        mock_cache = MagicMock()
        mock_cache.enabled = True
        mock_cache.pipelined_event_buffer = AsyncMock(return_value=(True, 1))

        with patch(
            "src.server.services.background_task_manager.get_cache_client",
            return_value=mock_cache,
        ):
            await btm._buffer_event_redis("thread-1", "id: 42\nevent: x\ndata: hi\n\n")

        assert mock_cache.pipelined_event_buffer.await_count == 1
        call = mock_cache.pipelined_event_buffer.await_args
        assert call.kwargs["events_key"] == "workflow:events:thread-1"
        assert call.kwargs["meta_key"] == "workflow:events:meta:thread-1"
        assert call.kwargs["last_event_id"] == 42
        assert call.kwargs["max_size"] == 1000
        assert call.kwargs["ttl"] == 86400

    @pytest.mark.asyncio
    async def test_malformed_event_id_still_writes(self):
        """An event without a parseable `id:` line still gets buffered."""
        btm = _make_btm()
        _register_task(btm)

        mock_cache = MagicMock()
        mock_cache.enabled = True
        mock_cache.pipelined_event_buffer = AsyncMock(return_value=(True, 1))

        with patch(
            "src.server.services.background_task_manager.get_cache_client",
            return_value=mock_cache,
        ):
            await btm._buffer_event_redis("thread-1", "event: x\ndata: hi\n\n")

        assert mock_cache.pipelined_event_buffer.await_count == 1
        assert mock_cache.pipelined_event_buffer.await_args.kwargs["last_event_id"] is None


class TestBufferEventRedisFallback:
    """CRITICAL regression tests: Redis failures must not drop events."""

    @pytest.mark.asyncio
    async def test_pipeline_failure_falls_back_to_in_memory(self):
        """Pipeline returns False → event lands in task_info.result_buffer."""
        btm = _make_btm(fallback=True)
        task_info = _register_task(btm)

        mock_cache = MagicMock()
        mock_cache.enabled = True
        mock_cache.pipelined_event_buffer = AsyncMock(return_value=(False, 0))

        with patch(
            "src.server.services.background_task_manager.get_cache_client",
            return_value=mock_cache,
        ):
            await btm._buffer_event_redis("thread-1", "id: 1\ndata: lost-if-broken\n\n")

        assert len(task_info.result_buffer) == 1
        assert "lost-if-broken" in task_info.result_buffer[0]

    @pytest.mark.asyncio
    async def test_cache_client_raises_falls_back_to_in_memory(self):
        """REGRESSION: if get_cache_client() itself throws, event still lands in deque.

        Pre-fix, the getter call sat outside the try/except. A misconfigured
        singleton would leak an exception out of _buffer_event_redis and kill
        the streaming handler for the thread. Now it's guarded — getter
        failure falls back to in-memory just like a pipeline failure.
        """
        btm = _make_btm(fallback=True)
        task_info = _register_task(btm)

        with patch(
            "src.server.services.background_task_manager.get_cache_client",
            side_effect=RuntimeError("cache singleton init failed"),
        ):
            await btm._buffer_event_redis("thread-1", "id: 42\ndata: must-survive\n\n")

        assert len(task_info.result_buffer) == 1
        assert "must-survive" in task_info.result_buffer[0]

    @pytest.mark.asyncio
    async def test_pipeline_failure_fallback_disabled_drops_event(self):
        """When fallback disabled, event is dropped but does not raise."""
        btm = _make_btm(fallback=False)
        task_info = _register_task(btm)

        mock_cache = MagicMock()
        mock_cache.enabled = True
        mock_cache.pipelined_event_buffer = AsyncMock(return_value=(False, 0))

        with patch(
            "src.server.services.background_task_manager.get_cache_client",
            return_value=mock_cache,
        ):
            await btm._buffer_event_redis("thread-1", "id: 1\ndata: x\n\n")

        assert len(task_info.result_buffer) == 0

    @pytest.mark.asyncio
    async def test_redis_disabled_writes_to_in_memory(self):
        """When cache.enabled is False, bypass Redis entirely."""
        btm = _make_btm(fallback=True)
        task_info = _register_task(btm)

        mock_cache = MagicMock()
        mock_cache.enabled = False
        mock_cache.pipelined_event_buffer = AsyncMock()

        with patch(
            "src.server.services.background_task_manager.get_cache_client",
            return_value=mock_cache,
        ):
            await btm._buffer_event_redis("thread-1", "id: 1\ndata: x\n\n")

        assert mock_cache.pipelined_event_buffer.await_count == 0
        assert len(task_info.result_buffer) == 1

    @pytest.mark.asyncio
    async def test_in_memory_buffer_respects_max_size(self):
        """Fallback path enforces max_stored_messages via popleft."""
        btm = _make_btm(fallback=True)
        task_info = _register_task(btm)
        btm.max_stored_messages = 3

        mock_cache = MagicMock()
        mock_cache.enabled = True
        mock_cache.pipelined_event_buffer = AsyncMock(return_value=(False, 0))

        with patch(
            "src.server.services.background_task_manager.get_cache_client",
            return_value=mock_cache,
        ):
            for i in range(5):
                await btm._buffer_event_redis("thread-1", f"id: {i}\ndata: {i}\n\n")

        assert len(task_info.result_buffer) == 3
        assert "id: 2" in task_info.result_buffer[0]
        assert "id: 4" in task_info.result_buffer[-1]
