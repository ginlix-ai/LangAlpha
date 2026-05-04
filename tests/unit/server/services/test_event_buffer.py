"""Tests for BackgroundTaskManager._buffer_event_redis.

Verifies that every spilled event hits the atomic ``pipelined_event_buffer``
helper exactly once with the right keys and parsed event id, and that
Redis-disabled / pipeline-failure paths quietly drop the event.
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


def _make_btm(backend: str = "redis") -> BackgroundTaskManager:
    with patch("src.server.services.background_task_manager.get_max_concurrent_workflows", return_value=10), \
         patch("src.server.services.background_task_manager.get_workflow_result_ttl", return_value=3600), \
         patch("src.server.services.background_task_manager.get_abandoned_workflow_timeout", return_value=3600), \
         patch("src.server.services.background_task_manager.get_cleanup_interval", return_value=60), \
         patch("src.server.services.background_task_manager.is_intermediate_storage_enabled", return_value=False), \
         patch("src.server.services.background_task_manager.get_max_stored_messages_per_agent", return_value=1000), \
         patch("src.server.services.background_task_manager.get_event_storage_backend", return_value=backend), \
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
        assert call.kwargs["stream_key"] == "workflow:stream:thread-1"
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


class TestBufferEventRedisFailureModes:
    """Redis-unavailable paths drop the event without raising.

    Pre-Streams there was an in-memory deque fallback; with the Streams
    cutover the only consumer is XREAD on the Stream key, so a Redis blip
    means the event simply doesn't reach any consumer. The producer must
    not crash the workflow over a transient Redis failure.
    """

    @pytest.mark.asyncio
    async def test_pipeline_failure_does_not_raise(self):
        """Pipeline returns False → log + drop, no exception bubbled up."""
        btm = _make_btm()
        _register_task(btm)

        mock_cache = MagicMock()
        mock_cache.enabled = True
        mock_cache.pipelined_event_buffer = AsyncMock(return_value=(False, 0))

        with patch(
            "src.server.services.background_task_manager.get_cache_client",
            return_value=mock_cache,
        ):
            # Must not raise.
            await btm._buffer_event_redis("thread-1", "id: 1\ndata: lost-if-broken\n\n")

        assert mock_cache.pipelined_event_buffer.await_count == 1

    @pytest.mark.asyncio
    async def test_cache_client_raises_does_not_raise(self):
        """A misconfigured cache singleton must not crash the workflow."""
        btm = _make_btm()
        _register_task(btm)

        with patch(
            "src.server.services.background_task_manager.get_cache_client",
            side_effect=RuntimeError("cache singleton init failed"),
        ):
            await btm._buffer_event_redis("thread-1", "id: 42\ndata: must-survive\n\n")

    @pytest.mark.asyncio
    async def test_redis_disabled_skips_pipeline(self):
        """When ``cache.enabled`` is False, no pipeline call is issued."""
        btm = _make_btm()
        _register_task(btm)

        mock_cache = MagicMock()
        mock_cache.enabled = False
        mock_cache.pipelined_event_buffer = AsyncMock()

        with patch(
            "src.server.services.background_task_manager.get_cache_client",
            return_value=mock_cache,
        ):
            await btm._buffer_event_redis("thread-1", "id: 1\ndata: x\n\n")

        assert mock_cache.pipelined_event_buffer.await_count == 0
