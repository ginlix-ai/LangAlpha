"""Tests for BackgroundTaskManager._buffer_event_redis.

Verifies that every spilled event hits the atomic ``pipelined_event_buffer``
helper exactly once with the right keys and parsed event id, and that every
transport failure is FATAL (I6): a dropped event would silently desync the
replay archive, so the buffer raises ``TransportLostError`` and the run
finalizes failed(transport_lost) instead of completing with holes.
"""

from __future__ import annotations

from datetime import datetime
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.server.services.background_task_manager import (
    BackgroundTaskManager,
    TaskInfo,
    TaskStatus,
    TransportLostError,
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


def _register_task(
    btm: BackgroundTaskManager, thread_id: str = "thread-1", run_id: str = "run-1"
) -> TaskInfo:
    task_info = TaskInfo(
        thread_id=thread_id,
        run_id=run_id,
        status=TaskStatus.RUNNING,
        created_at=datetime.now(),
        started_at=datetime.now(),
    )
    btm.tasks[(thread_id, run_id)] = task_info
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
            await btm._buffer_event_redis("thread-1", "run-1", "id: 42\nevent: x\ndata: hi\n\n")

        assert mock_cache.pipelined_event_buffer.await_count == 1
        call = mock_cache.pipelined_event_buffer.await_args
        # Main workflow path is stream-only; persistence comes from
        # StreamEventAccumulator, not from a separate List.
        assert "events_key" not in call.kwargs
        assert call.kwargs["meta_key"] == "workflow:events:meta:thread-1:run-1"
        assert call.kwargs["stream_key"] == "workflow:stream:thread-1:run-1"
        assert call.kwargs["last_event_id"] == 42
        assert call.kwargs["max_size"] == 1000
        assert call.kwargs["ttl"] == 86400

    @pytest.mark.asyncio
    async def test_malformed_event_id_is_fatal(self):
        """An event without a parseable ``id:`` line must not be silently
        skipped — the archive would diverge from what the model produced."""
        btm = _make_btm()
        _register_task(btm)

        mock_cache = MagicMock()
        mock_cache.enabled = True
        mock_cache.pipelined_event_buffer = AsyncMock(return_value=(True, 1))

        with patch(
            "src.server.services.background_task_manager.get_cache_client",
            return_value=mock_cache,
        ):
            with pytest.raises(TransportLostError):
                await btm._buffer_event_redis(
                    "thread-1", "run-1", "event: x\ndata: hi\n\n"
                )

        assert mock_cache.pipelined_event_buffer.await_count == 0


class TestBufferEventRedisFailureModes:
    """Every Redis-backend transport failure raises TransportLostError (I6).

    The failure handler turns it into failed(transport_lost); the run must
    never complete with events missing from its stream/archive. Only the
    memory backend (no stream consumers by configuration) stays best-effort.
    """

    @pytest.mark.asyncio
    async def test_pipeline_failure_is_fatal(self):
        btm = _make_btm()
        _register_task(btm)

        mock_cache = MagicMock()
        mock_cache.enabled = True
        mock_cache.pipelined_event_buffer = AsyncMock(return_value=(False, 0))

        with patch(
            "src.server.services.background_task_manager.get_cache_client",
            return_value=mock_cache,
        ):
            with pytest.raises(TransportLostError):
                await btm._buffer_event_redis(
                    "thread-1", "run-1", "id: 1\ndata: lost-if-dropped\n\n"
                )

        assert mock_cache.pipelined_event_buffer.await_count == 1

    @pytest.mark.asyncio
    async def test_cache_client_failure_is_fatal(self):
        btm = _make_btm()
        _register_task(btm)

        with patch(
            "src.server.services.background_task_manager.get_cache_client",
            side_effect=RuntimeError("cache singleton init failed"),
        ):
            with pytest.raises(TransportLostError):
                await btm._buffer_event_redis(
                    "thread-1", "run-1", "id: 42\ndata: x\n\n"
                )

    @pytest.mark.asyncio
    async def test_redis_disabled_is_fatal_for_redis_backend(self):
        btm = _make_btm()
        _register_task(btm)

        mock_cache = MagicMock()
        mock_cache.enabled = False
        mock_cache.pipelined_event_buffer = AsyncMock()

        with patch(
            "src.server.services.background_task_manager.get_cache_client",
            return_value=mock_cache,
        ):
            with pytest.raises(TransportLostError):
                await btm._buffer_event_redis("thread-1", "run-1", "id: 1\ndata: x\n\n")

        assert mock_cache.pipelined_event_buffer.await_count == 0

    @pytest.mark.asyncio
    async def test_memory_backend_stays_best_effort(self):
        """No stream transport configured -> nothing to lose, no raise."""
        btm = _make_btm(backend="memory")
        _register_task(btm)

        with patch(
            "src.server.services.background_task_manager.get_cache_client",
            side_effect=RuntimeError("never called"),
        ):
            await btm._buffer_event_redis("thread-1", "run-1", "id: 1\ndata: x\n\n")
