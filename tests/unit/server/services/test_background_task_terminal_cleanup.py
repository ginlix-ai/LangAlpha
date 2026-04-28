"""Tests for terminal cleanup of BackgroundTaskManager.

Covers _release_terminal_refs, persistence_complete fan-out across all four
terminal mark methods, and the _mark_completed early-return when the
completion callback fails.
"""

from __future__ import annotations

import asyncio
from datetime import datetime
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.server.services.background_task_manager import (
    BackgroundTaskManager,
    TaskInfo,
    TaskStatus,
)


def _make_btm() -> BackgroundTaskManager:
    with patch("src.server.services.background_task_manager.get_max_concurrent_workflows", return_value=10), \
         patch("src.server.services.background_task_manager.get_workflow_result_ttl", return_value=3600), \
         patch("src.server.services.background_task_manager.get_abandoned_workflow_timeout", return_value=3600), \
         patch("src.server.services.background_task_manager.get_cleanup_interval", return_value=60), \
         patch("src.server.services.background_task_manager.is_intermediate_storage_enabled", return_value=False), \
         patch("src.server.services.background_task_manager.get_max_stored_messages_per_agent", return_value=1000), \
         patch("src.server.services.background_task_manager.get_event_storage_backend", return_value="memory"), \
         patch("src.server.services.background_task_manager.is_event_storage_fallback_enabled", return_value=False), \
         patch("src.server.services.background_task_manager.get_redis_ttl_workflow_events", return_value=86400):
        return BackgroundTaskManager()


def _make_info(thread_id: str = "t-1", **metadata) -> TaskInfo:
    md = {
        "workspace_id": "ws-1",
        "user_id": "u-1",
        "is_byok": False,
        "dispatch_kind": "foreground",
        "response_id": "r-1",
        "handler": object(),
        "token_callback": object(),
        "sandbox": object(),
    }
    md.update(metadata)
    info = TaskInfo(
        thread_id=thread_id,
        status=TaskStatus.RUNNING,
        created_at=datetime.now(),
        metadata=md,
    )
    info.graph = object()
    info.completion_callback = AsyncMock()
    info.result_buffer.extend(["a", "b", "c"])
    return info


# ---------------------------------------------------------------------------
# _release_terminal_refs
# ---------------------------------------------------------------------------


class TestReleaseTerminalRefs:

    @pytest.mark.asyncio
    async def test_releases_heavy_refs_keeps_scalars(self):
        btm = _make_btm()
        info = _make_info()
        btm.tasks["t-1"] = info

        btm._release_terminal_refs("t-1")

        assert info.graph is None
        assert info.completion_callback is None
        assert len(info.result_buffer) == 0
        assert "handler" not in info.metadata
        assert "token_callback" not in info.metadata
        assert "sandbox" not in info.metadata
        # Scalars preserved
        assert info.metadata["workspace_id"] == "ws-1"
        assert info.metadata["user_id"] == "u-1"
        assert info.metadata["is_byok"] is False
        assert info.metadata["dispatch_kind"] == "foreground"
        assert info.metadata["response_id"] == "r-1"

    @pytest.mark.asyncio
    async def test_idempotent(self):
        btm = _make_btm()
        info = _make_info()
        btm.tasks["t-1"] = info

        btm._release_terminal_refs("t-1")
        btm._release_terminal_refs("t-1")  # No exception

        assert info.graph is None
        assert info.metadata["workspace_id"] == "ws-1"

    @pytest.mark.asyncio
    async def test_missing_thread_is_safe(self):
        btm = _make_btm()
        btm._release_terminal_refs("missing")  # Must not raise

    @pytest.mark.asyncio
    async def test_inner_task_only_dropped_when_done(self):
        btm = _make_btm()
        info = _make_info()
        running = MagicMock(spec=asyncio.Task)
        running.done.return_value = False
        info.inner_task = running
        btm.tasks["t-1"] = info

        btm._release_terminal_refs("t-1")

        assert info.inner_task is running


# ---------------------------------------------------------------------------
# Terminal-mark methods set persistence_complete and release refs
# ---------------------------------------------------------------------------


class TestMarkCompletedReleases:

    @pytest.mark.asyncio
    async def test_mark_completed_releases_and_signals(self):
        btm = _make_btm()
        info = _make_info()
        info.completion_callback = AsyncMock()
        info.metadata["workspace_id"] = "ws-1"
        info.metadata["user_id"] = "u-1"
        btm.tasks["t-1"] = info

        bg_store = MagicMock()
        bg_store.get_instance.return_value.get_registry = AsyncMock(return_value=None)
        ps_module = MagicMock()
        ps_module.ConversationPersistenceService.get_instance.return_value._current_response_id = None

        with patch("src.server.services.background_task_manager.release_burst_slot", new=AsyncMock()), \
             patch.dict("sys.modules", {
                 "src.server.services.background_registry_store": bg_store,
                 "src.server.services.persistence.conversation": ps_module,
             }):
            await btm._mark_completed("t-1")

        assert info.persistence_complete.is_set()
        assert info.graph is None
        assert info.completion_callback is None
        assert info.metadata["workspace_id"] == "ws-1"


class TestMarkFailedReleases:

    @pytest.mark.asyncio
    async def test_mark_failed_releases_and_signals(self):
        btm = _make_btm()
        info = _make_info()
        info.metadata["workspace_id"] = None  # Skip persistence body
        btm.tasks["t-1"] = info

        with patch("src.server.services.background_task_manager.release_burst_slot", new=AsyncMock()):
            await btm._mark_failed("t-1", "boom")

        assert info.persistence_complete.is_set()
        assert info.graph is None
        assert info.metadata["user_id"] == "u-1"


class TestMarkCancelledReleases:

    @pytest.mark.asyncio
    async def test_mark_cancelled_releases_and_signals(self):
        btm = _make_btm()
        info = _make_info()
        info.metadata["workspace_id"] = None  # Skip persistence body
        btm.tasks["t-1"] = info

        with patch("src.server.services.background_task_manager.release_burst_slot", new=AsyncMock()):
            await btm._mark_cancelled("t-1")

        assert info.persistence_complete.is_set()
        assert info.graph is None
        assert info.metadata["dispatch_kind"] == "foreground"


class TestMarkSoftInterruptedReleases:

    @pytest.mark.asyncio
    async def test_mark_soft_interrupted_releases_and_signals(self):
        btm = _make_btm()
        info = _make_info()
        info.metadata["workspace_id"] = None  # Skip persistence body
        btm.tasks["t-1"] = info

        with patch("src.server.services.background_task_manager.release_burst_slot", new=AsyncMock()):
            await btm._mark_soft_interrupted("t-1")

        assert info.persistence_complete.is_set()
        assert info.graph is None


# ---------------------------------------------------------------------------
# _mark_completed early return on callback failure
# ---------------------------------------------------------------------------


class TestMarkCompletedCallbackFailure:

    @pytest.mark.asyncio
    async def test_callback_failure_skips_collector_and_only_one_burst_release(self):
        btm = _make_btm()
        info = _make_info()
        info.completion_callback = AsyncMock(side_effect=RuntimeError("boom"))
        btm.tasks["t-1"] = info

        burst_release = AsyncMock()
        bg_store_module = MagicMock()
        bg_store_instance = MagicMock()
        bg_store_instance.get_registry = AsyncMock(return_value=None)
        bg_store_module.BackgroundRegistryStore.get_instance.return_value = bg_store_instance

        ps_module = MagicMock()
        ps_module.ConversationPersistenceService.get_instance.return_value._current_response_id = "r-1"

        # Track every asyncio.create_task call from background_task_manager
        # so we can assert the post-`else` collector spawn never fires.
        real_create_task = asyncio.create_task
        spawn_names: list[str | None] = []

        def tracking_create_task(coro, *args, **kwargs):
            spawn_names.append(kwargs.get("name"))
            return real_create_task(coro, *args, **kwargs)

        with patch("src.server.services.background_task_manager.release_burst_slot", new=burst_release), \
             patch("src.server.services.background_task_manager.asyncio.create_task", side_effect=tracking_create_task), \
             patch.dict("sys.modules", {
                 "src.server.services.background_registry_store": bg_store_module,
                 "src.server.services.persistence.conversation": ps_module,
             }):
            await btm._mark_completed("t-1")

        # Burst slot released exactly once (via _mark_failed only)
        assert burst_release.call_count == 1
        # Collector spawn skipped — bg_registry was never fetched
        bg_store_instance.get_registry.assert_not_called()
        # Direct: the post-`else` collector spawn names tasks
        # `subagent-collector-{thread_id}-post-tail`. Assert no such task was
        # created. Future refactors that inline registry lookup or move it
        # into _mark_failed will still trip this guard.
        assert not any(
            name and "subagent-collector" in name and "post-tail" in name
            for name in spawn_names
        ), f"unexpected collector task spawned: {spawn_names}"


# ---------------------------------------------------------------------------
# _await_drain_and_cleanup_tasks heavy-ref release
# ---------------------------------------------------------------------------


class _FakeTask:
    def __init__(self, task_id: str = "abc123") -> None:
        from collections import deque

        self.task_id = task_id
        self.tool_call_id = f"tc-{task_id}"
        self.display_id = f"Task-{task_id}"
        self.agent_id = f"general-purpose:{task_id}"
        self.captured_events_tail = deque(
            [{"seq": 1, "event": "x", "data": {}, "agent_id": self.agent_id}],
            maxlen=1000,
        )
        self.captured_event_seq = 1
        self.captured_event_count = 1
        self.captured_event_bytes = 0
        self.redis_write_failed = False
        self.per_call_records = [{"tokens": 1}]
        self.asyncio_task = MagicMock(spec=asyncio.Task)
        self.handler_task = MagicMock(spec=asyncio.Task)
        self.new_event_signal = asyncio.Event()
        self.new_event_signal.set()
        self.sse_drain_complete = asyncio.Event()
        self.sse_drain_complete.set()
        self.completed = True
        self.result = {"success": True, "value": 42}
        self.error = None


class TestAwaitDrainCleanup:

    @pytest.mark.asyncio
    async def test_drops_handles_keeps_scalars(self):
        btm = _make_btm()
        task = _FakeTask("abc123")

        cache = MagicMock()
        cache.delete = AsyncMock()
        with patch("src.server.services.background_task_manager.get_cache_client", return_value=cache), \
             patch("src.server.services.background_task_manager.get_sse_drain_timeout", return_value=0.01):
            await btm._await_drain_and_cleanup_tasks([task], "thread-x")

        assert len(task.captured_events_tail) == 0
        assert task.per_call_records == []
        assert task.asyncio_task is None
        assert task.handler_task is None
        assert not task.new_event_signal.is_set()
        # Scalars + result preserved (cross-turn TaskOutput contract)
        assert task.task_id == "abc123"
        assert task.tool_call_id == "tc-abc123"
        assert task.display_id == "Task-abc123"
        assert task.agent_id.endswith(":abc123")
        assert task.completed is True
        assert task.result == {"success": True, "value": 42}
        # Tiny scalars stay (not memory pressure)
        assert task.captured_event_seq == 1
        assert task.captured_event_count == 1

    @pytest.mark.asyncio
    async def test_deletes_both_redis_keys(self):
        """Cleanup must delete both the events list AND the meta hash from
        Redis after persistence — paired with the producer-driven spill."""
        btm = _make_btm()
        task = _FakeTask("abc123")

        cache = MagicMock()
        cache.delete = AsyncMock()
        with patch("src.server.services.background_task_manager.get_cache_client", return_value=cache), \
             patch("src.server.services.background_task_manager.get_sse_drain_timeout", return_value=0.01):
            await btm._await_drain_and_cleanup_tasks([task], "thread-x")

        deleted_keys = {call.args[0] for call in cache.delete.await_args_list}
        assert "subagent:events:thread-x:abc123" in deleted_keys
        assert "subagent:events:meta:thread-x:abc123" in deleted_keys

    @pytest.mark.asyncio
    async def test_cache_client_failure_still_releases_local_refs(self):
        """Redis client init failure must not block local heavy-ref cleanup."""
        btm = _make_btm()
        task = _FakeTask("abc123")

        with patch(
            "src.server.services.background_task_manager.get_cache_client",
            side_effect=RuntimeError("cache singleton init failed"),
        ), patch(
            "src.server.services.background_task_manager.get_sse_drain_timeout",
            return_value=0.01,
        ):
            await btm._await_drain_and_cleanup_tasks([task], "thread-x")

        assert len(task.captured_events_tail) == 0
        assert task.per_call_records == []
        assert task.asyncio_task is None
        assert task.handler_task is None
        assert not task.new_event_signal.is_set()
