"""Regression test for resume-time Redis spool cleanup.

Without clearing the Redis spool when a completed task is resumed, the
new run's records (with seq starting at 1 again) would RPUSH onto the
prior run's list, causing reconnect/persistence to replay both runs
interleaved with overlapping seqs.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from ptc_agent.agent.middleware.background_subagent.middleware import (
    BackgroundSubagentMiddleware,
)
from ptc_agent.agent.middleware.background_subagent.registry import (
    BackgroundTask,
    BackgroundTaskRegistry,
)


def _make_completed_task(task_id: str = "abc123") -> BackgroundTask:
    task = BackgroundTask(
        tool_call_id="tc-1",
        task_id=task_id,
        description="prior run",
        prompt="prior prompt",
        subagent_type="general-purpose",
        agent_id="general-purpose",
    )
    task.completed = True
    task.result = {"messages": []}
    # Pretend the prior run captured events
    task.captured_event_seq = 7
    task.captured_event_count = 7
    task.captured_event_bytes = 1234
    task.redis_write_failed = False
    return task


@pytest.mark.asyncio
async def test_reset_for_resume_deletes_both_redis_keys():
    """Resume must DELETE both subagent:events:{thread}:{task} and
    subagent:events:meta:{thread}:{task} before resetting seq counters.
    Otherwise the new run's seq=1 record collides with the prior run's
    seq=1 record on the same Redis list."""
    registry = BackgroundTaskRegistry(thread_id="thread-x")
    middleware = BackgroundSubagentMiddleware(registry=registry, enabled=True)
    task = _make_completed_task("abc123")

    cache = MagicMock()
    cache.enabled = True
    cache.delete = AsyncMock()

    with patch(
        "src.utils.cache.redis_cache.get_cache_client",
        return_value=cache,
    ):
        await middleware._reset_task_for_resume(task)

    deleted_keys = [call.args[0] for call in cache.delete.await_args_list]
    assert "subagent:events:thread-x:abc123" in deleted_keys
    assert "subagent:events:meta:thread-x:abc123" in deleted_keys


@pytest.mark.asyncio
async def test_reset_for_resume_resets_seq_counters_after_redis_clear():
    """After Redis cleanup, in-memory seq counters are reset to 0 so the
    next append starts at seq=1 on a fresh Redis list."""
    registry = BackgroundTaskRegistry(thread_id="thread-x")
    middleware = BackgroundSubagentMiddleware(registry=registry, enabled=True)
    task = _make_completed_task()

    cache = MagicMock()
    cache.enabled = True
    cache.delete = AsyncMock()

    with patch(
        "src.utils.cache.redis_cache.get_cache_client",
        return_value=cache,
    ):
        await middleware._reset_task_for_resume(task)

    assert task.completed is False
    assert task.result is None
    assert task.captured_event_seq == 0
    assert task.captured_event_count == 0
    assert task.captured_event_bytes == 0
    assert task.redis_write_failed is False


@pytest.mark.asyncio
async def test_reset_for_resume_redis_failure_does_not_raise():
    """Cache failure during cleanup must not crash the resume path —
    the new run still proceeds; replay may include stale events."""
    registry = BackgroundTaskRegistry(thread_id="thread-x")
    middleware = BackgroundSubagentMiddleware(registry=registry, enabled=True)
    task = _make_completed_task()

    cache = MagicMock()
    cache.enabled = True
    cache.delete = AsyncMock(side_effect=RuntimeError("redis down"))

    with patch(
        "src.utils.cache.redis_cache.get_cache_client",
        return_value=cache,
    ):
        await middleware._reset_task_for_resume(task)

    # Counters still reset even though Redis cleanup failed
    assert task.captured_event_seq == 0
    assert task.completed is False


@pytest.mark.asyncio
async def test_reset_for_resume_skips_redis_when_no_thread_id():
    """Tests that construct a bare registry without a thread_id should
    not attempt Redis operations."""
    registry = BackgroundTaskRegistry(thread_id="")
    middleware = BackgroundSubagentMiddleware(registry=registry, enabled=True)
    task = _make_completed_task()

    cache = MagicMock()
    cache.enabled = True
    cache.delete = AsyncMock()

    with patch(
        "src.utils.cache.redis_cache.get_cache_client",
        return_value=cache,
    ):
        await middleware._reset_task_for_resume(task)

    cache.delete.assert_not_awaited()
