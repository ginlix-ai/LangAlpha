"""Tests for per-turn state isolation under ``run_id`` keying.

The previous design relied on identity-guard scaffolding (TaskInfo identity
checks, ``_tracker_write_is_safe``, ``acquire_for_new_execution``, etc.)
because state was keyed by ``thread_id`` alone. After the run_id refactor,
state is keyed by ``(thread_id, run_id)`` so cross-turn aliasing is
impossible by construction — these tests pin that invariant.
"""

from __future__ import annotations

from unittest.mock import patch

import pytest

from datetime import datetime

from src.server.services.background_task_manager import (
    BackgroundTaskManager,
    TaskInfo,
    TaskStatus,
)
def _new_task_info(
    thread_id: str, run_id: str, status: "TaskStatus"
) -> "TaskInfo":
    return TaskInfo(
        thread_id=thread_id,
        run_id=run_id,
        status=status,
        created_at=datetime.now(),
    )


def _make_btm() -> BackgroundTaskManager:
    with patch("src.server.services.background_task_manager.get_max_concurrent_workflows", return_value=10), \
         patch("src.server.services.background_task_manager.get_workflow_result_ttl", return_value=3600), \
         patch("src.server.services.background_task_manager.get_abandoned_workflow_timeout", return_value=3600), \
         patch("src.server.services.background_task_manager.get_cleanup_interval", return_value=60), \
         patch("src.server.services.background_task_manager.is_intermediate_storage_enabled", return_value=False), \
         patch("src.server.services.background_task_manager.get_max_stored_messages_per_agent", return_value=1000), \
         patch("src.server.services.background_task_manager.get_event_storage_backend", return_value="redis"), \
         patch("src.server.services.background_task_manager.get_redis_ttl_workflow_events", return_value=86400):
        return BackgroundTaskManager()


# ---------------------------------------------------------------------------
# BTM: per-run keying invariants
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_pre_register_uses_per_run_key():
    """Each ``(thread_id, run_id)`` gets its own slot — same thread, two
    runs coexist in the cache."""
    btm = _make_btm()
    assert await btm.pre_register("thread-X", "run-A") is True
    assert await btm.pre_register("thread-X", "run-B") is True
    assert ("thread-X", "run-A") in btm.tasks
    assert ("thread-X", "run-B") in btm.tasks


@pytest.mark.asyncio
async def test_pre_register_rejects_duplicate_run():
    """Re-registering the exact same ``(thread_id, run_id)`` is a no-op
    that returns False."""
    btm = _make_btm()
    assert await btm.pre_register("thread-X", "run-A") is True
    assert await btm.pre_register("thread-X", "run-A") is False


def test_event_buffer_is_never_deleted_at_finalize():
    """1.5 retention contract: streams live to redis_event_ttl so reconnects
    can replay terminal runs — no code path DELs them at finalize. The old
    ``clear_event_buffer`` must stay gone."""
    btm = _make_btm()
    assert not hasattr(btm, "clear_event_buffer")


@pytest.mark.asyncio
async def test_admission_lock_is_per_thread_and_idempotent():
    """Admission lock is thread-scoped because admission is a per-thread
    invariant (one foreground turn at a time on a thread)."""
    import asyncio
    btm = _make_btm()

    a1 = await btm.get_admission_lock("thread-A")
    a2 = await btm.get_admission_lock("thread-A")
    b = await btm.get_admission_lock("thread-B")

    assert a1 is a2
    assert a1 is not b
    assert isinstance(a1, asyncio.Lock)


@pytest.mark.asyncio
async def test_get_task_info_with_run_id_targets_specific_run():
    """``get_task_info(tid, rid)`` targets exactly that run."""
    btm = _make_btm()
    ti_a = _new_task_info("thread-X", "run-A", TaskStatus.RUNNING)
    ti_b = _new_task_info("thread-X", "run-B", TaskStatus.QUEUED)
    btm.tasks[("thread-X", "run-A")] = ti_a
    btm.tasks[("thread-X", "run-B")] = ti_b

    fetched_a = await btm.get_task_info("thread-X", "run-A")
    fetched_b = await btm.get_task_info("thread-X", "run-B")

    assert fetched_a is ti_a
    assert fetched_b is ti_b


@pytest.mark.asyncio
async def test_get_task_info_without_run_id_returns_latest():
    """``get_task_info(tid)`` (no run_id) returns the latest-created run."""
    import asyncio as _a

    btm = _make_btm()
    older = _new_task_info("thread-X", "run-A", TaskStatus.COMPLETED)
    # Tiny sleep so created_at strictly differs.
    await _a.sleep(0.001)
    newer = _new_task_info("thread-X", "run-B", TaskStatus.RUNNING)
    btm.tasks[("thread-X", "run-A")] = older
    btm.tasks[("thread-X", "run-B")] = newer

    fetched = await btm.get_task_info("thread-X")
    assert fetched is newer


@pytest.mark.asyncio
async def test_cleanup_preserves_admission_locks():
    """Admission locks are NOT reclaimed by cleanup.

    Reclaiming them creates a race: ``get_admission_lock`` returns the
    Lock object under ``task_lock`` and the caller then awaits
    ``acquire()`` outside the lock. A cleanup-time deletion in that gap
    would let a concurrent caller create a fresh Lock for the same
    thread, and both POSTs would acquire DIFFERENT lock objects — silently
    defeating admission. The dict is tiny; we keep it.
    """
    from datetime import timedelta

    btm = _make_btm()
    btm.result_ttl = 0

    lock = await btm.get_admission_lock("thread-L")
    assert "thread-L" in btm._admission_locks

    ti = _new_task_info("thread-L", "run-1", TaskStatus.COMPLETED)
    ti.completed_at = datetime.now() - timedelta(hours=1)
    btm.tasks[("thread-L", "run-1")] = ti

    await btm._cleanup_abandoned_tasks()

    # Task entry is evicted, but the admission lock survives.
    assert ("thread-L", "run-1") not in btm.tasks
    assert "thread-L" in btm._admission_locks
    assert btm._admission_locks["thread-L"] is lock
    assert not lock.locked()
