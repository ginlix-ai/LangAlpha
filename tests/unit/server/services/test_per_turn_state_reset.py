"""Tests for per-turn state isolation under ``run_id`` keying.

The previous design relied on identity-guard scaffolding (TaskInfo identity
checks, ``_tracker_write_is_safe``, ``acquire_for_new_execution``, etc.)
because state was keyed by ``thread_id`` alone. After the run_id refactor,
state is keyed by ``(thread_id, run_id)`` so cross-turn aliasing is
impossible by construction — these tests pin that invariant.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from datetime import datetime

from src.server.services.background_task_manager import (
    BackgroundTaskManager,
    TaskInfo,
    TaskStatus,
)
from src.server.services.persistence import conversation as persistence_module
from src.server.services.persistence.conversation import (
    ConversationPersistenceService,
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


@pytest.fixture(autouse=True)
def _clear_persistence_singleton_cache():
    persistence_module._service_instances.clear()
    yield
    persistence_module._service_instances.clear()


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


@pytest.mark.asyncio
async def test_clear_event_buffer_is_run_scoped():
    """clear_event_buffer DELs only the per-run keys — never bleeds across
    runs on the same thread."""
    btm = _make_btm()

    cache = MagicMock()
    cache.enabled = True
    cache.delete = AsyncMock()
    with patch(
        "src.server.services.background_task_manager.get_cache_client",
        return_value=cache,
    ):
        await btm.clear_event_buffer("thread-Z", "run-A")

    deleted = [call.args[0] for call in cache.delete.await_args_list]
    assert "workflow:stream:thread-Z:run-A" in deleted
    assert "workflow:events:meta:thread-Z:run-A" in deleted
    # No legacy thread-only key DEL: the new scheme never writes it.
    assert "workflow:stream:thread-Z" not in deleted


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
async def test_mark_failed_marks_only_targeted_run():
    """``_mark_failed(tid, rid)`` mutates only that run — a concurrent
    different-run TaskInfo on the same thread is untouched."""
    btm = _make_btm()
    old = _new_task_info("thread-F", "run-old", TaskStatus.RUNNING)
    new = _new_task_info("thread-F", "run-new", TaskStatus.RUNNING)
    old.metadata = {"workspace_id": None, "user_id": None}
    new.metadata = {"workspace_id": None, "user_id": None}
    btm.tasks[("thread-F", "run-old")] = old
    btm.tasks[("thread-F", "run-new")] = new

    await btm._mark_failed("thread-F", "run-old", "boom")

    assert old.status == TaskStatus.FAILED
    assert old.error == "boom"
    assert new.status == TaskStatus.RUNNING
    assert new.error is None


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


# ---------------------------------------------------------------------------
# Persistence service: per-run keying invariants
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_get_instance_keyed_by_thread_and_run():
    """Two runs on the same thread get distinct persistence instances."""
    a = ConversationPersistenceService.get_instance("thread-X", "run-A")
    b = ConversationPersistenceService.get_instance("thread-X", "run-B")
    assert a is not b
    # Same key returns the same instance.
    a2 = ConversationPersistenceService.get_instance("thread-X", "run-A")
    assert a is a2


@pytest.mark.asyncio
async def test_cleanup_is_run_scoped():
    """Cleaning up one run's service must not touch another run's cache entry."""
    a = ConversationPersistenceService.get_instance("thread-X", "run-A")
    b = ConversationPersistenceService.get_instance("thread-X", "run-B")

    await a.cleanup()

    assert ("thread-X", "run-A") not in persistence_module._service_instances
    assert persistence_module._service_instances[("thread-X", "run-B")] is b


@pytest.mark.asyncio
async def test_persist_query_start_does_not_gate_on_in_memory_set():
    """``persist_query_start`` always calls ``create_query`` — the DB's
    ON CONFLICT is the canonical idempotency check."""
    svc = ConversationPersistenceService.get_instance(
        "thread-Q", "run-Q",
        workspace_id="ws-1", user_id="u-1",
    )
    # The in-memory dedup set was removed; ON CONFLICT in the DB is the
    # only idempotency check. persist_query_start must always call create_query.
    svc._turn_index_cache = 1

    db_returned_uuid = "11111111-1111-1111-1111-111111111111"
    create_query_mock = AsyncMock(
        return_value={"conversation_query_id": db_returned_uuid, "turn_index": 1}
    )

    with patch(
        "src.server.services.persistence.conversation.qr_db.create_query",
        create_query_mock,
    ):
        returned = await svc.persist_query_start(
            content="hello", query_type="initial",
        )

    create_query_mock.assert_awaited_once()
    kwargs = create_query_mock.call_args.kwargs
    assert kwargs["conversation_thread_id"] == "thread-Q"
    assert kwargs["turn_index"] == 1
    assert returned == db_returned_uuid


@pytest.mark.asyncio
async def test_persist_query_start_raises_on_content_conflict():
    """QueryConflictError surfaces (not silently overwritten) when an
    existing row's content differs."""
    from src.server.database.conversation import QueryConflictError

    svc = ConversationPersistenceService.get_instance(
        "thread-Q", "run-Q",
        workspace_id="ws-1", user_id="u-1",
    )

    async def _raise(*args, **kwargs):
        raise QueryConflictError(
            thread_id="thread-Q", turn_index=0,
            existing_content="earlier content",
        )

    with patch.object(persistence_module.qr_db, "create_query", side_effect=_raise), \
         patch.object(persistence_module.qr_db, "get_next_turn_index", AsyncMock(return_value=0)):
        with pytest.raises(QueryConflictError) as excinfo:
            await svc.persist_query_start(
                content="new content", query_type="initial",
            )

    assert excinfo.value.turn_index == 0


# ---------------------------------------------------------------------------
# _setup_fork_and_persistence wiring
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_setup_fork_and_persistence_uses_run_id_keyed_get_instance():
    """``_setup_fork_and_persistence`` must call ``get_instance(tid, rid, ...)``
    so the per-run keying is the canonical source of truth — no
    ``acquire_for_new_execution`` workaround anymore."""
    from src.server.handlers.chat import _common
    from src.server.models.chat import ChatRequest

    request = MagicMock(spec=ChatRequest)
    request.query_type = None
    request.hitl_response = None
    request.checkpoint_id = None
    request.messages = []
    request.fork_from_turn = None

    sentinel = MagicMock(spec=ConversationPersistenceService)
    sentinel.reset_for_fork = MagicMock()
    sentinel.get_or_calculate_turn_index = AsyncMock(return_value=0)

    with patch.object(
        _common.ConversationPersistenceService,
        "get_instance",
        return_value=sentinel,
    ) as get_instance_mock:
        query_type, is_fork, persistence_service = await _common._setup_fork_and_persistence(
            request=request,
            thread_id="thread-W",
            run_id="run-W",
            workspace_id="ws-W",
            user_id="user-W",
        )

    get_instance_mock.assert_called_once_with(
        thread_id="thread-W", run_id="run-W",
        workspace_id="ws-W", user_id="user-W",
    )
    assert persistence_service is sentinel
    assert query_type == "initial"
    assert is_fork is False
    sentinel.get_or_calculate_turn_index.assert_awaited_once()


@pytest.mark.asyncio
async def test_persist_completion_uses_run_id_as_response_id():
    """``persist_completion`` writes the response row with
    ``conversation_response_id == self.run_id`` (1:1 contract)."""
    svc = ConversationPersistenceService.get_instance(
        "thread-C", "11111111-2222-3333-4444-555555555555",
        workspace_id="ws-1", user_id="u-1",
    )

    captured = {}

    async def _create_response(**kwargs):
        captured.update(kwargs)
        return None

    mock_conn = MagicMock()
    mock_conn.transaction = MagicMock(
        return_value=MagicMock(
            __aenter__=AsyncMock(return_value=None),
            __aexit__=AsyncMock(return_value=None),
        )
    )

    @AsyncMock
    async def _update_thread_status(*args, **kwargs):
        return None

    class _ConnCtx:
        async def __aenter__(self):
            return mock_conn

        async def __aexit__(self, *args):
            return None

    with patch.object(persistence_module.qr_db, "get_next_turn_index",
                      AsyncMock(return_value=0)), \
         patch.object(persistence_module.qr_db, "create_response",
                      side_effect=_create_response), \
         patch.object(persistence_module.qr_db, "update_thread_status",
                      AsyncMock(return_value=None)), \
         patch.object(persistence_module.qr_db, "get_db_connection",
                      return_value=_ConnCtx()), \
         patch.object(svc, "_get_latest_checkpoint_id",
                      AsyncMock(return_value=None)):
        response_id = await svc.persist_completion(
            metadata={"msg_type": "ptc"},
            execution_time=0.1,
        )

    assert response_id == "11111111-2222-3333-4444-555555555555"
    assert captured["conversation_response_id"] == "11111111-2222-3333-4444-555555555555"


@pytest.mark.asyncio
async def test_persist_completion_after_cleanup_short_circuits():
    """A second ``persist_completion`` after the first one has finalized
    (cleanup ran inside ``_finalize_pair``) must short-circuit: return the
    cached ``run_id`` without hitting the DB layer again. Prevents the
    stale-instance PK-collision scenario where a lingering ref re-INSERTs
    at a recalculated turn_index.
    """
    run_id = "11111111-2222-3333-4444-555555555555"
    svc = ConversationPersistenceService.get_instance(
        "thread-C2", run_id,
        workspace_id="ws-1", user_id="u-1",
    )

    create_response_mock = AsyncMock(return_value=None)

    class _ConnCtx:
        async def __aenter__(self):
            conn = MagicMock()
            conn.transaction = MagicMock(
                return_value=MagicMock(
                    __aenter__=AsyncMock(return_value=None),
                    __aexit__=AsyncMock(return_value=None),
                )
            )
            return conn

        async def __aexit__(self, *args):
            return None

    with patch.object(persistence_module.qr_db, "get_next_turn_index",
                      AsyncMock(return_value=0)), \
         patch.object(persistence_module.qr_db, "create_response",
                      create_response_mock), \
         patch.object(persistence_module.qr_db, "update_thread_status",
                      AsyncMock(return_value=None)), \
         patch.object(persistence_module.qr_db, "get_db_connection",
                      return_value=_ConnCtx()), \
         patch.object(svc, "_get_latest_checkpoint_id",
                      AsyncMock(return_value=None)):
        first = await svc.persist_completion(
            metadata={"msg_type": "ptc"},
            execution_time=0.1,
        )
        # cleanup() inside _finalize_pair should now have flipped _finalized.
        assert svc._finalized is True

        # A second call on the same (now post-cleanup) instance must NOT
        # re-INSERT — it must return the cached response_id.
        second = await svc.persist_completion(
            metadata={"msg_type": "ptc"},
            execution_time=0.1,
        )

    assert first == run_id
    assert second == run_id
    create_response_mock.assert_awaited_once()
