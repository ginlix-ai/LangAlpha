"""Locks for the STARTING-task cancel window.

Between registration and writer publication the spawn path awaits setup
(admission, meta write, opener append) with ``asyncio_task=None``. A stop
landing in that window must stamp the handle-less task cancelled and the
subsequent ``publish_writer`` must abort — otherwise the task is classified
writer-less, dropped, and the later-spawned writer runs to completion with
every append silently discarded.
"""

from __future__ import annotations

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from ptc_agent.agent.middleware.background_subagent.middleware import (
    BackgroundSubagentMiddleware,
    _make_task_done_callback,
)
from ptc_agent.agent.middleware.background_subagent.registry import (
    BackgroundTask,
    BackgroundTaskRegistry,
    TaskWriterLive,
)


async def _register_starting_task(
    registry: BackgroundTaskRegistry, run_id: str = "run-1"
):
    return await registry.register(
        tool_call_id="tc-1",
        description="starting task",
        prompt="do things",
        subagent_type="general-purpose",
        asyncio_task=None,
        run_id=run_id,
    )


@pytest.mark.asyncio
async def test_cancel_run_tasks_stamps_starting_task():
    registry = BackgroundTaskRegistry(thread_id="thread-x")
    task = await _register_starting_task(registry)

    cancelled = await registry.cancel_run_tasks("run-1")

    assert cancelled == 1
    assert task.cancelled is True
    assert task.completed is True
    assert task.error == "Cancelled"
    assert task.result == {
        "success": False,
        "error": "Cancelled",
        "status": "cancelled",
    }


@pytest.mark.asyncio
async def test_publish_writer_aborts_after_cancel():
    registry = BackgroundTaskRegistry(thread_id="thread-x")
    task = await _register_starting_task(registry)
    await registry.cancel_run_tasks("run-1")

    def _factory():
        raise AssertionError("factory must not run after cancel")

    assert await registry.publish_writer(task, _factory) is None


@pytest.mark.asyncio
async def test_publish_writer_publishes_on_live_task():
    registry = BackgroundTaskRegistry(thread_id="thread-x")
    task = await _register_starting_task(registry)

    async def _noop():
        return None

    handle = await registry.publish_writer(
        task, lambda: asyncio.get_running_loop().create_task(_noop())
    )

    assert handle is not None
    assert task.asyncio_task is handle
    await handle


@pytest.mark.asyncio
async def test_publish_writer_refuses_displaced_task_object():
    """Cancel removes the entry, a re-execution re-registers the same
    tool_call_id — the aborted spawn's late publish must not attach its
    writer to the replacement task."""
    registry = BackgroundTaskRegistry(thread_id="thread-x")
    task_a = await _register_starting_task(registry, run_id="run-1")
    await registry.cancel_run_tasks("run-1")
    task_b = await registry.register(
        tool_call_id="tc-1",
        description="re-execution",
        prompt="do things",
        subagent_type="general-purpose",
        asyncio_task=None,
        run_id="run-2",
    )

    def _factory():
        raise AssertionError("factory must not run for a displaced task")

    assert await registry.publish_writer(task_a, _factory) is None
    assert task_b.asyncio_task is None


@pytest.mark.asyncio
async def test_evicted_task_terminal_append_lands_without_touching_replacement():
    """Identity-exact append: after cancel teardown evicts A and a
    re-execution registers B under the same tool_call_id, A's terminal
    settle must reach A's own stream state — and must not pollute B's."""
    registry = BackgroundTaskRegistry(thread_id="thread-x")
    task_a = await _register_starting_task(registry, run_id="run-1")
    await registry.cancel_run_tasks("run-1")
    task_b = await registry.register(
        tool_call_id="tc-1",
        description="re-execution",
        prompt="do things",
        subagent_type="general-purpose",
        asyncio_task=None,
        run_id="run-2",
    )

    await registry.append_event_for_task(
        task_a,
        {
            "event": "steering_returned",
            "data": {"agent": f"task:{task_a.task_id}", "content": "c"},
        },
        terminal=True,
    )

    assert task_a.captured_event_seq == 1
    assert task_a.captured_event_count == 1
    assert task_b.captured_event_seq == 0


@pytest.mark.asyncio
async def test_register_refuses_to_displace_starting_entry():
    """A checkpoint re-execution during the starting window must get the
    idempotent TaskWriterLive answer, not silently replace the entry the
    pending spawn is about to publish a writer onto."""
    registry = BackgroundTaskRegistry(thread_id="thread-x")
    task_a = await _register_starting_task(registry)

    with pytest.raises(TaskWriterLive) as exc_info:
        await registry.register(
            tool_call_id="tc-1",
            description="re-execution",
            prompt="do things",
            subagent_type="general-purpose",
            asyncio_task=None,
            run_id="run-2",
        )
    assert exc_info.value.task is task_a

    # A settled entry (completed) is a dead writer — retry re-registers.
    task_a.completed = True
    replacement = await registry.register(
        tool_call_id="tc-1",
        description="retry",
        prompt="do things",
        subagent_type="general-purpose",
        asyncio_task=None,
        run_id="run-2",
    )
    assert replacement is not task_a


@pytest.mark.asyncio
async def test_cancel_all_stamps_starting_task():
    registry = BackgroundTaskRegistry(thread_id="thread-x")
    task = await _register_starting_task(registry)

    cancelled = await registry.cancel_all()

    assert cancelled == 1
    assert task.cancelled is True
    assert task.completed is True


def _pipeline_harness(order: list[str]):
    """Middleware + cancelled task wired so every terminal-pipeline step
    records its position in ``order``."""

    def _step(name: str, result=None):
        async def _fx(*_a, **_k):
            order.append(name)
            return result

        return _fx

    ledger = SimpleNamespace(
        finalize_task_run=AsyncMock(
            side_effect=_step("cas", {"applied": True, "run": {"status": "cancelled"}})
        ),
        append_run_end=AsyncMock(side_effect=_step("run_end")),
    )
    registry = MagicMock()
    registry.run_ledger = ledger
    registry.write_task_meta = AsyncMock(side_effect=_step("meta"))
    registry.append_sentinel_for_task = AsyncMock(side_effect=_step("sentinel"))
    registry.stamp_terminal_retention_for_task = AsyncMock(
        side_effect=_step("retention")
    )
    owner = SimpleNamespace(release_task_ns=AsyncMock(side_effect=_step("release")))
    middleware = BackgroundSubagentMiddleware(
        registry=registry, enabled=True, namespace_owner=owner
    )
    task = BackgroundTask(
        tool_call_id="tc-1",
        task_id="abc123",
        description="d",
        prompt="p",
        subagent_type="general-purpose",
        agent_id="general-purpose:x",
    )
    task.cancelled = True
    task.completed = True
    task.task_run_id = "tr-1"
    return middleware, ledger, task


@pytest.mark.asyncio
async def test_finalize_cancelled_before_spawn_runs_owner_pipeline():
    """The pre-spawn fallback must settle through the SAME terminal
    pipeline as an owning writer: CAS (deferred run_end) → terminal meta →
    steering sweep → run_end → sentinel → retention → fence release. An
    abbreviated finalize would drop acknowledged steering before run_end
    and leave the opener's stream unsealed."""
    order: list[str] = []
    middleware, ledger, task = _pipeline_harness(order)

    with patch(
        "ptc_agent.agent.middleware.background_subagent.middleware."
        "_return_unconsumed_steering",
        AsyncMock(side_effect=lambda *a, **k: order.append("sweep")),
    ):
        await middleware._finalize_cancelled_before_spawn(task)

    assert order == [
        "cas", "meta", "sweep", "run_end", "sentinel", "retention", "release",
    ]
    cas_kwargs = ledger.finalize_task_run.await_args.kwargs
    assert ledger.finalize_task_run.await_args.args[1] == "cancelled"
    assert cas_kwargs["failure"] is None
    assert cas_kwargs["defer_run_end"] is True
    # Identity-exact stream ops: the settle may outlive the registry entry
    # (or its tool_call_id may be reused), so sentinel/retention must take
    # the exact task object, never re-resolve by id.
    assert middleware.registry.append_sentinel_for_task.await_args.args[0] is task
    assert (
        middleware.registry.stamp_terminal_retention_for_task.await_args.args[0]
        is task
    )


@pytest.mark.asyncio
async def test_finalize_cancelled_before_spawn_suppresses_run_end_on_torn_stream():
    """A torn transport must never receive a positive run_end from the
    fallback either — the stream has an undetectable hole and resolves
    through the resync path."""
    order: list[str] = []
    middleware, _ledger, task = _pipeline_harness(order)
    task.redis_write_failed = True

    with patch(
        "ptc_agent.agent.middleware.background_subagent.middleware."
        "_return_unconsumed_steering",
        AsyncMock(side_effect=lambda *a, **k: order.append("sweep")),
    ):
        await middleware._finalize_cancelled_before_spawn(task)

    assert "run_end" not in order
    assert order == ["cas", "meta", "sweep", "sentinel", "retention", "release"]


@pytest.mark.asyncio
async def test_never_started_writer_cancel_finalizes_via_done_callback():
    """A published writer cancelled before its coroutine's first tick never
    enters _run_background_task, so its settle finally never runs — the
    done callback must finalize the run as cancelled-before-spawn.
    ``handler_task`` stays None exactly in this shape (it is assigned
    before the coroutine's first await)."""
    registry = BackgroundTaskRegistry(thread_id="thread-x")
    task = await _register_starting_task(registry)
    middleware = MagicMock()
    middleware._finalize_cancelled_before_spawn = AsyncMock()

    async def _writer_body():
        task.handler_task = MagicMock()

    writer = asyncio.get_running_loop().create_task(_writer_body())
    writer.add_done_callback(_make_task_done_callback(task, middleware))
    writer.cancel()
    try:
        await writer
    except asyncio.CancelledError:
        pass
    for _ in range(3):
        await asyncio.sleep(0)

    assert task.handler_task is None
    middleware._finalize_cancelled_before_spawn.assert_awaited_once_with(task)


@pytest.mark.asyncio
async def test_started_writer_cancel_skips_done_callback_finalization():
    """A writer that began executing settles through its own finally —
    the done-callback fallback must not double-finalize."""
    registry = BackgroundTaskRegistry(thread_id="thread-x")
    task = await _register_starting_task(registry)
    middleware = MagicMock()
    middleware._finalize_cancelled_before_spawn = AsyncMock()

    started = asyncio.Event()

    async def _writer_body():
        task.handler_task = MagicMock()
        started.set()
        await asyncio.sleep(30)

    writer = asyncio.get_running_loop().create_task(_writer_body())
    await started.wait()
    writer.add_done_callback(_make_task_done_callback(task, middleware))
    writer.cancel()
    try:
        await writer
    except asyncio.CancelledError:
        pass
    for _ in range(3):
        await asyncio.sleep(0)

    middleware._finalize_cancelled_before_spawn.assert_not_awaited()


@pytest.mark.asyncio
async def test_done_writer_pending_callback_is_not_restamped():
    """Handle present + done() + not completed = a finished writer whose
    done-callback hasn't settled it yet — it must settle as what it was,
    not be rewritten to cancelled."""
    registry = BackgroundTaskRegistry(thread_id="thread-x")
    task = await _register_starting_task(registry)

    async def _noop():
        return None

    handle = asyncio.get_running_loop().create_task(_noop())
    await handle
    task.asyncio_task = handle

    cancelled = await registry.cancel_run_tasks("run-1")

    assert cancelled == 0
    assert task.cancelled is False
    assert task.completed is False
