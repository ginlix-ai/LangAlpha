"""Locks for the user-targeted single-task cancel (``registry.cancel_task``).

``POST /threads/{tid}/tasks/{task_id}/cancel`` resolves to this method on the
worker owning the live writer. ``force=True`` is the load-bearing detail: the
run wrapper shields its handler, so a wrapper-only cancel is absorbed and the
task would run on to completion.
"""

from __future__ import annotations

import asyncio

import pytest

from ptc_agent.agent.middleware.background_subagent.registry import (
    BackgroundTaskRegistry,
)


async def _register(registry: BackgroundTaskRegistry, tool_call_id: str = "tc-1"):
    return await registry.register(
        tool_call_id=tool_call_id,
        description="task",
        prompt="p",
        subagent_type="general-purpose",
        asyncio_task=None,
        run_id="run-1",
    )


@pytest.mark.asyncio
async def test_cancel_task_cancels_only_the_matching_live_task():
    registry = BackgroundTaskRegistry(thread_id="thread-x")
    target = await _register(registry, "tc-1")
    other = await _register(registry, "tc-2")

    assert await registry.cancel_task(target.task_id) is True

    assert target.cancelled is True
    assert target.completed is True
    assert other.cancelled is False


@pytest.mark.asyncio
async def test_cancel_task_unknown_or_settled_returns_false():
    registry = BackgroundTaskRegistry(thread_id="thread-x")
    task = await _register(registry)
    task.terminal_status = "completed"

    assert await registry.cancel_task("zzzzzz") is False
    assert await registry.cancel_task(task.task_id) is False
    assert task.cancelled is False


@pytest.mark.asyncio
async def test_force_cancel_reaches_the_shielded_handler():
    registry = BackgroundTaskRegistry(thread_id="thread-x")
    task = await _register(registry)
    task.asyncio_task = asyncio.create_task(asyncio.Event().wait())
    task.handler_task = asyncio.create_task(asyncio.Event().wait())

    assert await registry.cancel_task(task.task_id, force=True) is True

    for handle in (task.asyncio_task, task.handler_task):
        with pytest.raises(asyncio.CancelledError):
            await handle


@pytest.mark.asyncio
async def test_wait_for_specific_reports_a_stop_instead_of_cancelling_its_waiter():
    """A stopped writer's ``.result()`` re-raises CancelledError, and that is a
    BaseException: escaping it cancels the tool node awaiting the task, which
    langgraph turns into a node cancellation and the whole turn dies."""
    registry = BackgroundTaskRegistry(thread_id="thread-x")
    task = await _register(registry)
    task.asyncio_task = asyncio.create_task(asyncio.Event().wait())
    task.asyncio_task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task.asyncio_task
    # The waiter was already past the completed-guard when the stop landed —
    # that is the only ordering in which the collect block sees a cancellation.
    assert task.completed is False

    result = await registry.wait_for_specific(task.task_id, timeout=5)

    assert result["success"] is False
    assert result["status"] == "cancelled"
    # The entry carries the same verdict: a later non-blocking TaskOutput reads
    # task.result, and an unset one there reads as an empty success.
    assert task.result is result
    assert task.cancelled is True


@pytest.mark.asyncio
async def test_wait_for_all_reports_a_stop_instead_of_cancelling_its_waiter():
    registry = BackgroundTaskRegistry(thread_id="thread-x")
    task = await _register(registry)
    task.asyncio_task = asyncio.create_task(asyncio.Event().wait())
    task.asyncio_task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task.asyncio_task

    results = await registry.wait_for_all(timeout=5)

    assert results[task.tool_call_id]["status"] == "cancelled"


@pytest.mark.asyncio
async def test_completed_means_settled_not_succeeded():
    """``completed`` answers "will this ever produce a result", which is why a
    stop has to flip it — but three agent- and user-facing call sites used to
    read it as success and reported stopped work as finished."""
    registry = BackgroundTaskRegistry(thread_id="thread-x")
    task = await _register(registry)

    task.mark_cancelled()

    assert task.completed is True
    assert task.is_pending is False
    assert task.terminal_status == "cancelled"


@pytest.mark.asyncio
async def test_a_writer_that_returns_a_failure_settles_as_error():
    """A delivered failure is not a cancellation and not a success: the
    outcome comes from the payload, not from the writer merely returning."""
    registry = BackgroundTaskRegistry(thread_id="thread-x")
    task = await _register(registry)

    async def _fail() -> dict:
        return {"success": False, "error": "subagent gave up"}

    writer = asyncio.create_task(_fail())
    await writer

    assert task.adopt_writer_outcome(writer)["error"] == "subagent gave up"
    assert task.terminal_status == "error"
    assert task.cancelled is False


@pytest.mark.asyncio
async def test_cancel_owner_children_is_scoped_and_forceful():
    """Owner-scoped teardown reaches the owner's live children (handler
    included, since nothing is left to report to) and nothing else."""
    registry = BackgroundTaskRegistry(thread_id="thread-x")
    child = await _register(registry, "tc-child")
    child.owner_task_id = "owner-1"
    child.asyncio_task = asyncio.create_task(asyncio.Event().wait())
    handler = asyncio.create_task(asyncio.Event().wait())
    child.handler_task = handler
    stranger = await _register(registry, "tc-stranger")
    stranger.owner_task_id = "owner-2"

    assert await registry.cancel_owner_children("owner-1", reason="Run failed") == 1

    with pytest.raises(asyncio.CancelledError):
        await child.asyncio_task
    assert child.cancelled is True
    assert child.error == "Run failed"
    assert handler.cancelled() or handler.cancelling()
    assert stranger.completed is False


@pytest.mark.asyncio
async def test_soft_cancel_leaves_the_handler_running():
    registry = BackgroundTaskRegistry(thread_id="thread-x")
    task = await _register(registry)
    task.asyncio_task = asyncio.create_task(asyncio.Event().wait())
    handler = asyncio.create_task(asyncio.Event().wait())
    task.handler_task = handler

    assert await registry.cancel_task(task.task_id) is True

    with pytest.raises(asyncio.CancelledError):
        await task.asyncio_task
    assert not handler.done()
    handler.cancel()
