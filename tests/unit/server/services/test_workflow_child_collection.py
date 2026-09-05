"""Adopt-time claim of workflow-owned children.

Workflow children register while the driver runs — after the launching
turn's terminal claim sweep — so the sweep never sees the late ones. The
collector claims them when it adopts the settled run task
(``_adopt_settled_batch`` → ``_claim_owner_children``), which is what puts
them on the billing/archival path.
"""

from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from ptc_agent.agent.middleware.background_subagent.registry import (
    BackgroundTask,
    BackgroundTaskRegistry,
)
from src.server.services.runs import subagent_collection

REGISTRY_STORE_MOD = "src.server.services.background_registry_store"


def _task(
    *,
    tool_call_id: str,
    task_id: str,
    owner_task_id: str | None = None,
    subagent_type: str = "general-purpose",
) -> BackgroundTask:
    return BackgroundTask(
        tool_call_id=tool_call_id,
        task_id=task_id,
        description="d",
        prompt="p",
        subagent_type=subagent_type,
        spawned_run_id="run-1",
        owner_task_id=owner_task_id,
    )


async def _noop() -> dict:
    return {"success": True, "result": "ok"}


def _patch_registry(registry: BackgroundTaskRegistry):
    bg_store = MagicMock()
    bg_store.get_registry = AsyncMock(return_value=registry)
    return patch(
        f"{REGISTRY_STORE_MOD}.BackgroundRegistryStore.get_instance",
        return_value=bg_store,
    )


async def _adopt_parent(parent: BackgroundTask, tasks: list, pending: dict) -> bool:
    """Run one adopt round where the parent's writer just finished."""
    writer = asyncio.create_task(_noop())
    await writer
    parent.asyncio_task = writer
    pending[writer] = parent
    return await subagent_collection._adopt_settled_batch(
        {writer}, pending, "thread-A", "run-1", [], tasks=tasks
    )


class TestAdoptTimeChildClaim:

    @pytest.mark.asyncio
    async def test_late_child_claimed_when_parent_adopted(self):
        """A child registered after the sweep is claimed, added to the
        collection's task list, and its live writer joins the wait set."""
        registry = BackgroundTaskRegistry(thread_id="thread-A")
        parent = _task(tool_call_id="tc-wf", task_id="wf1", subagent_type="workflow")
        parent.collector_response_id = "run-1"
        child = _task(tool_call_id="tc-c1", task_id="c1", owner_task_id="wf1")
        child.per_call_records = [{"tokens": 1}]
        gate = asyncio.Event()

        async def _child_body() -> dict:
            await gate.wait()
            return {"success": True, "result": "child"}

        child.asyncio_task = asyncio.create_task(_child_body())
        registry._tasks["tc-wf"] = parent
        registry._tasks["tc-c1"] = child

        tasks = [parent]
        pending: dict = {}
        with _patch_registry(registry):
            ok = await _adopt_parent(parent, tasks, pending)

        assert ok is True
        assert child.collector_response_id == "run-1"
        assert child in tasks
        assert pending == {child.asyncio_task: child}
        gate.set()
        await child.asyncio_task

    @pytest.mark.asyncio
    async def test_pre_settled_parent_children_claimed_at_entry(self):
        """A parent that finished before collection start still gets its
        children claimed (entry-path mirror of the adopt-time claim)."""
        registry = BackgroundTaskRegistry(thread_id="thread-A")
        parent = _task(tool_call_id="tc-wf", task_id="wf1", subagent_type="workflow")
        parent.collector_response_id = "run-1"
        parent.terminal_status = "completed"
        child = _task(tool_call_id="tc-c1", task_id="c1", owner_task_id="wf1")
        child.per_call_records = [{"tokens": 1}]
        writer = asyncio.create_task(_noop())
        await writer
        child.asyncio_task = writer
        registry._tasks["tc-wf"] = parent
        registry._tasks["tc-c1"] = child

        tasks = [parent]
        pending: dict = {}
        with _patch_registry(registry):
            await subagent_collection._claim_settled_parents(
                "thread-A", tasks, "run-1", pending
            )

        assert child.collector_response_id == "run-1"
        assert child in tasks
        assert child.completed is True

    @pytest.mark.asyncio
    async def test_settled_child_events_replayed_in_same_batch(self):
        """A child already settled at parent-adopt time is claimed and its
        captured events replay in the same batch."""
        registry = BackgroundTaskRegistry(thread_id="thread-A")
        parent = _task(tool_call_id="tc-wf", task_id="wf1", subagent_type="workflow")
        parent.collector_response_id = "run-1"
        child = _task(tool_call_id="tc-c1", task_id="c1", owner_task_id="wf1")
        child.captured_event_count = 1
        child.asyncio_task = asyncio.create_task(_noop())
        await child.asyncio_task
        registry._tasks["tc-wf"] = parent
        registry._tasks["tc-c1"] = child

        replayed: list[str] = []

        async def _fake_replay(thread_id, task, response_id, out) -> bool:
            replayed.append(task.task_id)
            return True

        tasks = [parent]
        with _patch_registry(registry), patch.object(
            subagent_collection, "replay_owned_task_events", _fake_replay
        ):
            ok = await _adopt_parent(parent, tasks, {})

        assert ok is True
        assert child.collector_response_id == "run-1"
        assert child in tasks
        assert "c1" in replayed

    @pytest.mark.asyncio
    async def test_already_claimed_child_not_double_added(self):
        """A first-wave child the sweep already claimed is left alone."""
        registry = BackgroundTaskRegistry(thread_id="thread-A")
        parent = _task(tool_call_id="tc-wf", task_id="wf1", subagent_type="workflow")
        parent.collector_response_id = "run-1"
        child = _task(tool_call_id="tc-c1", task_id="c1", owner_task_id="wf1")
        child.collector_response_id = "run-1"
        child.per_call_records = [{"tokens": 1}]
        registry._tasks["tc-wf"] = parent
        registry._tasks["tc-c1"] = child

        tasks = [parent, child]
        with _patch_registry(registry):
            await _adopt_parent(parent, tasks, {})

        assert tasks.count(child) == 1

    @pytest.mark.asyncio
    async def test_stolen_parent_claims_nothing(self):
        """A parent stolen back by a resume adopts nothing — its children
        stay unclaimed for the new owner's collection."""
        registry = BackgroundTaskRegistry(thread_id="thread-A")
        parent = _task(tool_call_id="tc-wf", task_id="wf1", subagent_type="workflow")
        parent.collector_response_id = "run-2"  # stolen
        child = _task(tool_call_id="tc-c1", task_id="c1", owner_task_id="wf1")
        child.per_call_records = [{"tokens": 1}]
        registry._tasks["tc-wf"] = parent
        registry._tasks["tc-c1"] = child

        tasks: list = []
        with _patch_registry(registry):
            await _adopt_parent(parent, tasks, {})

        assert child.collector_response_id is None
        assert tasks == []

    @pytest.mark.asyncio
    async def test_claim_is_scoped_to_the_requested_owner(self):
        """The registry is thread-keyed and outlives the turn, so concurrent
        workflows share it — an unscoped scan would stamp one collector's
        response_id onto every other workflow's children and bill their events
        to the wrong run."""
        registry = BackgroundTaskRegistry(thread_id="thread-A")
        registry._tasks["tc-a"] = _task(
            tool_call_id="tc-a", task_id="a", owner_task_id="wf1"
        )
        registry._tasks["tc-b"] = _task(
            tool_call_id="tc-b", task_id="b", owner_task_id="wf2"
        )
        registry._tasks["tc-c"] = _task(tool_call_id="tc-c", task_id="c")

        claimed = await registry.claim_owner_children("wf1", "run-1")

        assert [t.task_id for t in claimed] == ["a"]
        assert registry._tasks["tc-b"].collector_response_id is None
        assert registry._tasks["tc-c"].collector_response_id is None
