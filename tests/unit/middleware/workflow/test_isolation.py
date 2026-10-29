"""Workflow-owned children remain invisible to turn-level surfaces.

A workflow run driver dispatches children with ``owner_task_id`` set to the
run task's id. Those children are billed and awaited by the driver, so they
must NOT surface to the main turn: no completion notification, and not waited
by ``wait_for_all``. The run task itself (``subagent_type="workflow"``,
``owner_task_id is None``) stays fully visible on both of those surfaces.

Covers:
- ``BackgroundSubagentOrchestrator.check_and_get_notification`` owner fence.
- ``BackgroundTaskRegistry.wait_for_all`` owner fence.
- ``create_task_output_tool`` drill-in on an owned child by its id.
"""

from __future__ import annotations

import asyncio
from contextlib import suppress
from unittest.mock import MagicMock

import pytest

from ptc_agent.agent.middleware.background_subagent.orchestrator import (
    BackgroundSubagentOrchestrator,
)
from ptc_agent.agent.middleware.background_subagent.registry import (
    BackgroundTaskRegistry,
)
from ptc_agent.agent.middleware.background_subagent.tools import (
    create_task_output_tool,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_orchestrator(registry: BackgroundTaskRegistry) -> BackgroundSubagentOrchestrator:
    """Wrap a real registry behind a mock middleware/agent for orchestrator tests."""
    mw = MagicMock()
    mw.registry = registry
    mw.timeout = 60.0
    return BackgroundSubagentOrchestrator(agent=MagicMock(), middleware=mw)


def _make_output_tool(registry: BackgroundTaskRegistry):
    mw = MagicMock()
    mw.registry = registry
    return create_task_output_tool(mw)


async def _register_completed(
    registry: BackgroundTaskRegistry,
    *,
    tool_call_id: str,
    owner_task_id: str | None = None,
    subagent_type: str = "general-purpose",
    result: dict | None = None,
):
    """Register a task and mark it completed (no asyncio_task attached)."""
    task = await registry.register(
        tool_call_id=tool_call_id,
        description=f"desc-{tool_call_id}",
        prompt=f"prompt-{tool_call_id}",
        subagent_type=subagent_type,
        owner_task_id=owner_task_id,
    )
    task.terminal_status = "completed"
    task.result = result or {"success": True, "result": f"out-{tool_call_id}"}
    return task


async def _cancel_task(task: asyncio.Task) -> None:
    task.cancel()
    with suppress(asyncio.CancelledError):
        await task


# ---------------------------------------------------------------------------
# 1. check_and_get_notification — owner fence
# ---------------------------------------------------------------------------


class TestNotificationOwnerFence:
    @pytest.mark.asyncio
    async def test_completed_owned_child_does_not_notify(self):
        registry = BackgroundTaskRegistry()
        child = await _register_completed(
            registry, tool_call_id="child-1", owner_task_id="wf-1"
        )
        orch = _make_orchestrator(registry)

        assert await orch.check_and_get_notification() is None
        # The owned child must not be marked seen by a turn-level surface.
        assert child.result_seen is False

    @pytest.mark.asyncio
    async def test_completed_run_task_notifies(self):
        registry = BackgroundTaskRegistry()
        run = await _register_completed(
            registry,
            tool_call_id="wfrun-1",
            subagent_type="workflow",
        )
        orch = _make_orchestrator(registry)

        notification = await orch.check_and_get_notification()
        assert notification is not None
        assert run.display_id in notification
        assert run.result_seen is True

    @pytest.mark.asyncio
    async def test_mixed_set_surfaces_only_non_owned(self):
        registry = BackgroundTaskRegistry()
        owned = await _register_completed(
            registry, tool_call_id="child-1", owner_task_id="wf-1"
        )
        regular = await _register_completed(
            registry, tool_call_id="reg-1"
        )
        orch = _make_orchestrator(registry)

        notification = await orch.check_and_get_notification()
        assert notification is not None
        assert regular.display_id in notification
        assert owned.display_id not in notification
        # Only the surfaced (non-owned) task flips result_seen.
        assert regular.result_seen is True
        assert owned.result_seen is False


# ---------------------------------------------------------------------------
# 2. wait_for_all — owner fence
# ---------------------------------------------------------------------------


class TestWaitForAllOwnerFence:
    @pytest.mark.asyncio
    async def test_pending_owned_child_is_not_waited(self):
        """A pending owned child is excluded from the wait set entirely: with
        only that child pending, wait_for_all returns immediately even though
        a large timeout was requested."""
        registry = BackgroundTaskRegistry()

        async def _never():
            await asyncio.Event().wait()

        child_task = asyncio.create_task(_never())
        try:
            await registry.register(
                tool_call_id="child-1",
                description="d",
                prompt="p",
                subagent_type="general-purpose",
                owner_task_id="wf-1",
                asyncio_task=child_task,
            )

            # A 30s timeout would hang here if the owned child were waited;
            # the 2s wrapper proves it returns without blocking.
            await asyncio.wait_for(
                registry.wait_for_all(timeout=30.0), timeout=2.0
            )
            assert not child_task.done()
            assert registry.get_by_tool_call_id("child-1").terminal_status is None
        finally:
            await _cancel_task(child_task)

    @pytest.mark.asyncio
    async def test_non_owned_waited_owned_excluded(self):
        """A non-owned task IS waited and settled from its writer; a
        concurrently pending owned child is neither waited nor settled."""
        registry = BackgroundTaskRegistry()

        async def _quick():
            return {"success": True, "result": "done"}

        async def _never():
            await asyncio.Event().wait()

        non_owned_task = asyncio.create_task(_quick())
        owned_task = asyncio.create_task(_never())
        try:
            await registry.register(
                tool_call_id="reg-1",
                description="d",
                prompt="p",
                subagent_type="general-purpose",
                asyncio_task=non_owned_task,
            )
            await registry.register(
                tool_call_id="child-1",
                description="d",
                prompt="p",
                subagent_type="general-purpose",
                owner_task_id="wf-1",
                asyncio_task=owned_task,
            )

            await asyncio.wait_for(
                registry.wait_for_all(timeout=30.0), timeout=2.0
            )
            waited = registry.get_by_tool_call_id("reg-1")
            assert waited.terminal_status == "completed"
            assert waited.result == {"success": True, "result": "done"}
            assert registry.get_by_tool_call_id("child-1").completed is False
            assert not owned_task.done()
        finally:
            await _cancel_task(owned_task)
            with suppress(asyncio.CancelledError):
                await non_owned_task


# ---------------------------------------------------------------------------
# 3. TaskOutput drill-in on a workflow's own child
# ---------------------------------------------------------------------------


class TestTaskOutputOwnedChild:
    @pytest.mark.asyncio
    async def test_owned_child_still_addressable_by_task_id(self):
        """A workflow's own child is still read by its id."""
        registry = BackgroundTaskRegistry()
        run = await _register_completed(
            registry,
            tool_call_id="wfrun-1",
            subagent_type="workflow",
        )
        owned = await _register_completed(
            registry,
            tool_call_id="child-1",
            owner_task_id=run.task_id,
            result={"success": True, "result": "CHILD_OUTPUT"},
        )
        tool = _make_output_tool(registry)

        output, _ = await tool.coroutine(task_id=owned.task_id)
        assert owned.display_id in output
        assert "CHILD_OUTPUT" in output
