"""Tests for the workflow_handler cancel path (signal-only + safety net).

Covers decision 1A/1C:
- cancel_workflow is signal-only when a task is active (the except-handler
  teardown owns cancel_and_clear).
- cancel_workflow runs the safety-net cancel_and_clear only when NO active
  task exists (orphaned registry after a crash).
"""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest


def _patch_common(*, manager_cancel_returns: bool):
    """Patch the collaborators of workflow_handler.cancel_workflow.

    Returns a context manager and the mocked registry_store so the test can
    assert on cancel_and_clear.
    """
    tracker = MagicMock()
    tracker.set_cancel_flag = AsyncMock(return_value=True)
    tracker.mark_cancelled = AsyncMock(return_value=True)

    manager = MagicMock()
    manager.cancel_workflow = AsyncMock(return_value=manager_cancel_returns)

    registry_store = MagicMock()
    registry_store.cancel_and_clear = AsyncMock(return_value=0)

    patches = [
        patch(
            "src.server.services.workflow_tracker.WorkflowTracker.get_instance",
            return_value=tracker,
        ),
        patch(
            "src.server.services.background_task_manager.BackgroundTaskManager.get_instance",
            return_value=manager,
        ),
        patch(
            "src.server.services.background_registry_store.BackgroundRegistryStore.get_instance",
            return_value=registry_store,
        ),
        patch(
            "src.server.database.conversation.update_thread_status",
            new=AsyncMock(),
        ),
    ]
    return patches, registry_store, manager


@pytest.mark.asyncio
async def test_cancel_with_active_task_is_signal_only():
    """When a task is active (manager.cancel_workflow → True), the handler must
    NOT call cancel_and_clear — the except-handler teardown owns it."""
    from src.server.handlers.workflow_handler import cancel_workflow

    patches, registry_store, _manager = _patch_common(manager_cancel_returns=True)
    for p in patches:
        p.start()
    try:
        result = await cancel_workflow("t-1")
    finally:
        for p in patches:
            p.stop()

    assert result["cancelled"] is True
    registry_store.cancel_and_clear.assert_not_awaited()


@pytest.mark.asyncio
async def test_cancel_with_no_active_task_runs_safety_net():
    """No active task (manager.cancel_workflow → False) ⇒ the safety-net
    cancel_and_clear runs to wipe any orphaned registry."""
    from src.server.handlers.workflow_handler import cancel_workflow

    patches, registry_store, _manager = _patch_common(manager_cancel_returns=False)
    for p in patches:
        p.start()
    try:
        result = await cancel_workflow("t-1")
    finally:
        for p in patches:
            p.stop()

    assert result["cancelled"] is True
    registry_store.cancel_and_clear.assert_awaited_once_with("t-1", force=True)
