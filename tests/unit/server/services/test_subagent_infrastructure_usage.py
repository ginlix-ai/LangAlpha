"""Subagent infrastructure (tool) usage persistence + cleanup.

_persist_subagent_usage must bill a task's infrastructure usage on its
``msg_type='task'`` row even when the task made no platform LLM calls
(per_call_records empty), and the post-turn cleanup must clear the snapshot
so a reused task object can't be re-billed.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from ptc_agent.agent.middleware.background_subagent.registry import BackgroundTask
from src.server.services.background_task_manager import BackgroundTaskManager

USAGE_MODULE = "src.server.services.persistence.usage"


def _make_btm() -> BackgroundTaskManager:
    with patch("src.server.services.background_task_manager.get_max_concurrent_workflows", return_value=10), \
         patch("src.server.services.background_task_manager.get_workflow_result_ttl", return_value=3600), \
         patch("src.server.services.background_task_manager.get_abandoned_workflow_timeout", return_value=3600), \
         patch("src.server.services.background_task_manager.get_cleanup_interval", return_value=60), \
         patch("src.server.services.background_task_manager.is_intermediate_storage_enabled", return_value=False), \
         patch("src.server.services.background_task_manager.get_max_stored_messages_per_agent", return_value=1000), \
         patch("src.server.services.background_task_manager.get_event_storage_backend", return_value="memory"), \
         patch("src.server.services.background_task_manager.get_redis_ttl_workflow_events", return_value=86400):
        return BackgroundTaskManager()


def _make_task(task_id: str = "task01", **kwargs) -> BackgroundTask:
    defaults = dict(
        tool_call_id=f"tc-{task_id}",
        task_id=task_id,
        description="d",
        prompt="p",
        subagent_type="general-purpose",
        agent_id=f"general-purpose:{task_id}",
    )
    defaults.update(kwargs)
    return BackgroundTask(**defaults)


@pytest.mark.asyncio
async def test_persists_infra_usage_with_empty_per_call_records():
    """A task with only tool_usage (no token records) is still persisted, and
    its tool usage is forwarded to the usage service for infra billing."""
    btm = _make_btm()
    task = _make_task(
        per_call_records=[],
        tool_usage={"TavilySearchTool:deep": 2},
        collector_response_id="resp-1",
    )

    fake_service = MagicMock()
    fake_service.track_llm_usage = AsyncMock()
    fake_service.record_tool_usage_batch = MagicMock()
    fake_service.persist_usage = AsyncMock()
    fake_service._token_usage = None

    with patch(f"{USAGE_MODULE}.UsagePersistenceService", return_value=fake_service):
        await btm._persist_subagent_usage(
            response_id="resp-1",
            tasks=[task],
            thread_id="thread-1",
            workspace_id="ws-1",
            user_id="user-1",
        )

    fake_service.record_tool_usage_batch.assert_called_once_with(
        {"TavilySearchTool:deep": 2}
    )
    fake_service.persist_usage.assert_awaited_once()
    assert fake_service.persist_usage.await_args.kwargs["msg_type"] == "task"


@pytest.mark.asyncio
async def test_tool_only_rows_carry_task_identity():
    """A tool-only task (no LLM records) still gets task_id/agent_id/
    subagent_type stamped onto token_usage: the real track_llm_usage([])
    leaves a zeroed dict, not None, so the stamp must run."""
    from src.server.services.persistence.usage import UsagePersistenceService

    btm = _make_btm()
    task = _make_task(
        per_call_records=[],
        tool_usage={"TavilySearchTool:deep": 2},
        collector_response_id="resp-1",
    )

    service = UsagePersistenceService(
        thread_id="thread-1", workspace_id="ws-1", user_id="user-1"
    )
    service.persist_usage = AsyncMock()

    with patch(f"{USAGE_MODULE}.UsagePersistenceService", return_value=service):
        await btm._persist_subagent_usage(
            response_id="resp-1",
            tasks=[task],
            thread_id="thread-1",
            workspace_id="ws-1",
            user_id="user-1",
        )

    service.persist_usage.assert_awaited_once()
    assert service._token_usage is not None
    assert service._token_usage["task_id"] == "task01"
    assert service._token_usage["agent_id"] == "general-purpose:task01"
    assert service._token_usage["subagent_type"] == "general-purpose"


@pytest.mark.asyncio
async def test_no_tool_batch_when_tool_usage_empty():
    """When a task has token records but no tool usage, the infra batch call
    is skipped (no spurious empty infrastructure rows)."""
    btm = _make_btm()
    task = _make_task(
        per_call_records=[{"dummy": "record"}],
        tool_usage={},
        collector_response_id="resp-1",
    )

    fake_service = MagicMock()
    fake_service.track_llm_usage = AsyncMock()
    fake_service.record_tool_usage_batch = MagicMock()
    fake_service.persist_usage = AsyncMock()
    fake_service._token_usage = {"by_model": {}}

    with patch(f"{USAGE_MODULE}.UsagePersistenceService", return_value=fake_service):
        await btm._persist_subagent_usage(
            response_id="resp-1",
            tasks=[task],
            thread_id="thread-1",
            workspace_id="ws-1",
            user_id="user-1",
        )

    fake_service.record_tool_usage_batch.assert_not_called()
    fake_service.persist_usage.assert_awaited_once()


@pytest.mark.asyncio
async def test_task_with_no_records_and_no_tool_usage_skipped():
    """A task with neither token records nor tool usage produces no row."""
    btm = _make_btm()
    task = _make_task(per_call_records=[], tool_usage={}, collector_response_id="resp-1")

    fake_service = MagicMock()
    fake_service.track_llm_usage = AsyncMock()
    fake_service.persist_usage = AsyncMock()

    with patch(f"{USAGE_MODULE}.UsagePersistenceService", return_value=fake_service):
        await btm._persist_subagent_usage(
            response_id="resp-1",
            tasks=[task],
            thread_id="thread-1",
            workspace_id="ws-1",
            user_id="user-1",
        )

    fake_service.persist_usage.assert_not_awaited()


@pytest.mark.asyncio
async def test_cleanup_clears_tool_usage():
    """_await_drain_and_cleanup_tasks resets tool_usage so a reused task
    object can't be billed twice."""
    btm = _make_btm()
    task = _make_task(
        tool_usage={"TavilySearchTool:deep": 1}, collector_response_id="resp-1"
    )
    task.sse_drain_complete.set()

    with patch.object(btm, "_await_drain_and_cleanup_tasks", wraps=btm._await_drain_and_cleanup_tasks), \
         patch("src.server.services.background_task_manager.get_sse_drain_timeout", return_value=0.1), \
         patch("src.server.services.background_task_manager.get_cache_client", side_effect=Exception("no cache")), \
         patch(
             "src.server.services.background_registry_store.BackgroundRegistryStore.get_instance"
         ) as mock_store:
        mock_store.return_value.get_registry = AsyncMock(return_value=None)
        await btm._await_drain_and_cleanup_tasks(
            [task], thread_id="thread-1", response_id="resp-1"
        )

    assert task.tool_usage == {}
    assert task.per_call_records == []


# ---------------------------------------------------------------------------
# Resume-window: ownership-gated snapshot-and-clear (exactly-once)
# ---------------------------------------------------------------------------


def _make_registry():
    from ptc_agent.agent.middleware.background_subagent.registry import (
        BackgroundTaskRegistry,
    )

    return BackgroundTaskRegistry(thread_id="thread-1")


async def _persist_with_registry(btm, registry, task, response_id="resp-1"):
    fake_service = MagicMock()
    fake_service.track_llm_usage = AsyncMock()
    fake_service.record_tool_usage_batch = MagicMock()
    fake_service.persist_usage = AsyncMock()
    fake_service._token_usage = None

    with patch(f"{USAGE_MODULE}.UsagePersistenceService", return_value=fake_service), \
         patch(
             "src.server.services.background_registry_store.BackgroundRegistryStore.get_instance"
         ) as mock_store:
        mock_store.return_value.get_registry = AsyncMock(return_value=registry)
        await btm._persist_subagent_usage(
            response_id=response_id,
            tasks=[task],
            thread_id="thread-1",
            workspace_id="ws-1",
            user_id="user-1",
        )
    return fake_service


@pytest.mark.asyncio
async def test_owner_persists_merged_usage_then_clears():
    """The collector that still owns the task bills the merged run-1+run-2
    usage exactly once and clears it from the task."""
    btm = _make_btm()
    registry = _make_registry()
    task = _make_task(
        per_call_records=[{"run": 1}, {"run": 2}],
        tool_usage={"TavilySearchTool:deep": 3},
    )
    task.collector_response_id = "resp-1"

    fake_service = await _persist_with_registry(btm, registry, task)

    fake_service.record_tool_usage_batch.assert_called_once_with(
        {"TavilySearchTool:deep": 3}
    )
    fake_service.persist_usage.assert_awaited_once()
    # Usage cleared after a successful persist so a later collector can't re-bill.
    assert task.per_call_records == []
    assert task.tool_usage == {}


@pytest.mark.asyncio
async def test_stale_collector_skips_after_resume_released_ownership():
    """A resume cleared collector_response_id; the stale turn-N collector
    (still holding response_id=resp-1) must NOT persist — usage is left intact
    for whichever collector next owns the task."""
    btm = _make_btm()
    registry = _make_registry()
    task = _make_task(
        per_call_records=[{"run": 1}],
        tool_usage={"TavilySearchTool:deep": 1},
    )
    # Resume reset → ownership cleared.
    task.collector_response_id = None

    fake_service = await _persist_with_registry(btm, registry, task, response_id="resp-1")

    fake_service.persist_usage.assert_not_awaited()
    # Run-1 usage preserved for the next owner.
    assert task.per_call_records == [{"run": 1}]
    assert task.tool_usage == {"TavilySearchTool:deep": 1}


@pytest.mark.asyncio
async def test_no_double_persist_across_two_collectors():
    """Two collectors (turn-N stale + turn-N+1) both reference the same task.
    Only the current owner persists; the other skips. Exactly-once."""
    btm = _make_btm()
    registry = _make_registry()
    task = _make_task(
        per_call_records=[{"run": 1}],
        tool_usage={"TavilySearchTool:deep": 1},
    )
    # Turn-N+1's collector currently owns it.
    task.collector_response_id = "resp-2"

    # Stale turn-N collector (resp-1) runs first: must skip.
    stale_service = await _persist_with_registry(btm, registry, task, response_id="resp-1")
    stale_service.persist_usage.assert_not_awaited()
    assert task.tool_usage == {"TavilySearchTool:deep": 1}

    # Current owner (resp-2) runs: persists once and clears.
    owner_service = await _persist_with_registry(btm, registry, task, response_id="resp-2")
    owner_service.persist_usage.assert_awaited_once()
    assert task.tool_usage == {}
    assert task.per_call_records == []


@pytest.mark.asyncio
async def test_no_registry_fallback_gates_on_ownership():
    """When the registry is gone (thread teardown), the fallback path applies
    the same ownership gate — a stale collector must not claim usage owned by
    another response."""
    btm = _make_btm()
    task = _make_task(
        per_call_records=[{"run": 1}],
        tool_usage={"TavilySearchTool:deep": 1},
        collector_response_id="resp-2",
    )

    fake_service = MagicMock()
    fake_service.track_llm_usage = AsyncMock()
    fake_service.record_tool_usage_batch = MagicMock()
    fake_service.persist_usage = AsyncMock()
    fake_service._token_usage = None

    with patch(f"{USAGE_MODULE}.UsagePersistenceService", return_value=fake_service):
        await btm._persist_subagent_usage(
            response_id="resp-1",
            tasks=[task],
            thread_id="thread-1",
            workspace_id="ws-1",
            user_id="user-1",
        )

    fake_service.persist_usage.assert_not_awaited()
    # Usage preserved for the actual owner.
    assert task.tool_usage == {"TavilySearchTool:deep": 1}
