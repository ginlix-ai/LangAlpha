"""A process drains when asked, and only when asked of it, and says so on
/health in the words the caller that restarts it reads.

The drain stops this process's subagents and lets them settle before it
cancels its turns, since a turn's own stop would otherwise end them as the
user's, then records the threads whose subagents it stopped.
"""

import asyncio
import os
import re
import time
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest

import ptc_agent.core.mcp_registry as mcp_registry
from src.server.app.utilities import health_check
from src.server.services.runs import restart_drain

# The patterns that caller greps the /health body with.
_CAPABLE = re.compile(r'"capabilities"\s*:\s*\[[^\]]*"restart_resume_v1"')
_DRAINING = re.compile(r'"draining"\s*:\s*true')


@pytest.fixture
def drain_file(tmp_path, monkeypatch):
    path = tmp_path / "langalpha-drain"
    monkeypatch.setattr(restart_drain, "DRAIN_FILE", str(path))
    monkeypatch.setattr(restart_drain, "_STARTED_AT", time.time() - 1)
    monkeypatch.setattr(restart_drain, "_draining", False)
    monkeypatch.setattr(restart_drain, "_drain_task", None)
    return path


def test_no_drain_request_is_no_drain(drain_file):
    assert not restart_drain.draining()


def test_a_drain_request_made_after_the_process_started_drains_it(drain_file):
    drain_file.touch()
    assert restart_drain.draining()


def test_a_drain_request_left_for_an_earlier_process_is_ignored(drain_file):
    drain_file.touch()
    stale = restart_drain._STARTED_AT - 60
    os.utime(drain_file, (stale, stale))
    assert not restart_drain.draining()


@pytest.mark.asyncio
async def test_health_advertises_the_capability(drain_file, monkeypatch):
    monkeypatch.setattr(mcp_registry, "_GLOBAL_REGISTRY", None)

    result = await health_check()

    assert result["capabilities"] == ["restart_resume_v1"]
    assert result["draining"] is False
    assert result["status"] == "healthy"


@pytest.mark.asyncio
async def test_a_draining_process_answers_503_with_draining_true(drain_file, monkeypatch):
    monkeypatch.setattr(mcp_registry, "_GLOBAL_REGISTRY", None)
    drain_file.touch()

    response = await health_check()

    assert response.status_code == 503
    body = response.body.decode()
    assert _CAPABLE.search(body)
    assert _DRAINING.search(body)


@pytest.mark.asyncio
async def test_the_drain_settles_subagents_before_it_cancels_turns(drain_file):
    order: list[str] = []

    async def settling_writer():
        await asyncio.sleep(0.05)
        order.append("subagent settled")

    writer = asyncio.create_task(settling_writer())
    store = SimpleNamespace(
        stop_all_for_restart=AsyncMock(return_value=([writer], {"thread-9"}))
    )

    async def cancel(thread_id, run_id, **kwargs):
        order.append(f"cancel {run_id}")
        assert kwargs == {"user_initiated": False, "restart": True}

    executor = SimpleNamespace(
        uncancelled_runs=AsyncMock(return_value=[("thread-1", "run-1")]),
        signal_cancel=cancel,
    )

    async def enqueue(thread_id):
        order.append(f"record {thread_id}")

    with (
        patch(
            "src.server.services.background_registry_store.BackgroundRegistryStore.get_instance",
            return_value=store,
        ),
        patch(
            "src.server.services.runs.executor.LocalRunExecutor.get_instance",
            return_value=executor,
        ),
        patch(
            "src.server.database.runs.restart_resume.enqueue_for_stopped_tasks",
            enqueue,
        ),
        patch.object(restart_drain, "_stop_turn_starters", AsyncMock()) as starters,
    ):
        first = restart_drain.begin("test")
        assert restart_drain.begin("again") is first
        await first

    assert order == ["subagent settled", "cancel run-1", "record thread-9"]
    starters.assert_awaited_once()
    assert restart_drain.draining()


@pytest.mark.asyncio
async def test_a_failed_record_does_not_stop_the_drain(drain_file):
    store = SimpleNamespace(
        stop_all_for_restart=AsyncMock(return_value=([], {"thread-8", "thread-9"}))
    )
    executor = SimpleNamespace(
        uncancelled_runs=AsyncMock(return_value=[]), signal_cancel=AsyncMock()
    )
    enqueue = AsyncMock(side_effect=[ConnectionError("db down"), None])
    with (
        patch(
            "src.server.services.background_registry_store.BackgroundRegistryStore.get_instance",
            return_value=store,
        ),
        patch(
            "src.server.services.runs.executor.LocalRunExecutor.get_instance",
            return_value=executor,
        ),
        patch("src.server.database.runs.restart_resume.enqueue_for_stopped_tasks", enqueue),
        patch.object(restart_drain, "_stop_turn_starters", AsyncMock()),
    ):
        await restart_drain.begin("test")

    assert enqueue.await_count == 2
