"""Steering handed back to the user is kept on the run's replay facts.

Replay reads a run's transcript from its checkpoint, where returned steering
never lands, so each sweep records what it returned on the run's ledger row.
"""

from __future__ import annotations

import json
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from langchain_core.messages import AIMessage, HumanMessage

from ptc_agent.agent.middleware.background_subagent.middleware import (
    current_background_tool_call_id,
)
from ptc_agent.agent.middleware.background_subagent.redis_stream import (
    record_returned,
)
from ptc_agent.agent.middleware.background_subagent.registry import BackgroundTask
from ptc_agent.agent.middleware.background_subagent.run_executor import (
    _return_unconsumed_steering,
)
from ptc_agent.agent.middleware.background_subagent.steering import (
    SubagentSteeringMiddleware,
)
from tests.unit.server.handlers.chat.redis_fakes import FakeCache


def _task(run_id: str | None = "run-9") -> BackgroundTask:
    return BackgroundTask(
        tool_call_id="tc-9",
        task_id="abc123",
        description="d",
        prompt="p",
        subagent_type="general-purpose",
        task_run_id=run_id,
    )


_RETURNED = [{"content": "c", "input_id": "i1", "reason": "run_ended"}]


@pytest.mark.asyncio
async def test_record_writes_the_returns_to_the_runs_row():
    ledger = SimpleNamespace(record_replay_facts=AsyncMock(return_value=True))

    assert await record_returned(
        SimpleNamespace(run_ledger=ledger), "run-9", _RETURNED
    )

    ledger.record_replay_facts.assert_awaited_once_with(
        "run-9", steering_returned=_RETURNED
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "outcome",
    [{"side_effect": RuntimeError("db down")}, {"return_value": False}],
    ids=["raises", "no_row"],
)
async def test_record_reports_a_fact_that_did_not_land_without_raising(outcome):
    ledger = SimpleNamespace(record_replay_facts=AsyncMock(**outcome))

    assert not await record_returned(
        SimpleNamespace(run_ledger=ledger), "run-9", _RETURNED
    )


@pytest.mark.asyncio
async def test_record_skips_when_nothing_was_returned_or_no_run():
    ledger = SimpleNamespace(record_replay_facts=AsyncMock())
    registry = SimpleNamespace(run_ledger=ledger)

    # Nothing is owed a row, so nothing holds a caller's queue back.
    assert await record_returned(registry, "run-9", [])
    assert await record_returned(registry, None, _RETURNED)
    assert await record_returned(SimpleNamespace(), "run-9", _RETURNED)

    ledger.record_replay_facts.assert_not_awaited()


def _sweep(task, ledger):
    """A run-end sweep over one queued entry whose append lands."""
    payload = json.dumps(
        {"content": "late note", "expected_task_run_id": "run-9", "input_id": "i1"}
    )

    def _surface(*_a, **_k):
        task.captured_event_seq += 1

    client = SimpleNamespace(lrange=AsyncMock(return_value=[payload]), lrem=AsyncMock())
    registry = SimpleNamespace(
        append_event_for_task=AsyncMock(side_effect=_surface), run_ledger=ledger
    )
    return client, registry


@pytest.mark.asyncio
async def test_run_end_sweep_records_what_it_returns():
    task = _task("run-9")
    ledger = SimpleNamespace(record_replay_facts=AsyncMock(return_value=True))
    client, registry = _sweep(task, ledger)
    cache = SimpleNamespace(enabled=True, client=client)

    with patch("src.utils.cache.redis_cache.get_cache_client", return_value=cache):
        await _return_unconsumed_steering(registry, task)

    returned = {"content": "late note", "input_id": "i1", "reason": "run_ended"}
    ledger.record_replay_facts.assert_awaited_once_with(
        "run-9", steering_returned=[returned]
    )
    # The event names the return exactly as the fact does, plus its lane.
    event = registry.append_event_for_task.await_args.args[1]
    assert event["event"] == "steering_returned"
    assert event["data"] == {"agent": "task:abc123", **returned}
    client.lrem.assert_awaited_once()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "outcome",
    [{"side_effect": RuntimeError("db down")}, {"return_value": False}],
    ids=["raises", "no_row"],
)
async def test_run_end_sweep_keeps_the_queue_when_the_returns_are_not_recorded(
    outcome,
):
    """Replay reads returned steering from the run's row, so a queue entry
    whose return never landed there stays in Redis until its TTL."""
    task = _task("run-9")
    ledger = SimpleNamespace(record_replay_facts=AsyncMock(**outcome))
    client, registry = _sweep(task, ledger)
    cache = SimpleNamespace(enabled=True, client=client)

    with patch("src.utils.cache.redis_cache.get_cache_client", return_value=cache):
        await _return_unconsumed_steering(registry, task)

    ledger.record_replay_facts.assert_awaited_once()
    client.lrem.assert_not_awaited()


@pytest.mark.asyncio
async def test_run_mismatch_return_records_after_the_last_message():
    call = "call-1"
    foreign = json.dumps(
        {"content": "stale", "expected_task_run_id": "run-OLD", "input_id": "i2"}
    )
    cache = FakeCache()
    from ptc_agent.agent.middleware.background_subagent.redis_stream import (
        steering_queue_key,
    )

    cache.client.lists[steering_queue_key(call, "run-A")] = [foreign]
    ledger = SimpleNamespace(record_replay_facts=AsyncMock())
    registry = SimpleNamespace(
        _tasks={call: SimpleNamespace(task_id="k7Xm2p", task_run_id="run-A")},
        append_captured_event=AsyncMock(),
        run_ledger=ledger,
    )
    state = {"messages": [HumanMessage("hi", id="m1"), AIMessage("ok", id="m2")]}

    token = current_background_tool_call_id.set(call)
    try:
        with patch("src.utils.cache.redis_cache.get_cache_client", return_value=cache):
            await SubagentSteeringMiddleware(registry).abefore_model(state, MagicMock())
    finally:
        current_background_tool_call_id.reset(token)

    returned = {"content": "stale", "input_id": "i2", "reason": "run_mismatch"}
    ledger.record_replay_facts.assert_awaited_once_with(
        "run-A", steering_returned=[{**returned, "after": "m2"}]
    )
    # ``after`` is the fact's alone; the event places itself by stream order.
    _, event = registry.append_captured_event.await_args.args
    assert event["event"] == "steering_returned"
    assert event["data"] == {"agent": "task:k7Xm2p", **returned}


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("row", "landed"),
    [({"thread_id": "t-1", "parent_run_id": "resp-1"}, True), (None, False)],
)
async def test_the_ledger_reports_whether_the_returns_landed(row, landed):
    from src.server.services import subagent_run_coordinator as coordinator

    refresh = MagicMock()
    with (
        patch.object(
            coordinator.facts_db,
            "append_returned_steering",
            AsyncMock(return_value=row),
        ),
        patch(
            "src.server.services.history.replay.refresh.schedule_replay_refresh",
            refresh,
        ),
    ):
        ok = await coordinator.SubagentRunCoordinator("t-1").record_replay_facts(
            "run-9", steering_returned=_RETURNED
        )

    assert ok is landed
    assert refresh.call_args_list == ([(("t-1", "resp-1"),)] if landed else [])


@pytest.mark.asyncio
async def test_a_ledger_write_that_fails_reports_not_landed():
    from src.server.services import subagent_run_coordinator as coordinator

    with patch.object(
        coordinator.facts_db,
        "append_returned_steering",
        AsyncMock(side_effect=RuntimeError("pool closed")),
    ):
        ok = await coordinator.SubagentRunCoordinator("t-1").record_replay_facts(
            "run-9", steering_returned=_RETURNED
        )

    assert ok is False
