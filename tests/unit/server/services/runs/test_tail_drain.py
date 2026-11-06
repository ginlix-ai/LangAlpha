"""The guard's tail drain waits on a run's surviving subagent writers for as
long as their own clocks run, and stops one still alive past every clock
instead of leaving it running on a discarded session."""

from __future__ import annotations

import asyncio
import time
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from langchain_core.messages import ToolMessage

from ptc_agent.agent.middleware.background_subagent import run_executor, tail_drain
from ptc_agent.agent.middleware.background_subagent.registry import (
    TIME_LIMIT_CANCEL,
    BackgroundTaskRegistry,
)
from src.server.services.runs.executor import LocalRunExecutor
from src.utils.tracking.per_call_token_tracker import PerCallTokenTracker

REGISTRY_STORE = (
    "src.server.services.background_registry_store.BackgroundRegistryStore.get_instance"
)


def _guard(*task_ids: str) -> SimpleNamespace:
    return SimpleNamespace(owns_task_ns=lambda task_id: task_id in task_ids)


async def _register(registry: BackgroundTaskRegistry, writer):
    return await registry.register(
        tool_call_id=f"tc-{id(writer)}",
        description="d",
        prompt="p",
        subagent_type="research",
        asyncio_task=writer,
    )


@pytest.fixture
def clocks(monkeypatch):
    """Every writer clock 0.2s long, no slack past it, a short unwind."""
    monkeypatch.setattr("src.config.settings.get_workflow_timeout", lambda: 0.2)
    monkeypatch.setattr(
        "src.config.settings.get_workflow_orchestration_config",
        lambda: SimpleNamespace(run_timeout=0.1),
    )
    monkeypatch.setattr(tail_drain, "TAIL_DRAIN_SLACK_S", 0.0)
    monkeypatch.setattr(
        "ptc_agent.agent.middleware.background_subagent.tail_drain.CANCEL_UNWIND_TIMEOUT",
        0.2,
    )
    monkeypatch.setattr(
        "ptc_agent.agent.middleware.background_subagent.tail_drain.SUBAGENT_CLOSE_TIMEOUT_S",
        0.0,
    )


async def _drain(registry, guard):
    store = MagicMock()
    store.get_registry = AsyncMock(return_value=registry)
    guard_clock = asyncio.timeout(5)
    with patch(REGISTRY_STORE, return_value=store):
        try:
            async with guard_clock:
                await LocalRunExecutor._drain_run_subagent_writers(
                    MagicMock(), "thread-1", "run-1", guard
                )
        except TimeoutError:
            if guard_clock.expired():
                pytest.fail("the drain did not return")
            raise


@pytest.mark.asyncio
async def test_a_writer_inside_its_clock_finishes_untouched(clocks):
    registry = BackgroundTaskRegistry()
    writer = asyncio.create_task(asyncio.sleep(0.05, result="done"))
    task = await _register(registry, writer)

    await _drain(registry, _guard(task.task_id))

    assert writer.result() == "done"
    assert task.terminal_status is None


@pytest.mark.asyncio
async def test_a_writer_past_every_clock_settles_as_a_timeout_that_reports_back(
    clocks,
):
    class _Ledger:
        def __init__(self) -> None:
            self.statuses: list[str] = []
            self.failures: list[dict | None] = []

        async def finalize_task_run(self, task_run_id, status, **kwargs):
            self.statuses.append(status)
            self.failures.append(kwargs.get("failure"))
            return {"applied": True, "run": {"status": status}}

        async def append_run_end(self, task_run_id, **kwargs):
            pass

    registry = BackgroundTaskRegistry()
    ledger = registry.run_ledger = _Ledger()
    task = await _register(registry, None)
    task.task_run_id = "run-1"

    async def stuck_in_a_tool(_request):
        await asyncio.Event().wait()

    writer = asyncio.create_task(
        run_executor._run_background_task(
            task, stuck_in_a_tool, None, PerCallTokenTracker(), "Subagent",
            registry=registry,
        )
    )
    task.asyncio_task = writer
    await asyncio.sleep(0)

    await _drain(registry, _guard(task.task_id))
    task.adopt_writer_outcome(writer)

    # An error to the ledger, so the report-back fires: a cancel would not,
    # and nobody chose this stop.
    assert ledger.statuses == ["error"]
    assert ledger.failures[0]["error_type"] == "SubagentTimeLimitError"
    assert task.terminal_status == "timeout"


@pytest.mark.asyncio
async def test_a_writer_that_will_not_unwind_fails_the_drain(clocks):
    registry = BackgroundTaskRegistry()

    async def stubborn() -> None:
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            await asyncio.sleep(1)

    writer = asyncio.create_task(stubborn())
    task = await _register(registry, writer)

    with pytest.raises(TimeoutError):
        await _drain(registry, _guard(task.task_id))
    writer.cancel()


@pytest.mark.asyncio
async def test_a_settled_task_still_unwinding_gets_the_unwind_bound_only(
    clocks, monkeypatch
):
    monkeypatch.setattr("src.config.settings.get_workflow_timeout", lambda: 600)
    monkeypatch.setattr(tail_drain, "SETTLED_UNWIND_S", 0.1)
    registry = BackgroundTaskRegistry()
    hung = asyncio.create_task(asyncio.Event().wait())
    settled = await _register(registry, hung)
    settled.terminal_status = "cancelled"
    working = asyncio.create_task(asyncio.sleep(0.5, result="done"))
    running = await _register(registry, working)

    await _drain(registry, _guard(settled.task_id, running.task_id))

    assert hung.cancelled()
    assert working.result() == "done"


@pytest.mark.asyncio
async def test_a_task_that_settles_mid_drain_gets_the_unwind_bound_from_then(
    clocks, monkeypatch
):
    monkeypatch.setattr("src.config.settings.get_workflow_timeout", lambda: 600)
    monkeypatch.setattr(tail_drain, "SETTLED_UNWIND_S", 0.3)
    loop = asyncio.get_running_loop()
    stopped_at: list[float] = []

    async def unwinding() -> None:
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            stopped_at.append(loop.time())
            raise

    registry = BackgroundTaskRegistry()
    stopping = await _register(registry, asyncio.create_task(unwinding()))
    waker = await _register(registry, asyncio.create_task(asyncio.sleep(0.55)))
    working = asyncio.create_task(asyncio.sleep(1.2, result="done"))
    running = await _register(registry, working)

    async def stop_later() -> float:
        await asyncio.sleep(0.5)
        stopping.terminal_status = "cancelled"
        stopping.last_updated_at = time.time()
        return loop.time()

    stop = asyncio.create_task(stop_later())
    await _drain(
        registry, _guard(stopping.task_id, waker.task_id, running.task_id)
    )

    assert stopped_at[0] - stop.result() >= 0.25
    assert working.result() == "done"


@pytest.mark.asyncio
async def test_the_unwind_bound_runs_from_the_settle_not_from_when_it_is_seen(
    clocks, monkeypatch
):
    monkeypatch.setattr("src.config.settings.get_workflow_timeout", lambda: 600)
    monkeypatch.setattr(tail_drain, "SETTLED_UNWIND_S", 0.4)
    loop = asyncio.get_running_loop()
    messages: list = []
    registry = BackgroundTaskRegistry()
    stopping = await _register(registry, asyncio.create_task(_recording_cancels(messages)))
    working = asyncio.create_task(asyncio.sleep(1.5, result="done"))
    running = await _register(registry, working)

    async def stop_later() -> float:
        await asyncio.sleep(0.1)
        stopping.terminal_status = "cancelled"
        stopping.last_updated_at = time.time()
        return loop.time()

    stop = asyncio.create_task(stop_later())
    drain = asyncio.create_task(_drain(registry, _guard(stopping.task_id, running.task_id)))
    while not messages:
        await asyncio.sleep(0.01)

    # Seen at the 0.4s look, but due 0.4s after the settle itself.
    assert loop.time() - stop.result() < 0.6
    await drain
    assert working.result() == "done"


@pytest.mark.asyncio
async def test_a_stuck_settled_writer_fails_the_drain_only_once_the_others_finish(
    clocks, monkeypatch
):
    monkeypatch.setattr("src.config.settings.get_workflow_timeout", lambda: 600)
    monkeypatch.setattr(tail_drain, "SETTLED_UNWIND_S", 0.1)

    async def stubborn() -> None:
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            await asyncio.sleep(3)

    registry = BackgroundTaskRegistry()
    hung = asyncio.create_task(stubborn())
    settled = await _register(registry, hung)
    settled.terminal_status = "cancelled"
    working = asyncio.create_task(asyncio.sleep(0.8, result="done"))
    running = await _register(registry, working)

    with pytest.raises(TimeoutError):
        await _drain(registry, _guard(settled.task_id, running.task_id))

    # Discarding the session sooner would fail a writer inside its clock.
    assert working.done()
    hung.cancel()


@pytest.mark.asyncio
async def test_a_stuck_writer_that_finishes_late_does_not_fail_the_drain(
    clocks, monkeypatch
):
    monkeypatch.setattr("src.config.settings.get_workflow_timeout", lambda: 600)
    monkeypatch.setattr(tail_drain, "SETTLED_UNWIND_S", 0.1)

    async def slow_to_unwind() -> None:
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            await asyncio.sleep(0.4)

    registry = BackgroundTaskRegistry()
    slow = asyncio.create_task(slow_to_unwind())
    settled = await _register(registry, slow)
    settled.terminal_status = "cancelled"
    working = asyncio.create_task(asyncio.sleep(1.0, result="done"))
    running = await _register(registry, working)

    await _drain(registry, _guard(settled.task_id, running.task_id))

    assert slow.done()
    assert working.result() == "done"


@pytest.mark.asyncio
async def test_a_writer_dropped_from_its_task_does_not_spin_the_drain(
    clocks, monkeypatch
):
    monkeypatch.setattr("src.config.settings.get_workflow_timeout", lambda: 600)
    monkeypatch.setattr(tail_drain, "SETTLED_UNWIND_S", 0.1)
    registry = BackgroundTaskRegistry()
    dropped = asyncio.create_task(asyncio.Event().wait())
    settled = await _register(registry, dropped)
    settled.terminal_status = "cancelled"
    working = asyncio.create_task(asyncio.sleep(0.5, result="done"))
    running = await _register(registry, working)

    reads = 0
    live_writers_by_task = registry.live_writers_by_task

    async def counted(predicate):
        nonlocal reads
        reads += 1
        assert reads < 200, "the drain is spinning"
        return await live_writers_by_task(predicate)

    registry.live_writers_by_task = counted

    async def drop_later() -> None:
        await asyncio.sleep(0.05)
        settled.asyncio_task = None

    dropper = asyncio.create_task(drop_later())
    await _drain(registry, _guard(settled.task_id, running.task_id))

    assert working.result() == "done"
    await dropper
    dropped.cancel()


async def _recording_cancels(messages: list) -> None:
    try:
        await asyncio.Event().wait()
    except asyncio.CancelledError as cancel:
        messages.append(cancel.args)
        raise


@pytest.mark.asyncio
async def test_a_writer_is_stopped_past_its_own_clock(clocks, monkeypatch):
    monkeypatch.setattr("src.config.settings.get_workflow_timeout", lambda: 600)
    registry = BackgroundTaskRegistry()
    messages: list = []
    task = await _register(registry, asyncio.create_task(_recording_cancels(messages)))
    task.stop_at = time.time() + 0.1

    await _drain(registry, _guard(task.task_id))

    assert messages == [(TIME_LIMIT_CANCEL,)]


@pytest.mark.asyncio
async def test_a_writer_already_past_its_own_clock_is_stopped_at_its_slack(
    clocks, monkeypatch
):
    monkeypatch.setattr("src.config.settings.get_workflow_timeout", lambda: 600)
    monkeypatch.setattr(tail_drain, "TAIL_DRAIN_SLACK_S", 0.3)
    registry = BackgroundTaskRegistry()
    messages: list = []
    task = await _register(registry, asyncio.create_task(_recording_cancels(messages)))
    task.stop_at = time.time() - 0.1

    await _drain(registry, _guard(task.task_id))

    assert messages == [(TIME_LIMIT_CANCEL,)]


@pytest.mark.asyncio
async def test_a_task_already_stopped_keeps_its_outcome(clocks, monkeypatch):
    monkeypatch.setattr("src.config.settings.get_workflow_timeout", lambda: 600)
    monkeypatch.setattr(tail_drain, "SETTLED_UNWIND_S", 0.1)
    registry = BackgroundTaskRegistry()
    messages: list = []
    task = await _register(registry, asyncio.create_task(_recording_cancels(messages)))
    task.terminal_status = "cancelled"

    await _drain(registry, _guard(task.task_id))

    assert messages == [()]


@pytest.mark.asyncio
async def test_a_straggler_holds_the_session_until_it_stops(clocks, monkeypatch):
    monkeypatch.setattr("src.config.settings.get_workflow_timeout", lambda: 600)
    monkeypatch.setattr(tail_drain, "SETTLED_UNWIND_S", 0.1)
    registry = BackgroundTaskRegistry()
    task = await _register(registry, None)
    task.terminal_status = "error"
    straggler = asyncio.create_task(asyncio.Event().wait())
    task.keep_straggler(straggler)

    await _drain(registry, _guard(task.task_id))

    assert straggler.cancelled()
    assert task.live_writers == []


@pytest.mark.asyncio
async def test_a_task_left_for_the_drain_stays_registered_while_a_straggler_writes():
    registry = BackgroundTaskRegistry()
    writer = asyncio.create_task(asyncio.sleep(0))
    task = await _register(registry, writer)
    await writer
    task.terminal_status = "cancelled"
    release = asyncio.Event()
    task.keep_straggler(asyncio.create_task(release.wait()))

    registry._remove_when_settled(task.tool_call_id, task)
    await asyncio.sleep(0.05)
    assert task.tool_call_id in registry._tasks

    release.set()
    await asyncio.sleep(0.05)
    assert task.tool_call_id not in registry._tasks


@pytest.mark.asyncio
async def test_a_straggler_outliving_its_handler_holds_the_fence_and_the_bill():
    from src.tools.decorators import get_tool_tracker

    registry = BackgroundTaskRegistry()
    task = await _register(registry, None)
    released: list[str] = []
    owner = SimpleNamespace(release_task_ns=AsyncMock(side_effect=released.append))
    release = asyncio.Event()

    async def late_tool_call() -> None:
        await release.wait()
        get_tool_tracker().record_usage("web_fetch")

    async def handler(_request):
        task.keep_straggler(asyncio.create_task(late_tool_call()))
        return ToolMessage(content="ok", tool_call_id="tc", name="Task")

    settle = AsyncMock()
    with patch.object(run_executor, "_settle_terminal_run", settle):
        writer = asyncio.create_task(
            run_executor._run_background_task(
                task, handler, object(), PerCallTokenTracker(), "test",
                namespace_owner=owner,
            )
        )
        await asyncio.sleep(0.05)
        settle.assert_awaited_once()
        assert not writer.done() and released == [] and task.tool_usage == {}

        release.set()
        await writer

    assert released == [task.task_id]
    assert task.tool_usage == {"web_fetch": 1}


@pytest.mark.asyncio
async def test_a_stop_during_the_straggler_wait_keeps_the_settled_result():
    registry = BackgroundTaskRegistry()
    task = await _register(registry, None)
    released: list[str] = []
    owner = SimpleNamespace(release_task_ns=AsyncMock(side_effect=released.append))
    straggler = asyncio.create_task(asyncio.Event().wait())

    async def handler(_request):
        task.keep_straggler(straggler)
        return ToolMessage(content="ok", tool_call_id="tc", name="Task")

    with patch.object(run_executor, "_settle_terminal_run", AsyncMock()):
        writer = asyncio.create_task(
            run_executor._run_background_task(
                task, handler, object(), PerCallTokenTracker(), "test",
                namespace_owner=owner,
            )
        )
        await asyncio.sleep(0.05)
        writer.cancel(TIME_LIMIT_CANCEL)
        result = await writer

    assert result["success"] is True
    assert released == []
    straggler.cancel()


@pytest.mark.asyncio
async def test_with_no_subagent_clock_the_drain_waits_the_writer_out(
    clocks, monkeypatch
):
    monkeypatch.setattr("src.config.settings.get_workflow_timeout", lambda: 0)
    registry = BackgroundTaskRegistry()
    writer = asyncio.create_task(asyncio.sleep(0.5, result="done"))
    task = await _register(registry, writer)

    await _drain(registry, _guard(task.task_id))

    assert writer.result() == "done"


@pytest.mark.asyncio
async def test_another_runs_writer_is_left_alone(clocks):
    registry = BackgroundTaskRegistry()
    foreign = asyncio.create_task(asyncio.Event().wait())
    task = await _register(registry, foreign)

    await _drain(registry, _guard())

    assert not foreign.done()
    assert task.terminal_status is None
    foreign.cancel()
