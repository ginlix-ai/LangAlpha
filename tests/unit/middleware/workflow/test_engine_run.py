from __future__ import annotations

import asyncio
import threading
import time
from typing import Any

import pytest
from quickjs_rs import Runtime

from ptc_agent.agent.middleware.background_subagent.workflow import engine
from ptc_agent.agent.middleware.background_subagent.workflow.engine import (
    WorkflowHostError,
    WorkflowLimits,
    run_workflow_script,
)

from .conftest import workflow_script as _script


class FakeHost:
    def __init__(self) -> None:
        self.agent_calls: list[tuple[str, dict[str, Any]]] = []
        self.phases: list[str] = []
        self.logs: list[str] = []

    async def agent(self, prompt: str, opts: dict[str, Any]) -> Any:
        self.agent_calls.append((prompt, opts))
        return {"prompt": prompt, "opts": opts}

    def phase(self, title: str) -> None:
        self.phases.append(title)

    def log(self, message: str) -> None:
        self.logs.append(message)


@pytest.mark.asyncio
async def test_plain_return_and_args_round_trip() -> None:
    outcome = await run_workflow_script(
        _script("return { value: args.value, items: [1, true, null] };"),
        {"value": "visible"},
        FakeHost(),
        WorkflowLimits(),
    )
    assert outcome.status == "completed"
    assert outcome.result == {"value": "visible", "items": [1, True, None]}


@pytest.mark.asyncio
async def test_agent_result_and_options_pass_through() -> None:
    host = FakeHost()
    outcome = await run_workflow_script(
        _script("return await agent('research', { agentType: 'deep', label: 'L' });"),
        None,
        host,
        WorkflowLimits(),
    )
    assert outcome.result == {
        "prompt": "research",
        "opts": {"agentType": "deep", "label": "L"},
    }
    assert host.agent_calls == [
        ("research", {"agentType": "deep", "label": "L"})
    ]


@pytest.mark.asyncio
async def test_host_none_is_javascript_null() -> None:
    class NoneHost(FakeHost):
        async def agent(self, prompt: str, opts: dict[str, Any]) -> Any:
            return None

    outcome = await run_workflow_script(
        _script("const value = await agent('none'); return value === null;"),
        None,
        NoneHost(),
        WorkflowLimits(),
    )
    assert outcome.result is True


@pytest.mark.asyncio
async def test_host_error_is_a_catchable_javascript_throw() -> None:
    class ErrorHost(FakeHost):
        async def agent(self, prompt: str, opts: dict[str, Any]) -> Any:
            raise WorkflowHostError("dispatch unavailable")

    outcome = await run_workflow_script(
        _script(
            "try { await agent('fail'); } "
            "catch (error) { return String(error); } "
            "return 'not caught';"
        ),
        None,
        ErrorHost(),
        WorkflowLimits(),
    )
    assert outcome.status == "completed"
    assert "dispatch unavailable" in outcome.result


@pytest.mark.asyncio
async def test_phase_and_log_wiring() -> None:
    host = FakeHost()
    outcome = await run_workflow_script(
        _script("phase('Gather'); log('hello'); return 'done';"),
        None,
        host,
        WorkflowLimits(),
    )
    assert outcome.result == "done"
    assert host.phases == ["Gather"]
    assert host.logs == ["hello"]


@pytest.mark.asyncio
async def test_phase_and_log_are_capped() -> None:
    host = FakeHost()
    outcome = await run_workflow_script(
        _script("phase('x'.repeat(600)); log('y'.repeat(600)); return true;"),
        None,
        host,
        WorkflowLimits(),
    )
    assert outcome.status == "completed"
    assert len(host.phases[0]) == 500
    assert len(host.logs[0]) == 500


@pytest.mark.asyncio
async def test_cpu_spin_maps_to_timeout() -> None:
    outcome = await run_workflow_script(
        _script("while (true) {}"),
        None,
        FakeHost(),
        WorkflowLimits(cpu_budget_s=0.1),
    )
    assert outcome.status == "cpu_timeout"
    assert "0.1s CPU budget" in (outcome.error or "")


@pytest.mark.asyncio
async def test_a_spin_after_a_host_await_is_still_a_budget_overrun() -> None:
    """The budget survives host awaits, and so must its name for the failure.

    quickjs-rs suspends its deadline while parked in a host call and re-arms
    it extended by the parked time, so the overrun IS caught — but the
    interrupt abandons the job, and the library's pending-promise check runs
    before its deadline check. Reported as a deadlock, the agent is told to go
    looking for a missing `is_async=True` that was never the problem.
    """

    class SlowHost(FakeHost):
        async def agent(self, prompt: str, opts: dict[str, Any]) -> Any:
            # Longer than the whole budget: if parked time counted, a script
            # that never spins at all would trip this.
            await asyncio.sleep(0.4)
            return "child"

    outcome = await run_workflow_script(
        _script("await agent('slow'); while (true) {}"),
        None,
        SlowHost(),
        WorkflowLimits(cpu_budget_s=0.2),
    )

    assert outcome.status == "cpu_timeout"
    assert "0.2s CPU budget" in (outcome.error or "")


@pytest.mark.asyncio
async def test_a_promise_nothing_resolves_is_still_the_scripts_own_bug() -> None:
    """The counterpart: a genuine deadlock must keep its own diagnosis."""
    outcome = await run_workflow_script(
        _script("await new Promise(() => {}); return 1;"),
        None,
        FakeHost(),
        WorkflowLimits(cpu_budget_s=5),
    )

    assert outcome.status == "script_error"
    assert "Deadlock" in (outcome.error or "")


@pytest.mark.asyncio
async def test_javascript_throw_maps_to_script_error_with_stack() -> None:
    outcome = await run_workflow_script(
        _script("throw new Error('x');"), None, FakeHost(), WorkflowLimits()
    )
    assert outcome.status == "script_error"
    assert outcome.error == "Error: x"
    assert outcome.error_stack and "eval_script" in outcome.error_stack


@pytest.mark.asyncio
async def test_unmarshalable_return_maps_to_script_error() -> None:
    outcome = await run_workflow_script(
        _script("return function nope() {};"), None, FakeHost(), WorkflowLimits()
    )
    assert outcome.status == "script_error"
    assert "JSON-serializable" in (outcome.error or "")


@pytest.mark.asyncio
async def test_undefined_return_becomes_none() -> None:
    outcome = await run_workflow_script(
        _script("const done = true;"), None, FakeHost(), WorkflowLimits()
    )
    assert outcome.status == "completed"
    assert outcome.result is None


@pytest.mark.asyncio
async def test_memory_bomb_maps_to_out_of_memory() -> None:
    outcome = await run_workflow_script(
        _script("return new Array(100_000_000).fill(1);"),
        None,
        FakeHost(),
        WorkflowLimits(memory_limit_mb=16, cpu_budget_s=3),
    )
    assert outcome.status == "out_of_memory"


@pytest.mark.asyncio
async def test_cancellation_unwinds_parked_host_coroutine() -> None:
    started = asyncio.Event()
    cancelled = asyncio.Event()

    class ParkedHost(FakeHost):
        async def agent(self, prompt: str, opts: dict[str, Any]) -> Any:
            started.set()
            try:
                await asyncio.Future()
            except asyncio.CancelledError:
                cancelled.set()
                raise

    task = asyncio.create_task(
        run_workflow_script(
            _script("return await agent('park');"),
            None,
            ParkedHost(),
            WorkflowLimits(),
        )
    )
    await asyncio.wait_for(started.wait(), timeout=5)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    await asyncio.wait_for(cancelled.wait(), timeout=5)


@pytest.mark.asyncio
async def test_a_script_cannot_swallow_a_stop_with_try_catch() -> None:
    """A user stop outranks the script. ``agent()`` rejects on child failure, so
    wrapping it in try/catch is idiomatic — and that same catch must not be able
    to turn a cancelled run into a completed one."""
    started = asyncio.Event()

    class ParkedHost(FakeHost):
        async def agent(self, prompt: str, opts: dict[str, Any]) -> Any:
            started.set()
            await asyncio.Future()

    task = asyncio.create_task(
        run_workflow_script(
            _script(
                "try { await agent('park'); }"
                " catch (e) { return { swallowed: true }; }"
                " return { swallowed: false };"
            ),
            None,
            ParkedHost(),
            WorkflowLimits(),
        )
    )
    await asyncio.wait_for(started.wait(), timeout=5)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task


@pytest.mark.asyncio
async def test_cancelling_cpu_spin_does_not_block_on_worker_join() -> None:
    task = asyncio.create_task(
        run_workflow_script(
            _script("while (true) {}"),
            None,
            FakeHost(),
            WorkflowLimits(cpu_budget_s=1),
        )
    )
    await asyncio.sleep(0.05)
    started = time.monotonic()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    # Guards against blocking on the worker join (~1s of remaining CPU
    # budget); generous headroom keeps loaded CI from flaking the proof.
    assert time.monotonic() - started < 0.8


def _worker_threads() -> set[threading.Thread]:
    return {t for t in threading.enumerate() if t.name == "workflow-quickjs"}


@pytest.mark.asyncio
async def test_cancelling_a_later_agent_call_stops_the_worker_thread() -> None:
    """A stop must end the QuickJS thread, not merely unblock the caller.

    Cancelling resumes the script's own ``catch``/``finally`` blocks while the
    engine drains pending jobs, so a script that keeps computing there holds
    the worker thread — and its CPU and heap — for the life of the process
    unless a stop can still interrupt JavaScript.
    """
    parked = asyncio.Event()

    class ParkOnSecondCallHost(FakeHost):
        async def agent(self, prompt: str, opts: dict[str, Any]) -> Any:
            if prompt == "first":
                return "done"
            parked.set()
            await asyncio.Future()

    before = _worker_threads()
    task = asyncio.create_task(
        run_workflow_script(
            _script(
                "await agent('first');"
                " try { await agent('second'); }"
                " catch (error) { while (true) {} }"
                " return 'unreachable';"
            ),
            None,
            ParkOnSecondCallHost(),
            WorkflowLimits(cpu_budget_s=0.5),
        )
    )
    await asyncio.wait_for(parked.wait(), timeout=5)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    deadline = time.monotonic() + 5
    while _worker_threads() - before and time.monotonic() < deadline:
        await asyncio.sleep(0.02)
    assert not _worker_threads() - before


# The two ways a quickjs-rs bump can break the interrupt override: the deadline
# slot it reads disappears, or the hook that installs the poll callback does.
class _RuntimeWithoutDeadlineSlot(Runtime):
    def __init__(self, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        del self._deadline


class _RuntimeWithoutInterruptHook(Runtime):
    class _NoHook:
        def __init__(self, real: Any) -> None:
            self._real = real

        def __getattr__(self, name: str) -> Any:
            if name == "set_interrupt_handler":
                raise AttributeError(name)
            return getattr(self._real, name)

    def __init__(self, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self._engine_rt = self._NoHook(self._engine_rt)


@pytest.mark.parametrize(
    "runtime_cls",
    [_RuntimeWithoutDeadlineSlot, _RuntimeWithoutInterruptHook],
    ids=["deadline-slot", "interrupt-hook"],
)
@pytest.mark.asyncio
async def test_a_renamed_quickjs_internal_costs_preemption_not_the_run(
    monkeypatch: pytest.MonkeyPatch, runtime_cls: type[Runtime]
) -> None:
    """Stop preemption reaches into quickjs-rs internals, so a dependency bump
    that renames either name has to degrade to the engine's own behaviour rather
    than fail every workflow."""
    monkeypatch.setattr(engine, "Runtime", runtime_cls)
    outcome = await run_workflow_script(
        _script("return await agent('probe');"), None, FakeHost(), WorkflowLimits()
    )
    assert outcome.status == "completed"
