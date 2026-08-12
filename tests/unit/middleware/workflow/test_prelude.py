from __future__ import annotations

import asyncio
from typing import Any

import pytest

from ptc_agent.agent.middleware.background_subagent.workflow.engine import (
    WorkflowHostError,
    WorkflowLimits,
    WorkflowUsageError,
    run_workflow_script,
)

from .conftest import workflow_script as _script


class PreludeHost:
    async def agent(self, prompt: str, opts: dict[str, Any]) -> Any:
        if prompt == "fail":
            raise WorkflowHostError("dispatch failed")
        if prompt == "misuse":
            raise WorkflowUsageError("Unknown agentType 'no-such-type'")
        return prompt

    def phase(self, title: str) -> None:
        pass

    def log(self, message: str) -> None:
        pass


@pytest.mark.asyncio
async def test_host_bindings_are_unreachable_after_the_prelude() -> None:
    """The raw host bindings take their arguments unchecked and skip the
    wrappers' argument validation, so the prelude captures them into closure
    scope and drops the globals. Only the wrappers stay reachable."""
    outcome = await run_workflow_script(
        _script(
            "return ["
            "  typeof globalThis.__host_agent,"
            "  typeof globalThis.__host_phase,"
            "  typeof globalThis.__host_log,"
            "  typeof agent"
            "];"
        ),
        None,
        PreludeHost(),
        WorkflowLimits(),
    )
    assert outcome.status == "completed"
    assert outcome.result == ["undefined", "undefined", "undefined", "function"]


@pytest.mark.parametrize(
    "name", ["agent", "phase", "log", "parallel", "pipeline"]
)
@pytest.mark.asyncio
async def test_prelude_helpers_cannot_be_rebound(name: str) -> None:
    """Integrity, not a security boundary: dispatch validation runs host-side in
    Python, so a script that rebinds ``agent`` reaches nothing and fools only
    itself. The freeze is what turns a stray assignment into a loud failure
    instead of a run that silently dispatches no work."""
    outcome = await run_workflow_script(
        _script(f"globalThis.{name} = () => 'shim'; return 1;"),
        None,
        PreludeHost(),
        WorkflowLimits(),
    )

    assert outcome.status == "script_error"
    assert "read-only" in (outcome.error or "")


@pytest.mark.asyncio
async def test_a_rebind_attempt_leaves_the_real_helper_in_place() -> None:
    outcome = await run_workflow_script(
        _script(
            "try { globalThis.agent = async () => 'shim'; } catch (e) {} "
            "return await agent('reached-the-host');"
        ),
        None,
        PreludeHost(),
        WorkflowLimits(),
    )

    # PreludeHost echoes the prompt, so the real host answered — not the shim.
    assert outcome.result == "reached-the-host"


@pytest.mark.asyncio
async def test_parallel_preserves_success_order() -> None:
    outcome = await run_workflow_script(
        _script("return await parallel([async () => 1, async () => 2, async () => 3]);"),
        None,
        PreludeHost(),
        WorkflowLimits(),
    )
    assert outcome.result == [1, 2, 3]


@pytest.mark.asyncio
async def test_parallel_turns_thunk_failures_into_null_slots() -> None:
    outcome = await run_workflow_script(
        _script(
            "return await parallel(["
            "async () => 'left', "
            "async () => { throw new Error('nope'); }, "
            "async () => 'right'"
            "]);"
        ),
        None,
        PreludeHost(),
        WorkflowLimits(),
    )
    assert outcome.result == ["left", None, "right"]


@pytest.mark.asyncio
async def test_parallel_absorbs_dispatch_time_agent_throw() -> None:
    outcome = await run_workflow_script(
        _script(
            "return await parallel(["
            "async () => agent('ok'), async () => agent('fail'), "
            "async () => agent('also-ok')"
            "]);"
        ),
        None,
        PreludeHost(),
        WorkflowLimits(),
    )
    assert outcome.result == ["ok", None, "also-ok"]


@pytest.mark.asyncio
async def test_parallel_awaits_already_started_promise_slots() -> None:
    """LLM authors write the idiomatic ``parallel([agent(...)])`` as readily as
    the documented thunk form — and by then the children are already dispatched,
    so the only wrong answer is the one this pins against: silently nulling
    every slot while the children succeed."""
    outcome = await run_workflow_script(
        _script(
            "return await parallel(["
            "agent('one'), async () => agent('two'), agent('three')"
            "]);"
        ),
        None,
        PreludeHost(),
        WorkflowLimits(),
    )
    assert outcome.status == "completed"
    assert outcome.result == ["one", "two", "three"]


@pytest.mark.asyncio
async def test_parallel_rejects_a_slot_that_is_neither_thunk_nor_promise() -> None:
    """The leniency above is for dispatched work, not for plain values.
    ``parallel(args.tickers)`` would otherwise resolve to the tickers having
    dispatched nothing — a run that reads like it worked."""
    outcome = await run_workflow_script(
        _script("return await parallel(['AAPL', 'MSFT']);"),
        None,
        PreludeHost(),
        WorkflowLimits(),
    )
    assert outcome.status == "script_error"
    assert "parallel slot 0" in (outcome.error or "")


@pytest.mark.asyncio
async def test_pipeline_rejects_a_stage_that_is_neither_callable_nor_promise() -> None:
    outcome = await run_workflow_script(
        _script("return await pipeline([1, 2], 'not-a-stage');"),
        None,
        PreludeHost(),
        WorkflowLimits(),
    )
    assert outcome.status == "script_error"
    assert "pipeline stage 0" in (outcome.error or "")


@pytest.mark.asyncio
async def test_parallel_nulls_a_rejected_promise_slot() -> None:
    outcome = await run_workflow_script(
        _script("return await parallel([agent('fail'), agent('ok')]);"),
        None,
        PreludeHost(),
        WorkflowLimits(),
    )
    assert outcome.result == [None, "ok"]


class LogCapturingHost(PreludeHost):
    def __init__(self) -> None:
        self.lines: list[str] = []

    def log(self, message: str) -> None:
        self.lines.append(message)


@pytest.mark.asyncio
async def test_parallel_escalates_an_engine_thrown_error() -> None:
    """A ReferenceError is the engine's verdict on broken code — every retry
    hits it again. Absorbing it to null made a broken script indistinguishable
    from a run whose children all failed; now it ends the run at the moment it
    executes, with the name and message in the terminal error."""
    outcome = await run_workflow_script(
        _script("return await parallel([async () => tickr, async () => agent('ok')]);"),
        None,
        PreludeHost(),
        WorkflowLimits(),
    )
    assert outcome.status == "script_error"
    assert "ReferenceError" in (outcome.error or "")
    assert "tickr" in (outcome.error or "")


@pytest.mark.asyncio
async def test_parallel_escalates_agent_argument_misuse() -> None:
    # agent()'s own validation throws TypeError — documented as "a call your
    # script got wrong". Inside a thunk it used to become an anonymous null.
    outcome = await run_workflow_script(
        _script("return await parallel([async () => agent(123)]);"),
        None,
        PreludeHost(),
        WorkflowLimits(),
    )
    assert outcome.status == "script_error"
    assert "prompt must be" in (outcome.error or "")


@pytest.mark.asyncio
async def test_parallel_escalates_a_dispatch_the_run_refused_as_invalid() -> None:
    """An invalid dispatch — unknown agentType, oversized prompt — fails the
    same way on every retry. Inside a thunk it used to become one more
    anonymous null; it now carries the same verdict as any other script bug."""
    outcome = await run_workflow_script(
        _script("return await parallel([async () => agent('misuse'), async () => 'ok']);"),
        None,
        PreludeHost(),
        WorkflowLimits(),
    )
    assert outcome.status == "script_error"
    assert "no-such-type" in (outcome.error or "")


@pytest.mark.asyncio
async def test_parallel_absorbs_a_reason_that_cannot_be_printed() -> None:
    # The diagnostic must never out-fail the failure it describes: a throwing
    # toString() on an absorbable reason must not become run-fatal.
    outcome = await run_workflow_script(
        _script(
            "return await parallel(["
            "async () => { throw { toString() { throw new Error('boom'); } }; }, "
            "async () => 'ok'"
            "]);"
        ),
        None,
        PreludeHost(),
        WorkflowLimits(),
    )
    assert outcome.status == "completed"
    assert outcome.result == [None, "ok"]


@pytest.mark.asyncio
async def test_escalation_reads_the_native_classes_not_the_globals() -> None:
    # A script that replaces globalThis.TypeError only fools itself: instances
    # of the impostor stay absorbable, and real engine throws still escalate.
    outcome = await run_workflow_script(
        _script(
            "globalThis.TypeError = class Fake extends Error {}; "
            "return await parallel([async () => { throw new TypeError('fake'); }, async () => 'ok']);"
        ),
        None,
        PreludeHost(),
        WorkflowLimits(),
    )
    assert outcome.status == "completed"
    assert outcome.result == [None, "ok"]


@pytest.mark.asyncio
async def test_parallel_absorbs_data_shaped_errors() -> None:
    """JSON.parse throws SyntaxError on bad *data*, not a broken script — a
    malformed child reply must cost its own slot, never the run."""
    outcome = await run_workflow_script(
        _script(
            "return await parallel([async () => JSON.parse('not json'), async () => 'ok']);"
        ),
        None,
        PreludeHost(),
        WorkflowLimits(),
    )
    assert outcome.status == "completed"
    assert outcome.result == [None, "ok"]


@pytest.mark.asyncio
async def test_parallel_notes_a_swallowed_rejection_on_the_run_log() -> None:
    host = LogCapturingHost()
    outcome = await run_workflow_script(
        _script(
            "return await parallel(["
            "async () => { throw new Error('nope'); }, async () => 'ok'"
            "]);"
        ),
        None,
        host,
        WorkflowLimits(),
    )
    assert outcome.result == [None, "ok"]
    assert any("slot 0" in line and "nope" in line for line in host.lines)


@pytest.mark.asyncio
async def test_pipeline_two_stages_receive_prev_original_and_index() -> None:
    outcome = await run_workflow_script(
        _script(
            "return await pipeline([2, 4], "
            "async (prev, original, index) => prev + original + index, "
            "async (prev, original, index) => ({ prev, original, index })"
            ");"
        ),
        None,
        PreludeHost(),
        WorkflowLimits(),
    )
    assert outcome.result == [
        {"prev": 4, "original": 2, "index": 0},
        {"prev": 9, "original": 4, "index": 1},
    ]


@pytest.mark.asyncio
async def test_pipeline_failure_is_per_item_and_skips_later_stages() -> None:
    outcome = await run_workflow_script(
        _script(
            "const later = []; "
            "const result = await pipeline([1, 2, 3], "
            "async (prev) => { if (prev === 2) throw new Error('bad'); return prev * 10; }, "
            "async (prev, original) => { later.push(original); return prev + 1; }"
            "); return { result, later };"
        ),
        None,
        PreludeHost(),
        WorkflowLimits(),
    )
    assert outcome.result == {"result": [11, None, 31], "later": [1, 3]}


@pytest.mark.asyncio
async def test_pipeline_awaits_a_non_callable_stage() -> None:
    """A promise handed where a stage belongs used to TypeError inside the
    per-item try — every item nulled at once, same silent verdict as the
    parallel() trap. Awaited instead, it feeds each item its resolved value."""
    outcome = await run_workflow_script(
        _script("return await pipeline([1, 2], agent('same'));"),
        None,
        PreludeHost(),
        WorkflowLimits(),
    )
    assert outcome.status == "completed"
    assert outcome.result == ["same", "same"]


@pytest.mark.asyncio
async def test_pipeline_escalates_an_engine_thrown_error() -> None:
    outcome = await run_workflow_script(
        _script("return await pipeline([1, 2], async (prev) => prevv + 1);"),
        None,
        PreludeHost(),
        WorkflowLimits(),
    )
    assert outcome.status == "script_error"
    assert "ReferenceError" in (outcome.error or "")


@pytest.mark.asyncio
async def test_pipeline_notes_a_swallowed_rejection_on_the_run_log() -> None:
    host = LogCapturingHost()
    outcome = await run_workflow_script(
        _script(
            "return await pipeline([1, 2], "
            "async (p) => { if (p === 2) throw new Error('bad'); return p; });"
        ),
        None,
        host,
        WorkflowLimits(),
    )
    assert outcome.result == [1, None]
    assert any("item 1" in line and "bad" in line for line in host.lines)


@pytest.mark.asyncio
async def test_pipeline_has_no_cross_item_stage_barrier() -> None:
    calls: list[str] = []

    class OrderingHost(PreludeHost):
        async def agent(self, prompt: str, opts: dict[str, Any]) -> Any:
            calls.append(prompt)
            if prompt == "s1-A":
                await asyncio.sleep(0.2)
            elif prompt == "s1-B":
                await asyncio.sleep(0.01)
            return prompt

    outcome = await run_workflow_script(
        _script(
            "return await pipeline(['A', 'B'], "
            "async (_prev, item) => agent('s1-' + item), "
            "async (_prev, item) => agent('s2-' + item)"
            ");"
        ),
        None,
        OrderingHost(),
        WorkflowLimits(),
    )
    assert outcome.status == "completed"
    assert calls.index("s2-B") < calls.index("s2-A")


