from __future__ import annotations

import asyncio
import json
import types
from dataclasses import dataclass
from typing import Any

import pytest
from langchain_core.messages import ToolMessage
from langgraph.types import Command

from ptc_agent.agent.middleware.background_subagent.registry import (
    BackgroundTaskRegistry,
)
from ptc_agent.agent.middleware.background_subagent.workflow.driver import (
    WorkflowDriver,
    WorkflowRunError,
    WorkflowRunSpec,
)
from ptc_agent.agent.middleware.background_subagent.workflow.emitter import (
    WorkflowEmitter,
)
from ptc_agent.agent.middleware.background_subagent.workflow.engine import compile_check
from ptc_agent.agent.middleware.background_subagent.workflow.ui_snapshot import (
    read_task_result,
)
from src.config.models import WorkflowOrchestrationConfig

from .conftest import FakeBackend, workflow_script


def _script(body: str) -> str:
    return workflow_script(body, name="driver-test", description="driver test")


def _caps(**overrides: Any) -> types.SimpleNamespace:
    """Real field set, deliberately without the real bounds.

    Timeouts here run far under the model's ``ge=1`` floor so the suite stays
    fast, which rules out constructing the model — but the keys are still
    checked, or a renamed field would silently become an unread attribute.
    """
    values = WorkflowOrchestrationConfig().model_dump()
    unknown = overrides.keys() - values.keys()
    assert not unknown, f"not WorkflowOrchestrationConfig fields: {sorted(unknown)}"
    values.update(overrides)
    return types.SimpleNamespace(**values)


@dataclass
class Immediate:
    result: Any
    usage: list[dict[str, Any]] | None = None


def _as_writer_result(result: Any) -> Any:
    """Restate a fixture's plain-string payload in the shape a writer returns.

    ``run_subagent_graph`` hands back a ``Command`` carrying a ``ToolMessage``,
    so a bare string exercises ``extract_result_content``'s ``str(inner)``
    fallback instead of the unwrapping branch every real dispatch takes.
    """
    inner = result.get("result") if isinstance(result, dict) else None
    if not isinstance(inner, str):
        return result
    message = ToolMessage(content=inner, tool_call_id="child")
    return {**result, "result": Command(update={"messages": [message]})}


@dataclass
class Hang:
    started: asyncio.Event


@dataclass
class HangThenBill:
    """A child whose usage exists only once its cancellation has been delivered.

    ``_merge_subagent_usage`` runs in the real writer's settle ``finally``, so
    a timed-out child's records are never populated at the instant
    ``force_cancel`` returns — a ``Hang`` that bills up front cannot show it.
    """

    started: asyncio.Event
    usage: list[dict[str, Any]]


class RecordingRegistry(BackgroundTaskRegistry):
    """Records what actually lands, by standing in for the Redis spill seam
    rather than the append itself: the real append seals a cancelled task's
    streams, and a fake that skips that check shows the driver's settle frames
    delivered in tests while they vanish on a live stop."""

    def __init__(self) -> None:
        super().__init__(thread_id="thread-1")
        self.events: list[dict[str, Any]] = []

    async def _spill_record_to_redis(
        self, task: Any, record: dict[str, Any]
    ) -> None:
        self.events.append(record["data"])


class FakeDispatcher:
    def __init__(
        self,
        registry: RecordingRegistry,
        behaviors: list[Immediate | Hang] | None = None,
        *,
        subagent_types: list[str] | None = None,
    ) -> None:
        self.registry = registry
        self.behaviors = list(behaviors or [])
        self.subagent_types = subagent_types or ["general-purpose", "research"]
        self.calls: list[dict[str, Any]] = []
        self.tasks: list[Any] = []

    async def dispatch(self, **kwargs: Any) -> Any:
        index = len(self.calls)
        self.calls.append(kwargs)
        behavior = (
            self.behaviors[index]
            if index < len(self.behaviors)
            else Immediate({"success": True, "result": f"child-{index}"})
        )
        task = await self.registry.register(
            tool_call_id=f"child-{index}",
            description=kwargs["description"],
            prompt=kwargs["prompt"],
            subagent_type=kwargs["subagent_type"],
            run_id=kwargs["run_id"],
            owner_task_id=kwargs["owner_task_id"],
        )
        self.tasks.append(task)
        if isinstance(behavior, HangThenBill):

            async def _hang_then_bill() -> Any:
                behavior.started.set()
                try:
                    await asyncio.Event().wait()
                finally:
                    task.per_call_records = list(behavior.usage)

            task.asyncio_task = asyncio.create_task(_hang_then_bill())
            task.handler_task = asyncio.create_task(asyncio.Event().wait())
        elif isinstance(behavior, Hang):

            async def _hang() -> Any:
                behavior.started.set()
                await asyncio.Event().wait()

            task.asyncio_task = asyncio.create_task(_hang())
            task.handler_task = asyncio.create_task(asyncio.Event().wait())
        else:
            task.per_call_records = list(behavior.usage or [])

            async def _finish() -> Any:
                return _as_writer_result(behavior.result)

            task.asyncio_task = asyncio.create_task(_finish())
        return task


async def _make_driver(
    script: str,
    *,
    behaviors: list[Immediate | Hang] | None = None,
    caps: Any | None = None,
    subagent_types: list[str] | None = None,
    source: str = "inline",
    checkpointer: Any = None,
) -> tuple[WorkflowDriver, RecordingRegistry, FakeDispatcher, FakeBackend]:
    registry = RecordingRegistry()
    run_task = await registry.register(
        tool_call_id="workflow-run",
        description="driver test",
        prompt=script,
        subagent_type="workflow",
        run_id="launch-1",
    )
    dispatcher = FakeDispatcher(
        registry, behaviors, subagent_types=subagent_types
    )
    backend = FakeBackend()
    spec = WorkflowRunSpec(
        run_task=run_task,
        registry=registry,
        dispatcher=dispatcher,
        backend=backend,
        checkpointer=checkpointer,
        thread_id="thread-1",
        short_thread_id="short-1",
        script=script,
        meta=compile_check(script),
        source=source,
        base_configurable={"user_id": "user-1"},
        caps=caps or _caps(),
    )
    return WorkflowDriver(spec), registry, dispatcher, backend


async def _ns_ui_records(saver: Any, task_id: str) -> list[dict[str, Any]]:
    """Raw read of the ui channel at the task namespace's latest checkpoint."""
    tup = await saver.aget_tuple(
        {
            "configurable": {
                "thread_id": "thread-1",
                "checkpoint_ns": f"task:{task_id}",
            }
        }
    )
    if tup is None:
        return []
    return list(tup.checkpoint.get("channel_values", {}).get("ui") or [])


@pytest.mark.asyncio
async def test_parallel_fanout_dispatches_collector_owned_children() -> None:
    """Children carry the launching turn's run stamp and the owner mark, and
    the driver leaves collection/billing entirely to the turn collector: no
    collector_response_id stamping, per_call_records intact."""
    usage = [{"usage": {"input_tokens": 3, "output_tokens": 2}}]
    driver, _, dispatcher, backend = await _make_driver(
        _script(
            "const values = await parallel([() => agent('a'), () => agent('b')]); "
            "return values.join('+');"
        ),
        behaviors=[
            Immediate({"success": True, "result": "A"}, usage),
            Immediate({"success": True, "result": "B"}, usage),
        ],
    )

    result = await driver.run()

    assert "2 subagent dispatch(es)" in result
    assert json.loads(backend.writes[f"{driver.base_rel}/result.json"]) == "A+B"
    assert all(task.owner_task_id == driver.wf_task_id for task in dispatcher.tasks)
    assert all(call["run_id"] == "launch-1" for call in dispatcher.calls)
    assert all(task.collector_response_id is None for task in dispatcher.tasks)
    assert all(task.per_call_records == usage for task in dispatcher.tasks)


@pytest.mark.asyncio
async def test_token_accrual_is_read_only() -> None:
    """Run token totals accrue from per_call_records WITHOUT clearing them —
    the turn collector later snapshots-and-clears when it bills."""
    records = [{"usage": {"total_tokens": 17}}]
    driver, registry, dispatcher, _ = await _make_driver(
        _script("return await agent('count');"),
        behaviors=[Immediate({"success": True, "result": "ok"}, records)],
    )

    await driver.run()

    done = next(e for e in registry.events if e["phase"] == "child_done")
    assert done["tokens_used"] == 17
    assert done["tokens_spent"] == 17
    assert dispatcher.tasks[0].per_call_records == records


@pytest.mark.asyncio
async def test_a_timed_out_child_still_reports_the_tokens_it_spent() -> None:
    """A timeout must not zero a child's usage on the run card.

    ``force_cancel`` only schedules the cancellation, and accrual caches the
    first number it reads — so reading before the writer's settle pins that
    child at zero for the life of the run.
    """
    started = asyncio.Event()
    driver, registry, dispatcher, _ = await _make_driver(
        _script("await agent('slow'); return 'done';"),
        behaviors=[HangThenBill(started, [{"usage": {"total_tokens": 4096}}])],
        caps=_caps(child_timeout=0.05),
    )

    await driver.run()

    done = next(e for e in registry.events if e["phase"] == "child_done")
    assert done["status"] == "timeout"
    assert done["tokens_used"] == 4096
    assert done["tokens_spent"] == 4096
    assert registry.events[-1]["tokens_spent"] == 4096


@pytest.mark.asyncio
async def test_a_bookkeeping_failure_is_scrubbed_before_it_becomes_the_result() -> None:
    """The terminal error reaches the ledger, the task result and TaskOutput.

    Its two siblings — the child record and the lifecycle frame — already
    scrub and clip; an infrastructure exception arriving here raw is the one
    delivery that does not.
    """
    driver, _, _, _ = await _make_driver(_script("return 'ok';"))
    leak = (
        "connection failed: postgresql://svc:hunter2@db.internal:5432/app "
        "authorization: Bearer abcd1234efgh5678 " + "x" * 8000
    )

    async def _explode(*_args: Any, **_kwargs: Any) -> Any:
        raise RuntimeError(leak)

    driver._finish_outcome = _explode  # type: ignore[method-assign]

    with pytest.raises(WorkflowRunError) as caught:
        await driver.run()

    delivered = str(caught.value)
    assert "hunter2" not in delivered
    assert "abcd1234efgh5678" not in delivered
    assert "Bearer [REDACTED]" in delivered
    assert len(delivered.encode()) <= 4000 + len("\n... [truncated]")


@pytest.mark.asyncio
async def test_schema_success_returns_parsed_value() -> None:
    driver, _, _, backend = await _make_driver(
        _script(
            "return await agent('json', { schema: { type: 'object', "
            "properties: { answer: { type: 'string' } }, required: ['answer'] } });"
        ),
        behaviors=[
            Immediate({"success": True, "result": "```json\n{\"answer\":\"yes\"}\n```"})
        ],
    )

    await driver.run()

    assert json.loads(backend.writes[f"{driver.base_rel}/result.json"]) == {
        "answer": "yes"
    }
    record = json.loads(backend.writes[f"{driver.base_rel}/children/000.json"])
    assert record["schema_valid"] is True


@pytest.mark.asyncio
async def test_schema_failure_retries_once_then_succeeds() -> None:
    driver, _, dispatcher, backend = await _make_driver(
        _script(
            "return await agent('json', { schema: { type: 'object', "
            "properties: { value: { type: 'number' } }, required: ['value'] } });"
        ),
        behaviors=[
            Immediate({"success": True, "result": "not json"}),
            Immediate({"success": True, "result": "{\"value\": 7}"}),
        ],
    )

    await driver.run()

    assert len(dispatcher.calls) == 2
    assert "previous response failed validation" in dispatcher.calls[1]["prompt"]
    assert json.loads(backend.writes[f"{driver.base_rel}/result.json"]) == {"value": 7}
    first = json.loads(backend.writes[f"{driver.base_rel}/children/000.json"])
    second = json.loads(backend.writes[f"{driver.base_rel}/children/001.json"])
    assert first["schema_valid"] is False
    assert second["schema_valid"] is True


@pytest.mark.asyncio
async def test_schema_double_failure_returns_null_and_reports_invalid_schema() -> None:
    """The script sees ``null``; the panel sees ``invalid_schema``. The status
    is the only signal distinguishing a child that answered unusably from one
    that failed outright, and it is what the run card colours on."""
    driver, registry, dispatcher, backend = await _make_driver(
        _script("return await agent('json', { schema: { type: 'number' } });"),
        behaviors=[
            Immediate({"success": True, "result": "wrong"}),
            Immediate({"success": True, "result": "still wrong"}),
        ],
    )

    await driver.run()

    assert len(dispatcher.calls) == 2
    assert json.loads(backend.writes[f"{driver.base_rel}/result.json"]) is None
    done = [e for e in registry.events if e["phase"] == "child_done"]
    assert done[-1]["status"] == "invalid_schema"


@pytest.mark.asyncio
async def test_schema_retry_is_skipped_when_dispatch_cap_is_reached() -> None:
    driver, _, dispatcher, backend = await _make_driver(
        _script("return await agent('json', { schema: { type: 'object' } });"),
        behaviors=[Immediate({"success": True, "result": "not json"})],
        caps=_caps(max_dispatches_per_run=1),
    )

    await driver.run()

    assert len(dispatcher.calls) == 1
    assert json.loads(backend.writes[f"{driver.base_rel}/result.json"]) is None


@pytest.mark.asyncio
async def test_child_timeout_returns_null_and_force_cancels() -> None:
    started = asyncio.Event()
    driver, _, dispatcher, backend = await _make_driver(
        _script("return await agent('slow');"),
        behaviors=[Hang(started)],
        caps=_caps(child_timeout=0.02),
    )

    await driver.run()

    assert started.is_set()
    assert dispatcher.tasks[0].cancelled is True
    assert dispatcher.tasks[0].result["status"] == "timeout"
    assert json.loads(backend.writes[f"{driver.base_rel}/result.json"]) is None


@pytest.mark.asyncio
async def test_fanout_width_is_bounded_by_max_concurrent_children() -> None:
    """The semaphore is the only thing bounding fan-out width. With every slot
    held by a child that never answers, no further dispatch can happen however
    many turns the loop takes — so this needs no timing assumption."""
    started = [asyncio.Event() for _ in range(5)]
    driver, _, dispatcher, _ = await _make_driver(
        _script("return await parallel([0,1,2,3,4].map(i => () => agent('a' + i)));"),
        behaviors=[Hang(event) for event in started],
        caps=_caps(max_concurrent_children=2),
    )

    run = asyncio.create_task(driver.run())
    await asyncio.wait_for(
        asyncio.gather(started[0].wait(), started[1].wait()), timeout=2
    )
    for _ in range(50):
        await asyncio.sleep(0)

    assert len(dispatcher.calls) == 2
    run.cancel()
    with pytest.raises(asyncio.CancelledError):
        await run


@pytest.mark.asyncio
async def test_a_timed_out_child_frees_its_slot() -> None:
    """``asyncio.shield`` leaves a timed-out child's coroutine running, so the
    slot has to come back on the timeout path — otherwise the first full width
    of slow children wedges every one behind them."""
    driver, registry, dispatcher, _ = await _make_driver(
        _script("return await parallel([0,1,2].map(i => () => agent('a' + i)));"),
        behaviors=[Hang(asyncio.Event()) for _ in range(3)],
        caps=_caps(max_concurrent_children=1, child_timeout=0.02),
    )

    await driver.run()

    assert len(dispatcher.calls) == 3
    done = [e["status"] for e in registry.events if e["phase"] == "child_done"]
    assert done == ["timeout", "timeout", "timeout"]


@pytest.mark.asyncio
async def test_dispatch_cap_is_js_catchable() -> None:
    """A plain Error, not a TypeError: spending the cap is a run reaching its
    limit, not a bug in the script, so it stays absorbable."""
    driver, _, dispatcher, backend = await _make_driver(
        _script(
            "await agent('first'); "
            "try { await agent('second'); } "
            "catch (e) { return e.constructor.name + ' | ' + e.message; }"
        ),
        caps=_caps(max_dispatches_per_run=1),
    )

    await driver.run()

    result = json.loads(backend.writes[f"{driver.base_rel}/result.json"])
    assert result.startswith("Error | ")
    assert "max_dispatches_per_run=1" in result
    assert len(dispatcher.calls) == 1


@pytest.mark.asyncio
async def test_a_run_at_its_cap_reports_the_cap_not_the_other_problem() -> None:
    """The cap is checked ahead of shape validation, so a spent run says so
    even when the call is also malformed."""
    driver, _, dispatcher, backend = await _make_driver(
        _script(
            "await agent('first'); "
            "try { await agent('second', {agentType: 'nope'}); } "
            "catch (e) { return String(e); }"
        ),
        caps=_caps(max_dispatches_per_run=1),
    )

    await driver.run()

    result = json.loads(backend.writes[f"{driver.base_rel}/result.json"])
    assert "max_dispatches_per_run=1" in result
    assert "agentType" not in result
    assert len(dispatcher.calls) == 1


@pytest.mark.asyncio
async def test_a_rejected_dispatch_does_not_spend_a_slot() -> None:
    """The increment follows validation, so a call that never dispatched leaves
    the budget where it was."""
    driver, _, dispatcher, _ = await _make_driver(
        _script(
            "try { await agent('bad', {agentType: 'nope'}); } catch (e) {} "
            "await agent('real'); return 'done';"
        ),
        caps=_caps(max_dispatches_per_run=1),
    )

    await driver.run()

    assert len(dispatcher.calls) == 1
    assert dispatcher.calls[0]["prompt"] == "real"


@pytest.mark.asyncio
async def test_dispatch_cap_holds_with_every_call_in_flight_at_once() -> None:
    """The cap is exact only because ``_cap_reached`` and the increment after
    it have no await between them. Every child hangs, so all eight calls are
    live before any resolves — an admission that read a stale count would show
    up here as a ninth dispatch."""
    started = [asyncio.Event() for _ in range(8)]
    driver, _, dispatcher, _ = await _make_driver(
        _script(
            "return await parallel([0,1,2,3,4,5,6,7].map(i => () => "
            "agent('a' + i).catch(() => 'refused')));"
        ),
        behaviors=[Hang(event) for event in started],
        caps=_caps(max_dispatches_per_run=2, max_concurrent_children=8),
    )

    run = asyncio.create_task(driver.run())
    # Both admitted children are parked before the count is read, so the spin
    # below is bounded work rather than a timing assumption.
    await asyncio.wait_for(
        asyncio.gather(started[0].wait(), started[1].wait()), timeout=5
    )
    for _ in range(50):
        await asyncio.sleep(0)

    assert len(dispatcher.calls) == 2
    run.cancel()
    with pytest.raises(asyncio.CancelledError):
        await run


@pytest.mark.asyncio
async def test_unknown_agent_type_is_js_catchable() -> None:
    """Catchable, and a TypeError — the class the prelude escalates on. A
    script that lets it through must end the run, not collect a null."""
    driver, _, dispatcher, backend = await _make_driver(
        _script(
            "try { await agent('x', { agentType: 'missing' }); } "
            "catch (e) { return e.constructor.name + ' | ' + e.message; }"
        )
    )

    await driver.run()

    result = json.loads(backend.writes[f"{driver.base_rel}/result.json"])
    assert result.startswith("TypeError | ")
    assert "Available: general-purpose, research" in result
    assert dispatcher.calls == []


@pytest.mark.asyncio
async def test_run_result_is_truncated_only_in_summary() -> None:
    driver, _, _, backend = await _make_driver(
        _script("return 'x'.repeat(100);"),
        caps=_caps(max_summary_bytes=20),
    )

    result = await driver.run()

    assert "... [truncated]" in result
    assert len(json.loads(backend.writes[f"{driver.base_rel}/result.json"])) == 100


@pytest.mark.asyncio
async def test_a_truncated_summary_says_where_the_rest_is() -> None:
    """Clipping is only safe because the whole value sits somewhere reachable,
    and the agent has to be told where — otherwise the omission reads as the
    result simply ending there. A complete summary advertises no such file.
    """
    clipped, _, _, _ = await _make_driver(
        _script("return 'x'.repeat(100);"), caps=_caps(max_summary_bytes=20)
    )
    assert f"{clipped.base_rel}/result.json" in await clipped.run()

    whole, _, _, _ = await _make_driver(_script("return 'small';"))
    assert "result.json" not in await whole.run()


@pytest.mark.asyncio
async def test_the_result_dump_cap_does_not_size_the_summary() -> None:
    """The two caps are independent: what a script manipulates in JS is not
    what a reader is handed. Sizing the summary off the dump cap put its whole
    allowance — up to megabytes of JSON, past any context window — into both
    the tool result and the checkpoint archive.
    """
    driver, _, _, backend = await _make_driver(
        _script("return 'x'.repeat(4000);"),
        caps=_caps(max_result_bytes=16 * 1024 * 1024, max_summary_bytes=1024),
    )

    summary = await driver.run()

    assert "... [truncated]" in summary
    assert len(summary.encode()) < 4000
    assert len(json.loads(backend.writes[f"{driver.base_rel}/result.json"])) == 4000


def _fail_writing(backend: Any, filename: str) -> None:
    """Make one artifact's write report failure, as a full sandbox would."""
    real_write = backend.awrite_text

    async def failing(path: str, content: str) -> bool:
        if path.endswith(f"/{filename}"):
            return False
        return await real_write(path, content)

    backend.awrite_text = failing


@pytest.mark.asyncio
async def test_a_truncated_result_fails_when_its_full_copy_did_not_land() -> None:
    """The clipped summary plus a missing result.json loses the remainder for
    good, so the run must not report success and hand out a ref to nothing."""
    driver, _, _, backend = await _make_driver(
        _script("return 'x'.repeat(100);"),
        caps=_caps(max_summary_bytes=20),
    )
    _fail_writing(backend, "result.json")

    with pytest.raises(WorkflowRunError, match="unrecoverable"):
        await driver.run()


@pytest.mark.asyncio
async def test_a_complete_result_survives_a_failed_artifact_write() -> None:
    """Nothing was omitted, so the summary already carries the whole result and
    the missing file costs the caller nothing — only truncation makes it load
    bearing."""
    driver, _, _, backend = await _make_driver(_script("return 'small';"))
    _fail_writing(backend, "result.json")

    assert "small" in await driver.run()


@pytest.mark.asyncio
async def test_result_preview_tracks_the_summary_cap() -> None:
    """The panel never shows more of the result than the summary the same run
    produced, and a result under the cap arrives parseable."""
    driver, registry, _, _ = await _make_driver(
        _script("return 'x'.repeat(400);"),
        caps=_caps(max_summary_bytes=64),
    )
    await driver.run()

    preview = registry.events[-1]["result_preview"]
    assert preview.endswith("... [truncated]")
    assert len(preview.encode()) <= 64 + len("\n... [truncated]")

    driver2, registry2, _, _ = await _make_driver(
        _script("return { blob: 'y'.repeat(60000) };")
    )
    await driver2.run()

    assert json.loads(registry2.events[-1]["result_preview"]) == {"blob": "y" * 60000}


@pytest.mark.asyncio
async def test_result_preview_stays_under_the_checkpoint_ceiling() -> None:
    """max_summary_bytes is configurable to 1 MiB; the frame that carries the
    preview into the checkpoint is not."""
    driver, registry, _, _ = await _make_driver(
        _script("return 'z'.repeat(400000);"),
        caps=_caps(max_summary_bytes=1024 * 1024),
    )
    await driver.run()

    preview = registry.events[-1]["result_preview"]
    assert len(preview.encode()) <= WorkflowEmitter._UI_RESULT_PREVIEW_CEILING + len(
        "\n... [truncated]"
    )


@pytest.mark.asyncio
async def test_script_error_fails_run_with_js_message() -> None:
    driver, _, _, backend = await _make_driver(_script("throw new Error('boom');"))

    with pytest.raises(WorkflowRunError, match="Error: boom"):
        await driver.run()

    error = json.loads(backend.writes[f"{driver.base_rel}/error.json"])
    assert error["status"] == "script_error"


@pytest.mark.asyncio
async def test_lifecycle_event_sequence_and_phase_propagation() -> None:
    driver, registry, _, _ = await _make_driver(
        _script(
            "phase('Gather'); log('starting'); "
            "return await agent('work', { label: 'Worker' });"
        )
    )

    await driver.run()

    assert [event["phase"] for event in registry.events] == [
        "run_started",
        "phase",
        "log",
        "child_started",
        "child_done",
        "run_completed",
    ]
    started = registry.events[3]
    assert started["seq"] == 0
    assert started["workflow_phase"] == "Gather"
    # Progress payloads consumed by the frontend workflow-run card.
    assert "tokens_spent" in registry.events[4]
    terminal = registry.events[-1]
    assert terminal["children_total"] == 1
    assert isinstance(terminal["duration_s"], float)
    assert "tokens_spent" in terminal


@pytest.mark.asyncio
async def test_cpu_timeout_fails_run() -> None:
    driver, _, _, backend = await _make_driver(
        _script("while (true) {}"), caps=_caps(cpu_budget_s=0.05)
    )

    with pytest.raises(WorkflowRunError, match="cpu_timeout"):
        await driver.run()

    status = json.loads(backend.writes[f"{driver.base_rel}/status.json"])
    assert status["status"] == "failed"


@pytest.mark.asyncio
async def test_stopping_one_child_nulls_that_call_and_leaves_the_run_alive() -> None:
    """A stop aimed at a single child is a child-level failure, not a run stop.
    ``agent()`` resolves to null for any child that does not succeed, so the
    script keeps going; unwinding instead strands its promise and the whole run
    dies on a QuickJS deadlock."""
    started = asyncio.Event()
    driver, registry, dispatcher, backend = await _make_driver(
        _script("const r = await agent('park'); return { child: r, survived: true };"),
        behaviors=[Hang(started)],
    )
    run_task = asyncio.create_task(driver.run())
    await asyncio.wait_for(started.wait(), timeout=5)

    dispatcher.tasks[0].asyncio_task.cancel()
    await asyncio.wait_for(run_task, timeout=5)

    assert json.loads(backend.writes[f"{driver.base_rel}/result.json"]) == {
        "child": None,
        "survived": True,
    }
    done = [e for e in registry.events if e["phase"] == "child_done"]
    assert [e["status"] for e in done] == ["cancelled"]
    terminal = registry.events[-1]
    assert terminal["phase"] == "run_completed"
    assert terminal["status"] == "completed"


@pytest.mark.asyncio
async def test_cancellation_mid_child_cancels_children_and_finalizes() -> None:
    started = asyncio.Event()
    driver, registry, dispatcher, backend = await _make_driver(
        _script("return await agent('park');"), behaviors=[Hang(started)]
    )
    run_task = asyncio.create_task(driver.run())
    await asyncio.wait_for(started.wait(), timeout=5)

    run_task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await run_task

    assert dispatcher.tasks[0].cancelled is True
    status = json.loads(backend.writes[f"{driver.base_rel}/status.json"])
    assert status["status"] == "cancelled"
    terminal = registry.events[-1]
    assert terminal["phase"] == "run_completed"
    assert terminal["status"] == "cancelled"
    # The in-flight child must report a terminal frame, and must do it BEFORE
    # the run's own: the terminal frame is what gets snapshotted, so a later
    # child frame misses replay and the panel spins that row forever.
    phases = [event["phase"] for event in registry.events]
    assert phases.index("child_done") < phases.index("run_completed")
    done = next(e for e in registry.events if e["phase"] == "child_done")
    assert done["status"] == "cancelled"


@pytest.mark.asyncio
async def test_user_stop_delivers_the_settle_frames_through_the_seal() -> None:
    """The real stop path, which the bare-cancel test above cannot reach: the
    registry stamps the run task cancelled BEFORE its writer unwinds, and a
    cancelled task's streams are sealed. Frames dropped by that seal never
    reach the live card, which then sits in "Stopping…" until a reload rebuilds
    it from the checkpoint snapshot."""
    started = asyncio.Event()
    driver, registry, dispatcher, backend = await _make_driver(
        _script("return await agent('park');"), behaviors=[Hang(started)]
    )
    entry = driver.spec.run_task
    entry.asyncio_task = asyncio.create_task(driver.run())
    await asyncio.wait_for(started.wait(), timeout=5)

    assert await registry.cancel_task(entry.task_id, force=True) is True
    with pytest.raises(asyncio.CancelledError):
        await entry.asyncio_task

    assert entry.cancelled is True
    phases = [event["phase"] for event in registry.events]
    assert phases[-1] == "run_completed"
    assert registry.events[-1]["status"] == "cancelled"
    done = next(e for e in registry.events if e["phase"] == "child_done")
    assert done["status"] == "cancelled"


@pytest.mark.asyncio
async def test_run_timeout_cancels_children_and_fails_run() -> None:
    started = asyncio.Event()
    driver, registry, dispatcher, backend = await _make_driver(
        _script("return await agent('park');"),
        behaviors=[Hang(started)],
        caps=_caps(run_timeout=0.3),
    )

    with pytest.raises(WorkflowRunError, match="timed out"):
        await driver.run()

    assert dispatcher.tasks[0].cancelled is True
    status = json.loads(backend.writes[f"{driver.base_rel}/status.json"])
    assert status["status"] == "failed"
    terminal = registry.events[-1]
    assert terminal["phase"] == "run_completed"
    assert terminal["status"] == "failed"
    assert "timed out" in terminal["error"]


@pytest.mark.asyncio
async def test_cancel_children_stamps_ledger_intent() -> None:
    """Driver-internal force-cancels stamp durable cancel intent for
    ledgered children first (registry cancel-path invariant): a worker
    dying mid-unwind must recover them as cancelled, not worker_lost."""
    from unittest.mock import AsyncMock

    started = asyncio.Event()
    driver, registry, dispatcher, _ = await _make_driver(
        _script("return await agent('park');"),
        behaviors=[Hang(started)],
        caps=_caps(run_timeout=0.3),
    )
    registry.run_ledger = AsyncMock()
    original_dispatch = dispatcher.dispatch

    async def _ledgered_dispatch(**kwargs: Any) -> Any:
        task = await original_dispatch(**kwargs)
        task.task_run_id = f"run-{task.task_id}"
        return task

    dispatcher.dispatch = _ledgered_dispatch

    with pytest.raises(WorkflowRunError, match="timed out"):
        await driver.run()

    child = dispatcher.tasks[0]
    assert child.cancelled is True
    stamped = [
        call.args[0]
        for call in registry.run_ledger.request_task_run_cancel.await_args_list
    ]
    assert stamped == [child.task_run_id]


@pytest.mark.asyncio
async def test_cancel_children_leaves_a_finished_writer_to_settle_as_itself() -> None:
    """A child whose writer returned but whose done-callback hasn't settled it
    yet is not the run's to overwrite: the registry's cancel predicate excludes
    exactly that task, and the driver has to inherit that predicate."""
    driver, registry, _, _ = await _make_driver(_script("return 1;"))

    async def _already_returned() -> str:
        return "child result"

    child = await registry.register(
        tool_call_id="child-settling",
        description="child",
        prompt="p",
        subagent_type="general-purpose",
        run_id="launch-1",
        owner_task_id=driver.wf_task_id,
    )
    child.asyncio_task = asyncio.create_task(_already_returned())
    await child.asyncio_task

    assert await driver._cancel_children(reason="Workflow script_error: boom") == 0
    assert child.completed is False
    assert child.cancelled is False
    assert child.error is None


@pytest.mark.asyncio
async def test_cancel_children_cancels_live_ones_with_the_run_s_reason() -> None:
    started = asyncio.Event()
    driver, _, dispatcher, _ = await _make_driver(
        _script("return await agent('park');"),
        behaviors=[Hang(started)],
        caps=_caps(run_timeout=0.3),
    )

    with pytest.raises(WorkflowRunError, match="timed out"):
        await driver.run()

    child = dispatcher.tasks[0]
    assert child.cancelled is True
    assert "timed out" in (child.error or "")


@pytest.mark.asyncio
async def test_script_chatter_emissions_are_capped(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(WorkflowEmitter, "_SCRIPT_EMIT_CAP", 10)
    driver, registry, _, _ = await _make_driver(
        _script("for (let i = 0; i < 50; i++) log('m' + i);\nreturn 1;")
    )

    await driver.run()

    logs = [e for e in registry.events if e["phase"] == "log"]
    assert len(logs) == 10
    assert "emit cap reached" in logs[-1]["message"]
    terminal = registry.events[-1]
    assert terminal["phase"] == "run_completed"
    assert terminal["status"] == "completed"


@pytest.mark.asyncio
async def test_dispatch_failure_records_child_error_without_killing_run() -> None:
    driver, registry, dispatcher, _ = await _make_driver(
        _script("const v = await agent('a'); return v === null;")
    )

    async def _explode(**kwargs: Any) -> Any:
        raise RuntimeError("spawn infra down")

    dispatcher.dispatch = _explode

    await driver.run()

    done = next(e for e in registry.events if e["phase"] == "child_done")
    assert done["status"] == "error"
    assert "spawn infra down" in done["error"]
    # No child was ever spawned, so the frame has no child to point at — the one
    # thing that tells this apart from a child that ran and then failed.
    assert done.get("child_task_id") is None
    terminal = registry.events[-1]
    assert terminal["status"] == "completed"
    assert json.loads(terminal["result_preview"]) is True


@pytest.mark.asyncio
async def test_run_started_carries_script_source() -> None:
    driver, registry, _, _ = await _make_driver(_script("return 1;"))
    await driver.run()
    assert registry.events[0]["source"] == "inline"

    driver2, registry2, _, _ = await _make_driver(
        _script("return 1;"), source="saved"
    )
    await driver2.run()
    assert registry2.events[0]["source"] == "saved"


@pytest.mark.asyncio
async def test_child_done_reports_tokens_and_terminal_result_preview() -> None:
    driver, registry, _, _ = await _make_driver(
        _script("const v = await agent('a'); return { v };"),
        behaviors=[
            Immediate(
                {"success": True, "result": "A"},
                usage=[
                    {"usage": {"total_tokens": 1200}},
                    {"usage": {"input_tokens": 30, "output_tokens": 20}},
                ],
            )
        ],
    )

    await driver.run()

    done = next(e for e in registry.events if e["phase"] == "child_done")
    assert done["tokens_used"] == 1250
    # Present iff there is one — replay drops a null error, so emitting the key
    # here would make the same run read differently after a reload.
    assert "error" not in done
    terminal = registry.events[-1]
    assert terminal["status"] == "completed"
    assert json.loads(terminal["result_preview"]) == {"v": "A"}


@pytest.mark.asyncio
async def test_child_failure_error_is_clipped_into_child_done() -> None:
    driver, registry, _, _ = await _make_driver(
        _script("const v = await agent('a'); return v === null;"),
        behaviors=[Immediate({"success": False, "error": "boom-" + "x" * 600})],
    )

    await driver.run()

    done = next(e for e in registry.events if e["phase"] == "child_done")
    assert done["status"] == "error"
    assert done["error"].startswith("ERROR: boom-")
    assert len(done["error"].encode()) <= WorkflowEmitter._UI_ERROR_BYTES + 16
    terminal = registry.events[-1]
    # Failed children don't fail the run; no preview beyond the JS result.
    assert json.loads(terminal["result_preview"]) is True


@pytest.mark.asyncio
async def test_child_error_credentials_are_scrubbed_before_the_wire() -> None:
    """A frame's dict is shared by the live stream and the checkpointed
    snapshot, so an unscrubbed provider error would leak into both at once —
    and nothing downstream re-parses the payload to catch it later."""
    driver, registry, _, _ = await _make_driver(
        _script("const v = await agent('a'); return v === null;"),
        behaviors=[
            Immediate({"success": False, "error": "401 from Bearer sk-liveKey12345678"})
        ],
    )

    await driver.run()

    done = next(e for e in registry.events if e["phase"] == "child_done")
    assert "sk-liveKey12345678" not in done["error"]
    assert "[REDACTED]" in done["error"]


@pytest.mark.asyncio
async def test_run_terminal_error_is_scrubbed_and_clipped_like_a_child() -> None:
    """The run's own failure text reaches the panel by the same route a child's
    does, so it gets the same treatment — scrubbed, then clipped."""
    driver, registry, _, _ = await _make_driver(
        _script(
            "throw new Error('401 from Bearer sk-liveKey12345678 ' "
            "+ 'x'.repeat(600));"
        )
    )

    with pytest.raises(WorkflowRunError):
        await driver.run()

    terminal = registry.events[-1]
    assert terminal["phase"] == "run_completed"
    assert "sk-liveKey12345678" not in terminal["error"]
    assert "[REDACTED]" in terminal["error"]
    assert len(terminal["error"].encode()) <= WorkflowEmitter._UI_ERROR_BYTES + 16


@pytest.mark.asyncio
async def test_absorbed_slot_diagnostic_is_scrubbed_like_an_error() -> None:
    """The prelude names the absorbed exception on the run log, so a `log`
    frame now carries error text the terminal path would have scrubbed — an
    upstream failure reaching the script is the same string either way."""
    driver, registry, _, _ = await _make_driver(
        _script(
            "return await parallel(["
            "  async () => { throw new Error('401 from Bearer sk-liveKey12345678'); }"
            "]);"
        )
    )

    await driver.run()

    logs = [e for e in registry.events if e["phase"] == "log"]
    assert any("[runtime]" in e["message"] for e in logs)
    assert not any("sk-liveKey12345678" in e["message"] for e in logs)
    assert any("[REDACTED]" in e["message"] for e in logs)


@pytest.mark.asyncio
async def test_unlisted_frame_field_never_reaches_the_wire() -> None:
    """The frame whitelist is closed: the live stream and the checkpointed
    snapshot are the same dict, so an unlisted key would leak into both."""
    driver, registry, _, _ = await _make_driver(_script("return 1;"))

    await driver._emitter.emit("log", message="visible", credentials="leak")

    frame = registry.events[-1]
    assert frame["message"] == "visible"
    assert "credentials" not in frame


@pytest.mark.asyncio
async def test_terminal_ui_snapshot_upserts_lifecycle_frames() -> None:
    """At terminal the driver upserts ONE `workflow_run` ui record with the
    full frame sequence under a stable id, written into the run task's own
    checkpoint namespace through the spec checkpointer (the guard-bound
    saver in production) — the checkpoint-sourced replay home for the
    workflow panel."""
    from langgraph.checkpoint.memory import InMemorySaver

    saver = InMemorySaver()
    driver, registry, _, _ = await _make_driver(
        _script(
            "phase('Research'); log('go'); const v = await agent('a'); "
            "return v;"
        ),
        behaviors=[Immediate({"success": True, "result": "A"})],
        checkpointer=saver,
    )

    await driver.run()

    records = await _ns_ui_records(saver, driver.wf_task_id)
    assert len(records) == 1
    record = records[0]
    assert record["name"] == "workflow_run"
    assert record["id"] == f"workflow-run-{driver.wf_task_id}"
    props = record["props"]
    assert props["task_id"] == driver.wf_task_id
    phases = [f["phase"] for f in props["frames"]]
    assert phases[0] == "run_started"
    assert phases[-1] == "run_completed"
    assert {"phase", "log", "child_started", "child_done"} <= set(phases)
    assert all(
        f["agent"] == f"task:{driver.wf_task_id}" for f in props["frames"]
    )
    # The snapshot mirrors the live capture stream exactly.
    assert props["frames"] == registry.events
    # Root namespace untouched: the write never forks the thread chain.
    root = await saver.aget_tuple({"configurable": {"thread_id": "thread-1"}})
    assert root is None


@pytest.mark.asyncio
async def test_snapshot_frames_replay_with_every_field_intact() -> None:
    """Live and replay read the same frame through two different code paths, so
    a field one side does not know about renders fine live and blanks on
    refresh — invisible to any test that only watches one path.

    One run stamps every declared field, so this covers the whole vocabulary.
    """
    from langgraph.checkpoint.memory import InMemorySaver

    from ptc_agent.agent.middleware.background_subagent.workflow.emitter import (
        WORKFLOW_FRAME_FIELDS,
    )
    from src.server.services.history.projector import workflow_run_items

    saver = InMemorySaver()
    driver, registry, _, _ = await _make_driver(
        _script("phase('Research'); log('go'); return await agent('a');"),
        behaviors=[Immediate({"success": True, "result": "A"})],
        checkpointer=saver,
    )

    await driver.run()

    stamped = {key for frame in registry.events for key in frame}
    assert stamped == set(WORKFLOW_FRAME_FIELDS)

    records = await _ns_ui_records(saver, driver.wf_task_id)
    history = types.SimpleNamespace(new_ui_records=records)
    replayed = [item["data"] for item in workflow_run_items(history)]
    assert replayed == registry.events


def test_every_run_status_the_driver_finalizes_is_declared() -> None:
    """The run vocabulary is closed both ways: the emitter declares exactly the
    statuses ``_finalize`` is called with, so neither side can drift alone."""
    import ast
    import inspect

    from ptc_agent.agent.middleware.background_subagent.workflow import (
        driver as driver_module,
    )
    from ptc_agent.agent.middleware.background_subagent.workflow.emitter import (
        WORKFLOW_RUN_STATUSES,
    )

    tree = ast.parse(inspect.getsource(driver_module))
    finalized = {
        node.args[0].value
        for node in ast.walk(tree)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and node.func.attr == "_finalize"
        and node.args
        and isinstance(node.args[0], ast.Constant)
    }
    assert finalized == set(WORKFLOW_RUN_STATUSES)


def test_child_status_never_widens_the_declared_child_vocabulary() -> None:
    """A widened ``TerminalStatus`` must not reach the wire unannounced: every
    task status maps into the declared child vocabulary or reads as a failure.
    """
    from typing import get_args

    from ptc_agent.agent.middleware.background_subagent.registry import TerminalStatus
    from ptc_agent.agent.middleware.background_subagent.workflow.driver import (
        _child_status,
    )
    from ptc_agent.agent.middleware.background_subagent.workflow.emitter import (
        WORKFLOW_CHILD_STATUSES,
    )

    for status in (*get_args(TerminalStatus), None):
        task = types.SimpleNamespace(terminal_status=status)
        assert _child_status(task) in WORKFLOW_CHILD_STATUSES

    assert _child_status(None) == "error"
    assert _child_status(types.SimpleNamespace(terminal_status="completed")) == "ok"
    assert _child_status(types.SimpleNamespace(terminal_status="never_started")) == (
        "error"
    )


@pytest.mark.asyncio
async def test_ui_snapshot_failure_never_breaks_the_run() -> None:
    class BrokenSaver:
        """Any checkpointer use fails — e.g. a guard session that lost its
        namespace refuses the write."""

        def __getattr__(self, name: str) -> Any:
            raise RuntimeError("saver down")

    driver, registry, _, backend = await _make_driver(
        _script("return await agent('a');"),
        behaviors=[Immediate({"success": True, "result": "A"})],
        checkpointer=BrokenSaver(),
    )

    result = await driver.run()

    assert "1 subagent dispatch(es)" in result
    status = json.loads(backend.writes[f"{driver.base_rel}/status.json"])
    assert status["status"] == "completed"


@pytest.mark.asyncio
async def test_ledgered_run_archives_its_taskoutput_text() -> None:
    """A ledgered run writes its summary into its own namespace under the
    ledger run id — the only durable source TaskOutput has for a run that
    leaves no transcript behind."""
    from langgraph.checkpoint.memory import InMemorySaver

    saver = InMemorySaver()
    driver, _, _, _ = await _make_driver(
        _script("return await agent('a');"),
        behaviors=[Immediate({"success": True, "result": "A"})],
        checkpointer=saver,
    )
    driver.spec.run_task.task_run_id = "run-1"

    summary = await driver.run()

    records = await _ns_ui_records(saver, driver.wf_task_id)
    archived = [r for r in records if r["name"] == "task_result"]
    assert len(archived) == 1
    assert archived[0]["id"] == "task-result-run-1"
    props = archived[0]["props"]
    assert props["task_run_id"] == "run-1"
    assert props["text"] == summary
    assert props["truncated"] is False
    assert props["result_ref"] == f"{driver.base_rel}/result.json"
    assert read_task_result(records, "run-1") == summary
    # Scoped to the run that wrote it — a successor in the same namespace
    # must not be handed its predecessor's answer.
    assert read_task_result(records, "run-2") is None


@pytest.mark.asyncio
async def test_unledgered_run_skips_the_archive_and_still_completes() -> None:
    """No ledger row means no ledger-authoritative read to satisfy, so the
    archive is not attempted and its absence is not a failure."""
    from langgraph.checkpoint.memory import InMemorySaver

    saver = InMemorySaver()
    driver, _, _, backend = await _make_driver(
        _script("return await agent('a');"),
        behaviors=[Immediate({"success": True, "result": "A"})],
        checkpointer=saver,
    )
    assert driver.spec.run_task.task_run_id is None

    await driver.run()

    records = await _ns_ui_records(saver, driver.wf_task_id)
    assert not [r for r in records if r["name"] == "task_result"]
    status = json.loads(backend.writes[f"{driver.base_rel}/status.json"])
    assert status["status"] == "completed"


@pytest.mark.asyncio
async def test_failed_archive_fails_the_run_instead_of_completing() -> None:
    """"Completed" and "result readable" are the same claim: a run whose
    archive write is refused must take the failure arm, not settle on a
    result nothing can recover."""

    class BrokenSaver:
        def __getattr__(self, name: str) -> Any:
            raise RuntimeError("saver down")

    driver, registry, _, backend = await _make_driver(
        _script("return await agent('a');"),
        behaviors=[Immediate({"success": True, "result": "A"})],
        checkpointer=BrokenSaver(),
    )
    driver.spec.run_task.task_run_id = "run-1"

    with pytest.raises(WorkflowRunError):
        await driver.run()

    status = json.loads(backend.writes[f"{driver.base_rel}/status.json"])
    assert status["status"] == "failed"
    # The card settles on the same verdict the ledger will take.
    assert registry.events[-1]["status"] == "failed"


@pytest.mark.asyncio
async def test_ledgered_run_without_a_checkpointer_fails() -> None:
    """Admission refuses this pairing; the driver refuses it too rather than
    quietly producing an unreadable result."""
    driver, _, _, backend = await _make_driver(
        _script("return await agent('a');"),
        behaviors=[Immediate({"success": True, "result": "A"})],
    )
    driver.spec.run_task.task_run_id = "run-1"

    with pytest.raises(WorkflowRunError):
        await driver.run()

    status = json.loads(backend.writes[f"{driver.base_rel}/status.json"])
    assert status["status"] == "failed"


@pytest.mark.asyncio
async def test_ui_snapshot_caps_script_chatter_keeps_structure() -> None:
    """`phase()`/`log()` spam is capped oldest-first in the snapshot;
    structural run/child frames are never evicted."""
    from langgraph.checkpoint.memory import InMemorySaver

    saver = InMemorySaver()
    driver, registry, _, _ = await _make_driver(
        _script(
            "for (let i = 0; i < 250; i++) log(`m${i}`); "
            "return await agent('a');"
        ),
        behaviors=[Immediate({"success": True, "result": "A"})],
        checkpointer=saver,
    )

    await driver.run()

    records = await _ns_ui_records(saver, driver.wf_task_id)
    frames = records[0]["props"]["frames"]
    chatter = [f for f in frames if f["phase"] in ("phase", "log")]
    assert len(chatter) == WorkflowEmitter._UI_CHATTER_FRAME_CAP
    # Oldest chatter evicted, newest kept.
    assert chatter[-1]["message"] == "m249"
    assert not any(f.get("message") == "m0" for f in chatter)
    structural = [f["phase"] for f in frames if f["phase"] not in ("phase", "log")]
    assert structural[0] == "run_started"
    assert structural[-1] == "run_completed"
    assert "child_started" in structural and "child_done" in structural
