"""Direct background-subagent dispatch path (``dispatch.py``).

This module is the extraction of the Task-tool spawn path so the workflow
driver can dispatch children directly against the same compiled subagent
graphs. These tests prove the extracted path preserves the init-path
invariants:

- ``run_subagent_graph`` builds the same per-task config the Task-tool init
  branch does (``checkpoint_ns=task:{task_id}`` under the parent thread,
  recursion_limit 2000, subagent_type metadata, checkpoint-position keys
  dropped, background token-tracker callback attached from the ContextVar).
- ``dispatch_background_subagent`` registers a task carrying
  ``spawned_run_id`` / ``owner_task_id``, spawns its writer through the
  canonical pipeline (admission → meta → run-opener → fenced publish →
  done-callback), runs the child to completion, and sets the per-task
  ContextVars inside the *child* context only (never leaking into the
  caller). Children ARE ledger-admitted — that is what routes their events
  onto a live v2 task lane — but with ``parent_run_id=None`` so no
  report-back job is ever born; a rejected admission aborts the dispatch.
- ``SubagentDispatcher`` exposes sorted types and threads its bound
  middleware / graphs / thread_id through to the module-level dispatch.

Fake graphs are hand-rolled with an ``astream`` matching what
``arun_subagent_streaming`` drives (``stream_mode=["values","messages",
"custom"]`` yielding ``(mode, data)`` tuples). The middleware is a
duck-typed fake over a real registry, matching the seam the workflow
driver wires in production.
"""

from __future__ import annotations

from typing import Any

import pytest
from langchain_core.messages import AIMessage, ToolMessage
from langgraph.types import Command

from ptc_agent.agent.middleware.background_subagent import dispatch as dispatch_mod
from ptc_agent.agent.middleware.background_subagent import spawn as spawn_mod
from ptc_agent.agent.middleware.background_subagent.context import (
    current_background_agent_id,
    current_background_token_tracker,
    current_background_tool_call_id,
)
from ptc_agent.agent.middleware.background_subagent.dispatch import (
    SubagentDispatcher,
    dispatch_background_subagent,
    run_subagent_graph,
)
from ptc_agent.agent.middleware.background_subagent.registry import (
    BackgroundTask,
    BackgroundTaskRegistry,
)
from src.utils.tracking.per_call_token_tracker import PerCallTokenTracker


class FakeMiddleware:
    """Duck-typed ``BackgroundSubagentMiddleware`` seam for the spawn
    pipeline: real registry, no namespace fence, no ledger (admission
    records its kwargs and returns "" like the real no-ledger path)."""

    def __init__(self, registry: BackgroundTaskRegistry | None = None) -> None:
        self.registry = registry or BackgroundTaskRegistry()
        self.namespace_owner = None
        self.openers: list[tuple[str, str]] = []
        self.admissions: list[dict[str, Any]] = []
        self.admission_result: str = ""
        self.admission_refusal: Exception | None = None

    async def _acquire_task_ns(self, task_id: str) -> bool:
        return True

    async def admit_task_run(self, task: Any, **kwargs: Any) -> str:
        self.admissions.append({"task": task, **kwargs})
        if self.admission_refusal is not None:
            raise self.admission_refusal
        return self.admission_result

    async def _append_run_opener(self, task: Any, prompt: str) -> None:
        self.openers.append((task.task_id, prompt))

    async def _abort_admitted_run(self, task: Any, exc: Exception) -> None:
        pass

    async def _finalize_cancelled_before_spawn(self, task: Any) -> None:
        pass


class _RecordingGraph:
    """Minimal compiled-graph stand-in for ``arun_subagent_streaming``.

    Records the state / config / stream_mode it was driven with, then yields
    a single final ``values`` snapshot carrying the AIMessage the subagent
    'produced'. Extra state keys (``extra_values``) exercise the non-message
    state pass-through in ``return_command_with_state_update``.
    """

    def __init__(self, final_text: str = "subagent done", extra_values=None) -> None:
        self.received_config: dict | None = None
        self.received_state: dict | None = None
        self.received_stream_mode = None
        self._final_text = final_text
        self._extra_values = extra_values or {}

    async def astream(self, state, config, stream_mode=None):
        self.received_state = state
        self.received_config = config
        self.received_stream_mode = stream_mode
        snapshot = {
            "messages": [AIMessage(content=self._final_text)],
            **self._extra_values,
        }
        yield ("values", snapshot)


# ---------------------------------------------------------------------------
# run_subagent_graph — config invariants
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_run_subagent_graph_config_invariants():
    """The driven config mirrors the Task-tool init path, dropping only the
    per-call pregel state the Task path gets fresh per tool call: the
    scratchpad (its shared subgraph counter suffixes each successive child's
    checkpoint_ns with ``|N``, silently killing token streaming for every
    child after the first) and the settled turn's stream handle. The
    remaining ``__pregel_*`` internals are KEPT — they mark the run as
    nested, which is what makes LangGraph preserve the ``task:<id>``
    checkpoint namespace instead of resetting it to the root namespace."""
    graph = _RecordingGraph()
    registry = BackgroundTaskRegistry()  # no task registered -> no forwarder
    base = {
        "thread_id": "OLD-should-be-overwritten",
        "checkpoint_ns": "leaked-ns",
        "checkpoint_id": "leaked-id",
        "checkpoint_map": {"x": 1},
        "__pregel_scratchpad": object(),
        "__pregel_stream": object(),
        "__pregel_send": object(),
        "__pregel_task_id": "parent-task",
        "user_id": "user-9",
        "run_name": "keep-me",
    }

    await run_subagent_graph(
        graph,
        prompt="P" * 250,
        parent_thread_id="parent-thread",
        task_id="task-abc",
        subagent_type="research",
        registry=registry,
        tool_call_id="tc-1",
        base_configurable=base,
    )

    cfg = graph.received_config
    conf = cfg["configurable"]
    # Per-task namespace under the parent thread.
    assert conf["checkpoint_ns"] == "task:task-abc"
    assert conf["thread_id"] == "parent-thread"
    # Non-checkpoint base keys pass through.
    assert conf["user_id"] == "user-9"
    assert conf["run_name"] == "keep-me"
    # Checkpoint-position keys are dropped.
    assert "checkpoint_id" not in conf
    assert "checkpoint_map" not in conf
    # Per-call pregel state is dropped; nesting markers are kept so the
    # child's checkpoint_ns survives (a non-nested run resets ns to "" and
    # writes checkpoints into the thread's root namespace).
    assert "__pregel_scratchpad" not in conf
    assert "__pregel_stream" not in conf
    assert conf["__pregel_task_id"] == "parent-task"
    assert conf["__pregel_send"] is base["__pregel_send"]
    # Recursion limit + metadata mirror the parent's with_config.
    assert cfg["recursion_limit"] == 2000
    assert cfg["metadata"]["subagent_type"] == "research"
    assert cfg["metadata"]["description"] == ("P" * 250)[:200]
    # Tracker unset -> no callbacks key at all.
    assert "callbacks" not in cfg
    # The drop-filter must not mutate the caller's base dict.
    assert base["checkpoint_ns"] == "leaked-ns"
    # The subagent is driven with the combined stream modes.
    assert set(graph.received_stream_mode) == {"values", "messages", "custom"}


@pytest.mark.asyncio
async def test_run_subagent_graph_attaches_token_tracker_callback():
    """When the background token-tracker ContextVar is set, it is attached as
    the sole callback so the child's LLM calls bill to the per-task tracker."""
    graph = _RecordingGraph()
    tracker = PerCallTokenTracker()
    token = current_background_token_tracker.set(tracker)
    try:
        await run_subagent_graph(
            graph,
            prompt="p",
            parent_thread_id="pt",
            task_id="t1",
            subagent_type="general-purpose",
            registry=BackgroundTaskRegistry(),
            tool_call_id="tc",
            base_configurable=None,
        )
    finally:
        current_background_token_tracker.reset(token)

    assert graph.received_config["callbacks"] == [tracker]


@pytest.mark.asyncio
async def test_run_subagent_graph_returns_command_with_final_text():
    """The final AIMessage is returned as a ToolMessage Command (trailing
    whitespace stripped), and non-message state keys pass through the update."""
    graph = _RecordingGraph(
        final_text="the final answer  ", extra_values={"scratch": "kept"}
    )
    cmd = await run_subagent_graph(
        graph,
        prompt="p",
        parent_thread_id="pt",
        task_id="t1",
        subagent_type="research",
        registry=BackgroundTaskRegistry(),
        tool_call_id="tc-final",
        base_configurable=None,
    )

    assert isinstance(cmd, Command)
    msgs = cmd.update["messages"]
    assert len(msgs) == 1
    tool_msg = msgs[0]
    assert isinstance(tool_msg, ToolMessage)
    assert tool_msg.tool_call_id == "tc-final"
    assert tool_msg.content == "the final answer"  # rstrip'd
    # Non-message state keys are merged into the command update.
    assert cmd.update["scratch"] == "kept"


# ---------------------------------------------------------------------------
# dispatch_background_subagent
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_dispatch_unknown_type_raises_and_registers_nothing():
    """An unknown subagent_type raises ValueError listing the sorted available
    types, and no task is registered."""
    mw = FakeMiddleware()
    graphs = {"general-purpose": _RecordingGraph(), "research": _RecordingGraph()}

    with pytest.raises(ValueError) as exc_info:
        await dispatch_background_subagent(
            mw,
            graphs,
            subagent_type="wizard",
            description="d",
            prompt="p",
            parent_thread_id="t",
            run_id="r1",
        )

    msg = str(exc_info.value)
    assert "Unknown subagent type 'wizard'" in msg
    assert "general-purpose, research" in msg  # sorted available
    assert mw.registry.task_count == 0


@pytest.mark.asyncio
async def test_dispatch_success_registers_task_and_runs_to_completion():
    """Success path registers a subagent-kind task carrying the run/owner ids
    and an auto-generated 'dispatch-' tool_call_id, ledger-admits it with a
    NULL parent (task lane, no report-back), spawns its writer through the
    canonical pipeline (run-opener appended), then runs the child graph to
    completion under the per-task namespace."""
    mw = FakeMiddleware()
    graph = _RecordingGraph(final_text="dispatched result")
    graphs = {"general-purpose": graph, "research": _RecordingGraph()}

    task = await dispatch_background_subagent(
        mw,
        graphs,
        subagent_type="general-purpose",
        description="desc",
        prompt="do it",
        parent_thread_id="parent-t",
        run_id="run-xyz",
        owner_task_id="owner-1",
    )

    # --- registration invariants ---
    assert mw.registry.task_count == 1
    assert task.owner_task_id == "owner-1"
    assert task.spawned_run_id == "run-xyz"
    assert task.tool_call_id.startswith("dispatch-")
    assert task.subagent_type == "general-purpose"
    # Ledger-admitted under the fence — cause "init", NULL parent (the
    # no-report-back shape), the dispatch tool_call_id as the launch id.
    # The no-ledger fake returns "", so task_run_id stays None here.
    assert len(mw.admissions) == 1
    admission = mw.admissions[0]
    assert admission["task"] is task
    assert admission["cause"] == "init"
    assert admission["parent_run_id"] is None
    assert admission["launch_tool_call_id"] == task.tool_call_id
    assert admission["description"] == "desc"
    assert task.task_run_id is None
    # The run opener carries the child's launching prompt.
    assert mw.openers == [(task.task_id, "do it")]

    # --- the spawned asyncio task runs to completion carrying the Command ---
    result = await task.asyncio_task
    assert result["success"] is True
    cmd = result["result"]
    assert isinstance(cmd, Command)
    tool_msg = cmd.update["messages"][0]
    assert isinstance(tool_msg, ToolMessage)
    assert tool_msg.content == "dispatched result"
    assert tool_msg.tool_call_id == task.tool_call_id

    # --- driven under the per-task namespace ---
    conf = graph.received_config["configurable"]
    assert conf["checkpoint_ns"] == f"task:{task.task_id}"
    assert conf["thread_id"] == "parent-t"


@pytest.mark.asyncio
async def test_dispatch_stamps_ledger_run_id_when_admitted():
    """A ledgered admission's task_run_id lands on the task — the stamp that
    routes its captured events onto the live v2 run stream."""
    mw = FakeMiddleware()
    mw.admission_result = "run-uuid-1"
    graphs = {"research": _RecordingGraph()}

    task = await dispatch_background_subagent(
        mw,
        graphs,
        subagent_type="research",
        description="d",
        prompt="p",
        parent_thread_id="t",
        run_id="r1",
    )

    assert task.task_run_id == "run-uuid-1"
    await task.asyncio_task  # drain to avoid a pending task at teardown


@pytest.mark.asyncio
async def test_dispatch_admission_rejection_aborts_before_spawn():
    """A refused admission raises SpawnError carrying the refusal
    reason and settles the task never-started: no writer, no opener, nothing
    for a collector to claim."""
    mw = FakeMiddleware()
    mw.admission_refusal = spawn_mod.TaskRunRefused("slot busy")
    graphs = {"research": _RecordingGraph()}

    with pytest.raises(spawn_mod.SpawnError) as exc_info:
        await dispatch_background_subagent(
            mw,
            graphs,
            subagent_type="research",
            description="d",
            prompt="p",
            parent_thread_id="t",
            run_id="r1",
        )

    assert "slot busy" in str(exc_info.value)
    assert mw.openers == []
    # The registered entry is settled inert (never-started).
    task = mw.admissions[0]["task"]
    assert task.terminal_status == "never_started"
    assert task.error == "run admission rejected"
    assert task.asyncio_task is None


@pytest.mark.asyncio
async def test_a_refused_dispatch_leaves_nothing_behind_in_the_registry():
    """This path mints its own tool_call_id and raises instead of returning it,
    so a settled entry left registered is one nobody can name — and with no
    writer, capture or usage no collector ever claims it away. Its prompt would
    stay resident for the life of the thread, once per refusal, and a ledger
    outage refuses every dispatch a workflow makes.
    """
    mw = FakeMiddleware()
    mw.admission_refusal = spawn_mod.TaskRunRefused("slot busy")
    graphs = {"research": _RecordingGraph()}

    for _ in range(3):
        with pytest.raises(spawn_mod.SpawnError):
            await dispatch_background_subagent(
                mw,
                graphs,
                subagent_type="research",
                description="d",
                prompt="P" * 4096,
                parent_thread_id="t",
                run_id="r1",
            )

    assert len(mw.admissions) == 3  # each attempt did register and refuse
    assert await mw.registry.get_all_tasks() == []


@pytest.mark.asyncio
async def test_a_dispatch_that_spawns_keeps_its_entry():
    """The eviction above is keyed on the raise, not on the settle: an entry
    whose handle reached the caller is the caller's to read afterwards."""
    mw = FakeMiddleware()
    graphs = {"research": _RecordingGraph()}

    task = await dispatch_background_subagent(
        mw,
        graphs,
        subagent_type="research",
        description="d",
        prompt="p",
        parent_thread_id="t",
        run_id="r1",
    )

    assert mw.registry.get_by_tool_call_id(task.tool_call_id) is task
    await task.asyncio_task  # drain to avoid a pending task at teardown


@pytest.mark.asyncio
async def test_dispatch_reports_a_fence_refusal_in_its_own_words():
    """A refusal at the fence and one at the ledger are different failures to
    whoever reads the raised message, so the fence keeps its own wording — and
    settles under its own reason."""
    mw = FakeMiddleware()
    mw.admission_refusal = spawn_mod.NamespaceUnfenced("unused here")
    graphs = {"research": _RecordingGraph()}

    with pytest.raises(spawn_mod.SpawnError) as exc_info:
        await dispatch_background_subagent(
            mw,
            graphs,
            subagent_type="research",
            description="d",
            prompt="p",
            parent_thread_id="t",
            run_id="r1",
        )

    assert "could not fence checkpoint namespace" in str(exc_info.value)
    task = mw.admissions[0]["task"]
    assert task.terminal_status == "never_started"
    assert task.error == "namespace fence unavailable"
    assert task.asyncio_task is None


@pytest.mark.asyncio
async def test_dispatch_uses_explicit_tool_call_id_and_defaults_run_id():
    """An explicit tool_call_id is used verbatim; a None run_id leaves
    spawned_run_id unset (registry falls back to its current_run_id)."""
    mw = FakeMiddleware()
    graphs = {"research": _RecordingGraph()}

    task = await dispatch_background_subagent(
        mw,
        graphs,
        subagent_type="research",
        description="d",
        prompt="p",
        parent_thread_id="t",
        run_id=None,
        tool_call_id="my-explicit-id",
    )

    assert task.tool_call_id == "my-explicit-id"
    assert task.spawned_run_id is None
    await task.asyncio_task  # drain to avoid a pending task at teardown


@pytest.mark.asyncio
async def test_dispatch_context_vars_set_in_child_not_leaked_to_caller():
    """The per-task ContextVars are set inside the spawned child's copied
    context; the caller's context is untouched before and after dispatch."""
    mw = FakeMiddleware()
    graphs = {"research": _RecordingGraph()}

    before_tcid = current_background_tool_call_id.get()
    before_agent = current_background_agent_id.get()

    task = await dispatch_background_subagent(
        mw,
        graphs,
        subagent_type="research",
        description="d",
        prompt="p",
        parent_thread_id="t",
        run_id=None,
    )
    await task.asyncio_task

    assert current_background_tool_call_id.get() == before_tcid
    assert current_background_agent_id.get() == before_agent


# ---------------------------------------------------------------------------
# SubagentDispatcher
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_subagent_dispatcher_dispatch_passes_through(monkeypatch):
    """``.dispatch()`` forwards the bound middleware / graphs / thread_id (as
    ``parent_thread_id``) plus the call kwargs to the module-level dispatch,
    and returns its result."""
    mw = FakeMiddleware()
    graphs = {"research": object()}
    sentinel = BackgroundTask(
        tool_call_id="tc",
        task_id="t",
        description="d",
        prompt="p",
        subagent_type="research",
    )
    captured: dict = {}

    async def fake_dispatch(middleware, subagent_graphs, **kwargs):
        captured["middleware"] = middleware
        captured["graphs"] = subagent_graphs
        captured["kwargs"] = kwargs
        return sentinel

    monkeypatch.setattr(dispatch_mod, "dispatch_background_subagent", fake_dispatch)

    dispatcher = SubagentDispatcher(mw, graphs, "thread-42")
    assert dispatcher.registry is mw.registry
    out = await dispatcher.dispatch(
        subagent_type="research",
        description="desc",
        prompt="prm",
        run_id="r9",
        owner_task_id="own",
        base_configurable={"k": "v"},
    )

    assert out is sentinel
    assert captured["middleware"] is mw
    assert captured["graphs"] is graphs
    kwargs = captured["kwargs"]
    assert kwargs["subagent_type"] == "research"
    assert kwargs["description"] == "desc"
    assert kwargs["prompt"] == "prm"
    assert kwargs["parent_thread_id"] == "thread-42"
    assert kwargs["run_id"] == "r9"
    assert kwargs["owner_task_id"] == "own"
    assert kwargs["base_configurable"] == {"k": "v"}
