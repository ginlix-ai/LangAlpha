from __future__ import annotations

import asyncio
import types
from typing import Any

import pytest

from ptc_agent.agent.middleware.background_subagent.registry import (
    BackgroundTaskRegistry,
)
from ptc_agent.agent.middleware.background_subagent.spawn import NamespaceUnfenced
from ptc_agent.agent.middleware.background_subagent.workflow import tool as tool_module
from ptc_agent.agent.middleware.background_subagent.workflow.engine import (
    MAX_DESCRIPTION_CHARS,
)
from ptc_agent.agent.middleware.background_subagent.workflow.tool import (
    _script_path_is_unreadable,
    create_run_workflow_tool,
)
from ptc_agent.core.paths import MEMO_USER_DIR, MEMORY_USER_DIR
from src.config.models import WorkflowOrchestrationConfig

from .conftest import FakeBackend, workflow_script


def _script(name: str = "demo", body: str = "return args;") -> str:
    return workflow_script(body, name=name)


class FakeMiddleware:
    """No-fence, no-ledger seam for the run-task admission pipeline."""

    def __init__(self, registry: BackgroundTaskRegistry) -> None:
        self.registry = registry
        self.namespace_owner = None
        self.checkpointer = None
        self.admissions: list[dict[str, Any]] = []

    async def admit_task_run(self, task: Any, **kwargs: Any) -> str:
        self.admissions.append({"task": task, **kwargs})
        return ""

    async def _append_run_opener(self, task: Any, prompt: str) -> None:
        pass


class FakeDispatcher:
    def __init__(self) -> None:
        self.middleware = FakeMiddleware(BackgroundTaskRegistry())

    @property
    def registry(self) -> BackgroundTaskRegistry:
        return self.middleware.registry


class CapturingDriver:
    specs: list[Any] = []

    def __init__(self, spec: Any) -> None:
        self.spec = spec
        self.specs.append(spec)

    async def run(self, _request: Any = None) -> str:
        return "unused"


class FakeStore:
    """Workflow tier seam. A ``str`` entry is script text; a ``dict`` is the
    raw stored envelope, so a row can be given a shape the reader must reject."""

    def __init__(self, workflows: dict[str, Any]) -> None:
        self.workflows = workflows
        self.calls: list[tuple[tuple[str, str], str]] = []

    async def aget(self, namespace: tuple[str, str], key: str) -> Any:
        self.calls.append((namespace, key))
        value = self.workflows.get(key)
        if value is None:
            return None
        return types.SimpleNamespace(
            value={"content": value} if isinstance(value, str) else value
        )


class FakePrebuilt:
    def __init__(self, workflows: dict[str, str]) -> None:
        self.workflows = workflows

    def get(self, name: str) -> str | None:
        return self.workflows.get(name)

    def names(self) -> list[str]:
        return sorted(self.workflows)


@pytest.fixture(autouse=True)
def _tool_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    CapturingDriver.specs = []
    monkeypatch.setattr(tool_module, "WorkflowDriver", CapturingDriver)

    async def _spawn(mw: Any, task: Any, runner: Any, **kwargs: Any) -> None:
        pass

    monkeypatch.setattr(tool_module, "spawn_task_writer", _spawn)
    monkeypatch.setattr(
        tool_module,
        "get_config",
        lambda: {
            "metadata": {"run_id": "launch-1"},
            "configurable": {
                "user_id": "user-1",
                "workspace_id": "workspace-1",
            },
        },
    )
    monkeypatch.setattr(
        "src.config.settings.get_workflow_orchestration_config",
        lambda: WorkflowOrchestrationConfig(),
    )


def _make_tool(
    backend: FakeBackend,
    *,
    store: Any = None,
    prebuilt: Any = None,
    dispatcher: FakeDispatcher | None = None,
) -> Any:
    return create_run_workflow_tool(
        dispatcher=dispatcher or FakeDispatcher(),
        backend=backend,
        thread_id="thread-1",
        short_thread_id="short-1",
        store=store,
        user_id="user-1",
        prebuilt_workflows=prebuilt,
    )


@pytest.mark.asyncio
async def test_exactly_one_script_source_is_required() -> None:
    workflow_tool = _make_tool(FakeBackend())
    none = await workflow_tool.coroutine()
    multiple = await workflow_tool.coroutine(
        script=_script(), workflow="saved"
    )
    assert "exactly one" in none
    assert "exactly one" in multiple


@pytest.mark.asyncio
async def test_inline_workflow_snapshots_and_builds_v2_spec() -> None:
    backend = FakeBackend()
    workflow_tool = _make_tool(backend)

    message = await workflow_tool.coroutine(
        script=_script(), params={"ticker": "AAPL"}
    )

    assert "Workflow: demo" in message
    spec = CapturingDriver.specs[0]
    assert spec.meta.name == "demo"
    assert spec.script_args == {"ticker": "AAPL"}
    assert spec.run_task.spawned_run_id == "launch-1"
    assert spec.run_task.subagent_type == "workflow"
    assert spec.run_task.owner_task_id is None
    assert any(path.endswith("/workflow.js") for path in backend.writes)
    assert any(path.endswith("/args.json") for path in backend.writes)


@pytest.mark.asyncio
async def test_tool_invokes_through_langchain_schema_layer() -> None:
    """Invoke via ainvoke (not .coroutine) so the inferred arg schema is
    exercised — langchain reserves the param name 'args', which is why the
    model-facing param is 'params'."""
    workflow_tool = _make_tool(FakeBackend())

    message = await workflow_tool.ainvoke(
        {
            "name": "RunWorkflow",
            "type": "tool_call",
            "id": "tc-run-workflow",
            "args": {
                "script": _script(),
                "params": {"ticker": "AAPL"},
            },
        }
    )

    assert "Workflow run started" in message.content
    assert CapturingDriver.specs[0].script_args == {"ticker": "AAPL"}
    # The injected tool_call_id becomes the run task's registration id.
    assert CapturingDriver.specs[0].run_task.tool_call_id == "tc-run-workflow"
    field_names = set(workflow_tool.args_schema.model_fields)
    assert "params" in field_names
    assert not {f for f in field_names if f.startswith("v__")}
    # The launch artifact binds the frontend card to the run's task lane.
    # action is 'workflow' — NOT 'init': init/resume artifacts make the
    # history TaskLaneProjector claim a checkpoint namespace this task
    # doesn't have.
    artifact = message.additional_kwargs["task_artifact"]
    run_task = CapturingDriver.specs[0].run_task
    assert artifact["task_id"] == run_task.task_id
    assert artifact["action"] == "workflow"
    assert artifact["type"] == "workflow"
    assert artifact["workflow"] == "demo"


@pytest.mark.asyncio
async def test_an_oversized_description_argument_is_bounded_like_the_declared_one() -> (
    None
):
    """`meta.description` is capped at extraction because it is copied into
    the registry, the ledger, the launch stream and the task artifact. The
    tool argument overrides it and reaches all four, so it answers to the same
    cap — otherwise the bound is only as good as the model's restraint."""
    workflow_tool = _make_tool(FakeBackend())

    message = await workflow_tool.ainvoke(
        {
            "name": "RunWorkflow",
            "type": "tool_call",
            "id": "tc-long-description",
            "args": {"script": _script(), "description": "D" * 40_000},
        }
    )

    run_task = CapturingDriver.specs[0].run_task
    assert len(run_task.description) == MAX_DESCRIPTION_CHARS
    assert len(message.additional_kwargs["task_artifact"]["description"]) == (
        MAX_DESCRIPTION_CHARS
    )
    assert len(message.additional_kwargs["task_artifact"]["prompt"]) == (
        MAX_DESCRIPTION_CHARS
    )


@pytest.mark.asyncio
async def test_script_path_guards_path_but_not_javascript_content() -> None:
    """The guard reads the path, never the script — a workflow whose JS merely
    mentions a store-backed path still runs."""
    source = _script(body="const p = '.agents/user/memories/x'; return p;")
    backend = FakeBackend({"workflows/demo.js": source})
    workflow_tool = _make_tool(backend)

    allowed = await workflow_tool.coroutine(script_path="workflows/demo.js")

    assert "Workflow run started" in allowed


@pytest.mark.parametrize(
    "blocked_path",
    [
        ".agents/user/memory/demo.js",
        ".agents/workspace/memory/demo.js",
        ".agents/user/memo/demo.js",
        "work/_internal/demo.js",
    ],
)
@pytest.mark.asyncio
async def test_an_unreadable_script_path_is_refused_with_routes_that_exist(
    blocked_path: str,
) -> None:
    """The refusal has to be RunWorkflow's own: this tool takes a script inline
    or by name, so advice to read the file and paste its content — correct for
    the sandbox code tools — sends the model down a route that isn't here."""
    workflow_tool = _make_tool(FakeBackend())

    reply = await workflow_tool.coroutine(script_path=blocked_path)

    assert reply.startswith(f"Error: script_path '{blocked_path}'")
    assert "'script'" in reply
    assert ".agents/workflows/<name>.js" in reply
    assert "ExecuteCode" not in reply
    # Refused before the read, so an unreadable path never reaches the backend
    # (which would have answered FileNotFoundError for this one).
    assert "Could not read" not in reply


@pytest.mark.asyncio
async def test_saved_workflow_precedes_prebuilt() -> None:
    saved = _script("named", "return 'saved';")
    store = FakeStore({"named.js": saved})
    prebuilt = FakePrebuilt({"named": _script("named", "return 'prebuilt';")})
    workflow_tool = _make_tool(FakeBackend(), store=store, prebuilt=prebuilt)

    message = await workflow_tool.coroutine(workflow="named")

    assert "Workflow: named" in message
    assert CapturingDriver.specs[0].script == saved
    assert store.calls == [(('user-1', 'workflows'), "named.js")]


@pytest.mark.asyncio
async def test_named_run_refuses_a_script_declaring_a_different_meta_name() -> None:
    """Announcement, frames and card all read meta.name, so a script saved as
    one name and declaring another runs as the other everywhere — while
    ``list_workflows`` reports it invalid."""
    store = FakeStore({"named.js": _script("other")})
    workflow_tool = _make_tool(FakeBackend(), store=store)

    message = await workflow_tool.coroutine(workflow="named")

    assert "declares meta.name 'other'" in message
    assert CapturingDriver.specs == []


@pytest.mark.asyncio
async def test_a_malformed_saved_row_never_resolves_to_the_builtin() -> None:
    """Absent and unreadable are different: falling through on unreadable runs
    the shipped script under a name the user saved their own script as."""
    store = FakeStore({"named.js": {"encoding": "utf-8"}})
    prebuilt = FakePrebuilt({"named": _script("named")})
    workflow_tool = _make_tool(FakeBackend(), store=store, prebuilt=prebuilt)

    message = await workflow_tool.coroutine(workflow="named")

    assert "could not read workflow 'named'" in message
    assert CapturingDriver.specs == []


@pytest.mark.asyncio
async def test_a_stalled_store_refuses_instead_of_wedging_the_turn(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Name resolution runs inline in the tool body, before the run exists, so
    ``run_timeout`` is not standing behind this read — an unbounded one would
    hold the tool call, and the turn making it, for as long as the store hangs.
    """

    class StalledStore(FakeStore):
        async def aget(self, namespace: tuple[str, str], key: str) -> Any:
            await asyncio.sleep(3600)

    monkeypatch.setattr(tool_module, "_STORE_OP_TIMEOUT_S", 0.05)
    prebuilt = FakePrebuilt({"named": _script("named")})
    workflow_tool = _make_tool(
        FakeBackend(), store=StalledStore({}), prebuilt=prebuilt
    )

    message = await asyncio.wait_for(workflow_tool.coroutine(workflow="named"), 5)

    # Refused, not resolved: a timeout that fell through would run the shipped
    # script under a name the user may have saved their own script as.
    assert "could not read workflow 'named'" in message
    assert CapturingDriver.specs == []


@pytest.mark.asyncio
async def test_prebuilt_fallback_and_unknown_workflow_message() -> None:
    prebuilt = FakePrebuilt({"alpha": _script("alpha")})
    workflow_tool = _make_tool(FakeBackend(), prebuilt=prebuilt)

    found = await workflow_tool.coroutine(workflow="alpha")
    missing = await workflow_tool.coroutine(workflow="missing")

    assert "Workflow: alpha" in found
    assert "Unknown workflow 'missing'." in missing
    assert "Available pre-built workflows: alpha" in missing
    assert ".agents/workflows/<name>.js" in missing
    # This wording alone used to read as prose, leaving the launch card it
    # could not settle spinning "Running". Invariant test below.
    assert missing.startswith("Error: ")


@pytest.mark.asyncio
async def test_invalid_script_size_and_syntax_are_rejected(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    caps = WorkflowOrchestrationConfig().model_copy(
        update={"max_script_bytes": 200}
    )
    monkeypatch.setattr(
        "src.config.settings.get_workflow_orchestration_config", lambda: caps
    )
    workflow_tool = _make_tool(FakeBackend())

    oversized = await workflow_tool.coroutine(script=_script() + "x" * 300)
    monkeypatch.setattr(
        "src.config.settings.get_workflow_orchestration_config",
        lambda: WorkflowOrchestrationConfig(),
    )
    bad_script = await workflow_tool.coroutine(script="return 1;")

    assert "script exceeds" in oversized
    assert "Invalid workflow script" in bad_script


@pytest.mark.asyncio
async def test_snapshot_failures_are_nonfatal() -> None:
    workflow_tool = _make_tool(FakeBackend(fail_writes=True))

    message = await workflow_tool.coroutine(script=_script())

    assert "Workflow run started" in message
    assert CapturingDriver.specs[0].meta.name == "demo"


class FakeRunLedger:
    """Cap-check seam: the boundary adapter is one async list method."""

    def __init__(
        self,
        rows: list[dict[str, Any]] | None = None,
        error: Exception | None = None,
    ) -> None:
        self.rows = rows or []
        self.error = error
        self.calls = 0

    async def list_open_workflow_runs(self) -> list[dict[str, Any]]:
        self.calls += 1
        if self.error is not None:
            raise self.error
        return self.rows


@pytest.mark.asyncio
async def test_ledgered_thread_without_a_checkpointer_refuses_to_run() -> None:
    """A ledgered run answers TaskOutput from its archived result, and only a
    checkpointer can hold one. Refusing at admission beats admitting a run
    whose result becomes unreadable the moment its turn ends."""
    dispatcher = FakeDispatcher()
    dispatcher.registry.run_ledger = FakeRunLedger()
    assert dispatcher.middleware.checkpointer is None
    workflow_tool = _make_tool(FakeBackend(), dispatcher=dispatcher)

    message = await workflow_tool.coroutine(script=_script())

    assert "no checkpointer is configured" in message
    # Refused before any side effect: no registration, no spawn.
    assert await dispatcher.registry.get_all_tasks() == []
    assert CapturingDriver.specs == []


@pytest.mark.asyncio
async def test_run_cap_counts_open_ledger_rows_across_workers() -> None:
    """The ledger is the cross-worker authority: an empty local registry
    must not admit a run when other workers hold the thread's quota."""
    dispatcher = FakeDispatcher()
    ledger = FakeRunLedger(
        rows=[{"task_id": "task_aaa111"}, {"task_id": "task_bbb222"}]
    )
    dispatcher.registry.run_ledger = ledger
    # A ledgered run must be able to archive its result, so the two travel
    # together — a ledger without a checkpointer is refused at admission.
    dispatcher.middleware.checkpointer = object()
    workflow_tool = _make_tool(FakeBackend(), dispatcher=dispatcher)

    message = await workflow_tool.coroutine(script=_script())

    assert "2 workflow run(s) already active" in message
    assert "task_aaa111" in message and "task_bbb222" in message
    assert ledger.calls == 1
    assert CapturingDriver.specs == []
    # Registration precedes the cap check (idempotency first), so the refused
    # run leaves an entry behind — and its id went nowhere, so nothing else
    # would ever claim or evict it. It has to be dropped here.
    assert await dispatcher.registry.get_all_tasks() == []


@pytest.mark.asyncio
async def test_run_cap_local_fallback_counts_only_pending_workflows() -> None:
    dispatcher = FakeDispatcher()
    registry = dispatcher.registry
    for i in range(2):
        await registry.register(
            tool_call_id=f"wf-{i}",
            description="d",
            prompt="p",
            subagent_type="workflow",
        )
    subagent = await registry.register(
        tool_call_id="sub-1",
        description="d",
        prompt="p",
        subagent_type="research",
    )
    workflow_tool = _make_tool(FakeBackend(), dispatcher=dispatcher)

    message = await workflow_tool.coroutine(script=_script())

    assert "2 workflow run(s) already active" in message
    assert subagent.task_id not in message
    assert CapturingDriver.specs == []


@pytest.mark.asyncio
async def test_run_cap_ledger_read_failure_refuses_instead_of_counting_locally() -> None:
    """Falling back to the local registry here defeats the cap outright: a
    ledgered deployment is multi-worker, so this process's view is a fraction
    of the truth rather than a degraded copy of it, and every worker would
    admit a fresh quota. Admission is the one place refusing is cheap — the
    re-read is the caller's next message, not a stuck state.
    """
    dispatcher = FakeDispatcher()
    ledger = FakeRunLedger(error=RuntimeError("db down"))
    dispatcher.registry.run_ledger = ledger
    dispatcher.middleware.checkpointer = object()
    workflow_tool = _make_tool(FakeBackend(), dispatcher=dispatcher)

    message = await workflow_tool.coroutine(script=_script())

    assert "could not read the workflow run ledger" in message
    assert ledger.calls == 1
    assert CapturingDriver.specs == []
    # The refusal must not itself hold the slot it declined to grant, or the
    # retry it invites would be blocked by its own predecessor.
    tasks = await dispatcher.registry.get_all_tasks()
    assert [t for t in tasks if t.is_pending] == []


def _reads_as_a_launch_failure(reply: Any) -> bool:
    """The client's rule for a reply carrying no artifact — ``isToolResultFailure``
    in ``web/src/pages/ChatAgent/session/subagents/subagentStatus.ts``, whose
    ``!artifact`` conjunct every refusal satisfies by answering bare text."""
    return isinstance(reply, str) and reply.strip()[:5].lower() == "error"


@pytest.mark.asyncio
async def test_every_refusal_reads_as_a_failure_to_the_launch_card(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A refused launch has nothing but this text to settle its card with.

    Nothing started, so no task artifact binds the card to a run and no
    channel will ever close — the client stamps 'error' from the reply's own
    opening word. One refusal phrased as ordinary prose is enough to leave a
    card spinning "Running" for the life of the thread, live and on reload,
    so the rule is asserted over every refusal rather than the newest one.
    """

    class _FailingStore:
        async def aget(self, *_: Any) -> Any:
            raise RuntimeError("store down")

    capped = FakeDispatcher()
    capped.registry.run_ledger = FakeRunLedger(
        rows=[{"task_id": "task_aaa111"}, {"task_id": "task_bbb222"}]
    )
    capped.middleware.checkpointer = object()
    uncheckpointed = FakeDispatcher()
    uncheckpointed.registry.run_ledger = FakeRunLedger()

    plain = _make_tool(FakeBackend())
    refusals = {
        "no source": plain.coroutine(),
        "two sources": plain.coroutine(script=_script(), workflow="alpha"),
        "unknown workflow": _make_tool(
            FakeBackend(), prebuilt=FakePrebuilt({})
        ).coroutine(workflow="missing"),
        "unreadable store": _make_tool(
            FakeBackend(), store=_FailingStore()
        ).coroutine(workflow="named"),
        "managed script_path": plain.coroutine(
            script_path=f"{MEMORY_USER_DIR}/x.js"
        ),
        "unreadable script_path": plain.coroutine(script_path="work/nope.js"),
        "empty script_path": _make_tool(
            FakeBackend({"work/empty.js": ""})
        ).coroutine(script_path="work/empty.js"),
        "invalid script": plain.coroutine(script="return 1;"),
        "meta name mismatch": _make_tool(
            FakeBackend(), prebuilt=FakePrebuilt({"alpha": _script("other")})
        ).coroutine(workflow="alpha"),
        "no checkpointer": _make_tool(
            FakeBackend(), dispatcher=uncheckpointed
        ).coroutine(script=_script()),
        "run cap reached": _make_tool(
            FakeBackend(), dispatcher=capped
        ).coroutine(script=_script()),
    }
    replies = {label: await coroutine for label, coroutine in refusals.items()}

    # Reads its cap when the script is already in hand, so it needs its own
    # config rather than a seat in the table above.
    monkeypatch.setattr(
        "src.config.settings.get_workflow_orchestration_config",
        lambda: WorkflowOrchestrationConfig().model_copy(
            update={"max_script_bytes": 200}
        ),
    )
    replies["oversized script"] = await plain.coroutine(
        script=_script() + "x" * 300
    )

    unsettled = {
        label: reply
        for label, reply in replies.items()
        if not _reads_as_a_launch_failure(reply)
    }
    assert unsettled == {}
    assert CapturingDriver.specs == []


@pytest.mark.asyncio
async def test_replayed_call_reattaches_before_cap_check() -> None:
    """A checkpoint re-execution of a live call answers idempotently even
    when the thread sits at its cap — its own run IS part of the quota."""
    dispatcher = FakeDispatcher()
    registry = dispatcher.registry
    existing = await registry.register(
        tool_call_id="wf-replay",
        description="d",
        prompt="p",
        subagent_type="workflow",
    )
    await registry.register(
        tool_call_id="wf-other",
        description="d",
        prompt="p",
        subagent_type="workflow",
    )
    workflow_tool = _make_tool(FakeBackend(), dispatcher=dispatcher)

    message = await workflow_tool.coroutine(
        script=_script(), tool_call_id="wf-replay"
    )

    assert "already active" in message.content
    assert existing.task_id in message.content
    # The reattach carries the artifact a fresh launch does: the recovered run
    # is what the card, the history projector and replay liveness bind to.
    assert message.additional_kwargs["task_artifact"]["task_id"] == existing.task_id
    assert CapturingDriver.specs == []


class RacingRunLedger:
    """Ledger seam with the two round-trips a real one has: the SELECT
    suspends, and an admitted row is invisible to other readers until the
    INSERT lands.

    The barrier pins the worst-case interleaving — every caller counts before
    any admits — instead of leaving it to how the compile threads happen to
    land. It breaks as soon as one caller gives up waiting, so serialized
    callers fall straight through rather than deadlocking.
    """

    def __init__(self, parties: int) -> None:
        self.rows: list[dict[str, Any]] = []
        self._barrier = asyncio.Barrier(parties)

    async def list_open_workflow_runs(self) -> list[dict[str, Any]]:
        try:
            await asyncio.wait_for(self._barrier.wait(), timeout=0.25)
        except (TimeoutError, asyncio.BrokenBarrierError):
            pass
        return list(self.rows)

    async def record_admission(self, task_id: str) -> None:
        await asyncio.sleep(0)
        self.rows.append({"task_id": task_id})


@pytest.mark.asyncio
async def test_concurrent_launches_cannot_exceed_the_per_thread_run_cap() -> None:
    """Two RunWorkflow calls in one assistant message are gathered by langgraph's
    ToolNode, so they run concurrently in this process — and the ledger read is
    a real round-trip. Unless counting and admitting are one critical section,
    every caller counts the thread empty and admits itself."""
    dispatcher = FakeDispatcher()
    cap = WorkflowOrchestrationConfig().max_runs_per_thread
    ledger = RacingRunLedger(parties=cap + 2)
    dispatcher.registry.run_ledger = ledger
    dispatcher.middleware.checkpointer = object()

    async def _admit(task: Any, **kwargs: Any) -> str:
        await ledger.record_admission(task.task_id)
        dispatcher.middleware.admissions.append({"task": task, **kwargs})
        return ""

    dispatcher.middleware.admit_task_run = _admit
    workflow_tool = _make_tool(FakeBackend(), dispatcher=dispatcher)

    messages = await asyncio.gather(
        *(
            workflow_tool.coroutine(script=_script(), tool_call_id=f"wf-{i}")
            for i in range(cap + 2)
        )
    )

    assert len(dispatcher.middleware.admissions) == cap
    assert len(ledger.rows) == cap
    refused = [m for m in messages if "already active" in str(m)]
    assert len(refused) == 2
    assert all(f"per-thread cap is {cap}" in str(m) for m in refused)


@pytest.mark.asyncio
async def test_a_refused_admission_names_its_reason_and_drops_the_entry() -> None:
    """The tool's reply is the model's only signal that nothing started, so it
    has to carry the refusal's own reason — and the entry goes with it. A
    refused id is never returned, and a never-started entry has no writer,
    capture or usage for a collector to claim, so nothing else evicts it."""
    dispatcher = FakeDispatcher()

    async def _refuse(task: Any, **kwargs: Any) -> str:
        raise NamespaceUnfenced(
            "its checkpoint namespace could not be fenced. Try again"
        )

    dispatcher.middleware.admit_task_run = _refuse
    workflow_tool = _make_tool(FakeBackend(), dispatcher=dispatcher)

    message = await workflow_tool.coroutine(script=_script(), tool_call_id="wf-1")

    assert message.startswith("Error: could not start Task-")
    assert message.endswith(
        "— its checkpoint namespace could not be fenced. Try again."
    )
    assert await dispatcher.registry.get_all_tasks() == []
    assert CapturingDriver.specs == []


@pytest.mark.parametrize(
    ("path", "unreadable"),
    [
        ("workflows/company_internal.js", False),
        ("notes/my_memory_notes.js", False),
        ("_internal/x.js", True),
        ("nested/_internal/x.js", True),
        (f"{MEMORY_USER_DIR}/x.js", True),
        (f"{MEMO_USER_DIR}/x.js", True),
    ],
)
def test_script_paths_are_refused_by_segment_not_substring(
    path: str, unreadable: bool
) -> None:
    """Both vocabularies name directories. Matching them anywhere in the text
    refuses ordinary files whose name merely contains one, and the refusal
    lands before the backend is ever asked whether the file exists.
    """
    assert _script_path_is_unreadable(path) is unreadable


class _PathBackend:
    """The sandbox path contract the guard has to agree with: normalize maps
    any spelling onto the working dir, virtualize strips it back off."""

    WORK_DIR = "/home/workspace"

    def normalize_path(self, path: str) -> str:
        if path.startswith(self.WORK_DIR):
            return path
        return f"{self.WORK_DIR}/{path.lstrip('/')}"

    def virtualize_path(self, path: str) -> str:
        return path[len(self.WORK_DIR):] if path.startswith(self.WORK_DIR) else path


@pytest.mark.parametrize(
    "path",
    [
        f"{MEMO_USER_DIR}/x.js",
        f"/home/workspace/{MEMO_USER_DIR}/x.js",
        f"/{MEMORY_USER_DIR}/x.js",
        "/home/workspace/nested/_internal/x.js",
    ],
)
def test_a_managed_path_is_refused_however_it_is_spelled(path: str) -> None:
    """The router picks a mount from the normalized path, so a guard reading
    only the caller's text refuses the relative spelling while admitting the
    absolute spelling of the very same file.
    """
    forms = tool_module._script_path_forms(_PathBackend(), path)

    assert any(_script_path_is_unreadable(form) for form in forms)


def test_canonicalization_cannot_admit_what_the_raw_check_refused() -> None:
    """A backend without the path helpers degrades to the raw check rather
    than opening a hole — the forms are a union, never a replacement."""

    class _NoHelpers:
        pass

    forms = tool_module._script_path_forms(_NoHelpers(), f"{MEMO_USER_DIR}/x.js")

    assert forms == {f"{MEMO_USER_DIR}/x.js"}
    assert any(_script_path_is_unreadable(form) for form in forms)


def test_an_ordinary_workspace_script_survives_canonicalization() -> None:
    """Normalizing must not turn a plain workspace path into a managed one."""
    forms = tool_module._script_path_forms(_PathBackend(), "workflows/company_internal.js")

    assert not any(_script_path_is_unreadable(form) for form in forms)
