"""Failure ToolMessages out of the Task tool must carry ``status="error"``.

``ToolMessage.status`` defaults to ``"success"`` and now rides the wire, where
the frontend reads it. A refused launch opens no run and no channel, so nothing
will ever arrive to settle its card: an unstamped failure leaves the card
spinning for the life of the thread.

A launch a stop refused mid-setup is the exception: a stop is a cancellation,
so its reply stays a success and settles the card through its artifact.
"""

from __future__ import annotations

import ast
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from ptc_agent.agent.middleware.background_subagent import task_actions
from ptc_agent.agent.middleware.background_subagent.registry import (
    BackgroundTask,
    BackgroundTaskRegistry,
)
from ptc_agent.agent.middleware.background_subagent.spawn import (
    SpawnStoppedError,
    TaskRunRefused,
)


class _RefusingMiddleware:
    """Duck-typed middleware whose ledger admission always refuses."""

    def __init__(self) -> None:
        self.registry = BackgroundTaskRegistry()
        self.namespace_owner = None
        self._resume_claims: set[str] = set()

    async def admit_task_run(self, task: Any, **kwargs: Any) -> str:
        raise TaskRunRefused("slot busy")


@pytest.mark.asyncio
async def test_a_refused_task_launch_is_stamped_an_error():
    mw = _RefusingMiddleware()

    message = await task_actions._handle_init(
        mw,
        None,
        None,
        subagent_type="research",
        description="d",
        prompt="p",
        tool_call_id="call-1",
        current_run_id="run-1",
    )

    assert message.content.startswith("Error: could not start ")
    assert message.status == "error"


def _task() -> BackgroundTask:
    return BackgroundTask(
        tool_call_id="call-1",
        task_id="k7Xm2p",
        description="d",
        prompt="p",
        subagent_type="research",
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "action, ending",
    [
        ("init", "was stopped before it started."),
        ("resume", "was stopped before the resume started."),
    ],
)
async def test_a_stopped_launch_settles_as_stopped_not_failed(
    monkeypatch: pytest.MonkeyPatch, action: str, ending: str
):
    """A stop mid-setup refuses the writer, and the run settles cancelled.

    The reply carries no task artifact, so nothing else settles the launch
    card. The frontend settles the record cancelled from this artifact, which
    an init card shows as "Stopped"; without it the card spins "Running", and
    an error status shows "Failed". A resume card reads "Resumed" either way.
    """

    async def _stopped(*_args: Any, **_kwargs: Any) -> None:
        raise SpawnStoppedError("Task-k7Xm2p was stopped before it started")

    monkeypatch.setattr(task_actions, "spawn_task_writer", _stopped)

    message = await task_actions._spawn_writer(
        _RefusingMiddleware(),
        _task(),
        None,
        None,
        prompt="p",
        description="d",
        tool_call_id="call-1",
        action=action,
    )

    assert message is not None
    assert message.content == f"Background subagent {_task().display_id} {ending}"
    assert message.status == "success"
    assert message.artifact == {"launch": "cancelled"}


@pytest.mark.asyncio
async def test_a_completed_spawn_carries_no_refusal_message(
    monkeypatch: pytest.MonkeyPatch,
):
    """Control: a spawn that completes hands the writer off with no message."""

    async def _spawned(*_args: Any, **_kwargs: Any) -> None:
        return None

    monkeypatch.setattr(task_actions, "spawn_task_writer", _spawned)

    assert (
        await task_actions._spawn_writer(
            _RefusingMiddleware(),
            _task(),
            None,
            None,
            prompt="p",
            description="d",
            tool_call_id="call-1",
            action="init",
        )
        is None
    )


class _TypedMiddleware(_RefusingMiddleware):
    def __init__(self, subagent_types: frozenset[str]) -> None:
        super().__init__()
        self.subagent_types = subagent_types


def _task_request(action: str = "init", subagent_type: str | None = "research") -> Any:
    args = {"action": action, "description": "d", "prompt": "p", "subagent_type": subagent_type}
    if action != "init":
        args["task_id"] = "k7Xm2p"
    return SimpleNamespace(tool_call={"id": "call-1", "args": args}, runtime=None)


@pytest.fixture
def routed(monkeypatch: pytest.MonkeyPatch) -> dict[str, list[str]]:
    """Which handler each action reached, with the real ones stubbed out."""
    reached: dict[str, list[str]] = {"init": [], "update": [], "resume": []}

    def _stub(name: str):
        async def _handle(*_args: Any, **kwargs: Any) -> str:
            reached[name].append(kwargs.get("subagent_type"))
            return name

        return _handle

    for name in reached:
        monkeypatch.setattr(task_actions, f"_handle_{name}", _stub(name))
    return reached


@pytest.mark.asyncio
# None: an omitted type means general-purpose, which this roster lacks.
@pytest.mark.parametrize("subagent_type", ["ghost", None])
async def test_an_init_of_a_type_with_no_graph_runs_in_place_and_registers_nothing(routed, subagent_type):
    mw = _TypedMiddleware(frozenset({"research"}))
    seen = []

    async def handler(request: Any) -> str:
        seen.append(request)
        return "in place"

    request = _task_request(subagent_type=subagent_type)

    assert await task_actions.handle_task_action(mw, request, handler) == "in place"
    assert seen == [request]
    assert routed["init"] == []
    assert not mw.registry._tasks


@pytest.mark.asyncio
@pytest.mark.parametrize(("roster", "subagent_type"), [({"research"}, "research"), ({"general-purpose"}, None)])
async def test_an_init_otherwise_takes_the_background_path(routed, roster, subagent_type):
    async def handler(_request: Any) -> str:
        raise AssertionError("ran in place")

    result = await task_actions.handle_task_action(
        _TypedMiddleware(frozenset(roster)), _task_request(subagent_type=subagent_type), handler
    )

    assert (result, routed["init"]) == ("init", [subagent_type])


@pytest.mark.asyncio
@pytest.mark.parametrize("action", ["update", "resume"])
async def test_an_update_or_resume_is_not_judged_by_the_type_set(routed, action):
    async def handler(_request: Any) -> str:
        raise AssertionError("ran in place")

    result = await task_actions.handle_task_action(
        _TypedMiddleware(frozenset({"research"})), _task_request(action, "ghost"), handler
    )

    assert (result, routed[action]) == (action, ["ghost"])


_SWEPT_FILES = ("task_actions.py", "middleware.py")


def _constant_text(node: ast.AST) -> str:
    """The literal text of an expression, ignoring its interpolated holes.

    An f-string's prefix is a Constant like any other, so a content expression
    built out of runtime values still exposes the wording that decides whether
    it reads as a failure.
    """
    return "".join(
        part.value
        for part in ast.walk(node)
        if isinstance(part, ast.Constant) and isinstance(part.value, str)
    )


def _name_bindings(tree: ast.AST) -> dict[str, str]:
    """Every ``name = <text>`` binding in the module, by name.

    Content assembled into a local and then passed as ``content=msg`` is
    invisible to a sweep that only reads the call site, which is how a
    constructed failure escapes. Names are not scoped here: a collision would
    only widen the sweep, and a false candidate is caught by its own status.
    """
    bindings: dict[str, str] = {}
    for node in ast.walk(tree):
        targets = []
        if isinstance(node, ast.Assign):
            targets = node.targets
        elif isinstance(node, ast.AnnAssign):
            targets = [node.target]
        elif isinstance(node, (ast.NamedExpr,)):
            targets = [node.target]
        for target in targets:
            if isinstance(target, ast.Name) and node.value is not None:
                text = _constant_text(node.value)
                if text:
                    bindings[target.id] = bindings.get(target.id, "") + text
    return bindings


def _reads_as_failure(text: str) -> bool:
    return text.lower().lstrip().startswith("error")


def _error_tool_messages(tree: ast.AST) -> list[ast.Call]:
    bindings = _name_bindings(tree)
    calls = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        func = node.func
        name = func.id if isinstance(func, ast.Name) else getattr(func, "attr", "")
        if name != "ToolMessage":
            continue
        for kw in node.keywords:
            if kw.arg != "content":
                continue
            text = _constant_text(kw.value)
            if isinstance(kw.value, ast.Name):
                text = bindings.get(kw.value.id, text)
            if _reads_as_failure(text):
                calls.append(node)
    return calls


def _unstamped(calls: list[ast.Call]) -> list[int]:
    return [
        call.lineno
        for call in calls
        if not any(
            kw.arg == "status"
            and isinstance(kw.value, ast.Constant)
            and kw.value.value == "error"
            for kw in call.keywords
        )
    ]


@pytest.mark.parametrize("filename", _SWEPT_FILES)
def test_every_error_tool_message_stamps_its_status(filename: str):
    path = Path(task_actions.__file__).parent / filename
    tree = ast.parse(path.read_text())
    calls = _error_tool_messages(tree)
    assert calls, f"no Error-prefixed ToolMessage found in {filename}"
    unstamped = _unstamped(calls)
    assert unstamped == [], f"{filename} lines {unstamped} return a failure as success"


def test_the_sweep_sees_a_failure_whose_content_was_built_elsewhere():
    """Guards the sweep itself: content bound to a local used to slip through.

    A failure assembled before the call site is the shape a literal-only sweep
    walks straight past, so the detector is exercised against one directly.
    """
    source = """
def build(task_id, action):
    reason = f"Error: task_id is required for '{action}' action."
    stamped = f"Error: Task-{task_id} not found."
    fine = f"Dispatched Task-{task_id}."
    return [
        ToolMessage(content=reason, tool_call_id="c"),
        ToolMessage(content=stamped, tool_call_id="c", status="error"),
        ToolMessage(content=fine, tool_call_id="c"),
    ]
"""
    calls = _error_tool_messages(ast.parse(source))

    assert len(calls) == 2, "the built failures are what the sweep must see"
    assert _unstamped(calls) == [7], "only the unstamped failure may be reported"
