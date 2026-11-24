"""A Task that names its own model: resolved before anything is registered,
admitted with that model's facts, and kept on resume through the ledger."""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from langchain_core.messages import ToolMessage

from ptc_agent.config.agent import Admitted, RunOn, TaskAsk, TaskModel, TaskModelRefused
from tests.unit.middleware.test_subagent_run_ledger_admission import (
    FakeLedger,
    FakeOwner,
    _middleware,
    _ok_handler,
    _register_settled_task,
    _request,
)


def _task_model(name="other-model", *, claude_code=None, model=None):
    return TaskModel(
        name=name, display_name=name.title(), client=SimpleNamespace(model=f"{name}-id"),
        guidance="", model=model, claude_code=claude_code,
    )


class _Gate:
    """``resolve`` answers from ``answers`` by the model asked for, None for
    one it lacks (the subagent's own model), and records what it was asked."""

    def __init__(self, answers: dict) -> None:
        self.answers = answers
        self.asked: list[tuple[str, str]] = []

    async def resolve(self, ask, subagent_type):
        self.asked.append((ask.model, subagent_type))
        return self.answers.get(ask.model)


def _gated(mw, answers: dict) -> _Gate:
    mw.task_models = _Gate(answers)
    return mw.task_models


class _LedgerWithLatest(FakeLedger):
    """A ledger whose task's latest run ran on ``latest``, a model its Task
    named, or on its subagent's own where None."""

    def __init__(self, latest: str | None, **kwargs) -> None:
        super().__init__(**kwargs)
        self.latest = latest

    async def get_latest_run(self, task_id: str):
        facts = {"model_override": self.latest} if self.latest else {}
        return {"task_run_id": "run-prev", "status": "completed", "replay_facts": facts}


class _LedgerReadFails(FakeLedger):
    async def get_latest_run(self, task_id: str):
        raise RuntimeError("ledger db down")


def _resume(task, tool_call_id="tc-2", **args):
    return _request(
        {"action": "resume", "task_id": task.task_id, "prompt": "more", **args}, tool_call_id
    )


@pytest.mark.asyncio
async def test_a_task_on_a_model_it_names_is_admitted_with_that_models_facts():
    """Its type runs Claude Code, but this task runs another model, so the
    run is no CLI its computer counts."""
    ledger = FakeLedger(reject="stop here")
    mw = _middleware(FakeOwner(), ledger, run_on={"general-purpose": RunOn(
        model={"id": "x", "provider": "p"}, claude_code=Admitted(cli_computer="c-1"),
        claude_code_model="claude-sonnet-5-5",
    )})
    picked = {"id": "other-model", "provider": "acme"}
    gate = _gated(mw, {"other-model": _task_model(model=picked)})

    await mw.awrap_tool_call(
        _request({"description": "d", "prompt": "p", "model": "other-model"}), _ok_handler
    )

    started = ledger.started[0]
    assert gate.asked == [("other-model", "general-purpose")]
    assert started["model"] == picked
    assert started["model_override"] == "other-model"
    assert (started["claude_code"], started["claude_code_model"], started["cli_computer"]) == (False, None, None)


@pytest.mark.asyncio
async def test_a_task_on_a_claude_code_row_it_names_is_admitted_as_a_cli_where_it_was_placed():
    ledger = FakeLedger(reject="stop here")
    mw = _middleware(FakeOwner(), ledger)
    placed = Admitted(hosted="saved_token", saved="saved_token", cli_computer="c-7")
    _gated(mw, {"cc-row": _task_model("cc-row", claude_code=placed)})

    await mw.awrap_tool_call(
        _request({"description": "d", "prompt": "p", "model": "cc-row"}), _ok_handler
    )

    started = ledger.started[0]
    assert (started["claude_code"], started["stored_token"]) == (True, True)
    assert (started["claude_code_model"], started["cli_computer"]) == ("cc-row-id", "c-7")


@pytest.mark.asyncio
async def test_a_task_naming_its_subagents_own_model_runs_the_graph_it_has_and_records_it():
    """Recorded, a resume naming none stays on it once the subagent's own
    model moves with the chat's."""
    ledger = FakeLedger(admit="run-1")
    mw = _middleware(FakeOwner(), ledger)
    mw.registry.write_task_meta = AsyncMock()
    _gated(mw, {})
    seen: list = []

    async def handler(request):
        seen.append(mw.registry._tasks["tc-1"].runs_on)
        return ToolMessage(content="done", tool_call_id="tc-1", name="Task")

    result = await mw.awrap_tool_call(
        _request({"description": "d", "prompt": "p", "model": "own-model"}), handler
    )
    await mw.registry._tasks["tc-1"].asyncio_task

    assert seen == [None]
    assert ledger.started[0]["model_override"] == "own-model"
    assert "- Model:" not in result.content


@pytest.mark.asyncio
async def test_a_model_the_turn_cannot_run_refuses_the_task_before_it_exists():
    ledger = FakeLedger()
    mw = _middleware(FakeOwner(), ledger)
    _gated(mw, {"off-plan": TaskModelRefused("Error: off-plan is not on the user's plan.", "r")})

    result = await mw.awrap_tool_call(
        _request({"description": "d", "prompt": "p", "model": "off-plan"}), _ok_handler
    )

    assert (result.content, result.status) == ("Error: off-plan is not on the user's plan.", "error")
    assert mw.registry._tasks == {}
    assert ledger.started == []


@pytest.mark.asyncio
async def test_a_build_with_no_resolver_refuses_a_named_model():
    mw = _middleware(FakeOwner(), FakeLedger())

    result = await mw.awrap_tool_call(
        _request({"description": "d", "prompt": "p", "model": "other-model"}), _ok_handler
    )

    assert result.status == "error"
    assert mw.registry._tasks == {}


@pytest.mark.asyncio
async def test_a_model_that_is_not_a_name_is_left_to_the_tools_own_schema():
    mw = _middleware(FakeOwner(), FakeLedger())
    handler = AsyncMock(return_value=ToolMessage(content="invalid", tool_call_id="tc-1", status="error"))

    result = await mw.awrap_tool_call(
        _request({"description": "d", "prompt": "p", "model": 1}), handler
    )

    handler.assert_awaited_once()
    assert result.status == "error"
    assert mw.registry._tasks == {}


@pytest.mark.asyncio
async def test_the_launch_reply_names_the_model_the_task_runs():
    ledger = FakeLedger(admit="run-1")
    mw = _middleware(FakeOwner(), ledger)
    mw.registry.write_task_meta = AsyncMock()
    _gated(mw, {"other-model": _task_model()})

    result = await mw.awrap_tool_call(
        _request({"description": "d", "prompt": "p", "model": "other-model"}), _ok_handler
    )
    await mw.registry._tasks["tc-1"].asyncio_task

    assert "- Model: Other-Model\n" in result.content
    assert result.additional_kwargs["task_artifact"]["model"] == "other-model"


@pytest.mark.asyncio
async def test_an_update_naming_a_model_is_refused():
    mw = _middleware(FakeOwner(), FakeLedger())
    task = await _register_settled_task(mw)

    result = await mw.awrap_tool_call(
        _request({"action": "update", "task_id": task.task_id, "prompt": "p", "model": "m"}),
        _ok_handler,
    )

    assert (result.content, result.status) == (
        "Error: model and effort apply to init and resume; a running task keeps both.", "error",
    )


@pytest.mark.asyncio
async def test_a_resume_naming_no_model_reruns_the_one_the_ledger_says_it_last_ran_on():
    ledger = _LedgerWithLatest("other-model", reject="stop here")
    mw = _middleware(FakeOwner(), ledger)
    task = await _register_settled_task(mw)
    gate = _gated(mw, {"other-model": _task_model()})

    await mw.awrap_tool_call(_resume(task), _ok_handler)

    assert gate.asked == [("other-model", "general-purpose")]
    assert ledger.started[0]["model_override"] == "other-model"


@pytest.mark.asyncio
async def test_a_resume_whose_last_run_named_no_model_runs_its_subagents_own():
    ledger = _LedgerWithLatest(None, reject="stop")
    mw = _middleware(FakeOwner(), ledger)
    task = await _register_settled_task(mw)
    gate = _gated(mw, {})

    await mw.awrap_tool_call(_resume(task), _ok_handler)

    assert gate.asked == []
    assert "model_override" not in ledger.started[0]


@pytest.mark.asyncio
async def test_a_resume_whose_last_model_cannot_be_read_is_refused_not_moved_to_its_own():
    """Run on the subagent's own instead, the new run would record no model
    and every later resume would follow it."""
    owner = FakeOwner()
    ledger = _LedgerReadFails()
    mw = _middleware(owner, ledger)
    task = await _register_settled_task(mw)
    gate = _gated(mw, {})

    result = await mw.awrap_tool_call(_resume(task), _ok_handler)

    assert (result.content, result.status) == (
        f"Error: the model {task.display_id} last ran on could not be checked just now. "
        "Try again.",
        "error",
    )
    assert (gate.asked, owner.acquired, ledger.started, mw._resume_claims) == ([], [], [], set())


@pytest.mark.asyncio
async def test_a_resume_whose_model_is_gone_says_so_and_holds_no_claim_or_fence():
    owner = FakeOwner()
    ledger = _LedgerWithLatest("other-model")
    mw = _middleware(owner, ledger)
    task = await _register_settled_task(mw)
    _gated(mw, {"other-model": TaskModelRefused("Error: whole.", "it is not on the user's plan")})

    result = await mw.awrap_tool_call(_resume(task), _ok_handler)

    assert (result.content, result.status) == (
        f"Error: {task.display_id} ran on other-model, which is not available now "
        "(it is not on the user's plan). Resume it with model set to another from "
        "`la models list`.",
        "error",
    )
    assert (owner.acquired, ledger.started, mw._resume_claims) == ([], [], set())


@pytest.mark.asyncio
async def test_a_second_resume_while_the_first_resolves_its_model_is_refused():
    """The claim comes before the model is read, so two resumes in one step
    never both pass the liveness check."""
    mw = _middleware(FakeOwner(), _LedgerWithLatest("other-model", reject="stop"))
    task = await _register_settled_task(mw)
    replies: list = []

    class _Reentrant(_Gate):
        async def resolve(self, ask, subagent_type):
            replies.append(await mw.awrap_tool_call(_resume(task, "tc-3"), _ok_handler))
            return await super().resolve(ask, subagent_type)

    mw.task_models = _Reentrant({"other-model": _task_model()})

    await mw.awrap_tool_call(_resume(task), _ok_handler)

    assert replies[0].content.endswith("is already being resumed. Use action='update' to send instructions to it.")


@pytest.mark.asyncio
async def test_a_resume_naming_a_model_it_cannot_run_is_refused_in_the_resolvers_words():
    owner = FakeOwner()
    mw = _middleware(owner, _LedgerWithLatest(None))
    task = await _register_settled_task(mw)
    _gated(mw, {"nope": TaskModelRefused("Error: whole.", "short")})

    result = await mw.awrap_tool_call(_resume(task, model="nope"), _ok_handler)

    assert result.content == "Error: whole."
    assert owner.acquired == []


async def _resumed_args(ledger, gate_answers: dict, **args) -> tuple[list, ToolMessage]:
    mw = _middleware(FakeOwner(), ledger)
    task = await _register_settled_task(mw)
    mw.registry.write_task_meta = AsyncMock()
    _gated(mw, gate_answers)
    seen: list = []

    async def handler(request):
        seen.append(task.runs_on.name if task.runs_on is not None else None)
        return ToolMessage(content="done", tool_call_id="tc-2", name="Task")

    result = await mw.awrap_tool_call(_resume(task, **args), handler)
    await task.asyncio_task
    return seen, result


@pytest.mark.asyncio
async def test_a_resumed_task_runs_on_its_model_and_its_call_names_it():
    seen, result = await _resumed_args(
        _LedgerWithLatest(None, admit="run-uuid-next"), {"other-model": _task_model()},
        model="other-model",
    )

    assert seen == ["other-model"]
    assert "- Model: Other-Model\n" in result.content


@pytest.mark.asyncio
async def test_a_resume_naming_no_model_runs_on_the_one_it_last_ran_on():
    seen, _ = await _resumed_args(
        _LedgerWithLatest("other-model", admit="run-uuid-next"), {"other-model": _task_model()},
    )

    assert seen == ["other-model"]


@pytest.mark.asyncio
async def test_a_resume_carrying_over_its_subagents_own_model_runs_its_graph_and_records_it():
    """Recorded again, a later resume stays on it once the subagent's own
    model moves with the chat's."""
    ledger = _LedgerWithLatest("own-model", admit="run-uuid-next")
    seen, result = await _resumed_args(ledger, {})

    assert seen == [None]
    assert ledger.started[0]["model_override"] == "own-model"
    assert "- Model:" not in result.content


@pytest.mark.asyncio
async def test_a_task_naming_only_an_effort_records_no_model():
    """Its model stays the subagent's own, free to follow the chat's on a
    later resume."""
    ledger = FakeLedger(admit="run-1")
    mw = _middleware(FakeOwner(), ledger)
    mw.registry.write_task_meta = AsyncMock()
    _gated(mw, {None: _task_model("own-model")})

    await mw.awrap_tool_call(
        _request({"description": "d", "prompt": "p", "effort": "high"}), _ok_handler
    )
    await mw.registry._tasks["tc-1"].asyncio_task

    started = ledger.started[0]
    assert "model_override" not in started
    assert started["effort_override"] == "high"


@pytest.mark.asyncio
async def test_with_no_ledger_a_resume_naming_nothing_keeps_the_effort_its_task_was_given():
    mw = _middleware(FakeOwner(), None)
    task = await _register_settled_task(mw)
    task.ask = TaskAsk(effort="high")
    asked: list[TaskAsk] = []

    async def resolve(ask, subagent_type):
        asked.append(ask)

    mw.task_models = SimpleNamespace(resolve=resolve)

    await mw.awrap_tool_call(_resume(task), _ok_handler)

    assert asked == [TaskAsk(effort="high")]


@pytest.mark.asyncio
async def test_a_resume_naming_its_subagents_own_model_goes_back_to_it():
    ledger = _LedgerWithLatest("other-model", admit="run-uuid-next")
    seen, result = await _resumed_args(ledger, {}, model="own-model")

    assert seen == [None]
    assert ledger.started[0]["model_override"] == "own-model"
    assert "- Model:" not in result.content
