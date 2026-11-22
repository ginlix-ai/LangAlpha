"""The plan's close-out: when it is asked, that its call never puts text
after the answer the user was shown, and that it never fails a turn whose
answer the user already has."""

from __future__ import annotations

import asyncio
from typing import Any

import pytest
from langchain.agents.middleware import AgentMiddleware
from langchain.agents.middleware.types import ModelRequest
from langchain_core.messages import AIMessage, HumanMessage, ToolMessage
from langgraph.errors import GraphInterrupt

from ptc_agent.agent.middleware.plan import PlanStateMiddleware
from ptc_agent.agent.middleware.plan_close_out import (
    REPORTS_BACK,
    PlanCloseOutMiddleware,
    close_out_ask,
    close_out_due,
    is_close_out_reply,
    plan_calls_only,
)
from ptc_agent.agent.middleware.runtime_context.changes import UPDATE_SCHEMA_VERSION
from ptc_agent.agent.middleware.runtime_context.durable import (
    DurableUpdate,
    build_update_message,
    render_update_row,
    runtime_update_from_message,
)
from ptc_agent.agent.middleware.runtime_context.turn import PLAN_CLOSE_OUT_ROW_KIND, TURN_ROW_KIND

from .plan_graph import PLAN, Thread, answer, bash, pending, plan_call, rows, texts, turn


def _turn_row():
    return build_update_message(DurableUpdate(kind=TURN_ROW_KIND, schema_version=UPDATE_SCHEMA_VERSION, text=""))


def _plan_call() -> AIMessage:
    return AIMessage(content="", tool_calls=[{"name": "UpdatePlan", "args": {"advance": True}, "id": "p"}])


def _stored(*statuses: str) -> list[dict]:
    return [{"title": f"Step {i + 1}", "status": s} for i, s in enumerate(statuses)]


def _answer(text: str = "Here is the answer.") -> AIMessage:
    return AIMessage(content=text, id="answer")


class TestAsk:
    def test_asks_after_an_answer_that_left_steps_unfinished(self):
        state = {"plan": _stored("completed", "in_progress", "pending"), "messages": [_turn_row(), _plan_call(), _answer()]}
        row = close_out_ask(state)
        assert row is not None and row.kind == PLAN_CLOSE_OUT_ROW_KIND
        assert render_update_row(row) == (
            "Your turn ended with 2 plan steps not marked finished: “Step 2”, “Step 3”. Nothing you started "
            "will report back. Mark the ones you finished with `UpdatePlan` (`done` or `advance`) and leave the "
            "rest. Send only that call: the user already has your answer, and any text or other call here is "
            "discarded."
        )

    def test_one_step_reads_singular(self):
        state = {"plan": _stored("completed", "in_progress"), "messages": [_turn_row(), _plan_call(), _answer()]}
        assert close_out_ask(state).text.startswith("Your turn ended with 1 plan step not marked finished")

    @pytest.mark.parametrize(
        "state",
        [
            # Finished.
            {"plan": _stored("completed"), "messages": [_turn_row(), _plan_call(), _answer()]},
            # Not an answer: the work goes on.
            {"plan": _stored("in_progress"), "messages": [_turn_row(), _plan_call(), *bash(1)]},
        ],
    )
    def test_no_ask_when_nothing_is_owed(self, state):
        assert close_out_ask(state) is None


class TestDue:
    def _asked(self, *after) -> list:
        ask = build_update_message(close_out_ask(_OWED))
        return [_turn_row(), _plan_call(), _answer(), ask, *after]

    def test_the_call_after_the_ask_is_the_close_out_whatever_rows_came_since(self):
        assert close_out_due(self._asked())
        assert close_out_due(self._asked(_turn_row()))

    def test_a_steer_delivered_since_makes_it_an_ordinary_call(self):
        steer = HumanMessage(content="and check X", additional_kwargs={"lc_source": "steering"}, id="s")
        assert not close_out_due(self._asked(steer))

    def test_no_close_out_once_its_reply_is_in_or_before_one_was_asked(self):
        assert not close_out_due(self._asked(plan_calls_only(_plan_call())))
        assert not close_out_due([_turn_row(), _plan_call(), _answer()])


_OWED = {"plan": _stored("in_progress"), "messages": [_turn_row(), _plan_call(), _answer()]}


class TestTrim:
    def test_keeps_reasoning_and_plan_calls_only(self):
        reply = AIMessage(
            content=[
                {"type": "thinking", "thinking": "mark it", "signature": "s"},
                {"type": "text", "text": "I updated the plan."},
                {"type": "tool_use", "id": "p1", "name": "UpdatePlan", "input": {"advance": True}},
                {"type": "tool_use", "id": "b1", "name": "Bash", "input": {}},
            ],
            tool_calls=[
                {"name": "UpdatePlan", "args": {"advance": True}, "id": "p1"},
                {"name": "Bash", "args": {}, "id": "b1"},
            ],
        )
        trimmed = plan_calls_only(reply)
        assert [b["type"] for b in trimmed.content] == ["thinking", "tool_use"]
        assert [c["id"] for c in trimmed.tool_calls] == ["p1"]
        assert is_close_out_reply(trimmed)

    def test_plain_text_goes(self):
        trimmed = plan_calls_only(AIMessage(content="Done.", tool_calls=[{"name": "UpdatePlan", "args": {}, "id": "p"}]))
        assert trimmed.content == ""

    def test_a_responses_function_call_is_kept_by_its_call_id(self):
        # Its ``id`` is the output item's, ``call_id`` the tool call's.
        reply = AIMessage(
            content=[
                {"type": "reasoning", "id": "rs_1", "summary": []},
                {"type": "text", "text": "Marking the plan."},
                {"type": "function_call", "id": "fc_1", "call_id": "call_1", "name": "UpdatePlan", "arguments": "{}"},
                {"type": "function_call", "id": "fc_2", "call_id": "call_2", "name": "Bash", "arguments": "{}"},
            ],
            tool_calls=[
                {"name": "UpdatePlan", "args": {"advance": True}, "id": "call_1"},
                {"name": "Bash", "args": {}, "id": "call_2"},
            ],
        )
        trimmed = plan_calls_only(reply)
        assert [(b["type"], b.get("call_id")) for b in trimmed.content] == [("reasoning", None), ("function_call", "call_1")]
        assert [c["id"] for c in trimmed.tool_calls] == ["call_1"]

    def test_a_provider_run_call_stays_with_its_result(self):
        # Not among the tool calls, so dropping it would orphan its result.
        reply = AIMessage(
            content=[
                {"type": "server_tool_use", "id": "srv_1", "name": "web_search", "input": {}},
                {"type": "web_search_tool_result", "tool_use_id": "srv_1", "content": []},
                {"type": "tool_use", "id": "p1", "name": "UpdatePlan", "input": {"advance": True}},
            ],
            tool_calls=[{"name": "UpdatePlan", "args": {"advance": True}, "id": "p1"}],
        )
        assert [b["type"] for b in plan_calls_only(reply).content] == [
            "server_tool_use",
            "web_search_tool_result",
            "tool_use",
        ]


_UNFINISHED = {"steps": [{"title": "Gather", "status": "in_progress"}, {"title": "Write", "status": "pending"}]}


async def _turn(*replies: Any, **kwargs: Any):
    """A turn that sets the plan, answers with it unfinished, then plays
    ``replies``."""
    return await turn(plan_call(_UNFINISHED), answer(), *replies, **kwargs)


def _asks(messages: list) -> list:
    return [m for m in messages if (r := runtime_update_from_message(m)) is not None and r.kind == PLAN_CLOSE_OUT_ROW_KIND]


class _SteersOnce(AgentMiddleware):
    """Delivers one steer ahead of the call after the close-out's ask, as
    steering does ahead of any model call."""

    @staticmethod
    async def abefore_model(state: Any, runtime: Any = None) -> dict[str, Any] | None:
        messages = state["messages"]
        if not _asks(messages[-1:]) or any(m.id == "steer" for m in messages):
            return None
        return {"messages": [HumanMessage("and check X", id="steer", additional_kwargs={"lc_source": "steering"})]}


class TestThroughTheGraph:
    @pytest.mark.asyncio
    async def test_marks_the_steps_and_leaves_the_answer_last(self):
        done = await _turn(
            AIMessage(
                content="Both steps are done, updating the plan.",
                id="m3",
                tool_calls=[
                    {"name": "UpdatePlan", "args": {"done": [1, 2]}, "id": "p2"},
                    {"name": "Bash", "args": {"command": "rm -rf"}, "id": "b1"},
                ],
            )
        )
        assert done.model.calls == 3
        assert [s["status"] for s in done.final["plan"]] == ["completed", "completed"]
        assert texts(done.final["messages"])[-1] == "The answer."
        assert not any(isinstance(m, ToolMessage) and m.name == "Bash" for m in done.final["messages"])
        assert "updating" not in done.streamed and "answer." in done.streamed

    @pytest.mark.asyncio
    async def test_a_reply_with_no_plan_call_leaves_no_trace(self):
        done = await _turn(AIMessage(content="Nothing to mark.", id="m3"))
        assert done.model.calls == 3
        assert [s["status"] for s in done.final["plan"]] == ["in_progress", "pending"]
        assert not any(isinstance(m, AIMessage) and m.id == "m3" for m in done.final["messages"])
        assert not _asks(done.final["messages"])
        assert texts(done.final["messages"])[-1] == "The answer."
        assert "Nothing" not in done.streamed

    @pytest.mark.asyncio
    async def test_no_close_out_while_work_will_report_back(self):
        done = await _turn(is_pending=True)
        assert done.model.calls == 2
        assert not _asks(done.final["messages"])

    @pytest.mark.asyncio
    async def test_a_failed_close_out_call_ends_the_turn_on_its_answer(self):
        done = await _turn(RuntimeError("overloaded"))
        assert done.model.calls == 3
        assert not _asks(done.final["messages"])
        assert done.final["messages"][-1].id == "m-answer"
        assert [s["status"] for s in done.final["plan"]] == ["in_progress", "pending"]

    @pytest.mark.asyncio
    @pytest.mark.parametrize("stop", [asyncio.CancelledError(), GraphInterrupt()], ids=["cancelled", "interrupted"])
    async def test_what_stops_the_run_is_no_failure_of_the_call(self, stop):
        ask = build_update_message(close_out_ask(_OWED))
        ask.id = "ask"
        request = ModelRequest(model=None, messages=[], state={"messages": [*_OWED["messages"], ask]})

        async def handler(request):
            raise stop

        with pytest.raises(type(stop)):
            await PlanCloseOutMiddleware(pending()).awrap_model_call(request, handler)

    @pytest.mark.asyncio
    async def test_a_steer_on_the_close_out_call_is_answered_as_any_call_answers_one(self):
        # The ask would tell the model to discard the very answer the user is
        # owed, so the call goes without it, and the next answer is closed out.
        done = await _turn(
            answer("Checked X too.", id="m-steered"),
            plan_call({"done": [1, 2]}, id="p2"),
            middleware=[PlanCloseOutMiddleware(pending()), PlanStateMiddleware(), _SteersOnce()],
        )
        assert done.model.calls == 4
        steered = done.model.sent[2]
        assert steered[-1].id == "steer" and not _asks(steered)
        assert "Checked X too." in done.streamed
        assert texts(done.final["messages"])[-1] == "Checked X too."
        (ask,) = _asks(done.final["messages"])
        assert done.final["messages"].index(ask) > [m.id for m in done.final["messages"]].index("m-steered")
        assert [s["status"] for s in done.final["plan"]] == ["completed", "completed"]

    @pytest.mark.asyncio
    async def test_asks_once_per_turn_and_again_in_the_next(self):
        thread = Thread()
        # Each close-out here keeps nothing, so its ask goes with it; the model
        # calls show it asked once in each turn.
        first = await thread.turn(plan_call(_UNFINISHED), answer(), AIMessage(content="", id="m3"))
        assert first.model.calls == 3 and not _asks(first.final["messages"])
        second = await thread.turn(
            answer("Still on it.", id="m-later"), AIMessage(content="", id="m4"),
            middleware=[PlanCloseOutMiddleware(pending()), PlanStateMiddleware(continues=True)],
            text="any news?",
        )
        assert second.model.calls == 2 and not _asks(second.final["messages"])

    @pytest.mark.asyncio
    async def test_the_check_ins_still_run_beside_it(self):
        done = await turn(plan_call(PLAN), *bash(13), answer(), AIMessage(content="", id="m-end"))
        assert rows(done.final["messages"], "plan_check_in") == [7, 13]


@pytest.mark.asyncio
async def test_only_work_that_reports_back_to_this_request_holds_it_back(monkeypatch):
    """An automation set to run is no reason to wait: its run is a request
    of its own, which starts with no plan. Nor is anything an earlier request
    set, such as a standing wakeup, and of what ended, only what ended in
    this turn is surely still to report: a task's report-back leaves its run
    unstamped."""
    from datetime import UTC, datetime

    from src.tools.thread_status import tool as thread_status

    from ptc_agent.agent.middleware.plan_close_out import PLAN_REQUEST_AT_KEY

    asked: dict[str, Any] = {}

    async def pending_work(thread_id, user_id, *, analysts, kinds, before=None, since=None, ended_since=None):
        asked.update(kinds=kinds, analysts=analysts, since=since, ended_since=ended_since)
        return None

    monkeypatch.setattr(thread_status, "pending_work", pending_work)
    close_out = PlanCloseOutMiddleware.for_thread("t", "u", analysts=True)
    opened = datetime(2026, 11, 20, 9, 30, tzinfo=UTC)
    row = DurableUpdate(kind=TURN_ROW_KIND, schema_version=UPDATE_SCHEMA_VERSION, text="", created_at=opened)
    state = {PLAN_REQUEST_AT_KEY: 1_763_600_000.0, "messages": [HumanMessage("go"), build_update_message(row), _answer()]}
    assert await close_out.nothing_pending(state)
    assert asked == {
        "kinds": REPORTS_BACK,
        "analysts": True,
        "since": datetime.fromtimestamp(1_763_600_000.0, tz=UTC),
        "ended_since": opened,
    }
    assert "automation" not in REPORTS_BACK


class TestNotAskedWhereItCannotPay:
    """The jump back to the model passes the credit gate and the tool stop
    again, so a close-out there would pause or fail a finished turn, or make
    a call whose ``UpdatePlan`` is refused."""

    @pytest.mark.asyncio
    async def test_past_the_credit_stop(self, monkeypatch):
        from ptc_agent.agent.middleware import plan_close_out

        monkeypatch.setattr(plan_close_out, "credit_stops", lambda: True)
        done = await _turn()
        assert done.model.calls == 2 and not _asks(done.final["messages"])

    @pytest.mark.asyncio
    async def test_for_an_account_that_ends(self, monkeypatch):
        from ptc_agent.agent.middleware import plan_close_out

        async def ends() -> bool:
            return True

        monkeypatch.setattr(plan_close_out, "account_ends", ends)
        done = await _turn()
        assert done.model.calls == 2 and not _asks(done.final["messages"])

    @pytest.mark.asyncio
    async def test_inside_the_tool_stop(self, monkeypatch):
        from datetime import UTC, datetime, timedelta

        from ptc_agent.agent.middleware import plan_close_out

        ends = datetime.now(tz=UTC) + timedelta(seconds=30)
        monkeypatch.setattr(plan_close_out, "_run_clock", lambda: (ends, timedelta(minutes=5), timedelta(minutes=2)))
        done = await _turn()
        assert done.model.calls == 2 and not _asks(done.final["messages"])
        # Before the stop, it is asked.
        monkeypatch.setattr(plan_close_out, "_run_clock", lambda: (ends + timedelta(hours=1), timedelta(minutes=5), timedelta(minutes=2)))
        assert (await _turn(AIMessage(content="", id="m3"))).model.calls == 3
