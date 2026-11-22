"""The plan's state through the real graph: it belongs to its request, a
running step that stops moving is checked in on, and a request that runs
long with no plan is asked for one once."""

from __future__ import annotations

from typing import Any

import pytest
from langchain.agents.middleware import AgentMiddleware
from langchain.agents.middleware.types import hook_config
from langchain_core.messages import AIMessage, HumanMessage
from langgraph.types import Command

from ptc_agent.agent.middleware.plan import (
    PLAN_CALLS_KEY,
    PLAN_CHECK_IN_CALLS,
    PLAN_NUDGE_CALLS,
    PlanOffMiddleware,
    PlanStateMiddleware,
)
from ptc_agent.agent.middleware.plan_close_out import PLAN_REQUEST_AT_KEY
from ptc_agent.agent.middleware.runtime_context.turn import PLAN_CHECK_IN_ROW_KIND, PLAN_NUDGE_ROW_KIND

from .plan_graph import PLAN, Ask, Script, Thread, answer, bash, plan_call, row_texts, rows, turn

pytestmark = pytest.mark.asyncio

NUDGE = (
    f"You have made {PLAN_NUDGE_CALLS} tool calls on this request without a plan. "
    "If the work has three or more distinct steps, show it with `UpdatePlan`."
)


def _check_in(calls: int = PLAN_CHECK_IN_CALLS) -> str:
    return (
        "Your plan has not moved for a while\n\n"
        f"Step 1 of 2, “Gather”, has been the step in progress for your last {calls} tool calls. "
        "If that step is done, call `UpdatePlan` with `advance` in the same message as your next tool call, or "
        "send the new `steps` if the plan itself has changed. If you are still on that step, carry on without "
        "calling it."
    )


class TestCheckIn:
    async def test_checks_in_after_each_run_of_calls_with_the_step_unmoved(self):
        # The plan's own result is the first tool result; the check-in follows
        # the sixth call after it, and the next one six calls on.
        done = await turn(plan_call(PLAN), *bash(13), answer(), middleware=[PlanStateMiddleware()])
        assert rows(done.final["messages"], PLAN_CHECK_IN_ROW_KIND) == [7, 13]
        assert row_texts(done.final["messages"], PLAN_CHECK_IN_ROW_KIND)[0] == _check_in()

    async def test_a_plan_call_restarts_the_count_and_its_other_calls_do_not_count(self):
        moved = AIMessage(
            content="",
            id="m-moved",
            tool_calls=[
                {"name": "UpdatePlan", "args": {"advance": True}, "id": "p2"},
                *({"name": "Bash", "args": {}, "id": f"x{i}"} for i in range(4)),
            ],
        )
        done = await turn(plan_call(PLAN), *bash(5), moved, *bash(5, start=5), answer(),
                          middleware=[PlanStateMiddleware()])
        assert rows(done.final["messages"], PLAN_CHECK_IN_ROW_KIND) == []
        assert done.final[PLAN_CALLS_KEY] == {"since_touch": 5, "without_plan": None}


class _InjectsCalls(AgentMiddleware):
    """Puts a model message with calls in the thread before the first model
    call, as steering does with a resume call, and sends it to the tools."""

    def __init__(self, calls: int) -> None:
        super().__init__()
        self._calls = calls

    @hook_config(can_jump_to=["tools"])
    async def abefore_model(self, state: Any, runtime: Any = None) -> dict[str, Any] | None:
        if any(m.id == "injected" for m in state["messages"]):
            return None
        calls = [{"name": "Bash", "args": {}, "id": f"i{i}"} for i in range(self._calls)]
        return {"messages": [AIMessage(content="", id="injected", tool_calls=calls)], "jump_to": "tools"}


class TestNudge:
    async def test_asks_once_when_a_request_runs_long_without_a_plan(self):
        done = await turn(*bash(12), answer(), middleware=[PlanStateMiddleware()])
        assert rows(done.final["messages"], PLAN_NUDGE_ROW_KIND) == [PLAN_NUDGE_CALLS]
        assert row_texts(done.final["messages"], PLAN_NUDGE_ROW_KIND) == [NUDGE]

    async def test_a_short_request_is_never_asked(self):
        done = await turn(*bash(PLAN_NUDGE_CALLS - 1), answer(), middleware=[PlanStateMiddleware()])
        assert rows(done.final["messages"], PLAN_NUDGE_ROW_KIND) == []

    async def test_a_request_with_a_plan_is_never_asked(self):
        done = await turn(plan_call(PLAN), *bash(12), answer(), middleware=[PlanStateMiddleware()])
        assert rows(done.final["messages"], PLAN_NUDGE_ROW_KIND) == []

    async def test_only_what_the_model_sent_counts(self):
        # A call another middleware put in the thread is not the model's step.
        done = await turn(answer(), middleware=[PlanStateMiddleware(), _InjectsCalls(PLAN_NUDGE_CALLS)])
        assert done.model.calls == 1
        assert rows(done.final["messages"], PLAN_NUDGE_ROW_KIND) == []
        assert done.final[PLAN_CALLS_KEY] == {"since_touch": 0, "without_plan": 0}


class TestPlanBelongsToItsRequest:
    async def test_a_new_request_starts_with_no_plan_and_no_counts(self):
        thread = Thread()
        first = await thread.turn(plan_call(PLAN), *bash(3), answer(), middleware=[PlanStateMiddleware()])
        assert first.final["plan"][0]["status"] == "in_progress"
        second = await thread.turn(answer("Next.", id="m-next"), middleware=[PlanStateMiddleware()], text="next")
        assert second.final["plan"] == []
        assert second.final[PLAN_CALLS_KEY] == {"since_touch": 0, "without_plan": 0}

    async def test_a_turn_that_carries_the_request_on_keeps_it_and_counts_on(self):
        # A report-back, a wake or a retry that inherits its rules.
        thread = Thread()
        await thread.turn(plan_call(PLAN), *bash(3), answer("Waiting."), middleware=[PlanStateMiddleware()])
        second = await thread.turn(*bash(3, start=3), answer(id="m-answer-2"), middleware=[PlanStateMiddleware(continues=True)],
                                   text="The research finished.")
        assert second.final["plan"][0]["status"] == "in_progress"
        assert row_texts(second.final["messages"], PLAN_CHECK_IN_ROW_KIND) == [_check_in()]

    async def test_the_request_keeps_when_it_began_through_the_turns_that_carry_it_on(self):
        # What it set to report back is bounded by it, so a later request's
        # close-out never waits on a wakeup an earlier one left standing.
        thread = Thread()
        first = await thread.turn(plan_call(PLAN), answer("Waiting."), middleware=[PlanStateMiddleware()])
        began = first.final[PLAN_REQUEST_AT_KEY]
        carried = await thread.turn(answer(id="m-answer-2"), middleware=[PlanStateMiddleware(continues=True)],
                                    text="The research finished.")
        assert carried.final[PLAN_REQUEST_AT_KEY] == began
        later = await thread.turn(answer("Next.", id="m-next"), middleware=[PlanStateMiddleware()], text="next")
        assert later.final[PLAN_REQUEST_AT_KEY] > began

    async def test_the_orchestrator_re_entering_the_request_keeps_it(self):
        thread = Thread()
        model = Script(replies=[plan_call(PLAN), answer("Waiting."), answer("Done.", id="m-done")])
        agent = thread.agent(model, PlanStateMiddleware())
        await thread.run(agent, model, {"messages": [HumanMessage("go")]})
        again = await thread.run(agent, model, {"messages": [HumanMessage("A report came in.", name="orchestrator")]})
        assert again.final["plan"][0]["status"] == "in_progress"

    async def test_a_resumed_interrupt_keeps_it_through_a_later_re_entry(self):
        # A resume enters at the interrupted node, never through before_agent,
        # on an agent built for the resuming request.
        thread = Thread()
        asks = AIMessage(content="", id="m-ask", tool_calls=[{"name": "Ask", "args": {"question": "?"}, "id": "a1"}])
        first = Script(replies=[plan_call(PLAN), asks])
        await thread.run(thread.agent(first, PlanStateMiddleware(), tools=[Ask]), first,
                         {"messages": [HumanMessage("go")]})
        model = Script(replies=[answer("Thanks."), answer("Done.", id="m-done")])
        agent = thread.agent(model, PlanStateMiddleware(), tools=[Ask])
        resumed = await thread.run(agent, model, Command(resume="yes"))
        assert resumed.final["plan"][0]["status"] == "in_progress"
        again = await thread.run(agent, model, {"messages": [HumanMessage("A report came in.", name="orchestrator")]})
        assert again.final["plan"][0]["status"] == "in_progress"

    async def test_with_the_plan_off_one_an_earlier_turn_left_is_taken_away(self):
        thread = Thread()
        await thread.turn(plan_call(PLAN), answer(), middleware=[PlanStateMiddleware()])
        off = await thread.turn(answer("Next.", id="m-next"), middleware=[PlanOffMiddleware()], text="next")
        assert off.final["plan"] == []
