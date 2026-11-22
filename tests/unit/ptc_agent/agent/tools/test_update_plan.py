"""UpdatePlan's contract: how a call resolves against the plan in state, and
what the model reads back. The reader is shown exactly one running step, so
these pin the rules that keep it that way."""

import pytest
from langchain.agents import create_agent
from langchain_core.language_models.fake_chat_models import GenericFakeChatModel
from langchain_core.messages import AIMessage, ToolMessage
from langgraph.types import Command

from ptc_agent.agent.middleware.plan import PlanStateMiddleware
from ptc_agent.agent.tools.plan import (
    MAX_STEPS,
    PlanStep,
    UpdatePlanTool,
    plan_payload,
    resolve,
)


def _steps(*statuses: str) -> list[PlanStep]:
    return [PlanStep(title=f"Step {i + 1}", status=s) for i, s in enumerate(statuses)]


def _stored(*statuses: str) -> list[dict]:
    return [{"title": f"Step {i + 1}", "status": s} for i, s in enumerate(statuses)]


def _statuses(plan: list[dict] | None) -> list[str]:
    assert plan is not None
    return [s["status"] for s in plan]


class TestResolve:
    def test_new_plan_is_shown_with_its_running_step(self):
        plan, text = resolve([], _steps("in_progress", "pending", "pending"), advance=False)
        assert _statuses(plan) == ["in_progress", "pending", "pending"]
        assert text == "Plan shown (3 steps). Step 1 of 3 is in progress: Step 1"

    def test_a_plan_with_no_running_step_starts_the_first_pending_one(self):
        plan, _ = resolve([], _steps("completed", "pending", "pending"), advance=False)
        assert _statuses(plan) == ["completed", "in_progress", "pending"]

    def test_a_second_running_step_goes_back_to_pending(self):
        plan, _ = resolve([], _steps("in_progress", "in_progress"), advance=False)
        assert _statuses(plan) == ["in_progress", "pending"]

    def test_advance_completes_the_running_step_and_starts_the_next(self):
        plan, text = resolve(_stored("in_progress", "pending"), None, advance=True)
        assert _statuses(plan) == ["completed", "in_progress"]
        assert text == "Step 2 of 2 is in progress: Step 2"

    def test_advancing_the_last_step_finishes_the_plan(self):
        plan, text = resolve(_stored("completed", "in_progress"), None, advance=True)
        assert _statuses(plan) == ["completed", "completed"]
        assert text == "Plan finished."

    def test_steps_win_over_advance_in_the_same_call(self):
        # Refusing the pair left a plan the model believed it had moved
        # standing still for the rest of the turn.
        plan, text = resolve(
            _stored("in_progress", "pending", "pending"),
            _steps("completed", "completed", "in_progress"),
            advance=True,
        )
        assert _statuses(plan) == ["completed", "completed", "in_progress"]
        assert text.startswith("Plan shown (3 steps). Step 3 of 3")

    def test_advance_with_no_plan_running_is_refused(self):
        plan, text = resolve([], None, advance=True)
        assert plan is None
        assert text == "There is no plan in progress to advance. Send `steps` to set one."

    @pytest.mark.parametrize("steps", [None, []])
    def test_a_call_with_nothing_to_do_is_refused(self, steps):
        plan, text = resolve(_stored("in_progress"), steps, advance=False)
        assert plan is None
        assert text == "Send `steps` to set the plan, or `advance` to move to the next step."

    def test_a_plan_longer_than_the_limit_is_refused(self):
        plan, text = resolve([], _steps(*["pending"] * (MAX_STEPS + 1)), advance=False)
        assert plan is None
        assert text == f"A plan holds at most {MAX_STEPS} steps. Merge steps and send it again."


class TestDoneAndStart:
    def test_two_steps_finish_at_once(self):
        plan, text = resolve(_stored("completed", "in_progress", "pending", "pending"), None, False, done=[2, 3])
        assert _statuses(plan) == ["completed", "completed", "completed", "in_progress"]
        assert text == "Step 4 of 4 is in progress: Step 4"

    def test_a_step_is_skipped_and_advance_comes_back_to_it(self):
        plan, _ = resolve(_stored("completed", "in_progress", "pending", "pending"), None, False, done=[2], start=4)
        assert _statuses(plan) == ["completed", "completed", "pending", "in_progress"]
        plan, text = resolve(plan, None, advance=True)
        assert _statuses(plan) == ["completed", "completed", "in_progress", "completed"]
        assert text == "Step 3 of 4 is in progress: Step 3"

    def test_starting_a_step_puts_the_running_one_back(self):
        plan, _ = resolve(_stored("in_progress", "pending", "pending"), None, False, start=3)
        assert _statuses(plan) == ["pending", "pending", "in_progress"]

    def test_finishing_the_running_step_alone_starts_the_next_pending_one(self):
        plan, _ = resolve(_stored("completed", "in_progress", "pending"), None, False, done=[2])
        assert _statuses(plan) == ["completed", "completed", "in_progress"]

    def test_finishing_every_step_finishes_the_plan(self):
        plan, text = resolve(_stored("in_progress", "pending"), None, False, done=[1, 2])
        assert _statuses(plan) == ["completed", "completed"]
        assert text == "Plan finished."

    @pytest.mark.parametrize(("done", "start", "bad"), [([5], None, 5), ([1], 0, 0), (None, 4, 4)])
    def test_a_step_not_in_the_plan_is_refused(self, done, start, bad):
        plan, text = resolve(_stored("in_progress", "pending", "pending"), None, False, done=done, start=start)
        assert plan is None
        assert text == f"The plan has no step {bad}; its steps are numbered 1 to 3."

    def test_with_no_plan_it_is_refused(self):
        plan, text = resolve([], None, False, done=[1])
        assert plan is None
        assert text == "There is no plan in progress to advance. Send `steps` to set one."

    def test_they_win_over_advance_and_steps_win_over_them(self):
        plan, _ = resolve(_stored("in_progress", "pending", "pending"), None, True, done=[1], start=3)
        assert _statuses(plan) == ["completed", "pending", "in_progress"]
        plan, _ = resolve(_stored("in_progress", "pending"), _steps("completed", "in_progress"), False, done=[2])
        assert _statuses(plan) == ["completed", "in_progress"]


def test_payload_carries_the_whole_resolved_list():
    assert plan_payload(_stored("completed", "in_progress", "pending")) == {
        "steps": _stored("completed", "in_progress", "pending"),
        "total": 3,
        "completed": 1,
        "current": 1,
    }


def test_the_model_sees_only_its_own_arguments():
    assert set(UpdatePlanTool().tool_call_schema.model_json_schema()["properties"]) == {
        "steps",
        "advance",
        "done",
        "start",
    }


@pytest.mark.asyncio
async def test_an_accepted_call_writes_the_plan_to_state_and_its_result():
    out = await UpdatePlanTool()._arun(
        steps=None, advance=True, state={"plan": _stored("in_progress", "pending")}, tool_call_id="tc-1"
    )
    assert isinstance(out, Command)
    assert _statuses(out.update["plan"]) == ["completed", "in_progress"]
    (message,) = out.update["messages"]
    assert isinstance(message, ToolMessage)
    assert message.content == "Step 2 of 2 is in progress: Step 2"
    assert message.artifact["current"] == 1


@pytest.mark.asyncio
async def test_a_refused_call_leaves_state_alone():
    out = await UpdatePlanTool()._arun(steps=None, advance=True, state={}, tool_call_id="tc-1")
    assert out == "There is no plan in progress to advance. Send `steps` to set one."


class _Scripted(GenericFakeChatModel):
    def bind_tools(self, tools, **kwargs):
        return self


async def _run(*replies: AIMessage) -> list:
    """The tool messages of a turn the model plays out as ``replies``, through
    the real tool node, so the state and call id arrive as they do live."""
    agent = create_agent(
        _Scripted(messages=iter([*replies, AIMessage(content="done")])),
        tools=[UpdatePlanTool()],
        middleware=[PlanStateMiddleware()],
    )
    out = await agent.ainvoke({"messages": [{"role": "user", "content": "go"}]})
    return [m for m in out["messages"] if isinstance(m, ToolMessage)]


def _plan_message(*calls: dict) -> AIMessage:
    return AIMessage(
        content="",
        tool_calls=[{"name": "UpdatePlan", "args": args, "id": f"p{i}"} for i, args in enumerate(calls)],
    )


def _titled(*titles: str) -> dict:
    return {"steps": [{"title": t} for t in titles]}


class TestThroughTheToolNode:
    @pytest.mark.asyncio
    async def test_of_two_calls_in_one_message_only_the_last_applies(self):
        # Two writes to ``plan`` in one step failed the turn, and each call
        # resolved against the plan from before the message.
        first, second, after = await _run(
            _plan_message(_titled("a", "b"), _titled("x", "y", "z")),
            _plan_message({"advance": True}),
        )
        assert first.content == (
            "Only the last `UpdatePlan` call in this message was applied; send one `UpdatePlan` call per message."
        )
        assert first.artifact is None
        assert second.content == "Plan shown (3 steps). Step 1 of 3 is in progress: x"
        assert after.content == "Step 2 of 3 is in progress: y"

    @pytest.mark.asyncio
    async def test_advance_false_spelled_as_a_string_does_not_advance(self):
        _, held, moved = await _run(
            _plan_message(_titled("a", "b")),
            _plan_message({"advance": "false"}),
            _plan_message({"advance": "true"}),
        )
        assert held.content == "Send `steps` to set the plan, or `advance` to move to the next step."
        assert moved.content == "Step 2 of 2 is in progress: b"


class TestSupersededOnlyByACallThatApplies:
    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "later",
        [
            {"steps": "not a list"},  # Fails the schema.
            {"steps": []},  # ``resolve`` refuses it.
            {"done": [9]},
        ],
        ids=["invalid", "empty", "no_such_step"],
    )
    async def test_a_later_call_that_is_refused_leaves_the_earlier_one_applied(self, later):
        # Otherwise neither applies, while the first's result says the last did.
        first, refused = await _run(_plan_message(_titled("a", "b"), later))
        assert first.content == "Plan shown (2 steps). Step 1 of 2 is in progress: a"
        assert refused.content != first.content


class TestTitles:
    def test_a_title_is_one_line(self):
        # It is quoted in the harness's rows and the summary's plan.
        assert PlanStep(title="  Pull\nprices\t\x00for  AAPL \x1b ").title == "Pull prices for AAPL"

    def test_the_length_is_checked_on_the_one_line(self):
        # 180 characters as sent, 119 once on one line.
        assert PlanStep(title="a\n\n" * 60).title == " ".join("a" * 60)
