"""The agent's plan as a Claude Code turn asks it: the counts and rows a
langalpha turn's model-call hooks keep, read where the runtime's steps meet
the CLI's loop. The close-out runs against the real CLI
(``test_runtime_cli.py``)."""

from __future__ import annotations

from langchain_core.messages import AIMessage, ToolMessage
from langgraph.types import Command

from ptc_agent.agent.claude_code.agent_plan import AgentPlan
from ptc_agent.agent.middleware.plan import PLAN_CALLS_KEY, PLAN_CHECK_IN_CALLS, plan_check_in
from ptc_agent.agent.middleware.runtime_context.durable import build_update_message, runtime_update_from_message
from ptc_agent.agent.middleware.runtime_context.turn import PLAN_CHECK_IN_ROW_KIND

RUNNING = [{"title": "Gather the data", "status": "in_progress"}, {"title": "Write it up", "status": "pending"}]


def _batch(n: int, name: str = "Bash") -> AIMessage:
    return AIMessage("", id="a", tool_calls=[{"name": name, "args": {}, "id": f"a{i}"} for i in range(n)])


def test_a_reply_is_counted_as_it_is_committed():
    counted = AgentPlan.counted({PLAN_CALLS_KEY: {"since_touch": 2, "without_plan": 2}}, _batch(3))
    assert counted == {PLAN_CALLS_KEY: {"since_touch": 5, "without_plan": 5}}
    assert AgentPlan.counted({}, _batch(1, "UpdatePlan")) == {PLAN_CALLS_KEY: {"since_touch": 0, "without_plan": None}}


def test_a_batch_that_runs_the_step_to_the_count_hands_the_check_in_a_langalpha_turn_would_write():
    held = {"since_touch": PLAN_CHECK_IN_CALLS, "without_plan": None}
    rows, update = AgentPlan.rows({"plan": RUNNING, PLAN_CALLS_KEY: held}, [])
    (row,) = rows
    assert row.id and runtime_update_from_message(row).kind == PLAN_CHECK_IN_ROW_KIND
    assert row.content == build_update_message(plan_check_in(RUNNING, held)).content
    # The check-in restarts the count.
    assert update == {PLAN_CALLS_KEY: {"since_touch": 0, "without_plan": None}}


def test_the_row_reads_the_plan_the_batch_left():
    """A batch whose UpdatePlan finished the plan is owed no check-in on the
    step it ran past."""
    done = [{**s, "status": "completed"} for s in RUNNING]
    out = [Command(update={"plan": done, "messages": [ToolMessage("Plan finished.", tool_call_id="p")]})]
    held = {"since_touch": PLAN_CHECK_IN_CALLS, "without_plan": None}
    assert AgentPlan.rows({"plan": RUNNING, PLAN_CALLS_KEY: held}, [out]) == ((), {})


def test_a_batch_short_of_the_count_hands_nothing():
    assert AgentPlan.rows({"plan": RUNNING, PLAN_CALLS_KEY: {"since_touch": 1}}, []) == ((), {})
