"""``WakeupQuietGuardMiddleware`` ends the run after an accepted quiet and
only then."""

import pytest
from langchain_core.messages import AIMessage, HumanMessage, ToolMessage

from ptc_agent.agent.middleware.wakeups import WakeupQuietGuardMiddleware

END = {"jump_to": "end"}


def call(name: str, call_id: str) -> dict:
    return {"name": name, "args": {}, "id": call_id}


def result(name: str, call_id: str, **artifact) -> ToolMessage:
    return ToolMessage(
        content="ok", name=name, tool_call_id=call_id, artifact=artifact or None
    )


WAKE = HumanMessage(content="<wakeup>\nIt is 08:30 EDT.\n</wakeup>")
QUIET_CALL = AIMessage(content="", tool_calls=[call("schedule_wakeup", "q1")])
QUIET_OK = result("schedule_wakeup", "q1", quiet=True, changed=False)


async def decide(messages):
    return await WakeupQuietGuardMiddleware().abefore_model({"messages": messages})


@pytest.mark.asyncio
async def test_a_round_that_ended_quietly_ends_the_run():
    assert await decide([WAKE, QUIET_CALL, QUIET_OK]) == END


@pytest.mark.asyncio
async def test_the_sync_hook_decides_the_same_way():
    assert (
        WakeupQuietGuardMiddleware().before_model(
            {"messages": [WAKE, QUIET_CALL, QUIET_OK]}
        )
        == END
    )
    assert WakeupQuietGuardMiddleware().before_model({"messages": [WAKE]}) is None


@pytest.mark.asyncio
async def test_a_refused_quiet_lets_the_run_go_on():
    refused = result("schedule_wakeup", "q1", quiet=False, changed=False)
    assert await decide([WAKE, QUIET_CALL, refused]) is None


@pytest.mark.asyncio
async def test_other_calls_of_the_round_still_ran_and_do_not_stop_it_ending():
    two = AIMessage(
        content="", tool_calls=[call("web_search", "s1"), call("schedule_wakeup", "q1")]
    )
    assert await decide([WAKE, two, result("web_search", "s1"), QUIET_OK]) == END


@pytest.mark.asyncio
async def test_a_quiet_in_an_earlier_round_does_not_end_a_later_one():
    later = AIMessage(content="", tool_calls=[call("web_search", "s1")])
    assert (
        await decide([WAKE, QUIET_CALL, QUIET_OK, later, result("web_search", "s1")])
        is None
    )


@pytest.mark.asyncio
async def test_something_new_after_the_results_is_answered_first():
    steered = HumanMessage(
        content="Also check AMD.",
        additional_kwargs={"lc_source": "steering", "steering_delivered": {"count": 1}},
    )
    assert await decide([WAKE, QUIET_CALL, QUIET_OK, steered]) is None


@pytest.mark.asyncio
async def test_an_ordinary_tool_result_does_not_end_the_run():
    assert (
        await decide(
            [
                WAKE,
                AIMessage(content="", tool_calls=[call("web_search", "s1")]),
                result("web_search", "s1"),
            ]
        )
        is None
    )
