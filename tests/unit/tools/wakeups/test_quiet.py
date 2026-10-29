"""When ``quiet`` may end a turn, and what the call reports about it."""

import pytest
from langchain_core.messages import AIMessage, HumanMessage, ToolMessage

from ptc_agent.agent.middleware.direct_mcp import METADATA_KEY
from src.tools.wakeups import tool as tool_mod
from src.tools.wakeups.quiet import quiet_refusal
from tests.unit.tools.wakeups.conftest import configurable

WAKE = {"wake_turn": True}


def wake_input() -> HumanMessage:
    return HumanMessage(content="<wakeup>\nIt is 08:30 EDT.\n</wakeup>")


def steer(count: int = 1) -> HumanMessage:
    """A message steered into a running turn; ``count`` is how many of the
    user's own messages it carries."""
    return HumanMessage(
        content="Also check AMD.",
        additional_kwargs={
            "lc_source": "steering",
            "steering_delivered": {"count": count},
        },
    )


def test_quiet_is_allowed_on_a_wake_turn_nobody_steered_into():
    messages = [
        AIMessage(content="earlier"),
        wake_input(),
        AIMessage(content="checking"),
    ]
    assert quiet_refusal(messages, WAKE) is None


def test_quiet_is_refused_once_the_user_steers_into_the_turn():
    messages = [wake_input(), AIMessage(content="checking"), steer()]
    assert quiet_refusal(messages, WAKE) == "user_turn"


def test_quiet_is_refused_for_a_steer_sent_while_the_turn_was_starting():
    # It landed after the previous turn's last reply and ahead of the wake input.
    messages = [
        AIMessage(content="earlier"),
        steer(),
        wake_input(),
        AIMessage(content="checking"),
    ]
    assert quiet_refusal(messages, WAKE) == "user_turn"


def test_a_steer_that_carries_no_user_message_does_not_count():
    # A hand-off or the Chief of Staff steering in is read by its sender, not answered.
    messages = [wake_input(), steer(count=0), AIMessage(content="checking")]
    assert quiet_refusal(messages, WAKE) is None


@pytest.mark.parametrize(
    "configurable_", [{}, {"wake_turn": False}, {"wake_turn": "yes"}]
)
def test_quiet_is_refused_outside_a_wake_turn(configurable_):
    assert (
        quiet_refusal([wake_input(), AIMessage(content="checking")], configurable_)
        == "user_turn"
    )


def call(name: str, call_id: str = "c1") -> dict:
    return {"name": name, "args": {}, "id": call_id, "type": "tool_call"}


def order_result(*, order) -> ToolMessage:
    """A direct brokerage call's result, stamped by the binder; ``order`` is
    what the call does to an order, None for a read."""
    return ToolMessage(
        content="ok",
        tool_call_id="c1",
        name="mcp__broker__place_order",
        artifact={METADATA_KEY: {"server": "broker", "tool": "place_order", "order": order}},
    )


def test_quiet_is_refused_once_the_turn_acted_on_an_order():
    # Placed, or refused before the vendor saw it: either way the user would
    # want to hear of it, and a quiet turn leaves the thread read.
    messages = [
        wake_input(),
        AIMessage(content="", tool_calls=[call("mcp__broker__place_order")]),
        order_result(order={"action": "place"}),
        AIMessage(content="", tool_calls=[call("schedule_wakeup", "c2")]),
    ]
    assert quiet_refusal(messages, WAKE) == "acted"


def test_a_read_from_the_brokerage_does_not_count_as_acting():
    messages = [
        wake_input(),
        AIMessage(content="", tool_calls=[call("mcp__broker__get_positions")]),
        order_result(order=None),
        AIMessage(content="", tool_calls=[call("schedule_wakeup", "c2")]),
    ]
    assert quiet_refusal(messages, WAKE) is None


def test_quiet_is_refused_after_a_hand_off_to_an_analyst():
    messages = [
        wake_input(),
        AIMessage(content="", tool_calls=[call("delegate_to_analyst")]),
        ToolMessage(content="handed off", tool_call_id="c1", name="delegate_to_analyst"),
        AIMessage(content="", tool_calls=[call("schedule_wakeup", "c2")]),
    ]
    assert quiet_refusal(messages, WAKE) == "acted"


def test_quiet_is_refused_beside_another_call_whose_result_is_unread():
    # The round runs together, so an order sent beside quiet has no result yet.
    messages = [
        wake_input(),
        AIMessage(
            content="",
            tool_calls=[call("mcp__broker__place_order"), call("schedule_wakeup", "c2")],
        ),
    ]
    assert quiet_refusal(messages, WAKE) == "other_calls"


def test_quiet_beside_another_schedule_wakeup_call_is_allowed():
    messages = [
        wake_input(),
        AIMessage(
            content="",
            tool_calls=[call("schedule_wakeup"), call("schedule_wakeup", "c2")],
        ),
    ]
    assert quiet_refusal(messages, WAKE) is None


# --- the call --------------------------------------------------------------


async def quiet_call(args, *, messages, **config):
    return await tool_mod._execute(args, {"messages": messages}, configurable(**config))


@pytest.mark.asyncio
async def test_an_accepted_quiet_ends_the_turn_and_says_nothing_changed(db):
    text, artifact, error = await quiet_call(
        {"quiet": "Checked NVDA, still above 200."},
        messages=[wake_input()],
        wake_turn=True,
    )
    assert (text, artifact, error) == (
        "Ended without a reply.",
        {"quiet": True, "changed": False},
        False,
    )


@pytest.mark.asyncio
async def test_a_quiet_check_that_set_a_wakeup_reports_a_change(db):
    text, artifact, _ = await quiet_call(
        {"quiet": "Reset the alert.", "triggers": [{"at": "2026-10-21T08:30"}]},
        messages=[wake_input()],
        wake_turn=True,
    )
    assert artifact == {"quiet": True, "changed": True}
    assert text.endswith("Ended without a reply.")


@pytest.mark.asyncio
async def test_quiet_on_a_turn_with_a_user_message_is_an_error_and_does_not_end_it(db):
    text, artifact, error = await quiet_call(
        {"quiet": "nothing"}, messages=[wake_input(), steer()], wake_turn=True
    )
    assert text == (
        "quiet only ends a turn a wakeup started; this turn has a user message, so reply to it."
    )
    assert (artifact["quiet"], error) == (False, True)


@pytest.mark.asyncio
async def test_quiet_after_an_order_is_an_error_that_asks_for_a_reply(db):
    text, artifact, error = await quiet_call(
        {"quiet": "Bought as planned."},
        messages=[
            wake_input(),
            AIMessage(content="", tool_calls=[call("mcp__broker__place_order")]),
            order_result(order={"action": "place"}),
        ],
        wake_turn=True,
    )
    assert text == (
        "quiet ends a check that found nothing to tell; this turn acted on an order "
        "or handed work to an analyst, so tell the user what you did."
    )
    assert (artifact["quiet"], error) == (False, True)


@pytest.mark.asyncio
async def test_quiet_must_be_a_line_of_text(db):
    text, artifact, _ = await quiet_call(
        {"quiet": "  "}, messages=[wake_input()], wake_turn=True
    )
    assert text == "Not done:\n- quiet: one short line of text."
    assert artifact["quiet"] is False
