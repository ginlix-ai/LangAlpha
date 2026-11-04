"""Calls that stop a Claude Code turn: the real CLI against the loopback API.

A call that waits for the user, or one the credit gate stops, is deferred: the
query ends with it open, the turn pauses, and a new CLI asks for it again once
the turn resumes and is handed its result.
"""

from __future__ import annotations

import dataclasses
import json

import pytest
from langchain_core.messages import AIMessage, HumanMessage, ToolMessage
from langchain_core.tools import tool
from langgraph.checkpoint.memory import InMemorySaver
from langgraph.prebuilt import ToolNode
from langgraph.types import Command, interrupt

from ptc_agent.agent.claude_code.compaction import CompactionRules
from ptc_agent.agent.claude_code.launcher import LocalLauncher
from ptc_agent.agent.claude_code.mid_turn import NOT_RUN
from ptc_agent.agent.claude_code.runtime import TURN_KEY, ClaudeTurn, build_claude_code_graph
from ptc_agent.agent.claude_code.session import RULES_KEY
from ptc_agent.agent.middleware.credit_gate import CreditGateMiddleware, CreditGateState, CreditLease, current_credit_gate
from src.tools.wakeups.quiet import quiet_result

from .fake_api import TOKEN, FakeMessagesAPI
from .harness import bash, spec

pytestmark = [pytest.mark.enable_socket, pytest.mark.asyncio]

ran: list[str] = []


@tool("AskUserQuestion")
def ask(command: str) -> str:
    """Ask the user."""
    return f"User answered: {interrupt({'question': command})}"


@tool("schedule_wakeup", response_format="content_and_artifact")
def wakeup(command: str) -> tuple[str, dict]:
    """End a wake turn."""
    return "Ended without a reply.", {"quiet": True, "changed": False}


@tool("schedule_wakeup", response_format="content_and_artifact")
def refused(command: str) -> tuple[str, dict]:
    """End a wake turn."""
    return "quiet only ends a turn a wakeup started.", {"quiet": False, "changed": False}


@tool("Bash")
def counted(command: str) -> str:
    """Run a shell command."""
    ran.append(command)
    return f"ran: {command}"


TOOLS = [bash, ask, wakeup]
SPEC = dataclasses.replace(spec("claude-haiku-5-5"), tools=TOOLS, waits=lambda name, args: name == "AskUserQuestion",
                           ends_turn=lambda message: quiet_result(message) is not None)


async def credit(state, config):
    return await CreditGateMiddleware().abefore_model(state, None)


def new_graph(tools=TOOLS, pause=()):
    return build_claude_code_graph(ToolNode(tools), pause=pause, checkpointer=InMemorySaver())


async def step(api, graph, thread, given, spec=SPEC, **turn_kw):
    """One run of the thread: its state after, and the model requests it cost."""
    turn = ClaudeTurn(spec, LocalLauncher(env={**api.cli_env(), "CLAUDE_CODE_MAX_RETRIES": "0"}),
                      identity="test-user", thread_key=thread, **turn_kw)
    config = {"configurable": {"thread_id": thread, TURN_KEY: turn}}
    before = len(api.model_requests)
    try:
        await graph.ainvoke(given, config)
    finally:
        await turn.close()
    return await graph.aget_state(config), len(api.model_requests) - before


def _shape(messages):
    return [(type(m).__name__, m.text if isinstance(m, AIMessage) else m.content, [c["name"] for c in getattr(m, "tool_calls", [])])
            for m in messages]


async def test_a_call_that_waits_for_the_user_pauses_the_turn_and_its_answer_reaches_claude_on_resume():
    graph = new_graph()
    async with FakeMessagesAPI() as api:
        paused, asked = await step(api, graph, "t-ask", {"messages": [HumanMessage("CALLS AskUserQuestion:fruit?")]})
        resumed, answered = await step(api, graph, "t-ask", Command(resume="apple"))
        after, next_turn = await step(api, graph, "t-ask", {"messages": [HumanMessage("hello")]})
    assert [i.value for i in paused.interrupts] == [{"question": "fruit?"}] and asked == 1
    assert _shape(resumed.values["messages"][1:]) == [
        ("AIMessage", "", ["AskUserQuestion"]),
        ("ToolMessage", "User answered: apple", []),
        ("AIMessage", "The tool said: User answered: apple", []),
    ]
    assert answered == 1 and not resumed.interrupts
    assert after.values["messages"][-1].text == "ECHO hello" and next_turn == 1


async def test_a_call_that_may_wait_but_does_not_pause_costs_a_new_cli_and_nothing_else():
    ran.clear()
    tools = [counted]
    graph = new_graph(tools)
    held = dataclasses.replace(SPEC, tools=tools, waits=lambda name, args: name == "Bash")
    async with FakeMessagesAPI() as api:
        done, requests = await step(api, graph, "t-pass", {"messages": [HumanMessage("RUN_TOOL please")]}, held)
    assert not done.interrupts and ran == ["echo spike-$((6*7))"] and requests == 2
    assert done.values["messages"][-1].text == "The tool said: ran: echo spike-$((6*7))"


async def test_a_second_call_of_a_batch_that_would_wait_is_answered_as_not_run():
    graph = new_graph()
    async with FakeMessagesAPI() as api:
        paused, _ = await step(api, graph, "t-two", {"messages": [HumanMessage("CALLS AskUserQuestion:a? AskUserQuestion:b?")]})
        resumed, _ = await step(api, graph, "t-two", Command(resume="apple"))
    first, second = paused.values["messages"][1].tool_calls
    [refused] = [m for m in paused.values["messages"] if isinstance(m, ToolMessage)]
    assert [i.value for i in paused.interrupts] == [{"question": "a?"}]
    assert refused.tool_call_id == second["id"] and refused.status == "error" and refused.content.endswith(NOT_RUN)
    assert [(m.tool_call_id, m.content) for m in resumed.values["messages"] if isinstance(m, ToolMessage)][-1] == (
        first["id"], "User answered: apple")


async def test_a_message_sent_while_a_call_waits_answers_it_as_cut_off_and_is_claudes_next_prompt():
    graph = new_graph()
    async with FakeMessagesAPI() as api:
        await step(api, graph, "t-cut", {"messages": [HumanMessage("CALLS AskUserQuestion:fruit?")]})
        after, requests = await step(api, graph, "t-cut", {"messages": [HumanMessage("never mind")]})
    call, cut, again, reply = after.values["messages"][1:]
    assert cut.tool_call_id == call.tool_calls[0]["id"] and cut.content.startswith("[Tool call interrupted")
    assert again.content == "never mind" and reply.text == "ECHO never mind"
    assert requests == 1 and not after.interrupts


async def test_the_calls_a_message_cut_off_are_handed_to_abandon_with_the_message_that_made_them():
    abandoned = []

    async def abandon(message, calls):
        abandoned.append((message.id, [c["id"] for c in calls]))

    graph = build_claude_code_graph(ToolNode(TOOLS), checkpointer=InMemorySaver(), abandon=abandon)
    async with FakeMessagesAPI() as api:
        paused, _ = await step(api, graph, "t-abandon", {"messages": [HumanMessage("CALLS AskUserQuestion:fruit?")]})
        assert abandoned == []
        await step(api, graph, "t-abandon", {"messages": [HumanMessage("never mind")]})
    call = paused.values["messages"][1]
    assert abandoned == [(call.id, [call.tool_calls[0]["id"]])]


async def test_a_result_that_ends_the_turn_stops_the_cli_before_it_asks_claude_again():
    graph = new_graph()
    async with FakeMessagesAPI() as api:
        ended, requests = await step(api, graph, "t-quiet", {"messages": [HumanMessage("CALLS schedule_wakeup:checked")]})
        after, _ = await step(api, graph, "t-quiet", {"messages": [HumanMessage("hello")]})
    assert _shape(ended.values["messages"][1:]) == [
        ("AIMessage", "", ["schedule_wakeup"]), ("ToolMessage", "Ended without a reply.", []),
    ]
    assert requests == 1 and not ended.next
    assert after.values["messages"][-1].text == "ECHO hello"


async def test_a_call_past_the_credit_boundary_runs_and_the_turn_stops_at_the_gate_before_claude_is_asked_again():
    ran.clear()
    tools = [counted]
    graph = new_graph(tools, pause=[("credit", credit)])
    gated = dataclasses.replace(SPEC, tools=tools)
    lease = CreditLease(user_id="u", run_ref="r", port=None)
    lease.denial = {"message": "Out of credits."}
    token = current_credit_gate.set(CreditGateState(run_ref="r", kind="run", port=None, lease=lease))
    async with FakeMessagesAPI() as api:
        try:
            paused, requests = await step(api, graph, "t-credit", {"messages": [HumanMessage("RUN_TOOL please")]}, gated)
        finally:
            current_credit_gate.reset(token)
        stopped = list(ran)
        resumed, _ = await step(api, graph, "t-credit", Command(resume={"decisions": [{"type": "approve"}]}), gated)
    assert [r["type"] for i in paused.interrupts for r in i.value["action_requests"]] == ["credit_pause"] and requests == 1
    assert stopped == ["echo spike-$((6*7))"] and ran == stopped
    assert resumed.values["messages"][-1].text == "The tool said: ran: echo spike-$((6*7))"


async def test_a_quiet_end_the_tool_refuses_past_the_credit_boundary_stops_at_the_gate_before_claude_is_asked_again():
    """Let past the gate to end the turn, the call would otherwise have the CLI
    ask Claude again with no gate in between."""
    tools = [refused]
    graph = new_graph(tools, pause=[("credit", credit)])
    gated = dataclasses.replace(SPEC, tools=tools)
    lease = CreditLease(user_id="u", run_ref="r", port=None)
    lease.denial = {"message": "Out of credits."}
    token = current_credit_gate.set(CreditGateState(run_ref="r", kind="run", port=None, lease=lease))
    async with FakeMessagesAPI() as api:
        try:
            paused, requests = await step(api, graph, "t-refused", {"messages": [HumanMessage("CALLS schedule_wakeup:checked")]}, gated)
        finally:
            current_credit_gate.reset(token)
        resumed, _ = await step(api, graph, "t-refused", Command(resume={"decisions": [{"type": "approve"}]}), gated)
    assert [r["type"] for i in paused.interrupts for r in i.value["action_requests"]] == ["credit_pause"] and requests == 1
    assert resumed.values["messages"][-1].text == "The tool said: quiet only ends a turn a wakeup started."


async def test_a_refused_quiet_end_the_gate_then_lets_on_carries_on_in_a_new_cli():
    async def lets_on(state, config):
        return None

    tools = [refused]
    graph = new_graph(tools, pause=[("credit", lets_on)])
    gated = dataclasses.replace(SPEC, tools=tools)
    lease = CreditLease(user_id="u", run_ref="r", port=None)
    lease.denial = {"message": "Out of credits."}
    token = current_credit_gate.set(CreditGateState(run_ref="r", kind="run", port=None, lease=lease))
    async with FakeMessagesAPI() as api:
        try:
            done, requests = await step(api, graph, "t-lets-on", {"messages": [HumanMessage("CALLS schedule_wakeup:checked")]}, gated)
        finally:
            current_credit_gate.reset(token)
    assert not done.interrupts and requests == 2
    assert done.values["messages"][-1].text == "The tool said: quiet only ends a turn a wakeup started."


async def test_a_compaction_after_an_answer_no_cli_has_taken_runs_on_the_session_holding_the_answer(monkeypatch):
    """The run the answer resumed stopped before its CLI recorded it: left
    deferred, the compaction's CLI asks for the call again with nothing to
    run it, and waits out the compaction budget."""
    monkeypatch.setattr("ptc_agent.agent.claude_code.compaction.get_compaction_timeout", lambda: 30)
    graph = new_graph()
    config = {"configurable": {"thread_id": "t-answered"}}
    async with FakeMessagesAPI(first_party=True) as api:
        launcher = LocalLauncher(env={**api.cli_env(), "CLAUDE_CODE_MAX_RETRIES": "0"}, token=TOKEN)
        for given, kw in [({"messages": [HumanMessage("CALLS AskUserQuestion:fruit?")]}, {}),
                          (Command(resume="apple"), {"interrupt_after": ["held"]})]:
            turn = ClaudeTurn(SPEC, launcher, identity="test-user", thread_key="t-answered")
            try:
                await graph.ainvoke(given, {**config, "configurable": {**config["configurable"], TURN_KEY: turn}}, **kw)
            finally:
                await turn.close()
        state = (await graph.aget_state(config)).values
        n = len(api.model_requests)
        compaction = await ClaudeTurn(SPEC, launcher, identity="test-user", thread_key="t-answered").compact_between_turns(state)
        (summary,) = api.model_requests[n:]
    assert state["messages"][-1].content == "User answered: apple"
    assert compaction.summary_text and "User answered: apple" in json.dumps(summary["messages"])


async def test_a_message_after_an_answer_no_cli_has_taken_reaches_claude_after_the_answer():
    """The run the answer resumed stopped before its CLI took it, and the
    user wrote again: the call ran, so Claude is told its result, not that it
    was cut off."""
    graph = new_graph()
    config = {"configurable": {"thread_id": "t-answered-then"}}
    async with FakeMessagesAPI() as api:
        launcher = LocalLauncher(env={**api.cli_env(), "CLAUDE_CODE_MAX_RETRIES": "0"})
        for given, kw in [({"messages": [HumanMessage("CALLS AskUserQuestion:fruit?")]}, {}),
                          (Command(resume="apple"), {"interrupt_after": ["held"]})]:
            turn = ClaudeTurn(SPEC, launcher, identity="test-user", thread_key="t-answered-then")
            try:
                await graph.ainvoke(given, {**config, "configurable": {**config["configurable"], TURN_KEY: turn}}, **kw)
            finally:
                await turn.close()
        n = len(api.model_requests)
        after, requests = await step(api, graph, "t-answered-then", {"messages": [HumanMessage("which fruit?")]})
        (sent,) = api.model_requests[n:]
    told = json.dumps(sent["messages"])
    assert "User answered: apple" in told and "interrupted" not in told
    assert after.values["messages"][-1].text == "ECHO which fruit?" and requests == 1


async def test_a_result_that_comes_back_after_the_credit_gate_stopped_stops_at_the_gate_before_claude_is_asked_again():
    """The gate let the call run and stopped while it ran, as a quota refresh
    can: no PreToolUse follows to stop the CLI's next request."""
    @tool("Bash")
    def denied_meanwhile(command: str) -> str:
        """Run a shell command."""
        lease.denial = {"message": "Out of credits."}
        return f"ran: {command}"

    tools = [denied_meanwhile]
    graph = new_graph(tools, pause=[("credit", credit)])
    gated = dataclasses.replace(SPEC, tools=tools)
    lease = CreditLease(user_id="u", run_ref="r", port=None)
    token = current_credit_gate.set(CreditGateState(run_ref="r", kind="run", port=None, lease=lease))
    async with FakeMessagesAPI() as api:
        try:
            paused, requests = await step(api, graph, "t-denied-meanwhile", {"messages": [HumanMessage("RUN_TOOL please")]}, gated)
        finally:
            current_credit_gate.reset(token)
        resumed, _ = await step(api, graph, "t-denied-meanwhile", Command(resume={"decisions": [{"type": "approve"}]}), gated)
    assert [r["type"] for i in paused.interrupts for r in i.value["action_requests"]] == ["credit_pause"] and requests == 1
    assert resumed.values["messages"][-1].text == "The tool said: ran: echo spike-$((6*7))"


async def test_a_wakeup_call_beside_a_quiet_end_in_its_batch_still_runs_before_the_turn_ends():
    """As a langalpha turn runs every call of the round before quiet ends it,
    a quiet beside another wakeup call (which quiet allows) included."""
    @tool("schedule_wakeup", response_format="content_and_artifact")
    def wakeups(command: str) -> tuple[str, dict]:
        """Set a wakeup or end a wake turn."""
        if command == "quiet":
            return "Ended without a reply.", {"quiet": True, "changed": True}
        return f"Set {command}.", {"changed": True}

    tools = [wakeups]
    graph = new_graph(tools)
    async with FakeMessagesAPI() as api:
        ended, requests = await step(api, graph, "t-quiet-beside", {"messages": [HumanMessage("CALLS schedule_wakeup:quiet schedule_wakeup:w1")]},
                                     dataclasses.replace(SPEC, tools=tools))
    assert _shape(ended.values["messages"][1:]) == [
        ("AIMessage", "", ["schedule_wakeup", "schedule_wakeup"]), ("ToolMessage", "Ended without a reply.", []), ("ToolMessage", "Set w1.", []),
    ]
    assert requests == 1 and not ended.next


@pytest.mark.parametrize("given", ["CALLS schedule_wakeup:checked", "CALLS AskUserQuestion:fruit?"], ids=["quiet", "pause"])
async def test_a_turn_that_stops_at_a_tool_result_keeps_the_compaction_rules_it_ran_under(given):
    """As a turn that ends at its reply does, so the hint before the next send
    reads them (``next_summary``)."""
    graph = new_graph()
    async with FakeMessagesAPI() as api:
        stopped, _ = await step(api, graph, "t-rules-" + given.split(":")[0][6:], {"messages": [HumanMessage(given)]},
                                compaction=CompactionRules(0, threshold=123_000))
    assert stopped.values[RULES_KEY]["threshold"] == 123_000


async def test_a_quiet_end_past_the_credit_boundary_still_runs_the_wakeup_calls_beside_it_and_ends_the_turn():
    """The gate stops the next request, which no batch that ends the turn makes,
    as a langalpha turn runs a round's calls and stops at its next model call."""
    @tool("schedule_wakeup", response_format="content_and_artifact")
    def wakeups(command: str) -> tuple[str, dict]:
        """Set a wakeup or end a wake turn."""
        if command == "quiet":
            return "Ended without a reply.", {"quiet": True, "changed": True}
        return f"Set {command}.", {"changed": True}

    tools = [wakeups]
    graph = new_graph(tools, pause=[("credit", credit)])
    lease = CreditLease(user_id="u", run_ref="r", port=None)
    lease.denial = {"message": "Out of credits."}
    token = current_credit_gate.set(CreditGateState(run_ref="r", kind="run", port=None, lease=lease))
    async with FakeMessagesAPI() as api:
        try:
            ended, requests = await step(api, graph, "t-quiet-gated", {"messages": [HumanMessage("CALLS schedule_wakeup:w1 schedule_wakeup:quiet")]},
                                         dataclasses.replace(SPEC, tools=tools))
        finally:
            current_credit_gate.reset(token)
    assert not ended.interrupts and not ended.next and requests == 1
    assert [m.content for m in ended.values["messages"] if isinstance(m, ToolMessage)] == ["Set w1.", "Ended without a reply."]


async def test_a_steer_waiting_as_a_query_ends_past_the_credit_boundary_stops_at_the_gate_before_claude_gets_it():
    from unittest import mock

    from ptc_agent.agent.middleware.steering import SteeringMiddleware

    from .test_runtime_cli import _Inbox

    inbox = _Inbox()
    lease = CreditLease(user_id="u", run_ref="r", port=None)

    @tool("Bash")
    def steered(command: str) -> str:
        """Run a shell command."""
        return f"ran: {command}"

    graph = new_graph([steered], pause=[("credit", credit)])
    given = dataclasses.replace(SPEC, tools=[steered])

    async def turn_on(thread_input, gated):
        claude = ClaudeTurn(given, LocalLauncher(env={**api.cli_env(), "CLAUDE_CODE_MAX_RETRIES": "0"}), identity="test-user",
                            thread_key="t-steer-gated", steering=SteeringMiddleware(report_inbox=inbox, kinds=("steer",)))
        config = {"configurable": {"thread_id": "t-steer-gated", TURN_KEY: claude}, "metadata": {"run_id": "run-1"}}
        token = current_credit_gate.set(CreditGateState(run_ref="r", kind="run", port=None, lease=lease)) if gated else None
        try:
            with mock.patch("ptc_agent.agent.claude_code.mid_turn.build_message_checker", inbox.checker):
                await graph.ainvoke(thread_input, config)
        finally:
            if token is not None:
                current_credit_gate.reset(token)
            await claude.close()
        return await graph.aget_state(config)

    async with FakeMessagesAPI() as api:
        n = len(api.model_requests)
        inbox.send("ECHO steered")
        lease.denial = {"message": "Out of credits."}
        paused = await turn_on({"messages": [HumanMessage("hello")]}, gated=True)
        asked = len(api.model_requests) - n
        resumed = await turn_on(Command(resume={"decisions": [{"type": "approve"}]}), gated=False)
    assert [r["type"] for i in paused.interrupts for r in i.value["action_requests"]] == ["credit_pause"] and asked == 1
    assert resumed.values["messages"][-1].text.startswith("ECHO")
