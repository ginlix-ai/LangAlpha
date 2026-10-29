"""A step the retired Flash agent left waiting, resumed by the Chief of Staff,
against real agent graphs.

Flash's threads continue in Home. An agent cannot resume a confirmation for a
tool it does not have, so that step is answered as cancelled. What these pin
is the LangGraph side of that: the update clears the interrupt, keeps the
result of a call that finished beside it, and leaves the next turn a model
call, whether it brings a new message or a stale resume.
"""

from __future__ import annotations

import pytest
from langchain.agents import create_agent
from langchain_core.language_models.fake_chat_models import GenericFakeChatModel
from langchain_core.messages import AIMessage, HumanMessage, ToolMessage
from langchain_core.tools import tool
from langgraph.checkpoint.memory import InMemorySaver
from langgraph.types import Command, interrupt

from ptc_agent.agent.state import DeltaAgentState
from src.server.handlers.chat.agent_change import cancel_tool_calls_it_lacks

CONFIG = {"configurable": {"thread_id": "thread-1"}}


class _Model(GenericFakeChatModel):
    def bind_tools(self, tools, **kwargs):
        return self


@tool("manage_workspaces")
def _flash_workspaces(action: str) -> str:
    """Flash's confirmation-gated workspace tool."""
    return str(interrupt({"action_requests": [{"type": "create_workspace"}]}))


@tool("get_quote")
def _quote(symbol: str) -> str:
    """A tool both agents have."""
    return f"quote {symbol}"


@tool("ask_user")
def _ask(question: str) -> str:
    """A question the full agent resumes itself."""
    return str(interrupt({"question": question}))


@tool("manage_workspaces")
def _home_workspaces(action: str) -> str:
    """The Chief of Staff's workspace tool, under the name Flash's has."""
    return str(interrupt({"action_requests": [{"type": "create_workspace"}]}))


@tool("delegate_to_analyst")
def _delegate(question: str) -> str:
    """The Chief of Staff's hand-off, which Flash does not have."""
    return str(interrupt({"action_requests": [{"type": "delegate"}]}))


@tool("ptc_agent")
def _dispatch(question: str) -> str:
    """Flash's hand-off, which the Chief of Staff does not have."""
    return str(interrupt({"action_requests": [{"type": "dispatch"}]}))


_FLASH_TOOLS = [_flash_workspaces, _dispatch, _quote, _ask]
_HOME_TOOLS = [_home_workspaces, _delegate, _quote, _ask]


def _agent(saver, tools, *replies):
    return create_agent(
        _Model(messages=iter(replies)),
        tools=tools,
        checkpointer=saver,
        state_schema=DeltaAgentState,
    )


async def _flash_waiting_on(saver, *calls):
    flash = _agent(
        saver,
        [_flash_workspaces, _quote, _ask],
        AIMessage(content="", tool_calls=[{**c, "type": "tool_call"} for c in calls]),
    )
    await flash.ainvoke({"messages": [HumanMessage("go", id="h-1")]}, CONFIG)


@pytest.mark.asyncio
@pytest.mark.parametrize("next_input", ["message", "resume"])
async def test_a_flash_confirmation_is_cancelled_and_its_neighbour_kept(next_input):
    saver = InMemorySaver()
    await _flash_waiting_on(
        saver,
        {"name": "manage_workspaces", "args": {"action": "create"}, "id": "call-ws"},
        {"name": "get_quote", "args": {"symbol": "AAPL"}, "id": "call-q"},
    )
    full = _agent(saver, [_quote, _ask], AIMessage(content="answer", id="a-2"))

    cancelled = await cancel_tool_calls_it_lacks(full, "thread-1")

    # Only the cancellation is returned for the turn to stream: the neighbour's
    # result already streamed when its call returned.
    assert [(m.tool_call_id, m.status) for m in cancelled] == [("call-ws", "error")]
    assert cancelled[0].id
    state = await full.aget_state(CONFIG)
    assert not state.interrupts
    results = {
        m.tool_call_id: m.content
        for m in state.values["messages"]
        if isinstance(m, ToolMessage)
    }
    assert results["call-q"] == "quote AAPL"
    assert results["call-ws"].startswith("Not run")

    # Run twice it changes nothing: the second turn finds no waiting step.
    assert await cancel_tool_calls_it_lacks(full, "thread-1") == []
    turn = (
        {"messages": [HumanMessage("next", id="h-2")]}
        if next_input == "message"
        else Command(resume={"decisions": [{"type": "approve"}]})
    )
    out = await full.ainvoke(turn, CONFIG)
    assert out["messages"][-1].content == "answer"


@pytest.mark.asyncio
async def test_a_question_the_full_agent_can_resume_is_left_waiting():
    saver = InMemorySaver()
    await _flash_waiting_on(
        saver, {"name": "ask_user", "args": {"question": "Which?"}, "id": "call-ask"}
    )
    full = _agent(saver, [_quote, _ask])

    await cancel_tool_calls_it_lacks(full, "thread-1")

    assert (await full.aget_state(CONFIG)).interrupts


@pytest.mark.asyncio
async def test_a_step_the_chief_of_staff_lacks_is_cancelled():
    """The approved call would otherwise reach an agent that has no such tool."""
    saver = InMemorySaver()
    waiting = _agent(
        saver,
        _FLASH_TOOLS,
        AIMessage(
            content="",
            tool_calls=[
                {
                    "name": "ptc_agent",
                    "args": {"question": "q"},
                    "id": "call-d",
                    "type": "tool_call",
                }
            ],
        ),
    )
    await waiting.ainvoke({"messages": [HumanMessage("go", id="h-1")]}, CONFIG)
    home = _agent(saver, _HOME_TOOLS, AIMessage(content="answer", id="a-2"))

    await cancel_tool_calls_it_lacks(home, "thread-1")

    state = await home.aget_state(CONFIG)
    assert not state.interrupts
    (result,) = [m for m in state.values["messages"] if isinstance(m, ToolMessage)]
    assert result.tool_call_id == "call-d"
    assert result.content.startswith("Not run")
    out = await home.ainvoke(
        Command(resume={"decisions": [{"type": "approve"}]}), CONFIG
    )
    assert out["messages"][-1].content == "answer"


@pytest.mark.asyncio
async def test_a_step_under_a_name_the_resuming_agent_has_is_left_waiting():
    """Matched by name, as the resume runs a call: both agents have a
    workspace tool, and the Chief of Staff resumes a pending one itself."""
    saver = InMemorySaver()
    await _flash_waiting_on(
        saver,
        {"name": "manage_workspaces", "args": {"action": "create"}, "id": "call-ws"},
    )
    home = _agent(saver, _HOME_TOOLS)

    await cancel_tool_calls_it_lacks(home, "thread-1")

    assert (await home.aget_state(CONFIG)).interrupts
