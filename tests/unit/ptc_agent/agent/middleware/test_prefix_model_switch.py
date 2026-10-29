"""Claude's thinking survives switching models, as the API asks.

A model that cannot read another model's thinking block drops it from that
request on its own and reads it again once the conversation returns to a model
that can. So every request to a Claude model sends every Claude thinking block
the thread holds, whichever Claude model or route wrote it and whatever model
answered in between, and only a block no Claude request ever carried, an
unsigned one, stays out. These drive a real agent graph across the switches
and check the Anthropic payloads.
"""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any

import pytest
from langchain.agents import create_agent
from langchain_core.messages import AIMessage, HumanMessage
from langchain_core.outputs import ChatGeneration, ChatResult
from langchain_core.tools import tool
from langchain_openai.chat_models.base import BaseChatOpenAI
from langgraph.checkpoint.memory import InMemorySaver

from ptc_agent.agent.middleware.reasoning_compat import ReasoningCompatibilityMiddleware
from tests.unit.ptc_agent.agent.prefix_stability import (
    THINKING_TYPES,
    Wire,
    all_breaks,
    anthropic_model,
    reply,
    thinking_prefixes,
)


@tool
def get_data(query: str) -> str:
    """Look something up."""
    return f"data for {query}"


def _agent(model: Any, saver: InMemorySaver):
    return create_agent(
        model=model,
        tools=[get_data],
        system_prompt="Answer plainly and briefly.",
        middleware=[ReasoningCompatibilityMiddleware()],
        checkpointer=saver,
    )


def _claude_view(wire: Wire) -> SimpleNamespace:
    """The calls that went to a Claude model, with the blocks they produced."""
    kept = [n for n, payload in enumerate(wire.payloads) if payload["model"].startswith("claude-")]
    index = {n: k for k, n in enumerate(kept)}
    return SimpleNamespace(
        payloads=[wire.payloads[n] for n in kept],
        produced=[(index[n], sig) for n, sig in wire.produced if n in index],
    )


def _thinking_blocks(payload: dict[str, Any]) -> list[dict[str, Any]]:
    return [
        block
        for message in payload["messages"]
        if isinstance(message.get("content"), list)
        for block in message["content"]
        if isinstance(block, dict) and block.get("type") in THINKING_TYPES
    ]


def _gpt(script: list[AIMessage], monkeypatch) -> None:
    async def _agenerate(model, messages, stop=None, run_manager=None, **kwargs):
        return ChatResult(generations=[ChatGeneration(message=script.pop(0))])

    monkeypatch.setattr(BaseChatOpenAI, "_agenerate", _agenerate)
    monkeypatch.setattr(BaseChatOpenAI, "_should_stream", lambda self, **_: False)


def _sends_every_earlier_block(view: SimpleNamespace) -> None:
    produced: list[str] = []
    for k, payload in enumerate(view.payloads):
        sent = set(thinking_prefixes(payload))
        assert set(produced) <= sent, f"Claude call {k + 1} left out {set(produced) - sent}"
        assert all(block.get("signature") for block in _thinking_blocks(payload))
        produced += [sig for n, sig in view.produced if n == k]


async def _switch(monkeypatch, turns: list[str]) -> SimpleNamespace:
    """Two calls a turn, a tool call and an answer, on each model in ``turns``."""
    script: list[Any] = []
    for i, name in enumerate(turns):
        if name.startswith("gpt-"):
            continue
        n = 2 * i + 1
        script += [reply(n, tool=("get_data", {"query": str(i)}), model_name=name), reply(n + 1, text=f"{i}.", model_name=name)]
    wire = Wire(script).install(monkeypatch)
    _gpt(
        [
            AIMessage(
                content="",
                tool_calls=[{"name": "get_data", "args": {"query": "g"}, "id": "call_g", "type": "tool_call"}],
                id="gpt_01",
                response_metadata={"model_name": "gpt-6-luna", "finish_reason": "tool_calls"},
            ),
            AIMessage(content="G.", id="gpt_02", response_metadata={"model_name": "gpt-6-luna"}),
        ],
        monkeypatch,
    )
    cfg = {"configurable": {"thread_id": "t-model-switch"}}
    saver = InMemorySaver()
    for i, name in enumerate(turns):
        await _agent(anthropic_model(name), saver).ainvoke({"messages": [HumanMessage(f"turn {i}")]}, cfg)
    return _claude_view(wire)


@pytest.mark.asyncio
@pytest.mark.parametrize("route", ["", "-oauth"], ids=["api-key", "oauth"])
async def test_every_claude_request_sends_every_claude_block(monkeypatch, route):
    # Across Claude generations, with an OpenAI turn and an Anthropic-protocol
    # non-Claude turn in between.
    turns = [
        f"claude-opus-5-5{route}",
        f"claude-sonnet-5-5{route}",
        "gpt-6-luna",
        f"claude-haiku-5-5{route}",
        "deepseek-flash",
        f"claude-fable-5-1{route}",
        f"claude-opus-5-5{route}",
    ]
    view = await _switch(monkeypatch, turns)

    assert len(view.payloads) == 10
    _sends_every_earlier_block(view)
    assert all_breaks(view) == []


@pytest.mark.asyncio
async def test_a_route_switch_still_sends_every_claude_block(monkeypatch):
    # The OAuth route puts its identity block in front of the system prompt,
    # so a switch between the routes edits every block's prefix whatever is
    # sent; leaving the blocks out would lose them for the way back too.
    view = await _switch(
        monkeypatch, ["claude-opus-5-5", "claude-opus-5-5-oauth", "claude-sonnet-5-5", "claude-fable-5-1-oauth"]
    )

    assert len(view.payloads) == 8
    _sends_every_earlier_block(view)


@pytest.mark.asyncio
async def test_an_unsigned_block_never_reaches_claude_and_breaks_nothing(monkeypatch):
    cut = AIMessage(
        content=[
            {"type": "thinking", "thinking": "Partial reasoning", "index": 0},
            {"type": "text", "text": "I can't finish that.", "index": 1},
        ],
        id="msg_cut",
        response_metadata={"model_name": "claude-opus-5-5", "stop_reason": "refusal"},
    )
    wire = Wire(
        [
            reply(1, tool=("get_data", {"query": "a"})),
            cut,
            reply(3, tool=("get_data", {"query": "b"})),
            reply(4, text="Done."),
            reply(5, text="Again."),
        ]
    ).install(monkeypatch)
    cfg = {"configurable": {"thread_id": "t-unsigned"}}
    saver = InMemorySaver()
    for i, name in enumerate(["claude-opus-5-5", "claude-sonnet-5-5", "claude-opus-5-5"]):
        await _agent(anthropic_model(name), saver).ainvoke({"messages": [HumanMessage(f"turn {i}")]}, cfg)

    assert len(wire.payloads) == 5
    for payload in wire.payloads:
        assert all(block.get("signature") for block in _thinking_blocks(payload))
    assert [len(thinking_prefixes(p)) for p in wire.payloads] == [0, 1, 1, 2, 3]
    signed = [(n, sig) for n, sig in wire.produced if sig != "None"]
    assert all_breaks(SimpleNamespace(payloads=wire.payloads, produced=signed)) == []
