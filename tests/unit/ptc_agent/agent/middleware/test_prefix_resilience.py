"""Preserved thinking survives a request the provider took only without it.

A 400 on a request whose history carries reasoning is retried once with every
reasoning block stripped. The stripped request is valid, and the answer's own
thinking is bound to it, so every later request has to send that history
stripped as well: putting the blocks back edits the prefix of the answer's
thinking, which the API then drops along with every thinking block after it.
The Wire answers as the API does, so the drops it would report are checked
too, and they have to reach the answers' ``response_metadata``.
"""

from __future__ import annotations

import pytest
from langchain.agents import create_agent
from langchain_core.messages import HumanMessage
from langchain_core.tools import tool
from langgraph.checkpoint.memory import InMemorySaver

from ptc_agent.agent.middleware.model_resilience import ModelResilienceMiddleware
from ptc_agent.agent.middleware.reasoning_compat import ReasoningCompatibilityMiddleware
from tests.unit.ptc_agent.agent.prefix_stability import (
    Wire,
    anthropic_model,
    api_drops,
    kept_breaks,
    listed,
    refusal,
    reply,
    thinking_prefixes,
)

THREAD = "b2c3d4e5-0000-4000-8000-000000000000"


@tool
def get_data(query: str) -> str:
    """Look something up."""
    return f"data for {query}"


def _agent(saver: InMemorySaver):
    model = anthropic_model()
    return create_agent(
        model=model,
        tools=[get_data],
        system_prompt="Answer plainly and briefly.",
        middleware=[
            ModelResilienceMiddleware(
                primary_name="claude-opus-5-5", primary_client=model, max_retries=0, initial_delay=0.0
            ),
            ReasoningCompatibilityMiddleware(),
        ],
        checkpointer=saver,
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("stream", [None, False, True], ids=["scripted", "response", "stream"])
async def test_a_stripped_retry_stays_stripped_for_the_thread(monkeypatch, stream):
    wire = Wire(
        [
            reply(1, tool=("get_data", {"query": "a"})),
            refusal("messages.1.content.0: Invalid `signature` in `thinking` block"),
            reply(3, text="Done."),
            reply(4, tool=("get_data", {"query": "b"})),
            reply(5, text="Still done."),
        ]
    ).install(monkeypatch, stream=stream)
    cfg = {"configurable": {"thread_id": THREAD, "user_id": "u-1"}}
    saver = InMemorySaver()
    for text in ("Look at NVDA.", "And now?"):
        state = await _agent(saver).ainvoke({"messages": [HumanMessage(text)]}, cfg)

    assert len(wire.payloads) == 5
    assert "sig-1" in thinking_prefixes(wire.payloads[1])
    assert "sig-1" not in thinking_prefixes(wire.payloads[2])
    assert api_drops(wire) == {}
    assert kept_breaks(wire) == []
    assert [sorted(thinking_prefixes(p)) for p in wire.payloads[3:]] == [
        ["sig-3"],
        ["sig-3", "sig-4"],
    ]
    assert listed(state["messages"]) == [[], [], [], []]
