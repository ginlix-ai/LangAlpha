"""Preserved thinking survives the image that takes a request past 20.

Anthropic holds every image to 2000 px a side once a request carries more than
20 of them. Drives a real agent graph with the multimodal strip through a
thread whose first message carries an image larger than that, and whose 21st
image arrives turns later, and checks the Anthropic payloads: every thinking
block an earlier call produced must be sent again with a byte-identical prefix.
"""

from __future__ import annotations

import base64
import io

import pytest
from langchain.agents import create_agent
from langchain_core.messages import HumanMessage
from langchain_core.tools import tool
from langgraph.checkpoint.memory import InMemorySaver
from PIL import Image

from ptc_agent.agent.middleware.file_operations.multimodal_strip import MultimodalStripMiddleware
from ptc_agent.agent.middleware.reasoning_compat import ReasoningCompatibilityMiddleware
from tests.unit.ptc_agent.agent.prefix_stability import (
    Wire,
    all_breaks,
    anthropic_model,
    reply,
    thinking_count,
)


@tool
def get_data(query: str) -> str:
    """Look something up."""
    return f"data for {query}"


def _png(size: tuple[int, int], shade: int) -> dict:
    buffer = io.BytesIO()
    Image.new("RGB", size, (shade, 128, 255 - shade)).save(buffer, "PNG")
    data = base64.b64encode(buffer.getvalue()).decode("ascii")
    return {"type": "image_url", "image_url": {"url": f"data:image/png;base64,{data}"}}


def _images(sent: list[dict]) -> list[dict]:
    return [
        block
        for message in sent
        if isinstance(message.get("content"), list)
        for block in message["content"]
        if block.get("type") == "image"
    ]


def _agent(model_name: str, checkpointer: InMemorySaver):
    return create_agent(
        model=anthropic_model(model_name),
        tools=[get_data],
        system_prompt="You are a test agent.",
        middleware=[
            MultimodalStripMiddleware(model_name=model_name),
            ReasoningCompatibilityMiddleware(),
        ],
        checkpointer=checkpointer,
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("model_name", ["claude-opus-5-5", "claude-opus-5-5-oauth"])
async def test_the_21st_image_keeps_every_earlier_thinking_prefix(monkeypatch, model_name):
    wire = Wire(
        [
            reply(1, tool=("get_data", {"query": "chart"})),
            reply(2, text="A wide chart."),
            reply(3, text="Nineteen swatches."),
            reply(4, tool=("get_data", {"query": "swatch"})),
            reply(5, text="One more swatch."),
        ]
    ).install(monkeypatch)
    cfg = {"configurable": {"thread_id": "t-images", "user_id": "u-1"}}
    saver = InMemorySaver()

    async def turn(*content: dict) -> None:
        # The agent is built per turn, as the server builds it.
        message = HumanMessage(content=list(content))
        await _agent(model_name, saver).ainvoke({"messages": [message]}, cfg)

    await turn({"type": "text", "text": "What does this show?"}, _png((3000, 1500), 0))
    await turn(
        {"type": "text", "text": "And these?"},
        *(_png((8, 8), shade) for shade in range(10, 200, 10)),
    )
    await turn({"type": "text", "text": "One more."}, _png((8, 8), 250))

    assert len(wire.payloads) == 5
    assert [len(_images(p["messages"])) for p in wire.payloads] == [1, 1, 20, 21, 21]
    assert [thinking_count(p) for p in wire.payloads] == [0, 1, 2, 3, 4]
    assert all_breaks(wire) == []
    first = _images(wire.payloads[0]["messages"])[0]
    assert all(_images(p["messages"])[0] == first for p in wire.payloads)
    with Image.open(io.BytesIO(base64.b64decode(first["source"]["data"]))) as sent:
        assert sent.size == (2000, 1000)
