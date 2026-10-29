"""Preserved thinking survives the harness's own edits to what the model sent.

Each scenario drives a real agent graph through two turns with the
middleware that touches one part of the history, and checks the Anthropic
payloads: every thinking block an earlier call produced must be sent again
with a byte-identical prefix.

- Tool arguments the harness parses for a tool (a stringified list, a dict
  passed as a file's content) stay in the message as the model sent them.
- An answer retried after it meant to call a tool and carried none keeps,
  in history, the broken answer and the hint it was produced behind.
- An image a request gives by URL is fetched once, so a later request
  cannot read other bytes behind the same address.
"""

from __future__ import annotations

import io
from typing import Any

import httpx
import pytest
from langchain.agents import create_agent
from langchain_core.messages import AIMessage, HumanMessage
from langchain_core.tools import StructuredTool, tool
from langgraph.checkpoint.memory import InMemorySaver
from PIL import Image

from ptc_agent.agent.middleware.direct_mcp import METADATA_KEY
from ptc_agent.agent.middleware.order_governance import (
    ExecutionGrant,
    OrderGovernanceMiddleware,
    Proposed,
)
from ptc_agent.agent.middleware.reasoning_compat import ReasoningCompatibilityMiddleware
from ptc_agent.agent.middleware.tool.argument_parsing import ToolArgumentParsingMiddleware
from ptc_agent.agent.middleware.tool.empty_call_retry import EmptyToolCallRetryMiddleware
from ptc_agent.agent.state import DeltaAgentState
from ptc_agent.agent.transcript.classify import human_kind
from src.server.handlers.chat import request_prep
from src.server.handlers.chat.image_intake import inline_image_urls
from src.server.models.chat import ChatMessage, ChatRequest, ContentItem
from src.tools.web.inhouse import guard
from tests.unit.ptc_agent.agent.prefix_stability import (
    Wire,
    all_breaks,
    anthropic_model,
    reply,
    thinking_count,
)

CFG = {"configurable": {"thread_id": "t-prefix-audit", "user_id": "u-1"}}


def _agent(tools: list, middleware: list, checkpointer: InMemorySaver):
    return create_agent(
        model=anthropic_model(),
        tools=tools,
        system_prompt="You are a test agent.",
        middleware=[*middleware, ReasoningCompatibilityMiddleware()],
        checkpointer=checkpointer,
        state_schema=DeltaAgentState,
    )


async def _two_turns(tools: list, middleware: list, first: list) -> InMemorySaver:
    """A turn, then a second one on a rebuilt agent, as the server runs them."""
    saver = InMemorySaver()
    await _agent(tools, middleware, saver).ainvoke({"messages": first}, CFG, durability="sync")
    await _agent(tools, middleware, saver).ainvoke(
        {"messages": [HumanMessage("And now?")]}, CFG, durability="sync"
    )
    return saver


def _tool_inputs(payload: dict) -> list[Any]:
    return [
        block["input"]
        for message in payload["messages"]
        if message["role"] == "assistant" and isinstance(message["content"], list)
        for block in message["content"]
        if block.get("type") == "tool_use"
    ]


# -- tool arguments ----------------------------------------------------------


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("name", "args", "received"),
    [
        ("Write", {"file_path": "a.json", "content": {"k": [1, 2]}}, '{"k": [1, 2]}'),
        ("get_rows", {"symbols": '["NVDA", "AMD"]'}, ["NVDA", "AMD"]),
    ],
    ids=["object-to-string", "string-to-list"],
)
async def test_parsed_tool_args_leave_the_call_as_sent(monkeypatch, name, args, received):
    seen: list[Any] = []

    @tool("Write")
    def write(file_path: str, content: str) -> str:
        """Write a file."""
        seen.append(content)
        return "ok"

    @tool("get_rows")
    def get_rows(symbols: list[str]) -> str:
        """Rows for some symbols."""
        seen.append(symbols)
        return "rows"

    wire = Wire([reply(1, tool=(name, args)), reply(2, text="Done."), reply(3, text="Still done.")])
    wire.install(monkeypatch)

    await _two_turns([write, get_rows], [ToolArgumentParsingMiddleware()], [HumanMessage("Go.")])

    assert all_breaks(wire) == []
    assert seen == [received]
    assert [_tool_inputs(p) for p in wire.payloads] == [[], [args], [args]]
    assert [thinking_count(p) for p in wire.payloads] == [0, 1, 2]


ORDER_TOOL = "mcp__broker__place_order"
ORDER_STAMP = {
    "server": "broker",
    "vendor": "broker",
    "tool": "place_order",
    "approval": True,
    "order": {"action": "place", "mode": "paper"},
}


class _ApprovedLedger:
    """A ledger whose attempts need no decision, recording what it hashed."""

    def __init__(self) -> None:
        self.proposed: list[dict] = []

    async def propose(self, message_id, tool_call_id, tool_name, stamp, args) -> Proposed:
        self.proposed.append(dict(args))
        return Proposed(attempt_id=f"att-{tool_call_id}", awaiting_decision=False)

    async def find(self, message_id, tool_call_id):
        return f"att-{tool_call_id}"

    async def consume(self, attempt_id):
        return ExecutionGrant(token=f"{attempt_id}.sig")

    async def complete(self, attempt_id, result):
        return None

    async def decide(self, attempt_id, decision, message):
        return None

    async def refuse(self, attempt_id, reason):
        return None


@pytest.mark.asyncio
async def test_an_order_needing_no_decision_leaves_its_call_as_sent(monkeypatch):
    def place_order(legs: Any) -> str:
        """Place an order."""
        return "placed"

    order = StructuredTool.from_function(func=place_order, name=ORDER_TOOL)
    order.metadata = {METADATA_KEY: dict(ORDER_STAMP)}
    args = {"legs": '[{"side": "buy", "ratio": 1}]'}
    ledger = _ApprovedLedger()
    wire = Wire([reply(1, tool=(ORDER_TOOL, args)), reply(2, text="Placed."), reply(3, text="Still placed.")])
    wire.install(monkeypatch)

    await _two_turns([order], [OrderGovernanceMiddleware(ledger, [order])], [HumanMessage("Buy it.")])

    assert all_breaks(wire) == []
    assert ledger.proposed == [{"legs": [{"side": "buy", "ratio": 1}]}]
    assert [_tool_inputs(p) for p in wire.payloads] == [[], [args], [args]]


# -- empty tool call retry ---------------------------------------------------


@pytest.mark.asyncio
async def test_a_retried_empty_tool_call_keeps_what_the_retry_was_produced_behind(monkeypatch):
    @tool
    def get_data(query: str) -> str:
        """Look something up."""
        return f"data for {query}"

    broken = AIMessage(
        content=[{"type": "thinking", "thinking": "", "signature": "sig-1"}, {"type": "text", "text": "Calling"}],
        id="msg_01",
        response_metadata={"model_name": "claude-opus-5-5", "stop_reason": "tool_use"},
    )
    wire = Wire(
        [broken, reply(2, tool=("get_data", {"query": "a"})), reply(3, text="Done."), reply(4, text="Again.")]
    )
    wire.install(monkeypatch)

    saver = await _two_turns([get_data], [EmptyToolCallRetryMiddleware()], [HumanMessage("Go.")])

    assert all_breaks(wire) == []
    assert [thinking_count(p) for p in wire.payloads] == [0, 1, 2, 3]
    history = (await _agent([get_data], [], saver).aget_state(CFG)).values["messages"]
    stamped = [human_kind(m) for m in history if isinstance(m, HumanMessage) and human_kind(m) != "plain"]
    assert stamped == ["empty-call-retry"]


# -- images given by URL -----------------------------------------------------


def _png(shade: int) -> bytes:
    buffer = io.BytesIO()
    Image.new("RGB", (8, 8), (shade, 128, 255 - shade)).save(buffer, "PNG")
    return buffer.getvalue()


IMAGE_URL = "https://images.example.com/chart.png?sig=abc"


@pytest.mark.asyncio
async def test_an_image_given_by_url_is_read_once(monkeypatch):
    @tool
    def get_data(query: str) -> str:
        """Look something up."""
        return f"data for {query}"

    behind = {IMAGE_URL: _png(10)}

    def serve(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, content=behind[str(request.url)])

    monkeypatch.setattr(guard, "GuardedAsyncTransport", lambda: httpx.MockTransport(serve))
    wire = Wire(
        [reply(1, tool=("get_data", {"query": "a"})), reply(2, text="A chart."), reply(3, text="Same chart.")],
        served=lambda url: behind[url],
    )
    wire.install(monkeypatch)
    request = ChatRequest(
        messages=[
            ChatMessage(
                role="user",
                content=[
                    ContentItem(type="text", text="What is this?"),
                    ContentItem(type="image", image_url=IMAGE_URL),
                ],
            )
        ]
    )

    messages = request_prep.normalize_request_messages(await inline_image_urls(request))
    saver = InMemorySaver()
    await _agent([get_data], [], saver).ainvoke({"messages": messages}, CFG, durability="sync")
    # The address now serves other bytes: a re-render, or a signed URL reissued.
    behind[IMAGE_URL] = _png(200)
    await _agent([get_data], [], saver).ainvoke({"messages": [HumanMessage("And now?")]}, CFG, durability="sync")

    assert all_breaks(wire) == []
    assert [thinking_count(p) for p in wire.payloads] == [0, 1, 2]
