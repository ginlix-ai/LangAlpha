"""Preserved thinking survives a change in the tools the model is offered.

Two scenarios, each driven through a real agent graph with the middleware that
owns the change: a skill's tool offered mid-turn once its SKILL.md is read, and
a directly bound tool in the next turn's build (a server the user connected
between turns) that is gone from the build after it. Every thinking block an
earlier call produced must be sent again with a byte-identical prefix.
"""

from __future__ import annotations

from datetime import UTC, datetime

import pytest
from langchain.agents import create_agent
from langchain_anthropic.middleware import AnthropicPromptCachingMiddleware
from langchain_core.messages import HumanMessage, ToolMessage
from langchain_core.tools import tool
from langgraph.checkpoint.memory import InMemorySaver

from ptc_agent.agent.middleware.reasoning_compat import ReasoningCompatibilityMiddleware
from ptc_agent.agent.middleware.runtime_context.tail import RowCarrierMiddleware
from ptc_agent.agent.middleware.runtime_context.turn import TurnContextMiddleware
from ptc_agent.agent.middleware.skills import SkillsMiddleware
from ptc_agent.agent.middleware.skills.registry import SkillDefinition
from ptc_agent.agent.middleware.tool_surface import ToolSurfaceMiddleware
from tests.unit.ptc_agent.agent.prefix_stability import (
    Wire,
    all_breaks,
    anthropic_model,
    reply,
    thinking_count,
)

_MODELS = ["claude-opus-5-5", "claude-opus-5-5-oauth"]
_CFG = {"configurable": {"thread_id": "t-prefix-tools", "user_id": "u-1"}}


@tool("Read")
def read_file(file_path: str) -> str:
    """Read a file."""
    return "# Charts"


@tool
def get_data(query: str) -> str:
    """Look something up."""
    return f"data for {query}"


@tool
def draw_chart(symbol: str) -> str:
    """Draw a chart."""
    return f"chart of {symbol}"


@tool
def place_order(symbol: str) -> str:
    """Place an order."""
    return f"order for {symbol}"


_CHARTS = SkillDefinition(
    name="charts",
    description="Draw charts.",
    tool_names=("draw_chart",),
    skill_md_path="charts/SKILL.md",
)


def _agent(model_name: str, saver: InMemorySaver, tools: list, *, skills: bool = False):
    """The agent as one turn's build makes it: the tool middleware in the main
    stack's order, around a turn row that lands beside a change message."""
    return create_agent(
        model=anthropic_model(model_name),
        tools=tools,
        system_prompt="You are a test agent.",
        middleware=[
            TurnContextMiddleware(now=datetime(2026, 7, 1, 14, 30, tzinfo=UTC), timezone="UTC"),
            *([SkillsMiddleware(skill_registry={"charts": _CHARTS}, inject_manifest=False)] if skills else []),
            AnthropicPromptCachingMiddleware(unsupported_model_behavior="ignore"),
            ToolSurfaceMiddleware(),
            RowCarrierMiddleware(),
            ReasoningCompatibilityMiddleware(),
        ],
        checkpointer=saver,
    )


def _names(payload: dict) -> list[str]:
    return [t["name"] for t in payload["tools"]]


def _changes(payload: dict) -> list[list[tuple[str, str, str]]]:
    """Each change message: (block type, how the tool is given, its name)."""
    return [
        [
            (b["type"], b["tool"]["type"], b["tool"].get("name") or b["tool"]["definition"]["name"])
            for b in m["content"]
            if b.get("type", "").startswith("tool_")
        ]
        for m in payload["messages"]
        if m["role"] == "system"
        and any(b.get("type") in ("tool_addition", "tool_removal") for b in m["content"])
    ]


@pytest.mark.asyncio
@pytest.mark.parametrize("model_name", _MODELS)
async def test_a_skill_loading_its_tools_keeps_every_earlier_thinking_prefix(monkeypatch, model_name):
    wire = Wire(
        [
            reply(1, tool=("Read", {"file_path": "/home/user/.agents/skills/charts/SKILL.md"})),
            reply(2, tool=("draw_chart", {"symbol": "NVDA"})),
            reply(3, text="Charted."),
            reply(4, tool=("draw_chart", {"symbol": "AMD"})),
            reply(5, text="Charted again."),
        ]
    ).install(monkeypatch)
    saver = InMemorySaver()
    tools = [read_file, get_data, draw_chart]
    await _agent(model_name, saver, tools, skills=True).ainvoke({"messages": [HumanMessage("Chart NVDA")]}, _CFG)
    await _agent(model_name, saver, tools, skills=True).ainvoke({"messages": [HumanMessage("And AMD")]}, _CFG)

    assert [thinking_count(p) for p in wire.payloads] == [0, 1, 2, 3, 4]
    assert all_breaks(wire) == []
    # The array, which leads the cache key, never changes: the skill's tool
    # is defined in a change message right after the read that loaded it.
    assert all(p["tools"] == wire.payloads[0]["tools"] for p in wire.payloads)
    assert _names(wire.payloads[0]) == ["Read", "get_data"]
    assert all(_changes(p) == [[("tool_addition", "tool_definition", "draw_chart")]] for p in wire.payloads[1:])
    for payload in wire.payloads:
        marked = [t["name"] for t in payload["tools"] if "cache_control" in t]
        assert marked == ["get_data"]


@pytest.mark.asyncio
@pytest.mark.parametrize("model_name", _MODELS)
async def test_a_tool_added_and_removed_between_turns_keeps_every_earlier_thinking_prefix(
    monkeypatch, model_name
):
    wire = Wire(
        [
            reply(1, tool=("get_data", {"query": "a"})),
            reply(2, text="Done."),
            reply(3, tool=("place_order", {"symbol": "NVDA"})),
            reply(4, text="Ordered."),
            reply(5, tool=("get_data", {"query": "b"})),
            reply(6, text="Done again."),
        ]
    ).install(monkeypatch)
    saver = InMemorySaver()
    # Turn 2's build has the server the user connected; turn 3's no longer does.
    for tools, text in (
        ([get_data], "Look up a"),
        ([get_data, place_order], "Buy NVDA"),
        ([get_data], "Look up b"),
    ):
        await _agent(model_name, saver, tools).ainvoke({"messages": [HumanMessage(text)]}, _CFG)

    assert [thinking_count(p) for p in wire.payloads] == [0, 1, 2, 3, 4, 5]
    assert all_breaks(wire) == []
    assert all(_names(p) == ["get_data"] for p in wire.payloads)
    added = [("tool_addition", "tool_definition", "place_order")]
    removed = [("tool_removal", "tool_reference", "place_order")]
    assert [_changes(p) for p in wire.payloads] == [
        [],
        [],
        [added],
        [added],
        [added, removed],
        [added, removed],
    ]
    # Each change joins the turn row in front of it in one system entry.
    entry = [m for m in wire.payloads[2]["messages"] if m["role"] == "system"][-1]
    assert [b["type"] for b in entry["content"]] == ["text", "tool_addition"]
    # The request's betas replace the client's beta header, so they carry it.
    header = anthropic_model(model_name).default_headers["anthropic-beta"].split(",")
    assert wire.payloads[2]["betas"][: len(header)] == header
    assert "inline-tools-2026-09-15" in wire.payloads[2]["betas"]


@tool("get_data")
def get_data_reworded(query: str) -> str:
    """Look something up, newest first."""
    return f"data for {query}"


@pytest.mark.asyncio
@pytest.mark.parametrize("model_name", _MODELS)
async def test_a_deploy_rewording_a_tool_keeps_every_earlier_thinking_prefix(monkeypatch, model_name):
    """A build between turns that changed a tool's description: the thread
    keeps the array it first sent, and the next turn start gives the model
    the new definition under the same name."""
    wire = Wire(
        [
            reply(1, tool=("get_data", {"query": "a"})),
            reply(2, text="Done."),
            reply(3, tool=("get_data", {"query": "b"})),
            reply(4, text="Done again."),
        ]
    ).install(monkeypatch)
    saver = InMemorySaver()
    await _agent(model_name, saver, [get_data]).ainvoke({"messages": [HumanMessage("Look up a")]}, _CFG)
    await _agent(model_name, saver, [get_data_reworded]).ainvoke({"messages": [HumanMessage("Look up b")]}, _CFG)

    assert [thinking_count(p) for p in wire.payloads] == [0, 1, 2, 3]
    assert all_breaks(wire) == []
    assert all(p["tools"] == wire.payloads[0]["tools"] for p in wire.payloads)
    assert wire.payloads[0]["tools"][0]["description"] == "Look something up."
    redefined = [("tool_addition", "tool_definition", "get_data")]
    assert [_changes(p) for p in wire.payloads] == [[], [], [redefined], [redefined]]
    change = [m for m in wire.payloads[2]["messages"] if m["role"] == "system"][-1]["content"][-1]
    assert change["tool"]["definition"]["description"] == "Look something up, newest first."


@pytest.mark.asyncio
async def test_a_tool_not_offered_is_refused_when_called(monkeypatch):
    """Before its skill loads, the skill's tool is in the build but not offered,
    so a call for it gets the answer an unknown tool gets and does not run."""
    ran: list[str] = []

    @tool("draw_chart")
    def spy_chart(symbol: str) -> str:
        """Draw a chart."""
        ran.append(symbol)
        return "chart"

    Wire([reply(1, tool=("draw_chart", {"symbol": "NVDA"})), reply(2, text="Sorry.")]).install(monkeypatch)
    out = await _agent("claude-opus-5-5", InMemorySaver(), [read_file, get_data, spy_chart], skills=True).ainvoke(
        {"messages": [HumanMessage("Chart NVDA")]}, _CFG
    )

    result = next(m for m in out["messages"] if isinstance(m, ToolMessage))
    assert ran == []
    assert result.status == "error"
    assert result.content == "Error: draw_chart is not a valid tool, try one of [Read, get_data]."


@pytest.mark.asyncio
async def test_other_targets_get_the_tools_the_stack_chose(monkeypatch):
    """A model without mid-conversation tool changes keeps today's request:
    the filtered tools, no deferred entry, no change message, no betas."""
    wire = Wire(
        [
            reply(1, tool=("Read", {"file_path": "/home/user/.agents/skills/charts/SKILL.md"})),
            reply(2, text="Charted."),
        ]
    ).install(monkeypatch)
    model = anthropic_model("claude-opus-5-5")
    monkeypatch.setattr(model, "profile", {**model.profile, "mid_conversation_tools": False})
    agent = create_agent(
        model=model,
        tools=[read_file, get_data, draw_chart],
        system_prompt="You are a test agent.",
        middleware=[
            SkillsMiddleware(skill_registry={"charts": _CHARTS}, inject_manifest=False),
            ToolSurfaceMiddleware(),
        ],
        checkpointer=InMemorySaver(),
    )
    await agent.ainvoke({"messages": [HumanMessage("Chart NVDA")]}, _CFG)

    assert [_names(p) for p in wire.payloads] == [["Read", "get_data"], ["Read", "get_data", "draw_chart"]]
    assert not any("defer_loading" in t for p in wire.payloads for t in p["tools"])
    assert all(_changes(p) == [] and "betas" not in p for p in wire.payloads)
