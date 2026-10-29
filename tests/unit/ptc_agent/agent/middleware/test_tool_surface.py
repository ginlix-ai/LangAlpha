"""The tool surface across a compaction, and on the compaction request itself.

A compaction is the one boundary where the array may be rendered afresh, and
only when no thinking survives it; the net changes a compaction block records
are already in force where the block is adopted, so the ledger must not state
them a second time; and the compaction request carries the array the kept
thinking was produced under, with nothing new in it.
"""

from __future__ import annotations

import pytest
from langchain.agents.middleware.types import ExtendedModelResponse, ModelRequest, ModelResponse
from langchain_anthropic.chat_models import convert_to_anthropic_tool
from langchain_core.messages import AIMessage, HumanMessage, ToolMessage
from langchain_core.tools import tool

from ptc_agent.agent.middleware.tool_surface import (
    TOOL_SURFACE_KEY,
    ToolSurfaceMiddleware,
    is_tool_change_message,
)
from ptc_agent.agent.middleware.runtime_context.changes import UPDATE_SCHEMA_VERSION
from ptc_agent.agent.middleware.runtime_context.durable import DurableUpdate, build_update_message
from ptc_agent.agent.middleware.runtime_context.turn import NOTES_CHECK_IN_ROW_KIND
from src.llms.operator_channel import INLINE_TOOLS_BETA
from src.llms.provider_compaction import SIDECAR_KEY, compaction_settings
from tests.unit.ptc_agent.agent.prefix_stability import anthropic_model


@tool
def get_data(query: str) -> str:
    """Look something up."""
    return query


@tool("get_data")
def get_data_reworded(query: str) -> str:
    """Look something up, newest first."""
    return query


@tool
def place_order(symbol: str) -> str:
    """Place an order."""
    return symbol


def _wire(t) -> dict:
    return dict(convert_to_anthropic_tool(t))


def _summary(tool_changes: list | None = None) -> HumanMessage:
    block = {"type": "compaction", "content": "summary", "signature": "sig-c"}
    if tool_changes is not None:
        block["tool_changes"] = tool_changes
    return HumanMessage(
        "[Context Summary]\nsummary",
        id="summary-1",
        additional_kwargs={
            "lc_source": "summarization",
            SIDECAR_KEY: {"block": block, "origin": "anthropic", "text": "summary"},
        },
    )


_KEPT = AIMessage(
    content=[{"type": "thinking", "thinking": "", "signature": "sig-1"}, {"type": "text", "text": "ok"}],
    response_metadata={"model_name": "claude-opus-5-5"},
)
_ADD_ORDER = {"type": "tool_addition", "tool": {"type": "tool_reference", "name": "place_order"}}


async def _call(messages, tools, surface, settings=None):
    seen: list[ModelRequest] = []

    async def handler(request):
        seen.append(request)
        return ModelResponse(result=[AIMessage("answer")])

    request = ModelRequest(
        model=anthropic_model("claude-opus-5-5"),
        messages=messages,
        system_prompt="You are a test agent.",
        state={TOOL_SURFACE_KEY: surface},
        tools=tools,
        model_settings=settings or {},
    )
    result = await ToolSurfaceMiddleware().awrap_model_call(request, handler)
    update = result.command.update[TOOL_SURFACE_KEY] if isinstance(result, ExtendedModelResponse) else None
    return seen[0], update


@pytest.mark.asyncio
async def test_a_compaction_that_kept_no_thinking_renders_the_array_afresh():
    sent, update = await _call(
        [_summary(), HumanMessage("next")],
        [get_data_reworded],
        {"tools": [_wire(get_data)], "epoch": None, "offered": ["get_data"]},
    )
    assert sent.tools == [_wire(get_data_reworded)]
    assert update["epoch"] == "summary-1"


@pytest.mark.asyncio
async def test_a_compaction_that_kept_thinking_keeps_the_array():
    sent, update = await _call(
        [_summary(), _KEPT, HumanMessage("next")],
        [get_data_reworded],
        {"tools": [_wire(get_data)], "epoch": None, "offered": ["get_data"]},
    )
    assert sent.tools == [_wire(get_data)]
    assert update is None


@pytest.mark.asyncio
async def test_another_lineages_thinking_does_not_keep_the_array():
    """DeepSeek's thinking reaches Claude stripped, so it binds nothing."""
    kept = _KEPT.model_copy(update={"response_metadata": {"model_name": "deepseek-flash"}})
    sent, update = await _call(
        [_summary(), kept, HumanMessage("next")],
        [get_data_reworded],
        {"tools": [_wire(get_data)], "epoch": None, "offered": ["get_data"]},
    )
    assert sent.tools == [_wire(get_data_reworded)]
    assert update["epoch"] == "summary-1"


@pytest.mark.asyncio
async def test_changes_a_compaction_block_records_are_not_stated_again():
    surface = {
        "tools": [_wire(get_data), {**_wire(place_order), "defer_loading": True}],
        "epoch": None,
        "offered": ["get_data", "place_order"],
    }
    sent, _ = await _call(
        [_summary([_ADD_ORDER]), _KEPT, HumanMessage("next")], [get_data, place_order], surface
    )
    assert not any(is_tool_change_message(m) for m in sent.messages)
    # The block's changes ride the request, and so does the beta they need.
    assert INLINE_TOOLS_BETA in sent.model_settings["betas"]


@pytest.mark.asyncio
async def test_changes_a_compaction_block_does_not_record_are_stated_again():
    surface = {
        "tools": [_wire(get_data), {**_wire(place_order), "defer_loading": True}],
        "epoch": None,
        "offered": ["get_data", "place_order"],
    }
    sent, _ = await _call([_summary(), _KEPT, HumanMessage("next")], [get_data, place_order], surface)
    assert sent.messages[-1].content == [_ADD_ORDER]


@pytest.mark.asyncio
async def test_the_compaction_request_carries_the_array_and_nothing_new():
    model = anthropic_model("claude-opus-5-5")
    surface = {"tools": [_wire(get_data)], "epoch": None, "offered": ["get_data"]}
    sent, update = await _call(
        [HumanMessage("first"), _KEPT, HumanMessage("second")],
        [get_data, place_order],
        surface,
        settings=compaction_settings(model, {}, "Summarize."),
    )
    assert sent.tools == [_wire(get_data)]
    assert not any(is_tool_change_message(m) for m in sent.messages)
    assert update is None


@pytest.mark.asyncio
async def test_a_tool_the_array_declares_is_offered_again_by_reference():
    """A tool a fresh render kept as a deferred entry is named, not defined
    a second time."""
    surface = {
        "tools": [_wire(get_data), {**_wire(place_order), "defer_loading": True}],
        "epoch": None,
        "offered": ["get_data"],
    }
    sent, update = await _call([HumanMessage("first"), _KEPT, HumanMessage("second")], [get_data, place_order], surface)
    assert sent.tools == surface["tools"]
    assert sent.messages[-1].content == [_ADD_ORDER]
    assert update["offered"] == ["get_data", "place_order"]


@pytest.mark.asyncio
async def test_a_tool_the_array_lacks_is_defined_in_the_change():
    surface = {"tools": [_wire(get_data)], "epoch": None, "offered": ["get_data"]}
    sent, update = await _call([HumanMessage("first"), _KEPT, HumanMessage("second")], [get_data, place_order], surface)
    assert sent.tools == [_wire(get_data)]
    assert sent.messages[-1].content == [
        {"type": "tool_addition", "tool": {"type": "tool_definition", "definition": _wire(place_order)}}
    ]
    assert update == {"tools": [_wire(get_data)], "epoch": None, "offered": ["get_data", "place_order"]}
    assert INLINE_TOOLS_BETA in sent.model_settings["betas"]


@pytest.mark.asyncio
async def test_a_redefinition_waits_for_a_turn_start():
    """A deploy reworded a tool mid-loop: right after tool results a model may
    pass over the system message, so the new definition goes out at the next
    turn start, under the same name, with the array as first sent."""
    surface = {"tools": [_wire(get_data)], "epoch": None, "offered": ["get_data"]}
    call = AIMessage(
        content=[{"type": "thinking", "thinking": "", "signature": "sig-1"}],
        tool_calls=[{"name": "get_data", "args": {"query": "a"}, "id": "c1", "type": "tool_call"}],
    )
    loop = [HumanMessage("first"), call, ToolMessage("a", tool_call_id="c1")]
    sent, update = await _call(loop, [get_data_reworded], surface)
    assert not any(is_tool_change_message(m) for m in sent.messages)
    assert update is None

    # A row written before a mid-turn call, such as a notes check-in, leaves
    # the call mid-turn.
    row = build_update_message(
        DurableUpdate(kind=NOTES_CHECK_IN_ROW_KIND, schema_version=UPDATE_SCHEMA_VERSION, text="check in")
    )
    sent, update = await _call([*loop, row], [get_data_reworded], surface)
    assert not any(is_tool_change_message(m) for m in sent.messages)
    assert update is None

    sent, update = await _call([*loop, AIMessage("done"), HumanMessage("second")], [get_data_reworded], surface)
    assert sent.tools == [_wire(get_data)]
    assert sent.messages[-1].content == [
        {"type": "tool_addition", "tool": {"type": "tool_definition", "definition": _wire(get_data_reworded)}}
    ]
    assert update is None
