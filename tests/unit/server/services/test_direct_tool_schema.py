"""A vendor schema that combines at the root, bound on the direct path.

langchain-anthropic drops such a tool on every route speaking the Anthropic
format, so a brokerage's order tool written that way would vanish while its
reads stayed bound.
"""

import pytest
from langchain_anthropic import ChatAnthropic

from src.server.services.egress import direct_tools
from src.server.services.mcp_tool_split import DirectServerTools, DirectTool
from src.server.services.tool_binding import Resolved


async def _bound(monkeypatch, input_schema: dict):
    monkeypatch.setattr(direct_tools, "EGRESS_RELAY_SECRET", "s")
    schema = {"name": "get_order", "description": "d", "input_schema": input_schema}
    binding = await direct_tools.prepare_direct_mcp_tools(
        user_id="u",
        workspace_id="w",
        sandbox_id="sb",
        grants={"broker": "grant-1"},
        by_server={"broker": DirectServerTools(tools=(DirectTool(schema, Resolved("direct", "policy")),))},
    )
    [tool] = binding.tools
    sent = ChatAnthropic(model="claude-haiku-5-5", api_key="k").bind_tools([tool]).kwargs["tools"]
    [sent_tool] = sent
    return sent_tool["input_schema"]


@pytest.mark.asyncio
async def test_a_choice_of_arguments_reaches_an_anthropic_route(monkeypatch):
    schema = await _bound(
        monkeypatch,
        {
            "type": "object",
            "properties": {"account": {"type": "string"}},
            "required": ["account"],
            "anyOf": [
                {"properties": {"order_id": {"type": "string"}}, "required": ["order_id"]},
                {"properties": {"ticker": {"type": "string"}}, "required": ["ticker"]},
            ],
        },
    )
    assert "anyOf" not in schema
    assert set(schema["properties"]) == {"account", "order_id", "ticker"}
    # Only what every branch asks for is required; the vendor checks the rest.
    assert schema["required"] == ["account"]


@pytest.mark.asyncio
async def test_every_part_of_an_all_of_stays_required(monkeypatch):
    schema = await _bound(
        monkeypatch,
        {
            "type": "object",
            "allOf": [
                {"properties": {"ticker": {"type": "string"}}, "required": ["ticker"]},
                {"properties": {"qty": {"type": "number"}}, "required": ["qty"]},
            ],
        },
    )
    assert "allOf" not in schema
    assert schema["required"] == ["qty", "ticker"]


@pytest.mark.asyncio
async def test_a_choice_keeps_every_value_its_branches_allow(monkeypatch):
    schema = await _bound(
        monkeypatch,
        {
            "type": "object",
            "oneOf": [
                {"properties": {"side": {"const": "buy"}}, "required": ["side"]},
                {"properties": {"side": {"const": "sell"}}, "required": ["side"]},
            ],
        },
    )
    assert schema["properties"]["side"] == {"anyOf": [{"const": "buy"}, {"const": "sell"}]}
    assert schema["required"] == ["side"]


@pytest.mark.asyncio
async def test_a_branch_named_by_reference_brings_its_arguments(monkeypatch):
    schema = await _bound(
        monkeypatch,
        {
            "type": "object",
            "$defs": {
                "order": {
                    "type": "object",
                    "properties": {"ticker": {"type": "string"}, "qty": {"type": "number"}},
                    "required": ["ticker", "qty"],
                },
            },
            "allOf": [{"$ref": "#/$defs/order"}],
        },
    )
    assert set(schema["properties"]) == {"ticker", "qty"}
    assert schema["required"] == ["qty", "ticker"]


@pytest.mark.asyncio
async def test_a_referenced_branch_composed_of_parts_brings_their_arguments(monkeypatch):
    schema = await _bound(
        monkeypatch,
        {
            "type": "object",
            "$defs": {
                "buy": {
                    "allOf": [
                        {"properties": {"ticker": {"type": "string"}}, "required": ["ticker"]},
                        {"properties": {"limit": {"type": "number"}}},
                    ],
                },
                "node": {"anyOf": [{"$ref": "#/$defs/node"}, {"properties": {"note": {"type": "string"}}}]},
            },
            "oneOf": [{"$ref": "#/$defs/buy"}, {"$ref": "#/$defs/node"}],
        },
    )
    # A definition that names itself is folded once, not followed forever.
    assert set(schema["properties"]) == {"ticker", "limit", "note"}
    assert "required" not in schema


@pytest.mark.asyncio
async def test_a_chain_of_refs_deeper_than_the_stack_still_binds(monkeypatch):
    defs = {f"d{i}": {"allOf": [{"$ref": f"#/$defs/d{i + 1}"}]} for i in range(1500)}
    schema = await _bound(monkeypatch, {"type": "object", "$defs": defs, "oneOf": [{"$ref": "#/$defs/d0"}]})
    assert schema["type"] == "object"
