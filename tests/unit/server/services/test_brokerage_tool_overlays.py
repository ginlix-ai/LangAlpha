"""What an overlay may do to a vendor's published schema, and to nobody else's."""

from __future__ import annotations

from src.server.services.brokerage_tool_overlays import overlay_tool_schemas


def _tool(name: str, **props) -> dict:
    return {"name": name, "input_schema": {"type": "object", "properties": props}}


def test_overlay_folds_the_enum_into_the_parameter_and_copies_only_that_tool():
    tools = [
        _tool(
            "sim_trade_input_order",
            order_side={"type": "integer", "description": "Side"},
        ),
        _tool("account_list"),
    ]
    out = overlay_tool_schemas("moomoo", tools)
    assert "1=BUY" in out[0]["input_schema"]["properties"]["order_side"]["description"]
    assert tools[0]["input_schema"]["properties"]["order_side"]["description"] == "Side"
    assert out[1] is tools[1]
    assert overlay_tool_schemas("nobody", tools) is tools
