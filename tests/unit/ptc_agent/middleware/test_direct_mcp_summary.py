from __future__ import annotations

from langchain_core.tools import StructuredTool

from ptc_agent.agent.middleware.direct_mcp import METADATA_KEY, direct_tool_summary


def _tool(name: str, description: str, approval: bool = False) -> StructuredTool:
    tool = StructuredTool.from_function(
        func=lambda: None, name=name, description=description
    )
    tool.metadata = {
        METADATA_KEY: {"server": "moomoo", "tool": name, "approval": approval}
    }
    return tool


def test_summary_is_one_line_per_tool_cut_to_the_first_line():
    out = direct_tool_summary(
        [
            _tool("mcp__moomoo__order", "Simulate an order.\nLong detail follows."),
            _tool("mcp__moomoo__cash", "x" * 200),
        ]
    )
    lines = out.splitlines()
    assert lines[0] == "- `mcp__moomoo__order`: Simulate an order."
    assert lines[1].endswith("...")
    assert len(lines[1]) < 200


def test_only_a_gated_tool_says_it_asks_first():
    out = direct_tool_summary(
        [
            _tool("mcp__moomoo__order", "Place an order.", True),
            _tool("mcp__moomoo__cash", "Read the balance."),
        ]
    )
    lines = out.splitlines()
    assert (
        lines[0]
        == "- `mcp__moomoo__order`: Place an order."
        " Asks the user to confirm before it runs."
    )
    assert "confirm" not in lines[1]
