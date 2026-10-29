"""A machine tool's call that names a computer the tool cannot take is refused.

The tool's schema would drop the argument and run the call on this computer,
whose result then reads as the named one's.
"""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from langchain_core.messages import ToolMessage
from langchain_core.tools import tool

from ptc_agent.agent.middleware.tool.computer_guard import (
    ComputerArgumentGuardMiddleware,
)


@tool("Bash")
async def bash(command: str) -> str:
    """Run a command."""
    return command


@tool("Bash")
async def reaching_bash(command: str, computer: str | None = None) -> str:
    """Run a command, anywhere."""
    return command


@tool("WebSearch")
async def web_search(query: str) -> str:
    """Search."""
    return query


def _request(the_tool, args):
    call = {"name": the_tool.name, "args": args, "id": "call-1", "type": "tool_call"}
    return SimpleNamespace(tool_call=call, tool=the_tool)


async def _run(the_tool, args):
    handler = AsyncMock(return_value="ran")
    result = await ComputerArgumentGuardMiddleware().awrap_tool_call(
        _request(the_tool, args), handler
    )
    return result, handler


@pytest.mark.asyncio
async def test_a_computer_the_tool_takes_no_argument_for_is_refused():
    result, handler = await _run(bash, {"command": "ls ~/Downloads", "computer": "Studio Mac"})
    handler.assert_not_awaited()
    assert isinstance(result, ToolMessage)
    assert result.status == "error"
    assert result.tool_call_id == "call-1"
    assert "did not run" in result.content


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "the_tool,args",
    [
        (reaching_bash, {"command": "ls", "computer": "Studio Mac"}),
        (bash, {"command": "ls"}),
        (bash, {"command": "ls", "computer": "  "}),
        (web_search, {"query": "x", "computer": "Studio Mac"}),
    ],
    ids=["takes-it", "names-none", "blank", "not-a-machine-tool"],
)
async def test_every_other_call_runs(the_tool, args):
    result, handler = await _run(the_tool, args)
    handler.assert_awaited_once()
    assert result == "ran"


def test_the_sync_path_refuses_too():
    handler = AsyncMock()
    result = ComputerArgumentGuardMiddleware().wrap_tool_call(
        _request(bash, {"command": "ls", "computer": "Studio Mac"}), handler
    )
    handler.assert_not_called()
    assert result.status == "error"
