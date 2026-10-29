"""A pattern no search route runs is an error, not a search with no lines."""

from unittest.mock import AsyncMock, MagicMock

import pytest

from ptc_agent.agent.tools.grep import _needs_backtracking, create_grep_tool


@pytest.mark.asyncio
@pytest.mark.parametrize("pattern", ["(?<=def )run", "(?!x)y", r"(a)\1", "(?>ab)"])
async def test_a_pattern_no_route_runs_is_reported_not_searched(pattern):
    backend = MagicMock()
    backend.agrep_rich = AsyncMock(return_value=[])

    result = await create_grep_tool(backend).ainvoke({"pattern": pattern})

    assert result.startswith("ERROR: Invalid regex pattern:")
    backend.agrep_rich.assert_not_awaited()


@pytest.mark.parametrize("pattern", ["(?x) def run", r"\bdef\b", r"\(?=", "a*+"])
def test_syntax_rg_runs_is_searched(pattern):
    assert not _needs_backtracking(pattern)
