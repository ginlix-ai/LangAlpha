"""The `<workspace>` block's name comes from the row, read once per turn.

Reading it where the turn's agent is built (rather than inside the model call)
is what makes it a per-turn value: the model never sees the name change under
it mid-answer, and there is no cache to invalidate on a rename.
"""

from unittest.mock import AsyncMock, patch

import pytest

from ptc_agent.agent.graph import _read_workspace_naming

WS = "a0000001-0000-4000-8000-000000000001"


def _patched_row(row=None, error=None):
    mock = AsyncMock(side_effect=error) if error else AsyncMock(return_value=row)
    return patch(
        "src.server.database.workspace.get_workspace_name_and_description", mock
    ), mock


@pytest.mark.asyncio
async def test_the_name_and_description_come_from_the_row():
    ctx, _ = _patched_row({"name": "Q3 Semis", "description": "Chip supply chain."})
    with ctx:
        assert await _read_workspace_naming(WS) == ("Q3 Semis", "Chip supply chain.")


@pytest.mark.asyncio
async def test_null_columns_read_as_empty_not_as_the_string_none():
    ctx, _ = _patched_row({"name": "Scratch", "description": None})
    with ctx:
        assert await _read_workspace_naming(WS) == ("Scratch", "")


@pytest.mark.asyncio
async def test_an_unreadable_row_costs_the_block_not_the_turn():
    # The prompt is worth less than the answer: a database that will not
    # answer must not take the turn down with it.
    ctx, _ = _patched_row(error=RuntimeError("db down"))
    with ctx:
        assert await _read_workspace_naming(WS) == ("", "")


@pytest.mark.asyncio
async def test_a_missing_row_yields_no_name():
    ctx, _ = _patched_row(None)
    with ctx:
        assert await _read_workspace_naming(WS) == ("", "")


@pytest.mark.asyncio
async def test_the_narrow_reader_is_used_not_the_whole_row():
    # get_workspace pulls the JSONB config/artifacts columns; this runs once
    # per turn to produce two short strings.
    ctx, mock = _patched_row({"name": "Scratch", "description": ""})
    with ctx, patch("src.server.database.workspace.get_workspace") as whole_row:
        await _read_workspace_naming(WS)
    mock.assert_awaited_once_with(WS)
    whole_row.assert_not_called()
