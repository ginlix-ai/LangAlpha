"""The `<workspace>` block's name and computer come from the rows, read once per turn.

Reading it where the turn's agent is built (rather than inside the model call)
is what makes it a per-turn value: the model never sees the name change under
it mid-answer, and there is no cache to invalidate on a rename.
"""

from unittest.mock import AsyncMock, patch

import pytest

from ptc_agent.agent.graph import _read_workspace_naming
from ptc_agent.agent.middleware.runtime_context.epoch import Workspace

WS = "a0000001-0000-4000-8000-000000000001"


def _patched_row(row=None, error=None):
    mock = AsyncMock(side_effect=error) if error else AsyncMock(return_value=row)
    return patch("src.server.database.workspace.get_workspace_naming", mock), mock


@pytest.mark.asyncio
async def test_the_name_and_description_come_from_the_row():
    ctx, _ = _patched_row({"name": "Q3 Semis", "description": "Chip supply chain."})
    with ctx:
        assert await _read_workspace_naming(WS) == Workspace("Q3 Semis", "Chip supply chain.")


@pytest.mark.asyncio
async def test_null_columns_read_as_empty_not_as_the_string_none():
    ctx, _ = _patched_row({"name": "Scratch", "description": None})
    with ctx:
        assert await _read_workspace_naming(WS) == Workspace("Scratch")


@pytest.mark.asyncio
async def test_an_unreadable_row_costs_the_block_not_the_turn():
    # The prompt is worth less than the answer: a database that will not
    # answer must not take the turn down with it. It answers None, not an
    # empty name, so the baseline does not file a row saying the name is gone.
    ctx, _ = _patched_row(error=RuntimeError("db down"))
    with ctx:
        assert await _read_workspace_naming(WS) is None


@pytest.mark.asyncio
async def test_a_missing_row_yields_no_name():
    ctx, _ = _patched_row(None)
    with ctx:
        assert await _read_workspace_naming(WS) == Workspace()


@pytest.mark.asyncio
async def test_the_narrow_reader_is_used_not_the_whole_row():
    # get_workspace pulls the JSONB config/artifacts columns; this runs once
    # per turn to produce a few short strings.
    ctx, mock = _patched_row({"name": "Scratch", "description": ""})
    with ctx, patch("src.server.database.workspace.get_workspace") as whole_row:
        await _read_workspace_naming(WS)
    mock.assert_awaited_once_with(WS)
    whole_row.assert_not_called()


@pytest.mark.asyncio
async def test_the_computer_is_named_when_the_user_has_another():
    ctx, _ = _patched_row(
        {"name": "Q3 Semis", "description": "", "computer_name": "Big box", "computers": 2}
    )
    with ctx:
        assert await _read_workspace_naming(WS) == Workspace("Q3 Semis", "", "Big box")


@pytest.mark.asyncio
async def test_home_names_its_folder_and_its_computer():
    from src.server.database.workspace_names import HOME_FOLDER

    ctx, _ = _patched_row(
        {"name": "Home", "description": None, "computer_name": "Cloud computer", "computers": 2}
    )
    with ctx:
        workspace = await _read_workspace_naming(WS, "chief_of_staff")
    assert (workspace.name, workspace.computer) == (HOME_FOLDER, "Cloud computer")


@pytest.mark.asyncio
@pytest.mark.parametrize("role", ["analyst", "chief_of_staff"])
@pytest.mark.parametrize(
    ("kind", "provider_config", "machine"),
    [
        ("ssh", {"helper": True, "os": "Darwin"}, "personal_mac"),
        ("ssh", {"helper": True, "os": "Linux"}, "personal"),
        ("ssh", {"helper": False, "os": "Darwin"}, "own_mac"),
        # Added before its system was kept.
        ("ssh", {"host": "203.0.113.10"}, "own"),
        ("daytona", {"helper": True, "os": "Darwin"}, ""),
        (None, None, ""),
    ],
)
async def test_whose_machine_it_is_is_told_with_one_computer_too(
    role, kind, provider_config, machine
):
    """Where the work runs is the user's machine whether or not a second
    computer gets it named."""
    ctx, _ = _patched_row(
        {
            "name": "Semis",
            "description": "",
            "computer_name": "Studio Mac",
            "computers": 1,
            "computer_kind": kind,
            "provider_config": provider_config,
            "root_dir": "/Users/me/langalpha",
        }
    )
    with ctx:
        workspace = await _read_workspace_naming(WS, role)
    # The folder goes with a machine of theirs, and a hosted one's is no rule.
    root = "/Users/me/langalpha" if machine else ""
    assert (workspace.computer, workspace.machine, workspace.root) == ("", machine, root)
