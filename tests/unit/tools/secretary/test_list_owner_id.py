"""The workspace listing answers with a short row per workspace.

The tool reads the user from the run config, so the owner's user_id is never
something the model passes back, and a listing comes before every hand-off,
so the row's settings, artifacts and machine bindings stay out of it. The
workspace id the model does pass back stays.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from unittest.mock import AsyncMock, patch

import pytest

from src.tools.secretary import tools

pytestmark = pytest.mark.asyncio

OWNER = "owner-5c81e2"


async def test_workspace_list_rows_carry_only_what_a_hand_off_reads():
    row = {
        "workspace_id": "ws-1",
        "user_id": OWNER,
        "name": "Semis",
        "description": "Chip makers",
        "sandbox_id": "sbx-1",
        "computer_id": "cmp-1",
        "dir_name": "semis",
        "previous_dir_names": ["chips"],
        "status": "running",
        "created_at": datetime(2026, 9, 30, 12, 0, tzinfo=timezone.utc),
        "last_activity_at": None,
        "config": {"mcp": {"servers": ["x"]}},
        "artifacts": [{"path": "report.md"}],
        "mcp_config_version": 4,
    }
    with patch(
        "src.server.database.workspace.get_workspaces_for_user",
        AsyncMock(return_value=([row], 1)),
    ), patch(
        "src.server.database.computer.get_computer_roster",
        AsyncMock(
            return_value=[{"computer_id": "cmp-1", "name": "Cloud computer", "is_home": True}]
        ),
    ):
        command = await tools.workspaces_list(OWNER, "call-1")
    content = command.update["messages"][0].content
    assert OWNER not in content
    listed = json.loads(content)["workspaces"]
    assert listed == [
        {
            "workspace_id": "ws-1",
            "name": "Semis",
            "description": "Chip makers",
            "dir_name": "semis",
            "status": "running",
            "created_at": "2026-09-30 12:00:00+00:00",
            "last_activity_at": None,
        }
    ]


async def test_each_row_names_its_folder_and_with_two_computers_its_computer():
    """The Chief of Staff reaches every computer's folders, so each row names
    its folder; with more than one computer each also names the computer it
    is on, the one to pass as ``computer``, and whether that is the user's own."""
    from src.tools.secretary.chief_of_staff import chief_of_staff_workspaces

    rows = [
        {"workspace_id": "ws-here", "computer_id": "cmp-home", "dir_name": "semis"},
        {"workspace_id": "ws-there", "computer_id": "cmp-other", "dir_name": "macro"},
    ]
    read_computers = AsyncMock(
        return_value=[
            {
                "computer_id": "cmp-home",
                "name": "Cloud computer",
                "is_home": True,
                "root_dir": "/home/workspace",
            },
            {
                "computer_id": "cmp-other",
                "name": "Big box",
                "is_home": False,
                "kind": "ssh",
                "provider_config": {"os": "Linux"},
                "root_dir": "/home/me/langalpha",
            },
        ]
    )
    with patch(
        "src.server.database.workspace.get_workspaces_for_user",
        AsyncMock(return_value=(rows, 2)),
    ), patch("src.server.database.computer.get_computer_roster", read_computers):
        command = await chief_of_staff_workspaces.ainvoke(
            {
                "name": "manage_workspaces",
                "args": {"action": "list", "state": {}},
                "id": "call-1",
                "type": "tool_call",
            },
            config={"configurable": {"user_id": OWNER}},
        )
    listed = json.loads(command.update["messages"][0].content)["workspaces"]
    assert {
        ws["workspace_id"]: (ws["dir_name"], ws["computer"], ws.get("machine"))
        for ws in listed
    } == {
        "ws-here": ("semis", "Cloud computer", None),
        "ws-there": ("macro", "Big box", "the user's own machine"),
    }
    assert "machine" not in listed[0]
    # Calls on another computer take its own paths, so its folder comes whole.
    assert "path" not in listed[0]
    assert listed[1]["path"] == "/home/me/langalpha/macro"
    read_computers.assert_awaited_once_with(OWNER)
