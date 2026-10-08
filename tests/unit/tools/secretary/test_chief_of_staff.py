"""The Chief of Staff hands work to a workspace it names, never its own.

A hand-off into Home starts a turn in Home, which is the Chief of Staff again
rather than any workspace's analyst, so it is refused before the user is asked.
A hand-off naming no workspace is refused too: the Chief of Staff creates a
new workspace itself, under that workspace's own approval, before handing off.
A delete of Home is refused before the user is asked too, and the card for any
other delete names the workspace it would lose.
"""

from __future__ import annotations

import json
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.tools.secretary.chief_of_staff import chief_of_staff_workspaces, delegate_to_analyst

USER_ID = "user-1"
HOME_ID = "33333333-3333-3333-3333-333333333333"
THREAD_ID = "11111111-1111-1111-1111-111111111111"


def _call(args: dict) -> dict:
    # ``state`` is what the graph injects; nothing here was approved in advance.
    return {"name": "delegate_to_analyst", "args": {**args, "state": {}}, "id": "call_1", "type": "tool_call"}


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "args",
    [
        {"question": "Value NVDA", "workspace_id": HOME_ID},
        {"question": "And the margins?", "thread_id": THREAD_ID},
        # The owner check reads any spelling the database accepts as the same row.
        {"question": "Value NVDA", "workspace_id": f"urn:uuid:{HOME_ID.upper()}"},
    ],
    ids=["new thread", "continuation", "another spelling of Home's id"],
)
async def test_a_hand_off_into_home_is_refused_before_the_user_is_asked(args):
    confirm = MagicMock(side_effect=AssertionError("the user must not be asked"))
    with patch(
        "src.server.database.conversation.threads_read.get_thread_owner_id",
        AsyncMock(return_value=USER_ID),
    ), patch(
        "src.server.database.conversation.threads_read.get_thread_by_id",
        AsyncMock(return_value={"workspace_id": HOME_ID}),
    ), patch(
        "src.server.database.workspace.get_workspace",
        AsyncMock(return_value={"user_id": USER_ID, "name": "Home"}),
    ), patch("src.tools.secretary.dispatch.hitl_confirm", confirm):
        result = await delegate_to_analyst.ainvoke(
            _call(args),
            config={"configurable": {"user_id": USER_ID, "workspace_id": HOME_ID}},
        )

    payload = json.loads(result.update["messages"][0].content)
    assert payload["success"] is False
    assert "workspace you are working in" in payload["error"]


@pytest.mark.asyncio
async def test_a_hand_off_without_a_workspace_is_refused_before_anything_is_created():
    confirm = MagicMock(side_effect=AssertionError("the user must not be asked"))
    create = AsyncMock(side_effect=AssertionError("no workspace may be created"))
    with patch("src.tools.secretary.dispatch.hitl_confirm", confirm), patch(
        "src.server.services.workspace_manager.WorkspaceManager.get_instance",
        MagicMock(return_value=MagicMock(create_workspace=create)),
    ):
        result = await delegate_to_analyst.ainvoke(
            _call({"question": "Start on lithium miners"}),
            config={"configurable": {"user_id": USER_ID, "workspace_id": HOME_ID}},
        )

    payload = json.loads(result.update["messages"][0].content)
    assert payload["success"] is False
    assert 'manage_workspaces(action="create")' in payload["error"]


def _delete_call(workspace_id: str) -> dict:
    return {
        "name": "manage_workspaces",
        "args": {"action": "delete", "workspace_id": workspace_id, "state": {}},
        "id": "call_1",
        "type": "tool_call",
    }


@pytest.mark.asyncio
async def test_deleting_home_is_refused_before_the_user_is_asked():
    confirm = MagicMock(side_effect=AssertionError("the user must not be asked"))
    with patch(
        "src.server.database.workspace.get_workspace",
        AsyncMock(return_value={"user_id": USER_ID, "workspace_id": HOME_ID, "status": "flash", "name": "Home"}),
    ), patch("src.tools.secretary.tools.hitl_confirm", confirm):
        result = await chief_of_staff_workspaces.ainvoke(
            _delete_call(HOME_ID),
            config={"configurable": {"user_id": USER_ID, "workspace_id": HOME_ID}},
        )

    payload = json.loads(result.update["messages"][0].content)
    assert payload["success"] is False
    assert "Home cannot be deleted" in payload["error"]


@pytest.mark.asyncio
async def test_the_delete_card_names_the_workspace():
    workspace_id = "44444444-4444-4444-4444-444444444444"
    confirm = MagicMock(return_value=(False, None))
    with patch(
        "src.server.database.workspace.get_workspace",
        AsyncMock(return_value={"user_id": USER_ID, "workspace_id": workspace_id, "status": "running", "name": "Lithium"}),
    ), patch("src.tools.secretary.tools.hitl_confirm", confirm):
        await chief_of_staff_workspaces.ainvoke(
            _delete_call(workspace_id),
            config={"configurable": {"user_id": USER_ID, "workspace_id": HOME_ID}},
        )

    confirm.assert_called_once_with(
        "delete_workspace", {"workspace_id": workspace_id, "workspace_name": "Lithium"},
    )
