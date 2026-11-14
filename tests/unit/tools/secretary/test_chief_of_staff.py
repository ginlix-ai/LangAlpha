"""The Chief of Staff hands work to a workspace it names, never its own.

A hand-off into Home starts a turn in Home, which is the Chief of Staff again
rather than any workspace's analyst, so it is refused before the user is asked.
A hand-off naming no workspace is refused too: the Chief of Staff creates a
new workspace itself with ``la``, under that workspace's own approval, before
handing off.
"""

from __future__ import annotations

import json
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.tools.secretary.chief_of_staff import CHIEF_OF_STAFF_TOOLS, delegate_to_analyst

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
async def test_a_hand_off_into_the_home_stand_in_is_refused_before_the_user_is_asked():
    from src.server.database.home_workspace import HOME_STAND_IN_KEY

    stand_in_id = "44444444-4444-4444-4444-444444444444"
    confirm = MagicMock(side_effect=AssertionError("the user must not be asked"))
    with patch(
        "src.server.database.workspace.get_workspace",
        AsyncMock(return_value={"user_id": USER_ID, "name": "stand-in", "config": {HOME_STAND_IN_KEY: True}}),
    ), patch("src.tools.secretary.dispatch.hitl_confirm", confirm):
        result = await delegate_to_analyst.ainvoke(
            _call({"question": "Value NVDA", "workspace_id": stand_in_id}),
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
    assert "`la workspaces create`" in payload["error"]


@pytest.mark.asyncio
@pytest.mark.parametrize("model", ["gpt-6.1-terra", None])
async def test_the_hand_off_card_names_the_model_the_call_named(model):
    """The user approves the model along with the work; a declined card
    stops before anything is sent."""
    confirm = MagicMock(return_value=(False, {}))
    args = {"question": "Value NVDA", "workspace_id": "ws-1"}
    if model:
        args["model"] = model
    with patch(
        "src.server.database.workspace.get_workspace",
        AsyncMock(return_value={"user_id": USER_ID, "name": "Semis"}),
    ), patch("src.tools.secretary.dispatch.hitl_confirm", confirm):
        await delegate_to_analyst.ainvoke(
            _call(args),
            config={"configurable": {"user_id": USER_ID, "workspace_id": HOME_ID}},
        )

    kind, payload = confirm.call_args.args
    assert kind == "ptc_agent"
    assert payload["model"] == model


def test_the_hand_off_is_the_chief_of_staffs_only_tool():
    """Workspaces and threads are listed, read, stopped and deleted with ``la``
    from Bash, whose deletes and new workspaces the app asks the user about."""
    assert [t.name for t in CHIEF_OF_STAFF_TOOLS] == ["delegate_to_analyst"]
