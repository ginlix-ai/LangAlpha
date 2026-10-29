"""A refused workspace name reaches the user as a message, never a traceback."""

from unittest.mock import AsyncMock, patch

import httpx
import pytest
from ptc_cli.agent.lifecycle import _create_named_workspace
from ptc_cli.api.client import (
    SSEStreamClient,
    WorkspaceNameInvalidError,
    WorkspaceNameTakenError,
)
from ptc_cli.commands.slash import _select_or_create_workspace_interactive


def _client_answering(status: int, detail: object) -> SSEStreamClient:
    client = SSEStreamClient(base_url="http://test", user_id="u-1")
    client.client = httpx.AsyncClient(
        transport=httpx.MockTransport(
            lambda _request: httpx.Response(status, json={"detail": detail})
        )
    )
    return client


@pytest.mark.asyncio
async def test_a_taken_name_carries_the_holder() -> None:
    client = _client_answering(
        409,
        {
            "code": "workspace_name_taken",
            "message": 'A workspace named "cli-bot" already exists.',
            "name": "cli-bot",
            "workspace_id": "ws-1",
        },
    )
    with pytest.raises(WorkspaceNameTakenError) as exc:
        await client.create_workspace(name="cli-bot")
    assert exc.value.workspace_id == "ws-1"


@pytest.mark.asyncio
async def test_an_invalid_name_carries_the_servers_message() -> None:
    message = '"tools" is reserved; choose another name.'
    client = _client_answering(
        400, {"code": "workspace_name_invalid", "message": message}
    )
    with pytest.raises(WorkspaceNameInvalidError, match="reserved"):
        await client.create_workspace(name="tools")


@pytest.mark.asyncio
async def test_a_new_workspace_is_numbered_past_a_taken_name() -> None:
    client = AsyncMock()
    client.create_workspace = AsyncMock(
        side_effect=[
            WorkspaceNameTakenError("taken", "ws-1"),
            {"workspace_id": "ws-2"},
        ]
    )
    result = await _create_named_workspace(client, "cli-bot", reuse_existing=False)
    assert result == ("ws-2", False)
    names = [c.kwargs["name"] for c in client.create_workspace.await_args_list]
    assert names == ["cli-bot", "cli-bot (2)"]


@pytest.mark.asyncio
async def test_a_long_agent_name_is_trimmed_to_fit_its_number() -> None:
    """The server refuses a name over 80 characters, so the number must fit."""
    client = AsyncMock()
    client.create_workspace = AsyncMock(
        side_effect=[WorkspaceNameTakenError("taken", "ws-1"), {"workspace_id": "ws-2"}]
    )
    await _create_named_workspace(client, "cli-" + "a" * 90, reuse_existing=False)
    names = [c.kwargs["name"] for c in client.create_workspace.await_args_list]
    assert names == ["cli-" + "a" * 76, "cli-" + "a" * 72 + " (2)"]


@pytest.mark.asyncio
async def test_a_numbered_name_keeps_its_number_inside_the_folder_limit() -> None:
    """The server cuts a folder to 255 bytes; a number past the cut keys every
    numbered name as the first, so each would come back taken."""
    client = AsyncMock()
    client.create_workspace = AsyncMock(
        side_effect=[WorkspaceNameTakenError("taken", "ws-1"), {"workspace_id": "ws-2"}]
    )
    await _create_named_workspace(client, "cli-" + "\U0001f600" * 63, reuse_existing=False)
    numbered = client.create_workspace.await_args_list[1].kwargs["name"]
    assert numbered == "cli-" + "\U0001f600" * 61 + " (2)"
    assert len(numbered.encode("utf-8")) <= 255


@pytest.mark.asyncio
async def test_a_name_the_request_model_refuses_is_an_invalid_name() -> None:
    client = _client_answering(
        422,
        [{"loc": ["body", "name"], "msg": "String should have at most 80 characters", "type": "string_too_long"}],
    )
    with pytest.raises(WorkspaceNameInvalidError, match="at most 80"):
        await client.create_workspace(name="x" * 81)


@pytest.mark.asyncio
async def test_a_persisted_session_reuses_the_workspace_holding_the_name() -> None:
    client = AsyncMock()
    client.create_workspace = AsyncMock(
        side_effect=WorkspaceNameTakenError("taken", "ws-1")
    )
    result = await _create_named_workspace(client, "cli-bot", reuse_existing=True)
    assert result == ("ws-1", True)


@pytest.mark.asyncio
async def test_the_interactive_create_asks_again_after_an_invalid_name() -> None:
    client = AsyncMock()
    client.list_workspaces = AsyncMock(return_value=[])
    client.create_workspace = AsyncMock(
        side_effect=[
            WorkspaceNameInvalidError('"tools" is reserved; choose another name.'),
            {"workspace_id": "ws-2"},
        ]
    )
    with (
        patch(
            "ptc_cli.commands.slash.create_interactive_menu",
            AsyncMock(return_value=(0, {"action": "create"})),
        ),
        patch("ptc_cli.commands.slash.console") as console,
    ):
        console.input.side_effect = ["tools", "research"]
        workspace_id = await _select_or_create_workspace_interactive(client)
    assert workspace_id == "ws-2"
    printed = " ".join(str(c.args[0]) for c in console.print.call_args_list)
    assert "reserved" in printed


@pytest.mark.asyncio
async def test_the_interactive_create_asks_again_after_a_taken_name() -> None:
    """Choosing to create never lands in the workspace already holding the name."""
    client = AsyncMock()
    client.list_workspaces = AsyncMock(return_value=[])
    client.create_workspace = AsyncMock(
        side_effect=[
            WorkspaceNameTakenError('A workspace named "Research" already exists.', "ws-1"),
            {"workspace_id": "ws-2"},
        ]
    )
    with (
        patch(
            "ptc_cli.commands.slash.create_interactive_menu",
            AsyncMock(return_value=(0, {"action": "create"})),
        ),
        patch("ptc_cli.commands.slash.console") as console,
    ):
        console.input.side_effect = ["research", "research 2"]
        workspace_id = await _select_or_create_workspace_interactive(client)
    assert workspace_id == "ws-2"
    names = [c.kwargs["name"] for c in client.create_workspace.await_args_list]
    assert names == ["research", "research 2"]
    printed = " ".join(str(c.args[0]) for c in console.print.call_args_list)
    assert 'A workspace named "Research" already exists. Choose another name.' in printed
