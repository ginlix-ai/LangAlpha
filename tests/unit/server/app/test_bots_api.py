"""The Bots API: the derived Chief of Staff is in every roster, and guards hold.

The list is the one place the Home bot is made without a Home turn, because
the Bots screen has to show it; a workspace-filtered read makes nothing. The
hand-made-only rules (no second bot in Home, no delete of the derived one)
are refused rather than silently allowed.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, patch

import pytest
from fastapi import HTTPException

from src.server.app.bots import delete_bot, get_bot, list_bots, update_bot
from src.server.models.bot import BotUpdate
from src.server.dependencies.principal import Principal
from src.server.models.bot import BotCreate

USER = "u-1"
PRINCIPAL = Principal(user_id=USER, auth="pat")
WS = "22222222-2222-2222-2222-222222222222"
HOME = "33333333-3333-3333-3333-333333333333"


def _bot(bot_id: str, role: str = "analyst", workspace_id: str = WS) -> dict:
    return {
        "bot_id": bot_id,
        "user_id": USER,
        "workspace_id": workspace_id,
        "name": "Quant" if role == "analyst" else "Chief of Staff",
        "role": role,
        "persona": "",
        "avatar": {},
        "provider": None,
        "model": None,
        "skills": [],
        "tools_allow": [],
        "tools_deny": [],
        "approval": "ask",
        "memory_scopes": ["workspace"],
        "status": "active",
        "primary_thread_id": None,
        "created_at": None,
        "updated_at": None,
    }


@pytest.mark.asyncio
async def test_an_unfiltered_list_makes_the_derived_chief_of_staff():
    home = _bot("b-home", role="chief_of_staff", workspace_id=HOME)
    with patch(
        "src.server.app.bots.bot_db.ensure_home_bot", AsyncMock(return_value=home)
    ) as ensure, patch(
        "src.server.app.bots.bot_db.list_bots", AsyncMock(return_value=[home])
    ):
        answer = await list_bots(PRINCIPAL, None)
    ensure.assert_awaited_once_with(USER)
    assert [b.role for b in answer.bots] == ["chief_of_staff"]


@pytest.mark.asyncio
async def test_a_filtered_list_makes_nothing():
    workspace = {"workspace_id": WS, "user_id": USER}
    with patch(
        "src.server.app.bots._own_workspace", AsyncMock(return_value=workspace)
    ), patch(
        "src.server.app.bots.bot_db.ensure_home_bot", AsyncMock()
    ) as ensure, patch(
        "src.server.app.bots.bot_db.list_bots", AsyncMock(return_value=[])
    ):
        answer = await list_bots(PRINCIPAL, workspace_id=WS)
    ensure.assert_not_awaited()
    assert answer.bots == []


@pytest.mark.asyncio
async def test_the_derived_chief_of_staff_cannot_be_deleted():
    home = _bot("b-home", role="chief_of_staff", workspace_id=HOME)
    with patch("src.server.app.bots._owned_bot", AsyncMock(return_value=home)):
        with pytest.raises(HTTPException) as caught:
            await delete_bot(PRINCIPAL, "b-home")
    assert caught.value.status_code == 409


@pytest.mark.asyncio
async def test_no_hand_made_bot_in_home():
    home_workspace = {"workspace_id": HOME, "user_id": USER, "name": "Home"}
    body = BotCreate(workspace_id=HOME, name="Second")
    with patch(
        "src.server.app.bots._own_workspace", AsyncMock(return_value=home_workspace)
    ), patch("src.server.app.bots.is_home_id", return_value=True):
        with pytest.raises(HTTPException) as caught:
            await _create(PRINCIPAL, body)
    assert caught.value.status_code == 400


async def _create(principal, body: BotCreate):
    from src.server.app.bots import create_bot

    return await create_bot(principal, body)


@pytest.mark.asyncio
async def test_a_computer_token_reads_only_its_own_computers_bots():
    """Unfiltered list: the DB call is scoped to the computer and Home is not minted."""
    computer = Principal(user_id=USER, auth="pat", computer_id="c-1")
    with patch(
        "src.server.app.bots.bot_db.ensure_home_bot", AsyncMock()
    ) as ensure, patch(
        "src.server.app.bots.bot_db.list_bots", AsyncMock(return_value=[])
    ) as list_db:
        await list_bots(computer, None)
    ensure.assert_not_awaited()
    assert list_db.await_args.kwargs["computer_id"] == "c-1"


@pytest.mark.asyncio
async def test_a_workspace_off_its_computer_is_refused():
    computer = Principal(user_id=USER, auth="pat", computer_id="c-1")
    workspace = {"workspace_id": WS, "user_id": USER}
    refused = HTTPException(status_code=403, detail="Forbidden")
    with patch(
        "src.server.app.bots._own_workspace", AsyncMock(return_value=workspace)
    ), patch(
        "src.server.app.bots.assert_reaches", AsyncMock(side_effect=refused)
    ):
        with pytest.raises(HTTPException) as caught:
            await list_bots(computer, workspace_id=WS)
    assert caught.value.status_code == 403


@pytest.mark.asyncio
async def test_a_bot_off_its_computer_is_refused_on_read():
    computer = Principal(user_id=USER, auth="pat", computer_id="c-1")
    bot = _bot("b-1")
    refused = HTTPException(status_code=403, detail="Forbidden")
    with patch(
        "src.server.app.bots._owned_bot", AsyncMock(return_value=bot)
    ), patch("src.server.app.bots.assert_reaches", AsyncMock(side_effect=refused)):
        with pytest.raises(HTTPException) as caught:
            await get_bot(computer, "b-1")
    assert caught.value.status_code == 403


@pytest.mark.asyncio
async def test_a_bot_model_the_account_cannot_run_is_refused_at_the_pick():
    body = BotCreate(workspace_id=WS, name="Quant", model="no-such-model")
    workspace = {"workspace_id": WS, "user_id": USER}
    refused = HTTPException(status_code=400, detail="Model not available")
    with patch(
        "src.server.app.bots._own_workspace", AsyncMock(return_value=workspace)
    ), patch(
        "src.server.services.llm.thread_model.require_selectable_model",
        AsyncMock(side_effect=refused),
    ):
        with pytest.raises(HTTPException) as caught:
            await _create(PRINCIPAL, body)
    assert caught.value.status_code == 400


@pytest.mark.asyncio
async def test_a_rename_onto_a_taken_name_is_refused_with_a_409():
    current = _bot("b-1")
    with patch(
        "src.server.app.bots._reachable_bot", AsyncMock(return_value=current)
    ), patch(
        "src.server.app.bots.bot_db.update_bot",
        AsyncMock(side_effect=__import__("src.server.database.bots", fromlist=["bots"]).BotNameTaken("Quant")),
    ):
        with pytest.raises(HTTPException) as caught:
            await update_bot(PRINCIPAL, "b-1", BotUpdate(name="Quant"))
    assert caught.value.status_code == 409


@pytest.mark.asyncio
async def test_the_derived_chief_of_staff_cannot_be_paused():
    chief = _bot("b-home", role="chief_of_staff", workspace_id=HOME)
    with patch(
        "src.server.app.bots._reachable_bot", AsyncMock(return_value=chief)
    ):
        with pytest.raises(HTTPException) as caught:
            await update_bot(PRINCIPAL, "b-home", BotUpdate(status="paused"))
    assert caught.value.status_code == 409


def test_a_blank_bot_name_is_not_a_name():
    with pytest.raises(ValueError):
        BotCreate(workspace_id=WS, name="   ")
