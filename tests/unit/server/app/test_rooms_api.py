"""The Rooms API's guards: workspace ownership, member scope, archived rooms."""

from __future__ import annotations

from unittest.mock import AsyncMock, patch

import pytest
from fastapi import HTTPException

from src.server.app.rooms import create_room, list_rooms, read_log, send_message
from src.server.models.room import RoomCreate, RoomMessageCreate
from src.server.dependencies.principal import Principal

USER = "u-1"
PRINCIPAL = Principal(user_id=USER, auth="pat")
WS = "22222222-2222-2222-2222-222222222222"


def _bot(workspace_id: str = WS) -> dict:
    return {"bot_id": "b-1", "name": "Quant", "workspace_id": workspace_id, "status": "active"}


@pytest.mark.asyncio
async def test_a_member_of_another_workspace_is_refused():
    body = RoomCreate(
        workspace_id=WS,
        name="R",
        member_bot_ids=["33333333-3333-3333-3333-333333333333"],
    )
    with patch(
        "src.server.app.rooms._own_workspace", AsyncMock(return_value={"workspace_id": WS})
    ), patch(
        "src.server.app.rooms.bot_db.get_bot_for_user",
        AsyncMock(return_value=_bot("other-ws")),
    ):
        with pytest.raises(HTTPException) as caught:
            await create_room(PRINCIPAL, body)
    assert caught.value.status_code == 400


@pytest.mark.asyncio
async def test_an_archived_room_takes_no_message():
    room = {"room_id": "r-1", "user_id": USER, "workspace_id": WS, "status": "archived"}
    with patch("src.server.app.rooms._owned_room", AsyncMock(return_value=room)), patch(
        "src.server.app.rooms.start_round", AsyncMock()
    ) as start:
        with pytest.raises(HTTPException) as caught:
            await send_message(PRINCIPAL, "r-1", RoomMessageCreate(content="hi"))
    assert caught.value.status_code == 409
    start.assert_not_awaited()


@pytest.mark.asyncio
async def test_the_log_reads_from_a_seq_and_reports_the_tail():
    room = {"room_id": "r-1", "user_id": USER, "workspace_id": WS, "status": "active"}
    entries = [
        {
            "seq": 3,
            "room_id": "r-1",
            "round_id": None,
            "sender_type": "user",
            "sender_id": USER,
            "kind": "message",
            "content": "hello",
            "created_at": None,
        }
    ]
    with patch("src.server.app.rooms._owned_room", AsyncMock(return_value=room)), patch(
        "src.server.app.rooms.room_db.read_log", AsyncMock(return_value=entries)
    ), patch("src.server.app.rooms.room_db.latest_seq", AsyncMock(return_value=3)):
        answer = await read_log(PRINCIPAL, "r-1", since_seq=2, limit=100)
    assert answer.latest_seq == 3
    assert answer.entries[0].content == "hello"


@pytest.mark.asyncio
async def test_a_member_row_carries_the_bots_avatar():
    from src.server.app.rooms import _view

    room = {
        "room_id": "r-1",
        "workspace_id": WS,
        "name": "R",
        "responder": "everyone",
        "bulletin": "",
        "status": "active",
    }
    members = [
        {
            "bot_id": "b-1",
            "name": "Quant",
            "role": "analyst",
            "status": "active",
            "room_role": "host",
            "avatar": {"kind": "mark", "color": "teal"},
        }
    ]
    with patch(
        "src.server.app.rooms.room_db.list_members_with_bots",
        AsyncMock(return_value=members),
    ):
        view = await _view(room)
    assert view.members[0].avatar == {"kind": "mark", "color": "teal"}


@pytest.mark.asyncio
async def test_a_room_off_the_computers_hold_is_refused():
    computer = Principal(user_id=USER, auth="pat", computer_id="c-1")
    room = {"room_id": "r-1", "user_id": USER, "workspace_id": WS, "status": "active"}
    refused = HTTPException(status_code=403, detail="Forbidden")
    with patch(
        "src.server.app.rooms._owned_room", AsyncMock(return_value=room)
    ), patch(
        "src.server.app.rooms.assert_reaches", AsyncMock(side_effect=refused)
    ):
        with pytest.raises(HTTPException) as caught:
            await read_log(computer, "r-1")
    assert caught.value.status_code == 403


@pytest.mark.asyncio
async def test_a_computer_token_reads_only_its_own_computers_rooms():
    computer = Principal(user_id=USER, auth="pat", computer_id="c-1")
    with patch(
        "src.server.app.rooms.room_db.list_rooms", AsyncMock(return_value=[])
    ) as list_db:
        await list_rooms(computer, None)
    assert list_db.await_args.kwargs["computer_id"] == "c-1"


def test_a_blank_room_message_is_refused_at_the_model():
    from src.server.models.room import RoomMessageCreate

    with pytest.raises(ValueError):
        RoomMessageCreate(content="   ")


def test_a_room_name_is_stripped_and_cannot_be_blank():
    from src.server.models.room import RoomCreate

    assert RoomCreate(workspace_id=WS, name="  Research  ").name == "Research"
    with pytest.raises(ValueError):
        RoomCreate(workspace_id=WS, name="   ")


@pytest.mark.asyncio
async def test_the_room_list_is_paginated():
    with patch(
        "src.server.app.rooms.room_db.list_rooms",
        AsyncMock(return_value=[]),
    ) as list_db:
        await list_rooms(PRINCIPAL, None, limit=50, offset=10)
    assert list_db.await_args.kwargs["limit"] == 50
    assert list_db.await_args.kwargs["offset"] == 10


def test_the_sse_writer_survives_uuid_fields():
    import uuid as _uuid

    from src.server.app.rooms import _sse

    frame = _sse(
        "room_message",
        {"seq": 1, "room_id": _uuid.uuid4(), "round_id": _uuid.uuid4()},
    )
    assert frame.startswith("event: room_message")
    assert '"seq": 1' in frame
