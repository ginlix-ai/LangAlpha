"""A room member's own tools: the log it can read, the note it can leave."""

from __future__ import annotations

from unittest.mock import AsyncMock, patch

import pytest

from src.tools.rooms.tool import post_to_room, read_own_chat, read_room

USER = "u-1"
WS = "22222222-2222-2222-2222-222222222222"
THREAD = "11111111-1111-1111-1111-111111111111"
ROOM = {"room_id": "r-1", "name": "Research"}
MEMBERS = [
    {"bot_id": "b-1", "name": "Quant", "room_role": "member"},
    {"bot_id": "b-2", "name": "Scout", "room_role": "host"},
]
ENTRIES = [
    {"seq": 1, "sender_type": "user", "sender_id": USER, "kind": "message", "content": "hello"},
    {"seq": 2, "sender_type": "bot", "sender_id": "b-2", "kind": "message", "content": "on it"},
]
CONFIG = {"configurable": {"user_id": USER, "workspace_id": WS, "thread_id": THREAD}}


def _call(args: dict, name: str = "post_to_room") -> dict:
    return {"name": name, "args": args, "id": "call_1", "type": "tool_call"}


async def _say(tool, args: dict) -> str:
    result = await tool.ainvoke(_call(args), config=CONFIG)
    return result.content if hasattr(result, "content") else str(result)


@pytest.mark.asyncio
async def test_reading_the_room_shows_senders_and_seq():
    with patch(
        "src.tools.rooms.tool.room_db.room_for_thread", AsyncMock(return_value="r-1")
    ), patch(
        "src.server.database.bots.get_thread_bot_id", AsyncMock(return_value="b-1")
    ), patch(
        "src.tools.rooms.tool.room_db.get_room", AsyncMock(return_value=ROOM)
    ), patch(
        "src.tools.rooms.tool.room_db.list_members_with_bots",
        AsyncMock(return_value=MEMBERS),
    ), patch(
        "src.tools.rooms.tool.room_db.read_recent_log", AsyncMock(return_value=ENTRIES)
    ):
        out = await read_room.ainvoke({}, config=CONFIG)
    assert '[Room "Research"]' in out
    assert "[1] the user: hello" in out
    assert "[2] Scout: on it" in out


@pytest.mark.asyncio
async def test_posting_returns_the_seq_it_landed_at():
    with patch(
        "src.tools.rooms.tool.room_db.room_for_thread", AsyncMock(return_value="r-1")
    ), patch(
        "src.server.database.bots.get_thread_bot_id", AsyncMock(return_value="b-1")
    ), patch(
        "src.tools.rooms.tool.room_db.append_log",
        AsyncMock(return_value={"seq": 9}),
    ) as append:
        out = await _say(post_to_room, {"content": "a note"})
    assert out == "posted at seq 9"
    append.assert_awaited_once()
    assert append.await_args.kwargs["sender_id"] == "b-1"


@pytest.mark.asyncio
async def test_a_thread_outside_a_room_is_refused():
    with patch(
        "src.tools.rooms.tool.room_db.room_for_thread", AsyncMock(return_value=None)
    ):
        out = await _say(post_to_room, {"content": "a note"})
    assert out.startswith("refused")
    assert "not a room member" in out


@pytest.mark.asyncio
async def test_reading_own_chat_records_the_look_in_the_room():
    with patch(
        "src.server.database.bots.get_thread_bot_id", AsyncMock(return_value="b-1")
    ), patch(
        "src.server.database.bots.get_private_thread",
        AsyncMock(return_value="th-private"),
    ), patch(
        "src.server.database.bots.get_bot", AsyncMock(return_value={"name": "Quant"})
    ), patch(
        "src.tools.secretary.utils.extract_text_from_thread",
        AsyncMock(return_value={"text": "private findings"}),
    ), patch(
        "src.tools.rooms.tool.room_db.room_for_thread", AsyncMock(return_value="r-1")
    ), patch(
        "src.tools.rooms.tool.room_db.append_log", AsyncMock(return_value={"seq": 11})
    ) as log:
        out = await read_own_chat.ainvoke({}, config=CONFIG)
    assert "private findings" in out
    log.assert_awaited_once()
    assert "consulted its own chat" in log.await_args.kwargs["content"]


# --- creating a room ------------------------------------------------------

BOTS = [
    {"bot_id": "b-1", "name": "Quant", "workspace_id": WS, "status": "active"},
    {"bot_id": "b-2", "name": "Scout", "workspace_id": WS, "status": "active"},
    {"bot_id": "b-p", "name": "Sleepy", "workspace_id": WS, "status": "paused"},
]
CREATED = {"room_id": "r-new", "name": "Trading desk"}


@pytest.mark.asyncio
async def test_the_agent_creates_a_room_with_bots():
    from src.tools.rooms.tool import create_room

    with patch(
        "src.server.database.bots.list_bots", AsyncMock(return_value=BOTS)
    ), patch(
        "src.server.database.rooms.create_room", AsyncMock(return_value=CREATED)
    ) as create:
        out = await _say(
            create_room,
            {"name": "Trading desk", "members": "Scout, b-1", "bulletin": "be brief"},
        )
    assert "created room Trading desk" in out and "r-new" in out
    kwargs = create.await_args.kwargs
    assert kwargs["member_bot_ids"] == ["b-2", "b-1"]
    assert kwargs["host_bot_id"] == "b-2"
    assert kwargs["bulletin"] == "be brief"


@pytest.mark.asyncio
async def test_a_room_member_must_resolve_and_be_active():
    from src.tools.rooms.tool import create_room

    with patch("src.server.database.bots.list_bots", AsyncMock(return_value=BOTS)):
        unknown = await _say(create_room, {"name": "R", "members": "Nobody"})
        paused = await _say(create_room, {"name": "R", "members": "Sleepy"})
        empty = await _say(create_room, {"name": "R", "members": " "})
    assert "no bot named or identified" in unknown
    assert "paused" in paused
    assert "at least one bot" in empty


@pytest.mark.asyncio
async def test_a_single_responder_is_a_member():
    from src.tools.rooms.tool import create_room

    with patch(
        "src.server.database.bots.list_bots", AsyncMock(return_value=BOTS)
    ), patch(
        "src.server.database.rooms.create_room", AsyncMock(return_value=CREATED)
    ) as create:
        await _say(
            create_room,
            {
                "name": "R",
                "members": "Quant, Scout",
                "responder": "member:Quant",
            },
        )
    assert create.await_args.kwargs["responder"] == "member:b-1"


@pytest.mark.asyncio
async def test_listing_rooms_names_members_and_ids():
    from src.tools.rooms.tool import list_rooms

    with patch(
        "src.server.database.rooms.list_rooms",
        AsyncMock(
            return_value=[
                {
                    "room_id": "r-1",
                    "name": "Research",
                    "status": "active",
                    "responder": "everyone",
                }
            ]
        ),
    ), patch(
        "src.server.database.rooms.list_members_with_bots",
        AsyncMock(return_value=MEMBERS),
    ):
        out = await _say(list_rooms, {})
    assert "Research" in out and "r-1" in out
    assert "Quant" in out and "Scout" in out
