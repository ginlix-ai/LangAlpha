"""Rooms: shared log and group rounds.

A room is a workspace-scoped shared log whose members are bots. A user message
opens a round; the round driver (``services/rooms/coordinator.py``) walks the
members serially and each member answers in its own thread. This router owns
the HTTP shape only: validation, ownership, the send, and the SSE tail.

Endpoints:
- POST   /api/v1/rooms
- GET    /api/v1/rooms?workspace_id=
- GET    /api/v1/rooms/{room_id}
- PATCH  /api/v1/rooms/{room_id}
- POST   /api/v1/rooms/{room_id}/members
- DELETE /api/v1/rooms/{room_id}/members/{bot_id}
- POST   /api/v1/rooms/{room_id}/messages
- GET    /api/v1/rooms/{room_id}/log?since_seq=
- GET    /api/v1/rooms/{room_id}/stream?last_seq=
"""

import asyncio
import json
import logging
from typing import Optional

from fastapi import APIRouter, HTTPException, Query
from psycopg.errors import UniqueViolation
from fastapi.responses import StreamingResponse

from src.server.app.status_stream import SSE_HEADERS
from src.server.database import bots as bot_db
from src.server.database import rooms as room_db
from src.server.database.workspace import get_workspace
from src.server.dependencies.principal import (
    ReadPrincipal,
    WorkspaceWritePrincipal,
    assert_reaches,
)
from src.server.models.room import (
    RoomCreate,
    RoomInfo,
    RoomListResponse,
    RoomLogEntry,
    RoomLogResponse,
    RoomMemberAdd,
    RoomMemberDeleteResponse,
    RoomMemberInfo,
    RoomMessageCreate,
    RoomSendResponse,
    RoomUpdate,
    RoomView,
)
from src.server.services.rooms.coordinator import start_round
from src.server.utils.api import require_workspace_owner

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1/rooms", tags=["Rooms"])

_STREAM_POLL_SECONDS = 1.2
_STREAM_KEEPALIVE_SECONDS = 20.0


def _info(room: dict) -> RoomInfo:
    return RoomInfo(**room)


async def _owned_room(user_id: str, room_id: str) -> dict:
    room = await room_db.get_room(room_id)
    if room is None or room.get("user_id") != user_id:
        raise HTTPException(status_code=404, detail="Room not found")
    return room


async def _reach_room(principal, room_id: str) -> dict:
    """An owned room the caller's computer also holds; body ids are checked here."""
    room = await _owned_room(principal.user_id, room_id)
    await assert_reaches(principal, workspace_id=room["workspace_id"])
    return room


async def _own_workspace(user_id: str, workspace_id: str) -> dict:
    workspace = await get_workspace(str(workspace_id))
    require_workspace_owner(workspace, user_id=user_id)
    return workspace


async def _validated_members(
    user_id: str, workspace_id: str, bot_ids: list[str]
) -> list[dict]:
    """The named bots, each live and of the room's workspace."""
    bots = []
    for bot_id in bot_ids:
        bot = await bot_db.get_bot_for_user(user_id, bot_id)
        if bot is None:
            raise HTTPException(status_code=400, detail=f"Unknown bot {bot_id}")
        if str(bot["workspace_id"]) != str(workspace_id):
            raise HTTPException(
                status_code=400,
                detail=f"Bot {bot['name']} is not of this room's workspace",
            )
        bots.append(bot)
    return bots


async def _view(room: dict) -> RoomView:
    members = await room_db.list_members_with_bots(room["room_id"])
    return RoomView(
        **_info(room).model_dump(),
        members=[
            RoomMemberInfo(
                bot_id=m["bot_id"],
                name=m.get("name") or m["bot_id"],
                role=m.get("role") or "analyst",
                status=m.get("status") or "active",
                room_role=m.get("room_role") or "member",
                avatar=m.get("avatar") or {},
            )
            for m in members
        ],
    )


@router.post("", response_model=RoomView, status_code=201)
async def create_room(
    principal: WorkspaceWritePrincipal, body: RoomCreate
) -> RoomView:
    user_id = principal.user_id
    workspace = await _own_workspace(user_id, str(body.workspace_id))
    await assert_reaches(principal, workspace_id=str(workspace["workspace_id"]))
    member_ids = [str(b) for b in body.member_bot_ids]
    if body.host_bot_id:
        member_ids = [str(body.host_bot_id), *member_ids]
    await _validated_members(user_id, str(body.workspace_id), member_ids)
    try:
        room = await room_db.create_room(
            user_id,
            str(body.workspace_id),
            body.name.strip(),
            host_bot_id=str(body.host_bot_id) if body.host_bot_id else None,
            responder=body.responder,
            bulletin=body.bulletin,
            member_bot_ids=member_ids,
        )
    except room_db.RoomMemberLimit as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from None
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from None
    return await _view(room)


@router.get("", response_model=RoomListResponse)
async def list_rooms(
    principal: ReadPrincipal,
    workspace_id: Optional[str] = Query(default=None),
    limit: int = Query(default=100, ge=1, le=200),
    offset: int = Query(default=0, ge=0),
) -> RoomListResponse:
    user_id = principal.user_id
    if workspace_id:
        workspace = await _own_workspace(user_id, workspace_id)
        await assert_reaches(
            principal, workspace_id=str(workspace["workspace_id"])
        )
    rooms = await room_db.list_rooms(
        user_id,
        workspace_id,
        computer_id=principal.computer_id,
        limit=limit,
        offset=offset,
    )
    return RoomListResponse(rooms=[_info(r) for r in rooms])


@router.get("/{room_id}", response_model=RoomView)
async def get_room(principal: ReadPrincipal, room_id: str) -> RoomView:
    return await _view(await _reach_room(principal, room_id))


@router.patch("/{room_id}", response_model=RoomInfo)
async def update_room(
    principal: WorkspaceWritePrincipal, room_id: str, body: RoomUpdate
) -> RoomInfo:
    room = await _reach_room(principal, room_id)
    changed = body.model_dump(exclude_none=True)
    try:
        updated = await room_db.update_room(room["room_id"], changed)
    except UniqueViolation:
        raise HTTPException(
            status_code=409, detail="A room with that name already exists."
        ) from None
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from None
    if updated is None:
        raise HTTPException(status_code=404, detail="Room not found")
    return _info(updated)


@router.post("/{room_id}/members", response_model=RoomView, status_code=201)
async def add_member(
    principal: WorkspaceWritePrincipal, room_id: str, body: RoomMemberAdd
) -> RoomView:
    room = await _reach_room(principal, room_id)
    await _validated_members(
        principal.user_id, room["workspace_id"], [str(body.bot_id)]
    )
    try:
        await room_db.add_member(room["room_id"], str(body.bot_id))
    except room_db.RoomMemberLimit as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from None
    return await _view(room)


@router.delete("/{room_id}/members/{bot_id}", response_model=RoomMemberDeleteResponse)
async def remove_member(
    principal: WorkspaceWritePrincipal, room_id: str, bot_id: str
) -> RoomMemberDeleteResponse:
    room = await _reach_room(principal, room_id)
    await room_db.remove_member(room["room_id"], bot_id)
    return RoomMemberDeleteResponse(bot_id=bot_id)


@router.post("/{room_id}/messages", response_model=RoomSendResponse, status_code=202)
async def send_message(
    principal: WorkspaceWritePrincipal, room_id: str, body: RoomMessageCreate
) -> RoomSendResponse:
    """Append the user's message and let the round walk the members."""
    user_id = principal.user_id
    room = await _reach_room(principal, room_id)
    if room.get("status") != "active":
        raise HTTPException(status_code=409, detail="Room is archived")
    result = await start_round(
        room,
        body.content.strip(),
        user_id=user_id,
        request_key=body.request_key,
    )
    round_row = result.get("round") or {}
    return RoomSendResponse(
        message_seq=int(result["message"]["seq"]),
        round_id=round_row.get("round_id"),
        round_status=round_row.get("status"),
    )


@router.get("/{room_id}/log", response_model=RoomLogResponse)
async def read_log(
    principal: ReadPrincipal,
    room_id: str,
    since_seq: int = Query(default=0, ge=0),
    limit: int = Query(default=100, ge=1, le=200),
) -> RoomLogResponse:
    room = await _reach_room(principal, room_id)
    entries = await room_db.read_log(room["room_id"], since_seq=since_seq, limit=limit)
    return RoomLogResponse(
        entries=[RoomLogEntry(**entry) for entry in entries],
        latest_seq=await room_db.latest_seq(room["room_id"]),
    )


def _sse(event: str, data: dict) -> str:
    return (
        f"event: {event}\n"
        f"data: {json.dumps(data, ensure_ascii=False, default=str)}\n\n"
    )


@router.get("/{room_id}/stream")
async def stream_room(
    principal: ReadPrincipal,
    room_id: str,
    last_seq: int = Query(default=0, ge=0),
) -> StreamingResponse:
    """Tail the shared log: messages as they land, plus the round's status.

    Reads the table rather than Redis: the room log is the record, and a
    client that misses a beat only needs to re-read from its last seq.
    """
    room = await _reach_room(principal, room_id)

    async def events():
        seq = int(last_seq)
        sent_round: Optional[str] = None
        sent_status: Optional[str] = None
        sent_cursor: Optional[int] = None
        sent_room_at: Optional[str] = None
        idle = 0.0
        ticks = 0
        while True:
            entries = await room_db.read_log(room["room_id"], since_seq=seq, limit=100)
            for entry in entries:
                seq = int(entry["seq"])
                yield _sse("room_message", entry)
                idle = 0.0
            # Membership/name/responder/bulletin edits are announced so an
            # open panel refetches instead of drawing a stale room.
            # The room row moves on membership/bulletin edits, not on every
            # message; polling it every few beats keeps the tail light.
            if ticks % 5 == 0:
                fresh = await room_db.get_room(room["room_id"])
            else:
                fresh = None
            ticks += 1
            if fresh is not None and fresh.get("updated_at") != sent_room_at:
                sent_room_at = fresh.get("updated_at")
                yield _sse(
                    "room_updated",
                    {"room_id": fresh["room_id"], "updated_at": sent_room_at},
                )
                idle = 0.0
            row = await room_db.active_round(room["room_id"])
            if row is None:
                row = await room_db.latest_round(room["room_id"])
            if row is not None:
                cursor = row.get("slot_cursor")
                if (
                    row["round_id"] != sent_round
                    or row["status"] != sent_status
                    or cursor != sent_cursor
                ):
                    sent_round, sent_status, sent_cursor = (
                        row["round_id"],
                        row["status"],
                        cursor,
                    )
                    yield _sse(
                        "round_status",
                        {
                            "round_id": row["round_id"],
                            "status": row["status"],
                            "slot_cursor": row.get("slot_cursor"),
                            "slots": row.get("slots"),
                        },
                    )
                    idle = 0.0
            if not entries:
                await asyncio.sleep(_STREAM_POLL_SECONDS)
                idle += _STREAM_POLL_SECONDS
                if idle >= _STREAM_KEEPALIVE_SECONDS:
                    idle = 0.0
                    yield ": keepalive\n\n"

    return StreamingResponse(events(), media_type="text/event-stream", headers=SSE_HEADERS)
