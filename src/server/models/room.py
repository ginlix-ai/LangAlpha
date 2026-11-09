"""Request and response models for Room (group chat) endpoints.

A room is a shared ordered log plus bot members; a user message opens a round
whose slots walk the members serially. Members must belong to the room's
workspace; the room is not a sandbox boundary.
"""

import uuid
from datetime import datetime
from typing import Any, Dict, List, Literal, Optional

from pydantic import BaseModel, ConfigDict, Field, field_validator

from src.server.database.rooms import MAX_ROOM_BOTS

Responder = str  # "everyone" | "mentions" | "member:<botId>"


def _named(value: str) -> str:
    cleaned = (value or "").strip()
    if not cleaned:
        raise ValueError("must not be blank")
    return cleaned


class RoomCreate(BaseModel):
    workspace_id: uuid.UUID
    name: str = Field(..., min_length=1, max_length=64)
    member_bot_ids: List[uuid.UUID] = Field(default_factory=list)
    host_bot_id: Optional[uuid.UUID] = None
    responder: Responder = "everyone"
    bulletin: str = Field(default="", max_length=2_000)

    _name = field_validator("name")(_named)


class RoomUpdate(BaseModel):
    name: Optional[str] = Field(default=None, min_length=1, max_length=64)
    responder: Optional[Responder] = None
    bulletin: Optional[str] = Field(default=None, max_length=2_000)
    status: Optional[Literal["active", "archived"]] = None

    _name = field_validator("name")(_named)


class RoomMemberAdd(BaseModel):
    bot_id: uuid.UUID


class RoomMessageCreate(BaseModel):
    content: str = Field(..., min_length=1, max_length=64 * 1024)
    #: A client retry sends the same key, so one message is appended once.
    request_key: Optional[str] = Field(default=None, max_length=128)

    @field_validator("content")
    @classmethod
    def _content(cls, value: str) -> str:
        cleaned = (value or "").strip()
        if not cleaned:
            raise ValueError("must not be blank")
        return cleaned


class RoomMemberInfo(BaseModel):
    model_config = ConfigDict(extra="ignore")

    bot_id: str
    name: str
    role: str
    status: str
    room_role: str
    avatar: Dict[str, Any] = Field(default_factory=dict)


class RoomInfo(BaseModel):
    model_config = ConfigDict(extra="ignore")

    room_id: str
    workspace_id: str
    name: str
    host_bot_id: Optional[str] = None
    responder: str = "everyone"
    bulletin: str = ""
    status: str = "active"
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None


class RoomView(RoomInfo):
    members: List[RoomMemberInfo] = Field(default_factory=list)


class RoomListResponse(BaseModel):
    rooms: List[RoomInfo]
    max_bots: int = MAX_ROOM_BOTS


class RoomLogEntry(BaseModel):
    model_config = ConfigDict(extra="ignore")

    seq: int
    room_id: str
    round_id: Optional[str] = None
    sender_type: str
    sender_id: str
    kind: str
    content: str
    created_at: Optional[datetime] = None


class RoomLogResponse(BaseModel):
    entries: List[RoomLogEntry]
    latest_seq: int


class RoomSendResponse(BaseModel):
    message_seq: int
    round_id: Optional[str] = None
    round_status: Optional[str] = None


class RoomMemberDeleteResponse(BaseModel):
    ok: bool = True
    bot_id: str
