"""Request and response models for the Bot management API.

A bot is the identity over a thread: it names the speaker, fixes the model and
tool surface for its turns, and belongs to an existing workspace (which stays
the unit of sandbox, files and workspace memory).
"""

import uuid
from datetime import datetime
from typing import Any, Dict, List, Literal, Optional

from pydantic import BaseModel, ConfigDict, Field, field_validator

#: Cap mirrors ``database.bots.MAX_BOTS_PER_WORKSPACE``.
MAX_BOTS_PER_WORKSPACE = 20


def _named(value: str) -> str:
    cleaned = (value or "").strip()
    if not cleaned:
        raise ValueError("must not be blank")
    return cleaned


class BotCreate(BaseModel):
    """Create a bot inside one of the caller's workspaces."""

    workspace_id: uuid.UUID
    name: str = Field(..., min_length=1, max_length=64)
    _name = field_validator("name")(_named)
    #: Only a workspace analyst can be created by hand. The Chief of Staff is
    #: derived per user (Home) and is created lazily by the resolver.
    role: Literal["analyst"] = "analyst"
    persona: str = Field(default="", max_length=20_000)
    avatar: Dict[str, Any] = Field(default_factory=dict)
    provider: Optional[str] = Field(default=None, max_length=64)
    model: Optional[str] = Field(
        default=None,
        max_length=128,
        description=(
            "The model this bot's turns run. None follows the thread's model, "
            "which is how a bot backfilled from a legacy workspace behaves."
        ),
    )
    skills: List[str] = Field(default_factory=list)
    tools_allow: List[str] = Field(default_factory=list)
    tools_deny: List[str] = Field(default_factory=list)
    approval: Literal["ask", "edits", "auto", "full"] = "ask"
    memory_scopes: List[str] = Field(default_factory=lambda: ["workspace"])


class BotUpdate(BaseModel):
    """Edit a bot. Absent fields are left alone; ``role`` is immutable."""

    name: Optional[str] = Field(default=None, min_length=1, max_length=64)

    _name = field_validator("name")(
        lambda value: None if value is None else _named(value)
    )
    persona: Optional[str] = Field(default=None, max_length=20_000)
    avatar: Optional[Dict[str, Any]] = None
    provider: Optional[str] = Field(default=None, max_length=64)
    model: Optional[str] = Field(default=None, max_length=128)
    skills: Optional[List[str]] = None
    tools_allow: Optional[List[str]] = None
    tools_deny: Optional[List[str]] = None
    approval: Optional[Literal["ask", "edits", "auto", "full"]] = None
    memory_scopes: Optional[List[str]] = None
    status: Optional[Literal["active", "paused"]] = None


class BotInfo(BaseModel):
    """One bot as the client reads it."""

    model_config = ConfigDict(extra="ignore")

    bot_id: str
    user_id: str
    workspace_id: str
    name: str
    role: Literal["analyst", "chief_of_staff"]
    persona: str = ""
    avatar: Dict[str, Any] = Field(default_factory=dict)
    provider: Optional[str] = None
    model: Optional[str] = None
    skills: List[str] = Field(default_factory=list)
    tools_allow: List[str] = Field(default_factory=list)
    tools_deny: List[str] = Field(default_factory=list)
    approval: Literal["ask", "edits", "auto", "full"] = "ask"
    memory_scopes: List[str] = Field(default_factory=lambda: ["workspace"])
    status: Literal["active", "paused"] = "active"
    primary_thread_id: Optional[str] = None
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None


class BotListResponse(BaseModel):
    bots: List[BotInfo]
    max_bots: int = MAX_BOTS_PER_WORKSPACE


class BotDeleteResponse(BaseModel):
    ok: bool = True
    bot_id: str
