"""The owner's artifact routes' request and response shapes."""

from __future__ import annotations

import math
import re
from datetime import datetime
from typing import Annotated, Any, Literal

from pydantic import (
    AfterValidator,
    BaseModel,
    ConfigDict,
    Field,
    field_serializer,
    BeforeValidator,
    field_validator,
    model_validator,
)

from src.server.database.artifacts import one_line
from src.server.services.artifacts import snapshot
from src.server.utils.pg_sanitize import uuid_text

_SHA256 = re.compile(r"[0-9a-f]{64}")

ThreadId = Annotated[str, AfterValidator(uuid_text)]


# --- records -----------------------------------------------------------------


class ArtifactVersionResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    seq: int
    entry_path: str
    origin: Literal["owner", "keep_updated", "legacy"]
    title: str | None
    label: str | None
    byte_len: int
    published_at: datetime


class ArtifactResponse(BaseModel):
    artifact_id: str
    code: str
    url: str
    workspace_id: str
    path: str
    name: str
    shared: bool
    shared_at: datetime | None
    keep_updated: bool
    latest_seq: int
    bound_thread_id: str | None
    open_comments: int
    unread: int
    created_at: datetime
    #: A link shared before versions whose files are past the caps: visitors
    #: get a 404 until the owner publishes.
    legacy_refused: Literal["too_many_files", "too_many_bytes"] | None = None


class ArtifactLookupResponse(BaseModel):
    artifact: ArtifactResponse | None


class ArtifactPathRequest(BaseModel):
    path: str = Field(min_length=1, max_length=4096)


class KeepUpdatedRequest(BaseModel):
    keep_updated: bool


class BindRequest(BaseModel):
    thread_id: ThreadId


class ArtifactThreadResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    thread_id: str
    title: str | None
    workspace_id: str
    usable: bool
    home: bool


class DefaultDestination(BaseModel):
    rule: Literal["bound", "beside", "creator", "new"]
    thread: ArtifactThreadResponse | None
    workspace_id: str


class DestinationsResponse(BaseModel):
    default: DefaultDestination
    bound: ArtifactThreadResponse | None
    beside: ArtifactThreadResponse | None
    creator: ArtifactThreadResponse | None


class ShareFileEntryResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    path: str
    size: int
    reason: str


class DriftResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    added: list[str]
    removed: list[str]
    changed: list[str]


class StatusResponse(BaseModel):
    latest_seq: int
    live_sha256: str
    files: list[ShareFileEntryResponse]
    total_size: int
    changed: bool | None
    drift: DriftResponse | None


def _label(value: str | None) -> str | None:
    """The owner's own note: refused past the column's limit, never clipped,
    unlike a title read off a file."""
    if value is None:
        return None
    text = one_line(value)
    if not text:
        return None
    if len(text) > 200:
        raise ValueError("a label is at most 200 characters")
    return text


class PublishRequest(BaseModel):
    #: The paths the owner reviewed, from ``status``.
    files: list[str] = Field(min_length=1, max_length=snapshot.MAX_VERSION_FILES * 2)
    label: Annotated[str | None, AfterValidator(_label)] = None
    keep_updated: bool | None = None


class PublishResponse(BaseModel):
    artifact: ArtifactResponse
    version: ArtifactVersionResponse
    cut: bool


class ArtifactGrantResponse(BaseModel):
    #: Joined with ``v/<seq>/<entry path>``. Absolute on the artifact's own
    #: origin when ``isolated``, else a path on the API origin.
    base: str
    expires_in: int
    isolated: bool


# --- comments ----------------------------------------------------------------


def _whole_pairs(value: Any) -> Any:
    """Half a surrogate pair gets through the request's JSON parser, from a page
    or a cut in its words, and is no string to validate or store."""
    if isinstance(value, str):
        return value.encode("utf-16", "surrogatepass").decode("utf-16", "replace")
    if isinstance(value, dict):
        return {k: _whole_pairs(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_whole_pairs(v) for v in value]
    return value


def _comment_text(value: str) -> str:
    # A NUL cannot be stored in TEXT; anything else is the owner's own words.
    value = value.replace("\x00", "").strip()
    if not value:
        raise ValueError("a comment needs words")
    return value


CommentText = Annotated[
    str,
    BeforeValidator(_whole_pairs),
    Field(max_length=8000),
    AfterValidator(_comment_text),
]


def _anchor_text(limit: int):
    return Field(default=None, max_length=limit)


class AnchorPoint(BaseModel):
    """Where on the anchored element the pin sits, for drawing it again."""

    model_config = ConfigDict(extra="forbid")
    x: float
    y: float

    @field_validator("x", "y")
    @classmethod
    def _finite(cls, v: float) -> float:
        if not math.isfinite(v):
            raise ValueError("must be finite")
        return v


class AreaElement(BaseModel):
    """One element of an area, named as an element anchor names it."""

    model_config = ConfigDict(extra="forbid")
    path: str | None = _anchor_text(1000)
    label: str | None = _anchor_text(300)
    quote: str | None = _anchor_text(2000)

    @model_validator(mode="after")
    def _findable(self) -> AreaElement:
        if not (self.path or self.quote):
            raise ValueError("an area's element needs a path or a quote")
        return self


class AreaRect(BaseModel):
    """Where an area sat, as fractions of the document's scroll size."""

    model_config = ConfigDict(extra="forbid")
    x: float = Field(ge=0, le=1)
    y: float = Field(ge=0, le=1)
    w: float = Field(ge=0, le=1)
    h: float = Field(ge=0, le=1)


class CommentAnchor(BaseModel):
    """What the owner pinned on the live file, so it can be found again after
    the file changes. Every text field was read off the file; the comment's
    own path names the file."""

    model_config = ConfigDict(extra="forbid")

    @model_validator(mode="before")
    @classmethod
    def _storable_text(cls, value: Any) -> Any:
        return _whole_pairs(value)

    #: A clicked element, a text selection, a spot the file names itself, or
    #: an area dragged around several elements.
    kind: Literal["element", "text", "named", "area"]
    #: A CSS path from the nearest ancestor with an id.
    path: str | None = _anchor_text(1000)
    #: A short name for the spot, from the file or its nearest heading.
    label: str | None = _anchor_text(300)
    #: The selected words, or the element's text as it showed.
    quote: str | None = _anchor_text(2000)
    #: The words around a selection, to find it again.
    prefix: str | None = _anchor_text(200)
    suffix: str | None = _anchor_text(200)
    #: What a named spot's file says about it.
    detail: str | None = _anchor_text(1000)
    point: AnchorPoint | None = None
    #: An area's first elements in document order, ``count`` of them in all,
    #: and where it sat. The label names its parts.
    elements: list[AreaElement] | None = Field(default=None, max_length=8)
    count: int | None = Field(default=None, ge=1, le=500)
    rect: AreaRect | None = None

    @model_validator(mode="after")
    def _area_fields(self) -> CommentAnchor:
        area = (self.elements, self.count, self.rect)
        if self.kind != "area":
            if any(v is not None for v in area):
                raise ValueError("elements, count and rect belong to an area")
            return self
        # One element is that element's comment; the file sends it as one.
        if not self.elements or len(self.elements) < 2:
            raise ValueError("an area holds at least two elements")
        if self.count is None or self.count < len(self.elements):
            raise ValueError("an area counts at least the elements it lists")
        return self


class CommentMessageResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    message_id: str
    seq: int
    author: Literal["user", "agent"]
    body: str
    sent_at: datetime | None
    request_key: str | None
    #: An owner's message: the thread a send carried it to. An agent's: the
    #: thread it answered from.
    thread_id: str | None
    created_at: datetime


class CommentResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    comment_id: str
    artifact_id: str
    number: int
    path: str
    file_sha256: str | None
    title: str | None
    anchor: CommentAnchor
    status: Literal["open", "resolved"]
    delivery: Literal["note", "queued", "sent"]
    thread_id: str | None
    thread_title: str | None
    thread_archived: bool
    created_at: datetime
    updated_at: datetime
    resolved_at: datetime | None
    messages: list[CommentMessageResponse]
    read_through: int
    unread: int

    @field_serializer("anchor")
    def _stored_shape(self, anchor: CommentAnchor) -> dict[str, Any]:
        # Rows hold the anchor without its unset fields, and the page reads
        # absent keys, so the typed model must not print them as nulls.
        return anchor.model_dump(exclude_none=True)


class QueuedCommentResponse(CommentResponse):
    workspace_id: str


class CommentListResponse(BaseModel):
    comments: list[CommentResponse]


class QueuedCommentListResponse(BaseModel):
    comments: list[QueuedCommentResponse]


def _sha256(value: str | None) -> str | None:
    if value is None:
        return None
    value = value.strip().lower()
    if not _SHA256.fullmatch(value):
        raise ValueError("a sha256 is 64 hex characters")
    return value


class CreateCommentRequest(BaseModel):
    anchor: CommentAnchor
    body: CommentText
    #: A new comment waits on the file or in a thread's queue; only a chat
    #: send carries it to the agent.
    delivery: Literal["note", "queued"] = "note"
    thread_id: ThreadId | None = None
    file_sha256: Annotated[str | None, AfterValidator(_sha256)] = None
    title: str | None = Field(default=None, max_length=2000)

    @field_validator("title", mode="before")
    @classmethod
    def _storable_title(cls, value: Any) -> Any:
        return _whole_pairs(value)


class AddCommentMessageRequest(BaseModel):
    body: CommentText
    delivery: Literal["note", "queued"] | None = None
    thread_id: ThreadId | None = None


class UpdateCommentRequest(BaseModel):
    delivery: Literal["note", "queued"] | None = None
    thread_id: ThreadId | None = None
    status: Literal["open", "resolved"] | None = None


class MarkCommentReadRequest(BaseModel):
    #: The ``seq`` of the newest message the view displayed, so a reply that
    #: landed after it stays unread.
    through: int = Field(ge=0, le=2**31 - 1)
