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
    model_serializer,
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
    #: Kept for the owner (their dashboard): versions without sharing.
    kept: bool = False
    #: The newest version, what the owner's views open.
    latest_seq: int
    #: The version visitors see, which a kept record's turns can leave behind
    #: ``latest_seq``; 0 when none is shared.
    shared_seq: int = 0
    bound_thread_id: str | None
    open_comments: int
    unread: int
    created_at: datetime
    #: A link shared before versions whose files are past the caps: visitors
    #: get a 404 until the owner publishes.
    legacy_refused: Literal["too_many_files", "too_many_bytes"] | None = None
    #: Who the link opens to: anyone with it, or members of the owner's
    #: organization only.
    audience: Literal["public", "organization"] = "public"
    #: The audiences the owner may choose now.
    audiences: list[Literal["public", "organization"]] = Field(
        default_factory=lambda: ["public"]
    )


class AudienceRequest(BaseModel):
    audience: Literal["public", "organization"]


class ArtifactLookupResponse(BaseModel):
    artifact: ArtifactResponse | None


class ArtifactPathRequest(BaseModel):
    path: str = Field(min_length=1, max_length=4096)


class KeepUpdatedRequest(BaseModel):
    keep_updated: bool


class LeftOutRequest(BaseModel):
    #: The column's limit (``served_paths.may_be_artifact``).
    path: str = Field(min_length=1, max_length=1024)
    left_out: bool


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
    #: Null for a left-out file nothing could probe (``ManifestEntry.size``).
    size: int | None
    reason: str
    #: The owner may leave it out (a link, a guess, a load no report names now).
    optional: bool = False


class DriftResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    added: list[str]
    removed: list[str]
    changed: list[str]


class ShareCapResponse(BaseModel):
    code: Literal["too_many_files", "too_many_bytes"]
    limit: int


class StatusResponse(BaseModel):
    latest_seq: int
    #: Null past a cap, when nothing was read: as for the page's code.
    live_sha256: str | None
    #: The page's code as this list was read (``Snapshot.page_sha256``), which
    #: a publish sends back.
    page_sha256: str | None
    files: list[ShareFileEntryResponse]
    #: What the owner left out and the page still names, to offer back.
    left_out: list[ShareFileEntryResponse]
    total_size: int
    changed: bool | None
    drift: DriftResponse | None
    #: The cap the list crossed, which a publish refuses until the owner leaves
    #: out enough; ``files`` then stops at the file that crossed it.
    over_cap: ShareCapResponse | None = None
    #: What the files load from the root of the origin, as written, which no
    #: view of them serves: a warning, never part of the list.
    root_refs: list[str] = []


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


#: As many as a page's bridge reports. Past the version cap the walk refuses the
#: list with that reason, which the review shows, rather than this refusing it.
_MAX_LOADED = 500
#: A path the column holds (``served_paths.may_be_artifact``), refused here
#: rather than folded on the event loop and dropped after.
LoadedPath = Annotated[str, Field(max_length=1024)]


class StatusRequest(BaseModel):
    #: What the live page reported loading. It only ever adds to the list the
    #: owner reviews, so a page that lies shows the owner a file, nothing more.
    loaded: list[LoadedPath] = Field(default_factory=list, max_length=_MAX_LOADED)
    #: What an earlier document of the page loaded in this tab and the one now
    #: has not: kept, as a load the record remembers is, and the owner's to
    #: leave out.
    earlier: list[LoadedPath] = Field(default_factory=list, max_length=_MAX_LOADED)


class KeptRequest(BaseModel):
    kept: bool
    #: What the open page reported loading, as a publish sends it: a file it
    #: fetches by a path built in code is named by nothing else.
    loaded: list[LoadedPath] = Field(default_factory=list, max_length=_MAX_LOADED)
    earlier: list[LoadedPath] = Field(default_factory=list, max_length=_MAX_LOADED)


class PublishRequest(BaseModel):
    #: The paths the owner reviewed, from ``status``.
    files: list[str] = Field(min_length=1, max_length=snapshot.MAX_VERSION_FILES * 2)
    label: Annotated[str | None, AfterValidator(_label)] = None
    keep_updated: bool | None = None
    #: The same report the reviewed list was read with.
    loaded: list[LoadedPath] = Field(default_factory=list, max_length=_MAX_LOADED)
    earlier: list[LoadedPath] = Field(default_factory=list, max_length=_MAX_LOADED)
    #: ``StatusResponse.page_sha256`` of the reviewed list. Absent from a tab on
    #: the previous build, which publishes unchecked as it did.
    page_sha256: Annotated[str, Field(pattern=r"^[0-9a-f]{64}$")] | None = None


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


def _ascending(v: str) -> str:
    first, _, last = v.partition("-")
    if last and int(last) < int(first):
        raise ValueError(f"lines {v} run backwards")
    return v


#: 1-based source lines, one or a span running forwards: "7", "14-15".
SourceLines = Annotated[
    str,
    Field(max_length=15, pattern=r"^[1-9][0-9]{0,6}(-[1-9][0-9]{0,6})?$"),
    AfterValidator(_ascending),
]


class AreaElement(BaseModel):
    """One element of an area, named as an element anchor names it, or a
    Markdown block as a block anchor names it."""

    model_config = ConfigDict(extra="forbid")
    lines: SourceLines | None = None
    path: str | None = _anchor_text(1000)
    label: str | None = _anchor_text(300)
    quote: str | None = _anchor_text(2000)

    @model_validator(mode="after")
    def _findable(self) -> AreaElement:
        if not (self.path or self.quote or self.lines):
            raise ValueError("an area's element needs a path, a quote or lines")
        return self


_MAX_COL = 16384
_MAX_ROW = 1048576


class CellRange(BaseModel):
    """A range of a sheet's cells and the values it showed, in reading order."""

    model_config = ConfigDict(extra="forbid")
    range: str = Field(
        max_length=32,
        pattern=r"^[A-Z]{1,3}[1-9][0-9]{0,6}(:[A-Z]{1,3}[1-9][0-9]{0,6})?$",
    )
    values: str | None = _anchor_text(2000)

    @field_validator("range")
    @classmethod
    def _on_a_sheet(cls, v: str) -> str:
        """Within a sheet's bounds (XFD, row 1048576), where a viewer can find it."""
        for cell in v.split(":"):
            letters = cell.rstrip("0123456789")
            col = 0
            for ch in letters:
                col = col * 26 + ord(ch) - 64
            if col > _MAX_COL or int(cell[len(letters) :]) > _MAX_ROW:
                raise ValueError(f"{cell} is past a sheet's last cell")
        return v


class AreaRect(BaseModel):
    """Where an area sat, as fractions of the document's scroll size."""

    model_config = ConfigDict(extra="forbid")
    x: float = Field(ge=0, le=1)
    y: float = Field(ge=0, le=1)
    w: float = Field(ge=0, le=1)
    h: float = Field(ge=0, le=1)


_SPOT_FIELDS = frozenset({"path", "label", "quote", "prefix", "suffix", "detail"})
#: What each kind may carry beyond ``kind`` and ``point``: only what its
#: rendering reads, so nothing stored is left out of what the agent is shown.
_KIND_FIELDS: dict[str, frozenset[str]] = {
    "element": _SPOT_FIELDS,
    "named": _SPOT_FIELDS,
    "text": _SPOT_FIELDS | {"lines", "page"},
    "area": _SPOT_FIELDS | {"elements", "count", "rect"},
    "block": frozenset({"lines", "label", "quote"}),
    "cells": frozenset({"sheet", "ranges"}),
}
_KIND_REQUIRES = {"block": "lines", "cells": "ranges", "area": "elements"}
_KIND_FIELDS_ALL = sorted(frozenset().union(*_KIND_FIELDS.values()))


class CommentAnchor(BaseModel):
    """What the owner pinned on the live file, so it can be found again after
    the file changes. Every text field was read off the file; the comment's
    own path names the file."""

    model_config = ConfigDict(extra="forbid")

    @model_validator(mode="before")
    @classmethod
    def _storable_text(cls, value: Any) -> Any:
        return _whole_pairs(value)

    #: A clicked element, a text selection, a spot the file names itself, an
    #: area dragged around several elements, a Markdown block, or cells of a
    #: sheet.
    kind: Literal["element", "text", "named", "area", "block", "cells"]
    #: The source lines a block, or selected words, are on.
    lines: SourceLines | None = None
    #: The PDF page selected words are on.
    page: int | None = Field(default=None, ge=1, le=100_000)
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
    #: An area's elements in document order, ``count`` of them in all, and
    #: where it sat. The label names its parts. Each part is listed, picked
    #: or dragged around, so the agent reads every one it is asked to change;
    #: a drag over 64 counts the rest.
    elements: list[AreaElement] | None = Field(default=None, max_length=64)
    count: int | None = Field(default=None, ge=1, le=500)
    rect: AreaRect | None = None
    #: The cells' sheet, absent for a CSV, and each range picked.
    sheet: str | None = _anchor_text(300)
    ranges: list[CellRange] | None = Field(default=None, min_length=1, max_length=64)

    @model_validator(mode="after")
    def _kind_fields(self) -> CommentAnchor:
        allowed = _KIND_FIELDS[self.kind]
        stray = [
            f
            for f in _KIND_FIELDS_ALL
            if f not in allowed and getattr(self, f) is not None
        ]
        if stray:
            raise ValueError(f"a {self.kind} anchor has no {', '.join(stray)}")
        needed = _KIND_REQUIRES.get(self.kind)
        if needed and not getattr(self, needed):
            raise ValueError(f"a {self.kind} anchor names its {needed}")
        if self.kind == "area":
            # One element is that element's comment; the file sends it as one.
            if len(self.elements) < 2:
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
    #: In a shared workspace, the member who wrote a person's message and
    #: their name. Left out of the answer otherwise, so a personal
    #: workspace's comments read as they always have.
    written_by: str | None = None
    written_by_name: str | None = None

    @model_serializer(mode="wrap")
    def _writer_only_when_named(self, handler: Any) -> dict[str, Any]:
        out = handler(self)
        if self.written_by is None:
            out.pop("written_by", None)
            out.pop("written_by_name", None)
        return out


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
    thread_hidden: bool = False

    @model_serializer(mode="wrap")
    def _hidden_only_when_so(self, handler: Any) -> dict[str, Any]:
        out = handler(self)
        if not self.thread_hidden:
            out.pop("thread_hidden", None)
        return out

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
