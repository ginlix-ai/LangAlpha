"""Request and response shapes of the Drive API (``/api/v1/drive``)."""

from __future__ import annotations

from datetime import datetime
from typing import Annotated, Literal

from pydantic import BaseModel, ConfigDict, Field


class _Read(BaseModel):
    """Built from the service's records by attribute (``model_validate``)."""

    model_config = ConfigDict(from_attributes=True)


class ModifiedByOut(_Read):
    kind: Literal["person", "agent", "system"]
    user_id: str
    agent: str | None
    via: str
    thread_id: str | None
    #: The thread's title, where the caller may see the thread; null for a
    #: thread they may not, one gone, or none. Filled on a single entry, the
    #: memos list, versions and the record; null in folder listings.
    thread_title: str | None = None


class MemoMarkOut(_Read):
    #: False: it was a memo and was un-marked; its record is kept.
    marked: bool
    status: Literal["draft", "final"] | None
    tickers: list[str]
    #: The version pinned when the status last became final.
    final_seq: int | None
    #: When that pin was set.
    final_at: datetime | None = None


class EntryOut(_Read):
    id: str = Field(validation_alias="entry_id")
    path: str
    name: str
    kind: Literal["file", "folder"]
    size: int | None
    sha256: str | None
    content_type: str | None
    is_text: bool | None
    #: The head version's seq; None for a folder.
    version: int | None = Field(validation_alias="head_seq")
    created_at: datetime
    modified_at: datetime
    modified_by: ModifiedByOut | None
    #: Set on a trashed entry, as a change feed's delete notice.
    deleted_at: datetime | None = None
    #: Null for a file that was never a memo, and for a folder.
    memo: MemoMarkOut | None = None
    #: Why the head version was made.
    note: str | None = Field(None, validation_alias="head_note")
    #: When the copy from before the last save goes; null with none.
    undo_until: datetime | None = None


class VersionActorOut(ModifiedByOut):
    run_id: str | None
    workspace_id: str | None


class VersionOut(_Read):
    seq: int
    size: int
    sha256: str
    created_at: datetime
    actor: VersionActorOut
    note: str | None
    restored_from: int | None
    #: This version's own kind, which can differ from the head's.
    is_text: bool
    #: When this undo copy goes; null for the head and a memo's versions.
    expires_at: datetime | None = None
    #: How its text is read: as stored, extracted (Word, PDF), or not at all.
    text: Literal["text", "extracted", "none"]
    #: The agent's change (check-out) it was made under; consecutive versions
    #: with one id are one change, whose reason is their note.
    change_id: str | None = None


class UsageOut(_Read):
    #: Counted against the allowance: live files and their old versions.
    bytes: int
    #: Kept only by the trash, not counted.
    trash_bytes: int
    entries: int
    max_bytes: int | None
    max_entries: int
    #: The most one request (``PUT /content``) carries.
    max_file_bytes: int
    #: The largest file an upload in parts carries.
    max_upload_bytes: int


class SpaceOut(_Read):
    id: str
    kind: Literal["personal", "team", "org"]
    created_at: datetime
    usage: UsageOut
    #: The space's current change cursor, for ``changes?since=``.
    change_seq: int
    #: Days a trashed entry is kept before it is purged.
    trash_days: int


class SpacesOut(BaseModel):
    spaces: list[SpaceOut]


class EntriesOut(_Read):
    entries: list[EntryOut]


class TrashItemOut(_Read):
    trash_id: str
    entry: EntryOut
    entries: int
    deleted_at: datetime
    deleted_by: str | None


class TrashOut(_Read):
    items: list[TrashItemOut]
    #: More items than ``limit`` are in the trash.
    more: bool = False


class VersionsOut(_Read):
    versions: list[VersionOut]
    #: Bytes the old versions hold that nothing else references: what
    #: clearing the file's history frees.
    history_bytes: int
    #: How many versions clearing the file's history drops: every one but
    #: the head and a memo's pinned final, as the clear itself reads them.
    clearable: int
    #: The head is too large to keep: a save replacing it keeps no old version.
    too_large_for_history: bool = False
    #: The file's memo mark, as on its entry; null for a file never marked.
    memo: MemoMarkOut | None = None


class HistoryClearIn(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    path: str | None = Field(None, min_length=1, max_length=2048)
    id: str | None = None
    #: Drop only versions below this seq; all old versions when absent.
    before: int | None = Field(None, ge=1)
    #: The memo's pinned final as the caller was shown it, null for none.
    #: When sent, a clear is refused ``changed`` once the pin has moved,
    #: since the version it kept is one the caller was told would stay.
    final_seq: int | None = Field(None, ge=1)


class HistoryClearedOut(BaseModel):
    dropped: int
    freed_bytes: int


class ChangesOut(_Read):
    entries: list[EntryOut]
    cursor: int
    resync: bool


class PathIn(BaseModel):
    path: str = Field(min_length=1, max_length=2048)


class MoveIn(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    src: str = Field(alias="from", min_length=1, max_length=2048)
    dst: str = Field(alias="to", min_length=1, max_length=2048)


class TrashIdIn(BaseModel):
    trash_id: str


class TrashedOut(BaseModel):
    trash_id: str


class RestoredOut(_Read):
    entries: list[EntryOut]


class PurgedOut(BaseModel):
    purged: int


class VersionRestoreIn(BaseModel):
    id: str
    seq: int = Field(ge=1)
    note: str | None = Field(None, max_length=500)


class MemoIn(BaseModel):
    """What to change of a file's mark: only the fields sent change."""

    path: str | None = Field(None, min_length=1, max_length=2048)
    id: str | None = None
    memo: bool | None = None
    #: ``draft``, ``final`` or null; anything else answers ``invalid``.
    status: str | None = Field(None, max_length=24)
    tickers: list[str] | None = Field(None, max_length=100)
    note: str | None = Field(None, max_length=500)
    #: The head's sha256 as the caller saw it: a status set to final pins
    #: the head, so the caller names the version it means to sign off.
    if_match: str | None = None


class MemoOut(BaseModel):
    entry: EntryOut
    versions: int


class MemosOut(BaseModel):
    memos: list[MemoOut]
    next: str | None


class RecordVersionOut(VersionOut):
    type: Literal["version"] = "version"
    #: Lines added and removed against the version before; null until counted.
    added: int | None = None
    removed: int | None = None


class RecordEventOut(_Read):
    type: Literal["event"] = "event"
    id: int = Field(validation_alias="event_id")
    kind: Literal["mark", "unmark", "update", "clear"]
    created_at: datetime
    actor: VersionActorOut
    note: str | None
    head_seq: int | None
    status_before: str | None
    status_after: str | None
    tickers_before: list[str] | None
    tickers_after: list[str] | None
    final_seq: int | None
    cleared: int | None


RecordItemOut = Annotated[RecordVersionOut | RecordEventOut, Field(discriminator="type")]


class RecordOut(BaseModel):
    items: list[RecordItemOut]
    next: str | None
    #: Some counts are null only because the read ran out of time: read again.
    counting: bool = False


class VersionTextOut(_Read):
    seq: int
    sha256: str
    #: ``text``: as stored; ``extracted``: read out of a Word or PDF file.
    source: Literal["text", "extracted"]
    #: Extracted text only: ``empty`` (none found, as in a scan), ``failed``
    #: (unreadable; tried again after an hour), ``too_large``.
    status: Literal["ok", "empty", "failed", "too_large"]
    text: str | None
    #: The extracted text was cut at 1 MiB.
    truncated: bool


class AttributionRunOut(BaseModel):
    #: 1-based first line of the run.
    start: int
    count: int
    #: The version that wrote these lines.
    seq: int


class AttributionOut(BaseModel):
    #: The head the attribution is of.
    seq: int
    sha256: str
    line_count: int
    runs: list[AttributionRunOut]
    #: Only the versions the runs name.
    versions: list[VersionOut]


class UndoIn(BaseModel):
    path: str | None = Field(None, min_length=1, max_length=2048)
    id: str | None = None
    #: The head the caller saw; refused ``changed`` when it moved since.
    if_match: str | None = Field(None, max_length=128)
    note: str | None = Field(None, max_length=500)


class VersionNoteIn(BaseModel):
    path: str | None = Field(None, min_length=1, max_length=2048)
    id: str | None = None
    #: The head when absent.
    seq: int | None = Field(None, ge=1)
    note: str = Field(min_length=1, max_length=500)


class CheckoutIn(BaseModel):
    path: str | None = Field(None, min_length=1, max_length=2048)
    id: str | None = None
    #: Opens a new change; exactly one of this and ``continue``.
    reason: str | None = Field(None, min_length=1, max_length=500)
    #: Reopens the chat's newest change to the memo.
    continue_: bool = Field(False, alias="continue")


class CheckinIn(BaseModel):
    path: str | None = Field(None, min_length=1, max_length=2048)
    id: str | None = None
    #: Rewords the change for every version made under it.
    reason: str | None = Field(None, min_length=1, max_length=500)


class ChangeOut(BaseModel):
    change_id: str
    path: str
    reason: str
    #: The answer to ``continue``: an earlier change reopened.
    continued: bool
    #: Versions made under the change so far; the seqs null with none.
    versions: int
    first_seq: int | None
    last_seq: int | None
    head_seq: int | None
    #: The head a check-out put under the change (a save with no reason).
    adopted_seq: int | None


class UploadIn(BaseModel):
    path: str = Field(min_length=1, max_length=2048)
    size: int = Field(ge=0)
    #: The whole file's SHA-256 as the client has it; checked, never taken.
    sha256: str | None = Field(None, pattern=r"^[0-9a-fA-F]{64}$")
    if_match: str | None = Field(None, max_length=128)
    if_none_match: bool = False
    note: str | None = Field(None, max_length=500)
    content_type: str | None = Field(None, max_length=255)
    route: Literal["auto", "relay"] = "auto"


class UploadPartIn(BaseModel):
    part_number: int = Field(ge=1)
    #: The part's MD5, base64, as ``Content-MD5`` carries it.
    md5: str | None = Field(None, max_length=64)


class UploadSignIn(BaseModel):
    parts: list[UploadPartIn] = Field(min_length=1, max_length=32)
