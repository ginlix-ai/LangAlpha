"""Request and response shapes of the Drive API (``/api/v1/drive``)."""

from __future__ import annotations

from datetime import datetime
from typing import Literal

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
    #: When this old version goes under the history rule; null for the head,
    #: for text and for one kept without a limit.
    expires_at: datetime | None = None


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
    #: The head is too large to keep: a save replacing it keeps no old version.
    too_large_for_history: bool = False


class HistoryClearIn(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    path: str | None = Field(None, min_length=1, max_length=2048)
    id: str | None = None
    #: Drop only versions below this seq; all old versions when absent.
    before: int | None = Field(None, ge=1)


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
