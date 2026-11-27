"""What the Drive's tables hold, as the service hands it out (migration 150)."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import TYPE_CHECKING, Any, Literal

if TYPE_CHECKING:
    from ptc_agent.core.sandbox.livefs_mount import CallContext

ActorKind = Literal["person", "agent", "system"]
Via = Literal["app", "cli", "tool", "mount", "api", "import"]
ErrorCode = Literal[
    "not_found",
    "exists",
    "changed",
    "is_folder",
    "not_folder",
    "not_empty",
    "invalid_path",
    "invalid_range",
    "too_large",
    "over_quota",
    "too_many_entries",
    "forbidden",
    "unavailable",
    "too_many_uploads",
    "upload_ended",
    "parts_missing",
    "bad_part",
    "bad_digest",
    "mismatch",
    "checkout_required",
    "not_memo",
    "not_text",
    "invalid",
]

#: How long the source of a rename over a file is kept as a delete notice.
TOMBSTONE_HOURS = 24
#: Deleted rows the sweep may purge: the trash past its days, and a rename's
#: tombstone past ``TOMBSTONE_HOURS``.
EXPIRED_SQL = (
    "e.deleted_at IS NOT NULL AND ("
    "(e.ended = 'trash' AND e.deleted_at < now() - make_interval(days => %(days)s))"
    " OR (e.ended = 'tombstone'"
    " AND e.deleted_at < now() - make_interval(hours => %(hours)s)))"
)


#: A space's bytes by what keeps them. ``live``: content any live entry's
#: versions reference, which the allowance counts. ``trash``: content only
#: trashed entries reference, which it does not. Content both reference is
#: counted once, as live. ``released``: replaced copies no version keeps,
#: waiting to be freed (``released_at``; a row is released exactly while no
#: version references it), counted in neither. Worked out from the deleted
#: side (``live`` is all content less what only deleted entries hold and
#: what is released), each lookup an index probe (each ``LATERAL ... OFFSET
#: 0``, which the planner keeps as one), so its cost follows the trash, not
#: the space: it runs under the space's lock on every save.
#: A space's counts (migration 150's ``COUNTERS``): ``live`` bytes a live
#: file's version holds, ``trash`` bytes only deleted entries hold,
#: ``released`` copies awaiting their free, and every entry row.
COUNTS_SQL = """
    SELECT held_bytes - released_bytes - gone_bytes AS live, gone_bytes AS trash,
           released_bytes AS released, entry_count AS entries
      FROM drive_spaces WHERE space_id = %(space)s
"""


def over_quota() -> DriveError:
    # The trash is outside the allowance, so emptying it frees nothing here.
    return DriveError("over_quota", "The Drive is full; delete files or clear a file's earlier versions")


@dataclass(frozen=True)
class Actor:
    """Who a change is recorded as made by.

    ``sitting`` names one stretch of work: saves to one file by the same
    actor under the same sitting are one version.
    """

    kind: ActorKind
    #: The person, or the person the agent acts for.
    user_id: str
    via: Via
    #: ``main`` or the subagent's name.
    agent: str | None = None
    thread_id: str | None = None
    run_id: str | None = None
    workspace_id: str | None = None
    computer_id: str | None = None
    sitting: str | None = None
    #: The shell command a change through the mount came from (its call,
    #: else its process session), which ties a memo renamed aside to a file
    #: written at its name after (``aside``). Never stored on a version.
    command: str | None = None

    @classmethod
    def of_call(
        cls,
        context: CallContext | None,
        *,
        user_id: str,
        via: Via,
        sitting: str | None,
        computer_id: str | None = None,
        command: str | None = None,
    ) -> Actor:
        """The agent, as the tool call ``context`` was filed for (None: a
        change no call filed, still the agent's, for no thread)."""
        return cls(
            kind="agent",
            user_id=user_id,
            via=via,
            agent=context.agent if context is not None else None,
            thread_id=context.thread_id if context is not None else None,
            run_id=context.run_id if context is not None else None,
            workspace_id=context.workspace_id if context is not None else None,
            computer_id=computer_id,
            sitting=sitting,
            command=command,
        )


class DriveError(Exception):
    def __init__(
        self,
        code: ErrorCode,
        message: str,
        details: dict[str, Any] | None = None,
        *,
        retry_after: int | None = None,
    ) -> None:
        super().__init__(message)
        self.code: ErrorCode = code
        self.message = message
        #: More the refusal says, which every surface adds to its answer.
        self.details = details or {}
        #: Seconds an ``unavailable`` caller waits before asking again.
        self.retry_after = retry_after


class DriveBusy(DriveError):
    """No room for a save among the bytes this worker holds, refused before
    its body is read, so sending it again cannot save it twice."""

    def __init__(self) -> None:
        super().__init__("unavailable", "Too many saves at once; retry shortly")


@dataclass(frozen=True)
class Space:
    space_id: str
    kind: Literal["personal", "team", "org"]
    #: The owner of a personal space.
    user_id: str | None
    created_at: datetime


@dataclass(frozen=True)
class MemoMark:
    """A file's mark as a memo. ``marked`` False: it was one and was
    un-marked, its record kept."""

    marked: bool
    status: Literal["draft", "final"] | None
    tickers: tuple[str, ...]
    #: The version pinned when the status last became final.
    final_seq: int | None
    #: When that pin was set.
    final_at: datetime | None = None
    #: The head when the mark was last put on or taken off. Versions up to
    #: it stay as written (no save folds into them), and are a former
    #: memo's record.
    kept_through_seq: int = 0


@dataclass(frozen=True)
class Entry:
    entry_id: str
    space_id: str
    path: str
    parent: str
    name: str
    kind: Literal["file", "folder"]
    #: The head version's seq; None for a folder.
    head_seq: int | None
    sha256: str | None
    size: int | None
    content_type: str | None
    is_text: bool | None
    created_at: datetime
    modified_at: datetime
    changed_seq: int
    deleted_at: datetime | None
    trash_id: str | None
    deleted_by: str | None
    #: Who wrote the head version; None for a folder.
    modified_by: Actor | None
    #: None for a file that was never a memo, and for a folder.
    memo: MemoMark | None = None
    #: Why the head version was made, as it says.
    head_note: str | None = None
    #: Until when Undo is offered; None when it is not.
    undo_until: datetime | None = None
    #: The agent's change (check-out) the head version was made under.
    head_change_id: str | None = None

    @property
    def is_memo(self) -> bool:
        """A live memo, which keeps every version and needs the agent's reasons."""
        return self.memo is not None and self.memo.marked

    @property
    def has_record(self) -> bool:
        """It is or was a memo, so it has a record of its mark."""
        return self.memo is not None


@dataclass(frozen=True)
class Version:
    seq: int
    sha256: str
    size: int
    content_type: str
    created_at: datetime
    note: str | None
    restored_from: int | None
    actor: Actor
    #: Held as text in the database.
    is_text: bool = False
    #: When this old version goes (an ordinary file's undo copy); None for
    #: the head and for a version a memo keeps.
    expires_at: datetime | None = None
    #: The agent's change (check-out) it was made under; its note is that
    #: change's reason.
    change_id: str | None = None


@dataclass(frozen=True)
class Content:
    """Where one version's bytes are, without the bytes."""

    space_id: str
    sha256: str
    size: int
    #: Read for a version (``content_of``), the version's kind; read by
    #: digest alone (``content_by_sha``), whether the bytes are text the
    #: database holds, which says nothing of any file's kind.
    is_text: bool
    content_type: str
    #: The user whose object-storage key holds the bytes; None when the
    #: database holds them.
    blob_user_id: str | None


@dataclass(frozen=True)
class TrashItem:
    """One delete: the entry the person deleted and how much went with it."""

    trash_id: str
    entry: Entry
    #: Entries the delete took, the deleted one included.
    entries: int
    deleted_at: datetime
    deleted_by: str | None


@dataclass(frozen=True)
class Usage:
    #: Bytes counted against the allowance: live files and their versions.
    bytes: int
    #: Bytes only the trash keeps, not counted.
    trash_bytes: int
    entries: int
    #: None: no cap on bytes.
    max_bytes: int | None
    max_entries: int
    #: The most one request carries whole: a plain save, the mount's, the agent's.
    max_file_bytes: int
    #: The largest file an upload in parts may carry.
    max_upload_bytes: int


@dataclass(frozen=True)
class Changes:
    #: Entries changed after the cursor, trashed ones included as the notice
    #: of their delete.
    entries: list[Entry]
    cursor: int
    #: The cursor is older than the last purge: start again from 0.
    resync: bool


@dataclass(frozen=True)
class MemoEvent:
    """A change to a file's mark, in its record beside its versions."""

    event_id: int
    kind: Literal["mark", "unmark", "update", "clear"]
    created_at: datetime
    actor: Actor
    note: str | None
    #: The file's head when it happened.
    head_seq: int | None
    status_before: str | None
    status_after: str | None
    tickers_before: tuple[str, ...] | None
    tickers_after: tuple[str, ...] | None
    #: The version this event pinned, if it pinned one.
    final_seq: int | None
    #: Versions a ``clear`` dropped.
    cleared: int | None


@dataclass(frozen=True)
class MemoChange:
    """One check-out of a memo by one chat: the agent's versions under it
    carry its reason as their note."""

    change_id: str
    entry_id: str
    thread_id: str | None
    computer_id: str | None
    reason: str
    opened_at: datetime
    touched_at: datetime
    #: When it was checked in, or ended by a newer change; None while it may
    #: be open (``policy.is_open`` decides).
    ended_at: datetime | None
    versions: int = 0
    first_seq: int | None = None
    last_seq: int | None = None


def a_file(found: Entry | None) -> Entry:
    """``found`` when it is a live file: ``not_found`` for nothing there,
    ``is_folder`` for a folder."""
    if found is None:
        raise DriveError("not_found", "No such entry")
    if found.kind != "file":
        raise DriveError("is_folder", f"{found.path} is a folder")
    return found


@dataclass(frozen=True)
class RecordVersion:
    version: Version
    #: Lines added and removed against the version before; None until counted.
    added: int | None
    removed: int | None


@dataclass(frozen=True)
class RecordPage:
    #: Newest first.
    items: list[RecordVersion | MemoEvent]
    #: Opaque; the page after this one, None at the end.
    next: str | None
    #: The time for counting lines ran out: the next read counts on.
    counting: bool = False


@dataclass(frozen=True)
class MemoPage:
    #: Each live memo with its number of versions.
    items: list[tuple[Entry, int]]
    next: str | None


@dataclass(frozen=True)
class VersionText:
    seq: int
    sha256: str
    source: Literal["text", "extracted"]
    status: Literal["ok", "empty", "failed", "too_large"]
    text: str | None
    truncated: bool


@dataclass(frozen=True)
class Attribution:
    seq: int
    sha256: str
    line_count: int
    #: ``(start_line, count, seq)``, 1-based: which version wrote each run.
    runs: list[tuple[int, int, int]]
    #: Only the versions the runs name.
    versions: list[Version]


@dataclass(frozen=True)
class HistoryRule:
    """What an ordinary file keeps of its history (``drive.history`` in
    config): the copy before its last save, for ``undo_days``, unless that
    copy is ``max_bytes`` or more. A memo keeps every version."""

    #: A replaced copy this size or larger is not kept; None keeps every size.
    max_bytes: int | None = 100 * 1024 * 1024
    undo_days: int = 7
    grace_seconds: int = 3600


@dataclass(frozen=True)
class Limits:
    #: None: no cap on bytes.
    max_space_bytes: int | None
    max_entries: int
    #: Bytes the trash alone may keep before its oldest items are purged;
    #: None with no cap on bytes, when only the trash's days bound it.
    #: Released copies waiting out their grace count toward it too.
    max_trash_bytes: int | None
    #: The largest file the space takes, by any route.
    max_file_bytes: int
    #: The most one request carries whole in a worker's memory.
    max_request_bytes: int
    history: HistoryRule = HistoryRule()

    def too_large(self, size: int, cap: int | None = None) -> None:
        """Refuse ``size`` past ``cap`` (``max_file_bytes`` unless given)."""
        cap = self.max_file_bytes if cap is None else cap
        if size > cap:
            raise DriveError("too_large", f"A file is at most {cap // (1024 * 1024)} MB")


#: ``Actor`` fields and the ``drive_versions`` columns that hold them.
_ACTOR_COLUMNS = {
    "kind": "actor_kind",
    "user_id": "actor_user_id",
    "via": "via",
    "agent": "agent",
    "thread_id": "thread_id",
    "run_id": "run_id",
    "workspace_id": "workspace_id",
    "computer_id": "computer_id",
    "sitting": "sitting",
}
_BY = ", ".join(f"v.{col} AS by_{name}" for name, col in _ACTOR_COLUMNS.items())

#: Columns named as ``Entry``'s fields; what the head holds and who wrote it
#: come from the head version.
ENTRY_COLUMNS = (
    "e.entry_id::text AS entry_id, e.space_id::text AS space_id, e.path, e.parent,"
    " e.name, e.kind, e.head_seq, v.sha256, v.byte_len AS size, v.content_type,"
    " v.is_text, e.created_at, e.modified_at, e.changed_seq, e.deleted_at,"
    f" e.trash_id::text AS trash_id, e.deleted_by, {_BY},"
    " m.is_memo AS memo_marked, m.status AS memo_status, m.tickers AS memo_tickers,"
    " m.final_seq AS memo_final_seq, m.final_at AS memo_final_at,"
    " m.kept_through_seq AS memo_kept_through_seq, v.note AS head_note,"
    " e.undo_until, v.change_id::text AS head_change_id"
)
ENTRY_FROM = (
    "drive_entries e LEFT JOIN drive_versions v"
    " ON v.entry_id = e.entry_id AND v.seq = e.head_seq"
    " LEFT JOIN drive_memos m ON m.entry_id = e.entry_id"
)
#: Columns named as ``Version``'s fields, read ``FROM drive_versions v``.
VERSION_COLUMNS = (
    "v.seq, v.sha256, v.byte_len AS size, v.content_type, v.created_at, v.note,"
    f" v.restored_from, v.is_text, v.change_id::text AS change_id, {_BY}"
)


#: Columns named as ``MemoEvent``'s fields, read ``FROM drive_memo_events v``;
#: an event has no sitting.
EVENT_COLUMNS = (
    "v.event_id, v.kind, v.created_at, v.note, v.head_seq, v.status_before,"
    " v.status_after, v.tickers_before, v.tickers_after, v.final_seq, v.cleared, "
    + ", ".join(
        "NULL AS by_sitting" if name == "sitting" else f"v.{col} AS by_{name}"
        for name, col in _ACTOR_COLUMNS.items()
    )
)


def _actor(row: dict[str, Any]) -> Actor | None:
    by = {name: row.pop(f"by_{name}") for name in _ACTOR_COLUMNS}
    return Actor(**by) if by["kind"] is not None else None


def entry_from_row(row: dict[str, Any]) -> Entry:
    row = dict(row)
    marked = row.pop("memo_marked")
    status, tickers, final_seq, final_at = (
        row.pop("memo_status"),
        row.pop("memo_tickers"),
        row.pop("memo_final_seq"),
        row.pop("memo_final_at"),
    )
    kept_through_seq = row.pop("memo_kept_through_seq")
    memo = None
    if marked is not None:
        memo = MemoMark(
            marked=marked,
            status=status,
            tickers=tuple(tickers or ()),
            final_seq=final_seq,
            final_at=final_at,
            kept_through_seq=kept_through_seq or 0,
        )
    return Entry(modified_by=_actor(row), memo=memo, **row)


def version_from_row(row: dict[str, Any]) -> Version:
    row = dict(row)
    return Version(actor=_actor(row), **row)


def event_from_row(row: dict[str, Any]) -> MemoEvent:
    row = dict(row)
    for side in ("tickers_before", "tickers_after"):
        if row[side] is not None:
            row[side] = tuple(row[side])
    return MemoEvent(actor=_actor(row), **row)
