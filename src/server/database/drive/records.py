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

    @classmethod
    def of_call(
        cls,
        context: CallContext | None,
        *,
        user_id: str,
        via: Via,
        sitting: str | None,
        computer_id: str | None = None,
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
        )


class DriveError(Exception):
    def __init__(self, code: ErrorCode, message: str, details: dict[str, Any] | None = None) -> None:
        super().__init__(message)
        self.code: ErrorCode = code
        self.message = message
        #: More the refusal says, which every surface adds to its answer.
        self.details = details or {}


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
    #: Held as text in the database, which keeps every version.
    is_text: bool = False
    #: When this old version goes; None for the head and one kept without limit.
    expires_at: datetime | None = None


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


def history_sql(scope: Literal["entry", "space", "spaces"]) -> str:
    """The history rule, the one place it is written: for each version in
    ``scope`` (one entry, one space, or a batch of them), ``too_large`` (a copy
    not kept once replaced) and ``goes_at`` (when it goes: now or earlier
    when it is due, NULL for the head, for text and for one kept without a
    time limit). Trimming deletes what is due, the listing shows ``goes_at``
    as ``expires_at``, the sweep looks for spaces with something due.

    Params: ``entry``, ``space`` or ``spaces`` by scope, and the rule as ``max``,
    ``keep`` and ``days`` (``HistoryRule.params``). An old version goes a
    rule's days after the save that replaced it; the count keeps the newest
    ``keep`` kept-by-time versions, text and too-large copies not counted.
    """
    spaces = {
        "entry": None,
        "space": "ARRAY[%(space)s::uuid]",
        "spaces": "%(spaces)s::uuid[]",
    }[scope]
    if spaces is None:
        where = "v.entry_id = %(entry)s::uuid"
    else:
        # Only entries with a version not held as text have anything due,
        # found by the index on those versions, a space at a time.
        where = (
            f"v.space_id = ANY({spaces}) AND v.entry_id IN (SELECT b.entry_id"
            f" FROM drive_versions b WHERE b.space_id = ANY({spaces}) AND NOT b.is_text)"
        )
    return f"""
        WITH versions AS (
            SELECT v.entry_id, v.space_id, v.seq, v.sha256, v.is_text,
                   v.seq = e.head_seq AS is_head,
                   (NOT v.is_text AND %(max)s::bigint IS NOT NULL
                    AND v.byte_len >= %(max)s::bigint) AS too_large,
                   min(v.created_at) OVER (
                       PARTITION BY v.entry_id ORDER BY v.seq DESC
                       ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS replaced_at
              FROM drive_versions v JOIN drive_entries e ON e.entry_id = v.entry_id
             WHERE {where}),
        ranked AS (
            SELECT *, count(*) FILTER (WHERE NOT (is_head OR is_text OR too_large)) OVER (
                       PARTITION BY entry_id ORDER BY seq DESC
                       ROWS UNBOUNDED PRECEDING) AS rank
              FROM versions),
        history AS (
            SELECT entry_id, space_id, seq, sha256, is_head, too_large,
                   CASE
                       WHEN is_head OR is_text THEN NULL
                       WHEN too_large OR rank > %(keep)s::int THEN now()
                       ELSE replaced_at + make_interval(days => %(days)s::int)
                   END AS goes_at
              FROM ranked)
    """


#: The old versions clearing a file's history drops (all, or those below
#: ``before``): shared by the clear and the listing's ``history_bytes``, so
#: the listing says what the clear frees. Params ``id``, ``head``, ``before``.
CLEARING_SQL = """
    dropped AS (
        SELECT seq, sha256 FROM drive_versions
         WHERE entry_id = %(id)s AND seq IS DISTINCT FROM %(head)s
           AND (%(before)s::int IS NULL OR seq < %(before)s::int))
"""


@dataclass(frozen=True)
class HistoryRule:
    """Which old versions a file keeps (``drive.history`` in config)."""

    #: A replaced copy this size or larger is not kept; None keeps every size.
    max_bytes: int | None = 100 * 1024 * 1024
    keep_days: int | None = 30
    keep_max: int | None = 10
    grace_seconds: int = 3600

    @property
    def params(self) -> dict[str, Any]:
        """The rule as ``history_sql`` takes it."""
        return {"max": self.max_bytes, "keep": self.keep_max, "days": self.keep_days}


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
    f" e.trash_id::text AS trash_id, e.deleted_by, {_BY}"
)
ENTRY_FROM = (
    "drive_entries e LEFT JOIN drive_versions v"
    " ON v.entry_id = e.entry_id AND v.seq = e.head_seq"
)
#: Columns named as ``Version``'s fields, read ``FROM drive_versions v``.
VERSION_COLUMNS = (
    "v.seq, v.sha256, v.byte_len AS size, v.content_type, v.created_at, v.note,"
    f" v.restored_from, v.is_text, {_BY}"
)


def _actor(row: dict[str, Any]) -> Actor | None:
    by = {name: row.pop(f"by_{name}") for name in _ACTOR_COLUMNS}
    return Actor(**by) if by["kind"] is not None else None


def entry_from_row(row: dict[str, Any]) -> Entry:
    row = dict(row)
    return Entry(modified_by=_actor(row), **row)


def version_from_row(row: dict[str, Any]) -> Version:
    row = dict(row)
    return Version(actor=_actor(row), **row)
