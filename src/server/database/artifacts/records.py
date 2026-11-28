"""The file's record: its ``share_links`` row, made on first need.

A chart has one too (``kind = 'chart'``, ``path`` its chart id), and so does
a Drive file (``kind = 'drive'``, in its owner's Home, ``path`` its entry id),
each only holding comments and where they go. A read by id takes any and the
record says which; a write that shares, publishes, keeps or versions takes
files alone (``FILE_RECORDS``), so none of them can reach the others."""

from __future__ import annotations

from collections.abc import Collection
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Literal, get_args

from psycopg.errors import ForeignKeyViolation
from psycopg.rows import dict_row, tuple_row

from src.server.database.pool import get_db_connection
from src.server.database.share_codes import mint_share_code
from src.server.utils.pg_sanitize import like_escape

# What the title CHECKs admit; a title is read off the file, so it is clipped
# here rather than refused.
_TITLE_CHARS = 300


RecordKind = Literal["file", "chart", "drive"]
_RECORD_KINDS: tuple[str, ...] = get_args(RecordKind)

#: The records a statement takes off ``share_links l``: a file's alone, or
#: any that holds comments, a file's, a chart's or a Drive file's.
FILE_RECORDS = "l.kind = 'file'"
COMMENT_RECORDS = "l.kind IN ('file', 'chart', 'drive')"

#: A Drive file's entry as ``de`` beside its record ``l``; null for any other
#: record. A Drive record's ``path`` is its entry id, so the file's own place
#: has to be read off the entry, which moves while the record stays put.
DRIVE_JOIN = "LEFT JOIN drive_entries de ON de.entry_id = l.drive_entry_id"
#: Where the record's file is now, over ``DRIVE_JOIN``: a Drive file's path in
#: its space, else the record's own ``path``.
PLACE_SQL = "COALESCE(de.path, l.path)"
#: The record's Drive file is deleted, over ``DRIVE_JOIN``: in the trash, or
#: ended as a tombstone (the source of a rename over a file), which nothing
#: restores and the sweep purges after a day.
DELETED_SQL = "de.deleted_at IS NOT NULL"
#: The record's Drive file is in the trash, where its owner can restore it,
#: over ``DRIVE_JOIN``. ``drive_entries_ended`` (migration 150) sets
#: ``trash_id`` on a trashed entry alone, and the test is false, never null,
#: on a record with no entry.
TRASHED_SQL = "de.trash_id IS NOT NULL"
#: The record's Drive file ended as a tombstone, over ``DRIVE_JOIN``: nothing
#: restores it, so its record and comments only wait for the sweep.
ENDED_SQL = f"({DELETED_SQL} AND NOT {TRASHED_SQL})"
#: Whether a send may carry comments on the record, over ``DRIVE_JOIN`` with
#: ``%(drive_on)s`` the owner's Drive: with it off, or the file deleted, the
#: agent has no file to open at the path the comment names.
SENDABLE_SQL = f"(l.kind <> 'drive' OR (%(drive_on)s AND NOT {DELETED_SQL}))"
#: The foreign key from a Drive record to its entry (migration 155).
_DRIVE_ENTRY_FK = "share_links_drive_entry_id_fkey"


class ArtifactNotFound(LookupError):
    """No file record by that id belongs to this user."""


def one_line(value: str | None) -> str:
    """Text as one line with single spaces; a NUL cannot be stored in TEXT."""
    return " ".join((value or "").replace("\x00", "").split())


def clip_title(value: str | None) -> str | None:
    """A title read off a file, as a record keeps it, or None for none."""
    return one_line(value)[:_TITLE_CHARS].rstrip() or None


@dataclass(frozen=True)
class Artifact:
    artifact_id: str
    code: str
    workspace_id: str
    kind: RecordKind
    #: Workspace-relative; a chart's id (``SYMBOL:timeframe``) for a chart,
    #: the entry id for a Drive file.
    path: str
    created_thread_id: str | None
    bound_thread_id: str | None
    keep_updated: bool
    #: The newest version, what the owner's views open; 0 until the first.
    latest_seq: int
    shared_at: datetime | None
    #: The paths a visitor may fetch: the ``shared_seq`` version's, or a list
    #: frozen by a share from before versions, until its first view snapshots it.
    files: tuple[str, ...] | None
    #: Moves on every share and every unshare, never on a new version.
    revision: int
    created_at: datetime
    #: Why a link shared before versions has none: its files are past the
    #: caps (``too_many_files``, ``too_many_bytes``). Visitors get a 404 and
    #: nothing tries again until the owner publishes.
    legacy_refused: str | None = None
    #: What the page loaded in code when last published, which only it can name.
    loaded_files: tuple[str, ...] = ()
    #: What the owner left out of a publish, which the page may still name.
    #: Each publish keeps only what its walk still found, so a path outlives
    #: the page naming it until the next publish at most.
    left_out_files: tuple[str, ...] = ()
    #: Kept for the owner (their dashboard): versions are cut without sharing,
    #: and the end of a turn cuts another when the files changed.
    kept_at: datetime | None = None
    #: The version visitors see, which a kept record's turns can leave behind
    #: ``latest_seq``; 0 when not shared, or shared before versions and not
    #: yet snapshotted.
    shared_seq: int = 0
    #: ``organization`` when the link opens only to members of its owner's
    #: organization; None opens it to anyone.
    audience: str | None = None

    @property
    def shared(self) -> bool:
        return self.shared_at is not None

    @property
    def kept(self) -> bool:
        return self.kept_at is not None

    @property
    def follows_turns(self) -> bool:
        """Whether the end of a turn cuts a version when the files changed: a
        record kept for its owner, shared or not, since visitors see
        ``shared_seq`` and the owner the latest, or a shared link kept updated;
        each once it has a version to follow from. A link shared before
        versions and not kept waits for its first view, which cuts v1 under
        the caps its share was made with. ``FOLLOWS_TURNS`` is the same rule
        in SQL."""
        return (self.kept and self.latest_seq > 0) or (
            self.shared and self.keep_updated and self.shared_seq > 0
        )

    @property
    def name(self) -> str:
        return self.path.rsplit("/", 1)[-1]


#: A record's columns off ``share_links l``; ``database/share_links`` reads a
#: link's with them, so a file's link carries its record.
ARTIFACT_COLUMNS = """
    l.artifact_id::text, l.code, l.workspace_id::text, l.path,
    l.created_thread_id::text, l.bound_thread_id::text, l.keep_updated,
    l.latest_seq, l.shared_at, l.files, l.revision, l.created_at,
    l.legacy_refused, l.loaded_files, l.left_out_files, l.kept_at, l.shared_seq,
    l.audience, l.kind
"""

#: ``Artifact.follows_turns`` over ``share_links l``; change the two together.
FOLLOWS_TURNS = """(
    (l.kept_at IS NOT NULL AND l.latest_seq > 0)
    OR (l.keep_updated AND l.shared_at IS NOT NULL AND l.shared_seq > 0)
)"""

#: The owner's records: ``l`` joined to a live workspace of ``%(user_id)s``.
OWNED = """
    share_links l
    JOIN workspaces w
      ON w.workspace_id = l.workspace_id
     AND w.user_id = %(user_id)s AND w.status <> 'deleted'
"""


def artifact_from_row(row: dict[str, Any] | None) -> Artifact | None:
    if row is None:
        return None
    files = row["files"]
    return Artifact(
        artifact_id=row["artifact_id"],
        code=row["code"],
        workspace_id=row["workspace_id"],
        kind=row["kind"],
        path=row["path"],
        created_thread_id=row["created_thread_id"],
        bound_thread_id=row["bound_thread_id"],
        keep_updated=row["keep_updated"],
        latest_seq=row["latest_seq"],
        shared_at=row["shared_at"],
        files=tuple(files) if files is not None else None,
        revision=row["revision"],
        created_at=row["created_at"],
        legacy_refused=row["legacy_refused"],
        loaded_files=tuple(row["loaded_files"]),
        left_out_files=tuple(row["left_out_files"]),
        kept_at=row.get("kept_at"),
        shared_seq=row.get("shared_seq", 0),
        audience=row.get("audience"),
    )


def _kind(kind: RecordKind) -> RecordKind:
    """``kind`` checked, since it is written into the statement: each kind's
    unique index is partial, which a conflict target or a generic plan finds
    only by a literal predicate, not a parameter."""
    if kind not in _RECORD_KINDS:
        raise ValueError(f"not a record kind: {kind!r}")
    return kind


async def _select_record(
    cur, workspace_id: str, kind: RecordKind, key: str
) -> Artifact | None:
    await cur.execute(
        f"""
        SELECT {ARTIFACT_COLUMNS} FROM share_links l
         WHERE l.workspace_id = %s AND l.kind = '{_kind(kind)}' AND l.path = %s
        """,
        (workspace_id, key),
    )
    return artifact_from_row(await cur.fetchone())


async def ensure_record(workspace_id: str, kind: RecordKind, key: str) -> Artifact:
    """The record of a file (``key`` its path), a chart (its chart id) or a
    Drive file (its entry id, in its owner's Home), made private on first
    sight.

    The caller has checked the workspace is reached and ``key`` is the one
    spelling of a file that may be shared, of a chart
    (``normalize_chart_id``), or of a live Drive file of the Home's owner.
    Two writers for one land on one row and one code. A Drive file purged
    after that check is ``ArtifactNotFound``, as a file already gone is.
    """
    kind = _kind(kind)
    try:
        async with get_db_connection() as conn:
            async with conn.cursor(row_factory=dict_row) as cur:
                await cur.execute(
                    f"""
                    INSERT INTO share_links AS l
                        (code, workspace_id, kind, path, drive_entry_id)
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (workspace_id, path) WHERE kind = '{kind}' DO NOTHING
                    RETURNING {ARTIFACT_COLUMNS}
                    """,
                    (
                        mint_share_code(),
                        workspace_id,
                        kind,
                        key,
                        key if kind == "drive" else None,
                    ),
                )
                created = artifact_from_row(await cur.fetchone())
                if created is not None:
                    return created
                existing = await _select_record(cur, workspace_id, kind, key)
    except ForeignKeyViolation as e:
        if e.diag.constraint_name != _DRIVE_ENTRY_FK:
            raise
        raise ArtifactNotFound(key) from None
    if existing is None:
        raise RuntimeError("artifact record vanished between insert and read")
    return existing


async def find_record(
    workspace_id: str, kind: RecordKind, key: str
) -> Artifact | None:
    """The file's, chart's or Drive file's record if it has one; never
    makes it."""
    async with get_db_connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            return await _select_record(cur, workspace_id, kind, key)


async def list_chart_records(workspace_id: str, symbol: str) -> list[Artifact]:
    """The workspace's chart records for ``symbol`` (the one spelling
    ``normalize_chart_symbol`` gives), every timeframe, by chart id.

    A chart id is the symbol, a colon, then the timeframe, and a symbol holds
    no colon, so the prefix up to the colon matches that symbol alone. The
    chart index's leading ``workspace_id`` finds the rows; the prefix filters
    them, since a LIKE under the database's collation cannot range the path."""
    async with get_db_connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                f"""
                SELECT {ARTIFACT_COLUMNS} FROM share_links l
                 WHERE l.workspace_id = %s AND l.kind = 'chart'
                   AND l.path LIKE %s ESCAPE '\\'
                 ORDER BY l.path
                """,
                (workspace_id, f"{like_escape(symbol)}:%"),
            )
            return [artifact_from_row(row) for row in await cur.fetchall()]


async def get_artifact(artifact_id: str, *, user_id: str) -> Artifact | None:
    """The user's record by id, of any kind that holds comments, or None. A
    Drive file ended as a tombstone has none: a comment written on it would
    go with the sweep, where one on a trashed file returns with a restore."""
    async with get_db_connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                f"""
                SELECT {ARTIFACT_COLUMNS} FROM {OWNED} {DRIVE_JOIN}
                 WHERE l.artifact_id = %(artifact_id)s AND {COMMENT_RECORDS}
                   AND NOT {ENDED_SQL}
                """,
                {"artifact_id": artifact_id, "user_id": user_id},
            )
            return artifact_from_row(await cur.fetchone())


async def artifact_workspace_id(artifact_id: str) -> str | None:
    """The workspace of a live record, whoever owns it; the caller judges
    whether it may be reached."""
    async with get_db_connection() as conn:
        async with conn.cursor(row_factory=tuple_row) as cur:
            await cur.execute(
                f"""
                SELECT l.workspace_id::text FROM share_links l
                  JOIN workspaces w ON w.workspace_id = l.workspace_id
                 WHERE l.artifact_id = %s AND {COMMENT_RECORDS}
                   AND w.status <> 'deleted'
                """,
                (artifact_id,),
            )
            row = await cur.fetchone()
    return row[0] if row else None


async def record_creator(
    *, workspace_id: str, path: str, thread_id: str
) -> Artifact | None:
    """Name ``thread_id`` the file's creator unless one is named already.

    The first writer wins; a creator whose thread was deleted has been
    cleared, so the next writer takes its place. The thread must be in the
    file's workspace: None, and nothing written, when it is not and the file
    has no record.
    """
    async with get_db_connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                f"""
                INSERT INTO share_links AS l
                    (code, workspace_id, kind, path, created_thread_id)
                SELECT %s, t.workspace_id, 'file', %s, t.conversation_thread_id
                  FROM conversation_threads t
                 WHERE t.conversation_thread_id = %s AND t.workspace_id = %s
                ON CONFLICT (workspace_id, path) WHERE kind = 'file' DO UPDATE
                   SET created_thread_id = EXCLUDED.created_thread_id
                 WHERE l.created_thread_id IS NULL
                RETURNING {ARTIFACT_COLUMNS}
                """,
                (mint_share_code(), path, thread_id, workspace_id),
            )
            written = artifact_from_row(await cur.fetchone())
            if written is not None:
                return written
            return await _select_record(cur, workspace_id, "file", path)


async def lock_owned(
    cur, *, user_id: str, artifact_id: str, kinds: str = FILE_RECORDS
) -> Artifact:
    """The user's record of ``kinds`` under its row lock, or
    ``ArtifactNotFound``."""
    await cur.execute(
        f"""
        SELECT {ARTIFACT_COLUMNS} FROM {OWNED}
         WHERE l.artifact_id = %(artifact_id)s AND {kinds}
           FOR UPDATE OF l
        """,
        {"artifact_id": artifact_id, "user_id": user_id},
    )
    artifact = artifact_from_row(await cur.fetchone())
    if artifact is None:
        raise ArtifactNotFound(artifact_id)
    return artifact


async def set_record(
    *,
    user_id: str,
    artifact_id: str,
    assignments: str,
    params: dict[str, Any],
    kinds: str = FILE_RECORDS,
) -> Artifact:
    """Apply ``assignments`` to the user's record of ``kinds``, or
    ``ArtifactNotFound``."""
    async with get_db_connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                f"""
                UPDATE share_links l
                   SET {assignments}
                  FROM workspaces w
                 WHERE l.artifact_id = %(artifact_id)s AND {kinds}
                   AND w.workspace_id = l.workspace_id
                   AND w.user_id = %(user_id)s AND w.status <> 'deleted'
                RETURNING {ARTIFACT_COLUMNS}
                """,
                {"artifact_id": artifact_id, "user_id": user_id, **params},
            )
            artifact = artifact_from_row(await cur.fetchone())
    if artifact is None:
        raise ArtifactNotFound(artifact_id)
    return artifact


async def set_keep_updated(
    *, user_id: str, artifact_id: str, keep_updated: bool
) -> Artifact:
    return await set_record(
        user_id=user_id,
        artifact_id=artifact_id,
        assignments="keep_updated = %(keep_updated)s",
        params={"keep_updated": keep_updated},
    )


async def stop_keeping(*, user_id: str, artifact_id: str) -> Artifact:
    """Stop keeping the record for its owner; the versions cut so far stay.
    Keeping is ``versions.keep_version``, which cuts a version in the same
    write, so a record is never kept with no version to show."""
    return await set_record(
        user_id=user_id,
        artifact_id=artifact_id,
        assignments="kept_at = NULL",
        params={},
    )


async def release_unlisted(
    *,
    user_id: str,
    workspace_id: str,
    listed: Collection[str],
    grace: timedelta,
) -> list[Artifact]:
    """Stop keeping the workspace's kept records that ``listed`` (the owner's
    Dashboard list) does not name, once the keep and the owner's last write to
    their preferences are both older than ``grace``. A pin keeps the page and
    lists it a moment later, and an undo lists a removed page again without
    keeping it, so a list read inside either may be about to change. Both are
    checked in the statement, so a keep or a list written after the caller
    read it holds. The versions cut so far stay, as ``stop_keeping`` leaves
    them."""
    async with get_db_connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                f"""
                UPDATE share_links l
                   SET kept_at = NULL
                  FROM workspaces w
                 WHERE l.workspace_id = %(workspace_id)s AND {FILE_RECORDS}
                   AND w.workspace_id = l.workspace_id
                   AND w.user_id = %(user_id)s AND w.status <> 'deleted'
                   AND l.kept_at < NOW() - %(grace)s
                   AND NOT (l.artifact_id::text = ANY(%(listed)s::text[]))
                   AND EXISTS (
                       SELECT 1 FROM user_preferences p
                        WHERE p.user_id = %(user_id)s
                          AND p.updated_at < NOW() - %(grace)s
                   )
                RETURNING {ARTIFACT_COLUMNS}
                """,
                {
                    "workspace_id": workspace_id,
                    "user_id": user_id,
                    "listed": sorted(listed),
                    "grace": grace,
                },
            )
            return [artifact_from_row(row) for row in await cur.fetchall()]


async def set_left_out(
    *, user_id: str, artifact_id: str, path: str, left_out: bool, cap: int
) -> Artifact:
    """Take one file off the list or put it back, in one statement, so two boxes
    changed at once both hold. A list already at ``cap`` takes no new path, which
    the answer shows by not holding it; the check is in the statement, so two
    at once cannot both pass it. The next version is what carries the change; a
    publish whose read began before it is refused under the lock
    (``versions.LeftOutChanged``), since this moves no ``revision``."""
    return await set_record(
        user_id=user_id,
        artifact_id=artifact_id,
        assignments=(
            "left_out_files = CASE "
            "WHEN NOT %(left_out)s::boolean "
            "THEN array_remove(l.left_out_files, %(path)s::text) "
            "WHEN %(path)s::text = ANY(l.left_out_files) "
            "OR cardinality(l.left_out_files) >= %(cap)s THEN l.left_out_files "
            "ELSE array_append(l.left_out_files, %(path)s::text) END"
        ),
        params={"path": path, "left_out": left_out, "cap": cap},
    )


async def unshare(*, user_id: str, artifact_id: str) -> Artifact:
    """Stop sharing, keeping every version. Unconditional, as a revoke wins
    over whatever it races; the moved ``revision`` ends every visitor grant."""
    return await set_record(
        user_id=user_id,
        artifact_id=artifact_id,
        assignments=(
            "shared_at = NULL, files = NULL, shared_seq = 0, "
            "revision = l.revision + 1"
        ),
        params={},
    )


async def set_audience(
    *, user_id: str, artifact_id: str, audience: str | None
) -> Artifact:
    """Who the link opens to. Not a revision move: every visitor's request
    reads the audience off the row, so the change holds on the next one."""
    return await set_record(
        user_id=user_id,
        artifact_id=artifact_id,
        assignments="audience = %(audience)s",
        params={"audience": audience},
    )


async def list_kept_updated(workspace_id: str) -> list[Artifact]:
    """The workspace's records the end of a turn there refreshes
    (``Artifact.follows_turns``)."""
    async with get_db_connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                f"""
                SELECT {ARTIFACT_COLUMNS} FROM share_links l
                 WHERE l.workspace_id = %s AND {FILE_RECORDS}
                   AND {FOLLOWS_TURNS}
                 ORDER BY l.path
                """,
                (workspace_id,),
            )
            return [artifact_from_row(row) for row in await cur.fetchall()]
