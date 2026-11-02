"""The file's record: its ``share_links`` row, made on first need."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any

from psycopg.rows import dict_row

from src.server.database.pool import get_db_connection
from src.server.database.share_codes import mint_share_code

# What the title CHECKs admit; a title is read off the file, so it is clipped
# here rather than refused.
_TITLE_CHARS = 300


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
    #: Workspace-relative.
    path: str
    created_thread_id: str | None
    bound_thread_id: str | None
    keep_updated: bool
    #: 0 until the first publish.
    latest_seq: int
    shared_at: datetime | None
    #: The paths a visitor may fetch: the latest version's, or a list frozen
    #: by a share from before versions, until its first view snapshots it.
    files: tuple[str, ...] | None
    #: Moves on every share and every unshare, never on a new version.
    revision: int
    created_at: datetime
    #: Why a link shared before versions has none: its files are past the
    #: caps (``too_many_files``, ``too_many_bytes``). Visitors get a 404 and
    #: nothing tries again until the owner publishes.
    legacy_refused: str | None = None

    @property
    def shared(self) -> bool:
        return self.shared_at is not None

    @property
    def name(self) -> str:
        return self.path.rsplit("/", 1)[-1]


#: A record's columns off ``share_links l``; ``database/share_links`` reads a
#: link's with them, so a file's link carries its record.
ARTIFACT_COLUMNS = """
    l.artifact_id::text, l.code, l.workspace_id::text, l.path,
    l.created_thread_id::text, l.bound_thread_id::text, l.keep_updated,
    l.latest_seq, l.shared_at, l.files, l.revision, l.created_at,
    l.legacy_refused
"""

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
    )


async def _select_by_path(cur, workspace_id: str, path: str) -> Artifact | None:
    await cur.execute(
        f"""
        SELECT {ARTIFACT_COLUMNS} FROM share_links l
         WHERE l.workspace_id = %s AND l.kind = 'file' AND l.path = %s
        """,
        (workspace_id, path),
    )
    return artifact_from_row(await cur.fetchone())


async def ensure_artifact(workspace_id: str, path: str) -> Artifact:
    """The file's record, made private on first sight.

    The caller has checked the workspace is the user's and ``path`` is a
    contained workspace path that may be shared. Two writers for one file
    land on one row and one code.
    """
    async with get_db_connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                f"""
                INSERT INTO share_links AS l (code, workspace_id, kind, path)
                VALUES (%s, %s, 'file', %s)
                ON CONFLICT (workspace_id, path) WHERE kind = 'file' DO NOTHING
                RETURNING {ARTIFACT_COLUMNS}
                """,
                (mint_share_code(), workspace_id, path),
            )
            created = artifact_from_row(await cur.fetchone())
            if created is not None:
                return created
            existing = await _select_by_path(cur, workspace_id, path)
    if existing is None:
        raise RuntimeError("artifact record vanished between insert and read")
    return existing


async def find_artifact(workspace_id: str, path: str) -> Artifact | None:
    """The file's record if it has one; never makes it."""
    async with get_db_connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            return await _select_by_path(cur, workspace_id, path)


async def get_artifact(artifact_id: str, *, user_id: str) -> Artifact | None:
    """The user's record by id, or None."""
    async with get_db_connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                f"""
                SELECT {ARTIFACT_COLUMNS} FROM {OWNED}
                 WHERE l.artifact_id = %(artifact_id)s AND l.kind = 'file'
                """,
                {"artifact_id": artifact_id, "user_id": user_id},
            )
            return artifact_from_row(await cur.fetchone())


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
            return await _select_by_path(cur, workspace_id, path)


async def lock_owned(cur, *, user_id: str, artifact_id: str) -> Artifact:
    """The user's record under its row lock, or ``ArtifactNotFound``."""
    await cur.execute(
        f"""
        SELECT {ARTIFACT_COLUMNS} FROM {OWNED}
         WHERE l.artifact_id = %(artifact_id)s AND l.kind = 'file'
           FOR UPDATE OF l
        """,
        {"artifact_id": artifact_id, "user_id": user_id},
    )
    artifact = artifact_from_row(await cur.fetchone())
    if artifact is None:
        raise ArtifactNotFound(artifact_id)
    return artifact


async def set_record(
    *, user_id: str, artifact_id: str, assignments: str, params: dict[str, Any]
) -> Artifact:
    """Apply ``assignments`` to the user's record, or ``ArtifactNotFound``."""
    async with get_db_connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                f"""
                UPDATE share_links l
                   SET {assignments}
                  FROM workspaces w
                 WHERE l.artifact_id = %(artifact_id)s AND l.kind = 'file'
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


async def unshare(*, user_id: str, artifact_id: str) -> Artifact:
    """Stop sharing, keeping every version. Unconditional, as a revoke wins
    over whatever it races; the moved ``revision`` ends every visitor grant."""
    return await set_record(
        user_id=user_id,
        artifact_id=artifact_id,
        assignments="shared_at = NULL, files = NULL, revision = l.revision + 1",
        params={},
    )


async def list_kept_updated(workspace_id: str) -> list[Artifact]:
    """The workspace's shared records kept updated, which the end of a turn
    there refreshes. A link shared before versions waits for its first view,
    which cuts v1 under the caps its share was made with."""
    async with get_db_connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                f"""
                SELECT {ARTIFACT_COLUMNS} FROM share_links l
                 WHERE l.workspace_id = %s AND l.kind = 'file'
                   AND l.keep_updated AND l.shared_at IS NOT NULL
                   AND l.latest_seq > 0
                 ORDER BY l.path
                """,
                (workspace_id,),
            )
            return [artifact_from_row(row) for row in await cur.fetchall()]
