"""Published versions: cut under the record's row lock, served by sequence."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Literal

from psycopg.rows import dict_row

from src.server.database.artifacts.records import (
    ARTIFACT_COLUMNS,
    Artifact,
    artifact_from_row,
    clip_title,
    lock_owned,
)
from src.server.database.pool import get_db_connection

#: Who cut a version: the owner's publish or Update, the end of a turn for a
#: shared artifact kept updated, or the first view of a link shared before
#: versions existed.
Origin = Literal["owner", "keep_updated", "legacy"]

#: Versions kept updated that an artifact keeps; a rebuilt report cuts one at
#: every turn's end, and each holds a full copy of its files.
KEEP_UPDATED_VERSIONS = 10


@dataclass(frozen=True)
class Version:
    seq: int
    entry_path: str
    origin: Origin
    #: The entry document's ``<title>`` when cut.
    title: str | None
    #: The owner's note on Publish or Update.
    label: str | None
    byte_len: int
    published_at: datetime


@dataclass(frozen=True)
class VersionFile:
    """One file of a version as publishing hands it over."""

    #: Workspace-relative.
    path: str
    sha256: str
    byte_len: int
    content_type: str
    #: The bytes, or None when the blob ``sha256`` names holds them, which
    #: must be stored and registered before the version is cut.
    content: bytes | None


@dataclass(frozen=True)
class StoredVersionFile:
    #: The blob's namespace.
    user_id: str
    sha256: str
    byte_len: int
    content_type: str
    content: bytes | None


class SharingChanged(Exception):
    """The record was shared or unshared while the owner's publish read its files."""


class VersionMoved(Exception):
    """A version was cut while the owner's publish read its files."""


@dataclass(frozen=True)
class PublishResult:
    artifact: Artifact
    #: The latest version after the call; 0 when there is none.
    seq: int
    #: Whether this call made a new version.
    cut: bool
    #: Whether this call shared a record that was not shared.
    shared_now: bool
    #: Whether anything another view shows moved: a version, the sharing, or
    #: the setting alone, which a publish of the same files can still change.
    changed: bool = False


async def _hashes(cur, artifact_id: str, seq: int) -> dict[str, str]:
    await cur.execute(
        """
        SELECT path, sha256 FROM artifact_version_files
         WHERE artifact_id = %s AND seq = %s
        """,
        (artifact_id, seq),
    )
    return {r["path"]: r["sha256"] for r in await cur.fetchall()}


async def publish_version(
    *,
    user_id: str,
    artifact_id: str,
    origin: Origin,
    files: list[VersionFile],
    title: str | None = None,
    label: str | None = None,
    keep_updated: bool | None = None,
    based_on: int | None = None,
    revision: int | None = None,
) -> PublishResult:
    """Cut the artifact's next version from ``files``, unless it would equal
    the latest one, and with ``origin="owner"`` share it.

    One transaction under the record's row lock. The entry is the record's
    own path, which ``files`` must hold. A version equal to the latest (the
    same paths and hashes) is not cut again, so re-sharing an unchanged file
    resumes at its latest version and an Update with no change is a no-op.

    ``revision`` is the record's when the owner's read began. Sharing or
    unsharing moves it, and that raises ``SharingChanged``: a stop that lands
    during the read must not be undone by the publish it overtook. ``based_on``
    is its latest version then, and a version cut since raises ``VersionMoved``:
    those bytes may be older, and would replace it.

    The other origins never change sharing, and each acts only while its
    reason holds when the lock is taken: ``keep_updated`` on a shared record
    kept updated whose latest version is still ``based_on``, the one it was
    when its read began, so a slow read never lands over a newer version;
    ``legacy`` on a shared record with no version yet, so of two workers
    snapshotting one link only the first cuts v1.

    A cut keeps the newest ``KEEP_UPDATED_VERSIONS`` versions kept updated and
    deletes older ones in the same transaction; the owner's and legacy
    versions and the latest always stay.
    """
    paths = [f.path for f in files]
    if len(set(paths)) != len(paths):
        raise ValueError("a version names each file once")
    async with get_db_connection() as conn:
        async with conn.transaction():
            async with conn.cursor(row_factory=dict_row) as cur:
                artifact = await lock_owned(
                    cur, user_id=user_id, artifact_id=artifact_id
                )
                if revision is not None and artifact.revision != revision:
                    raise SharingChanged
                if origin == "keep_updated":
                    proceed = (
                        artifact.shared
                        and artifact.keep_updated
                        and (based_on is None or artifact.latest_seq == based_on)
                    )
                elif origin == "legacy":
                    proceed = artifact.shared and artifact.latest_seq == 0
                else:
                    if based_on is not None and artifact.latest_seq != based_on:
                        raise VersionMoved
                    proceed = True
                if not proceed:
                    return PublishResult(artifact, artifact.latest_seq, False, False)
                if artifact.path not in paths:
                    raise ValueError("a version holds its entry file")

                latest = (
                    await _hashes(cur, artifact_id, artifact.latest_seq)
                    if artifact.latest_seq
                    else None
                )
                cut = latest != {f.path: f.sha256 for f in files}
                seq = artifact.latest_seq + 1 if cut else artifact.latest_seq
                if cut:
                    await cur.execute(
                        """
                        INSERT INTO artifact_versions
                            (artifact_id, seq, entry_path, origin, title, label,
                             byte_len)
                        VALUES (%s, %s, %s, %s, %s, %s, %s)
                        """,
                        (
                            artifact_id,
                            seq,
                            artifact.path,
                            origin,
                            clip_title(title),
                            label,
                            sum(f.byte_len for f in files),
                        ),
                    )
                    await cur.executemany(
                        """
                        INSERT INTO artifact_version_files
                            (artifact_id, seq, path, user_id, sha256, byte_len,
                             content_type, content)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                        """,
                        [
                            (
                                artifact_id,
                                seq,
                                f.path,
                                user_id,
                                f.sha256,
                                f.byte_len,
                                f.content_type,
                                f.content,
                            )
                            for f in files
                        ],
                    )
                    await _prune_kept_updated(cur, artifact_id=artifact_id, latest=seq)
                # Every origin that gets here leaves the record shared, so its
                # list becomes this version's.
                share = origin == "owner"
                shared_now = share and not artifact.shared
                # A link with no version was live until now, whichever build
                # shared it, so its first cut leaves it following the file
                # rather than frozen at the first view.
                setting = (
                    True if origin == "legacy" else keep_updated if share else None
                )
                await cur.execute(
                    f"""
                    UPDATE share_links l
                       SET latest_seq = %(seq)s,
                           files = %(files)s,
                           shared_at = CASE WHEN %(share)s
                                            THEN COALESCE(l.shared_at, NOW())
                                            ELSE l.shared_at END,
                           revision = l.revision
                                      + CASE WHEN %(shared_now)s THEN 1 ELSE 0 END,
                           keep_updated = COALESCE(%(keep_updated)s, l.keep_updated),
                           legacy_refused = NULL
                     WHERE l.artifact_id = %(artifact_id)s
                    RETURNING {ARTIFACT_COLUMNS}
                    """,
                    {
                        "seq": seq,
                        "share": share,
                        "shared_now": shared_now,
                        "files": sorted(paths),
                        "keep_updated": setting,
                        "artifact_id": artifact_id,
                    },
                )
                updated = artifact_from_row(await cur.fetchone())
    changed = cut or shared_now or setting not in (None, artifact.keep_updated)
    return PublishResult(updated, seq, cut, shared_now, changed)


async def _prune_kept_updated(cur, *, artifact_id: str, latest: int) -> None:
    """Delete the versions kept updated past the newest ones kept. Their
    files go with them (``ON DELETE CASCADE``), and a blob no other row names
    is the blob sweep's (``blob_keys.REFERENCED_SQL``)."""
    await cur.execute(
        """
        DELETE FROM artifact_versions v
         WHERE v.artifact_id = %(artifact_id)s
           AND v.origin = 'keep_updated'
           AND v.seq <> %(latest)s
           AND v.seq < (
               SELECT MIN(k.seq) FROM (
                   SELECT seq FROM artifact_versions
                    WHERE artifact_id = %(artifact_id)s AND origin = 'keep_updated'
                    ORDER BY seq DESC
                    LIMIT %(keep)s
               ) k
           )
        """,
        {"artifact_id": artifact_id, "latest": latest, "keep": KEEP_UPDATED_VERSIONS},
    )


async def refuse_legacy(*, artifact_id: str, code: str) -> Artifact | None:
    """Remember that a link shared before versions is past the caps, so its
    visitors stop asking for a first version. The record when this call set
    it; None when it was set already, or the link has a version or is not
    shared any more."""
    async with get_db_connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                f"""
                UPDATE share_links l
                   SET legacy_refused = %s
                 WHERE l.artifact_id = %s AND l.kind = 'file'
                   AND l.shared_at IS NOT NULL AND l.latest_seq = 0
                   AND l.legacy_refused IS NULL
                RETURNING {ARTIFACT_COLUMNS}
                """,
                (code, artifact_id),
            )
            return artifact_from_row(await cur.fetchone())


_VERSION_COLUMNS = """
    v.seq, v.entry_path, v.origin, v.title, v.label, v.byte_len, v.published_at
"""


async def list_versions(artifact_id: str) -> list[Version]:
    """Every version of an artifact, newest first."""
    async with get_db_connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                f"""
                SELECT {_VERSION_COLUMNS} FROM artifact_versions v
                 WHERE v.artifact_id = %s
                 ORDER BY v.seq DESC
                """,
                (artifact_id,),
            )
            return [Version(**row) for row in await cur.fetchall()]


async def get_version(artifact_id: str, seq: int) -> Version | None:
    async with get_db_connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                f"""
                SELECT {_VERSION_COLUMNS} FROM artifact_versions v
                 WHERE v.artifact_id = %s AND v.seq = %s
                """,
                (artifact_id, seq),
            )
            row = await cur.fetchone()
    return Version(**row) if row else None


async def get_version_hashes(artifact_id: str, seq: int) -> dict[str, str]:
    """A version's files by path, each its hash: what a change check compares."""
    async with get_db_connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            return await _hashes(cur, artifact_id, seq)


def _stored_file(row: dict[str, Any] | None) -> StoredVersionFile | None:
    if row is None:
        return None
    content = row["content"]
    return StoredVersionFile(
        user_id=row["user_id"],
        sha256=row["sha256"],
        byte_len=row["byte_len"],
        content_type=row["content_type"],
        content=bytes(content) if content is not None else None,
    )


async def get_version_file(
    artifact_id: str, seq: int, path: str
) -> StoredVersionFile | None:
    """A file of any version, for the owner's grant, which a deleted workspace
    ends however long it has left."""
    async with get_db_connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                """
                SELECT f.user_id, f.sha256, f.byte_len, f.content_type, f.content
                  FROM artifact_version_files f
                  JOIN share_links l ON l.artifact_id = f.artifact_id
                  JOIN workspaces w
                    ON w.workspace_id = l.workspace_id AND w.status <> 'deleted'
                 WHERE f.artifact_id = %s AND f.seq = %s AND f.path = %s
                """,
                (artifact_id, seq, path),
            )
            return _stored_file(await cur.fetchone())


async def get_shared_version_file(
    *, code: str, artifact_id: str, revision: int, seq: int, path: str
) -> StoredVersionFile | None:
    """A file of version ``seq``, if the link ``code`` still opens it to anyone.

    One statement, so the answer is one snapshot: the link is the artifact's,
    shared, at the revision the visitor's grant was minted at (an unshare
    moves it, and so does sharing again after one), its workspace is not
    deleted, and ``seq`` is still the latest. A new version therefore retires
    the one before it for every visitor at once. None for any of it, which the
    caller answers with the same 404 as a missing file.
    """
    async with get_db_connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                """
                SELECT f.user_id, f.sha256, f.byte_len, f.content_type, f.content
                  FROM share_links l
                  JOIN workspaces w
                    ON w.workspace_id = l.workspace_id AND w.status <> 'deleted'
                  JOIN artifact_version_files f
                    ON f.artifact_id = l.artifact_id AND f.seq = l.latest_seq
                 WHERE l.code = %(code)s
                   AND l.kind = 'file'
                   AND l.artifact_id = %(artifact_id)s
                   AND l.shared_at IS NOT NULL
                   AND l.revision = %(revision)s
                   AND l.latest_seq = %(seq)s
                   AND f.path = %(path)s
                """,
                {
                    "code": code,
                    "artifact_id": artifact_id,
                    "revision": revision,
                    "seq": seq,
                    "path": path,
                },
            )
            return _stored_file(await cur.fetchone())
