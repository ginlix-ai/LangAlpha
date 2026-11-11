"""Published versions: cut under the record's row lock, served by sequence."""

from __future__ import annotations

from collections.abc import AsyncIterator, Iterable, Sequence
from contextlib import asynccontextmanager
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
#: shared artifact kept updated or a kept one (and the keep itself), or the
#: first view of a link shared before versions existed.
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
    """The record was shared or unshared while its files were read."""


class VersionMoved(Exception):
    """A version was cut while the record's files were read."""


class LeftOutChanged(Exception):
    """The owner took a file off the list, or put one back, while the record's
    files were read."""


@dataclass(frozen=True)
class PublishResult:
    artifact: Artifact
    #: The latest version after the call; 0 when there is none. An owner's
    #: publish also shares it.
    seq: int
    #: Whether this call made a new version.
    cut: bool
    #: Whether this call shared a record that was not shared.
    shared_now: bool
    #: Whether anything another view shows moved: a version, the sharing, or
    #: the setting alone, which a publish of the same files can still change.
    changed: bool = False


#: What other views show of a record, so a write that moves none of it is
#: news to nobody.
_SHOWN = ("latest_seq", "shared_seq", "shared_at", "keep_updated", "kept")


@dataclass(frozen=True)
class ReadBasis:
    """What a read of the record's files was based on, checked under the row
    lock before its version lands. A share or unshare since (``revision``)
    changes what the cut may give visitors, and must not be undone by the
    write it overtook; a version cut since is newer than these bytes; a file
    left out or put back since was walked by the old choice. The left-out list
    is compared rather than moving ``revision``, since visitors' grants are
    minted at that."""

    revision: int
    latest_seq: int
    left_out: frozenset[str]

    @classmethod
    def of(cls, artifact: Artifact) -> ReadBasis:
        return cls(
            artifact.revision, artifact.latest_seq, frozenset(artifact.left_out_files)
        )

    def check(self, locked: Artifact) -> None:
        if locked.revision != self.revision:
            raise SharingChanged
        if frozenset(locked.left_out_files) != self.left_out:
            raise LeftOutChanged
        if locked.latest_seq != self.latest_seq:
            raise VersionMoved


def visitors_follow_turn(
    locked: Artifact, paths: Iterable[str], left_out: Iterable[str] = ()
) -> bool:
    """Whether a turn's cut of ``paths`` moves visitors to it.

    Only a shared link kept updated, and only to a version of the list someone
    reviewed (``files``): no path off it, so visitors never get one nobody
    reviewed, and none of it missing but what the owner left out (``left_out``
    is what the read left out for that choice, which takes a file's other
    paths with it). The same rule whether the read walked the page or read the
    list, so a file left out after the publish never strands visitors."""
    if not (locked.shared and locked.keep_updated):
        return False
    reviewed = set(locked.files or ())
    cut = set(paths)
    return cut <= reviewed and reviewed - cut <= {*locked.left_out_files, *left_out}


@asynccontextmanager
async def _locked(user_id: str, artifact_id: str) -> AsyncIterator[tuple[Any, Artifact]]:
    async with get_db_connection() as conn:
        async with conn.transaction():
            async with conn.cursor(row_factory=dict_row) as cur:
                yield cur, await lock_owned(
                    cur, user_id=user_id, artifact_id=artifact_id
                )


async def _hashes(cur, artifact_id: str, seq: int) -> dict[str, str]:
    await cur.execute(
        """
        SELECT path, sha256 FROM artifact_version_files
         WHERE artifact_id = %s AND seq = %s
        """,
        (artifact_id, seq),
    )
    return {r["path"]: r["sha256"] for r in await cur.fetchall()}


def _distinct(files: list[VersionFile]) -> None:
    paths = [f.path for f in files]
    if len(set(paths)) != len(paths):
        raise ValueError("a version names each file once")


async def publish_version(
    *,
    user_id: str,
    artifact_id: str,
    read: ReadBasis,
    files: list[VersionFile],
    title: str | None,
    label: str | None,
    keep_updated: bool | None,
    loaded_files: list[str],
    left_out_files: list[str],
) -> PublishResult:
    """The owner's Publish or Update: share the record at a version of
    ``files``, the list the owner reviewed. Visitors move even with no cut, as
    the latest may be a version kept for the owner alone. What the page loaded
    and what the owner left out are replaced in the same write."""
    _distinct(files)
    async with _locked(user_id, artifact_id) as (cur, locked):
        read.check(locked)
        return await _cut(
            cur,
            locked,
            files,
            user_id=user_id,
            origin="owner",
            title=title,
            label=label,
            to_visitors=True,
            share=True,
            also=(
                "keep_updated = COALESCE(%(keep_updated)s, l.keep_updated), "
                "loaded_files = %(loaded_files)s::text[], "
                "left_out_files = %(left_out_files)s::text[]"
            ),
            params={
                "keep_updated": keep_updated,
                "loaded_files": loaded_files,
                "left_out_files": left_out_files,
            },
        )


async def keep_version(
    *,
    user_id: str,
    artifact_id: str,
    read: ReadBasis,
    files: list[VersionFile],
    title: str | None,
    loaded_files: list[str],
) -> PublishResult:
    """The owner keeping a record: kept in the write that cuts its version, so
    a read that failed leaves it as it was, never kept with no version to
    show. Visitors stay where they are. What the page loaded is replaced, as
    a publish replaces it. Every keep restamps ``kept_at``, since a pin keeps
    the page before it lists it, and the release of a record kept off the
    list (``records.release_unlisted``) waits on the stamp."""
    _distinct(files)
    async with _locked(user_id, artifact_id) as (cur, locked):
        read.check(locked)
        return await _cut(
            cur,
            locked,
            files,
            user_id=user_id,
            origin="keep_updated",
            title=title,
            to_visitors=False,
            also=(
                "kept_at = NOW(), "
                "loaded_files = %(loaded_files)s::text[]"
            ),
            params={"loaded_files": loaded_files},
        )


async def turn_version(
    *,
    user_id: str,
    artifact_id: str,
    read: ReadBasis,
    files: list[VersionFile],
    title: str | None,
    left_out: Sequence[str] = (),
) -> PublishResult:
    """The cut at a turn's end of a record that still follows turns
    (``Artifact.follows_turns``); nothing when it stopped. Visitors move as
    ``visitors_follow_turn`` says, which ``left_out`` (what the read left out
    for the owner's choice) feeds."""
    _distinct(files)
    async with _locked(user_id, artifact_id) as (cur, locked):
        if not locked.follows_turns:
            return PublishResult(locked, locked.latest_seq, False, False)
        read.check(locked)
        return await _cut(
            cur,
            locked,
            files,
            user_id=user_id,
            origin="keep_updated",
            title=title,
            to_visitors=visitors_follow_turn(
                locked, (f.path for f in files), left_out
            ),
        )


async def legacy_version(
    *,
    user_id: str,
    artifact_id: str,
    read: ReadBasis,
    files: list[VersionFile],
    title: str | None,
) -> PublishResult:
    """The first shared version of a link shared before versions, from its
    frozen list; nothing once it has one, so of two workers snapshotting one
    link only the first cuts it. A link with no version was live until now,
    whichever build shared it, so it is left following the file rather than
    frozen at the first view."""
    _distinct(files)
    async with _locked(user_id, artifact_id) as (cur, locked):
        if not (locked.shared and locked.shared_seq == 0):
            return PublishResult(locked, locked.latest_seq, False, False)
        read.check(locked)
        return await _cut(
            cur,
            locked,
            files,
            user_id=user_id,
            origin="legacy",
            title=title,
            to_visitors=True,
            also="keep_updated = TRUE",
        )


async def _cut(
    cur,
    locked: Artifact,
    files: list[VersionFile],
    *,
    user_id: str,
    origin: Origin,
    title: str | None,
    to_visitors: bool,
    share: bool = False,
    label: str | None = None,
    also: str = "",
    params: dict[str, Any] | None = None,
) -> PublishResult:
    """Cut the next version from ``files`` unless it would equal the latest
    (the same paths and hashes), prune, and move the record's pointers, in the
    caller's transaction under its row lock.

    ``latest_seq`` moves on every cut; ``shared_seq`` and ``files``, what
    visitors see and may fetch, move together, only ``to_visitors``, which also
    clears a legacy refusal. ``share`` shares the record; ``also`` is the
    caller's own assignments, bound from ``params``."""
    artifact_id = locked.artifact_id
    paths = [f.path for f in files]
    if locked.path not in paths:
        raise ValueError("a version holds its entry file")
    latest = (
        await _hashes(cur, artifact_id, locked.latest_seq) if locked.latest_seq else None
    )
    cut = latest != {f.path: f.sha256 for f in files}
    seq = locked.latest_seq + 1 if cut else locked.latest_seq
    shared_seq = seq if to_visitors else locked.shared_seq
    if cut:
        await cur.execute(
            """
            INSERT INTO artifact_versions
                (artifact_id, seq, entry_path, origin, title, label, byte_len)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            """,
            (
                artifact_id,
                seq,
                locked.path,
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
        await _prune_kept_updated(
            cur, artifact_id=artifact_id, latest=seq, shared=shared_seq
        )
    sets = ["latest_seq = %(seq)s", "shared_seq = %(shared_seq)s"]
    if to_visitors:
        sets += ["files = %(files)s::text[]", "legacy_refused = NULL"]
    if share:
        # Old values on the right: the revision moves only when this shares.
        sets += [
            "shared_at = COALESCE(l.shared_at, NOW())",
            "revision = l.revision + CASE WHEN l.shared_at IS NULL THEN 1 ELSE 0 END",
        ]
    if also:
        sets.append(also)
    await cur.execute(
        f"""
        UPDATE share_links l
           SET {", ".join(sets)}
         WHERE l.artifact_id = %(artifact_id)s
        RETURNING {ARTIFACT_COLUMNS}
        """,
        {
            **(params or {}),
            "seq": seq,
            "shared_seq": shared_seq,
            "files": sorted(paths),
            "artifact_id": artifact_id,
        },
    )
    updated = artifact_from_row(await cur.fetchone())
    changed = any(getattr(locked, f) != getattr(updated, f) for f in _SHOWN)
    return PublishResult(updated, seq, cut, share and not locked.shared, changed)


async def _prune_kept_updated(
    cur, *, artifact_id: str, latest: int, shared: int
) -> None:
    """Delete the versions kept updated past the newest ones kept, but never
    the latest or the shared one. Their files go with them (``ON DELETE
    CASCADE``), and a blob no other row names is the blob sweep's
    (``blob_keys.REFERENCED_SQL``)."""
    await cur.execute(
        """
        DELETE FROM artifact_versions v
         WHERE v.artifact_id = %(artifact_id)s
           AND v.origin = 'keep_updated'
           AND v.seq NOT IN (%(latest)s, %(shared)s)
           AND v.seq < (
               SELECT MIN(k.seq) FROM (
                   SELECT seq FROM artifact_versions
                    WHERE artifact_id = %(artifact_id)s AND origin = 'keep_updated'
                    ORDER BY seq DESC
                    LIMIT %(keep)s
               ) k
           )
        """,
        {
            "artifact_id": artifact_id,
            "latest": latest,
            "shared": shared,
            "keep": KEEP_UPDATED_VERSIONS,
        },
    )


async def refuse_legacy(*, artifact_id: str, code: str) -> Artifact | None:
    """Remember that a link shared before versions is past the caps, so its
    visitors stop asking for a first version. The record when this call set
    it; None when it was set already, or the link has a shared version or is
    not shared any more."""
    async with get_db_connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                f"""
                UPDATE share_links l
                   SET legacy_refused = %s
                 WHERE l.artifact_id = %s AND l.kind = 'file'
                   AND l.shared_at IS NOT NULL AND l.shared_seq = 0
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
    deleted, and ``seq`` is still the shared one (``shared_seq``, never a
    newer version kept for the owner alone). A new shared version therefore
    retires the one before it for every visitor at once. None for any of it,
    which the caller answers with the same 404 as a missing file.
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
                    ON f.artifact_id = l.artifact_id AND f.seq = l.shared_seq
                 WHERE l.code = %(code)s
                   AND l.kind = 'file'
                   AND l.artifact_id = %(artifact_id)s
                   AND l.shared_at IS NOT NULL
                   AND l.revision = %(revision)s
                   AND l.shared_seq = %(seq)s
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
