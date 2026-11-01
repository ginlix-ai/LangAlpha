"""Reading an artifact's files into a version, and the checks around it.

A version holds the file and the files it reads, found by the share
manifest's walk, each under its workspace-relative path, so a link like
``../../data/x.json`` resolves inside the copy. Files are read the way the
owner's file routes read them: live from a running computer, else the stored
copy. Only the owner's own request may acquire a computer; a visitor's view
and the end of a turn read a session this worker already holds or nothing,
so none of them starts a stopped computer.

Text is redacted of the owner's secrets before it is hashed, the bytes the
file routes serve, so a live hash and a version's agree when nothing changed
and a version never holds a secret the owner's view would not show.
"""

from __future__ import annotations

import asyncio
import hashlib
import html
import logging
import re
from dataclasses import dataclass, field
from typing import Any

from src.server.services.share_manifest import (
    MAX_FILES,
    MAX_TOTAL_BYTES,
    REASON_ENTRY,
    ManifestEntry,
    ManifestEntryMissing,
    ManifestTooLarge,
    NoLiveSession,
    Reader,
    ReadMode,
    snapshot_reader,
    walk_manifest,
)
from src.server.database import artifacts as artifacts_db
from src.server.database.blob_keys import BlobError
from src.server.database.artifacts import Artifact, VersionFile, clip_title
from src.server.database.workspace import get_workspace as db_get_workspace
from src.server.database.workspace_file_blobs import store_unregistered_blobs
from src.server.services.artifacts.events import publish_artifact_change
from src.server.services.served_paths import (
    is_text_content_type,
    is_utf8,
    may_be_artifact,
)
from src.server.services.transcripts import INLINE_FILE_MAX_BYTES
from src.server.services.workspace_file_containment import EXEC_READ_MAX_BYTES
from src.server.utils.secret_redactor import (
    get_redactor,
    get_vault_secrets_for_redaction,
)
from src.utils.mime import resolve_content_type
from src.utils.storage import is_storage_enabled

logger = logging.getLogger(__name__)

#: A version's caps. The share list may run larger, but a version is a copy
#: kept in Postgres or object storage for as long as the record lives.
MAX_VERSION_FILES = MAX_FILES
MAX_VERSION_BYTES = 25 * 1024 * 1024
# Reads side by side, short of the six an SSH connection runs at once, so a
# turn's own commands still get a slot.
_READ_CONCURRENCY = 4
_TITLE_OPEN = re.compile(rb"<title\b", re.IGNORECASE)
_TITLE_CLOSE = re.compile(rb"</title\s*>", re.IGNORECASE)
# A <title> belongs in the head, so a long body is not searched for one.
_TITLE_SCAN_BYTES = 256 * 1024


class SnapshotUnavailable(Exception):
    """The files cannot be read now."""


class LegacyRefused(Exception):
    """A link shared before versions whose files are past the caps: it gets
    no version until the owner publishes."""


class SnapshotRefused(Exception):
    """A snapshot that cannot be made as asked, under a code the routes answer."""

    def __init__(
        self,
        code: str,
        *,
        limit: int | None = None,
        files: list[ManifestEntry] | None = None,
    ) -> None:
        super().__init__(code)
        self.code = code
        self.limit = limit
        self.files = files


@dataclass(frozen=True)
class Snapshot:
    #: The files as the walk found them, the entry first.
    entries: list[ManifestEntry]
    #: Each file as its version row would hold it, in the same order.
    files: list[VersionFile]
    #: Bytes bound for object storage, by hash.
    blobs: dict[str, bytes]
    title: str | None

    @property
    def total(self) -> int:
        return sum(f.byte_len for f in self.files)

    @property
    def entry_sha256(self) -> str:
        return self.files[0].sha256


@dataclass(frozen=True)
class Drift:
    """The live files against the latest version's, by path."""

    added: list[str] = field(default_factory=list)
    removed: list[str] = field(default_factory=list)
    changed: list[str] = field(default_factory=list)

    def __bool__(self) -> bool:
        return bool(self.added or self.removed or self.changed)


@dataclass(frozen=True)
class Status:
    snapshot: Snapshot
    latest_seq: int
    #: None before the first version.
    changed: bool | None
    #: None unless ``changed``.
    drift: Drift | None


async def _workspace(artifact: Artifact) -> dict[str, Any]:
    workspace = await db_get_workspace(artifact.workspace_id)
    if not workspace or workspace.get("status") == "deleted":
        raise SnapshotRefused("file_missing")
    return workspace


@dataclass(frozen=True)
class _Caps:
    files: int
    bytes: int

    def check(self, count: int, total: int) -> None:
        if count > self.files:
            raise SnapshotRefused("too_many_files", limit=self.files)
        if total > self.bytes:
            raise SnapshotRefused("too_many_bytes", limit=self.bytes)


_VERSION_CAPS = _Caps(MAX_VERSION_FILES, MAX_VERSION_BYTES)
# A link shared before versions was shared under the share list's caps, so
# its one snapshot keeps them rather than breaking a link that worked.
_LEGACY_CAPS = _Caps(MAX_FILES, MAX_TOTAL_BYTES)


@dataclass(frozen=True)
class _Walk:
    """The manifest's walk from the entry. With ``expected``, its list must
    equal that before any bytes are read, so a publish never carries a file
    the owner has not reviewed."""

    expected: list[str] | None = None


@dataclass(frozen=True)
class _Listed:
    """Exactly ``paths``: one that is gone is left out, the entry never."""

    paths: tuple[str, ...]
    caps: _Caps = _VERSION_CAPS


async def _capture(
    artifact: Artifact,
    *,
    user_id: str,
    mode: ReadMode,
    source: _Walk | _Listed = _Walk(),
) -> Snapshot:
    """Read the artifact's files as ``source`` names them, from where
    ``mode`` allows."""
    caps = source.caps if isinstance(source, _Listed) else _VERSION_CAPS
    workspace = await _workspace(artifact)
    try:
        async with snapshot_reader(workspace, user_id, mode) as reader:
            if isinstance(source, _Listed):
                entries = await _listed(reader, artifact, source.paths)
            else:
                entries = await walk_manifest(reader, artifact.path)
                if source.expected is not None and {e.path for e in entries} != set(
                    source.expected
                ):
                    raise SnapshotRefused("files_changed", files=entries)
            caps.check(len(entries), sum(e.size for e in entries))
            contents = await _read_all(reader, entries)
    except ManifestEntryMissing:
        raise SnapshotRefused("file_missing") from None
    except ManifestTooLarge as e:
        raise SnapshotRefused(e.code, limit=e.limit) from None
    except NoLiveSession:
        # Every turn's end on a computer this worker does not hold: expected.
        raise SnapshotUnavailable("no live session to read") from None
    except (SnapshotRefused, SnapshotUnavailable):
        raise
    except Exception as e:
        # The computer's or the store's failure: a 503, whatever it was.
        logger.warning(
            "Reading artifact %s failed: %s", artifact.artifact_id, e, exc_info=True
        )
        raise SnapshotUnavailable(str(e)) from e

    vault_secrets = await get_vault_secrets_for_redaction(user_id)
    files, blobs = await asyncio.to_thread(
        _seal,
        [(e.path, data) for e, data in zip(entries, contents)],
        get_redactor(),
        vault_secrets,
        not is_storage_enabled(),
    )
    caps.check(len(files), sum(f.byte_len for f in files))
    entry = files[0]
    title = _html_title(
        entry.content if entry.content is not None else blobs[entry.sha256]
    )
    return Snapshot(entries=entries, files=files, blobs=blobs, title=title)


async def _listed(
    reader: Reader, artifact: Artifact, paths: tuple[str, ...]
) -> list[ManifestEntry]:
    wanted = [p for p in dict.fromkeys([artifact.path, *paths]) if may_be_artifact(p)]
    # A path that resolves to a file that may not leave is as missing as one
    # that is not there: the read would refuse it.
    found = {
        p: hit
        for p, hit in (await reader.files.stat(wanted)).items()
        if may_be_artifact(hit[0])
    }
    if artifact.path not in found:
        raise ManifestEntryMissing(artifact.path)
    return [
        ManifestEntry(p, found[p][1], REASON_ENTRY if p == artifact.path else "listed")
        for p in wanted
        if p in found
    ]


async def _read_all(reader: Reader, entries: list[ManifestEntry]) -> list[bytes]:
    gate = asyncio.Semaphore(_READ_CONCURRENCY)

    async def read(entry: ManifestEntry) -> bytes:
        async with gate:
            data = await reader.files.read(entry.path)
        if data is None:
            if entry.size > EXEC_READ_MAX_BYTES:
                # Past what one read takes, and it will be next time too.
                raise SnapshotRefused("too_many_bytes", limit=EXEC_READ_MAX_BYTES)
            # Listed a moment ago: gone.
            raise SnapshotUnavailable(f"{entry.path} could not be read")
        return data

    try:
        async with asyncio.TaskGroup() as group:
            tasks = [group.create_task(read(e)) for e in entries]
    except* Exception as failed:
        raise failed.exceptions[0] from None
    return [task.result() for task in tasks]


def _seal(
    files: list[tuple[str, bytes]],
    redactor: Any,
    vault_secrets: dict[str, str],
    inline_all: bool,
) -> tuple[list[VersionFile], dict[str, bytes]]:
    """Each file as its row will hold it, and the bytes that go to blobs.

    Off the event loop: redacting and hashing 25 MiB is CPU work.
    """
    sealed: list[VersionFile] = []
    blobs: dict[str, bytes] = {}
    for path, data in files:
        content_type = resolve_content_type(path)
        # The file routes' test for what they redact: a text type, or any
        # bytes that read as UTF-8, since an extension does not make a file
        # binary.
        if is_text_content_type(content_type) or is_utf8(data):
            data = redactor.redact_bytes(data, vault_secrets=vault_secrets)
        sha256 = hashlib.sha256(data).hexdigest()
        inline = inline_all or len(data) <= INLINE_FILE_MAX_BYTES
        if not inline:
            blobs[sha256] = data
        sealed.append(
            VersionFile(
                path=path,
                sha256=sha256,
                byte_len=len(data),
                content_type=content_type,
                content=data if inline else None,
            )
        )
    return sealed, blobs


def _html_title(data: bytes) -> str | None:
    """The first ``<title>``'s text. A scan rather than one pattern: on the
    event loop, a lazy match tried at each of a run of unclosed tags is seconds."""
    head = data[:_TITLE_SCAN_BYTES]
    opened = _TITLE_OPEN.search(head)
    if opened is None:
        return None
    start = head.find(b">", opened.end()) + 1
    closed = _TITLE_CLOSE.search(head, start) if start else None
    if closed is None:
        return None
    text = head[start : closed.start()].decode("utf-8", errors="replace")
    return clip_title(html.unescape(text))


async def _cut(
    artifact: Artifact,
    snapshot: Snapshot,
    *,
    user_id: str,
    origin: artifacts_db.Origin,
    label: str | None = None,
    keep_updated: bool | None = None,
    based_on: int | None = None,
    revision: int | None = None,
) -> artifacts_db.PublishResult:
    # The blobs are claimed and registered before the rows that name them, as
    # transcripts store theirs, so the sweep's grace covers the gap.
    try:
        await store_unregistered_blobs(user_id, snapshot.blobs)
    except BlobError as e:
        raise SnapshotUnavailable(str(e)) from e
    result = await artifacts_db.publish_version(
        user_id=user_id,
        artifact_id=artifact.artifact_id,
        origin=origin,
        files=snapshot.files,
        title=snapshot.title,
        label=label,
        keep_updated=keep_updated,
        based_on=based_on,
        revision=revision,
    )
    if result.changed:
        logger.info(
            "Artifact %s at v%d (%s, %d files, %d bytes, cut=%s)",
            artifact.artifact_id,
            result.seq,
            origin,
            len(snapshot.files),
            snapshot.total,
            result.cut,
        )
        await publish_artifact_change(user_id, result.artifact)
    return result


def _drift(snapshot: Snapshot, before: dict[str, str]) -> Drift:
    now = {f.path: f.sha256 for f in snapshot.files}
    return Drift(
        added=sorted(now.keys() - before.keys()),
        removed=sorted(before.keys() - now.keys()),
        changed=sorted(p for p in now.keys() & before.keys() if now[p] != before[p]),
    )


async def status(artifact: Artifact, *, user_id: str) -> Status:
    """The live file against the latest version, and what a publish now
    would carry. Raises ``SnapshotRefused`` or ``SnapshotUnavailable``."""
    snapshot = await _capture(artifact, user_id=user_id, mode=ReadMode.OWNER)
    if not artifact.latest_seq:
        return Status(snapshot, 0, None, None)
    latest = await artifacts_db.get_version_hashes(
        artifact.artifact_id, artifact.latest_seq
    )
    drift = _drift(snapshot, latest)
    return Status(snapshot, artifact.latest_seq, bool(drift), drift or None)


async def publish(
    artifact: Artifact,
    *,
    user_id: str,
    reviewed: list[str],
    label: str | None,
    keep_updated: bool | None,
) -> artifacts_db.PublishResult:
    """The owner's Publish or Update: share, and cut a version from the files
    the owner reviewed. ``SnapshotRefused('files_changed')`` carries the list
    as it is now when it differs from ``reviewed``."""
    snapshot = await _capture(
        artifact, user_id=user_id, mode=ReadMode.OWNER, source=_Walk(reviewed)
    )
    return await _cut(
        artifact,
        snapshot,
        user_id=user_id,
        origin="owner",
        label=label,
        keep_updated=keep_updated,
        based_on=artifact.latest_seq,
        revision=artifact.revision,
    )


# Every link shared before versions comes here on its first anonymous view, and
# each read holds up to the old cap in memory: a popular link's visitors share
# one read, and a worker runs two at once, beside the turns it is running.
_LEGACY_READS = asyncio.Semaphore(2)
_legacy_cuts: dict[str, asyncio.Future[Artifact]] = {}


async def legacy_snapshot(artifact: Artifact) -> Artifact:
    """Cut v1 of a link shared before versions, from its frozen list, reading
    a warm session or the stored copy, never waking a computer. Returns the
    record as it stands after, which another worker may have snapshotted
    first. ``SnapshotUnavailable`` when the files cannot be read now.

    Files past the caps the link was shared under raise ``LegacyRefused``,
    once recorded on the link (``legacy_refused``) so no visitor asks again; an
    entry that is gone raises it unrecorded, since the file may come back,
    and the owner's views are told once.
    """
    if artifact.legacy_refused:
        raise LegacyRefused(artifact.legacy_refused)
    artifact_id = artifact.artifact_id
    cut = _legacy_cuts.get(artifact_id)
    if cut is None:

        def done(finished: asyncio.Future[Artifact]) -> None:
            _legacy_cuts.pop(artifact_id, None)
            # Every visitor may have left by now.
            if not finished.cancelled():
                finished.exception()

        cut = asyncio.ensure_future(_legacy_cut(artifact))
        _legacy_cuts[artifact_id] = cut
        cut.add_done_callback(done)
    # A visitor who leaves does not cancel the read the others wait on.
    return await asyncio.shield(cut)


async def _legacy_cut(artifact: Artifact) -> Artifact:
    async with _LEGACY_READS:
        return await _read_legacy(artifact)


async def _read_legacy(artifact: Artifact) -> Artifact:
    workspace = await db_get_workspace(artifact.workspace_id)
    if not workspace:
        raise SnapshotUnavailable("workspace gone")
    user_id = workspace["user_id"]
    try:
        snapshot = await _capture(
            artifact,
            user_id=user_id,
            mode=ReadMode.WARM_OR_STORED,
            source=_Listed(artifact.files or (), caps=_LEGACY_CAPS),
        )
        result = await _cut(artifact, snapshot, user_id=user_id, origin="legacy")
    except SnapshotRefused as e:
        if e.code == "file_missing":
            # Not remembered: the file may come back, and asking is cheap.
            raise LegacyRefused(e.code) from None
        if e.code not in ("too_many_files", "too_many_bytes"):
            raise SnapshotUnavailable(e.code) from None
        refused = await artifacts_db.refuse_legacy(
            artifact_id=artifact.artifact_id, code=e.code
        )
        if refused is not None:
            logger.info(
                "Shared link %s gets no first version: %s", artifact.code, e.code
            )
            await publish_artifact_change(user_id, refused)
        raise LegacyRefused(e.code) from None
    return result.artifact


async def keep_updated_after_turn(*, workspace_id: str, user_id: str) -> None:
    """Cut a version of each shared artifact kept updated in the workspace
    whose files changed. Never raises.

    Reads only a session this worker holds: a stored copy can lag the
    computer, and a version cut from it could undo the one before. No warm
    session means no check this turn; the next turn's end looks again.
    """
    try:
        kept = await artifacts_db.list_kept_updated(workspace_id)
        if not kept:
            return
        workspace = await db_get_workspace(workspace_id)
        if not workspace or workspace.get("status") != "running":
            return
        for artifact in kept:
            await _keep_one(artifact, user_id)
    except Exception:
        logger.warning(
            "Keeping artifacts updated in workspace %s failed",
            workspace_id,
            exc_info=True,
        )


async def _keep_one(artifact: Artifact, user_id: str) -> None:
    try:
        snapshot = await _capture(
            artifact,
            user_id=user_id,
            mode=ReadMode.WARM_ONLY,
            source=_Listed(artifact.files or ()),
        )
    except (SnapshotRefused, SnapshotUnavailable) as e:
        logger.info("Not refreshing artifact %s: %s", artifact.artifact_id, e)
        return
    # A listed file gone, maybe only while a rebuild runs: a version without it
    # would stay without it, since the list never widens on its own. The link
    # keeps its last version until the file is back or the owner's Update.
    if set(artifact.files or ()) - {f.path for f in snapshot.files}:
        logger.info(
            "Not refreshing artifact %s: a listed file is gone", artifact.artifact_id
        )
        return
    # Compared against the version the record named before the read: one cut
    # meanwhile (an Update, another turn's end) is newer than these bytes.
    await _cut(
        artifact,
        snapshot,
        user_id=user_id,
        origin="keep_updated",
        based_on=artifact.latest_seq,
    )
