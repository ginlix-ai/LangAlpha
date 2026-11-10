"""Reading an artifact's files into a version, and the checks around it.

A version holds the file and the files it reads, found by the manifest's
walk, each under its workspace-relative path, so a link like
``../../data/x.json`` resolves inside the copy. Files are read the way the
owner's file routes read them: live from a running computer, else the stored
copy, which keeps no library folder, so a page loading from one needs its
computer. Only the owner's own request may acquire a computer; a visitor's view
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
from datetime import UTC, datetime, timedelta
from typing import Any

from src.server.services.artifacts.manifest import (
    MAX_BYTES,
    MAX_FILES,
    MAX_LISTED_BYTES,
    REASON_ENTRY,
    REASON_LOADED,
    Manifest,
    ManifestEntry,
    ManifestEntryMissing,
    ManifestTooLarge,
    walk_manifest,
)
from src.server.services.artifacts import dashboards
from src.server.services.artifacts.references import REASON_LINK
from src.server.services.artifacts.reader import (
    NoLiveSession,
    NotInStoredCopy,
    Reader,
    ReadMode,
    snapshot_reader,
)
from src.server.database import artifacts as artifacts_db
from src.server.database.blob_keys import BlobError
from src.server.database.artifacts import Artifact, VersionFile, clip_title
from src.server.database.artifacts.versions import (
    LeftOutChanged,
    ReadBasis,
    SharingChanged,
    VersionMoved,
)
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

#: A version's caps, which the walk stops at. A version is a copy kept in
#: Postgres or object storage for as long as the record lives.
MAX_VERSION_FILES = MAX_FILES
MAX_VERSION_BYTES = MAX_BYTES
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
        left_out: list[ManifestEntry] | None = None,
    ) -> None:
        super().__init__(code)
        self.code = code
        self.limit = limit
        self.files = files
        self.left_out = left_out


@dataclass(frozen=True)
class Snapshot:
    #: The files as the walk found them, the entry first.
    entries: list[ManifestEntry]
    #: What the owner left out and the page still names; never read or carried.
    left_out: list[ManifestEntry]
    #: Each file as its version row would hold it, in the same order.
    files: list[VersionFile]
    #: Bytes bound for object storage, by hash.
    blobs: dict[str, bytes]
    title: str | None
    #: What the files load from the origin's root, which no view serves
    #: (``Manifest.root_refs``).
    root_refs: list[str] = field(default_factory=list)
    #: What the files name that is not there (``Manifest.absent``).
    absent: frozenset[str] = frozenset()

    @property
    def total(self) -> int:
        return sum(f.byte_len for f in self.files)

    @property
    def entry_sha256(self) -> str:
        return self.files[0].sha256

    @property
    def page_sha256(self) -> str:
        """The page's code and data as this read found it: every file on the
        list, whatever named it, so a module another script imports counts too,
        and so does data a script may build a path from, of any type or none. A
        change here can change what the page loads as it runs, so the review
        loads the page again. A link to another page stays out, and so does
        media a page draws without reading. Membership has to hold still while the scout reloads: a guess is
        listed from the source whatever the report says, and a reported load
        stays listed once its document closes, so neither leaves and rejoins."""
        digest = hashlib.sha256()
        for entry, sealed in zip(self.entries, self.files):
            if not _drives_loads(entry.path) or (
                entry.optional and entry.reason == REASON_LINK
            ):
                continue
            digest.update(f"{entry.path}\0{sealed.sha256}\n".encode())
        return digest.hexdigest()


# What a page shows or plays and no script reads for a path. SVG is not here:
# it is markup, and framed or embedded it loads files of its own.
_MEDIA_SUFFIXES = (
    ".png", ".jpg", ".jpeg", ".gif", ".webp", ".avif", ".bmp", ".ico", ".tif", ".tiff",
    ".woff", ".woff2", ".ttf", ".otf", ".eot",
    ".mp3", ".wav", ".ogg", ".oga", ".m4a", ".flac", ".aac", ".opus",
    ".mp4", ".webm", ".mov", ".m4v", ".ogv",
)  # fmt: skip


def _drives_loads(path: str) -> bool:
    return not path.lower().endswith(_MEDIA_SUFFIXES)


@dataclass(frozen=True)
class Drift:
    """The live files against the latest version's, by path."""

    added: list[str] = field(default_factory=list)
    removed: list[str] = field(default_factory=list)
    changed: list[str] = field(default_factory=list)

    def __bool__(self) -> bool:
        return bool(self.added or self.removed or self.changed)


@dataclass(frozen=True)
class OverCap:
    """A review past a cap: what the walk listed before it stopped, the file
    that crossed the cap last, for the owner to leave out what need not go.
    Nothing was read or sealed, so there is no snapshot to publish."""

    code: str
    limit: int
    entries: list[ManifestEntry]
    left_out: list[ManifestEntry]


_CAP_CODES = frozenset({"too_many_files", "too_many_bytes"})


@dataclass(frozen=True)
class Status:
    #: None only ``over_cap``.
    snapshot: Snapshot | None
    latest_seq: int
    #: Against the shared version when shared, else the latest; None before
    #: there is one.
    changed: bool | None
    #: None unless ``changed``.
    drift: Drift | None
    over_cap: OverCap | None = None


async def _workspace(artifact: Artifact) -> dict[str, Any]:
    workspace = await db_get_workspace(artifact.workspace_id)
    if not workspace or workspace.get("status") == "deleted":
        raise SnapshotRefused("file_missing")
    return workspace


@dataclass(frozen=True)
class _Caps:
    files: int
    #: The files' bytes, a file reached by several paths counted once.
    bytes: int
    #: Every path's bytes, as they are read and stored (``MAX_LISTED_BYTES``).
    listed: int

    def check(self, count: int, unique: int, listed: int, manifest: Manifest) -> None:
        """Refuses past a cap with the manifest's list, which a review offers."""
        listing = {"files": manifest.entries, "left_out": manifest.left_out}
        if count > self.files:
            raise SnapshotRefused("too_many_files", limit=self.files, **listing)
        # The limit named is the one crossed, for the owner to get under.
        if unique > self.bytes:
            raise SnapshotRefused("too_many_bytes", limit=self.bytes, **listing)
        if listed > self.listed:
            raise SnapshotRefused("too_many_bytes", limit=self.listed, **listing)


_VERSION_CAPS = _Caps(MAX_VERSION_FILES, MAX_VERSION_BYTES, MAX_LISTED_BYTES)
# A link shared before versions was shared under these, so its one snapshot
# keeps them rather than breaking a link that worked.
_LEGACY_CAPS = _Caps(MAX_FILES, 256 * 1024 * 1024, 256 * 1024 * 1024)


@dataclass(frozen=True)
class _Walked:
    """The manifest's walk from the entry. With ``expected``, its list must
    equal that before any bytes are read, so a publish never carries a file
    the owner has not reviewed. ``loaded`` is what the live page reported
    loading, which no scan of its source can name; ``earlier`` is what an
    earlier document of it loaded in the owner's tab."""

    expected: list[str] | None = None
    loaded: tuple[str, ...] = ()
    earlier: tuple[str, ...] = ()
    caps: _Caps = _VERSION_CAPS

    async def manifest(self, reader: Reader, artifact: Artifact) -> Manifest:
        # What it loaded when last published, or before a reload in this tab,
        # is added, not replaced: a file only a click loaded may not have been
        # asked for again by this review. The owner may leave such a file out,
        # and the publish then forgets it.
        found = await walk_manifest(
            reader,
            artifact.path,
            loaded=self.loaded,
            remembered=(*artifact.loaded_files, *self.earlier),
            left_out=artifact.left_out_files,
        )
        if self.expected is not None and {e.path for e in found.entries} != set(
            self.expected
        ):
            raise SnapshotRefused("files_changed", files=found.entries)
        return found


@dataclass(frozen=True)
class _Listed:
    """Exactly ``paths``: one that is gone is left out, the entry never."""

    paths: tuple[str, ...]
    caps: _Caps = _VERSION_CAPS

    async def manifest(self, reader: Reader, artifact: Artifact) -> Manifest:
        wanted = [
            p
            for p in dict.fromkeys([artifact.path, *self.paths])
            if may_be_artifact(p)
        ]
        # A path that resolves to a file that may not leave is as missing as one
        # that is not there: the read would refuse it.
        found = {
            p: hit
            for p, hit in (await reader.files.stat(wanted)).items()
            if may_be_artifact(hit[0])
        }
        if artifact.path not in found:
            raise ManifestEntryMissing(artifact.path)
        dropped = await _spellings_left_out(reader, artifact, found)
        entries = [
            ManifestEntry(
                p, found[p][1], REASON_ENTRY if p == artifact.path else "listed"
            )
            for p in wanted
            if p in found and p not in dropped
        ]
        kept = {e.path for e in entries}
        sizes = {hit[0]: hit[1] for p, hit in found.items() if p in kept}
        left_out = [
            ManifestEntry(p, found[p][1], "listed", optional=True) for p in dropped
        ]
        return Manifest(entries, left_out, sum(sizes.values()))


def _frozen_list(artifact: Artifact) -> tuple[str, ...]:
    """The list a cut that does not walk reads: the record's, less what the
    owner has left out since. A file only a left-out page used stays until the
    owner's next publish walks the page again."""
    left_out = set(artifact.left_out_files)
    return tuple(p for p in artifact.files or () if p not in left_out)


async def _spellings_left_out(
    reader: Reader, artifact: Artifact, found: dict[str, tuple[str, int]]
) -> list[str]:
    """The listed paths to a file the owner left out under another of its
    paths, which go with it unless the page loads the file by one of them.

    A walk leaves out every path an optional reference reaches a left-out file
    by; the record keeps no reasons to tell those from a path the page loads,
    so only when one is listed does a walk decide."""
    asked = [
        p
        for p in artifact.left_out_files
        if p in (artifact.files or ()) and may_be_artifact(p)
    ]
    if not asked:
        return []
    try:
        gone = {hit[0] for hit in (await reader.files.stat(asked)).values()}
    except NotInStoredCopy:
        # Not a file the stored copy speaks for: nothing to tell its paths by.
        return []
    aliases = [
        p for p, hit in found.items() if p != artifact.path and hit[0] in gone
    ]
    if not aliases:
        return []
    walked = await walk_manifest(
        reader,
        artifact.path,
        remembered=artifact.loaded_files,
        left_out=artifact.left_out_files,
    )
    needed = {e.path for e in walked.entries}
    return [p for p in aliases if p not in needed]


async def _capture(
    artifact: Artifact,
    *,
    user_id: str,
    mode: ReadMode,
    source: _Walked | _Listed,
) -> Snapshot:
    """Read the artifact's files as ``source`` names them, from where
    ``mode`` allows."""
    workspace = await _workspace(artifact)
    try:
        async with snapshot_reader(workspace, user_id, mode) as reader:
            manifest = await source.manifest(reader, artifact)
            entries = manifest.entries
            source.caps.check(
                len(entries),
                manifest.byte_total,
                sum(e.size or 0 for e in entries),
                manifest,
            )
            contents = await _read_all(reader, entries, manifest.contents)
    except ManifestEntryMissing:
        raise SnapshotRefused("file_missing") from None
    except ManifestTooLarge as e:
        raise SnapshotRefused(
            e.code, limit=e.limit, files=e.entries, left_out=e.left_out
        ) from None
    except NoLiveSession:
        # Every turn's end on a computer this worker does not hold: expected.
        raise SnapshotUnavailable("no live session to read") from None
    except NotInStoredCopy:
        # A list read without the page's libraries is short, and a version cut
        # from it is broken. The owner can start the computer; a visitor waits.
        if mode is ReadMode.OWNER:
            raise SnapshotRefused("needs_computer") from None
        raise SnapshotUnavailable("a library folder is only on the computer") from None
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
    # Two paths to one file seal to the same bytes, which count once.
    source.caps.check(
        len(files),
        sum({f.sha256: f.byte_len for f in files}.values()),
        sum(f.byte_len for f in files),
        manifest,
    )
    entry = files[0]
    title = _html_title(
        entry.content if entry.content is not None else blobs[entry.sha256]
    )
    return Snapshot(
        entries=entries,
        left_out=manifest.left_out,
        files=files,
        blobs=blobs,
        title=title,
        root_refs=manifest.root_refs,
        absent=manifest.absent,
    )


async def _read_all(
    reader: Reader, entries: list[ManifestEntry], scanned: dict[str, bytes]
) -> list[bytes]:
    """Each entry's bytes: as the walk scanned it where it did, so a file the
    agent changes meanwhile cannot go out with a list built from what it was."""
    gate = asyncio.Semaphore(_READ_CONCURRENCY)

    async def read(entry: ManifestEntry) -> bytes:
        if entry.path in scanned:
            return scanned[entry.path]
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


async def _store_blobs(user_id: str, snapshot: Snapshot) -> None:
    """Claim and register the blobs before the rows that name them, as
    transcripts store theirs, so the sweep's grace covers the gap."""
    try:
        await store_unregistered_blobs(user_id, snapshot.blobs)
    except BlobError as e:
        raise SnapshotUnavailable(str(e)) from e


async def _announce(
    user_id: str, snapshot: Snapshot, result: artifacts_db.PublishResult
) -> None:
    if not result.changed:
        return
    logger.info(
        "Artifact %s at v%d (%d files, %d bytes, cut=%s)",
        result.artifact.artifact_id,
        result.seq,
        len(snapshot.files),
        snapshot.total,
        result.cut,
    )
    await publish_artifact_change(user_id, result.artifact)


def _drift(snapshot: Snapshot, before: dict[str, str]) -> Drift:
    now = {f.path: f.sha256 for f in snapshot.files}
    return Drift(
        added=sorted(now.keys() - before.keys()),
        removed=sorted(before.keys() - now.keys()),
        changed=sorted(p for p in now.keys() & before.keys() if now[p] != before[p]),
    )


async def status(
    artifact: Artifact,
    *,
    user_id: str,
    loaded: tuple[str, ...],
    earlier: tuple[str, ...] = (),
) -> Status:
    """The live file against what a publish now would replace, and what it
    would carry: the shared version for a shared record, since a newer one
    kept for the owner is not what visitors see, else the latest. Past a cap,
    what the walk listed with ``over_cap`` rather than a refusal: a file the
    owner may leave out is what puts it over as often as not, and its box is
    the way back under. Raises ``SnapshotRefused`` or ``SnapshotUnavailable``."""
    try:
        snapshot = await _capture(
            artifact,
            user_id=user_id,
            mode=ReadMode.OWNER,
            source=_Walked(loaded=loaded, earlier=earlier),
        )
    except SnapshotRefused as e:
        if e.code not in _CAP_CODES or e.files is None:
            raise
        over = OverCap(e.code, e.limit or 0, e.files, e.left_out or [])
        return Status(None, artifact.latest_seq, None, None, over)
    against = artifact.shared_seq if artifact.shared else artifact.latest_seq
    if not against:
        return Status(snapshot, artifact.latest_seq, None, None)
    before = await artifacts_db.get_version_hashes(artifact.artifact_id, against)
    drift = _drift(snapshot, before)
    return Status(snapshot, artifact.latest_seq, bool(drift), drift or None)


async def publish(
    artifact: Artifact,
    *,
    user_id: str,
    reviewed: list[str],
    label: str | None,
    keep_updated: bool | None,
    loaded: tuple[str, ...],
    earlier: tuple[str, ...] = (),
    reviewed_page: str | None = None,
) -> artifacts_db.PublishResult:
    """The owner's Publish or Update: share, and cut a version from the files
    the owner reviewed. ``SnapshotRefused('files_changed')`` carries the list
    as it is now when it differs from ``reviewed``, or when the page's code
    is no longer ``reviewed_page`` (``Snapshot.page_sha256``): the report the
    list was read with came from a page that ran other code.

    What the owner left out is kept only while the page still names it, or a
    path no page names any more fills the cap with a box the review never
    shows to clear."""
    snapshot = await _capture(
        artifact,
        user_id=user_id,
        mode=ReadMode.OWNER,
        source=_Walked(reviewed, loaded, earlier),
    )
    if reviewed_page is not None and snapshot.page_sha256 != reviewed_page:
        raise SnapshotRefused("files_changed", files=snapshot.entries)
    await _store_blobs(user_id, snapshot)
    try:
        result = await artifacts_db.publish_version(
            user_id=user_id,
            artifact_id=artifact.artifact_id,
            read=ReadBasis.of(artifact),
            files=snapshot.files,
            title=snapshot.title,
            label=label,
            keep_updated=keep_updated,
            loaded_files=[
                e.path for e in snapshot.entries if e.reason == REASON_LOADED
            ],
            left_out_files=[e.path for e in snapshot.left_out],
        )
    except LeftOutChanged:
        # The list a walk with the new choice finds is unknown without reading
        # again, which the owner's review does once it hears of the change.
        raise SnapshotRefused("files_changed", files=snapshot.entries) from None
    await _announce(user_id, snapshot, result)
    return result


async def keep(
    artifact: Artifact,
    *,
    user_id: str,
    loaded: tuple[str, ...] = (),
    earlier: tuple[str, ...] = (),
) -> Artifact:
    """Keep a record for its owner, with a version cut now from the files its
    page names and the ones it reported loading, as a publish takes them, so
    the turns after find them too. It reads as the owner's views read (a
    stopped computer is started). The record is kept in the write that cuts it, so a refused read
    leaves it as it was. A shared record's visitors stay on their version.
    Raises ``SnapshotRefused``, ``SnapshotUnavailable`` or, for a share or a
    version landing during the read, ``SharingChanged`` or ``VersionMoved``."""
    snapshot = await _capture(
        artifact,
        user_id=user_id,
        mode=ReadMode.OWNER,
        source=_Walked(loaded=loaded, earlier=earlier),
    )
    await _store_blobs(user_id, snapshot)
    try:
        result = await artifacts_db.keep_version(
            user_id=user_id,
            artifact_id=artifact.artifact_id,
            read=ReadBasis.of(artifact),
            files=snapshot.files,
            title=snapshot.title,
            loaded_files=[
                e.path for e in snapshot.entries if e.reason == REASON_LOADED
            ],
        )
    except LeftOutChanged:
        # As for a publish: the list with the new choice is unknown unread.
        raise SnapshotRefused("files_changed", files=snapshot.entries) from None
    await _announce(user_id, snapshot, result)
    return result.artifact


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
            source=_Listed(_frozen_list(artifact), caps=_LEGACY_CAPS),
        )
        await _store_blobs(user_id, snapshot)
        result = await artifacts_db.legacy_version(
            user_id=user_id,
            artifact_id=artifact.artifact_id,
            read=ReadBasis.of(artifact),
            files=snapshot.files,
            title=snapshot.title,
        )
    except (LeftOutChanged, VersionMoved, SharingChanged):
        # The record moved during the read (a file taken off, a version kept
        # for the owner): the visitor's next view reads it as it is now.
        raise SnapshotUnavailable("the record changed during the read") from None
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
    await _announce(user_id, snapshot, result)
    return result.artifact


async def keep_updated_after_turn(*, workspace_id: str, user_id: str) -> None:
    """Cut a version of each artifact in the workspace that follows turns
    (``Artifact.follows_turns``) whose files changed. Never raises.

    Reads only a session this worker holds: a stored copy can lag the
    computer, and a version cut from it could undo the one before. No warm
    session means no check this turn; the next turn's end looks again.
    """
    try:
        kept = await artifacts_db.list_kept_updated(workspace_id)
        if not kept:
            return
        kept = await _release_unlisted(kept, workspace_id=workspace_id, user_id=user_id)
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


#: How long a keep, and the owner's last write to their preferences, is left
#: alone before a record kept off the Dashboard list is let go: far past the
#: moment between a pin's keep and its listing, and the removal's undo window.
KEPT_RELEASE_GRACE = timedelta(minutes=10)


async def _release_unlisted(
    kept: list[Artifact], *, workspace_id: str, user_id: str
) -> list[Artifact]:
    """Let go of the records kept for a Dashboard the owner no longer lists,
    here, where keeping them costs a read and a version: the web's release
    after a removal is lost with a tab closed inside its undo window or a
    request that failed. A list that cannot be read lets go of nothing.
    Returns the records that still follow turns."""
    cutoff = datetime.now(UTC) - KEPT_RELEASE_GRACE
    if not any(a.kept_at is not None and a.kept_at < cutoff for a in kept):
        return kept
    listed = await dashboards.listed_pages(user_id)
    if listed is None:
        return kept
    if all(a.artifact_id in listed for a in kept if a.kept):
        return kept
    released = await artifacts_db.release_unlisted(
        user_id=user_id,
        workspace_id=workspace_id,
        listed=listed,
        grace=KEPT_RELEASE_GRACE,
    )
    if not released:
        return kept
    now = {a.artifact_id: a for a in released}
    for artifact in released:
        logger.info(
            "Artifact %s no longer kept: not on its owner's Dashboard list",
            artifact.artifact_id,
        )
        await publish_artifact_change(user_id, artifact)
    after = (now.get(a.artifact_id, a) for a in kept)
    return [a for a in after if a.follows_turns]


#: A burst of saves (an editor autosaving) waits this long and is read once.
_CHANGE_SETTLE_S = 5.0


def _change_pass_key(workspace_id: str) -> str:
    return f"artifacts:keep-pass:{workspace_id}"


def keep_updated_after_change(*, workspace_id: str, user_id: str) -> None:
    """Run the turn's-end pass after the owner changed the workspace's files
    outside a turn (the file panel's write, upload, delete), in the background,
    so a Dashboard follows an edit now rather than at the next turn's end."""
    from src.server.services.runs.coordinator import spawn_protected

    spawn_protected(
        _after_change(workspace_id, user_id),
        f"artifacts-keep-updated-change-{workspace_id}",
    )


async def _after_change(workspace_id: str, user_id: str) -> None:
    try:
        if not await artifacts_db.list_kept_updated(workspace_id):
            return
        if not await _claim_change_pass(workspace_id):
            return
    except Exception:
        logger.warning(
            "Scheduling a keep-updated pass in workspace %s failed",
            workspace_id,
            exc_info=True,
        )
        return
    # Past the claim's expiry, so a save landing once this pass may already be
    # reading claims a pass of its own.
    await asyncio.sleep(_CHANGE_SETTLE_S + 0.1)
    await keep_updated_after_turn(workspace_id=workspace_id, user_id=user_id)


async def _claim_change_pass(workspace_id: str) -> bool:
    """Whether this change starts a pass: the first in the workspace's window,
    on any worker, claims it, and the others' changes are read by that pass,
    which reads after the window ends. The pass runs on the worker that took
    the first change, which holds the warm session the read needs. Without
    Redis every change runs its own pass, which costs reads, never a wrong
    version: each cut is checked under the record's row lock."""
    from src.utils.cache.redis_cache import get_cache_client

    cache = get_cache_client()
    if not (cache.enabled and cache.client):
        return True
    try:
        return bool(
            await cache.client.set(
                _change_pass_key(workspace_id),
                1,
                nx=True,
                px=int(_CHANGE_SETTLE_S * 1000),
            )
        )
    except Exception:
        logger.debug("Claiming a keep-updated pass failed", exc_info=True)
        return True


async def _keep_one(artifact: Artifact, user_id: str) -> None:
    try:
        await _cut_after_turn(artifact, user_id)
        return
    except VersionMoved:
        # The version that landed during the read may hold files read before
        # these (another turn's end that began earlier and finished first), so
        # it is looked at once more, on the record as it now stands.
        fresh = await artifacts_db.get_artifact(artifact.artifact_id, user_id=user_id)
    if not fresh or not fresh.follows_turns:
        return
    try:
        await _cut_after_turn(fresh, user_id)
    except VersionMoved:
        logger.info("Not refreshing artifact %s: VersionMoved", artifact.artifact_id)


async def _cut_after_turn(artifact: Artifact, user_id: str) -> None:
    """One read and cut. ``VersionMoved`` is the caller's to act on."""
    listed = _frozen_list(artifact)
    # A kept record is walked again, since the owner's Dashboard follows the
    # newest copy: a data file the page starts reading joins the next version
    # without a review, and visitors follow only a version of the list they
    # were given (``visitors_follow_turn``). A shared one not kept keeps to
    # its list.
    walk = artifact.kept
    try:
        snapshot = await _capture(
            artifact,
            user_id=user_id,
            mode=ReadMode.WARM_ONLY,
            source=_Walked() if walk else _Listed(listed),
        )
    except (SnapshotRefused, SnapshotUnavailable) as e:
        logger.info("Not refreshing artifact %s: %s", artifact.artifact_id, e)
        return
    carried = {f.path for f in snapshot.files}
    left_out = [e.path for e in snapshot.left_out]
    # A listed file gone, maybe only while a rebuild runs: a version without it
    # would stay without it, since the list never widens on its own. The link
    # keeps its last version until the file is back or the owner's Update.
    if not walk and set(listed) - carried - set(left_out):
        logger.info(
            "Not refreshing artifact %s: a listed file is gone", artifact.artifact_id
        )
        return
    # A file the last version held that the page still names and is not there,
    # maybe only while a rebuild runs or a failed turn left it half written:
    # the walk drops it without a word, and the Dashboard would show the page
    # without it. A file the page stopped naming may go.
    if walk and snapshot.absent & set(
        await artifacts_db.get_version_hashes(artifact.artifact_id, artifact.latest_seq)
    ):
        logger.info(
            "Not refreshing artifact %s: a file its last version held is gone",
            artifact.artifact_id,
        )
        return
    # A share or unshare, or a file left out during the read, changes what
    # these bytes may give visitors; the next turn's end looks again.
    try:
        await _store_blobs(user_id, snapshot)
        result = await artifacts_db.turn_version(
            user_id=user_id,
            artifact_id=artifact.artifact_id,
            read=ReadBasis.of(artifact),
            files=snapshot.files,
            title=snapshot.title,
            left_out=left_out,
        )
    except (SnapshotUnavailable, LeftOutChanged, SharingChanged) as e:
        logger.info(
            "Not refreshing artifact %s: %s",
            artifact.artifact_id,
            type(e).__name__,
        )
        return
    await _announce(user_id, snapshot, result)
