"""The Drive: a server-held file tree per space, read and written by people
and their agents, every change kept as a version.

Every surface (the REST routes, the agent's file tools, the file mount, the
CLI) goes through these functions, and ``reach`` is the one permission check:
a surface asks it (or ``open``, which checks the ``drive`` feature first) for
the space and hands the answer to everything else.

Postgres holds the tree and the versions; the bytes are ``content``'s.
"""

from __future__ import annotations

import asyncio
import logging
import time
import uuid
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import asynccontextmanager
from typing import Literal

from src.config.core import get_infrastructure_config
from src.server.database.blob_keys import blob_key
from src.server.database.drive import reads, retention, writes
from src.server.database.drive.keys import normalize
from src.server.database.drive.records import (
    Actor,
    Changes,
    Content,
    DriveBusy,
    DriveError,
    Entry,
    HistoryRule,
    Limits,
    Space,
    TrashItem,
    Usage,
    Version,
)
from src.server.database.drive.writes import ContentRef
from src.server.services.drive import content_bytes as _bytes
from src.server.services.drive import feed
from src.server.services.drive.content_bytes import TEXT_MAX_BYTES, holding, read_range, stream
from src.server.services import membership
from src.server.services.features import user_feature_enabled
from src.utils.storage import get_signed_url, is_storage_enabled, multipart_enabled

logger = logging.getLogger(__name__)

__all__ = [
    "FEATURE",
    "Actor",
    "Changes",
    "Content",
    "DriveBusy",
    "DriveError",
    "Entry",
    "Limits",
    "Space",
    "TrashItem",
    "Usage",
    "Version",
    "changes",
    "content",
    "content_by_sha",
    "cursor",
    "enabled",
    "limits",
    "maintain",
    "holding",
    "list_dir",
    "mkdir",
    "move",
    "open",
    "purge",
    "reach",
    "read_range",
    "replace",
    "restore",
    "restore_version",
    "room_for",
    "save",
    "signed_url",
    "spaces",
    "stat",
    "stream",
    "text_bytes",
    "text_pages",
    "trash",
    "trashed",
    "tree",
    "usage",
    "versions",
    "write",
]

FEATURE = "drive"

NOTE_MAX = 500

#: The longest a ``changes`` call holds for the next change.
CHANGES_WAIT_MAX_S = 30

_TEXT_PAGE = 200
_TEXT_PAGE_BYTES = 16 * 1024 * 1024


# --- access ---------------------------------------------------------------


async def enabled(user_id: str) -> bool:
    """Whether the ``drive`` feature is on for the user."""
    return await user_feature_enabled(user_id, FEATURE)


def _uuid(value: str, message: str) -> str:
    """``value`` as a canonical id; anything else names nothing."""
    try:
        return str(uuid.UUID(value))
    except (ValueError, TypeError, AttributeError):
        raise DriveError("not_found", message) from None


async def reach(user_id: str, space_ref: str) -> Space:
    """The space ``space_ref`` names (``personal`` or a space id), if the
    user may use it.

    A space the user cannot reach answers ``not_found``, as one that does not
    exist would, so a guessed id learns nothing. Only a personal space, its
    owner's alone, exists for now, so reaching one is reaching it to write.
    """
    if space_ref == "personal":
        return await reads.personal_space(user_id)
    space = await reads.get_space(_uuid(space_ref, "No such space"))
    if space is None or space.kind != "personal" or space.user_id != user_id:
        raise DriveError("not_found", "No such space")
    return space


async def open(user_id: str, space_ref: str) -> Space:
    """``reach``, for a surface that has not checked the feature: with the
    Drive off for the user every space answers ``not_found``."""
    if not await enabled(user_id):
        raise DriveError("not_found", "No such space")
    return await reach(user_id, space_ref)


async def spaces(user_id: str) -> list[Space]:
    return [await reads.personal_space(user_id)]


# --- limits ---------------------------------------------------------------


def _config():
    return get_infrastructure_config().drive


#: The most one object the store keeps: the largest file with no cap on bytes.
_STORE_OBJECT_MAX_BYTES = 5 * 1024**4


async def allowance(user_id: str | None) -> int | None:
    """Bytes the space may keep live; None for no cap.

    The allowance the deployment's account service gives the owner, when it
    gives one, over the config. With no object storage the database holds
    the bytes, so its own cap stands whatever the account service says.
    """
    cfg = _config()
    if not is_storage_enabled():
        return cfg.max_space_bytes_in_database
    given = await membership.drive_allowance(user_id) if user_id else None
    if given is None:
        return cfg.max_space_bytes
    return None if given < 0 else given


def history_rule() -> HistoryRule:
    h = _config().history
    return HistoryRule(
        max_bytes=h.max_bytes,
        keep_days=h.keep_days,
        keep_max=h.keep_max,
        grace_seconds=h.replaced_grace_seconds,
    )


async def limits(space: Space) -> Limits:
    """Everything that bounds the space, resolved once for a call to pass on.

    The largest file is the allowance where the store takes uploads in
    parts, unless the deployment sets less; a store that does not, or none,
    keeps what one request carries."""
    return await _limits(space.user_id)


async def _limits(user_id: str | None) -> Limits:
    cfg = _config()
    cap = await allowance(user_id)
    if not is_storage_enabled():
        largest = max(cfg.max_file_bytes_in_database, TEXT_MAX_BYTES)
    elif multipart_enabled():
        largest = _STORE_OBJECT_MAX_BYTES if cap is None else cap
    else:
        largest = cfg.max_request_bytes
    if cfg.max_file_bytes is not None:
        largest = min(largest, cfg.max_file_bytes)
    return Limits(
        max_space_bytes=cap,
        max_entries=cfg.max_entries,
        max_trash_bytes=None if cap is None else int(cap * cfg.trash_max_ratio),
        max_file_bytes=largest,
        max_request_bytes=min(largest, cfg.max_request_bytes),
        history=history_rule(),
    )


async def _space_limits(space_id: str) -> Limits:
    """The limits of a space known only by id, for the sweep."""
    space = await reads.get_space(space_id)
    return await _limits(space.user_id if space is not None else None)


async def usage(space: Space, bounds: Limits | None = None) -> Usage:
    kept, trashed, entries = await reads.usage(space.space_id)
    bounds = bounds or await limits(space)
    return Usage(
        bytes=kept,
        trash_bytes=trashed,
        entries=entries,
        max_bytes=bounds.max_space_bytes,
        max_entries=bounds.max_entries,
        max_file_bytes=bounds.max_request_bytes,
        max_upload_bytes=bounds.max_file_bytes,
    )


# --- reads ----------------------------------------------------------------


async def stat(space: Space, path: str | None = None, *, entry_id: str | None = None) -> Entry:
    if entry_id is not None:
        found = await reads.live_entry(space.space_id, entry_id=_uuid(entry_id, "No such entry"))
    else:
        found = await reads.live_entry(space.space_id, path=normalize(path or ""))
    if found is None:
        raise DriveError("not_found", f"Nothing at {path}" if path else "No such entry")
    return found


async def _folder(space: Space, path: str) -> str:
    """The stored path of the folder at ``path``; ``''`` is the top."""
    path = normalize(path, root=True)
    if not path:
        return ""
    folder = await stat(space, path)
    if folder.kind != "folder":
        raise DriveError("not_folder", f"{folder.path} is a file")
    return folder.path


async def list_dir(space: Space, path: str = "") -> list[Entry]:
    return await reads.children(space.space_id, await _folder(space, path))


async def tree(space: Space, path: str = "") -> list[Entry]:
    """Every live entry, or every one under the folder at ``path``."""
    return await reads.subtree(space.space_id, await _folder(space, path))


async def content(
    space: Space,
    path: str | None = None,
    *,
    entry_id: str | None = None,
    seq: int | None = None,
) -> tuple[Entry, Content]:
    """Where a file's bytes are (its head, or version ``seq``), without
    reading them, so a surface reads only the range it serves."""
    entry = await stat(space, path, entry_id=entry_id)
    if entry.kind != "file":
        raise DriveError("is_folder", f"{entry.path} is a folder")
    found = await reads.content_of(space.space_id, entry.entry_id, seq or entry.head_seq)
    if found is None:
        raise DriveError("not_found", f"{entry.path} has no version {seq}")
    return entry, found


#: How long a signed download address works.
LINK_TTL_S = 300


async def signed_url(
    found: Content,
    *,
    ttl: int = LINK_TTL_S,
    disposition: str | None = None,
    for_browser: bool = True,
) -> str | None:
    """A short-lived address on the store for ``found``'s bytes, served as
    ``disposition`` names it; None when the store holds no object for them
    or cannot sign one. The store answers ``Range`` itself, and the address
    names the bytes, not a path, so it keeps serving that version after the
    file changes."""
    if found.blob_user_id is None or not is_storage_enabled():
        return None
    key = blob_key(found.blob_user_id, found.sha256)
    return await asyncio.to_thread(
        get_signed_url,
        key,
        ttl,
        content_disposition=disposition,
        content_type=found.content_type,
        for_browser=for_browser,
    )


async def content_by_sha(space: Space, sha256: str) -> Content:
    """Content the space holds under ``sha256``, from any version or the
    trash. Addressed by its bytes, so a read of it never meets a newer save."""
    sha256 = (sha256 or "").strip().lower()
    found = None
    if len(sha256) == 64 and all(c in "0123456789abcdef" for c in sha256):
        found = await reads.content_by_sha(space.space_id, sha256)
    if found is None:
        raise DriveError("not_found", "The space holds no such content")
    return found


async def text_pages(
    space: Space, entry_ids: list[str] | None = None, *, under: str | None = None
) -> AsyncIterator[list[tuple[Entry, bytes]]]:
    """Every live text file the database holds, with its bytes as stored, a
    page at a time as read: at most ``_TEXT_PAGE`` files and, past the
    first, ``_TEXT_PAGE_BYTES``, so a whole space is held a page at a time.
    Each entry's ``sha256`` names the bytes that come with it. ``under`` is
    a folder's path, filtered in SQL; a path with nothing at it is
    ``not_found``."""
    stored_under = None
    if under is not None and normalize(under, root=True):
        stored_under = (await stat(space, under)).path
    if entry_ids is not None:
        valid = []
        for entry_id in entry_ids:
            try:
                valid.append(str(uuid.UUID(entry_id)))
            except (ValueError, TypeError, AttributeError):
                continue
        entry_ids = valid
        if not entry_ids:
            return
    after = None
    while page := await reads.texts(
        space.space_id,
        entry_ids,
        after,
        _TEXT_PAGE,
        max_bytes=_TEXT_PAGE_BYTES,
        under=stored_under,
    ):
        yield page
        after = page[-1][0].entry_id


async def text_bytes(
    space: Space, entry_ids: list[str] | None = None, *, under: str | None = None
) -> AsyncIterator[tuple[Entry, bytes]]:
    """``text_pages`` a file at a time."""
    async for page in text_pages(space, entry_ids, under=under):
        for found in page:
            yield found


async def versions(space: Space, entry: Entry) -> tuple[list[Version], bool]:
    """A file's versions, newest first, each with when the history rule
    lets it go, and whether the head is too large to keep once replaced."""
    if entry.kind != "file":
        raise DriveError("is_folder", f"{entry.path} is a folder")
    return await reads.versions(entry.entry_id, history_rule())


async def history_bytes(space: Space, entry: Entry) -> int:
    """What clearing ``entry``'s history would take out of the counts."""
    return await reads.history_bytes(space.space_id, entry.entry_id, entry.head_seq)


async def clear_history(
    space: Space, entry_id: str, *, before: int | None = None
) -> tuple[int, int]:
    """Drop a file's old versions (all, or those below seq ``before``),
    keeping the head: ``(versions dropped, bytes that left the counts)``.
    Bytes another version or entry references stay."""
    entry_id = _uuid(entry_id, "No such entry")
    # It only frees, so it is never refused for room. A clear that dropped
    # anything is a change to the file, which the feed carries.
    async with writes.upkeep(space.space_id, await limits(space)) as tx:
        cleared = await retention.clear(tx, entry_id, before)
    if cleared[0]:
        await feed.announce(space.space_id)
    return cleared


async def trashed(space: Space, *, limit: int | None = None) -> list[TrashItem]:
    """Trash items, newest first, at most ``limit`` of them."""
    return await reads.trashed(space.space_id, limit)


async def cursor(space: Space) -> int:
    """The space's last change number: ``changes`` from it answers what
    comes after."""
    return await reads.cursor(space.space_id)


async def changes(
    space: Space, since: int, *, wait: float = 0, limit: int | None = None
) -> Changes:
    """What changed after ``since``. With ``wait`` (seconds, at most
    ``CHANGES_WAIT_MAX_S``) and nothing yet, holds until something changes
    or the wait ends, then answers empty with the same cursor. Past ``limit``
    changed entries the answer is ``resync``: read the whole space instead."""
    wait = max(0.0, min(float(wait), CHANGES_WAIT_MAX_S))
    if wait == 0:
        return await reads.changes(space.space_id, since, limit)
    deadline = time.monotonic() + wait
    with feed.Waiter(space.space_id) as waiter:
        while True:
            found = await reads.changes(space.space_id, since, limit)
            left = deadline - time.monotonic()
            if found.entries or found.resync or found.cursor != since or left <= 0:
                return found
            await waiter.sleep(left)


# --- writes ---------------------------------------------------------------


def _blob_user(space: Space) -> str:
    """Whose object-storage keys hold the space's bytes."""
    assert space.user_id is not None
    return space.user_id


def _note(note: str | None) -> str | None:
    note = (note or "").strip()
    return note[:NOTE_MAX] or None


@asynccontextmanager
async def _changing(space: Space, bounds: Limits | None = None) -> AsyncIterator[writes.Tx]:
    """A change to the space under its lock; once it commits, a ``changes``
    call waiting on the space wakes."""
    async with writes.locked(space.space_id, bounds or await limits(space)) as tx:
        yield tx
    await feed.announce(space.space_id)


async def write(
    space: Space,
    path: str,
    data: bytes,
    *,
    actor: Actor,
    if_match: str | None = None,
    if_none_match: bool = False,
    note: str | None = None,
    content_type: str | None = None,
    bounds: Limits | None = None,
) -> Entry:
    """Save ``data`` as the file at ``path``, making the folders above it.

    ``if_match`` is the head's sha256 the caller edited, or ``*`` for
    whatever file is there; ``if_none_match`` asks that nothing be at the
    path. Saves by one actor under one sitting are one version; the same
    bytes as the head record nothing. ``bounds``: the space's ``limits``
    when the caller has them already.
    """
    bounds = bounds or await limits(space)
    path = normalize(path)
    is_text = _bytes.kept_as_text(path, data)
    stored = is_storage_enabled() and not is_text
    cap = bounds.max_request_bytes
    if not (stored or is_text):
        cap = min(cap, _config().max_file_bytes_in_database)
    bounds.too_large(len(data), cap)
    sha = await _bytes.digest(data)
    ref = ContentRef(
        sha256=sha,
        size=len(data),
        is_text=is_text,
        content_type=_bytes.content_type(path.rsplit("/", 1)[-1], is_text, content_type),
    )
    blob_user_id = None
    if stored:
        blob_user_id = _blob_user(space)
        if not await _bytes.registered(blob_user_id, sha):
            await room_for(
                space,
                path,
                ref,
                actor,
                if_match=if_match,
                if_none_match=if_none_match,
                bounds=bounds,
            )
            await _bytes.upload(blob_user_id, sha, data)
    return await save(
        space,
        path,
        ref,
        actor,
        data=None if stored else data,
        blob_user_id=blob_user_id,
        if_match=if_match,
        if_none_match=if_none_match,
        note=note,
        bounds=bounds,
    )


async def save(
    space: Space,
    path: str,
    ref: ContentRef,
    actor: Actor,
    *,
    data: bytes | None = None,
    blob_user_id: str | None = None,
    if_match: str | None = None,
    if_none_match: bool = False,
    note: str | None = None,
    bounds: Limits | None = None,
    then: Callable[[writes.Tx, Entry], Awaitable[None]] | None = None,
) -> Entry:
    """Record content described by ``ref`` as the file at ``path``: bytes
    the space or the store holds already (``blob_user_id``), or ``data``
    for the database. ``then`` runs in the same transaction, for a caller
    whose own record must land with the version."""
    async with _changing(space, bounds) as tx:
        entry = await tx.save(
            normalize(path),
            ref,
            actor,
            data=data,
            blob_user_id=blob_user_id,
            if_match=if_match,
            if_none_match=if_none_match,
            note=_note(note),
        )
        if then is not None:
            await then(tx, entry)
        return entry


async def room_for(
    space: Space,
    path: str,
    ref: ContentRef,
    actor: Actor,
    *,
    if_match: str | None,
    if_none_match: bool,
    bounds: Limits,
) -> None:
    """Refuse, before a file's bytes move, a save its commit would refuse:
    the commit's own code, run and rolled back, so bytes the space already
    holds, a sitting's fold and the history rule count as they will. The
    commit checks again; this spares a transfer that would be refused.
    ``ref`` names the bytes by digest where it is known and by a name no
    digest has where it is not."""
    await writes.trial(
        space.space_id,
        bounds,
        lambda tx: tx.save(
            normalize(path),
            ref,
            actor,
            blob_user_id=_blob_user(space),
            if_match=if_match,
            if_none_match=if_none_match,
            note=None,
        ),
    )


async def mkdir(space: Space, path: str) -> Entry:
    async with _changing(space) as tx:
        return await tx.mkdir(normalize(path))


async def move(space: Space, src: str, dst: str) -> Entry:
    """Rename or move ``src`` to ``dst``; a folder takes everything under it."""
    async with _changing(space) as tx:
        return await tx.move(normalize(src), normalize(dst))


async def replace(
    space: Space, src: str, dst: str, *, actor: Actor
) -> tuple[Entry, str | None]:
    """Move the file at ``src`` over the file at ``dst`` (a plain move when
    nothing is there), as a rename over an existing file does: ``dst`` keeps
    its id and history and takes ``src``'s content as a new version by
    ``actor``, and ``src`` ends as a tombstone, reported by ``changes`` as
    deleted, never in the trash, purged after a day. Its bytes live on as
    ``dst``'s version, so keeping it would only leave every editor's temp
    file in the person's trash. This is how a program's save by rename (an
    editor, ``sed -i``) lands.

    A source with earlier versions goes to the trash instead (``changes``
    reports it deleted the same way), so nothing a replace displaces is
    lost where the person cannot restore it.

    Returns the entry now at ``dst`` and the id of the source when it ended.
    Raises ``is_folder`` over a folder and ``not_folder`` for a folder source
    over a file.
    """
    async with _changing(space) as tx:
        return await tx.replace(normalize(src), normalize(dst), actor)


async def trash(
    space: Space,
    path: str,
    *,
    actor: Actor,
    kind: Literal["file", "folder"] | None = None,
    empty: bool = False,
    if_match: str | None = None,
) -> str:
    """Move the entry at ``path``, and everything under it, to the trash;
    the answer is the id that restores them together. Deletes by one actor
    in one sitting within a minute share an id. ``kind`` must match
    what is there (``is_folder``, ``not_folder``), and ``empty`` refuses a
    folder that holds anything (``not_empty``), as ``rmdir`` does.
    ``if_match``: only while the file's head is that sha256 (``changed``)."""
    async with _changing(space) as tx:
        return await tx.trash(normalize(path), actor, kind, empty, if_match)


async def restore(space: Space, trash_id: str) -> list[Entry]:
    """Bring a trash item back. Each top entry goes beside a live one on its
    path under a numbered name, and folders it sat in are made again."""
    trash_id = _uuid(trash_id, "Nothing in the trash under that id")
    async with _changing(space) as tx:
        return await tx.restore(trash_id)


async def purge(space: Space, trash_id: str | None = None) -> int:
    """Delete for good one delete in the trash, or the whole trash. For a
    person only: surfaces an agent uses do not offer it. A change cursor
    from before the newest purged notice must start over."""
    if trash_id is not None:
        trash_id = _uuid(trash_id, "Nothing in the trash under that id")
    async with writes.upkeep(space.space_id, await limits(space)) as tx:
        purged = await tx.purge(trash_id, None)
    if tx.changed:
        await feed.announce(space.space_id)
    return purged


async def maintain(space_id: str) -> None:
    """What the sweep does for a space, under one lock: recount it (a drift
    is corrected and logged), purge the trash and tombstones past their
    days, trim history the rule says is due, free released copies past
    their grace; committing bounds the trash."""
    bounds = await _space_limits(space_id)
    # Never refused for room: a recount that raises the live bytes of a
    # space over its allowance must still commit, or it is never healed.
    async with writes.upkeep(space_id, bounds) as tx:
        drift = await retention.reconcile(tx)
        if drift:
            # Counted wrong somewhere; healed here, and shown so it is found.
            logger.warning("[Drive] space %s counters drifted, corrected: %s", space_id, drift)
        await tx.purge(None, _config().trash_days)
        await retention.trim(tx)
        await retention.free_released(tx, older_than_seconds=bounds.history.grace_seconds)
    # Freeing released copies moves no count a client reads; the rest take a
    # change number when they change anything, the trash's bound at commit too.
    if tx.changed:
        await feed.announce(space_id)


async def restore_version(space: Space, entry_id: str, seq: int, *, actor: Actor) -> Entry:
    """Make version ``seq``'s content the head again, as a new version that
    names the one it came from."""
    entry_id = _uuid(entry_id, "No such entry")
    async with _changing(space) as tx:
        return await tx.restore_version(entry_id, seq, actor)
