"""Drive mutations, each one transaction under the space's row lock.

The lock serialises everything a change depends on in one space: the change
sequence every changed entry takes, the quota check against what is kept, the
case-insensitive path uniqueness and the folders a write creates. Bytes bound
for object storage are stored before the transaction opens; a transaction that
then fails leaves an object the blob sweep collects.

Paths are looked up by ``path_key`` (``keys.path_key``), never by ``path``,
so the case a caller sends never decides which entry it meets.
"""

from __future__ import annotations

import dataclasses
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import datetime
from typing import Any

import psycopg
from psycopg.rows import dict_row

from src.server.database.drive import aside, changes, reads, retention, trash
from src.server.database.drive.policy import UNDO_TARGET_SQL, folds
from src.server.database.drive.keys import (
    MAX_DEPTH,
    MAX_PATH_BYTES,
    check,
    path_bytes,
    path_depth,
    path_key,
    split_path,
)
from src.server.database.drive.records import (
    ENTRY_COLUMNS,
    ENTRY_FROM,
    Actor,
    DriveError,
    Entry,
    Limits,
    entry_from_row,
)
from src.server.database.pool import get_db_connection



@dataclass(frozen=True)
class ContentRef:
    """The content a version points at: its digest and what is known of it."""

    sha256: str
    size: int
    is_text: bool
    content_type: str


class ContentGone(DriveError):
    """A save naming content the space held when it was asked for and has
    freed since: the caller sends the bytes instead."""

    def __init__(self) -> None:
        super().__init__("not_found", "The space no longer holds that content")


@dataclass(frozen=True)
class Renamed:
    """What a rename on a computer left at its destination."""

    #: The entry at the destination now.
    entry: Entry
    #: The entry that ended: the file replaced, or the source whose bytes
    #: the destination took.
    ended_id: str | None
    #: Whether ``entry`` is the source itself rather than a file that took
    #: its bytes.
    carried: bool
    #: The trash id, when the source was a memo and went to the trash.
    trashed_memo: str | None = None


class _Trial(Exception):
    """Rolls back a change tried only to learn whether it would commit."""


def refuse_conflict(found: Entry | None, if_match: str | None, if_none_match: bool) -> None:
    """Refuse a save over ``found`` (what is at the path now) that its
    conditions rule out: ``if_match`` the head's sha256 or ``*``,
    ``if_none_match`` nothing there."""
    if found is not None and found.kind == "folder":
        raise DriveError("is_folder", f"{found.path} is a folder")
    if if_none_match and found is not None:
        raise DriveError("exists", f"{found.path} already exists")
    if if_match == "*" and found is None:
        raise DriveError("changed", "Nothing is at that path")
    if if_match not in (None, "*") and (found is None or found.sha256 != if_match):
        raise DriveError("changed", "The file changed since that version was read")


class Tx:
    """One locked transaction on a space, and the changes made in it."""

    def __init__(self, cur: Any, space_id: str, limits: Limits) -> None:
        self.cur = cur
        self.space_id = space_id
        self.limits = limits
        self._seq: int | None = None
        #: Each file this transaction gave a new head, by id.
        self.written: set[str] = set()
        #: Those still live, as the transaction leaves them: read once it is
        #: done, what the caller hands on once it commits.
        self.heads: list[Entry] = []

    # --- building blocks ---------------------------------------------------

    async def seq(self) -> int:
        """The space's next change number, taken once per transaction."""
        if self._seq is None:
            row = await self.one(
                "UPDATE drive_spaces SET change_seq = change_seq + 1"
                " WHERE space_id = %(space)s RETURNING change_seq",
                {"space": self.space_id},
            )
            self._seq = row["change_seq"]
        return self._seq

    @property
    def changed(self) -> bool:
        """Whether this transaction took a change number: it changed what a
        client sees, so the feed is woken once it commits. Upkeep with
        nothing to show takes none, keeping every cursor where it was."""
        return self._seq is not None

    async def one(self, sql: str, params: dict[str, Any]) -> dict[str, Any] | None:
        await self.cur.execute(sql, params)
        return await self.cur.fetchone()

    async def all(self, sql: str, params: dict[str, Any]) -> list[dict[str, Any]]:
        await self.cur.execute(sql, params)
        return list(await self.cur.fetchall())

    async def live(self, path: str | None = None, *, entry_id: str | None = None) -> Entry | None:
        return await reads.live_entry(
            self.space_id, path=path, entry_id=entry_id, conn=self.cur.connection
        )

    async def file(self, entry_id: str) -> Entry:
        """The live file ``entry_id`` names, under the lock, or the refusal."""
        return await reads.live_file(self.space_id, entry_id, conn=self.cur.connection)

    async def entry(self, entry_id: str) -> Entry:
        row = await self.one(
            f"SELECT {ENTRY_COLUMNS} FROM {ENTRY_FROM} WHERE e.entry_id = %(id)s",
            {"id": entry_id},
        )
        assert row is not None
        return entry_from_row(row)

    async def now(self) -> datetime:
        """The transaction's time, the one every ``now()`` in it reads."""
        row = await self.one("SELECT now() AS now", {})
        assert row is not None
        return row["now"]

    async def check_room(self, adding: int) -> None:
        """Every row counts, the trash and delete notices included: a burst
        of deletes would otherwise grow the table without bound."""
        if adding <= 0:
            return
        if (await retention.counts(self))["entries"] + adding > self.limits.max_entries:
            raise DriveError(
                "too_many_entries",
                f"A space holds at most {self.limits.max_entries} files and folders",
            )

    async def resolve_parent(self, path: str) -> tuple[str, list[str]]:
        """The deepest live folder above ``path`` as stored (its case may
        differ from the one asked for), and the segments under it that do
        not exist yet. Nothing lives under a missing folder, so everything
        below the first missing one is missing too."""
        segments = path.split("/")[:-1]
        if not segments:
            return "", []
        keys = [path_key("/".join(segments[: i + 1])) for i in range(len(segments))]
        rows = await self.all(
            "SELECT e.path, e.kind FROM unnest(%(keys)s::text[]) WITH ORDINALITY u(k, i)"
            " LEFT JOIN drive_entries e ON e.space_id = %(space)s"
            " AND e.deleted_at IS NULL AND e.path_key = u.k"
            " ORDER BY u.i",
            {"space": self.space_id, "keys": keys},
        )
        base = ""
        for i, row in enumerate(rows):
            if row["path"] is None:
                return base, segments[i:]
            if row["kind"] != "folder":
                raise DriveError("not_folder", f"{row['path']} is a file, not a folder")
            base = row["path"]
        return base, []

    async def make_folders(self, base: str, missing: list[str]) -> str:
        path = base
        for name in missing:
            parent = path
            path = f"{parent}/{name}" if parent else name
            await self.cur.execute(
                "INSERT INTO drive_entries"
                " (space_id, path, path_key, parent, name, kind, changed_seq)"
                " VALUES (%s, %s, %s, %s, %s, 'folder', %s)",
                (self.space_id, path, path_key(path), parent, name, await self.seq()),
            )
        return path

    async def place(self, path: str, extra: int, *, checked: bool = True) -> str:
        """Make the folders missing above ``path``, with room for them and
        ``extra`` entries more; the answer is ``path`` as it will be stored,
        in the case of the folders it lands in. That spelling can be longer
        in bytes than the one asked for (``ẞ`` for ``ss``), so it is checked
        again unless ``checked`` is off for a caller that fits it itself."""
        base, missing = await self.resolve_parent(path)
        await self.check_room(len(missing) + extra)
        parent = await self.make_folders(base, missing)
        name = split_path(path)[1]
        stored = f"{parent}/{name}" if parent else name
        if checked:
            check(stored)
        return stored

    async def relocate(
        self, top_id: str, old: str, new: str, *, revive: list[str] | None = None
    ) -> None:
        """Rewrite the paths of ``top_id`` and everything under it from
        ``old`` to ``new``, in one statement. ``revive`` brings back those
        trashed rows instead of moving the live ones."""
        if revive is None:
            rows = "deleted_at IS NULL AND (entry_id = %(id)s OR starts_with(path, %(old)s || '/'))"
            also = "modified_at = CASE WHEN entry_id = %(id)s AND kind = 'folder' THEN now() ELSE modified_at END"
        else:
            rows = "entry_id = ANY(%(ids)s::uuid[])"
            also = "ended = NULL, deleted_at = NULL, trash_id = NULL, deleted_by = NULL, trash_sitting = NULL"
        parent, name = split_path(new)
        await self.cur.execute(
            f"""
            UPDATE drive_entries SET
                path = %(new)s || substr(path, %(n)s + 1),
                path_key = %(new_key)s || substr(path_key, %(nk)s + 1),
                parent = CASE WHEN entry_id = %(id)s THEN %(parent)s
                              ELSE %(new)s || substr(parent, %(n)s + 1) END,
                name = CASE WHEN entry_id = %(id)s THEN %(name)s ELSE name END,
                {also}, changed_seq = %(seq)s
             WHERE space_id = %(space)s AND {rows}
            """,
            {
                "new": new,
                "n": len(old),
                "new_key": path_key(new),
                "nk": len(path_key(old)),
                "id": top_id,
                "old": old,
                "parent": parent,
                "name": name,
                "seq": await self.seq(),
                "space": self.space_id,
                "ids": revive,
            },
        )

    async def retain(
        self, ref: ContentRef, *, data: bytes | None = None, blob_user_id: str | None = None
    ) -> None:
        """Keep newly supplied bytes in the space unless it holds them already,
        checked against the space's bytes: ``data`` for the database to hold,
        or ``blob_user_id``, whose object-storage key holds them. Content the
        space holds needs neither.

        A file's kind is its version's (``is_text``, by its name's rule); the
        content only says where the bytes are. Text is searched where the
        database holds it, so bytes a data file put in the store, saved again
        as text with ``data``, move into the database; the store's copy is
        left to its sweep once nothing names it."""
        known = await self.one(
            "SELECT released_at IS NOT NULL AS released, content IS NULL AS in_store"
            " FROM drive_contents WHERE space_id = %(space)s AND sha256 = %(sha)s",
            {"space": self.space_id, "sha": ref.sha256},
        )
        if known is not None:
            moved = ref.is_text and data is not None and known["in_store"]
            if known["released"] or moved:
                # A copy waiting to be freed, kept again: it counts again.
                await self.cur.execute(
                    "UPDATE drive_contents SET released_at = NULL,"
                    " content = COALESCE(%(data)s, content), is_text = is_text OR %(text)s,"
                    " blob_user_id = CASE WHEN %(moved)s THEN NULL ELSE blob_user_id END"
                    " WHERE space_id = %(space)s AND sha256 = %(sha)s",
                    {
                        "data": data if moved else None,
                        "text": moved,
                        "moved": moved,
                        "space": self.space_id,
                        "sha": ref.sha256,
                    },
                )
            return
        if data is None and blob_user_id is None:
            # Content the caller saw the space hold, freed since.
            raise ContentGone()
        if data is not None and blob_user_id is not None:
            raise ValueError("retain takes the bytes or their blob owner, not both")
        await self.cur.execute(
            "INSERT INTO drive_contents"
            " (space_id, sha256, byte_len, is_text, content, blob_user_id)"
            " VALUES (%s, %s, %s, %s, %s, %s)",
            (self.space_id, ref.sha256, ref.size, ref.is_text, data, blob_user_id),
        )

    async def stored(self, entry_id: str, seq: int | None) -> ContentRef | None:
        """The content version ``seq`` of ``entry_id`` points at, read in this
        transaction; None with no such version."""
        row = await self.one(
            "SELECT v.sha256, c.byte_len AS size, v.is_text, v.content_type"
            " FROM drive_versions v LEFT JOIN drive_contents c"
            " ON c.space_id = v.space_id AND c.sha256 = v.sha256"
            " WHERE v.entry_id = %(id)s AND v.seq = %(seq)s",
            {"id": entry_id, "seq": seq},
        )
        if row is None:
            return None
        if row["size"] is None:
            raise DriveError("not_found", "The space no longer holds that content")
        return ContentRef(**row)

    async def record_version(
        self,
        entry_id: str,
        seq: int,
        content: ContentRef,
        actor: Actor,
        note: str | None,
        restored_from: int | None,
        change_id: str | None,
    ) -> None:
        await self.cur.execute(
            "INSERT INTO drive_versions (entry_id, space_id, seq, sha256, byte_len,"
            " content_type, is_text, actor_kind, actor_user_id, agent, via, thread_id,"
            " run_id, workspace_id, computer_id, sitting, note, restored_from, change_id)"
            " VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
            (
                entry_id,
                self.space_id,
                seq,
                content.sha256,
                content.size,
                content.content_type,
                content.is_text,
                actor.kind,
                actor.user_id,
                actor.agent,
                actor.via,
                actor.thread_id,
                actor.run_id,
                actor.workspace_id,
                actor.computer_id,
                actor.sitting,
                note,
                restored_from,
                change_id,
            ),
        )

    async def write(
        self,
        found: Entry | None,
        path: str,
        content: ContentRef,
        actor: Actor,
        *,
        note: str | None = None,
        restored_from: int | None = None,
    ) -> Entry:
        """Record ``content``, which the space holds, as the file ``found``
        or as a new one at ``path``. Saves by one actor in one sitting are
        one version; the same bytes as the head record nothing."""
        if found is not None and found.sha256 == content.sha256:
            return found
        change_id = None
        if found is not None:
            # A memo the agent changes says why through its check-out.
            change_id, note = await changes.for_save(self, found, actor, note)
        seq = await self.seq()
        if found is None:
            path = await self.place(path, 1)
            parent, name = split_path(path)
            row = await self.one(
                "INSERT INTO drive_entries (space_id, path, path_key, parent, name, kind,"
                " head_seq, changed_seq) VALUES (%(space)s, %(path)s, %(key)s, %(parent)s,"
                " %(name)s, 'file', 1, %(seq)s) RETURNING entry_id::text AS id",
                {
                    "space": self.space_id,
                    "path": path,
                    "key": path_key(path),
                    "parent": parent,
                    "name": name,
                    "seq": seq,
                },
            )
            await self.record_version(row["id"], 1, content, actor, note, restored_from, None)
            return await self.wrote(row["id"])
        if folds(found, actor, change_id):
            version = found.head_seq
            await self.cur.execute(
                "UPDATE drive_versions SET sha256 = %s, byte_len = %s, content_type = %s,"
                " is_text = %s, created_at = now(), thread_id = %s, run_id = %s,"
                " workspace_id = %s, computer_id = %s, via = %s, note = COALESCE(%s, note)"
                " WHERE entry_id = %s AND seq = %s",
                (
                    content.sha256,
                    content.size,
                    content.content_type,
                    content.is_text,
                    actor.thread_id,
                    actor.run_id,
                    actor.workspace_id,
                    actor.computer_id,
                    actor.via,
                    note,
                    found.entry_id,
                    version,
                ),
            )
            # The copy it replaced is no version now: released, not dropped,
            # so a reader holding its digest can finish.
            await retention.release(self, [found.sha256])
        else:
            version = (found.head_seq or 0) + 1
            await self.record_version(
                found.entry_id, version, content, actor, note, restored_from, change_id
            )
        await self.cur.execute(
            "UPDATE drive_entries SET head_seq = %s, modified_at = now(), changed_seq = %s"
            " WHERE entry_id = %s",
            (version, seq, found.entry_id),
        )
        # A save folded into the head is still the file's last save.
        await retention.keep(self, [found.entry_id], saved=True)
        return await self.wrote(found.entry_id)

    async def wrote(self, entry_id: str) -> Entry:
        """The file as it now stands, noted in ``written``."""
        self.written.add(entry_id)
        return await self.entry(entry_id)

    async def _read_heads(self) -> None:
        if not self.written:
            return
        rows = await self.all(
            f"SELECT {ENTRY_COLUMNS} FROM {ENTRY_FROM}"
            " WHERE e.entry_id = ANY(%(ids)s::uuid[]) AND e.deleted_at IS NULL",
            {"ids": list(self.written)},
        )
        self.heads = [entry_from_row(r) for r in rows]

    async def found(self, path: str) -> Entry:
        found = await self.live(path)
        if found is None:
            raise DriveError("not_found", f"Nothing at {path}")
        return found

    # --- changes -----------------------------------------------------------

    async def save(
        self,
        path: str,
        content: ContentRef,
        actor: Actor,
        *,
        data: bytes | None = None,
        blob_user_id: str | None = None,
        if_match: str | None = None,
        if_none_match: bool = False,
        note: str | None = None,
    ) -> Entry:
        """Save newly supplied bytes (``data``, or a blob ``blob_user_id``
        holds) as the file at ``path``."""
        found = await self.live(path)
        refuse_conflict(found, if_match, if_none_match)
        if found is None and await aside.reclaim(self, path, actor):
            found = await self.live(path)
        if found is not None and found.sha256 == content.sha256:
            return found
        await self.retain(content, data=data, blob_user_id=blob_user_id)
        return await self.write(found, path, content, actor, note=note)

    async def mkdir(self, path: str) -> Entry:
        found = await self.live(path)
        if found is not None:
            if found.kind != "folder":
                raise DriveError("exists", f"{found.path} is a file")
            return found
        parent, name = split_path(await self.place(path, 1))
        made = await self.make_folders(parent, [name])
        return await self.found(made)

    async def move(self, src: str, dst: str) -> Entry:
        """A move through the Drive's API (the app, ``la``), never a
        command's save: a memo it moves stays where it lands, not taken back
        by a command that renamed it aside (``aside.forget``)."""
        found = await self.found(src)
        moved = await self._move(found, dst)
        if found.is_memo and moved.path != found.path:
            await aside.forget(self, found.entry_id)
        return moved

    async def _move(self, found: Entry, dst: str) -> Entry:
        src = found.path
        if dst == src:
            return found
        if found.kind == "folder" and path_key(dst).startswith(path_key(src) + "/"):
            raise DriveError("invalid_path", "A folder cannot move into itself")
        taken = await self.live(dst)
        if taken is not None and taken.entry_id != found.entry_id:
            raise DriveError("exists", f"{taken.path} already exists")
        dst = await self.place(dst, 0)
        if dst == src:
            return found
        if found.kind == "folder":
            deepest = await self.one(
                "SELECT COALESCE(max(octet_length(path)), 0) AS bytes,"
                " COALESCE(max(array_length(string_to_array(path, '/'), 1)), 0) AS depth"
                " FROM drive_entries WHERE space_id = %(space)s AND deleted_at IS NULL"
                " AND starts_with(path, %(src)s || '/')",
                {"space": self.space_id, "src": src},
            )
            if deepest["bytes"] and (
                deepest["bytes"] - path_bytes(src) + path_bytes(dst) > MAX_PATH_BYTES
                or deepest["depth"] - path_depth(src) + path_depth(dst) > MAX_DEPTH
            ):
                raise DriveError(
                    "invalid_path", "A path under the folder would grow past the limit"
                )
        await self.relocate(found.entry_id, src, dst)
        return await self.entry(found.entry_id)

    async def rename(self, src: str, dst: str, actor: Actor, *, replace: bool) -> Renamed:
        """A rename on a computer: onto a free name, or with ``replace`` over
        a file, as rename(2) does (``exists`` without it). Over a file,
        ``dst`` keeps its id and history and takes ``src``'s content as a new
        version by ``actor``, and ``src`` ends as a tombstone, reported by
        ``changes`` as deleted, never in the trash, purged after a day: its
        bytes live on as ``dst``'s version, so keeping it would only leave
        every editor's temp file in the person's trash. This is how a
        program's save by rename (an editor, ``sed -i``) lands. A source with
        earlier versions or a memo's record goes to the trash instead, where
        the person can restore it. A memo moved onto an ordinary file keeps
        its identity at ``dst``, and the file there goes to the trash. What
        lands at a name a memo was renamed away from by the same command
        takes the memo back (``aside``). Raises ``is_folder`` over a folder
        and ``not_folder`` for a folder source over a file."""
        found, taken = await self.found(src), await self.live(dst)
        if taken is not None and taken.entry_id != found.entry_id and not replace:
            raise DriveError("exists", f"{taken.path} already exists")
        # Only a file is a save by rename; a folder moves into the free name.
        if (
            taken is None
            and found.kind == "file"
            and await aside.reclaim(self, dst, actor, renaming=found.entry_id)
        ):
            found, taken = await self.found(src), await self.live(dst)
        if taken is None or taken.entry_id == found.entry_id:
            return Renamed(await self._carry(found, dst, actor), None, carried=True)
        if found.kind != "file":
            # As rename(2) answers a folder moved over a file.
            raise DriveError("not_folder", f"{taken.path} is a file, not a folder")
        if taken.kind != "file":
            raise DriveError("is_folder", f"{taken.path} is a folder")
        if aside.keeps_source(found, taken):
            # A memo moved onto an ordinary file is still that memo; what it
            # replaced goes to the trash, as an overwrite would lose it.
            await trash.end(self, taken, actor, await trash.item_for(self, actor))
            return Renamed(await self._carry(found, dst, actor), taken.entry_id, carried=True)
        head = await self.stored(found.entry_id, found.head_seq)
        assert head is not None  # a live file's head version always exists
        target = await self.write(taken, taken.path, head, actor)
        # The head lives on as the target's version; earlier versions of the
        # source, or its record as a memo, would end with a notice nobody
        # can restore, so a source with either goes to the trash instead,
        # where it can be.
        kept = found.has_record or await self.one(
            "SELECT 1 AS n FROM drive_versions WHERE entry_id = %(id)s AND seq <> %(head)s LIMIT 1",
            {"id": found.entry_id, "head": found.head_seq},
        )
        trash_id = await trash.item_for(self, actor) if kept else None
        await trash.end(self, found, actor, trash_id)
        return Renamed(
            target, found.entry_id, carried=False, trashed_memo=trash_id if found.is_memo else None
        )

    async def _carry(self, found: Entry, dst: str, actor: Actor) -> Entry:
        """``found`` moved to ``dst`` as itself, noted for ``aside`` when a
        memo left its name."""
        moved = await self._move(found, dst)
        if moved.path != found.path:
            await aside.note(self, found, moved.path, actor)
        return moved

    async def restore_version(
        self, entry_id: str, seq: int, actor: Actor, note: str | None = None
    ) -> Entry:
        found = await self.file(entry_id)
        version = await self.stored(found.entry_id, seq)
        if version is None:
            raise DriveError("not_found", f"{found.path} has no version {seq}")
        return await self.write(
            found,
            found.path,
            version,
            dataclasses.replace(actor, sitting=None),
            note=note,
            restored_from=seq,
        )

    async def undo(
        self, entry_id: str, actor: Actor, if_match: str | None, note: str | None
    ) -> Entry:
        """Put back the state before the last save (``UNDO_TARGET_SQL``) as
        a new version. The save undone is then the version below the head,
        so undoing again redoes it. A file not a live memo can be undone
        only within its window: past it, or after an un-mark, the version
        below may be a former memo's record that outlives the window, which
        is not a copy the file still offers to go back to."""
        found = await self.file(entry_id)
        refuse_conflict(found, if_match, False)
        # On the database's clock, as the sweep's ``expire``: a window past
        # its end is over even before the sweep clears it.
        below = await self.one(
            f"SELECT {UNDO_TARGET_SQL} AS seq FROM drive_entries e WHERE e.entry_id = %(id)s"
            " AND (%(memo)s OR e.undo_until > now())",
            {"id": found.entry_id, "memo": found.is_memo},
        )
        if below is None or below["seq"] is None:
            raise DriveError("not_found", "No earlier copy to go back to")
        return await self.restore_version(found.entry_id, below["seq"], actor, note)



async def trial(space_id: str, limits: Limits, change: Callable[[Tx], Awaitable[Any]]) -> None:
    """Run ``change`` and every check its commit makes, then roll it back:
    raises what the commit would. A caller about to move a file's bytes asks
    this first, so it learns the commit's answer from the commit's code."""
    try:
        async with locked(space_id, limits, trial=True) as tx:
            await change(tx)
    except _Trial:
        pass


@asynccontextmanager
async def locked(space_id: str, limits: Limits, *, trial: bool = False) -> AsyncIterator[Tx]:
    """A change to the space: refused at commit if it took counted bytes
    past the allowance (``retention.settle``)."""
    async with _locked(space_id, limits, trial=trial, allowance=True) as tx:
        yield tx


@asynccontextmanager
async def upkeep(space_id: str, limits: Limits) -> AsyncIterator[Tx]:
    """The sweep's work on a space (a recount, purges, trims, frees): none
    of it adds bytes, so none of it is ever refused for room, whatever the
    counts say once it is done; the trash's bound still applies."""
    async with _locked(space_id, limits, trial=False, allowance=False) as tx:
        yield tx


@asynccontextmanager
async def _locked(
    space_id: str, limits: Limits, *, trial: bool, allowance: bool
) -> AsyncIterator[Tx]:
    async with get_db_connection() as conn, conn.transaction():
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                "SELECT 1 FROM drive_spaces WHERE space_id = %s FOR UPDATE", (space_id,)
            )
            if await cur.fetchone() is None:
                raise DriveError("not_found", "No such space")
            tx = Tx(cur, space_id, limits)
            live_before = await retention.opening(tx) if allowance else None
            try:
                yield tx
                await tx._read_heads()
                # The allowance and the trash's bound are checked once, on what
                # the whole change left, whatever path it took there.
                seen = await retention.settle(tx, live_before, allowance=allowance)
                await trash.bound(tx, seen)
                if trial:
                    raise _Trial()
            except psycopg.errors.UniqueViolation:
                # The lock makes this unreachable; kept as the answer it would be.
                raise DriveError("exists", "Something already has that name") from None
