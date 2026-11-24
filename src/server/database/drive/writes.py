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
import uuid
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import Any, Literal

import psycopg
from psycopg.rows import dict_row

from src.server.database.drive import reads, retention
from src.server.database.drive.keys import (
    MAX_DEPTH,
    MAX_PATH_BYTES,
    MAX_SEGMENT_BYTES,
    check,
    path_key,
)
from src.server.database.drive.records import (
    ENTRY_COLUMNS,
    ENTRY_FROM,
    EXPIRED_SQL,
    TOMBSTONE_HOURS,
    Actor,
    DriveError,
    Entry,
    Limits,
    entry_from_row,
)
from src.server.database.pool import get_db_connection

#: How long after one delete another by the same actor in the same sitting
#: joins its trash item.
TRASH_BURST_SECONDS = 60


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


class _Trial(Exception):
    """Rolls back a change tried only to learn whether it would commit."""


def _split(path: str) -> tuple[str, str]:
    parent, _, name = path.rpartition("/")
    return parent, name


def _depth(path: str) -> int:
    return path.count("/") + 1


def _bytes(path: str) -> int:
    return len(path.encode("utf-8"))


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


def same_sitting(head: Actor | None, actor: Actor) -> bool:
    """Whether a save folds into the head version. A restored version is
    recorded with no sitting, so nothing ever folds into it."""
    return (
        head is not None
        and actor.sitting is not None
        and head.sitting == actor.sitting
        and head.kind == actor.kind
        and head.user_id == actor.user_id
        and head.agent == actor.agent
    )


class Tx:
    """One locked transaction on a space, and the changes made in it."""

    def __init__(self, cur: Any, space_id: str, limits: Limits) -> None:
        self.cur = cur
        self.space_id = space_id
        self.limits = limits
        self._seq: int | None = None

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

    async def entry(self, entry_id: str) -> Entry:
        row = await self.one(
            f"SELECT {ENTRY_COLUMNS} FROM {ENTRY_FROM} WHERE e.entry_id = %(id)s",
            {"id": entry_id},
        )
        assert row is not None
        return entry_from_row(row)

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
        name = _split(path)[1]
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
        parent, name = _split(new)
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

    async def end(self, entry: Entry, actor: Actor, trash_id: str | None) -> None:
        """Delete ``entry`` and everything under it: into the trash item
        ``trash_id``, or with none as a tombstone, kept only as the notice
        of its end."""
        await self.cur.execute(
            "UPDATE drive_entries SET deleted_at = now(), ended = %(ended)s,"
            " trash_id = %(trash)s, deleted_by = %(by)s, trash_sitting = %(sitting)s,"
            " changed_seq = %(seq)s"
            " WHERE space_id = %(space)s AND deleted_at IS NULL"
            " AND (entry_id = %(id)s OR starts_with(path, %(path)s || '/'))",
            {
                "ended": "tombstone" if trash_id is None else "trash",
                "trash": trash_id,
                "by": actor.user_id,
                "sitting": actor.sitting,
                "seq": await self.seq(),
                "space": self.space_id,
                "id": entry.entry_id,
                "path": entry.path,
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
    ) -> None:
        await self.cur.execute(
            "INSERT INTO drive_versions (entry_id, space_id, seq, sha256, byte_len,"
            " content_type, is_text, actor_kind, actor_user_id, agent, via, thread_id,"
            " run_id, workspace_id, computer_id, sitting, note, restored_from)"
            " VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
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
        seq = await self.seq()
        if found is None:
            path = await self.place(path, 1)
            parent, name = _split(path)
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
            await self.record_version(row["id"], 1, content, actor, note, restored_from)
            return await self.entry(row["id"])
        if same_sitting(found.modified_by, actor):
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
            await self.record_version(found.entry_id, version, content, actor, note, restored_from)
        await self.cur.execute(
            "UPDATE drive_entries SET head_seq = %s, modified_at = now(), changed_seq = %s"
            " WHERE entry_id = %s",
            (version, seq, found.entry_id),
        )
        if version != found.head_seq:
            await retention.trim(self, found.entry_id)
        return await self.entry(found.entry_id)

    async def _found(self, path: str) -> Entry:
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
        parent, name = _split(await self.place(path, 1))
        made = await self.make_folders(parent, [name])
        return await self._found(made)

    async def move(self, src: str, dst: str) -> Entry:
        return await self._move(await self._found(src), dst)

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
                deepest["bytes"] - _bytes(src) + _bytes(dst) > MAX_PATH_BYTES
                or deepest["depth"] - _depth(src) + _depth(dst) > MAX_DEPTH
            ):
                raise DriveError(
                    "invalid_path", "A path under the folder would grow past the limit"
                )
        await self.relocate(found.entry_id, src, dst)
        return await self.entry(found.entry_id)

    async def replace(self, src: str, dst: str, actor: Actor) -> tuple[Entry, str | None]:
        found = await self._found(src)
        taken = await self.live(dst)
        if taken is None or taken.entry_id == found.entry_id:
            return await self._move(found, dst), None
        if found.kind != "file":
            # As rename(2) answers a folder moved over a file.
            raise DriveError("not_folder", f"{taken.path} is a file, not a folder")
        if taken.kind != "file":
            raise DriveError("is_folder", f"{taken.path} is a folder")
        head = await self.stored(found.entry_id, found.head_seq)
        assert head is not None  # a live file's head version always exists
        target = await self.write(taken, taken.path, head, actor)
        # The head lives on as the target's version; earlier versions of the
        # source would end with a notice nobody can restore, so a source
        # with any goes to the trash instead, where it can be.
        history = await self.one(
            "SELECT 1 AS n FROM drive_versions WHERE entry_id = %(id)s AND seq <> %(head)s LIMIT 1",
            {"id": found.entry_id, "head": found.head_seq},
        )
        await self.end(found, actor, None if history is None else await self._trash_id(actor))
        return target, found.entry_id

    async def trash(
        self,
        path: str,
        actor: Actor,
        kind: Literal["file", "folder"] | None,
        empty: bool,
        if_match: str | None = None,
    ) -> str:
        found = await self._found(path)
        if if_match is not None and (found.kind != "file" or found.sha256 != if_match):
            # A delete of the version the caller saw, not of whatever is there now.
            raise DriveError("changed", "The file changed since that version was read")
        if kind == "file" and found.kind != "file":
            raise DriveError("is_folder", f"{found.path} is a folder")
        if kind == "folder" and found.kind != "folder":
            raise DriveError("not_folder", f"{found.path} is a file, not a folder")
        if empty and found.kind == "folder":
            inside = await self.one(
                "SELECT 1 AS n FROM drive_entries WHERE space_id = %(space)s"
                " AND deleted_at IS NULL AND parent = %(path)s LIMIT 1",
                {"space": self.space_id, "path": found.path},
            )
            if inside is not None:
                raise DriveError("not_empty", f"{found.path} is not empty")
        trash_id = await self._trash_id(actor)
        await self.end(found, actor, trash_id)
        return trash_id

    async def _trash_id(self, actor: Actor) -> str:
        """``rm -r`` on a computer arrives one call per file: a delete joins
        the item the same actor's last one in the same sitting made, if that
        was within TRASH_BURST_SECONDS, so the person finds one item. One
        descent of ``ix_drive_entries_burst``, however long the burst."""
        recent = None
        if actor.sitting is not None:
            recent = await self.one(
                "SELECT trash_id::text AS trash_id FROM drive_entries"
                " WHERE space_id = %(space)s AND ended = 'trash'"
                " AND trash_sitting = %(sitting)s AND deleted_by = %(by)s"
                " AND deleted_at > now() - make_interval(secs => %(burst)s)"
                " ORDER BY deleted_at DESC LIMIT 1",
                {
                    "space": self.space_id,
                    "sitting": actor.sitting,
                    "by": actor.user_id,
                    "burst": TRASH_BURST_SECONDS,
                },
            )
        return recent["trash_id"] if recent is not None else str(uuid.uuid4())

    async def restore(self, trash_id: str) -> list[Entry]:
        rows = await self.all(
            "SELECT entry_id::text AS entry_id, path, kind, changed_seq FROM drive_entries"
            " WHERE space_id = %(space)s AND trash_id = %(trash)s",
            {"space": self.space_id, "trash": trash_id},
        )
        if not rows:
            raise DriveError("not_found", "Nothing in the trash under that id")
        # Where each folder this restore brought back landed, by its old key:
        # a later delete of the same burst under it (``rm -r`` deletes the
        # children first) follows it there, not into a folder made since.
        landed: dict[str, str] = {}
        for top, members in _roots(rows):
            old = top["path"]
            below = max(_bytes(m["path"]) for m in members) - _bytes(old)
            wanted = await self.place(_under_landed(old, landed), 0, checked=False)
            target, n = wanted, 0
            while _bytes(target) + below > MAX_PATH_BYTES or await self.live(target) is not None:
                n += 1
                if n > 1000:
                    raise DriveError("exists", f"No free name beside {wanted}")
                target = beside(wanted, n, folder=top["kind"] == "folder", below=below)
            ids = [m["entry_id"] for m in members]
            await self.relocate(top["entry_id"], old, target, revive=ids)
            if top["kind"] == "folder" and target != old:
                landed[path_key(old)] = target
        restored = await self.all(
            f"SELECT {ENTRY_COLUMNS} FROM {ENTRY_FROM}"
            " WHERE e.entry_id = ANY(%(ids)s::uuid[]) ORDER BY e.path",
            {"ids": [r["entry_id"] for r in rows]},
        )
        return [entry_from_row(r) for r in restored]

    async def restore_version(self, entry_id: str, seq: int, actor: Actor) -> Entry:
        found = await self.live(entry_id=entry_id)
        if found is None:
            raise DriveError("not_found", "No such entry")
        if found.kind != "file":
            raise DriveError("is_folder", f"{found.path} is a folder")
        version = await self.stored(entry_id, seq)
        if version is None:
            raise DriveError("not_found", f"{found.path} has no version {seq}")
        return await self.write(
            found,
            found.path,
            version,
            dataclasses.replace(actor, sitting=None),
            restored_from=seq,
        )

    async def purge(
        self, trash_id: str | None, older_than_days: int | None, *, entry_id: str | None = None
    ) -> int:
        """Delete for good: one trash item (``trash_id``), what is past its
        days (``older_than_days``), the deleted entry ``entry_id`` (a
        replaced file's notice), or with none the whole trash."""
        params: dict[str, Any] = {"space": self.space_id}
        where = "e.space_id = %(space)s AND "
        if entry_id is not None and trash_id is None:
            where += "e.entry_id = %(entry)s::uuid AND e.deleted_at IS NOT NULL"
            params["entry"] = entry_id
        elif older_than_days is not None:
            where += EXPIRED_SQL
            params.update(days=older_than_days, hours=TOMBSTONE_HOURS)
        elif trash_id is not None:
            where += "e.trash_id = %(trash)s"
            params["trash"] = trash_id
        else:
            where += "e.ended = 'trash'"
        shas = await self.all(
            "SELECT DISTINCT v.sha256 FROM drive_versions v"
            f" JOIN drive_entries e ON e.entry_id = v.entry_id WHERE {where}",
            params,
        )
        gone = await self.one(
            f"WITH gone AS (DELETE FROM drive_entries e WHERE {where}"
            " RETURNING e.changed_seq)"
            " SELECT count(*) AS n, max(changed_seq) AS top FROM gone",
            params,
        )
        if not gone["n"]:
            if trash_id is not None:
                raise DriveError("not_found", "Nothing in the trash under that id")
            return 0
        await self.cur.execute(
            "UPDATE drive_spaces SET purged_seq = GREATEST(purged_seq, %s)"
            " WHERE space_id = %s",
            (gone["top"], self.space_id),
        )
        # The trash and its bytes changed with no entry to show it: a change
        # number of its own moves every cursor, and a feed answers it empty.
        await self.seq()
        await retention.release(self, [r["sha256"] for r in shas])
        return gone["n"]


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
                # The allowance and the trash's bound are checked once, on what
                # the whole change left, whatever path it took there.
                await retention.settle(tx, live_before, allowance=allowance)
                if trial:
                    raise _Trial()
            except psycopg.errors.UniqueViolation:
                # The lock makes this unreachable; kept as the answer it would be.
                raise DriveError("exists", "Something already has that name") from None


def _cut(text: str, budget: int) -> str:
    """``text`` cut to at most ``budget`` bytes of UTF-8, on a character."""
    return text.encode("utf-8")[: max(budget, 0)].decode("utf-8", "ignore").rstrip(" ")


def beside(path: str, n: int, *, folder: bool, below: int = 0) -> str:
    """``path`` renamed ``name (n).ext`` (a folder: ``name (n)``), its stem
    shortened so the name and, with ``below`` more bytes under it, every
    path under it keep the limits.

    Raises:
        DriveError: ``invalid_path``, when no stem fits.
    """
    parent, name = _split(path)
    stem, dot, ext = name.rpartition(".")
    if folder or not stem or not dot:
        stem, dot, ext = name, "", ""
    suffix = f" ({n}){dot}{ext}"
    room = MAX_PATH_BYTES - below - (_bytes(parent) + 1 if parent else 0)
    budget = min(MAX_SEGMENT_BYTES, room) - _bytes(suffix)
    short = _cut(stem, budget)
    if not short:
        raise DriveError(
            "invalid_path",
            f"{path} cannot be restored beside what now has its name; "
            "move or rename that first",
        )
    named = f"{short}{suffix}"
    out = f"{parent}/{named}" if parent else named
    check(out)
    return out


def _under_landed(path: str, landed: dict[str, str]) -> str:
    """``path`` under where its nearest restored ancestor landed; as it is
    when none moved."""
    segments = path.split("/")
    for depth in range(len(segments) - 1, 0, -1):
        to = landed.get(path_key("/".join(segments[:depth])))
        if to is not None:
            return "/".join([to, *segments[depth:]])
    return path


def _roots(rows: list[dict[str, Any]]) -> list[tuple[dict[str, Any], list[dict[str, Any]]]]:
    """A trash item's deletes, each its top entry with the rows deleted with
    it, shallowest first: one delete has one, a burst of deletes may have
    many. Grouped by the delete (its change number), never by path: a burst
    may hold two generations of one path (a file deleted, made again and
    deleted again), which were never live at once and so cannot come back
    as one; the later is put beside the earlier."""
    deletes: dict[int, list[dict[str, Any]]] = {}
    for row in rows:
        deletes.setdefault(row["changed_seq"], []).append(row)
    groups = []
    for members in deletes.values():
        top = min(members, key=lambda r: (_depth(r["path"]), r["path"]))
        groups.append((top, members))
    return sorted(groups, key=lambda g: (_depth(g[0]["path"]), g[0]["path"], g[0]["changed_seq"]))
