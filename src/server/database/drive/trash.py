"""The Drive's trash: what a delete ends (``end``), the item it joins
(``item_for``), bringing an item back (``restore``), deleting for good
(``purge``) and keeping what the trash holds under its limit (``bound``).
Each runs inside a locked transaction (``writes.Tx``), as ``retention``'s
functions do.
"""

from __future__ import annotations

import uuid
from typing import TYPE_CHECKING, Any, Literal

from src.server.database.drive import retention
from src.server.database.drive.keys import (
    MAX_PATH_BYTES,
    MAX_SEGMENT_BYTES,
    check,
    path_bytes,
    path_depth,
    path_key,
    split_path,
)
from src.server.database.drive.records import (
    ENTRY_COLUMNS,
    ENTRY_FROM,
    EXPIRED_SQL,
    TOMBSTONE_HOURS,
    Actor,
    DriveError,
    Entry,
    entry_from_row,
)

if TYPE_CHECKING:
    from src.server.database.drive.writes import Tx

#: How long after one delete another by the same actor in the same sitting
#: joins its trash item.
TRASH_BURST_SECONDS = 60


async def end(tx: Tx, entry: Entry, actor: Actor, trash_id: str | None) -> None:
    """Delete ``entry`` and everything under it: into the trash item
    ``trash_id``, or with none as a tombstone, kept only as the notice
    of its end."""
    await tx.cur.execute(
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
            "seq": await tx.seq(),
            "space": tx.space_id,
            "id": entry.entry_id,
            "path": entry.path,
        },
    )


async def trash(
    tx: Tx,
    path: str,
    actor: Actor,
    kind: Literal["file", "folder"] | None,
    empty: bool,
    if_match: str | None = None,
) -> str:
    found = await tx.found(path)
    if if_match is not None and (found.kind != "file" or found.sha256 != if_match):
        # A delete of the version the caller saw, not of whatever is there now.
        raise DriveError("changed", "The file changed since that version was read")
    if kind == "file" and found.kind != "file":
        raise DriveError("is_folder", f"{found.path} is a folder")
    if kind == "folder" and found.kind != "folder":
        raise DriveError("not_folder", f"{found.path} is a file, not a folder")
    if empty and found.kind == "folder":
        inside = await tx.one(
            "SELECT 1 AS n FROM drive_entries WHERE space_id = %(space)s"
            " AND deleted_at IS NULL AND parent = %(path)s LIMIT 1",
            {"space": tx.space_id, "path": found.path},
        )
        if inside is not None:
            raise DriveError("not_empty", f"{found.path} is not empty")
    trash_id = await item_for(tx, actor)
    await end(tx, found, actor, trash_id)
    return trash_id


async def item_for(tx: Tx, actor: Actor) -> str:
    """``rm -r`` on a computer arrives one call per file: a delete joins
    the item the same actor's last one in the same sitting made, if that
    was within TRASH_BURST_SECONDS, so the person finds one item. One
    descent of ``ix_drive_entries_burst``, however long the burst."""
    recent = None
    if actor.sitting is not None:
        recent = await tx.one(
            "SELECT trash_id::text AS trash_id FROM drive_entries"
            " WHERE space_id = %(space)s AND ended = 'trash'"
            " AND trash_sitting = %(sitting)s AND deleted_by = %(by)s"
            " AND deleted_at > now() - make_interval(secs => %(burst)s)"
            " ORDER BY deleted_at DESC LIMIT 1",
            {
                "space": tx.space_id,
                "sitting": actor.sitting,
                "by": actor.user_id,
                "burst": TRASH_BURST_SECONDS,
            },
        )
    return recent["trash_id"] if recent is not None else str(uuid.uuid4())


async def restore(tx: Tx, trash_id: str) -> list[Entry]:
    rows = await tx.all(
        "SELECT entry_id::text AS entry_id, path, kind, changed_seq FROM drive_entries"
        " WHERE space_id = %(space)s AND trash_id = %(trash)s",
        {"space": tx.space_id, "trash": trash_id},
    )
    if not rows:
        raise DriveError("not_found", "Nothing in the trash under that id")
    # Where each folder this restore brought back landed, by its old key:
    # a later delete of the same burst under it (``rm -r`` deletes the
    # children first) follows it there, not into a folder made since.
    landed: dict[str, str] = {}
    for top, members in _roots(rows):
        old = top["path"]
        below = max(path_bytes(m["path"]) for m in members) - path_bytes(old)
        wanted = await tx.place(_under_landed(old, landed), 0, checked=False)
        target, n = wanted, 0
        while path_bytes(target) + below > MAX_PATH_BYTES or await tx.live(target) is not None:
            n += 1
            if n > 1000:
                raise DriveError("exists", f"No free name beside {wanted}")
            target = beside(wanted, n, folder=top["kind"] == "folder", below=below)
        ids = [m["entry_id"] for m in members]
        await tx.relocate(top["entry_id"], old, target, revive=ids)
        if top["kind"] == "folder" and target != old:
            landed[path_key(old)] = target
    restored = await tx.all(
        f"SELECT {ENTRY_COLUMNS} FROM {ENTRY_FROM}"
        " WHERE e.entry_id = ANY(%(ids)s::uuid[]) ORDER BY e.path",
        {"ids": [r["entry_id"] for r in rows]},
    )
    return [entry_from_row(r) for r in restored]


async def purge(
    tx: Tx, trash_id: str | None, older_than_days: int | None, *, entry_id: str | None = None
) -> int:
    """Delete for good: one trash item (``trash_id``), what is past its
    days (``older_than_days``), the deleted entry ``entry_id`` (a
    replaced file's notice), or with none the whole trash."""
    params: dict[str, Any] = {"space": tx.space_id}
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
    shas = await tx.all(
        "SELECT DISTINCT v.sha256 FROM drive_versions v"
        f" JOIN drive_entries e ON e.entry_id = v.entry_id WHERE {where}",
        params,
    )
    gone = await tx.one(
        f"WITH gone AS (DELETE FROM drive_entries e WHERE {where}"
        " RETURNING e.changed_seq)"
        " SELECT count(*) AS n, max(changed_seq) AS top FROM gone",
        params,
    )
    if not gone["n"]:
        if trash_id is not None:
            raise DriveError("not_found", "Nothing in the trash under that id")
        return 0
    await tx.cur.execute(
        "UPDATE drive_spaces SET purged_seq = GREATEST(purged_seq, %s)"
        " WHERE space_id = %s",
        (gone["top"], tx.space_id),
    )
    # The trash and its bytes changed with no entry to show it: a change
    # number of its own moves every cursor, and a feed answers it empty.
    await tx.seq()
    await retention.release(tx, [r["sha256"] for r in shas])
    return gone["n"]


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
    parent, name = split_path(path)
    stem, dot, ext = name.rpartition(".")
    if folder or not stem or not dot:
        stem, dot, ext = name, "", ""
    suffix = f" ({n}){dot}{ext}"
    room = MAX_PATH_BYTES - below - (path_bytes(parent) + 1 if parent else 0)
    budget = min(MAX_SEGMENT_BYTES, room) - path_bytes(suffix)
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
        top = min(members, key=lambda r: (path_depth(r["path"]), r["path"]))
        groups.append((top, members))
    return sorted(groups, key=lambda g: (path_depth(g[0]["path"]), g[0]["path"], g[0]["changed_seq"]))


#: The oldest deleted entry still holding bytes nothing live holds: purging
#: it (its trash item, or the notice of a replaced file) brings those bytes
#: down, which purging one whose bytes a live file shares would not. A memo
#: or former memo comes after every other file: its record is in no other
#: place, so the bound takes it only when nothing else is left to take. So
#: does everything deleted with one, since a purge takes the whole item.
_HOLDING_SQL = """
    SELECT e.entry_id::text AS entry_id, e.trash_id::text AS trash_id
      FROM drive_entries e
     WHERE e.space_id = %(space)s AND e.deleted_at IS NOT NULL
       AND EXISTS (
           SELECT 1 FROM drive_versions v JOIN drive_contents c
               ON c.space_id = v.space_id AND c.sha256 = v.sha256
            WHERE v.entry_id = e.entry_id AND c.live_refs = 0 AND c.released_at IS NULL)
     ORDER BY EXISTS (
                  SELECT 1 FROM drive_entries u JOIN drive_memos m ON m.entry_id = u.entry_id
                   WHERE u.space_id = e.space_id
                     AND (u.entry_id = e.entry_id OR u.trash_id = e.trash_id)),
              e.deleted_at, e.entry_id
     LIMIT 1
"""


async def bound(tx: Tx, seen: dict[str, int] | None = None) -> int:
    """While deleted entries' and released copies' bytes are past
    ``max_trash_bytes``, free released copies first, oldest first, since no
    one can restore them, then purge the oldest deleted entries that hold
    bytes no live file does: both are outside the allowance, so this is what
    keeps them from holding without end. The entries purged."""
    limit = tx.limits.max_trash_bytes
    if limit is None:
        return 0
    seen = seen or await retention.counts(tx)
    purged = 0
    while (over := seen["trash"] + seen["released"] - limit) > 0:
        if seen["released"]:
            if not await retention.free_released(tx, over_bytes=over):
                break
        else:
            oldest = await tx.one(_HOLDING_SQL, {"space": tx.space_id})
            if oldest is None:
                break
            # What it held is released, which the next round frees.
            purged += await purge(tx, oldest["trash_id"], None, entry_id=oldest["entry_id"])
        seen = await retention.counts(tx)
    return purged
