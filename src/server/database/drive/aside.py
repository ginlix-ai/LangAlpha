"""The record stays with the name: a memo a shell command renames aside and
then writes again at its name (``sed -i.bak``, ``perl -i.bak``, an editor
that keeps a backup) is still the memo at that name, and what the command
left at the other name is an ordinary file holding the bytes the memo had.

Only for changes through the mount, which name their command
(``Actor.command``: the filed call, else the process session). A rename of
a memo through the mount is noted on its mark (``drive_memos.aside_*``),
under the space's lock, so the rename and the write may land on different
workers. The next file that lands at the old name in the same command within
``WINDOW_S`` takes the memo back (``reclaim``) and clears the note, when the
memo then sits where a backup would (``_beside``). Moved anywhere else it was
moved on purpose (``mv q3.md archive/q3-final.md && cp template.md q3.md``):
it keeps its record under the new name, as does a memo whose note nothing
takes up.
"""

from __future__ import annotations

import shlex
from typing import TYPE_CHECKING

from src.server.database.drive.keys import path_key, split_path
from src.server.database.drive.records import Actor, Entry

if TYPE_CHECKING:
    from src.server.database.drive.writes import Tx

#: How long after a rename a file written at the old name takes the memo
#: back: longer than a mount's save may trail the command that wrote it.
WINDOW_S = 600


def _named(actor: Actor) -> bool:
    return actor.via == "mount" and actor.command is not None


def _beside(moved: str, left: str) -> bool:
    """Whether ``moved`` reads as a backup of ``left``: in its folder, under
    a name that holds its whole name (``q3.md.bak``, ``q3.md~``,
    ``orig_q3.md``), not one that only ends in it (``data.md`` for ``a.md``)."""
    (folder, name), (at, was) = split_path(path_key(moved)), split_path(path_key(left))
    if folder != at or name == was:
        return False
    starts = [i for i in range(len(name) - len(was) + 1) if name.startswith(was, i)]
    return any(i == 0 or not name[i - 1].isalnum() for i in starts)


def keeps_source(found: Entry, taken: Entry) -> bool:
    """A rename of ``found`` over the file ``taken``: whether ``found``
    keeps its identity at the name, ``taken`` going to the trash. Only for a
    memo moved onto an ordinary file, which a person would still call that
    memo; otherwise the destination keeps its own (a memo stays the memo it
    was, an ordinary file's save by rename lands as its next version)."""
    return found.kind == taken.kind == "file" and found.is_memo and not taken.is_memo


async def note(tx: Tx, found: Entry, moved_to: str, actor: Actor) -> None:
    """``found``, a memo, was renamed away from its path to ``moved_to`` by
    ``actor``'s command. A backup renamed again by that command is still
    away from the name it first left; a memo the command had moved on
    purpose is away from the name it was moved to (``mv q3.md archive/ &&
    sed -i.bak archive/q3.md``). Back at the name it first left, it is away
    from nothing, so what the command later writes at the other name is
    not taken for the memo. Renamed by anything but a named command, it
    was moved on purpose, so nothing later takes it back."""
    if not found.is_memo:
        return
    if not _named(actor):
        await forget(tx, found.entry_id)
        return
    noted = await tx.one(
        "SELECT aside_key FROM drive_memos WHERE entry_id = %(id)s"
        " AND aside_command = %(command)s"
        " AND aside_at > now() - make_interval(secs => %(window)s)",
        {"id": found.entry_id, "command": actor.command, "window": WINDOW_S},
    )
    first = noted["aside_key"] if noted is not None else None
    if first is not None and path_key(moved_to) == first:
        await forget(tx, found.entry_id)
        return
    await tx.cur.execute(
        "UPDATE drive_memos SET aside_key = %(key)s, aside_command = %(command)s,"
        " aside_at = now() WHERE entry_id = %(id)s",
        {
            "key": first if first and _beside(found.path, first) else path_key(found.path),
            "command": actor.command,
            "id": found.entry_id,
        },
    )


async def forget(tx: Tx, entry_id: str) -> None:
    """End any note on ``entry_id``: wherever it now is, it stays."""
    await tx.cur.execute(
        "UPDATE drive_memos SET aside_key = NULL, aside_command = NULL, aside_at = NULL"
        " WHERE entry_id = %s AND aside_key IS NOT NULL",
        (entry_id,),
    )


async def reclaim(tx: Tx, path: str, actor: Actor, *, renaming: str | None = None) -> bool:
    """Bring back to ``path``, where nothing is now, the memo ``actor``'s
    command renamed aside from it; whether one came back. The name it was
    renamed to keeps an ordinary file with the bytes the memo had then, in
    the trash with the memo's delete when the command removed it there. A
    memo anyone else put in the trash meanwhile stays there. ``renaming``
    is the entry being renamed onto ``path``: the memo itself renamed back
    is a plain move (``Tx.rename``), which needs no file left behind."""
    if not _named(actor):
        return False
    row = await tx.one(
        "SELECT m.entry_id::text AS id FROM drive_memos m"
        " JOIN drive_entries e ON e.entry_id = m.entry_id"
        " WHERE m.space_id = %(space)s AND m.is_memo AND m.aside_key = %(key)s"
        " AND m.aside_command = %(command)s"
        " AND m.aside_at > now() - make_interval(secs => %(window)s)"
        " AND m.entry_id IS DISTINCT FROM %(renaming)s::uuid"
        " AND (e.deleted_at IS NULL OR (e.ended = 'trash' AND e.deleted_by = %(by)s"
        " AND e.trash_sitting = %(sitting)s))"
        " ORDER BY m.aside_at DESC LIMIT 1",
        {
            "space": tx.space_id,
            "key": path_key(path),
            "command": actor.command,
            "window": WINDOW_S,
            "by": actor.user_id,
            "sitting": actor.sitting,
            "renaming": renaming,
        },
    )
    if row is None:
        return False
    memo = await tx.entry(row["id"])
    if not _beside(memo.path, path):
        return False
    await tx.cur.execute(
        "UPDATE drive_memos SET aside_key = NULL, aside_command = NULL, aside_at = NULL"
        " WHERE entry_id = %s",
        (memo.entry_id,),
    )
    head = await tx.stored(memo.entry_id, memo.head_seq)
    assert head is not None
    target = await tx.place(path, 0)
    if memo.deleted_at is None:
        await tx.relocate(memo.entry_id, memo.path, target)
        await tx.write(None, memo.path, head, actor)
        return True
    if await tx.live(memo.path) is None:
        # What the command deleted at the other name was the memo's old
        # bytes under that name: they stay in the trash as that delete, under
        # its change number, which is what a restore tells deletes apart by.
        copy = await tx.write(None, memo.path, head, actor)
        await tx.cur.execute(
            "UPDATE drive_entries c SET deleted_at = m.deleted_at, ended = m.ended,"
            " trash_id = m.trash_id, deleted_by = m.deleted_by, trash_sitting = m.trash_sitting,"
            " changed_seq = m.changed_seq FROM drive_entries m"
            " WHERE c.entry_id = %s AND m.entry_id = %s",
            (copy.entry_id, memo.entry_id),
        )
    await tx.relocate(memo.entry_id, memo.path, target, revive=[memo.entry_id])
    return True


def moved_onto_memo(source: str, destination: str, trash_id: str) -> str:
    """The line a command's result carries for a memo it moved onto another
    memo, which keeps its own record and takes the moved text as a version."""
    return (
        f"{source} was a memo: moving it onto {destination}, also a memo, saved its text as "
        f"that memo's next version and put {source} with its record in the Drive's trash. "
        f"To bring it back: la drive trash restore {shlex.quote(trash_id)}"
    )
