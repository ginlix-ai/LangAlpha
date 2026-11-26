"""The agent's changes to memos (``drive_memo_changes``): a check-out by one
chat of one memo, whose reason every version made under it carries as its
note. ``policy`` says when a save needs one and when one is open; this reads
and writes the rows, under the space's lock (``writes.Tx``).

The reason lives on the change and is copied into each version's note, so
every reader of a version (the record, the versions list, the entry's head)
reads it as before; ``_reword`` is the one writer of that copy.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

from src.server.database.drive import policy
from src.server.database.drive.memos import changed
from src.server.database.drive.reads import fetch
from src.server.database.drive.records import Actor, DriveError, Entry, MemoChange
from src.server.database.pool import get_db_connection
from src.server.database.runs.thread_activity import thread_busy

if TYPE_CHECKING:
    from src.server.database.drive.writes import Tx


@dataclass(frozen=True)
class Checked:
    """A check-out or check-in as the routes answer it."""

    change: MemoChange
    entry: Entry
    continued: bool = False
    #: The head a check-out adopted: a save through the mount it explains.
    adopted_seq: int | None = None


_CHANGE_COLUMNS = (
    "c.change_id::text AS change_id, c.entry_id::text AS entry_id, c.thread_id,"
    " c.computer_id, c.reason, c.opened_at, c.touched_at, c.ended_at,"
    " n.versions, n.first_seq, n.last_seq"
)
_COUNTED = (
    " CROSS JOIN LATERAL (SELECT count(*)::int AS versions, min(v.seq) AS first_seq,"
    " max(v.seq) AS last_seq FROM drive_versions v WHERE v.change_id = c.change_id) n"
)
#: The chat's changes to a memo: by thread, and with no thread by computer.
_CHAT = (
    "c.entry_id = %(entry)s AND c.user_id = %(user)s"
    " AND c.thread_id IS NOT DISTINCT FROM %(thread)s"
    " AND (%(thread)s::text IS NOT NULL OR c.computer_id IS NOT DISTINCT FROM %(computer)s)"
)


def _chat(entry_id: str, actor: Actor) -> dict[str, Any]:
    return {
        "entry": entry_id,
        "user": actor.user_id,
        "thread": actor.thread_id,
        "computer": actor.computer_id,
    }


async def latest(entry_id: str, actor: Actor, conn: Any = None) -> MemoChange | None:
    """The chat's newest change to the memo, open or not, read on ``conn``
    or a pooled connection."""
    rows = await fetch(
        f"SELECT {_CHANGE_COLUMNS} FROM drive_memo_changes c{_COUNTED}"
        f" WHERE {_CHAT} ORDER BY c.opened_at DESC, c.change_id DESC LIMIT 1",
        _chat(entry_id, actor),
        conn,
    )
    return MemoChange(**rows[0]) if rows else None


async def _read(tx: Tx, change_id: str) -> MemoChange:
    row = await tx.one(
        f"SELECT {_CHANGE_COLUMNS} FROM drive_memo_changes c{_COUNTED}"
        " WHERE c.change_id = %(id)s",
        {"id": change_id},
    )
    assert row is not None
    return MemoChange(**row)


async def _busy(tx: Tx, thread_id: str | None, since: datetime) -> list[policy.Span] | None:
    if thread_id is None:
        return None
    return await thread_busy(tx.cur.connection, thread_id, since)


async def gone_quiet(found: Entry, actor: Actor) -> bool:
    """Whether ``actor``'s chat went quiet since ``found``'s head was
    written, which is when ``_adopt`` leaves that head. For the line that
    promises an adoption; it reads the chat only for a head older than
    ``QUIET``, which a save that just made its head never is."""
    if datetime.now(UTC) - found.modified_at < policy.QUIET:
        return False
    busy = None
    if actor.thread_id is not None:
        async with get_db_connection() as conn:
            busy = await thread_busy(conn, actor.thread_id, found.modified_at)
    return policy.went_quiet(found.modified_at, busy, datetime.now(UTC))


async def _is_open(tx: Tx, change: MemoChange) -> bool:
    busy = await _busy(tx, change.thread_id, change.touched_at)
    return policy.is_open(change, busy, await tx.now())


async def _start(tx: Tx, found: Entry, actor: Actor, reason: str, ending: MemoChange | None) -> str:
    """A new change, which ends the chat's one before it; its id."""
    if ending is not None and ending.ended_at is None:
        await _end(tx, ending.change_id)
    change_id = str(uuid.uuid4())
    # The clock as it reads under the space's lock, not the transaction's
    # start before it: of two check-outs at once, the one that got the lock
    # last is the chat's newest.
    await tx.cur.execute(
        "INSERT INTO drive_memo_changes (change_id, entry_id, space_id, user_id, thread_id,"
        " computer_id, reason, opened_at, touched_at)"
        " VALUES (%s, %s, %s, %s, %s, %s, %s, clock_timestamp(), clock_timestamp())",
        (
            change_id,
            found.entry_id,
            tx.space_id,
            actor.user_id,
            actor.thread_id,
            actor.computer_id,
            reason,
        ),
    )
    return change_id


async def _end(tx: Tx, change_id: str) -> None:
    await tx.cur.execute(
        "UPDATE drive_memo_changes SET ended_at = COALESCE(ended_at, now()) WHERE change_id = %s",
        (change_id,),
    )


async def _touch(tx: Tx, change_id: str, *, reopen: bool = False) -> str:
    await tx.cur.execute(
        "UPDATE drive_memo_changes SET touched_at = now()"
        + (", ended_at = NULL" if reopen else "")
        + " WHERE change_id = %s",
        (change_id,),
    )
    return change_id


async def for_save(
    tx: Tx, found: Entry, actor: Actor, note: str | None
) -> tuple[str | None, str | None]:
    """The change ``actor``'s save to ``found`` is recorded under and the
    version's note, starting or touching that change; refused
    ``checkout_required`` per ``policy.under``."""
    if not policy.owes_checkout(found, actor):
        return None, note
    current = await latest(found.entry_id, actor, tx.cur.connection)
    open_ = current is not None and await _is_open(tx, current)
    match policy.under(found, actor, note, current, open_):
        case policy.Start(reason):
            return await _start(tx, found, actor, reason, current), reason
        case policy.Join(change_id, reason):
            return await _touch(tx, change_id), reason
        case policy.Unreasoned():
            return None, None


def _a_memo(found: Entry) -> None:
    if not found.is_memo:
        raise DriveError(
            "not_memo", f"{found.path} is not a memo: edit it directly, it needs no check-out."
        )


async def _adopt(tx: Tx, found: Entry, actor: Actor, change_id: str, reason: str) -> int | None:
    """Put the head under the change when this chat's agent saved it through
    the mount, after the file was last marked a memo, with no change and no
    reason, and the chat has not gone quiet since: the save whose result
    asked for this check-out, never an older one this reason was not written
    for. Its seq, or None."""
    made, memo = found.modified_by, found.memo
    if made is None or not policy.unreasoned_mount_head(found) or not policy.same_chat(made, actor):
        return None
    assert found.head_seq is not None and memo is not None
    if found.head_seq <= memo.kept_through_seq:
        return None
    busy = await _busy(tx, actor.thread_id, found.modified_at)
    if policy.went_quiet(found.modified_at, busy, await tx.now()):
        return None
    await tx.cur.execute(
        "UPDATE drive_versions SET change_id = %s, note = %s WHERE entry_id = %s AND seq = %s",
        (change_id, reason, found.entry_id, found.head_seq),
    )
    await changed(tx, found)
    return found.head_seq


async def checkout(tx: Tx, found: Entry, actor: Actor, reason: str | None) -> Checked:
    """Open a change of ``found`` with ``reason``, or with none carry on with
    the chat's newest one, whatever its age, checked in or not."""
    _a_memo(found)
    continued = reason is None
    current = await latest(found.entry_id, actor, tx.cur.connection)
    if reason is not None:
        change_id = await _start(tx, found, actor, reason, current)
    elif current is None:
        raise DriveError("not_found", policy.nothing_to_continue(found.path))
    else:
        change_id, reason = await _touch(tx, current.change_id, reopen=True), current.reason
    adopted = await _adopt(tx, found, actor, change_id, reason)
    return Checked(
        await _read(tx, change_id),
        await tx.entry(found.entry_id),
        continued=continued,
        adopted_seq=adopted,
    )


async def checkin(tx: Tx, found: Entry, actor: Actor, reason: str | None) -> Checked:
    """End the chat's newest change of ``found``, rewording it and every
    version under it when ``reason`` is given."""
    _a_memo(found)
    current = await latest(found.entry_id, actor, tx.cur.connection)
    if current is None:
        raise DriveError("not_found", policy.nothing_to_check_in(found.path))
    if reason is not None and reason != current.reason:
        await _reword(tx, found, current, reason)
    await _end(tx, current.change_id)
    return Checked(await _read(tx, current.change_id), await tx.entry(found.entry_id))


async def _reword(tx: Tx, found: Entry, change: MemoChange, reason: str) -> None:
    await tx.cur.execute(
        "UPDATE drive_memo_changes SET reason = %s WHERE change_id = %s",
        (reason, change.change_id),
    )
    await tx.cur.execute(
        "UPDATE drive_versions SET note = %s WHERE change_id = %s", (reason, change.change_id)
    )
    # Under an earlier version too: a client reads the record again when
    # the entry moves in the feed.
    await changed(tx, found)
