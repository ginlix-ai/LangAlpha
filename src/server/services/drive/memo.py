"""Memos: Drive files marked as ones that keep every version, with a status,
tickers and a record of who changed them and why, the agent's why through a
check-out (``checkout``, ``checkin``), a person's by a note on the save or
afterwards (``explain``). Also a file's one-step undo."""

from __future__ import annotations

import logging
import shlex
from collections.abc import Mapping
from typing import Any

from src.server.database.drive import changes as checkouts
from src.server.database.drive import memos as rows
from src.server.database.drive import policy
from src.server.database.drive.memos import Sort
from src.server.database.drive.records import (
    Actor,
    DriveError,
    Entry,
    MemoPage,
    RecordPage,
    Version,
)
from src.server.database.drive.writes import refuse_conflict
from src.server.database.drive import reads
from src.server.services.drive import changing, note_of, texts

logger = logging.getLogger(__name__)

__all__ = [
    "CHECKOUT_PERSONAL",
    "checkin",
    "checkout",
    "explain",
    "explain_refused",
    "memos",
    "record",
    "set_memo",
    "unchecked",
    "undo",
]

MEMOS_MAX = 500
RECORD_MAX = 200

#: What a person is told when they ask to check out a memo.
CHECKOUT_PERSONAL = (
    "Only an agent checks out a memo. A person's note goes with the save itself."
)


def explain_refused(path: str) -> str:
    """What the agent is told when it asks to say why afterwards."""
    return (
        "A version's reason comes from your check-out: "
        f"la drive checkout {shlex.quote(path)} '<what you are changing and why>'"
    )


def _agent_only(actor: Actor) -> None:
    """A check-out is how the agent says why (``policy.owes_checkout``); a
    person says why with the save."""
    if actor.kind != "agent":
        raise DriveError("forbidden", CHECKOUT_PERSONAL)


async def set_memo(
    space,
    entry_id: str,
    *,
    actor: Actor,
    change: Mapping[str, Any],
    note: str | None = None,
    if_match: str | None = None,
) -> Entry:
    """Mark a file a memo (``memo: true``), un-mark it (``false``), or change
    its ``status`` or ``tickers``: ``change`` holds only the keys sent. The
    file as it then stands. One call is one event in its record; a call
    that changes nothing records nothing. ``if_match`` is the head the
    caller saw: final pins the head, which must be the one they meant."""
    change = rows.checked(change)
    async with changing(space) as tx:
        found = await tx.file(entry_id)
        refuse_conflict(found, if_match, False)
        return await rows.set_mark(tx, found, actor, change, note=note_of(note)) or found


async def memos(
    space,
    *,
    sort: Sort = "revised",
    descending: bool = True,
    status: str | None = None,
    ticker: str | None = None,
    limit: int = 100,
    cursor: str | None = None,
) -> MemoPage:
    """The space's live memos, a page at a time, with how many versions
    each keeps; ``status`` ``none`` for those with none."""
    if status not in (None, "draft", "final", "none"):
        raise DriveError("invalid", "A memo's status is draft, final or none")
    return await rows.memo_page(
        space.space_id,
        sort=sort,
        descending=descending,
        status=status,
        ticker=rows.ticker_of(ticker) if ticker else None,
        limit=max(1, min(limit, MEMOS_MAX)),
        cursor=cursor,
    )


async def record(space, entry_id: str, *, before: str | None = None, limit: int = 50) -> RecordPage:
    """A file's record, newest first: its versions and, for a memo or one
    that was, the changes to its mark. An ordinary file shows at most its
    head and its undo copy."""
    entry = await reads.live_file(space.space_id, entry_id)
    page = await rows.record_page(
        entry.entry_id, before=before, limit=max(1, min(limit, RECORD_MAX))
    )
    return await texts.fill_counts(entry, page)


async def undo(
    space,
    entry_id: str,
    *,
    actor: Actor,
    if_match: str | None = None,
    note: str | None = None,
) -> Entry:
    """Put a file back as it was before its last save, as a new version;
    ``if_match``, the head the caller saw."""
    async with changing(space) as tx:
        return await tx.undo(entry_id, actor, if_match, note_of(note))


async def explain(space, entry_id: str, seq: int | None, note: str, *, actor: Actor) -> Version:
    """Say why a version (the head by default) was made, once, as the person
    who made it. The agent says why through its check-out instead."""
    async with changing(space) as tx:
        found = await tx.file(entry_id)
        if actor.kind == "agent":
            raise DriveError("forbidden", explain_refused(found.path))
        said = note_of(note)
        if said is None:
            raise DriveError("invalid", "Say why in a few words")
        return await rows.explain(tx, found, seq, said, actor)


def _reason(reason: str | None) -> str | None:
    if reason is not None and note_of(reason) is None:
        raise DriveError("invalid", "Say what you are changing and why in a few words")
    return note_of(reason)


async def checkout(
    space, entry_id: str, *, actor: Actor, reason: str | None, carry_on: bool
) -> checkouts.Checked:
    """Open a change of a memo with ``reason`` for ``actor``'s chat, or with
    ``carry_on`` reopen its newest one; exactly one of them."""
    _agent_only(actor)
    if (reason is None) == (not carry_on):
        raise DriveError("invalid", "Send a reason, or continue: true")
    reason = _reason(reason)
    async with changing(space) as tx:
        return await checkouts.checkout(tx, await tx.file(entry_id), actor, reason)


async def checkin(
    space, entry_id: str, *, actor: Actor, reason: str | None = None
) -> checkouts.Checked:
    """End ``actor``'s chat's newest change of a memo, with ``reason``
    rewording it for all its versions."""
    _agent_only(actor)
    reason = _reason(reason)
    async with changing(space) as tx:
        return await checkouts.checkin(tx, await tx.file(entry_id), actor, reason)


async def unchecked(saved: Entry, shown: str | None, actor: Actor) -> str | None:
    """The line a command's result carries for its save through the mount
    to a memo under no check-out (``policy.unchecked``); None for any other,
    and when it cannot be read: the save it speaks of has already landed."""
    if not policy.needs_line(saved, shown, actor):
        return None
    try:
        # A save of the same bytes leaves an old head in place, and a
        # check-out takes no head its chat went quiet after.
        if await checkouts.gone_quiet(saved, actor):
            return None
        last = await checkouts.latest(saved.entry_id, actor)
    except Exception:  # noqa: BLE001 - the save stands; only its line is lost
        logger.warning("[Drive] no check-out line for %s", saved.entry_id, exc_info=True)
        return None
    return policy.unchecked(saved, shown, actor, last)
