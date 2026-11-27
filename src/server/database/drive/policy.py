"""What a file keeps of its versions, and when a save is one version.

``HISTORY_SQL`` is the one statement of which versions a file keeps. Each
version takes one role: the ``head``; the memo's pinned ``final``; its
``record`` (every version of a live memo, and of a former memo every
version at or below ``kept_through_seq``, the head when it was un-marked);
the ``undo`` copy, the newest version below the head and above that floor,
while it is smaller than the rule's ``max_bytes``; or ``spare``. The undo
copy stays while a sitting's saves pass back through its bytes, so the
sitting's next save can still be undone to it; Undo itself takes only the
version directly below the head, and only with other bytes
(``UNDO_TARGET_SQL``). A save drops what is spare, the sweep drops
the undo copy once its window ends, a clear drops everything but the head
and the final, and the version listing shows the undo copy's end.
``undo_until``, when that copy goes, is written only in ``retention``
(``window``, ``expire``).

It is also the one statement of when the agent's save to a memo needs a
check-out (``under``), when a check-out is open (``is_open``), and how the
agent is told either (``checkout_refusal``, ``unchecked``).
"""

from __future__ import annotations

import shlex
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from src.server.database.drive.records import Actor, DriveError, Entry, MemoChange

#: ``history``: each version of the files ``%(ids)s`` with its ``role`` and
#: whether it is ``small`` enough to be kept as an undo copy (``%(max)s``,
#: the rule's ``max_bytes``; null keeps every size). Used as a CTE.
HISTORY_SQL = """
    history AS (
        SELECT v.entry_id, v.seq, v.sha256,
               (%(max)s::bigint IS NULL OR v.byte_len < %(max)s::bigint) AS small,
               COALESCE(m.is_memo, false) AS memo,
               CASE
                   WHEN v.seq = e.head_seq THEN 'head'
                   WHEN v.seq = m.final_seq THEN 'final'
                   WHEN COALESCE(m.is_memo, false)
                        OR v.seq <= COALESCE(m.kept_through_seq, 0) THEN 'record'
                   WHEN v.seq = max(v.seq) FILTER (
                            WHERE v.seq <> e.head_seq AND v.seq > COALESCE(m.kept_through_seq, 0))
                            OVER (PARTITION BY v.entry_id)
                        AND (%(max)s::bigint IS NULL OR v.byte_len < %(max)s::bigint) THEN 'undo'
                   ELSE 'spare'
               END AS role
          FROM drive_versions v
          JOIN drive_entries e ON e.entry_id = v.entry_id
          LEFT JOIN drive_memos m ON m.entry_id = v.entry_id
         WHERE v.entry_id = ANY(%(ids)s::uuid[]))
"""

#: The roles a clear drops: every version but the head and a memo's final.
CLEARED = ("record", "undo", "spare")

#: The version an undo of ``e`` (an alias of ``drive_entries``) puts back:
#: the state before the last save, or nothing. A new version takes the
#: head's seq plus one and a save folded into a sitting keeps the head's,
#: so that state is the version at ``head_seq - 1``, whatever its role (the
#: first save after an un-mark is undone to the record below it). Gone
#: (too large, swept, cleared) or holding the head's bytes, there is
#: nothing to undo: an older version, a record or a pinned final, is not
#: what the last save replaced. ``retention.window`` sets ``undo_until`` by
#: whether this exists; Undo puts it back on a live memo, and on any other
#: file only while ``undo_until`` is set (``Tx.undo``), else answers
#: ``not_found``.
UNDO_TARGET_SQL = """(
    SELECT b.seq FROM drive_versions b, drive_versions h
     WHERE b.entry_id = e.entry_id AND b.seq = e.head_seq - 1
       AND h.entry_id = e.entry_id AND h.seq = e.head_seq AND b.sha256 <> h.sha256)"""


def same_sitting(head: Actor | None, actor: Actor) -> bool:
    """Whether ``actor`` saves in the sitting that wrote ``head``. A restored
    version is recorded with no sitting, so nothing ever folds into it."""
    return (
        head is not None
        and actor.sitting is not None
        and head.sitting == actor.sitting
        and head.kind == actor.kind
        and head.user_id == actor.user_id
        and head.agent == actor.agent
    )


def folds(found: Entry, actor: Actor, change_id: str | None) -> bool:
    """Whether a save to ``found`` under the agent's change ``change_id``
    replaces its head rather than making a version: the same sitting and the
    same change, so a new reason in one turn starts a new version, unless the
    head is part of a memo's record that must stay as written: its pinned
    final version, or the version the mark was put on or taken off at."""
    memo = found.memo
    held = memo is not None and found.head_seq is not None and (
        memo.final_seq == found.head_seq or found.head_seq <= memo.kept_through_seq
    )
    return (
        same_sitting(found.modified_by, actor)
        and found.head_change_id == change_id
        and not held
    )


# --- check-outs ---------------------------------------------------------------

#: How long a chat's check-out outlives its last turn, or with no chat its
#: last check-out or save.
QUIET = timedelta(hours=1)


#: When a chat was busy: a turn or a background task, ``(start, end)``,
#: ``end`` None while it runs (``thread_activity.thread_busy``).
Span = tuple[datetime, datetime | None]


@dataclass(frozen=True)
class Start:
    """The note sent with the save starts a change with it as the reason."""

    reason: str


@dataclass(frozen=True)
class Join:
    """The chat's open change, whose reason the version carries."""

    change_id: str
    reason: str


@dataclass(frozen=True)
class Unreasoned:
    """A save through the mount kept with no change and no reason."""


#: Where the agent's save to a memo goes (``under``).
Under = Start | Join | Unreasoned


def owes_checkout(entry: Entry, actor: Actor) -> bool:
    """A change by the agent to a memo, which says why through its check-out."""
    return entry.is_memo and actor.kind == "agent"


def unreasoned_mount_head(entry: Entry) -> bool:
    """The head is a save the agent made through the mount with no change
    and no reason: one whose result asked for a check-out."""
    made = entry.modified_by
    return (
        made is not None
        and entry.head_seq is not None
        and made.kind == "agent"
        and made.via == "mount"
        and entry.head_change_id is None
        and entry.head_note is None
    )


def same_chat(made: Actor, actor: Actor) -> bool:
    """Whether ``made`` and ``actor`` are one chat: by thread, and with no
    thread by computer, as ``changes`` reads a chat's changes."""
    return (
        made.user_id == actor.user_id
        and made.thread_id == actor.thread_id
        and (actor.thread_id is not None or made.computer_id == actor.computer_id)
    )


def went_quiet(since: datetime, busy: list[Span] | None, now: datetime) -> bool:
    """Whether a chat has been quiet for ``QUIET`` at any time from ``since``
    to ``now``, quiet being no turn or background task of it running;
    ``busy`` is its spans from then on. With no chat (``busy``
    None), whether ``QUIET`` has passed."""
    if busy is None:
        return now - since >= QUIET
    quiet_from = since
    for start, end in sorted(busy, key=lambda span: span[0]):
        if start - quiet_from >= QUIET:
            return True
        quiet_from = max(quiet_from, end or now)
    return now - quiet_from >= QUIET


def is_open(change: MemoChange, busy: list[Span] | None, now: datetime) -> bool:
    """A change not checked in is open until its chat goes quiet
    (``went_quiet``) after it was last touched (a check-out, a continue, a
    save under it). The turn a save comes from runs, so a long turn never
    loses its check-out midway."""
    return change.ended_at is None and not went_quiet(change.touched_at, busy, now)


def under(found: Entry, actor: Actor, note: str | None, current: MemoChange | None, open_: bool) -> Under:
    """Where ``actor``'s save to ``found``, which owes a check-out
    (``owes_checkout``), goes, ``current`` being the chat's newest change to
    it: a note sent starts a change; else the open change; else a save
    through the mount is kept with no reason, since refusing it would lose
    the edit on the computer (``unchecked`` asks for the check-out after);
    anything else is refused."""
    if note is not None:
        return Start(note)
    if current is not None and open_:
        return Join(current.change_id, current.reason)
    if actor.via == "mount":
        return Unreasoned()
    last = None
    if current is not None:
        last = {
            "change_id": current.change_id,
            "reason": current.reason,
            "date": current.touched_at.isoformat(),
            "versions": current.versions,
            "checked_in": current.ended_at is not None,
        }
    raise DriveError(
        "checkout_required",
        checkout_refusal(found.path, found.path, last),
        {"path": found.path, "last_change": last},
    )


def _day(at: str, timezone: str | None) -> str:
    """``at`` (ISO) as ``Nov 26`` in ``timezone``, else UTC."""
    when = datetime.fromisoformat(at)
    zone: Any = UTC
    if timezone:
        try:
            zone = ZoneInfo(timezone)
        except (ZoneInfoNotFoundError, ValueError):
            zone = UTC
    when = when.astimezone(zone)
    return f"{when:%b} {when.day}"


def _command(q: str, placeholder: str) -> str:
    """A check-out command with the reason in single quotes, which the shell
    leaves as typed (a ``$`` in it stays)."""
    return f"la drive checkout {q} '<{placeholder}>'"


def checkout_refusal(
    shown: str,
    path: str,
    last: dict[str, Any] | None,
    timezone: str | None = None,
    *,
    bash: bool = True,
) -> str:
    """What the agent is told of a save refused ``checkout_required``, the
    file shown at ``shown``, ``path`` in the Drive, ``last`` the refusal's
    ``last_change``; ``bash`` False for an agent that cannot run ``la``."""
    q = shlex.quote(path)
    if not bash:
        return (
            f"{shown} was not saved: it is a memo, which is checked out with a Bash command "
            "before it is changed, and you have no Bash. Report this to the agent that gave you "
            f"the task: once it runs {_command(q, 'what is being changed and why')}, this edit "
            "goes through; until then a retry is refused."
        )
    new = _command(q, "what you are changing and why")
    if last is None:
        return (
            f"{shown} was not saved: it is a memo, which you check out before changing. "
            f"Run {new}, then make this call again."
        )
    day, reason = _day(last["date"], timezone), last["reason"]
    empty = ", with no change made" if not last.get("versions") else ""
    if last.get("checked_in"):
        return (
            f"{shown} was not saved: it is a memo and you checked in your change of {day}, "
            f'"{reason}"{empty}. To carry on with that change: la drive checkout {q} --continue. '
            f"For a different change: {new}. Then make this call again."
        )
    return (
        f'{shown} was not saved: it is a memo and your check-out of {day}, "{reason}", '
        f"ended after the conversation was quiet for an hour{empty}. "
        f"To carry on with that change: la drive checkout {q} --continue. "
        f"For a different change: {new}. Then make this call again."
    )


def nothing_to_continue(path: str) -> str:
    """What ``checkout --continue`` is told where the conversation has no change of the memo."""
    new = _command(shlex.quote(path), "what you are changing and why")
    return f"This conversation has no earlier change to {path} to carry on with. Start one: {new}"


def nothing_to_check_in(path: str) -> str:
    return f"This conversation has no change to {path} to check in."


def unchecked(saved: Entry, shown: str | None, actor: Actor, last: MemoChange | None) -> str | None:
    """The line a command's result carries for a save through the mount to
    a memo under no check-out, shown to the agent at ``shown``, ``last``
    being the chat's newest change to it. Only for a head ``actor``'s
    sitting made: a command that changed nothing leaves a head someone else
    may have made."""
    if not needs_line(saved, shown, actor):
        return None
    q = shlex.quote(saved.path)
    # Opens with the save being kept, so the line cannot read as a refusal.
    line = (
        f"{shown} was saved. It is a memo and this save has no reason yet: "
        f"{_command(q, 'what changed and why')} gives it one"
    )
    if last is not None:
        line += f', or la drive checkout {q} --continue if it is part of "{last.reason}"'
    return line + "."


def needs_line(saved: Entry, shown: str | None, actor: Actor) -> bool:
    """Whether ``unchecked`` has a line for this save, checked before the
    chat's last change is read for it. Not for a head from before the mark,
    which a save of the same bytes leaves in place: a check-out never takes
    it (``changes._adopt``)."""
    if not (
        shown
        and owes_checkout(saved, actor)
        and actor.via == "mount"
        and unreasoned_mount_head(saved)
    ):
        return False
    made, memo = saved.modified_by, saved.memo
    assert made is not None and memo is not None and saved.head_seq is not None
    return (
        (made.user_id, made.sitting) == (actor.user_id, actor.sitting)
        and saved.head_seq > memo.kept_through_seq
    )
