"""Memos: the mark on a file (``drive_memos``), the events that change it
(``drive_memo_events``) and the record that reads both with the versions.

Marking writes under the space's lock (``writes.Tx``); the record and the
list of memos are plain reads, as ``reads`` are.
"""

from __future__ import annotations

import base64
import dataclasses
import json
import re
from collections.abc import Mapping
from datetime import datetime
from typing import TYPE_CHECKING, Any, Literal

from src.server.database.drive import retention
from src.server.database.drive.reads import fetch
from src.server.database.drive.records import (
    ENTRY_COLUMNS,
    ENTRY_FROM,
    EVENT_COLUMNS,
    VERSION_COLUMNS,
    Actor,
    DriveError,
    Entry,
    MemoEvent,
    MemoMark,
    MemoPage,
    RecordPage,
    RecordVersion,
    Version,
    entry_from_row,
    event_from_row,
    version_from_row,
)
from src.server.database.pool import get_db_connection

if TYPE_CHECKING:
    from src.server.database.drive.writes import Tx

Sort = Literal["revised", "name", "path", "status"]
#: What a change to a mark may say; a key left out leaves that part as it is.
CHANGE_KEYS = frozenset({"memo", "status", "tickers"})

TICKERS_MAX = 20
_TICKER = re.compile(r"[A-Z0-9][A-Z0-9.:/-]{0,23}")


def ticker_of(raw: Any) -> str:
    """``raw`` stripped and upper-cased, refused ``invalid`` unless a ticker."""
    ticker = str(raw or "").strip().upper()
    if not _TICKER.fullmatch(ticker):
        raise DriveError("invalid", f"{ticker or 'An empty name'} is not a ticker")
    return ticker


def _tickers(given: Any) -> tuple[str, ...]:
    """Stripped, upper-cased, each once in the order given."""
    if not isinstance(given, (list, tuple)):
        raise DriveError("invalid", "Tickers are a list")
    out: list[str] = []
    for raw in given:
        if not str(raw or "").strip():
            continue
        ticker = ticker_of(raw)
        if ticker not in out:
            out.append(ticker)
    if len(out) > TICKERS_MAX:
        raise DriveError("invalid", f"A memo has at most {TICKERS_MAX} tickers")
    return tuple(out)


def checked(change: Mapping[str, Any]) -> dict[str, Any]:
    """``change`` (the keys sent: ``memo``, ``status``, ``tickers``) with its
    tickers normalised; refused ``invalid`` for anything else, so a save
    carrying a mark is refused before its bytes move."""
    if not CHANGE_KEYS.issuperset(change):
        raise DriveError("invalid", "A mark changes memo, status or tickers")
    out = dict(change)
    if out.get("memo") is False and ("status" in out or "tickers" in out):
        raise DriveError("invalid", "Un-marking a memo takes no status or tickers")
    if out.get("status") not in (None, "draft", "final"):
        raise DriveError("invalid", "A memo's status is draft, final or none")
    if "tickers" in out:
        out["tickers"] = _tickers(out["tickers"] if out["tickers"] is not None else [])
    return out


def next_mark(
    old: MemoMark | None, change: Mapping[str, Any], head_seq: int, now: datetime, path: str
) -> MemoMark | None:
    """The mark ``change`` (``checked``) leaves on the file at ``path``, whose
    mark is ``old`` and whose head is ``head_seq``; equal to ``old`` when it changes
    nothing. Setting ``final`` pins the head unless it is pinned already;
    leaving ``final`` unpins. Marking and un-marking both set
    ``kept_through_seq`` to the head: the version a file was marked at stays
    as it was then, and un-marking keeps status, tickers and pin, and every
    version so far as the record."""
    live = old is not None and old.marked
    memo = change.get("memo")
    if memo is False:
        if not live:
            raise DriveError("not_memo", f"{path} is not a memo")
        return dataclasses.replace(old, marked=False, kept_through_seq=head_seq)
    if memo is None and not live:
        if "status" in change or "tickers" in change:
            raise DriveError(
                "not_memo", f"{path} is not a memo; mark it as one to give it a status or tickers"
            )
        return old
    base = old or MemoMark(marked=False, status=None, tickers=(), final_seq=None)
    status = change.get("status", base.status)
    final_seq, final_at = base.final_seq, base.final_at
    if status != "final":
        final_seq = final_at = None
    elif change.get("status") == "final" and (base.status != "final" or base.final_seq != head_seq):
        final_seq, final_at = head_seq, now
    return dataclasses.replace(
        base,
        marked=True,
        status=status,
        tickers=change.get("tickers", base.tickers),
        final_seq=final_seq,
        final_at=final_at,
        kept_through_seq=base.kept_through_seq if live else head_seq,
    )


async def add_event(
    tx: Tx,
    entry: Entry,
    actor: Actor,
    kind: Literal["mark", "unmark", "update", "clear"],
    *,
    note: str | None,
    old: MemoMark | None = None,
    new: MemoMark | None = None,
    cleared: int | None = None,
) -> None:
    """One item of the record: a change from ``old`` to ``new``, or a clear."""
    pinned = new is not None and new.final_at is not None and (old is None or old.final_at != new.final_at)
    await tx.cur.execute(
        "INSERT INTO drive_memo_events (entry_id, space_id, kind, head_seq, status_before,"
        " status_after, tickers_before, tickers_after, final_seq, cleared, actor_kind,"
        " actor_user_id, agent, via, thread_id, run_id, workspace_id, computer_id, note)"
        " VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
        (
            entry.entry_id,
            tx.space_id,
            kind,
            entry.head_seq,
            old.status if old else None,
            new.status if new else None,
            list(old.tickers) if old else None,
            list(new.tickers) if new else None,
            new.final_seq if pinned else None,
            cleared,
            actor.kind,
            actor.user_id,
            actor.agent,
            actor.via,
            actor.thread_id,
            actor.run_id,
            actor.workspace_id,
            actor.computer_id,
            note,
        ),
    )


async def set_mark(
    tx: Tx, entry: Entry, actor: Actor, change: Mapping[str, Any], *, note: str | None
) -> Entry | None:
    """Apply ``change`` (``checked``) to ``entry``'s mark as one event in its
    record; the file as it then stands, or None when nothing changed (and
    nothing was recorded)."""
    old = entry.memo
    now = await tx.now()
    new = next_mark(old, change, entry.head_seq, now, entry.path)
    if new == old:
        return None
    await tx.cur.execute(
        """
        INSERT INTO drive_memos (entry_id, space_id, is_memo, status, tickers, final_seq,
                                 final_at, kept_through_seq)
        VALUES (%(id)s, %(space)s, %(marked)s, %(status)s, %(tickers)s, %(final_seq)s,
                %(final_at)s, %(kept)s)
        ON CONFLICT (entry_id) DO UPDATE SET
            is_memo = EXCLUDED.is_memo, status = EXCLUDED.status, tickers = EXCLUDED.tickers,
            final_seq = EXCLUDED.final_seq, final_at = EXCLUDED.final_at,
            kept_through_seq = EXCLUDED.kept_through_seq, updated_at = now()
        """,
        {
            "id": entry.entry_id,
            "space": tx.space_id,
            "marked": new.marked,
            "status": new.status,
            "tickers": list(new.tickers),
            "final_seq": new.final_seq,
            "final_at": new.final_at,
            "kept": new.kept_through_seq,
        },
    )
    if not new.marked:
        kind = "unmark"
    else:
        kind = "update" if old is not None and old.marked else "mark"
    await add_event(tx, entry, actor, kind, note=note, old=old, new=new)
    await retention.keep(tx, [entry.entry_id], saved=False)
    await changed(tx, entry)
    return await tx.entry(entry.entry_id)


async def changed(tx: Tx, entry: Entry) -> None:
    """A change a client shows on the entry (its mark, a version's why, a
    clear) with its content as it was: it moves in the feed, and its
    modified time and author stay those of its last save."""
    await tx.cur.execute(
        "UPDATE drive_entries SET changed_seq = %s WHERE entry_id = %s",
        (await tx.seq(), entry.entry_id),
    )


def _token(value: Any) -> str:
    return base64.urlsafe_b64encode(json.dumps(value).encode()).decode().rstrip("=")


def _bad_cursor() -> DriveError:
    return DriveError("invalid", "That page cursor is not one this list gave")


def _untoken(token: str) -> Any:
    try:
        return json.loads(base64.urlsafe_b64decode(token + "=" * (-len(token) % 4)))
    except ValueError:
        raise _bad_cursor() from None


def _record_mark(token: str) -> tuple[datetime, str, int]:
    """Where a record page starts, from the cursor the page before gave."""
    mark = _untoken(token)
    try:
        at, kind, key = mark
        at = datetime.fromisoformat(at)
    except (TypeError, ValueError):
        raise _bad_cursor() from None
    if kind not in ("version", "event") or type(key) is not int or not 0 <= key < 2**63:
        raise _bad_cursor()
    return at, kind, key


#: A page of the record's keys, newest first: by time, then at one instant
#: an event above a version (a file saved as a memo is marked in the save's
#: own transaction, after it), then by seq or event id.
_RECORD_KEYS_SQL = """
    SELECT type, key, at FROM (
        SELECT 'version' AS type, 0 AS rank, seq::bigint AS key, created_at AS at
          FROM drive_versions WHERE entry_id = %(id)s
        UNION ALL
        SELECT 'event', 1, event_id, created_at FROM drive_memo_events WHERE entry_id = %(id)s
    ) r
    WHERE %(at)s::timestamptz IS NULL
       OR (r.at, r.rank, r.key)
          < (%(at)s::timestamptz, CASE %(type)s::text WHEN 'event' THEN 1 ELSE 0 END, %(key)s)
    ORDER BY r.at DESC, r.rank DESC, r.key DESC
    LIMIT %(limit)s
"""


async def record_page(entry_id: str, *, before: str | None, limit: int) -> RecordPage:
    """A page of a file's record, newest first: its versions and the events
    of its mark, with ``next`` naming where the page after starts. The keys
    are paged first, then each kind is read with its own columns, all in one
    snapshot."""
    at = kind = key = None
    if before is not None:
        at, kind, key = _record_mark(before)
    async with get_db_connection() as conn, conn.transaction():
        await conn.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
        keys = await fetch(
            _RECORD_KEYS_SQL,
            {"id": entry_id, "at": at, "type": kind, "key": key, "limit": limit + 1},
            conn,
        )
        more = len(keys) > limit
        keys = keys[:limit]
        seqs = [k["key"] for k in keys if k["type"] == "version"]
        ids = [k["key"] for k in keys if k["type"] == "event"]
        versions = {
            r["seq"]: version_from_row(r)
            for r in await fetch(
                f"SELECT {VERSION_COLUMNS} FROM drive_versions v"
                " WHERE v.entry_id = %(id)s AND v.seq = ANY(%(seqs)s::int[])",
                {"id": entry_id, "seqs": seqs},
                conn,
            )
        } if seqs else {}
        events = {
            r["event_id"]: event_from_row(r)
            for r in await fetch(
                f"SELECT {EVENT_COLUMNS} FROM drive_memo_events v"
                " WHERE v.event_id = ANY(%(ids)s::bigint[])",
                {"ids": ids},
                conn,
            )
        } if ids else {}
    items: list[RecordVersion | MemoEvent] = [
        RecordVersion(version=versions[k["key"]], added=None, removed=None)
        if k["type"] == "version"
        else events[k["key"]]
        for k in keys
    ]
    after = None
    if more:
        last = keys[-1]
        after = _token([last["at"].isoformat(), last["type"], last["key"]])
    return RecordPage(items=items, next=after)


_SORTS: dict[str, str] = {
    "revised": "v.created_at",
    "name": "lower(e.name)",
    "path": "e.path_key",
    "status": "m.status",
}


async def memo_page(
    space_id: str,
    *,
    sort: Sort,
    descending: bool,
    status: str | None,
    ticker: str | None,
    limit: int,
    cursor: str | None,
) -> MemoPage:
    """The space's live memos, each with how many versions it keeps."""
    # An offset, not a keyset: one over four sort keys is not simpler, and a
    # person's memos are few.
    offset = 0
    if cursor is not None:
        offset = _untoken(cursor)
        if type(offset) is not int or not 0 <= offset < 2**31:
            raise _bad_cursor()
    order = "DESC NULLS LAST" if descending else "ASC NULLS LAST"
    where = "e.space_id = %(space)s AND e.deleted_at IS NULL AND m.is_memo"
    if status is not None:
        where += " AND m.status IS NOT DISTINCT FROM %(status)s"
    if ticker is not None:
        where += " AND %(ticker)s = ANY(m.tickers)"
    rows = await fetch(
        f"SELECT {ENTRY_COLUMNS},"
        " (SELECT count(*) FROM drive_versions c WHERE c.entry_id = e.entry_id) AS version_count"
        f" FROM {ENTRY_FROM}"
        f" WHERE {where}"
        f" ORDER BY {_SORTS[sort]} {order}, e.entry_id {'DESC' if descending else 'ASC'}"
        " LIMIT %(limit)s OFFSET %(offset)s",
        {
            "space": space_id,
            "status": None if status == "none" else status,
            "ticker": ticker,
            "limit": limit + 1,
            "offset": offset,
        },
    )
    more = len(rows) > limit
    items = []
    for row in rows[:limit]:
        count = row.pop("version_count")
        items.append((entry_from_row(row), int(count)))
    return MemoPage(items=items, next=_token(offset + limit) if more else None)


async def explain(tx: Tx, entry: Entry, seq: int | None, note: str, actor: Actor) -> Version:
    """Say why version ``seq`` (the head by default) was made, where it does
    not say already, as only whoever made it may."""
    seq = entry.head_seq if seq is None else seq
    found = await tx.one(
        f"SELECT {VERSION_COLUMNS} FROM drive_versions v"
        " WHERE v.entry_id = %(id)s AND v.seq = %(seq)s FOR UPDATE",
        {"id": entry.entry_id, "seq": seq},
    )
    if found is None:
        raise DriveError("not_found", f"{entry.path} has no version {seq}")
    version = version_from_row(found)
    if version.note is not None:
        raise DriveError("exists", f"Version {seq} of {entry.path} already says why.")
    if (version.actor.kind, version.actor.user_id) != (actor.kind, actor.user_id):
        raise DriveError("forbidden", f"Only whoever made version {seq} can say why.")
    await tx.cur.execute(
        "UPDATE drive_versions SET note = %s WHERE entry_id = %s AND seq = %s",
        (note, entry.entry_id, seq),
    )
    # An earlier version's why shows only in the record, which a client
    # reads again when the entry moves in the feed.
    await changed(tx, entry)
    return dataclasses.replace(version, note=note)
