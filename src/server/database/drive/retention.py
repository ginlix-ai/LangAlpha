"""What a space keeps of its bytes, and what it lets go.

A content row is counted while a live entry's version references it, sits
outside the allowance while only deleted entries do (the trash, or the
notice a replaced file leaves), and is released when nothing does: out of
every count at once, readable by its digest for the history rule's grace,
then freed by ``free_released``, the one place content rows are deleted.
The counts are kept on the space's row as the rows change (migration
150's ``COUNTERS``), so reading them costs nothing. Everything here runs
inside a locked transaction (``writes.Tx``), and ``settle`` is what
``writes.locked`` runs before it commits: the allowance, then the bound on
what deleted entries and released copies hold.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from src.server.database.drive.records import COUNTS_SQL, CLEARING_SQL, DriveError, history_sql, over_quota

if TYPE_CHECKING:
    from src.server.database.drive.writes import Tx


async def counts(tx: Tx) -> dict[str, int]:
    """The space's ``live``, ``trash`` (held only by deleted entries) and
    ``released`` bytes, and its ``entries``, as ``tx`` sees them."""
    row = await tx.one(COUNTS_SQL, {"space": tx.space_id})
    assert row is not None
    return {k: int(row[k]) for k in ("live", "trash", "released", "entries")}


async def opening(tx: Tx) -> int | None:
    """Live bytes as the transaction opens, for ``settle`` to tell growth
    from a space already over its allowance; None with no allowance."""
    if tx.limits.max_space_bytes is None:
        return None
    return (await counts(tx))["live"]


async def settle(tx: Tx, live_before: int | None, *, allowance: bool = True) -> None:
    """Before commit: refuse a change that took counted bytes past the
    allowance (one that adds nothing passes, over the allowance or not),
    then free what the bound on deleted and released bytes no longer holds.
    ``allowance`` False for upkeep (``writes.upkeep``), which adds nothing."""
    cap = tx.limits.max_space_bytes
    seen = await counts(tx)
    over = cap is not None and seen["live"] > cap
    if allowance and over and (live_before is None or seen["live"] > live_before):
        raise over_quota()
    await bound(tx, seen)


async def release(tx: Tx, shas: list[str | None]) -> int:
    """Mark the rows among ``shas`` that no version references any more as
    released; the bytes that left the counts."""
    shas = list({s for s in shas if s})
    if not shas:
        return 0
    row = await tx.one(
        """
        WITH marked AS (
            UPDATE drive_contents c SET released_at = now()
             WHERE c.space_id = %(space)s AND c.sha256 = ANY(%(shas)s)
               AND c.released_at IS NULL
               AND NOT EXISTS (
                   SELECT 1 FROM drive_versions v
                    WHERE v.space_id = c.space_id AND v.sha256 = c.sha256)
            RETURNING c.byte_len)
        SELECT COALESCE(sum(byte_len), 0) AS n FROM marked
        """,
        {"space": tx.space_id, "shas": shas},
    )
    return int(row["n"]) if row else 0


async def free_released(
    tx: Tx, *, older_than_seconds: int | None = None, over_bytes: int | None = None
) -> int:
    """Delete released rows: those released longer ago than
    ``older_than_seconds``, or the oldest until ``over_bytes`` are gone. The
    bytes freed; an object in the store goes with the blob sweep once no
    row of the user's names it."""
    if over_bytes is not None:
        row = await tx.one(
            """
            WITH oldest AS (
                SELECT sha256, sum(byte_len) OVER (ORDER BY released_at, sha256)
                       - byte_len AS before
                  FROM drive_contents
                 WHERE space_id = %(space)s AND released_at IS NOT NULL),
            freed AS (
                DELETE FROM drive_contents c USING oldest o
                 WHERE c.space_id = %(space)s AND c.sha256 = o.sha256
                   AND o.before < %(over)s
                RETURNING c.byte_len)
            SELECT COALESCE(sum(byte_len), 0) AS n FROM freed
            """,
            {"space": tx.space_id, "over": over_bytes},
        )
    else:
        row = await tx.one(
            """
            WITH freed AS (
                DELETE FROM drive_contents
                 WHERE space_id = %(space)s AND released_at IS NOT NULL
                   AND released_at < now() - make_interval(secs => %(secs)s)
                RETURNING byte_len)
            SELECT COALESCE(sum(byte_len), 0) AS n FROM freed
            """,
            {"space": tx.space_id, "secs": older_than_seconds or 0},
        )
    return int(row["n"]) if row else 0


_REFS_SQL = """
    WITH refs AS (
        SELECT v.sha256,
               count(*) FILTER (WHERE e.deleted_at IS NULL) AS live,
               count(*) FILTER (WHERE e.deleted_at IS NOT NULL) AS gone
          FROM drive_versions v JOIN drive_entries e ON e.entry_id = v.entry_id
         WHERE v.space_id = %(space)s
         GROUP BY v.sha256),
    truth AS (
        SELECT c.sha256, COALESCE(r.live, 0) AS live, COALESCE(r.gone, 0) AS gone
          FROM drive_contents c LEFT JOIN refs r ON r.sha256 = c.sha256
         WHERE c.space_id = %(space)s)
    UPDATE drive_contents c SET live_refs = t.live, gone_refs = t.gone
      FROM truth t
     WHERE c.space_id = %(space)s AND c.sha256 = t.sha256
       AND (c.live_refs <> t.live OR c.gone_refs <> t.gone)
    RETURNING c.sha256
"""

_SPACE_SQL = """
    SELECT
      (SELECT count(*) FROM drive_entries WHERE space_id = %(space)s) AS entry_count,
      (SELECT COALESCE(sum(byte_len), 0) FROM drive_contents WHERE space_id = %(space)s) AS held_bytes,
      (SELECT COALESCE(sum(byte_len), 0) FROM drive_contents
        WHERE space_id = %(space)s AND released_at IS NOT NULL) AS released_bytes,
      (SELECT COALESCE(sum(byte_len), 0) FROM drive_contents
        WHERE space_id = %(space)s AND released_at IS NULL
          AND live_refs = 0 AND gone_refs > 0) AS gone_bytes,
      (SELECT count(*) FROM drive_versions
        WHERE space_id = %(space)s AND NOT is_text) AS binary_versions
"""

_KEPT = ("entry_count", "held_bytes", "released_bytes", "gone_bytes", "binary_versions")
#: The counters ``usage`` is made from.
_SHOWN = ("entry_count", "held_bytes", "released_bytes", "gone_bytes")


async def reconcile(tx: Tx) -> dict[str, int]:
    """Recount the space from its rows and set its counters (migration
    150's ``COUNTERS``) to what the rows say: the content rows' references
    first, then the space's sums. The difference each corrected counter
    had, which the caller reports; empty when the counters were right. A
    scan of the space, for the sweep's visit, never a request's path."""
    fixed = await tx.all(_REFS_SQL, {"space": tx.space_id})
    truth = await tx.one(_SPACE_SQL, {"space": tx.space_id})
    kept = await tx.one(
        f"SELECT {', '.join(_KEPT)} FROM drive_spaces WHERE space_id = %(space)s",
        {"space": tx.space_id},
    )
    assert truth is not None and kept is not None
    drift = {k: int(kept[k]) - int(truth[k]) for k in _KEPT if kept[k] != truth[k]}
    if fixed:
        drift["content_refs"] = len(fixed)
    if any(k in drift for k in _KEPT):
        await tx.cur.execute(
            f"UPDATE drive_spaces SET {', '.join(f'{k} = %({k})s' for k in _KEPT)}"
            " WHERE space_id = %(space)s",
            {**truth, "space": tx.space_id},
        )
    if any(k in drift for k in _SHOWN):
        # The usage a client holds was wrong; the change number tells it so.
        await tx.seq()
    return drift


#: The oldest deleted entry still holding bytes nothing live holds: purging
#: it (its trash item, or the notice of a replaced file) brings those bytes
#: down, which purging one whose bytes a live file shares would not.
_HOLDING_SQL = """
    SELECT e.entry_id::text AS entry_id, e.trash_id::text AS trash_id
      FROM drive_entries e
     WHERE e.space_id = %(space)s AND e.deleted_at IS NOT NULL
       AND EXISTS (
           SELECT 1 FROM drive_versions v JOIN drive_contents c
               ON c.space_id = v.space_id AND c.sha256 = v.sha256
            WHERE v.entry_id = e.entry_id AND c.live_refs = 0 AND c.released_at IS NULL)
     ORDER BY e.deleted_at, e.entry_id
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
    seen = seen or await counts(tx)
    purged = 0
    while (over := seen["trash"] + seen["released"] - limit) > 0:
        if seen["released"]:
            if not await free_released(tx, over_bytes=over):
                break
        else:
            oldest = await tx.one(_HOLDING_SQL, {"space": tx.space_id})
            if oldest is None:
                break
            # What it held is released, which the next round frees.
            purged += await tx.purge(oldest["trash_id"], None, entry_id=oldest["entry_id"])
        seen = await counts(tx)
    return purged


async def trim(tx: Tx, entry_id: str | None = None) -> tuple[int, int]:
    """Drop the old versions the history rule says are due, of one file or
    the whole space; the versions dropped and the bytes released."""
    scope = "entry" if entry_id is not None else "space"
    dropped = await tx.all(
        history_sql(scope)
        + """
        DELETE FROM drive_versions v USING history h
         WHERE h.goes_at <= now() AND v.entry_id = h.entry_id AND v.seq = h.seq
        RETURNING v.sha256
        """,
        {"entry": entry_id, "space": tx.space_id, **tx.limits.history.params},
    )
    if dropped:
        # Versions lists and the usage changed, no entry with them.
        await tx.seq()
    return len(dropped), await release(tx, [r["sha256"] for r in dropped])


async def clear(tx: Tx, entry_id: str, before: int | None) -> tuple[int, int]:
    """Drop a file's old versions, every one or those below seq ``before``,
    keeping the head: the versions dropped and the bytes that left the
    counts, both as the file stands when the lock is taken. Bytes another
    version still references stay."""
    found = await tx.live(entry_id=entry_id)
    if found is None:
        raise DriveError("not_found", "No such entry")
    if found.kind != "file":
        raise DriveError("is_folder", f"{found.path} is a folder")
    dropped = await tx.all(
        f"WITH {CLEARING_SQL}"
        " DELETE FROM drive_versions v USING dropped d"
        " WHERE v.entry_id = %(id)s AND v.seq = d.seq RETURNING v.sha256",
        {"id": found.entry_id, "head": found.head_seq, "before": before},
    )
    if dropped:
        # A change to the file in the feed, head and modified time as they
        # were: other clients read its versions and the usage again, and a
        # computer's mount, seeing nothing a reader could tell apart, keeps
        # its bytes.
        await tx.cur.execute(
            "UPDATE drive_entries SET changed_seq = %s WHERE entry_id = %s",
            (await tx.seq(), found.entry_id),
        )
    return len(dropped), await release(tx, [r["sha256"] for r in dropped])
