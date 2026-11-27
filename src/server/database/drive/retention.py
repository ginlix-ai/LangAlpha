"""What a space keeps of its bytes, and what it lets go.

A content row is counted while a live entry's version references it, sits
outside the allowance while only deleted entries do (the trash, or the
notice a replaced file leaves), and is released when nothing does: out of
every count at once, readable by its digest for the history rule's grace,
then freed by ``free_released``, the one place content rows are deleted.
The counts are kept on the space's row as the rows change (migration
150's ``COUNTERS``), so reading them costs nothing. Everything here runs
inside a locked transaction (``writes.Tx``), and ``settle`` is what
``writes.locked`` runs before it commits: the allowance, before
``trash.bound`` frees what deleted entries and released copies hold past
their own limit.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from src.server.database.drive.policy import CLEARED, HISTORY_SQL, UNDO_TARGET_SQL
from src.server.database.drive.records import COUNTS_SQL, Entry, over_quota

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


async def settle(tx: Tx, live_before: int | None, *, allowance: bool = True) -> dict[str, int]:
    """Before commit: refuse a change that took counted bytes past the
    allowance (one that adds nothing passes, over the allowance or not).
    ``allowance`` False for upkeep (``writes.upkeep``), which adds nothing.
    The space's counts, for the trash's bound that follows."""
    cap = tx.limits.max_space_bytes
    seen = await counts(tx)
    over = cap is not None and seen["live"] > cap
    if allowance and over and (live_before is None or seen["live"] > live_before):
        raise over_quota()
    return seen


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
          AND live_refs = 0 AND gone_refs > 0) AS gone_bytes
"""

#: The counters, every one of which ``usage`` is made from.
_KEPT = ("entry_count", "held_bytes", "released_bytes", "gone_bytes")


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
        # The usage a client holds was wrong; the change number tells it so.
        await tx.seq()
    return drift


async def _drop(tx: Tx, ids: list[str], roles: tuple[str, ...], before: int | None = None) -> list[str]:
    """Delete the versions of ``ids`` whose role (``policy.HISTORY_SQL``) is
    one of ``roles``, those below seq ``before`` when given; their digests.
    The database keeps a memo's record against every delete but one that
    says it is a clear (migration 154's ``drive_version_kept``), which
    dropping the ``record`` role is."""
    clearing = "record" in roles
    if clearing:
        await tx.cur.execute("SELECT set_config('drive.clearing', 'on', true)")
    rows = await tx.all(
        f"WITH {HISTORY_SQL}"
        " DELETE FROM drive_versions v USING history h"
        " WHERE v.entry_id = h.entry_id AND v.seq = h.seq AND h.role = ANY(%(roles)s)"
        " AND (%(before)s::int IS NULL OR v.seq < %(before)s::int)"
        " RETURNING v.sha256",
        {"ids": ids, "max": tx.limits.history.max_bytes, "roles": list(roles), "before": before},
    )
    if clearing:
        await tx.cur.execute("SELECT set_config('drive.clearing', 'off', true)")
    return [r["sha256"] for r in rows]


async def keep(tx: Tx, ids: list[str], *, saved: bool) -> tuple[int, int]:
    """After any change to the versions or the mark of the files ``ids``:
    drop what they no longer keep, then set ``undo_until`` (``window``).
    ``saved``: a save changed the head, as a new version or folded into it,
    which starts the rule's days. The versions dropped and the bytes
    released."""
    dropped = await _drop(tx, ids, ("spare",))
    await window(tx, ids, saved=saved)
    if dropped:
        await tx.seq()
    return len(dropped), await release(tx, dropped)


async def window(tx: Tx, ids: list[str], *, saved: bool) -> None:
    """Set ``undo_until``, when an ordinary file's one undo copy goes. It
    is null on a live memo (it keeps every version) and on a file with
    nothing to go back to (``policy.UNDO_TARGET_SQL``): the version below
    its head gone, or an undo copy with the head's bytes, which the sweep
    leaves and the next save either drops or, folded into the same
    sitting, gives days again. A window already past stays as it is,
    so the sweep's ``expire`` still finds it and drops the copy. After an
    un-mark, the first save sets one though the version below is part of
    the record and stays when it ends.
    Undo acts on a file that is not a live memo only while ``undo_until``
    is set, so an un-mark, which leaves it null, has nothing to undo until
    the next save. This sets ``undo_until`` and ``expire`` ends it; nothing
    else writes it."""
    await tx.cur.execute(
        f"""
        UPDATE drive_entries e SET undo_until = CASE
                   WHEN EXISTS (SELECT 1 FROM drive_memos m
                                 WHERE m.entry_id = e.entry_id AND m.is_memo)
                        OR {UNDO_TARGET_SQL} IS NULL THEN NULL
                   WHEN %(saved)s THEN now() + make_interval(days => %(days)s)
                   ELSE e.undo_until
               END
         WHERE e.entry_id = ANY(%(ids)s::uuid[])
        """,
        {"ids": ids, "saved": saved, "days": tx.limits.history.undo_days},
    )


#: Files one pass of ``expire`` takes, so a space where many fall due at
#: once (every file with history, the day its stage 1 history ends) is
#: worked off over passes rather than under one hold of its lock.
EXPIRE_BATCH = 500


async def expire(tx: Tx) -> tuple[int, int]:
    """End the undo windows past their time, the space's ``EXPIRE_BATCH``
    longest due, dropping each file's undo copy: the versions dropped and
    the bytes released. Each file moves in the feed, so a client showing
    its Undo or listing its versions reads them again."""
    due = await tx.all(
        "SELECT entry_id::text AS id FROM drive_entries"
        " WHERE space_id = %(space)s AND deleted_at IS NULL AND undo_until <= now()"
        " ORDER BY undo_until LIMIT %(batch)s",
        {"space": tx.space_id, "batch": EXPIRE_BATCH},
    )
    if not due:
        return 0, 0
    ids = [r["id"] for r in due]
    gone = await _drop(tx, ids, ("undo", "spare"))
    await tx.cur.execute(
        "UPDATE drive_entries SET undo_until = NULL, changed_seq = %(seq)s"
        " WHERE entry_id = ANY(%(ids)s::uuid[])",
        {"seq": await tx.seq(), "ids": ids},
    )
    return len(gone), await release(tx, gone)


async def clear(tx: Tx, found: Entry, before: int | None) -> tuple[int, int]:
    """Drop ``found``'s old versions (``CLEARED``), every one or those below
    seq ``before``, as the file stands under the lock: the versions dropped
    and the bytes that left the counts. Bytes another version still
    references stay. A clear that dropped anything moves the file in the
    feed, head and modified time as they were, so other clients read its
    versions and the usage again while a mount keeps its bytes."""
    dropped = await _drop(tx, [found.entry_id], CLEARED, before)
    if not dropped:
        return 0, 0
    # Not ``keep``: its spare drop has no bound, and a file from before
    # memos can hold spares at or above ``before``.
    await window(tx, [found.entry_id], saved=False)
    await tx.cur.execute(
        "UPDATE drive_entries SET changed_seq = %s WHERE entry_id = %s",
        (await tx.seq(), found.entry_id),
    )
    return len(dropped), await release(tx, dropped)
