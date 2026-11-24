"""Drive reads. None of them lock: a reader sees the last committed change."""

from __future__ import annotations

from typing import Any

from psycopg.rows import dict_row

from src.server.database.drive.keys import path_key
from src.server.database.drive.records import (
    CLEARING_SQL,
    COUNTS_SQL,
    ENTRY_COLUMNS,
    ENTRY_FROM,
    EXPIRED_SQL,
    TOMBSTONE_HOURS,
    VERSION_COLUMNS,
    Changes,
    Content,
    DriveError,
    Entry,
    HistoryRule,
    Space,
    TrashItem,
    Version,
    entry_from_row,
    history_sql,
    version_from_row,
)
from src.server.database.pool import get_db_connection

_SPACE_COLUMNS = "space_id::text AS space_id, kind, user_id, created_at"


async def _rows(sql: str, params: dict[str, Any], conn=None) -> list[dict[str, Any]]:
    async with get_db_connection(conn) as c:
        async with c.cursor(row_factory=dict_row) as cur:
            await cur.execute(sql, params)
            return list(await cur.fetchall())


async def personal_space(user_id: str) -> Space:
    """The user's personal space, made on first use."""
    found = await _rows(
        f"SELECT {_SPACE_COLUMNS} FROM drive_spaces"
        " WHERE kind = 'personal' AND user_id = %(user_id)s",
        {"user_id": user_id},
    )
    if not found:
        found = await _rows(
            "INSERT INTO drive_spaces (kind, user_id) VALUES ('personal', %(user_id)s)"
            " ON CONFLICT (user_id) WHERE kind = 'personal' DO NOTHING"
            f" RETURNING {_SPACE_COLUMNS}",
            {"user_id": user_id},
        )
    if not found:
        # Another request made it between the two statements.
        found = await _rows(
            f"SELECT {_SPACE_COLUMNS} FROM drive_spaces"
            " WHERE kind = 'personal' AND user_id = %(user_id)s",
            {"user_id": user_id},
        )
    return Space(**found[0])


async def get_space(space_id: str) -> Space | None:
    found = await _rows(
        f"SELECT {_SPACE_COLUMNS} FROM drive_spaces WHERE space_id = %(id)s",
        {"id": space_id},
    )
    return Space(**found[0]) if found else None


async def live_entry(
    space_id: str, *, path: str | None = None, entry_id: str | None = None, conn=None
) -> Entry | None:
    if entry_id is not None:
        where, params = "e.entry_id = %(id)s", {"id": entry_id}
    else:
        where, params = "e.path_key = %(key)s", {"key": path_key(path or "")}
    found = await _rows(
        f"SELECT {ENTRY_COLUMNS} FROM {ENTRY_FROM}"
        f" WHERE e.space_id = %(space)s AND e.deleted_at IS NULL AND {where}",
        {"space": space_id, **params},
        conn,
    )
    return entry_from_row(found[0]) if found else None


async def children(space_id: str, parent: str) -> list[Entry]:
    rows = await _rows(
        f"SELECT {ENTRY_COLUMNS} FROM {ENTRY_FROM}"
        " WHERE e.space_id = %(space)s AND e.deleted_at IS NULL AND e.parent = %(parent)s"
        " ORDER BY e.kind DESC, lower(e.name)",
        {"space": space_id, "parent": parent},
    )
    return [entry_from_row(r) for r in rows]


#: Entries at or below a folder, by its key: under byte order (``path_key``
#: is ``COLLATE "C"``) everything below ``k`` sorts from ``k/`` up to ``k0``,
#: ``0`` being the byte after ``/``, so the live path index finds the subtree.
_BELOW = "e.path_key >= %(under_key)s || '/' AND e.path_key < %(under_key)s || '0'"
_AT_OR_BELOW = f"(e.path_key = %(under_key)s OR ({_BELOW}))"


async def subtree(space_id: str, under: str) -> list[Entry]:
    """Every live entry below ``under`` (a folder's own path), or the whole
    space for ``''``."""
    rows = await _rows(
        f"SELECT {ENTRY_COLUMNS} FROM {ENTRY_FROM}"
        " WHERE e.space_id = %(space)s AND e.deleted_at IS NULL"
        + (f" AND {_BELOW}" if under else "")
        + " ORDER BY e.path",
        {"space": space_id, "under_key": path_key(under)},
    )
    return [entry_from_row(r) for r in rows]


async def versions(entry_id: str, rule: HistoryRule) -> tuple[list[Version], bool]:
    """A file's versions, newest first, each with ``expires_at`` from the
    history rule, and whether the head is too large to keep once replaced."""
    rows = await _rows(
        history_sql("entry")
        + f" SELECT {VERSION_COLUMNS}, h.goes_at AS expires_at, h.too_large AND h.is_head AS head_large"
        " FROM drive_versions v JOIN history h ON h.entry_id = v.entry_id AND h.seq = v.seq"
        " ORDER BY v.seq DESC",
        {"entry": entry_id, **rule.params},
    )
    # Popped from every row before any() looks: it stops at the first true.
    flags = [r.pop("head_large") for r in rows]
    large = any(flags)
    return [version_from_row(r) for r in rows], large


async def history_bytes(space_id: str, entry_id: str, head_seq: int | None) -> int:
    """The bytes clearing a file's whole history would take out of the
    counts (``CLEARING_SQL``, the clear's own definition): those no version
    left behind references."""
    rows = await _rows(
        f"WITH {CLEARING_SQL}"
        """
        SELECT (SELECT COALESCE(sum(c.byte_len), 0) FROM drive_contents c
                 WHERE c.space_id = %(space)s AND c.released_at IS NULL
                   AND c.sha256 IN (SELECT sha256 FROM dropped)
                   AND NOT EXISTS (
                       SELECT 1 FROM drive_versions o
                        WHERE o.space_id = c.space_id AND o.sha256 = c.sha256
                          AND NOT (o.entry_id = %(id)s
                                   AND o.seq IN (SELECT seq FROM dropped)))) AS n
        """,
        {"space": space_id, "id": entry_id, "head": head_seq, "before": None},
    )
    return int(rows[0]["n"])


async def content_of(space_id: str, entry_id: str, seq: int) -> Content | None:
    found = await _rows(
        "SELECT c.space_id::text AS space_id, c.sha256, c.byte_len AS size, v.is_text,"
        " v.content_type, c.blob_user_id"
        " FROM drive_versions v JOIN drive_contents c"
        " ON c.space_id = %(space)s AND c.sha256 = v.sha256"
        " WHERE v.entry_id = %(id)s AND v.seq = %(seq)s",
        {"space": space_id, "id": entry_id, "seq": seq},
    )
    return Content(**found[0]) if found else None


async def held_bytes(space_id: str, sha256: str, offset: int, length: int) -> bytes:
    """A slice of bytes the database holds, read without the rest of the row."""
    async with get_db_connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                "SELECT substring(content FROM %s FOR %s) FROM drive_contents"
                " WHERE space_id = %s AND sha256 = %s AND content IS NOT NULL",
                (offset + 1, length, space_id, sha256),
            )
            row = await cur.fetchone()
    if row is None:
        raise DriveError("not_found", "The file's content is gone")
    return bytes(row[0])


async def content_by_sha(space_id: str, sha256: str) -> Content | None:
    """Content the space holds under ``sha256``, whichever version has it."""
    found = await _rows(
        "SELECT c.space_id::text AS space_id, c.sha256, c.byte_len AS size, c.is_text,"
        " 'application/octet-stream' AS content_type, c.blob_user_id FROM drive_contents c"
        " WHERE c.space_id = %(space)s AND c.sha256 = %(sha)s",
        {"space": space_id, "sha": sha256},
    )
    return Content(**found[0]) if found else None


_CHOSEN = " AND e.entry_id = ANY(%(ids)s::uuid[])"


async def texts(
    space_id: str,
    entry_ids: list[str] | None,
    after: str | None,
    limit: int,
    max_bytes: int,
    under: str | None = None,
) -> list[tuple[Entry, bytes]]:
    """A page of live text files the database holds, by entry id after
    ``after``: at most ``limit`` files and, past the first, ``max_bytes`` of
    text. The sizes are read before the bytes, so a page of large files is
    never all read only to be cut. ``under`` (a stored path) keeps those at
    or below it. Paged on the uuid itself, never its text, so each page
    walks the primary key from where the last one ended."""
    where = (
        " WHERE e.space_id = %(space)s AND e.deleted_at IS NULL AND e.kind = 'file'"
        " AND v.is_text AND c.content IS NOT NULL"
    )
    if after is not None:
        where += " AND e.entry_id > %(after)s::uuid"
    if under is not None:
        where += f" AND {_AT_OR_BELOW}"
    params = {
        "space": space_id,
        "ids": entry_ids,
        "after": after,
        "limit": limit,
        "under_key": path_key(under) if under is not None else None,
    }
    joined = (
        f" FROM {ENTRY_FROM}"
        " JOIN drive_contents c ON c.space_id = e.space_id AND c.sha256 = v.sha256"
    )
    # One snapshot: a head replaced between the two reads would otherwise
    # bring bytes the sizing never counted.
    async with get_db_connection() as conn:
        async with conn.transaction():
            await conn.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
            sized = await _rows(
                f"SELECT e.entry_id::text AS entry_id, c.byte_len{joined}{where}"
                f"{'' if entry_ids is None else _CHOSEN} ORDER BY e.entry_id LIMIT %(limit)s",
                params,
                conn,
            )
            chosen, total = [], 0
            for row in sized:
                if chosen and total + row["byte_len"] > max_bytes:
                    break
                chosen.append(row["entry_id"])
                total += row["byte_len"]
            if not chosen:
                return []
            rows = await _rows(
                f"SELECT {ENTRY_COLUMNS}, c.content{joined}{where}{_CHOSEN}"
                " ORDER BY e.entry_id LIMIT %(limit)s",
                {**params, "ids": chosen},
                conn,
            )
    held = [bytes(r.pop("content")) for r in rows]
    return [(entry_from_row(r), data) for r, data in zip(rows, held, strict=True)]


async def trashed(space_id: str, limit: int | None = None) -> list[TrashItem]:
    """Trash items newest first; a rename's tombstone is never one."""
    rows = await _rows(
        f"""
        SELECT * FROM (
            SELECT DISTINCT ON (e.trash_id) {ENTRY_COLUMNS},
                   count(*) OVER (PARTITION BY e.trash_id) AS group_size
              FROM {ENTRY_FROM}
             WHERE e.space_id = %(space)s AND e.ended = 'trash'
             ORDER BY e.trash_id, length(e.path)
        ) t ORDER BY deleted_at DESC, trash_id
        """
        + (" LIMIT %(limit)s" if limit is not None else ""),
        {"space": space_id, "limit": limit},
    )
    return [
        TrashItem(
            trash_id=r["trash_id"],
            entries=r.pop("group_size"),
            deleted_at=r["deleted_at"],
            deleted_by=r["deleted_by"],
            entry=entry_from_row(r),
        )
        for r in rows
    ]


async def usage(space_id: str) -> tuple[int, int, int]:
    """Bytes live files keep, bytes only deleted entries keep, and the
    entries counted against the space's limit: every row, the trash and
    delete notices too. Read off the space's own row (``COUNTS_SQL``)."""
    rows = await _rows(COUNTS_SQL, {"space": space_id})
    if not rows:
        raise DriveError("not_found", "No such space")
    return int(rows[0]["live"]), int(rows[0]["trash"]), int(rows[0]["entries"])


async def cursor(space_id: str) -> int:
    """The space's last change number, for ``changes`` to start after."""
    found = await _rows(
        "SELECT change_seq FROM drive_spaces WHERE space_id = %(space)s", {"space": space_id}
    )
    if not found:
        raise DriveError("not_found", "No such space")
    return found[0]["change_seq"]


async def changes(space_id: str, since: int, limit: int | None = None) -> Changes:
    """One snapshot, so the cursor covers exactly the rows returned.

    One change can touch many rows (a folder move takes all under it, at one
    sequence number), so a page cannot end inside it; past ``limit`` rows the
    answer is ``resync``, and the caller reads the whole space instead.
    """
    async with get_db_connection() as conn:
        async with conn.transaction():
            await conn.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
            space = await _rows(
                "SELECT change_seq, purged_seq FROM drive_spaces WHERE space_id = %(space)s",
                {"space": space_id},
                conn,
            )
            if not space:
                raise DriveError("not_found", "No such space")
            cursor, purged = space[0]["change_seq"], space[0]["purged_seq"]
            if since < purged or since > cursor:
                return Changes(entries=[], cursor=cursor, resync=True)
            if since == cursor:
                return Changes(entries=[], cursor=cursor, resync=False)
            rows = await _rows(
                f"SELECT {ENTRY_COLUMNS} FROM {ENTRY_FROM}"
                " WHERE e.space_id = %(space)s AND e.changed_seq > %(since)s"
                " ORDER BY e.changed_seq, e.path"
                + (" LIMIT %(limit)s" if limit is not None else ""),
                {"space": space_id, "since": since, "limit": (limit or 0) + 1},
                conn,
            )
    if limit is not None and len(rows) > limit:
        return Changes(entries=[], cursor=cursor, resync=True)
    return Changes(entries=[entry_from_row(r) for r in rows], cursor=cursor, resync=False)


async def spaces_to_maintain(trash_days: int, rule: HistoryRule, limit: int) -> list[str]:
    """Spaces the sweep has work in: trash or tombstones past their days,
    released copies past their grace, or history the rule says is due.
    History is looked for in at most ``limit`` spaces a pass, those holding
    versions not kept as text (``binary_versions``) that were looked at
    longest ago, read off ``ix_drive_spaces_history``, so a pass costs the
    batch and those spaces' versions, never every space; each is stamped as
    looked at."""
    async with get_db_connection() as conn:
        async with conn.transaction(), conn.cursor(row_factory=dict_row) as cur:
            # Stamped in a transaction of its own, so a space being changed is
            # passed over (SKIP LOCKED) and none is held while history is read.
            await cur.execute(
                """
                WITH batch AS (
                    SELECT s.space_id FROM drive_spaces s
                     WHERE s.binary_versions > 0
                     ORDER BY s.history_checked_at NULLS FIRST, s.space_id
                     LIMIT %(limit)s
                       FOR UPDATE SKIP LOCKED)
                UPDATE drive_spaces s SET history_checked_at = now()
                  FROM batch WHERE s.space_id = batch.space_id
                RETURNING s.space_id
                """,
                {"limit": limit},
            )
            batch = [r["space_id"] for r in await cur.fetchall()]
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                history_sql("spaces")
                + f"""
                (SELECT DISTINCT e.space_id::text AS space_id FROM drive_entries e
                  WHERE {EXPIRED_SQL} LIMIT %(limit)s)
                UNION
                (SELECT DISTINCT space_id::text FROM drive_contents
                  WHERE released_at < now() - make_interval(secs => %(grace)s) LIMIT %(limit)s)
                UNION
                (SELECT DISTINCT space_id::text FROM history WHERE goes_at <= now())
                """,
                {
                    "days": trash_days,
                    "hours": TOMBSTONE_HOURS,
                    "grace": rule.grace_seconds,
                    "limit": limit,
                    "spaces": batch,
                    **rule.params,
                },
            )
            return [r["space_id"] for r in await cur.fetchall()]
