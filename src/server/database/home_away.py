"""When a user's Home ran on the cloud, and how far that work is handed back (migrations 132 and 133).

A Home turn admitted while the machine Home lives on can't be reached runs in
Home's stand-in and marks the user away here, in its START transaction, so the
mark and the run row commit together. What it wrote is owed to Home while
``last_away_at`` is newer than ``handed_back_through``. The hand-back reads
that mark under Home's row lock (``begin_handback``) and moves the watermark
with a guarded update under its advisory lock (``services.home_handback``).
"""

from __future__ import annotations

import contextlib
from collections.abc import AsyncIterator
from datetime import datetime
from typing import Any, Dict, List, NotRequired, Optional, Tuple, TypedDict

from psycopg.rows import dict_row
from psycopg.types.json import Json

from src.server.database.home_workspace import (
    get_home_stand_in_id,
    get_home_workspace_id,
)
from src.server.database.pool import get_db_connection

_COLUMNS = (
    "user_id, away_since, last_away_at, handed_back_through, last_handback, "
    "handback_seen_at, handback_attempts, next_handback_at"
)

# A file the stand-in held, as its listing gave it: ``[size, mtime]``, the
# mtime as ``find`` printed it.
Listed = List[Any]


class HandbackNotice(TypedDict):
    """The user's notice of the last hand-back (``home_away.last_handback``)."""

    handback_id: str
    #: Where in Home the files went, relative to Home; None when none did.
    folder: Optional[str]
    files: int
    #: Paths in the stand-in's folder left there: over the per-file cap, or
    #: not readable as a name or a file.
    left_on_cloud: List[str]
    unreadable: List[str]
    computer_name: str
    cloud_name: str
    created_at: str
    #: The hand-back gave up; everything it owed stays on the cloud.
    failed: NotRequired[bool]


# Whether a row's cloud work is not on Home yet, as SQL over home_away.
OWED_SQL = (
    "last_away_at IS NOT NULL"
    " AND last_away_at > COALESCE(handed_back_through, '-infinity'::timestamptz)"
)

# A failed hand-back waits this long, doubling per failure up to the cap.
_RETRY_FIRST = "1 minute"
_RETRY_CAP = "1 hour"


def is_owed(row: Optional[Dict[str, Any]]) -> bool:
    """``OWED_SQL`` for a row already read."""
    if not row or row.get("last_away_at") is None:
        return False
    through = row.get("handed_back_through")
    return through is None or row["last_away_at"] > through


def left_on_stand_in(last: Optional[HandbackNotice]) -> Optional[Dict[str, Any]]:
    """What the last hand-back left in the stand-in and Home does not have:
    ``failed`` when it gave up, else the paths too large or unreadable to
    carry. None when it left nothing."""
    if last is None:
        return None
    paths = [*last["left_on_cloud"], *last["unreadable"]]
    failed = last.get("failed", False)
    if not failed and not paths:
        return None
    return {"failed": failed, "paths": paths}


def stand_in_lock_key(user_id: str) -> str:
    """The advisory lock a Home turn's mark shares and a removal or retirement
    of the stand-in's computer holds alone (``stand_in_held``)."""
    return f"HOME_STAND_IN:{user_id}"


class StandInGone(Exception):
    """The stand-in a Home turn was placed in was removed before the turn
    started, or its computer is being removed or retired now: refused, so
    nothing is written into a folder that is going."""


@contextlib.asynccontextmanager
async def stand_in_held(user_id: str) -> AsyncIterator[bool]:
    """Hold the user's stand-in against new Home turns, or yield False while
    one is being admitted. Never waits.

    A turn placed in the stand-in takes the shared side in its START
    transaction, where it marks that Home is owed files (``mark_away``). So
    a removal or retirement that reads nothing owed under this hold deletes
    the stand-in before any turn's mark, and that turn then finds it gone, or
    finds this held, and is refused rather than writing into it."""
    from src.server.database.session_lock import try_own_session_lock

    async with try_own_session_lock(stand_in_lock_key(user_id)) as held:
        yield held


async def mark_away(
    user_id: str, *, computer_id: Optional[str] = None, conn=None
) -> None:
    """Record a Home turn admitted onto the stand-in on ``computer_id``, now.

    Run in the turn's START transaction (``conn``), after it took Home's row
    FOR SHARE, so the mark is later than any hand-back's watermark read under
    that row's lock. The clock is read here, not at the transaction's start,
    for the same reason. New work owes a hand-back its retries afresh.

    The shared side of ``stand_in_lock_key`` is held to the transaction's end:
    a removal that read nothing owed has already deleted the stand-in, which
    this then finds. One still holding it (``stand_in_held``) refuses the turn
    at once: its teardown can take minutes, which START would hold Home's row
    and a connection through."""
    async with get_db_connection(conn) as c, c.cursor(row_factory=dict_row) as cur:
        await cur.execute(
            "SELECT pg_try_advisory_xact_lock_shared(hashtextextended(%s, 0)) AS held",
            (stand_in_lock_key(user_id),),
        )
        if not (await cur.fetchone())["held"]:
            raise StandInGone()
        # On the computer the turn was placed on, which its run row names for
        # the idle sweep: one placed again since is refused, as one gone is.
        # FOR SHARE to the transaction's end: a placement moving it waits for
        # this turn to be admitted, and one that moved it first is seen.
        await cur.execute(
            "SELECT 1 FROM workspaces WHERE workspace_id = %s AND status <> 'deleted'"
            " AND (%s::uuid IS NULL OR computer_id = %s::uuid) FOR SHARE",
            (get_home_stand_in_id(user_id), computer_id, computer_id),
        )
        if await cur.fetchone() is None:
            raise StandInGone()
        await cur.execute(
            """
            INSERT INTO home_away (user_id, home_id, away_since, last_away_at)
            VALUES (%(user_id)s, %(home_id)s, clock_timestamp(), clock_timestamp())
            ON CONFLICT (user_id) DO UPDATE SET
                away_since = COALESCE(home_away.away_since, clock_timestamp()),
                last_away_at = clock_timestamp(),
                handback_attempts = 0,
                next_handback_at = NULL,
                updated_at = NOW()
            """,
            {"user_id": user_id, "home_id": get_home_workspace_id(user_id)},
        )


async def get_home_away(user_id: str, *, conn=None) -> Optional[Dict[str, Any]]:
    async with get_db_connection(conn) as c, c.cursor(row_factory=dict_row) as cur:
        await cur.execute(f"SELECT {_COLUMNS} FROM home_away WHERE user_id = %s", (user_id,))
        row = await cur.fetchone()
    return dict(row) if row else None


async def handed_back(user_id: str) -> Dict[str, Listed]:
    """Each file handed back so far, by its path in the stand-in's folder, as
    it was listed when it last went (``home_handbacks.files``)."""
    async with get_db_connection() as conn, conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(
            "SELECT files FROM home_handbacks WHERE user_id = %s ORDER BY through",
            (user_id,),
        )
        rows = await cur.fetchall()
    carried: Dict[str, Listed] = {}
    for row in rows:
        carried.update(row["files"])
    return carried


async def handback_owed(user_id: str, *, conn=None) -> bool:
    """Whether files Home's turns wrote on its stand-in are not on Home yet."""
    return is_owed(await get_home_away(user_id, conn=conn))


async def list_owed(limit: int = 50) -> List[Dict[str, Any]]:
    """Users owed a hand-back that could run now: Home's computer not behind a
    tunnel that is down, and no failure waiting out its delay. The longest
    waiting first. Read without a lock: the hand-back takes the user's
    advisory lock and moves the watermark with a guarded update."""
    from src.server.database.ssh_tunnels import computer_tunnel_down_sql

    async with get_db_connection() as conn, conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(
            f"""
            SELECT ha.user_id FROM home_away ha
            LEFT JOIN workspaces w ON w.workspace_id = ha.home_id
            WHERE {OWED_SQL}
              AND (ha.next_handback_at IS NULL OR ha.next_handback_at <= NOW())
              AND NOT {computer_tunnel_down_sql("w.computer_id::text")}
            ORDER BY COALESCE(ha.next_handback_at, ha.last_away_at)
            LIMIT %(limit)s
            """,
            {"limit": limit},
        )
        return [dict(r) for r in await cur.fetchall()]


async def begin_handback(user_id: str) -> Optional[Tuple[Dict[str, Any], datetime]]:
    """The row and the watermark a hand-back starting now copies up to; None
    while a run of Home's is live.

    Home's row is locked FOR UPDATE first. A turn's START holds it FOR SHARE
    until its run row and its away mark commit, so a turn admitted before is
    seen live here, and one admitted after marks the user away later than
    the watermark read here, which leaves its work owed."""
    from src.server.database.runs.lifecycle import workspace_has_live_runs

    home_id = get_home_workspace_id(user_id)
    async with get_db_connection() as conn, conn.transaction():
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                "SELECT 1 FROM workspaces WHERE workspace_id = %s FOR UPDATE",
                (home_id,),
            )
            if await workspace_has_live_runs(cur, home_id):
                return None
            await cur.execute(
                f"SELECT {_COLUMNS}, clock_timestamp() AS through"
                " FROM home_away WHERE user_id = %s",
                (user_id,),
            )
            row = await cur.fetchone()
    if row is None:
        return None
    row = dict(row)
    return row, row.pop("through")


async def finish_handback(
    user_id: str,
    *,
    previous: Optional[datetime],
    through: datetime,
    record: Optional[HandbackNotice],
    carried: Optional[Dict[str, Listed]] = None,
    done: bool = True,
    listed_at: Optional[datetime] = None,
) -> bool:
    """A hand-back ended. ``record`` is what arrived, for the user's notice;
    None when nothing did, which leaves the last notice as it was. ``carried``
    is each path copied into the record's folder as it was listed, kept in
    one row per hand-back for the file cards of the turns that wrote them and
    for the next hand-back (``handed_back``). ``done`` False leaves the
    watermark, so files a pass had no room for stay owed.

    The hand-back's row reaches ``listed_at``, when the stand-in was listed,
    rather than the watermark ``through``: a turn admitted between the two
    may have files in it, and its cards look for them in hand-backs reaching
    past its start. The watermark stays ``through``, so that turn stays owed.

    Guarded on the watermark the caller read (``previous``): a second writer
    that slipped past the lock changes nothing. False when it moved."""
    async with get_db_connection() as conn, conn.transaction(), conn.cursor() as cur:
        await cur.execute(
            """
            UPDATE home_away SET
                handed_back_through = CASE WHEN %(done)s
                    THEN %(through)s ELSE handed_back_through END,
                away_since = CASE WHEN %(done)s AND last_away_at <= %(through)s
                    THEN NULL ELSE away_since END,
                last_handback = COALESCE(%(record)s::jsonb, last_handback),
                handback_seen_at = CASE WHEN %(record)s::jsonb IS NULL
                    THEN handback_seen_at ELSE NULL END,
                handback_attempts = 0,
                next_handback_at = NULL,
                updated_at = NOW()
            WHERE user_id = %(user_id)s
              AND handed_back_through IS NOT DISTINCT FROM %(previous)s
            """,
            {
                "done": done,
                "through": through,
                "previous": previous,
                "record": Json(record) if record is not None else None,
                "user_id": user_id,
            },
        )
        if cur.rowcount == 0:
            return False
        if record is not None and carried:
            await cur.execute(
                """
                INSERT INTO home_handbacks (handback_id, user_id, through, folder, files)
                VALUES (%s, %s, %s, %s, %s)
                """,
                (
                    record["handback_id"],
                    user_id,
                    listed_at or through,
                    record["folder"],
                    Json(carried),
                ),
            )
        return True


async def fail_handback(
    user_id: str, *, max_attempts: int, given_up: HandbackNotice
) -> bool:
    """Count a failed hand-back and put the next one off, doubling the delay.

    The ``max_attempts``-th failure in a row gives up: the work is no longer
    owed, so the stand-in's computer can be removed, and ``given_up`` becomes
    the user's notice, saying the files are still on the cloud. New work on
    the stand-in owes a hand-back again (``mark_away``). True when it gave up."""
    async with get_db_connection() as conn, conn.transaction(), conn.cursor(
        row_factory=dict_row
    ) as cur:
        await cur.execute(
            f"""
            UPDATE home_away SET
                handback_attempts = handback_attempts + 1,
                next_handback_at = clock_timestamp() + LEAST(
                    interval '{_RETRY_FIRST}' * power(2, handback_attempts),
                    interval '{_RETRY_CAP}'),
                updated_at = NOW()
            WHERE user_id = %(user_id)s AND {OWED_SQL}
            RETURNING handback_attempts
            """,
            {"user_id": user_id},
        )
        row = await cur.fetchone()
        if row is None or row["handback_attempts"] < max_attempts:
            return False
        await cur.execute(
            """
            UPDATE home_away SET
                handed_back_through = last_away_at,
                away_since = NULL,
                last_handback = %(record)s::jsonb,
                handback_seen_at = NULL,
                handback_attempts = 0,
                next_handback_at = NULL,
                updated_at = NOW()
            WHERE user_id = %(user_id)s
            """,
            {"user_id": user_id, "record": Json(given_up)},
        )
        return True


async def defer_handback(user_id: str, seconds: float) -> None:
    """Put the user's hand-back behind the others for a while, without
    counting a failure: Home was busy, and the sweep should not keep finding
    it first."""
    async with get_db_connection() as conn, conn.cursor() as cur:
        await cur.execute(
            """
            UPDATE home_away SET
                next_handback_at = clock_timestamp() + make_interval(secs => %s),
                updated_at = NOW()
            WHERE user_id = %s
            """,
            (seconds, user_id),
        )


async def list_handbacks(user_id: str, limit: int = 100) -> List[Dict[str, Any]]:
    """The user's latest hand-backs, oldest first: how far each reached, the
    folder in Home it filled, and the paths (relative to the stand-in's
    folder) it carried there, sorted."""
    async with get_db_connection() as conn, conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(
            """
            SELECT through, folder, files FROM (
                SELECT through, folder, files FROM home_handbacks
                WHERE user_id = %s ORDER BY through DESC LIMIT %s
            ) latest ORDER BY through
            """,
            (user_id, limit),
        )
        return [{**r, "files": sorted(r["files"])} for r in await cur.fetchall()]


async def mark_handback_seen(user_id: str, handback_id: Optional[str] = None) -> None:
    """The user dismissed the notice; only the one they saw when named."""
    async with get_db_connection() as conn, conn.cursor() as cur:
        await cur.execute(
            """
            UPDATE home_away SET handback_seen_at = NOW(), updated_at = NOW()
            WHERE user_id = %s AND handback_seen_at IS NULL
              AND (%s::text IS NULL OR last_handback->>'handback_id' = %s::text)
            """,
            (user_id, handback_id, handback_id),
        )
