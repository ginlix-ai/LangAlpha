"""Cancelling wakeups (the agent, the user, the setting, archive, workspace
delete), and the cancel notice the agent is owed for the user's."""

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, List, Mapping, Optional, Tuple

from psycopg.rows import dict_row

from src.server.database.pool import get_db_connection
from src.server.database.wakeups.contract import (
    CANCELLERS,
    FORCED_CANCELLERS,
    TOO_LATE_SQL,
    WAKEUP_COLUMNS,
    CancelledBy,
    Outcome,
    attempt_sql,
    canonical_uuid,
    cols,
    is_uuid,
    jobs_of_sql,
    normalize,
    open_sql,
    unread_fire_sql,
    uuids,
)


@dataclass(frozen=True)
class CancelOutcome:
    wakeup_id: str
    short_id: str
    outcome: Outcome
    row: Optional[dict]


def past_window(row: Mapping[str, Any], now: datetime) -> bool:
    expires_at = row.get("expires_at")
    return expires_at is not None and expires_at <= now


def cancel_decision(
    row: Mapping[str, Any], *, late: bool, forced: bool, now: datetime
) -> Outcome:
    """What a cancel does to ``row``, given whether one of its unread fires
    is past recall (``late``, as ``TOO_LATE_SQL`` reads its job) and whether
    the cancel ends it outright (``forced``).

    A wakeup that can still fire is stopped, and a fire past recall still
    arrives (``cancelled_in_flight``). One with nothing left to fire is
    called back while its unread notice is within reach, and left as it is
    once that notice is past recall (``too_late``). A forced cancel ends both
    outright. The thread API asks this too, for whether to offer a cancel.
    """
    unread = row["last_fired_seq"] > row["delivered_seq"]
    late = late and unread and not forced
    if row["status"] == "armed" and not past_window(row, now):
        return "cancelled_in_flight" if late else "cancelled"
    if row["status"] in ("fired", "finished") and unread:
        return "too_late" if late else "cancelled"
    return "not_pending"


async def _cancel_locked(
    cur, where: str, params: Dict[str, Any], *, by: CancelledBy
) -> List[Tuple[Dict[str, Any], Outcome]]:
    """Cancel the rows ``where`` selects, in the caller's transaction; each
    row as it now stands with what the cancel did to it, an order's with
    ``attempt``.

    The rows are locked first and then their notice jobs, so no claim, hold
    or rider stamp can slip between reading a job's state and deciding on
    it. A job past recall belongs to the turn or executor that has it, which
    settles it. Every other job of a row the cancel ends is closed with it,
    a dead one included so the revive sweep cannot bring it back, and a
    parked one so it stops holding up the thread's later notices.
    """
    if by not in CANCELLERS:
        raise ValueError(f"unknown canceller {by!r}")
    await cur.execute(
        f"""
        SELECT {cols("w")}, {attempt_sql("w")}, NOW() AS now
        FROM wakeups w
        WHERE {where}
        ORDER BY w.wakeup_id
        FOR UPDATE OF w
        """,
        params,
    )
    rows = await cur.fetchall()
    if not rows:
        return []
    await cur.execute(
        f"""
        SELECT o.hook_outbox_id, w.wakeup_id,
               (o.status <> 'dead' AND {TOO_LATE_SQL}) AS too_late,
               COALESCE({unread_fire_sql("o", "w")}, false) AS unread_fire
        FROM hook_outbox o
        JOIN wakeups w ON {jobs_of_sql("o", "w")}
        WHERE w.wakeup_id = ANY(%s::uuid[])
          AND o.status IN ('pending', 'claimed', 'dead')
        ORDER BY (o.payload ? 'rides'), o.hook_outbox_id
        FOR UPDATE OF o
        """,
        ([str(r["wakeup_id"]) for r in rows],),
    )
    jobs = await cur.fetchall()
    forced = by in FORCED_CANCELLERS
    # Only a fire can be in flight: a lapse reminder on a cancelled price
    # would only tell the agent to re-arm what the user just stopped.
    late = {str(j["wakeup_id"]) for j in jobs if j["too_late"] and j["unread_fire"]}
    decided: Dict[str, Outcome] = {
        str(row["wakeup_id"]): cancel_decision(
            row, late=str(row["wakeup_id"]) in late, forced=forced, now=row["now"]
        )
        for row in rows
    }

    ending = [wid for wid, outcome in decided.items()
              if outcome in ("cancelled", "cancelled_in_flight")]
    after: Dict[str, Dict[str, Any]] = {}
    if ending:
        await cur.execute(
            f"""
            UPDATE wakeups
            SET status = 'cancelled', cancelled_by = %s, cancelled_at = NOW(),
                updated_at = NOW()
            WHERE wakeup_id = ANY(%s::uuid[])
            RETURNING {WAKEUP_COLUMNS}
            """,
            (by, ending),
        )
        after = {str(r["wakeup_id"]): r for r in await cur.fetchall()}
        closing = [j["hook_outbox_id"] for j in jobs
                   if not j["too_late"] and str(j["wakeup_id"]) in after]
        if closing:
            await cur.execute(
                """
                UPDATE hook_outbox
                SET status = 'done', completed_at = NOW(), next_retry_at = NULL
                WHERE hook_outbox_id = ANY(%s) AND status IN ('pending', 'dead')
                """,
                (closing,),
            )
    out = []
    for row in rows:
        wid = str(row["wakeup_id"])
        if wid in after:
            current = {**after[wid], "attempt": row["attempt"]}
        else:
            current = {k: v for k, v in row.items() if k != "now"}
        out.append((normalize(current), decided[wid]))
    return out


async def cancel_wakeups(
    thread_id: str, ids: Optional[List[str]], *, by: CancelledBy
) -> List[CancelOutcome]:
    """Cancel wakeups on a thread by id (a ``wakeup_id`` or a ``short_id``),
    or every open one when ``ids`` is None; one outcome per id in the order
    given, or per open row.

    An id that names nothing on this thread comes back as given,
    ``not_pending`` with no row: an id from another thread is no id here.
    """
    if ids is not None and not ids:
        return []
    params: Dict[str, Any] = {"thread": thread_id}
    if ids is None:
        where = f"w.conversation_thread_id = %(thread)s AND {open_sql('w')}"
    else:
        params["uuids"] = uuids(ids)
        params["shorts"] = [str(i).strip().lower() for i in ids if not is_uuid(i)]
        where = (
            "w.conversation_thread_id = %(thread)s"
            " AND (w.wakeup_id = ANY(%(uuids)s::uuid[])"
            " OR w.short_id = ANY(%(shorts)s::text[]))"
        )
    async with get_db_connection() as conn:
        async with conn.transaction():
            async with conn.cursor(row_factory=dict_row) as cur:
                done = await _cancel_locked(cur, where, params, by=by)
    if ids is None:
        return [
            CancelOutcome(row["wakeup_id"], row["short_id"], outcome, row)
            for row, outcome in done
        ]
    by_id = {}
    for row, outcome in done:
        by_id[row["wakeup_id"]] = (row, outcome)
        by_id[row["short_id"]] = (row, outcome)
    results: List[CancelOutcome] = []
    seen = set()
    for given in ids:
        key = str(given).strip()
        hit = by_id.get(canonical_uuid(key) or key.lower())
        if hit is None:
            results.append(CancelOutcome(key, key, "not_pending", None))
            continue
        row, outcome = hit
        if row["wakeup_id"] in seen:
            continue
        seen.add(row["wakeup_id"])
        results.append(CancelOutcome(row["wakeup_id"], row["short_id"], outcome, row))
    return results


_SCOPES = {
    "user_id": "w.user_id = %(scope)s",
    "thread_id": "w.conversation_thread_id = %(scope)s",
    "workspace_id": (
        "w.conversation_thread_id IN (SELECT conversation_thread_id"
        " FROM conversation_threads WHERE workspace_id = %(scope)s)"
    ),
}


async def cancel_open(
    *,
    by: CancelledBy,
    user_id: Optional[str] = None,
    thread_id: Optional[str] = None,
    workspace_id: Optional[str] = None,
) -> Dict[str, List[CancelOutcome]]:
    """Cancel every open wakeup of one user, thread or workspace, in one
    transaction; the outcomes by thread."""
    given = {
        name: value
        for name, value in (
            ("user_id", user_id), ("thread_id", thread_id), ("workspace_id", workspace_id)
        )
        if value is not None
    }
    if len(given) != 1:
        raise ValueError("cancel_open takes exactly one of user_id, thread_id, workspace_id")
    [(name, value)] = given.items()
    where = f"{_SCOPES[name]} AND {open_sql('w')}"
    async with get_db_connection() as conn:
        async with conn.transaction():
            async with conn.cursor(row_factory=dict_row) as cur:
                done = await _cancel_locked(cur, where, {"scope": value}, by=by)
    out: Dict[str, List[CancelOutcome]] = {}
    for row, outcome in done:
        out.setdefault(row["conversation_thread_id"], []).append(
            CancelOutcome(row["wakeup_id"], row["short_id"], outcome, row)
        )
    return out


async def user_cancels(thread_id: str) -> List[Dict[str, Any]]:
    """The cancels on a thread by the user or the setting, in the order they
    happened, each order's with ``attempt``: every one, since the agent's
    history, not this table, says which it was told of."""
    async with get_db_connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                f"""
                SELECT {cols("w")}, {attempt_sql("w")} FROM wakeups w
                WHERE w.conversation_thread_id = %s
                  AND w.cancelled_by IN ('user', 'setting_off')
                ORDER BY w.cancelled_at, w.created_at
                """,
                (thread_id,),
            )
            return [normalize(r) for r in await cur.fetchall()]
