"""The relay's record of paid vendor calls, from the hold at admission to the
charge on a thread.

Postgres rather than Redis, unlike the file mount's per-call reports, because
a row here is money owed: losing one to a cache eviction is unbilled usage.
A row is collected (``collected_at``) in the transaction that writes its
usage row, so it is charged exactly once.
"""

from __future__ import annotations

import math
import time
from collections.abc import Callable, Sequence
from typing import Any

from psycopg import AsyncConnection
from psycopg.rows import dict_row

from src.server.database.pool import get_db_connection
from src.server.database.sql_fences import advisory_key

# A hold whose call never settled it (the worker died mid-call) is charged at
# its estimate once it is this old: a relayed call ends within a minute.
_STALE_HOLD = "interval '5 minutes'"

# How much earlier than the balance read a charge still counts against it.
# ``collected_at`` is stamped by the charging transaction's last statement,
# so a charge is visible to the auth service within one commit of its stamp;
# this covers that commit and the round trip carrying the read's age here.
_READ_MARGIN_S = 1.0

# A row as charged: what it cost, and the thread it lands on with that
# thread's workspace, NULL when the thread is unknown or gone.
_CHARGED = """
    RETURNING m.id, m.vendor, m.tool, m.vendor_credits::float AS vendor_credits,
              m.thread_id::text AS thread_id,
              (SELECT t.workspace_id::text FROM conversation_threads t
               WHERE t.conversation_thread_id = m.thread_id) AS thread_workspace_id
"""


async def reserve(
    *,
    user_id: str,
    computer_id: str | None,
    workspace_id: str | None,
    call_id: str | None,
    thread_id: str | None,
    vendor: str,
    tool: str,
    estimate: float,
    owed_cap: float,
    allowed: float | None,
    balance_read_at: float | None = None,
    on_hold: Callable[[int], None] | None = None,
) -> tuple[str, int | None]:
    """Admit one paid call against what the user owes but has not been
    charged, and hold its estimated price while it runs.

    Returns ``(verdict, hold id)``. The verdict is ``ok``, ``charges_pending``
    (uncharged spend is at ``owed_cap``, or with no ``allowed`` this call
    would take it past), ``exhausted`` (it already uses up ``allowed``) or
    ``exceeds`` (this call would take it past ``allowed``, or with no
    ``allowed`` it alone is over ``owed_cap``); ``allowed`` is in vendor
    credits, None for no limit. With a balance the balance bounds the
    call, so a funded user's one large call still passes. Every admitted call
    gets a hold, a free one too: it is what a command's result waits on. The
    lock makes the check and the hold one step per user, so calls made at
    once each see the others' holds. ``on_hold`` gets the hold's id before
    the commit, for a caller cancelled while it commits.

    ``balance_read_at`` is when the read of ``allowed`` began, as
    ``time.monotonic()``. A call charged since then (by another worker,
    between that read and this lock) is no longer owed but may be missing
    from ``allowed``, so it is counted against ``allowed`` too; the cap
    counts only what is owed. The instant is carried into the database's
    clock as an age taken under the lock, so the comparison is with
    ``collected_at`` in one clock, less ``_READ_MARGIN_S``.
    """
    # A negative hold would offset what the user owes and let other calls
    # past the balance and the cap.
    if not (math.isfinite(estimate) and estimate >= 0):
        raise ValueError(f"a hold needs a finite estimate of at least 0, not {estimate!r}")
    # Checked in the units a call is charged, its credits rounded up, so a
    # fractional price is never admitted on headroom its charge lacks.
    units = math.ceil(estimate)
    async with get_db_connection() as conn, conn.transaction():
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                "SELECT pg_advisory_xact_lock(%s)",
                (advisory_key("metered_vendor_owed", user_id, vendor),),
            )
            await cur.execute(
                """
                SELECT
                    COALESCE(SUM(CEIL(vendor_credits)) FILTER (WHERE collected_at IS NULL), 0)
                        AS owed,
                    COALESCE(
                        SUM(CEIL(vendor_credits)) FILTER (WHERE collected_at >= balance.read_at), 0
                    ) AS unseen
                FROM metered_vendor_calls,
                     (SELECT clock_timestamp() - make_interval(secs => %(age)s) AS read_at)
                         AS balance
                WHERE user_id = %(user_id)s AND vendor = %(vendor)s
                  AND (collected_at IS NULL OR collected_at >= balance.read_at)
                """,
                {
                    "user_id": user_id,
                    "vendor": vendor,
                    "age": None
                    if balance_read_at is None
                    else time.monotonic() - balance_read_at + _READ_MARGIN_S,
                },
            )
            row = await cur.fetchone()
            owed = float(row["owed"] if row else 0)
            # Against the balance: what is owed, and what was charged after
            # the balance was read.
            spent = owed + float((row or {}).get("unseen") or 0)
            if owed >= owed_cap:
                return "charges_pending", None
            if allowed is None and owed + units > owed_cap:
                # Alone over the cap, the call can never pass: a smaller one
                # might. Otherwise it passes once earlier calls are charged.
                return ("exceeds" if units > owed_cap else "charges_pending"), None
            if allowed is not None and spent > 0 and spent >= allowed:
                return "exhausted", None
            if allowed is not None and spent + units > allowed:
                return "exceeds", None
            await cur.execute(
                """
                INSERT INTO metered_vendor_calls
                    (user_id, computer_id, workspace_id, call_id, thread_id,
                     vendor, tool, vendor_credits, status)
                VALUES (%s, %s::uuid, %s::uuid, %s, %s::uuid, %s, %s, %s, 'held')
                RETURNING id
                """,
                (
                    user_id, computer_id, workspace_id, call_id, thread_id,
                    vendor, tool, estimate,
                ),
            )
            hold_id = int((await cur.fetchone())["id"])
            if on_hold is not None:
                on_hold(hold_id)
            return "ok", hold_id


async def stamp_collected(conn: AsyncConnection, ids: Sequence[int]) -> None:
    """Restamp rows this transaction charged with the time of its last
    statement. ``NOW()`` is when the transaction began, but the auth service
    sees the charge only once it commits, which is what a balance read races
    (see ``reserve``). Called last, on the charging transaction's connection.
    """
    if ids:
        await conn.execute(
            "UPDATE metered_vendor_calls SET collected_at = clock_timestamp()"
            " WHERE id = ANY(%s) AND collected_at IS NOT NULL",
            (list(ids),),
        )


async def mark_job(hold_id: int, job_ref: str, credits: float | None) -> None:
    """Name the job a held call reports on before it settles, so that if its
    settle never lands the sweep still nets it against the job's polls.

    ``credits`` is the job's running total as the reply reported it, None
    when it reported none. It replaces the estimate on the hold because the
    sweep nets a held row's ``vendor_credits`` against what the job charged:
    left at the estimate (one credit for a crawl poll), the job's growth this
    poll reported would be lost with its settle."""
    async with get_db_connection() as conn:
        await conn.execute(
            "UPDATE metered_vendor_calls SET job_ref = %s,"
            " vendor_credits = COALESCE(%s, vendor_credits)"
            " WHERE id = %s AND status = 'held'",
            (job_ref, credits, hold_id),
        )


async def release(hold_id: int) -> None:
    """Drop a hold whose call cost nothing: refused, failed, never sent."""
    async with get_db_connection() as conn:
        await conn.execute(
            "DELETE FROM metered_vendor_calls WHERE id = %s AND status = 'held'",
            (hold_id,),
        )


async def settle(
    conn: AsyncConnection, hold_id: int, *, credits: float | None, job_ref: str | None
) -> dict[str, Any] | None:
    """Settle a hold on the caller's connection: at ``credits`` when the reply
    said what the call cost, else at its estimate. A row whose thread exists
    is collected here, so the usage row the caller writes in the same
    transaction is its only charge. Returns the row as charged, or None when
    nothing is owed: the hold is gone (taken as stale), or the call cost
    nothing.

    With ``job_ref`` the cost is the job's running total, so only its growth
    over what the job's earlier rows recorded is kept. The advisory lock
    serializes two polls of one job landing on different workers, which
    would otherwise both read the old total and bill the same growth twice.
    """
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(
            """
            SELECT user_id, vendor, vendor_credits::float AS estimate
            FROM metered_vendor_calls WHERE id = %s AND status = 'held'
            FOR UPDATE
            """,
            (hold_id,),
        )
        hold = await cur.fetchone()
        if hold is None:
            return None
        cost = hold["estimate"] if credits is None else credits
        if job_ref is not None:
            await cur.execute(
                "SELECT pg_advisory_xact_lock(%s)",
                (advisory_key("metered_vendor_job", hold["user_id"], hold["vendor"], job_ref),),
            )
            await cur.execute(
                """
                SELECT COALESCE(SUM(vendor_credits), 0)::float AS seen
                FROM metered_vendor_calls
                WHERE user_id = %s AND vendor = %s AND job_ref = %s
                  AND id <> %s AND status <> 'held'
                """,
                (hold["user_id"], hold["vendor"], job_ref, hold_id),
            )
            seen = await cur.fetchone()
            cost -= seen["seen"] if seen else 0.0
        if cost <= 0:
            await cur.execute("DELETE FROM metered_vendor_calls WHERE id = %s", (hold_id,))
            return None
        await cur.execute(
            f"""
            UPDATE metered_vendor_calls m
            SET vendor_credits = %s, job_ref = %s, status = %s,
                collected_at = CASE WHEN EXISTS (
                    SELECT 1 FROM conversation_threads t
                    WHERE t.conversation_thread_id = m.thread_id) THEN NOW() END
            WHERE m.id = %s
            {_CHARGED}
            """,
            (cost, job_ref, "estimated" if credits is None else "settled", hold_id),
        )
        return await cur.fetchone()


# What the turn-start sweep takes: everything not yet charged except a hold
# still in flight.
_OWED = f"""
    m.user_id = %s AND m.collected_at IS NULL
    AND (m.status <> 'held' OR m.created_at < NOW() - {_STALE_HOLD})
"""


async def has_owed(user_id: str) -> bool:
    """Whether ``user_id`` has anything for :func:`take_owed` to take: one
    indexed probe, so a turn with nothing owed pays only this."""
    async with get_db_connection() as conn:
        cur = await conn.execute(
            f"SELECT EXISTS (SELECT 1 FROM metered_vendor_calls m WHERE {_OWED})",
            (user_id,),
        )
        row = await cur.fetchone()
        return bool(row and row[0])


async def _net_stale_jobs(cur, user_id: str, owed: list[dict[str, Any]]) -> None:
    """Net the stale holds that name a vendor job against what the job has
    charged, as a settle nets a poll's running total. Each job's holds are
    taken together, oldest first, so each counts the ones before it, and
    under the job's lock, so a poll settling meanwhile is either seen whole
    or waits for this sweep. A hold netted to nothing is dropped, as a settle
    drops a poll that added nothing."""
    jobs: dict[tuple[str, str], list[dict[str, Any]]] = {}
    for row in owed:
        if row["status"] == "held" and row["job_ref"] is not None:
            jobs.setdefault((row["vendor"], row["job_ref"]), []).append(row)
    for (vendor, job_ref), holds in sorted(jobs.items()):
        await cur.execute(
            "SELECT pg_advisory_xact_lock(%s)",
            (advisory_key("metered_vendor_job", user_id, vendor, job_ref),),
        )
        await cur.execute(
            """
            SELECT COALESCE(SUM(vendor_credits), 0)::float AS charged
            FROM metered_vendor_calls
            WHERE user_id = %s AND vendor = %s AND job_ref = %s AND status <> 'held'
            """,
            (user_id, vendor, job_ref),
        )
        charged = (await cur.fetchone())["charged"]
        for hold in sorted(holds, key=lambda r: (r["created_at"], r["id"])):
            cost = hold["vendor_credits"] - charged
            if cost <= 0:
                await cur.execute("DELETE FROM metered_vendor_calls WHERE id = %s", (hold["id"],))
                continue
            await cur.execute(
                "UPDATE metered_vendor_calls SET vendor_credits = %s, status = 'estimated'"
                " WHERE id = %s",
                (cost, hold["id"]),
            )
            charged += cost


async def take_owed(conn: AsyncConnection, user_id: str) -> list[dict[str, Any]]:
    """Take what ``user_id`` spent that no settle charged: calls with no
    thread known or whose thread is gone, and holds gone stale, at their
    estimate. On the caller's connection, so the charge it writes commits
    with the take; a call still in flight keeps its hold for its own settle.

    Each row also names the workspace the call ran in and its most recently
    active thread (``workspace_thread_id``), NULL when it has none left, for
    a call whose own thread is unknown or gone to land on.

    Locks in the order a settle does, rows before a job's lock: every row
    taken is locked first, by id, and only those rows are taken, so a hold
    turning stale midway is never locked after a job lock its settle may
    hold while waiting on that row.
    """
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(
            f"""
            SELECT m.id, m.vendor, m.job_ref, m.status, m.created_at,
                   m.vendor_credits::float AS vendor_credits
            FROM metered_vendor_calls m
            WHERE {_OWED}
            ORDER BY m.id
            FOR UPDATE
            """,
            (user_id,),
        )
        owed = list(await cur.fetchall())
        if not owed:
            return []
        await _net_stale_jobs(cur, user_id, owed)
        await cur.execute(
            f"""
            UPDATE metered_vendor_calls m
            SET collected_at = NOW(),
                status = CASE WHEN m.status = 'held' THEN 'estimated' ELSE m.status END
            WHERE m.id = ANY(%s) AND m.collected_at IS NULL
            {_CHARGED},
              m.workspace_id::text AS workspace_id,
              (SELECT t.conversation_thread_id::text
               FROM conversation_threads t
               JOIN workspaces w ON w.workspace_id = t.workspace_id
               WHERE t.workspace_id = m.workspace_id AND t.archived_at IS NULL
                 AND w.user_id = m.user_id AND w.status <> 'deleted'
               ORDER BY t.updated_at DESC NULLS LAST
               LIMIT 1) AS workspace_thread_id
            """,
            ([row["id"] for row in owed],),
        )
        return list(await cur.fetchall())


async def calls_of(user_id: str, call_id: str) -> list[dict[str, Any]]:
    """The paid calls ``user_id``'s command running under ``call_id`` made,
    held or charged. By user too: the sandbox picks its call ids."""
    async with get_db_connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                """
                SELECT vendor, tool, vendor_credits::float AS vendor_credits, status
                FROM metered_vendor_calls WHERE call_id = %s AND user_id = %s ORDER BY id
                """,
                (call_id, user_id),
            )
            return list(await cur.fetchall())
