"""Price targets on a thread's market watch (``market_watch_targets``).

A target moves ``armed`` → ``fired`` (the price hit it), ``expired`` (its
window closed first) or ``cancelled`` (replaced, unwatched, or cancelled by
the user). Every transition is a guarded UPDATE, so the price monitor running
in each worker can race on one tick and exactly one wins.

Each target sends the thread at most two notices, ``hit`` and ``expiring``.
A notice is a ``market_target`` job in ``hook_outbox``, enqueued on the same
transaction as the transition that makes it due, so a claimed hit can never
lose its alert to a crash. Whoever puts a notice in front of the model, the
running turn or a wake turn, stamps it delivered with a guarded UPDATE, which
is what keeps a notice from landing twice.
"""

import logging
from datetime import datetime
from typing import Any, Collection, Dict, List, Literal, Optional, Sequence, Tuple

from psycopg.rows import dict_row

from src.server.database.pool import get_db_connection
from src.server.utils.pg_sanitize import SafeJson

logger = logging.getLogger(__name__)

HOOK_TYPE = "market_target"

Notice = Literal["hit", "expiring"]
TargetKind = Literal["above", "below", "pct_up", "pct_down"]

TARGET_COLUMNS = """
    target_id, conversation_thread_id, user_id, symbol, market, kind, value,
    pct_from, armed_price, status, expires_at, warn_at, warned_at, fired_at,
    fired_price, hit_delivered_at, hit_delivered_run_id, warn_delivered_at,
    warn_delivered_run_id, created_at, updated_at
"""


def ordering_key(thread_id: str) -> str:
    """Notices for one thread run in order, apart from the thread's other
    jobs: a parked notice must never hold up a subagent's report-back."""
    return f"{HOOK_TYPE}:{thread_id}"


# Whether the notice of the job ``o`` is still owed, as ``owed()`` in
# market_targets/delivery.py decides it: a hit until it is delivered, a
# reminder while its target is armed, unexpired and not yet reminded. The
# CASEs keep a malformed id from failing the statement; the executor drops
# that job.
MARKET_TARGET_OWED_SQL = """EXISTS (
    SELECT 1 FROM market_watch_targets t
    WHERE t.target_id = CASE
            WHEN o.payload->>'target_id'
                 ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
            THEN (o.payload->>'target_id')::uuid
        END
      AND CASE o.payload->>'notice'
            WHEN 'hit' THEN t.hit_delivered_at IS NULL
            WHEN 'expiring' THEN t.warn_delivered_at IS NULL
                                 AND t.status = 'armed'
                                 AND t.expires_at > NOW()
            ELSE false
        END
)"""


def idempotency_key(target_id: str, notice: Notice) -> str:
    return f"{HOOK_TYPE}:{target_id}:{notice}"


def _normalize(row: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    """String ids and float prices: rows cross into JSON payloads and tool
    text, where a UUID or a Decimal is a trap."""
    if row is None:
        return None
    out = dict(row)
    for key in ("target_id", "conversation_thread_id", "hit_delivered_run_id",
                "warn_delivered_run_id"):
        if out.get(key) is not None:
            out[key] = str(out[key])
    for key in ("value", "armed_price", "fired_price"):
        if out.get(key) is not None:
            out[key] = float(out[key])
    return out


async def _enqueue_notice(cur, target: Dict[str, Any], notice: Notice) -> None:
    thread_id = str(target["conversation_thread_id"])
    target_id = str(target["target_id"])
    await cur.execute(
        """
        INSERT INTO hook_outbox (
            run_id, conversation_thread_id, hook_type,
            payload, ordering_key, idempotency_key
        )
        VALUES (NULL, %s, %s, %s, %s, %s)
        ON CONFLICT (idempotency_key) DO NOTHING
        """,
        (
            thread_id,
            HOOK_TYPE,
            SafeJson({
                "target_id": target_id,
                "notice": notice,
                "thread_id": thread_id,
                "user_id": target["user_id"],
            }),
            ordering_key(thread_id),
            idempotency_key(target_id, notice),
        ),
    )


# ---------------------------------------------------------------------------
# Arming and reading (the agent's tool, the thread API)
# ---------------------------------------------------------------------------


class _CapReached(Exception):
    pass


async def arm_target(**kwargs: Any) -> Optional[Dict[str, Any]]:
    """Arm a target, replacing an armed one of the same kind on the listing,
    the symbol in its market.

    Returns the new row, or None when the thread already holds ``max_armed``
    other armed targets. The replace and the cap check share a transaction
    under a row lock on the thread, so two parallel tool calls in one turn
    can neither both pass the cap nor both insert the same kind. A refusal
    rolls the replace back with it: it must not cancel the target it meant
    to replace.
    """
    try:
        return await _arm(**kwargs)
    except _CapReached:
        return None


async def _arm(
    *,
    thread_id: str,
    user_id: str,
    symbol: str,
    market: str,
    kind: TargetKind,
    value: float,
    pct_from: Optional[str],
    armed_price: Optional[float],
    expires_at: datetime,
    warn_at: datetime,
    max_armed: int,
) -> Dict[str, Any]:
    async with get_db_connection() as conn:
        async with conn.transaction():
            async with conn.cursor(row_factory=dict_row) as cur:
                # NO KEY UPDATE still serializes two arms on one thread, but
                # lets through the KEY SHARE that hook_outbox's foreign key
                # takes on this row. fire_targets and claim_due_warnings hold
                # a target row and then enqueue a notice, so under FOR UPDATE
                # an arm waiting on that target row deadlocks with them.
                await cur.execute(
                    """
                    SELECT 1 FROM conversation_threads
                    WHERE conversation_thread_id = %s
                    FOR NO KEY UPDATE
                    """,
                    (thread_id,),
                )
                await cur.execute(
                    """
                    UPDATE market_watch_targets
                    SET status = 'cancelled', updated_at = NOW()
                    WHERE conversation_thread_id = %s AND symbol = %s
                      AND market = %s AND kind = %s AND status = 'armed'
                    """,
                    (thread_id, symbol, market, kind),
                )
                await cur.execute(
                    """
                    SELECT COUNT(*) AS n FROM market_watch_targets
                    WHERE conversation_thread_id = %s AND status = 'armed'
                      AND expires_at > NOW()
                    """,
                    (thread_id,),
                )
                if (await cur.fetchone())["n"] >= max_armed:
                    raise _CapReached()
                await cur.execute(
                    f"""
                    INSERT INTO market_watch_targets (
                        conversation_thread_id, user_id, symbol, market, kind,
                        value, pct_from, armed_price, expires_at, warn_at
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    RETURNING {TARGET_COLUMNS}
                    """,
                    (thread_id, user_id, symbol, market, kind, value, pct_from,
                     armed_price, expires_at, warn_at),
                )
                return _normalize(await cur.fetchone())


async def get_target(target_id: str) -> Optional[Dict[str, Any]]:
    async with get_db_connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                f"SELECT {TARGET_COLUMNS} FROM market_watch_targets WHERE target_id = %s",
                (target_id,),
            )
            return _normalize(await cur.fetchone())


async def list_thread_targets(
    thread_id: str, *, recent_seconds: int = 86400
) -> List[Dict[str, Any]]:
    """Armed targets plus those that settled within ``recent_seconds``,
    newest first: what the chip and the status panel show."""
    async with get_db_connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                f"""
                SELECT {TARGET_COLUMNS} FROM market_watch_targets
                WHERE conversation_thread_id = %s
                  AND (status = 'armed'
                       OR updated_at > NOW() - make_interval(secs => %s))
                ORDER BY created_at DESC
                """,
                (thread_id, recent_seconds),
            )
            return [_normalize(r) for r in await cur.fetchall()]


async def cancel_targets(
    thread_id: str,
    *,
    listings: Optional[List[Tuple[str, str]]] = None,
    target_id: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """Cancel armed targets on a thread: one by id, those on ``listings``
    (``(symbol, market)`` pairs), or all of them when neither is given.
    Returns the rows it cancelled.

    A symbol alone would reach both listings stored under it, HSI the US
    ticker and HSI the Hang Seng. One past its expiry is left for the sweep
    to close as expired, which is how every reader already shows it.

    Rows are locked in id order, as ``fire_targets`` locks them: cancelling
    several while a tick claims several of the same would otherwise deadlock."""
    clauses = [
        "conversation_thread_id = %s", "status = 'armed'", "expires_at > NOW()"
    ]
    params: List[Any] = [thread_id]
    if target_id is not None:
        clauses.append("target_id = %s")
        params.append(target_id)
    if listings is not None:
        clauses.append(
            "(symbol, market) IN (SELECT * FROM unnest(%s::text[], %s::text[]))"
        )
        params.extend([[symbol for symbol, _ in listings], [market for _, market in listings]])
    async with get_db_connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                f"""
                UPDATE market_watch_targets
                SET status = 'cancelled', updated_at = NOW()
                WHERE target_id IN (
                    SELECT target_id FROM market_watch_targets
                    WHERE {' AND '.join(clauses)}
                    ORDER BY target_id
                    FOR UPDATE
                )
                RETURNING {TARGET_COLUMNS}
                """,
                params,
            )
            return [_normalize(r) for r in await cur.fetchall()]


async def cancel_workspace_targets(workspace_id: str) -> List[Dict[str, Any]]:
    """Cancel the armed targets on every thread of a workspace; the rows it
    cancelled. Locked in id order, as ``cancel_targets`` locks them."""
    async with get_db_connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                f"""
                UPDATE market_watch_targets
                SET status = 'cancelled', updated_at = NOW()
                WHERE target_id IN (
                    SELECT t.target_id FROM market_watch_targets t
                    JOIN conversation_threads c
                      ON c.conversation_thread_id = t.conversation_thread_id
                    WHERE c.workspace_id = %s
                      AND t.status = 'armed' AND t.expires_at > NOW()
                    ORDER BY t.target_id
                    FOR UPDATE OF t
                )
                RETURNING {TARGET_COLUMNS}
                """,
                (workspace_id,),
            )
            return [_normalize(r) for r in await cur.fetchall()]


# ---------------------------------------------------------------------------
# The price monitor
# ---------------------------------------------------------------------------


async def list_armed_targets(
    thread_ids: Optional[Collection[str]] = None,
) -> List[Dict[str, Any]]:
    """Armed targets of every thread, or of ``thread_ids`` only: a change
    names the thread it touched, so a monitor re-reads that thread alone."""
    clauses = ["status = 'armed'", "expires_at > NOW()"]
    params: List[Any] = []
    if thread_ids is not None:
        clauses.append("conversation_thread_id = ANY(%s::uuid[])")
        params.append(list(thread_ids))
    async with get_db_connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                f"""
                SELECT {TARGET_COLUMNS} FROM market_watch_targets
                WHERE {' AND '.join(clauses)}
                """,
                params,
            )
            return [_normalize(r) for r in await cur.fetchall()]


async def fire_targets(
    target_ids: Sequence[str],
    fired_price: float,
    *,
    printed_at: Optional[datetime] = None,
) -> List[Dict[str, Any]]:
    """Claim the hits one price made and enqueue their notices, all in one
    transaction; the fired rows. A target missing from them is no longer
    armed: another worker won it, or it was cancelled or expired since the
    monitor loaded it.

    ``printed_at``, when the price traded, becomes ``fired_at``: the alert
    pairs that time with ``fired_price``, and a delayed feed prints well
    before the claim. Without it ``fired_at`` is the claim time, and it is
    never later than the claim, so a source whose clock runs ahead cannot
    date a hit in the future.

    Rows are locked in id order whatever plan reads them, so two workers
    claiming overlapping hits off one tick queue on each other rather than
    deadlock; the guard is checked again on the row a wait ends on.
    """
    if not target_ids:
        return []
    async with get_db_connection() as conn:
        async with conn.transaction():
            async with conn.cursor(row_factory=dict_row) as cur:
                await cur.execute(
                    f"""
                    UPDATE market_watch_targets
                    SET status = 'fired',
                        fired_at = LEAST(COALESCE(%s::timestamptz, NOW()), NOW()),
                        fired_price = %s,
                        updated_at = NOW()
                    WHERE target_id IN (
                        SELECT target_id FROM market_watch_targets
                        WHERE target_id = ANY(%s::uuid[]) AND status = 'armed'
                          AND expires_at > NOW()
                        ORDER BY target_id
                        FOR UPDATE
                    )
                    RETURNING {TARGET_COLUMNS}
                    """,
                    (printed_at, fired_price, list(target_ids)),
                )
                rows = await cur.fetchall()
                for row in rows:
                    await _enqueue_notice(cur, row, "hit")
                return [_normalize(r) for r in rows]


async def claim_due_warnings(limit: int = 100) -> List[Dict[str, Any]]:
    """Stamp armed targets whose warning time has come and enqueue their
    ``expiring`` notices; the claimed rows. ``SKIP LOCKED`` lets every
    worker's sweep run at once without two claiming the same row."""
    async with get_db_connection() as conn:
        async with conn.transaction():
            async with conn.cursor(row_factory=dict_row) as cur:
                await cur.execute(
                    f"""
                    UPDATE market_watch_targets
                    SET warned_at = NOW(), updated_at = NOW()
                    WHERE target_id IN (
                        SELECT target_id FROM market_watch_targets
                        WHERE status = 'armed' AND warned_at IS NULL
                          AND warn_at <= NOW() AND expires_at > NOW()
                        ORDER BY warn_at
                        LIMIT %s
                        FOR UPDATE SKIP LOCKED
                    )
                    RETURNING {TARGET_COLUMNS}
                    """,
                    (limit,),
                )
                rows = await cur.fetchall()
                for row in rows:
                    await _enqueue_notice(cur, row, "expiring")
                return [_normalize(r) for r in rows]


async def expire_due_targets() -> int:
    """Close armed targets whose window has passed; how many it closed.

    Rows are locked in id order, as ``fire_targets`` and ``cancel_targets``
    lock them, so a sweep closing several never deadlocks with a claim or a
    cancel holding some of the same."""
    async with get_db_connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                """
                UPDATE market_watch_targets
                SET status = 'expired', updated_at = NOW()
                WHERE target_id IN (
                    SELECT target_id FROM market_watch_targets
                    WHERE status = 'armed' AND expires_at <= NOW()
                    ORDER BY target_id
                    FOR UPDATE
                )
                """
            )
            return cur.rowcount


# ---------------------------------------------------------------------------
# Delivery
# ---------------------------------------------------------------------------


async def mark_delivered(target_id: str, notice: Notice, run_id: Optional[str]) -> bool:
    """Stamp a notice delivered to ``run_id``; False when it already was.

    What keeps a notice from reaching the model twice: a job finding its
    notice stamped drops it. A running turn stamps once the model step that
    reads the notice has begun, before closing its job. A wake stamps once
    its turn is dispatched, since a stamp ahead of a POST that never landed
    would lose the notice; a crash in between is healed by the next claim
    adopting the run by its request key.
    """
    prefix = "hit" if notice == "hit" else "warn"
    async with get_db_connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                f"""
                UPDATE market_watch_targets
                SET {prefix}_delivered_at = NOW(),
                    {prefix}_delivered_run_id = %s,
                    updated_at = NOW()
                WHERE target_id = %s AND {prefix}_delivered_at IS NULL
                """,
                (run_id, target_id),
            )
            return cur.rowcount == 1

