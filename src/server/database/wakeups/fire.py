"""Firing wakeups (the sweep, the price monitor, order reconciliation) and
stamping their notices delivered."""

from datetime import datetime
from typing import Any, Dict, List, Optional, Sequence, Tuple

from psycopg.rows import dict_row

from src.server.database.pool import get_db_connection
from src.server.database.runs.outbox import enqueue_hooks
from src.server.database.wakeups.contract import (
    HOOK_TYPE,
    WAKEUP_COLUMNS,
    WAKEUP_OWED_SQL,
    Notice,
    cols,
    is_uuid,
    normalize,
    notice_job,
    pending_sql,
    uuids,
)
from src.server.services.brokerage_orders.models import TERMINAL_STATUSES

# The order states a wakeup reports as its end; a fill short of one is
# reported too, as ``partial``.
_ORDER_ENDS = ", ".join(f"'{status}'" for status in TERMINAL_STATUSES)


async def _enqueue(conn, notices: List[Tuple[Dict[str, Any], Notice, int]]) -> None:
    """Enqueue each row's notice in the order the rows were set, so notices
    that come due together reach the thread in that order: each insert is
    dated by the clock, not by the transaction."""
    for row, notice, seq in sorted(
        notices, key=lambda n: (n[0]["created_at"], str(n[0]["wakeup_id"]))
    ):
        await enqueue_hooks(
            conn,
            thread_id=str(row["conversation_thread_id"]),
            jobs=[notice_job(row, notice, seq)],
        )


async def fire_due_slots(*, limit: int = 50) -> List[Dict[str, Any]]:
    """Consume every slot that has come due, firing or skipping each; the
    rows as consumed, each with the ``seq`` of its slot and whether it
    ``fired``.

    A slot fires unless an earlier fire is still unread and the slot is not
    the series' last, so at most one fire waits per series and the last one
    always arrives to say it is the last. The waiting fire reports the slots
    consumed after it as skipped. Batches repeat until none comes back, so a
    pass after downtime consumes every slot it missed, the middle ones as
    skips, and each batch commits on its own so the other workers' sweeps
    take rows alongside it. ``SKIP LOCKED`` keeps them off each other's rows,
    and the guard on ``fires_done`` keeps a slot from being consumed twice.
    """
    consumed: List[Dict[str, Any]] = []
    while True:
        batch = await _fire_due_batch(limit)
        if not batch:
            return consumed
        consumed.extend(batch)


async def _fire_due_batch(limit: int) -> List[Dict[str, Any]]:
    async with get_db_connection() as conn:
        async with conn.transaction():
            async with conn.cursor(row_factory=dict_row) as cur:
                # Every right-hand side reads the row as it was, so ``fire`` is
                # one expression repeated rather than a column set first.
                fire = (
                    "NOT (w.last_fired_seq > w.delivered_seq"
                    " AND w.fires_done + 1 < cardinality(w.schedule))"
                )
                await cur.execute(
                    f"""
                    WITH due AS (
                        SELECT wakeup_id, fires_done FROM wakeups
                        WHERE status = 'armed' AND kind IN ('at', 'cron')
                          AND next_fire_at <= NOW()
                        ORDER BY next_fire_at, created_at, wakeup_id
                        LIMIT %s
                        FOR UPDATE SKIP LOCKED
                    )
                    UPDATE wakeups w
                    SET fires_done = w.fires_done + 1,
                        last_fired_seq = CASE WHEN {fire}
                            THEN w.fires_done + 1 ELSE w.last_fired_seq END,
                        last_fired_at = CASE WHEN {fire}
                            THEN w.schedule[w.fires_done + 1] ELSE w.last_fired_at END,
                        next_fire_at = w.schedule[w.fires_done + 2],
                        status = CASE
                            WHEN w.fires_done + 1 < cardinality(w.schedule) THEN w.status
                            WHEN w.kind = 'at' THEN 'fired'
                            ELSE 'finished'
                        END,
                        updated_at = NOW()
                    FROM due
                    WHERE w.wakeup_id = due.wakeup_id
                      AND w.fires_done = due.fires_done
                    RETURNING {cols("w")},
                              w.fires_done AS seq,
                              w.last_fired_seq = w.fires_done AS fired
                    """,
                    (limit,),
                )
                rows = await cur.fetchall()
                await _enqueue(conn, [(r, "fire", r["seq"]) for r in rows if r["fired"]])
                return [normalize(r) for r in rows]


async def fire_price_wakeups(
    wakeup_ids: Sequence[str],
    price: float,
    *,
    printed_at: Optional[datetime] = None,
) -> List[Dict[str, Any]]:
    """Claim the price wakeups one print met and enqueue their notices, all
    in one transaction; the fired rows. One missing from them is no longer
    armed: another worker won it, or it was cancelled or lapsed since the
    monitor loaded it.

    ``printed_at``, when the price traded, is the fire's time, since a
    delayed feed prints well before the claim, but never later than the
    claim, so a source whose clock runs ahead cannot date a fire in the
    future. Rows are locked in id order, so two workers claiming overlapping
    hits off one tick queue on each other rather than deadlock.
    """
    ids = uuids(wakeup_ids)
    if not ids:
        return []
    async with get_db_connection() as conn:
        async with conn.transaction():
            async with conn.cursor(row_factory=dict_row) as cur:
                await cur.execute(
                    f"""
                    UPDATE wakeups
                    SET status = 'fired', fires_done = 1, last_fired_seq = 1,
                        fired_price = %s,
                        last_fired_at = LEAST(COALESCE(%s::timestamptz, NOW()), NOW()),
                        updated_at = NOW()
                    WHERE wakeup_id IN (
                        SELECT wakeup_id FROM wakeups
                        WHERE wakeup_id = ANY(%s::uuid[]) AND kind = 'price'
                          AND {pending_sql()}
                        ORDER BY wakeup_id
                        FOR UPDATE
                    )
                    RETURNING {WAKEUP_COLUMNS}
                    """,
                    (price, printed_at, ids),
                )
                rows = await cur.fetchall()
                await _enqueue(conn, [(r, "fire", 1) for r in rows])
                return [normalize(r) for r in rows]


def _order_fires_sql(w: str, a: str) -> str:
    """An order wakeup is due on any fill past what had filled when it was
    set, or on any end. Measured against the stored baseline rather than the
    row's previous value, so the write that moved the order needs no
    before-image and a later read of the same state fires nothing."""
    return (
        f"(COALESCE({a}.filled_qty, 0) > COALESCE({w}.baseline_filled_qty, 0)"
        f" OR {a}.status IN ({_ORDER_ENDS}))"
    )


async def fire_order_wakeups(
    attempt_ids: Optional[Sequence[str]] = None, *, conn=None
) -> List[Dict[str, Any]]:
    """Fire the armed order wakeups on ``attempt_ids`` (every armed one with
    None) whose order filled or ended; the fired rows, each with
    ``order_outcome``, the order as it stood when it fired.

    Reconciliation passes its ``conn`` so its observation and the fire it
    causes commit together; it is the only writer that moves an open order,
    since every other ledger write is guarded on a state no wakeup watches.
    The sweep calls this with None for what that fire cannot see, a wakeup
    set while the observation committed or a fire its savepoint lost, and
    the guard on ``status`` keeps the two, and every worker, to one notice. The order
    can only have left an open state, so ``rejected_by_user`` and
    ``refused``, which no open order reaches, never come up.
    """
    if attempt_ids is not None:
        attempt_ids = uuids(attempt_ids)
        if not attempt_ids:
            return []
    scope = "" if attempt_ids is None else "AND w2.attempt_id = ANY(%(attempts)s::uuid[])"
    async with get_db_connection(conn) as conn:
        async with conn.transaction():
            async with conn.cursor(row_factory=dict_row) as cur:
                await cur.execute(
                    f"""
                    UPDATE wakeups w
                    SET status = 'fired', fires_done = 1, last_fired_seq = 1,
                        last_fired_at = NOW(),
                        order_outcome = jsonb_build_object(
                            'outcome', CASE
                                WHEN a.status = 'filled' THEN 'filled'
                                WHEN a.status = 'cancelled'
                                     AND COALESCE(a.filled_qty, 0) > 0
                                    THEN 'partial_cancelled'
                                WHEN a.status = 'cancelled' THEN 'cancelled'
                                WHEN a.status = 'rejected_by_vendor' THEN 'rejected'
                                WHEN a.status = 'expired'
                                     AND COALESCE(a.filled_qty, 0) > 0
                                    THEN 'partial_expired'
                                WHEN a.status = 'expired' THEN 'expired'
                                -- A staged instruction the vendor's own list
                                -- reported confirmed ends with no unlisted
                                -- stamp; one that only left the list keeps it.
                                WHEN a.status = 'handed_off'
                                     AND a.unlisted_at IS NULL THEN 'confirmed'
                                WHEN a.status = 'handed_off' THEN 'handed_off'
                                WHEN a.status IN ({_ORDER_ENDS}) THEN 'failed'
                                ELSE 'partial'
                            END,
                            'status', a.status,
                            'filled_qty', a.filled_qty,
                            'qty', CASE WHEN (a.order_json->>'qty') ~ '^[0-9]+(\\.[0-9]+)?$'
                                        THEN (a.order_json->>'qty')::numeric END,
                            'avg_fill_price', a.avg_fill_price,
                            'reason', a.failure->>'message'),
                        updated_at = NOW()
                    FROM order_attempts a
                    WHERE w.wakeup_id IN (
                            SELECT w2.wakeup_id FROM wakeups w2
                            JOIN order_attempts a2 ON a2.attempt_id = w2.attempt_id
                            WHERE w2.kind = 'order' AND {pending_sql("w2")} {scope}
                              AND {_order_fires_sql("w2", "a2")}
                            ORDER BY w2.wakeup_id
                            FOR UPDATE OF w2
                        )
                      AND a.attempt_id = w.attempt_id
                      AND w.status = 'armed'
                      AND {_order_fires_sql("w", "a")}
                    RETURNING {cols("w")}
                    """,
                    {"attempts": attempt_ids},
                )
                rows = await cur.fetchall()
                await _enqueue(conn, [(r, "fire", 1) for r in rows])
                return [normalize(r) for r in rows]


async def claim_due_lapses(*, limit: int = 50) -> List[Dict[str, Any]]:
    """Stamp the price wakeups whose lapse reminder has come due and enqueue
    the reminders; the claimed rows. ``SKIP LOCKED`` lets every worker's
    sweep run at once without two claiming the same row."""
    async with get_db_connection() as conn:
        async with conn.transaction():
            async with conn.cursor(row_factory=dict_row) as cur:
                await cur.execute(
                    f"""
                    UPDATE wakeups
                    SET warned_at = NOW(), updated_at = NOW()
                    WHERE wakeup_id IN (
                        SELECT wakeup_id FROM wakeups
                        WHERE kind = 'price' AND {pending_sql()}
                          AND warned_at IS NULL AND warn_at <= NOW()
                        ORDER BY warn_at
                        LIMIT %s
                        FOR UPDATE SKIP LOCKED
                    )
                    RETURNING {WAKEUP_COLUMNS}
                    """,
                    (limit,),
                )
                rows = await cur.fetchall()
                await _enqueue(conn, [(r, "lapse", 0) for r in rows])
                return [normalize(r) for r in rows]


async def expire_due_wakeups() -> int:
    """Close the armed price and order wakeups whose window has passed; how
    many it closed. No notice: a price was already reminded, and an order's
    set confirmation named the time it would stop watching.

    Locked in id order, as every fire and cancel locks, so a sweep closing
    several never deadlocks with one of them holding some of the same."""
    async with get_db_connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                """
                UPDATE wakeups
                SET status = 'expired', updated_at = NOW()
                WHERE wakeup_id IN (
                    SELECT wakeup_id FROM wakeups
                    WHERE status = 'armed' AND kind IN ('price', 'order')
                      AND expires_at <= NOW()
                    ORDER BY wakeup_id
                    FOR UPDATE
                )
                """
            )
            return cur.rowcount


async def job_owed(job_id: str) -> bool:
    if not is_uuid(job_id):
        return False
    async with get_db_connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                f"""
                SELECT {WAKEUP_OWED_SQL} FROM hook_outbox o
                WHERE o.hook_outbox_id = %s AND o.hook_type = %s
                """,
                (str(job_id), HOOK_TYPE),
            )
            row = await cur.fetchone()
            return bool(row and row[0])


async def mark_delivered(
    wakeup_id: str, notice: Notice, seq: int, run_id: Optional[str]
) -> bool:
    """Stamp a notice delivered to ``run_id``; False when it already was.

    Never guarded on status: a delivery is a fact, and a wakeup cancelled
    after its notice reached the model was still delivered.
    """
    if not is_uuid(wakeup_id):
        return False
    async with get_db_connection() as conn:
        async with conn.cursor() as cur:
            if notice == "fire":
                await cur.execute(
                    """
                    UPDATE wakeups
                    SET delivered_seq = GREATEST(delivered_seq, %s),
                        delivered_at = NOW(), delivered_run_id = %s,
                        updated_at = NOW()
                    WHERE wakeup_id = %s AND delivered_seq < %s
                    """,
                    (seq, run_id, str(wakeup_id), seq),
                )
            else:
                await cur.execute(
                    """
                    UPDATE wakeups
                    SET lapse_delivered_at = NOW(), lapse_delivered_run_id = %s,
                        updated_at = NOW()
                    WHERE wakeup_id = %s AND lapse_delivered_at IS NULL
                    """,
                    (run_id, str(wakeup_id)),
                )
            return cur.rowcount == 1
