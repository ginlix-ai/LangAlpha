"""Setting wakeups: the agent's tool arms them, up to a thread's cap."""

import secrets
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

from psycopg.rows import dict_row

from src.server.database.pool import get_db_connection
from src.server.database.wakeups.contract import (
    WAKEUP_COLUMNS,
    Kind,
    normalize,
    pending_sql,
)


@dataclass(frozen=True)
class WakeupSpec:
    kind: Kind
    label: Optional[str] = None
    # at / cron
    timezone: Optional[str] = None
    cron: Optional[str] = None
    until_at: Optional[datetime] = None
    schedule: Tuple[datetime, ...] = ()
    stop_reason: Optional[str] = None
    # price
    symbol: Optional[str] = None
    market: Optional[str] = None
    price_kind: Optional[str] = None
    price_value: Optional[float] = None
    pct_from: Optional[str] = None
    armed_price: Optional[float] = None
    warn_at: Optional[datetime] = None
    # price + order
    expires_at: Optional[datetime] = None
    # order
    attempt_id: Optional[str] = None
    vendor_order_id: Optional[str] = None
    baseline_filled_qty: Optional[float] = None


# A thread's short ids are drawn from 65536 at first; past this many draws
# that all collided, from 16 million.
_SHORT_DRAWS = 8
_MAX_DRAWS = 32


async def arm_wakeups(
    thread_id: str, user_id: str, specs: List[WakeupSpec], *, cap: int = 10
) -> Tuple[List[Dict[str, Any]], List[int]]:
    """Arm ``specs`` up to the thread's cap of open wakeups; the armed rows in
    spec order, and the indexes of the specs the cap left out.

    The count and the inserts share a transaction under a lock on the thread
    row, so two parallel calls in one turn can never both pass the cap. NO
    KEY UPDATE serializes them but lets through the KEY SHARE that a notice
    job's foreign key takes on the same row: a fire holds a wakeup row and
    then enqueues, so under FOR UPDATE an arm waiting here would deadlock it.
    """
    if not specs:
        return [], []
    async with get_db_connection() as conn:
        async with conn.transaction():
            async with conn.cursor(row_factory=dict_row) as cur:
                await cur.execute(
                    """
                    SELECT 1 FROM conversation_threads
                    WHERE conversation_thread_id = %s
                    FOR NO KEY UPDATE
                    """,
                    (thread_id,),
                )
                await cur.execute(
                    f"""
                    SELECT COUNT(*) AS n FROM wakeups
                    WHERE conversation_thread_id = %s AND {pending_sql()}
                    """,
                    (thread_id,),
                )
                room = max(cap - (await cur.fetchone())["n"], 0)
                armed = [
                    await _insert(cur, thread_id, user_id, spec)
                    for spec in specs[:room]
                ]
                return armed, list(range(room, len(specs)))


async def _insert(cur, thread_id: str, user_id: str, spec: WakeupSpec) -> Dict[str, Any]:
    timed = spec.kind in ("at", "cron")
    for draw in range(_MAX_DRAWS):
        short_id = "w_" + secrets.token_hex(2 if draw < _SHORT_DRAWS else 3)
        await cur.execute(
            f"""
            INSERT INTO wakeups (
                short_id, conversation_thread_id, user_id, label, kind,
                timezone, cron, until_at, schedule, stop_reason, next_fire_at,
                symbol, market, price_kind, price_value, pct_from, armed_price,
                warn_at, expires_at, attempt_id, vendor_order_id,
                baseline_filled_qty
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s::timestamptz[], %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT ON CONSTRAINT wakeups_short_id_unique DO NOTHING
            RETURNING {WAKEUP_COLUMNS}
            """,
            (
                short_id, thread_id, user_id, spec.label, spec.kind,
                spec.timezone, spec.cron, spec.until_at, list(spec.schedule),
                spec.stop_reason, spec.schedule[0] if timed and spec.schedule else None,
                spec.symbol, spec.market, spec.price_kind, spec.price_value,
                spec.pct_from, spec.armed_price, spec.warn_at, spec.expires_at,
                spec.attempt_id, spec.vendor_order_id, spec.baseline_filled_qty,
            ),
        )
        row = await cur.fetchone()
        if row is not None:
            return normalize(row)
    raise RuntimeError(f"no free wakeup id on thread {thread_id}")
