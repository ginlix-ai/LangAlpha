"""Which attempts the reconciliation sweep takes, against real Postgres.

The gate is a predicate over two clocks and a stamp the same statement writes,
so a stubbed cursor shows only that the SQL says it. Here the rows show it: a
read that settles nothing writes only ``swept_at``, and that stamp is what has
to keep every pass in the fleet from asking the vendor the same question again.
"""

from __future__ import annotations

import uuid
from datetime import timedelta

import pytest
import pytest_asyncio

pytestmark = [pytest.mark.integration, pytest.mark.asyncio(loop_scope="session")]

POLL_INTERVAL = 60

# Both populations the sweep reads, each as its own turn leaves it and long
# past the windows ``updated_at`` opens, so the read clock is all that differs.
_POPULATIONS = {
    "open": {"status": "working", "vendor_order_id": "staged-0001"},
    "unanswered": {"status": "submitting", "vendor_order_id": None},
}


@pytest_asyncio.fixture(loop_scope="session")
async def user_id(patched_get_db_connection, test_db_pool):
    user_id = f"stale-sweep-test-{uuid.uuid4().hex[:12]}"
    try:
        yield user_id
    finally:
        async with test_db_pool.connection() as conn:
            await conn.execute(
                "DELETE FROM order_attempts WHERE user_id = %s", (user_id,)
            )


async def _attempt(
    db_conn,
    user_id: str,
    *,
    status: str,
    vendor_order_id: str | None,
    swept_ago: timedelta | None,
) -> str:
    cur = await db_conn.execute(
        """
        INSERT INTO order_attempts
            (user_id, message_id, tool_call_id, server, vendor, action, status,
             vendor_order_id, executed_at, dispatched_at, updated_at, swept_at)
        VALUES (%s, %s, %s, 'broker', 'ibkr', 'stage', %s, %s,
                NOW() - INTERVAL '1 hour', NOW() - INTERVAL '1 hour',
                NOW() - INTERVAL '1 hour', NOW() - %s::interval)
        RETURNING attempt_id::text AS id
        """,
        (
            user_id,
            uuid.uuid4().hex,
            uuid.uuid4().hex,
            status,
            vendor_order_id,
            swept_ago,
        ),
    )
    return (await cur.fetchone())["id"]


@pytest.mark.parametrize("population", sorted(_POPULATIONS))
async def test_a_row_asked_about_within_the_poll_interval_is_not_taken(
    population, user_id, db_conn
):
    from src.server.database.order_reconciliation import list_stale_attempts

    shape = _POPULATIONS[population]
    recent = await _attempt(
        db_conn, user_id, swept_ago=timedelta(seconds=10), **shape
    )
    lapsed = await _attempt(
        db_conn, user_id, swept_ago=timedelta(seconds=POLL_INTERVAL * 2), **shape
    )
    never = await _attempt(db_conn, user_id, swept_ago=None, **shape)

    async def _take() -> set[str]:
        rows = await list_stale_attempts(
            submitting_grace_seconds=120,
            open_after_seconds=POLL_INTERVAL,
            resweep_after_seconds=POLL_INTERVAL,
            limit=500,
        )
        return {row["attempt_id"] for row in rows if row["user_id"] == user_id}

    # Asked ten seconds ago and left open: nothing about it has been written
    # since, which is exactly the row the updated_at windows alone let through.
    assert await _take() == {lapsed, never}

    cur = await db_conn.execute(
        """
        SELECT attempt_id::text AS id FROM order_attempts
         WHERE user_id = %s AND swept_at > NOW() - INTERVAL '5 seconds'
        """,
        (user_id,),
    )
    # Taking a row is asking about it, so the take is what stamps it.
    assert {row["id"] for row in await cur.fetchall()} == {lapsed, never}
    # And the stamp holds them off: a sibling worker's pass straight after
    # finds nothing to read, the one just held off included.
    assert await _take() == set()

    # Held off, not dropped: once its interval has run out it comes round.
    await db_conn.execute(
        "UPDATE order_attempts SET swept_at = NOW() - %s::interval"
        " WHERE attempt_id = %s::uuid",
        (timedelta(seconds=POLL_INTERVAL * 2), recent),
    )
    assert await _take() == {recent}
