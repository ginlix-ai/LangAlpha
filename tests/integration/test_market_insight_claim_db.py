"""The system insight claim, against real Postgres.

Every backend worker runs the insight schedule and wakes on the same instant,
so only a real database shows that exactly one of them claims the slot.
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone

import pytest
import pytest_asyncio

pytestmark = [pytest.mark.integration, pytest.mark.asyncio(loop_scope="session")]

WINDOW = timedelta(minutes=30)
IN_FLIGHT = timedelta(minutes=15)


@pytest_asyncio.fixture(loop_scope="session")
async def insights(patched_get_db_connection, test_db_pool):
    yield
    async with test_db_pool.connection() as conn:
        await conn.execute("TRUNCATE market_insights")


async def _claim(type: str = "market_update"):
    from src.server.database.market_insight import claim_system_insight

    return await claim_system_insight(
        type, fresh_within=WINDOW, in_flight_within=IN_FLIGHT, model="background"
    )


async def _rows(test_db_pool, type: str = "market_update") -> int:
    async with test_db_pool.connection() as conn:
        cur = await conn.execute(
            "SELECT count(*) AS n FROM market_insights WHERE type = %s", (type,)
        )
        return (await cur.fetchone())["n"]


async def _seed(test_db_pool, *, status: str, age: timedelta, completed: bool):
    at = datetime.now(timezone.utc) - age
    async with test_db_pool.connection() as conn:
        await conn.execute(
            """
            INSERT INTO market_insights
                (market_insight_id, type, status, model, created_at, completed_at)
            VALUES (gen_random_uuid(), 'market_update', %s, 'background', %s, %s)
            """,
            (status, at, at if completed else None),
        )


async def test_workers_waking_together_claim_once(insights, test_db_pool):
    # Open every pool slot first, so the claims run side by side on their own
    # sessions instead of queueing for a connection one at a time.
    async def hold():
        async with test_db_pool.connection():
            await asyncio.sleep(0.05)

    await asyncio.gather(*(hold() for _ in range(test_db_pool.max_size)))
    claims = await asyncio.gather(*(_claim() for _ in range(test_db_pool.max_size)))

    assert sum(claim.row is not None for claim in claims) == 1
    assert all(claim.holder_in_flight for claim in claims if claim.row is None)
    assert await _rows(test_db_pool) == 1


async def test_an_in_flight_insight_holds_the_type(insights, test_db_pool):
    await _seed(test_db_pool, status="generating", age=timedelta(minutes=5), completed=False)

    claim = await _claim()
    assert claim.row is None and claim.holder_in_flight


async def test_a_fresh_insight_holds_the_type_and_an_old_one_does_not(
    insights, test_db_pool
):
    await _seed(test_db_pool, status="completed", age=timedelta(minutes=10), completed=True)
    claim = await _claim()
    assert claim.row is None and not claim.holder_in_flight

    async with test_db_pool.connection() as conn:
        await conn.execute(
            "UPDATE market_insights SET completed_at = completed_at - interval '1 hour'"
        )
    assert (await _claim()).row is not None


async def test_failed_or_abandoned_rows_do_not_hold_the_type(insights, test_db_pool):
    await _seed(test_db_pool, status="failed", age=timedelta(minutes=1), completed=False)
    # Past the generation bound but inside the staleness window: its worker
    # died, so it no longer holds the type.
    await _seed(test_db_pool, status="generating", age=timedelta(minutes=20), completed=False)

    assert (await _claim()).row is not None


async def test_types_claim_independently(insights, test_db_pool):
    assert (await _claim("market_update")).row is not None
    assert (await _claim("post_market")).row is not None
