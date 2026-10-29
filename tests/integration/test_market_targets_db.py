"""Market-watch target identity against real PostgreSQL.

A target is on a listing, the symbol in its market: HSI the US ticker and HSI
the Hang Seng are both stored as HSI. Only the database shows that the armed
kind's unique index and arm_target's replace agree on that, so arming one
listing never cancels the other, or fails on it.
"""

from __future__ import annotations

import uuid
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest
import pytest_asyncio

pytestmark = [pytest.mark.integration, pytest.mark.asyncio(loop_scope="session")]


@pytest_asyncio.fixture(loop_scope="session")
async def thread(patched_get_db_connection, test_db_pool):
    from src.server.database.conversation import create_thread
    from src.server.database.user import create_user
    from src.server.database.workspace import create_workspace

    user_id = f"targets-test-{uuid.uuid4().hex[:12]}"
    await create_user(user_id=user_id, email=f"{user_id}@example.com", name="Test")
    workspace = await create_workspace(user_id=user_id, name="Targets", status="running")
    workspace_id = str(workspace["workspace_id"])
    thread_id = str(uuid.uuid4())
    await create_thread(
        conversation_thread_id=thread_id, workspace_id=workspace_id, current_status="completed"
    )

    async def armed():
        async with test_db_pool.connection() as conn:
            rows = await (
                await conn.execute(
                    "SELECT market, value FROM market_watch_targets "
                    "WHERE conversation_thread_id = %s AND status = 'armed' ORDER BY market",
                    (thread_id,),
                )
            ).fetchall()
        return [(r["market"], float(r["value"])) for r in rows]

    try:
        yield SimpleNamespace(
            id=thread_id, user_id=user_id, workspace_id=workspace_id, armed=armed
        )
    finally:
        async with test_db_pool.connection() as conn:
            await conn.execute(
                "DELETE FROM conversation_threads WHERE workspace_id = %s", (workspace_id,)
            )
            await conn.execute("DELETE FROM workspaces WHERE workspace_id = %s", (workspace_id,))
            await conn.execute("DELETE FROM users WHERE user_id = %s", (user_id,))


async def _arm(thread, market: str, value: float):
    from src.server.database.market_targets import arm_target

    now = datetime.now(timezone.utc)
    return await arm_target(
        thread_id=thread.id, user_id=thread.user_id, symbol="HSI", market=market,
        kind="above", value=value, pct_from=None, armed_price=None,
        expires_at=now + timedelta(hours=6), warn_at=now + timedelta(hours=5), max_armed=10,
    )


async def test_one_kind_on_both_listings_of_a_symbol_stays_armed(thread):
    await _arm(thread, "stock", 13.0)
    await _arm(thread, "index", 21000.0)
    assert await thread.armed() == [("index", 21000.0), ("stock", 13.0)]

    # Setting the kind again replaces it on its own listing only.
    await _arm(thread, "stock", 14.0)
    assert await thread.armed() == [("index", 21000.0), ("stock", 14.0)]


async def test_a_cancel_by_listing_leaves_the_other_listing_armed(thread):
    from src.server.database.market_targets import cancel_targets

    await _arm(thread, "stock", 13.0)
    await _arm(thread, "index", 21000.0)

    cancelled = await cancel_targets(thread.id, listings=[("HSI", "index")])

    assert [r["market"] for r in cancelled] == ["index"]
    assert await thread.armed() == [("stock", 13.0)]


async def _arm_on(thread, symbol: str, *, expires_in: timedelta = timedelta(hours=6)):
    from src.server.database.market_targets import arm_target

    now = datetime.now(timezone.utc)
    row = await arm_target(
        thread_id=thread.id, user_id=thread.user_id, symbol=symbol, market="stock",
        kind="above", value=10.0, pct_from=None, armed_price=None,
        expires_at=now + expires_in, warn_at=now + expires_in - timedelta(minutes=30),
        max_armed=10,
    )
    return row["target_id"]


async def _hit_jobs(test_db_pool, thread_id: str) -> dict:
    async with test_db_pool.connection() as conn:
        rows = await (
            await conn.execute(
                "SELECT idempotency_key, COUNT(*) AS n FROM hook_outbox "
                "WHERE conversation_thread_id = %s AND hook_type = 'market_target' "
                "GROUP BY idempotency_key",
                (thread_id,),
            )
        ).fetchall()
    return {r["idempotency_key"]: r["n"] for r in rows}


async def test_two_workers_claiming_the_same_hits_each_win_a_target_once(thread, test_db_pool):
    """Every worker's monitor sees the same tick, each with the targets its
    last load left it. Claiming its hits in one transaction must still hand
    each target to exactly one of them, with exactly one notice, whichever
    order each listed them in."""
    import asyncio

    from src.server.database.market_targets import fire_targets

    symbols = ["AAA", "BBB", "CCC", "DDD", "EEE"]
    rounds = 3
    for _ in range(rounds):
        ids = [await _arm_on(thread, symbol) for symbol in symbols]

        # Both claims queue on rows held elsewhere, so they contend when it
        # lets go instead of running one after the other.
        async with test_db_pool.connection() as blocker:
            async with blocker.transaction():
                await blocker.execute(
                    "SELECT 1 FROM market_watch_targets "
                    "WHERE target_id = ANY(%s::uuid[]) FOR UPDATE",
                    (ids,),
                )
                claims = asyncio.ensure_future(asyncio.gather(
                    fire_targets(ids[:4], 11.0),
                    fire_targets(list(reversed(ids[1:])), 11.0),
                ))
                await asyncio.sleep(0.2)
                assert not claims.done()
        first, second = await asyncio.wait_for(claims, timeout=10)

        won_first = {r["target_id"] for r in first}
        won_second = {r["target_id"] for r in second}
        assert not won_first & won_second
        assert won_first | won_second == set(ids)

    jobs = await _hit_jobs(test_db_pool, thread.id)
    assert len(jobs) == rounds * len(symbols)
    assert set(jobs.values()) == {1}
    assert await thread.armed() == []


async def test_a_claim_skips_a_target_no_longer_armed(thread, test_db_pool):
    from src.server.database.market_targets import cancel_targets, fire_targets

    armed = await _arm_on(thread, "AAA")
    cancelled = await _arm_on(thread, "BBB")
    lapsed = await _arm_on(thread, "CCC", expires_in=timedelta(minutes=-1))
    await cancel_targets(thread.id, target_id=cancelled)
    printed = datetime.now(timezone.utc) - timedelta(minutes=15)

    fired = await fire_targets([armed, cancelled, lapsed], 11.0, printed_at=printed)

    assert [r["target_id"] for r in fired] == [armed]
    assert fired[0]["status"] == "fired"
    assert fired[0]["fired_price"] == 11.0
    # A delayed print dates the hit when it traded.
    assert fired[0]["fired_at"] == printed
    assert await _hit_jobs(test_db_pool, thread.id) == {f"market_target:{armed}:hit": 1}


async def test_a_thread_reload_reads_that_threads_armed_targets_only(thread):
    from src.server.database.market_targets import cancel_targets, list_armed_targets

    armed = await _arm_on(thread, "AAA")
    cancelled = await _arm_on(thread, "BBB")
    await cancel_targets(thread.id, target_id=cancelled)

    assert [r["target_id"] for r in await list_armed_targets(thread_ids=[thread.id])] == [armed]
    assert await list_armed_targets(thread_ids=[str(uuid.uuid4())]) == []


async def test_a_workspace_cancel_ends_its_threads_targets_and_no_others(thread, test_db_pool):
    from src.server.database.conversation import create_thread
    from src.server.database.market_targets import (
        cancel_workspace_targets,
        list_armed_targets,
    )
    from src.server.database.workspace import create_workspace

    sibling = SimpleNamespace(id=str(uuid.uuid4()), user_id=thread.user_id)
    await create_thread(
        conversation_thread_id=sibling.id, workspace_id=thread.workspace_id,
        current_status="completed",
    )
    other = await create_workspace(user_id=thread.user_id, name="Elsewhere", status="running")
    other_id = str(other["workspace_id"])
    elsewhere = SimpleNamespace(id=str(uuid.uuid4()), user_id=thread.user_id)
    await create_thread(
        conversation_thread_id=elsewhere.id, workspace_id=other_id, current_status="completed"
    )
    try:
        here = [await _arm_on(thread, "AAA"), await _arm_on(sibling, "BBB")]
        kept = await _arm_on(elsewhere, "CCC")

        cancelled = await cancel_workspace_targets(thread.workspace_id)

        assert sorted(r["target_id"] for r in cancelled) == sorted(here)
        assert {r["status"] for r in cancelled} == {"cancelled"}
        assert [r["target_id"] for r in await list_armed_targets(thread_ids=[elsewhere.id])] == [
            kept
        ]
    finally:
        async with test_db_pool.connection() as conn:
            await conn.execute(
                "DELETE FROM conversation_threads WHERE workspace_id = %s", (other_id,)
            )
            await conn.execute("DELETE FROM workspaces WHERE workspace_id = %s", (other_id,))
