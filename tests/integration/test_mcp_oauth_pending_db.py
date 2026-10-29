"""The parked sign-ins against real PostgreSQL: how many one user may hold."""

from __future__ import annotations

import asyncio
import uuid

import pytest

pytestmark = [pytest.mark.integration, pytest.mark.asyncio(loop_scope="session")]


@pytest.fixture(autouse=True)
def _encryption_key(monkeypatch):
    monkeypatch.setenv("BYOK_ENCRYPTION_KEY", "test-pending-key")


async def _park(user: str, server: str, *, ttl: int = 660) -> str:
    from src.server.database.mcp_oauth_pending import store_pending

    state = f"state-{uuid.uuid4()}"
    await store_pending(
        state,
        user_id=user,
        kind="server",
        server_name=server,
        payload={"server": server},
        ttl_seconds=ttl,
    )
    return state


async def _held(db_conn, user: str) -> int:
    cur = await db_conn.execute(
        "SELECT count(*) AS n FROM mcp_oauth_pending WHERE user_id = %s", (user,)
    )
    return (await cur.fetchone())["n"]


async def test_a_user_keeps_only_the_newest_three(
    patched_get_db_connection, db_conn
):
    from src.server.database.mcp_oauth_pending import claim_pending

    user, other = f"test-pending-{uuid.uuid4()}", f"test-pending-{uuid.uuid4()}"
    neighbour = await _park(other, "theirs")
    states = []
    for i in range(5):
        states.append(await _park(user, f"server-{i}"))
        # created_at is the transaction's start, so each park its own instant.
        await asyncio.sleep(0.01)

    assert await _held(db_conn, user) == 3
    # The two pushed out read as expired at their callbacks.
    assert await claim_pending(states[0], user) is None
    assert await claim_pending(states[1], user) is None
    assert (await claim_pending(states[4], user)).server_name == "server-4"
    # Nobody else's sign-ins count.
    assert (await claim_pending(neighbour, other)).server_name == "theirs"


async def test_an_expired_sign_in_holds_no_place(patched_get_db_connection, db_conn):
    user = f"test-pending-{uuid.uuid4()}"
    for server, ttl in (("a", 660), ("b", 660), ("stale", -1)):
        await _park(user, server, ttl=ttl)
        await asyncio.sleep(0.01)

    # The newest row is the expired one, and it is the one that goes.
    await _park(user, "c")

    cur = await db_conn.execute(
        "SELECT server_name FROM mcp_oauth_pending WHERE user_id = %s "
        "ORDER BY created_at",
        (user,),
    )
    assert [r["server_name"] for r in await cur.fetchall()] == ["a", "b", "c"]


async def test_starts_at_once_still_leave_three(patched_get_db_connection, db_conn):
    user = f"test-pending-{uuid.uuid4()}"

    await asyncio.gather(*(_park(user, f"server-{i}") for i in range(8)))

    assert await _held(db_conn, user) == 3
