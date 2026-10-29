"""The tunnel claims, against real Postgres.

A claim stamps ``swept_at``, and a fresh stamp refuses every pairing of the
machine while a take-off may be running its uninstall. So a claim may take
only a tunnel that a take-off or a forget follows at once: one it would leave
on its machine has to stay unstamped, or the pairing it was left for is
refused as if the tunnel were going.
"""

from __future__ import annotations

import asyncio
import uuid
from datetime import datetime, timedelta, timezone

import pytest
import pytest_asyncio

from src.server.database import ssh_tunnels as db

pytestmark = [pytest.mark.integration, pytest.mark.asyncio(loop_scope="session")]

USER = "test-user-ssh-tunnels"
DAY = timedelta(days=1)
GIVE_UP_AFTER = timedelta(days=30)


@pytest_asyncio.fixture(loop_scope="session")
async def tunnels(patched_get_db_connection, test_db_pool):
    yield
    async with test_db_pool.connection() as conn:
        await conn.execute("DELETE FROM ssh_tunnels WHERE user_id = %s", (USER,))


async def _tunnel(test_db_pool, *, login: bool = True, **at: timedelta) -> str:
    """A tunnel of USER's; each of ``at`` is a timestamp column, that long ago."""
    now = datetime.now(timezone.utc)
    cols = {"paired_at": now, **{k: now - v for k, v in at.items()}}
    if login:
        cols.update(username="dev", host_key="ssh-ed25519 AAAA")
    names = ", ".join(cols)
    async with test_db_pool.connection() as conn:
        cur = await conn.execute(
            f"INSERT INTO ssh_tunnels (user_id, public_key, fingerprint, {names}) "
            f"VALUES (%s, 'ssh-ed25519 AAAA', %s, {', '.join(['%s'] * len(cols))}) "
            "RETURNING tunnel_id::text AS tunnel_id",
            (USER, uuid.uuid4().hex, *cols.values()),
        )
        return (await cur.fetchone())["tunnel_id"]


async def _release(tunnel_id: str, made_ago: timedelta | None):
    made_at = None if made_ago is None else datetime.now(timezone.utc) - made_ago
    return await db.claim_released_tunnel(
        tunnel_id,
        retry_after=db.TAKE_OFF_WINDOW,
        made_at=made_at,
        paired_within=DAY,
    )


async def _sweep() -> set[str]:
    rows = await db.claim_unused_tunnels(
        paired_before=DAY,
        retry_after=timedelta(hours=1),
        give_up_after=GIVE_UP_AFTER,
        limit=25,
    )
    return {row["tunnel_id"] for row in rows}


async def test_a_release_leaves_a_tunnel_paired_again_for_a_new_computer(
    tunnels, test_db_pool
):
    tunnel_id = await _tunnel(test_db_pool)

    assert await _release(tunnel_id, made_ago=timedelta(hours=1)) is None
    assert await db.renew_tunnel(USER, tunnel_id) is True


async def test_a_release_takes_a_tunnel_not_paired_since_its_computer(
    tunnels, test_db_pool
):
    tunnel_id = await _tunnel(test_db_pool, paired_at=timedelta(hours=2))
    assert await _release(tunnel_id, made_ago=timedelta(hours=1)) is not None
    assert await db.renew_tunnel(USER, tunnel_id) is False

    unknown_age = await _tunnel(test_db_pool)
    assert await _release(unknown_age, made_ago=None) is not None


async def test_a_release_after_the_sweep_claimed_it_leaves_it_to_the_sweep(
    tunnels, test_db_pool
):
    swept = await _tunnel(test_db_pool, login=False, paired_at=2 * DAY)
    paired_again = await _tunnel(test_db_pool)

    assert swept in await _sweep()

    assert await _release(swept, made_ago=3 * DAY) is None
    assert await db.taken_off_elsewhere(swept, within=db.TAKE_OFF_WINDOW) is True
    assert await _release(paired_again, made_ago=timedelta(hours=1)) is None
    assert (
        await db.taken_off_elsewhere(paired_again, within=db.TAKE_OFF_WINDOW) is False
    )


async def test_the_sweep_leaves_a_machine_away_less_than_its_give_up(
    tunnels, test_db_pool
):
    away = dict(
        paired_at=2 * DAY, connected_at=2 * DAY, disconnected_at=DAY, seen_at=DAY
    )
    resting = await _tunnel(test_db_pool, **away)
    given_up = await _tunnel(
        test_db_pool, **away | {"disconnected_at": 40 * DAY, "seen_at": 40 * DAY}
    )
    live = await _tunnel(
        test_db_pool,
        paired_at=2 * DAY,
        connected_at=timedelta(hours=1),
        seen_at=timedelta(0),
    )
    no_login = await _tunnel(test_db_pool, login=False, paired_at=2 * DAY)

    claimed = await _sweep()

    assert resting not in claimed
    assert {given_up, live, no_login} <= claimed
    assert await db.renew_tunnel(USER, resting) is True


async def test_pairings_at_once_stop_at_the_cap(tunnels, test_db_pool):
    for _ in range(db.MAX_TUNNELS_PER_USER - 1):
        await _tunnel(test_db_pool)

    # Warm the pool, so each pairing below holds its own connection at once.
    async def hold():
        async with test_db_pool.connection():
            await asyncio.sleep(0.2)

    await asyncio.gather(*(hold() for _ in range(test_db_pool.max_size)))

    async def pair():
        try:
            return await db.upsert_tunnel(
                USER,
                public_key="ssh-ed25519 AAAA",
                fingerprint=uuid.uuid4().hex,
                hostname="box",
            )
        except db.TooManyTunnels:
            return None

    made = await asyncio.gather(*(pair() for _ in range(test_db_pool.max_size)))

    assert sum(row is not None for row in made) == 1
    async with test_db_pool.connection() as conn:
        cur = await conn.execute(
            "SELECT count(*) AS held FROM ssh_tunnels WHERE user_id = %s", (USER,)
        )
        assert (await cur.fetchone())["held"] == db.MAX_TUNNELS_PER_USER
