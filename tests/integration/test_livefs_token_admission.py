"""The Redis scripts that decide whether a mount token is live.

Every file operation a computer's mount makes authenticates through them, and
the unit tests run with Redis off, so only here do the scripts themselves run
against the row they mirror: the mirror's drop on revoke, the previous slot it
holds after a rotation, each entry's own expiry, the generation that fences a
row read racing a change, and the computer's rate.

Requires a real Redis and Postgres (``REDIS_URL`` and ``TEST_DB_*``).
"""

from __future__ import annotations

import asyncio
import os
import secrets
import uuid
from datetime import UTC, datetime, timedelta

import pytest
import pytest_asyncio

from src.server.services.livefs import cache as livefs_cache
from src.server.services.livefs import tokens
from src.server.services.livefs.tokens import (
    LivefsAuthError,
    LivefsIdentity,
    LivefsThrottled,
    authenticate,
    mint_token,
    revoke,
)

pytestmark = [pytest.mark.integration, pytest.mark.asyncio(loop_scope="session")]


@pytest_asyncio.fixture(loop_scope="session")
async def redis(monkeypatch):
    """The livefs services' Redis, pointed at ``REDIS_URL``."""
    redis_url = os.getenv("REDIS_URL", "redis://localhost:6379/0")
    if not redis_url.startswith("redis://"):
        pytest.skip("REDIS_URL not set to a real Redis instance")

    from src.utils.cache.redis_cache import RedisCacheClient

    client = RedisCacheClient(url=redis_url, max_connections=10)
    try:
        await client.connect()
    except Exception as exc:
        pytest.skip(f"Redis is not reachable at REDIS_URL: {exc}")
    if not client.enabled or not client.client:
        pytest.skip("Redis client did not initialize")
    monkeypatch.setattr(livefs_cache, "get_cache_client", lambda: client)
    yield client.client
    await client.disconnect()


@pytest_asyncio.fixture(loop_scope="session")
async def computer(seed_user, patched_get_db_connection, redis):
    """A running computer under a fresh random id, so its keys are its own."""
    from src.server.database.computer import create_computer

    row = await create_computer(
        seed_user["user_id"],
        kind="daytona",
        name="Cloud computer",
        is_primary=True,
        status="running",
        computer_id=str(uuid.uuid4()),
    )
    cid = str(row["computer_id"])
    yield LivefsIdentity(cid, seed_user["user_id"], row["root_dir"])
    await redis.delete(*tokens._keys(cid))


@pytest.fixture
def row_reads(monkeypatch) -> list[str]:
    """Each read of the token row, which only a request the mirror could not
    answer makes."""
    reads: list[str] = []
    real = tokens.db.load_token

    async def load_token(computer_id):
        reads.append(computer_id)
        return await real(computer_id)

    monkeypatch.setattr(tokens.db, "load_token", load_token)
    return reads


def _bearer(token: str) -> str:
    return f"Bearer {token}"


async def _refused(authorization: str) -> bool:
    try:
        await authenticate(authorization)
    except LivefsAuthError:
        return True
    return False


async def test_a_revoked_token_is_refused_though_the_mirror_held_it(
    computer, redis, row_reads
):
    minted = await mint_token(computer.computer_id, computer.user_id)
    assert await authenticate(_bearer(minted.token)) == computer
    assert await authenticate(_bearer(minted.token)) == computer
    # The second request was the mirror's alone.
    assert len(row_reads) == 1

    await revoke(computer.computer_id)

    auth_key, _, _ = tokens._keys(computer.computer_id)
    assert await redis.exists(auth_key) == 0
    assert await _refused(_bearer(minted.token))


@pytest.mark.parametrize(
    ("status", "serves"),
    [("starting", True), ("error", True), ("stopping", False), ("stopped", False)],
)
async def test_a_token_serves_only_while_its_computer_is_in_service(
    computer, redis, test_db_pool, status, serves
):
    """A stop whose revoke failed, or a start reverted to stopped, leaves the
    row in place; the computer's status alone has to end it."""
    minted = await mint_token(computer.computer_id, computer.user_id)
    async with test_db_pool.connection() as conn:
        await conn.execute(
            "UPDATE computers SET status = %s WHERE computer_id = %s",
            (status, computer.computer_id),
        )
    # What a stop's revoke drops even when its row delete fails.
    await redis.delete(*tokens._keys(computer.computer_id))

    assert (not await _refused(_bearer(minted.token))) is serves


async def test_the_previous_token_is_admitted_from_the_mirror_after_a_rotation(
    computer, row_reads
):
    """A daemon mid-request holds the token it read before the rewrite, so the
    mirror written back after a rotation carries both slots."""
    old = await mint_token(computer.computer_id, computer.user_id)
    assert await authenticate(_bearer(old.token)) == computer
    new = await mint_token(computer.computer_id, computer.user_id)

    assert await authenticate(_bearer(new.token)) == computer
    reads = len(row_reads)
    assert await authenticate(_bearer(old.token)) == computer
    assert len(row_reads) == reads

    # A second rotation pushes it out of the row, and the mint drops the
    # mirror that still held it.
    await mint_token(computer.computer_id, computer.user_id)
    assert await _refused(_bearer(old.token))


async def test_an_expired_entry_still_in_the_mirror_is_refused(
    computer, redis, row_reads
):
    """The mirror lapses with its longest-lived token, so a shorter one's
    entry outlives it there and the script has to read each entry's expiry."""
    from src.server.database import livefs_tokens as db

    short, long = secrets.token_urlsafe(32), secrets.token_urlsafe(32)
    short_until = datetime.now(UTC) + timedelta(seconds=2)
    await db.save_token(
        computer.computer_id, computer.user_id, tokens._digest(short), short_until
    )
    await db.save_token(
        computer.computer_id,
        computer.user_id,
        tokens._digest(long),
        datetime.now(UTC) + timedelta(hours=1),
    )
    presented = _bearer(f"{tokens.PREFIX}.{computer.computer_id}.{short}")

    assert await authenticate(presented) == computer
    assert await authenticate(presented) == computer
    assert len(row_reads) == 1

    await asyncio.sleep(
        (short_until - datetime.now(UTC)).total_seconds() + 1.0
    )
    auth_key, _, _ = tokens._keys(computer.computer_id)
    assert await redis.hexists(auth_key, tokens._field(tokens._digest(short)))

    assert await _refused(presented)
    # Refused by the row, which the mirror sent the request to.
    assert len(row_reads) == 2


@pytest.mark.parametrize("change", ["revoke", "rotation"])
async def test_a_row_read_racing_a_change_is_not_written_back(
    computer, redis, monkeypatch, change
):
    """A request that read the row before a revoke, or before two mints pushed
    its token out, must not put that token back in the mirror."""
    minted = await mint_token(computer.computer_id, computer.user_id)
    real = tokens.db.load_token

    async def load_then_change(computer_id):
        row = await real(computer_id)
        if change == "revoke":
            await revoke(computer_id)
        else:
            await mint_token(computer_id, computer.user_id)
            await mint_token(computer_id, computer.user_id)
        return row

    monkeypatch.setattr(tokens.db, "load_token", load_then_change)
    # Admitted on the row it read, which said the token was live.
    assert await authenticate(_bearer(minted.token)) == computer

    monkeypatch.setattr(tokens.db, "load_token", real)
    auth_key, gen_key, _ = tokens._keys(computer.computer_id)
    assert await redis.exists(auth_key) == 0
    assert int(await redis.get(gen_key)) > 0
    assert await _refused(_bearer(minted.token))


async def test_the_request_past_the_burst_is_throttled(
    computer, redis, monkeypatch
):
    # Refill slowed so the loop's own duration cannot move the boundary.
    monkeypatch.setattr(tokens, "RATE_PER_S", 0.01)
    minted = await mint_token(computer.computer_id, computer.user_id)
    _, _, rate_key = tokens._keys(computer.computer_id)

    # An unknown token spends nothing of the computer's rate.
    forged = f"{tokens.PREFIX}.{computer.computer_id}.{secrets.token_urlsafe(32)}"
    for _ in range(3):
        assert await _refused(_bearer(forged))
    assert await redis.exists(rate_key) == 0

    for _ in range(tokens.BURST):
        assert await authenticate(_bearer(minted.token)) == computer

    with pytest.raises(LivefsThrottled) as throttled:
        await authenticate(_bearer(minted.token))
    assert throttled.value.identity == computer
    assert throttled.value.retry_after >= 1
