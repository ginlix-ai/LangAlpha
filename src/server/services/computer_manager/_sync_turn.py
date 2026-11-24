"""One asset sync per computer at a time, across workers.

Each worker holds its own sandbox object for a computer, so the lock a sync
takes on it keeps apart only that worker's syncs. Two workers that both find
the manifest behind would upload the same files at once, and a stop on one
would not know the other was mid-upload.

The turn is a Redis lease, renewed while its sync runs. It saves the second
upload and tells of a sync in progress; it guards no truth, which stays in
the manifest on the computer. With the lease lost or Redis away each worker
syncs for itself, as it did before there was one.
"""

import asyncio
import logging
import time
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from uuid import uuid4

from src.server.database.session_lock import await_settled
from src.utils.cache.redis_cache import get_cache_client

logger = logging.getLogger(__name__)

#: A holder that dies is waited on for no longer than this.
LEASE_MS = 30_000
RENEW_S = 10.0
POLL_S = 0.2
#: The longest a sync waits for another's. Past it this one runs beside it.
MAX_WAIT_S = 240.0
#: How long a stop or a delete waits for a sync it cannot cancel, one another
#: worker runs or one on a request's own path, before it goes ahead.
STOP_WAIT_S = 15.0


def _key(computer_id: str) -> str:
    return f"asset-sync:{computer_id}"


async def _take(key: str, token: str) -> bool:
    """Whether the lease was taken. False is a sync that runs without one."""
    cache = get_cache_client()
    began = time.monotonic()
    while True:
        taken = await cache.acquire_lock(key, token, LEASE_MS)
        if taken is not False:
            return bool(taken)
        if time.monotonic() - began >= MAX_WAIT_S:
            logger.warning(
                f"[SYNC_TURN] {key} still held after {MAX_WAIT_S:.0f}s; "
                "syncing beside its holder"
            )
            return False
        await asyncio.sleep(POLL_S)


async def _renew(key: str, token: str) -> None:
    cache = get_cache_client()
    while True:
        await asyncio.sleep(RENEW_S)
        if not await cache.extend_lock(key, token, LEASE_MS):
            return


@asynccontextmanager
async def sync_turn(computer_id: str) -> AsyncIterator[None]:
    """Hold the computer's sync turn for the block, waiting for another
    holder's sync to end first: the manifest this one then reads is the one
    that sync wrote, so it uploads nothing a second time."""
    key, token = _key(computer_id), uuid4().hex
    began = time.monotonic()
    if not await _take(key, token):
        yield
        return
    waited = time.monotonic() - began
    if waited >= 1.0:
        logger.info(f"[SYNC_TURN] computer={computer_id[:8]} waited={waited * 1000:.0f}ms")
    renewing = asyncio.ensure_future(_renew(key, token))
    try:
        yield
    finally:
        renewing.cancel()
        # Through a cancellation: a lease left behind holds the next sync
        # for its whole length.
        await await_settled(
            asyncio.ensure_future(get_cache_client().release_lock(key, token))
        )


async def wait_for_sync(computer_id: str) -> float:
    """Wait until no sync holds the computer's turn, or ``STOP_WAIT_S``, and
    say how long that took in seconds. For a stop and a delete, after they
    have cancelled the catch-ups their own worker runs."""
    cache = get_cache_client()
    key = _key(computer_id)
    began = time.monotonic()
    while await cache.exists(key):
        if time.monotonic() - began >= STOP_WAIT_S:
            break
        await asyncio.sleep(POLL_S)
    return time.monotonic() - began
