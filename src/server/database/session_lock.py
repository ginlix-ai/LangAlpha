"""Release a session-level advisory lock so it never outlives its holder."""

from __future__ import annotations

import asyncio
from typing import Any


async def await_settled(task: asyncio.Future) -> Any:
    """Await ``task`` to completion through every cancellation delivery.

    An AnyIO scope re-delivers a cancellation at every await, so one shield
    is not enough for work that must finish once started. The cancellation
    is re-raised once the task has settled; otherwise its result is returned.
    """
    cancelled = False
    while not task.done():
        try:
            await asyncio.shield(task)
        except asyncio.CancelledError:
            cancelled = True
    if cancelled:
        if not task.cancelled():
            task.exception()  # retrieved, so the loop does not warn
        raise asyncio.CancelledError()
    return task.result()


async def _unlock_or_close(conn, key: str) -> None:
    try:
        async with conn.cursor() as cur:
            await cur.execute(
                "SELECT pg_advisory_unlock(hashtextextended(%s, 0))", (key,)
            )
    except BaseException:
        await conn.close()
        raise


async def release_session_lock(conn, key: str) -> None:
    """Release the session lock, or close the session so the lock dies with it.

    The release runs as its own task and is awaited until it finishes no
    matter how often cancellation is delivered: a cancelled holder would
    otherwise cancel the unlock itself and hand the pool a connection that
    still holds the lock. When the release cannot be confirmed the
    connection is closed, which the server treats as a release and the pool
    as a slot to replace.
    """
    await await_settled(asyncio.ensure_future(_unlock_or_close(conn, key)))
