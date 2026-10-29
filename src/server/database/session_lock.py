"""Session-level advisory locks: releases that never outlive their holder,
and the one session every shared lock in a process is held on."""

from __future__ import annotations

import asyncio
import logging
import weakref
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Any

import psycopg

from src.server.database import pool

logger = logging.getLogger(__name__)


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


def _lock_arg(key: str | int) -> str:
    """A string names a lock and is hashed to its key; an int already is one."""
    return "%s" if isinstance(key, int) else "hashtextextended(%s, 0)"


async def _unlock_or_close(conn, key: str | int, unlock: str) -> None:
    try:
        async with conn.cursor() as cur:
            await cur.execute(f"SELECT {unlock}({_lock_arg(key)})", (key,))
    except BaseException:
        await conn.close()
        raise


async def release_session_lock(conn, key: str | int, *, shared: bool = False) -> None:
    """Release the session lock, or close the session so the lock dies with it.

    The release runs as its own task and is awaited until it finishes no
    matter how often cancellation is delivered: a cancelled holder would
    otherwise cancel the unlock itself and hand the pool a connection that
    still holds the lock. When the release cannot be confirmed the
    connection is closed, which the server treats as a release and the pool
    as a slot to replace.
    """
    unlock = "pg_advisory_unlock_shared" if shared else "pg_advisory_unlock"
    await await_settled(asyncio.ensure_future(_unlock_or_close(conn, key, unlock)))


@asynccontextmanager
async def try_session_lock(key: str | int) -> AsyncIterator[bool]:
    """Hold a session advisory lock, or yield False when someone else has it.

    Session-scoped rather than transaction-scoped: a holder makes network calls
    between its statements, and wrapping those in a transaction would pin a
    connection inside an open snapshot for as long as they take.
    """
    async with pool.get_db_connection() as conn:
        try:
            cur = await conn.execute(
                f"SELECT pg_try_advisory_lock({_lock_arg(key)})", (key,)
            )
            row = await cur.fetchone()
        except BaseException:
            # The lock may already have been granted: a session lock survives a
            # cancelled fetch, so release it rather than pool a locked session.
            await release_session_lock(conn, key)
            raise
        if not (row and row[0]):
            yield False
            return
        try:
            yield True
        finally:
            await release_session_lock(conn, key)


@asynccontextmanager
async def try_own_session_lock(key: str | int) -> AsyncIterator[bool]:
    """``try_session_lock`` on a session outside the pool, closed on exit.

    For a holder that reads and writes through the pool while it holds the
    lock across slow calls: a pooled session would keep a slot for the whole
    hold, and with a pool of one, leave none for the holder's own reads.
    Closing the session is the release, however the holder ends.
    """
    conn = await pool.open_session_connection()
    try:
        cur = await conn.execute(f"SELECT pg_try_advisory_lock({_lock_arg(key)})", (key,))
        row = await cur.fetchone()
        yield bool(row and row[0])
    finally:
        await await_settled(asyncio.ensure_future(conn.close()))


class SharedLockBusy(Exception):
    """An exclusive holder kept the key for the whole wait."""


async def _try_shared(conn, key: str) -> bool:
    async with conn.cursor() as cur:
        await cur.execute(
            "SELECT pg_try_advisory_lock_shared(hashtextextended(%s, 0))", (key,)
        )
        return bool((await cur.fetchone())[0])


class SharedLockSession:
    """This process's shared advisory locks, all held on one session of its own.

    A session lock lasts as long as its session, and a pooled connection kept
    per holder through a slow sandbox call is a slot nothing else can use:
    holders that each want one more for a read can take every slot between
    them. Postgres counts a session's shared locks per key, so one connection
    carries every holder's, each hold adding one and its release taking it
    back. Only shared locks belong here; an exclusive one would admit every
    task in the process at once. The session is execution context: who holds
    what is still read from Postgres.
    """

    def __init__(self) -> None:
        self._conn: Any = None
        self._opening = asyncio.Lock()

    async def _session(self) -> Any:
        async with self._opening:
            if self._conn is not None and self._conn.closed:
                logger.warning("Shared lock session lost, and every lock held on it")
                self._conn = None
            if self._conn is None:
                self._conn = await pool.open_session_connection()
            return self._conn

    async def acquire(self, key: str, *, wait_s: float, retry_s: float = 0.1) -> Any:
        """Hold ``key`` shared, and return the session to release it on.

        Polls rather than waits in Postgres: a waiting statement would hold up
        every other holder's lock and release on the one session."""
        loop = asyncio.get_running_loop()
        deadline = loop.time() + wait_s
        while True:
            conn = await self._session()
            try:
                if await self._try(conn, key):
                    return conn
            except psycopg.OperationalError:
                if not conn.closed:
                    raise
                # A broken session; the next attempt opens another.
            if loop.time() >= deadline:
                raise SharedLockBusy(key)
            await asyncio.sleep(retry_s)

    async def _try(self, conn: Any, key: str) -> bool:
        attempt = asyncio.ensure_future(_try_shared(conn, key))
        try:
            return await await_settled(attempt)
        except asyncio.CancelledError:
            # Taken as its holder gave up: left counted, the key would stay
            # held for as long as the session lives.
            if not attempt.cancelled() and attempt.exception() is None and attempt.result():
                await self.release(conn, key)
            raise

    async def release(self, conn: Any, key: str) -> None:
        # A session since lost took the lock with it, and its replacement
        # counts only the holds taken on it.
        if conn.closed:
            return
        try:
            await release_session_lock(conn, key, shared=True)
        except psycopg.Error:
            # Unconfirmed, the release closed the session, which lets go of
            # this lock with the rest; the holder's work is already done.
            if not conn.closed:
                raise

    async def close(self) -> None:
        if self._conn is not None:
            await self._conn.close()
            self._conn = None


# Per event loop, as a connection is: one in the server, one per test loop.
_shared_sessions: weakref.WeakKeyDictionary[asyncio.AbstractEventLoop, SharedLockSession] = (
    weakref.WeakKeyDictionary()
)


def shared_lock_session() -> SharedLockSession:
    loop = asyncio.get_running_loop()
    session = _shared_sessions.get(loop)
    if session is None:
        session = _shared_sessions[loop] = SharedLockSession()
    return session


async def close_shared_lock_session() -> None:
    session = _shared_sessions.pop(asyncio.get_running_loop(), None)
    if session is not None:
        await session.close()
