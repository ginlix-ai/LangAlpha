"""The one session a process holds its shared advisory locks on."""

from __future__ import annotations

import asyncio
from collections import Counter
from contextlib import asynccontextmanager
from unittest.mock import patch

import psycopg
import pytest

from src.server.database.session_lock import SharedLockBusy, SharedLockSession

_OPEN = "src.server.database.pool.open_session_connection"


class _Cursor:
    def __init__(self, conn) -> None:
        self._conn = conn
        self._row = None

    async def execute(self, sql: str, params=()) -> None:
        conn = self._conn
        if conn.broken:
            conn.closed = True
            raise psycopg.OperationalError("server closed the connection unexpectedly")
        (key,) = params
        if "pg_try_advisory_lock_shared" in sql:
            conn.asked.set()
            await conn.reply.wait()
            granted = key not in conn.exclusive
            conn.held[key] += granted
            self._row = (granted,)
        elif "pg_advisory_unlock_shared" in sql:
            conn.held[key] -= 1
            self._row = (True,)

    async def fetchone(self):
        return self._row


class _Conn:
    def __init__(self) -> None:
        self.closed = False
        self.broken = False
        self.held: Counter[str] = Counter()
        self.exclusive: set[str] = set()
        self.asked = asyncio.Event()
        self.reply = asyncio.Event()
        self.reply.set()

    @asynccontextmanager
    async def cursor(self, **_kw):
        yield _Cursor(self)

    async def close(self) -> None:
        self.closed = True


def _sessions(*conns):
    opened = list(conns)

    async def open_session_connection():
        return opened.pop(0)

    return patch(_OPEN, open_session_connection)


@pytest.mark.asyncio
async def test_holds_on_one_key_stack_and_release_one_at_a_time():
    conn = _Conn()
    locks = SharedLockSession()
    with _sessions(conn):
        first = await locks.acquire("k", wait_s=1)
        second = await locks.acquire("k", wait_s=1)
        assert first is second is conn
        await locks.release(first, "k")
        assert conn.held["k"] == 1
        await locks.release(second, "k")

    assert conn.held["k"] == 0


@pytest.mark.asyncio
async def test_a_hold_waits_out_an_exclusive_holder_then_gives_up():
    conn = _Conn()
    conn.exclusive.add("k")
    locks = SharedLockSession()
    with _sessions(conn):
        with pytest.raises(SharedLockBusy):
            await locks.acquire("k", wait_s=0.05, retry_s=0.01)
        waiting = asyncio.create_task(locks.acquire("k", wait_s=5, retry_s=0.01))
        await asyncio.sleep(0.05)
        assert not waiting.done()
        conn.exclusive.clear()
        assert await asyncio.wait_for(waiting, timeout=5) is conn

    assert conn.held["k"] == 1


@pytest.mark.asyncio
async def test_a_lock_granted_as_its_holder_gives_up_is_given_back():
    """The grant lands server-side as the cancellation arrives. Left counted,
    the key stays held for as long as the session lives."""
    conn = _Conn()
    conn.reply.clear()
    locks = SharedLockSession()
    with _sessions(conn):
        acquiring = asyncio.create_task(locks.acquire("k", wait_s=1))
        await asyncio.wait_for(conn.asked.wait(), timeout=5)
        acquiring.cancel()
        conn.reply.set()
        with pytest.raises(asyncio.CancelledError):
            await acquiring

    assert conn.held["k"] == 0
    assert not conn.closed


@pytest.mark.asyncio
async def test_a_lost_session_is_replaced_and_its_holds_are_not_released_on_the_new_one():
    lost, fresh = _Conn(), _Conn()
    locks = SharedLockSession()
    with _sessions(lost, fresh):
        held_on_lost = await locks.acquire("k", wait_s=1)
        lost.broken = True
        # The next hold finds the session broken and opens another.
        held_on_fresh = await locks.acquire("k", wait_s=1, retry_s=0.01)
        await locks.release(held_on_lost, "k")

    assert held_on_fresh is fresh
    assert fresh.held["k"] == 1


@pytest.mark.asyncio
async def test_a_release_the_session_cannot_confirm_does_not_fail_the_holder():
    """The session closes, which lets go of the lock anyway, after the work
    under it is done."""
    conn = _Conn()
    locks = SharedLockSession()
    with _sessions(conn):
        held = await locks.acquire("k", wait_s=1)
        conn.broken = True
        await locks.release(held, "k")

    assert conn.closed
