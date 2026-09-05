"""The GC sweep's session lock is released the way the workspace sync lock is.

The sweep holds one session-level advisory lock across many transactions. A
cancelled or failing sweep that returned its connection to the pool with the
lock still held would silence every later sweep on every worker until that
connection happened to be closed.
"""

from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from unittest.mock import AsyncMock, patch

import pytest

from src.server.database import workspace_file_blobs as blobs


class _Cursor:
    def __init__(self, conn) -> None:
        self._conn = conn

    async def execute(self, sql: str, params=None) -> None:
        flat = " ".join(sql.split())
        self._conn.executed.append(flat)
        if "pg_advisory_unlock" in flat and self._conn.refuse_unlock:
            raise RuntimeError("connection lost")

    async def fetchone(self):
        if self._conn.fetch_responses:
            return self._conn.fetch_responses.pop(0)
        return (True,)


class _Conn:
    def __init__(self) -> None:
        self.executed: list[str] = []
        self.refuse_unlock = False
        self.closed = False
        self.fetch_responses: list = []

    @asynccontextmanager
    async def cursor(self, **_kw):
        yield _Cursor(self)

    @asynccontextmanager
    async def transaction(self):
        self.executed.append("BEGIN")
        try:
            yield
        finally:
            self.executed.append("END")

    async def close(self) -> None:
        self.closed = True


@pytest.fixture
def conn():
    c = _Conn()

    @asynccontextmanager
    async def _acquire(existing=None):
        yield c

    with patch.object(blobs, "get_db_connection", _acquire):
        yield c


def _unlocks(conn: _Conn) -> int:
    return sum("pg_advisory_unlock" in s for s in conn.executed)


@pytest.mark.asyncio
async def test_a_failing_sweep_still_releases_the_lock(conn):
    with (
        patch.object(
            blobs, "condemn_orphan_blobs", AsyncMock(side_effect=RuntimeError("boom"))
        ),
        patch.object(blobs, "reap_condemned_blobs", AsyncMock()),
    ):
        with pytest.raises(RuntimeError, match="boom"):
            await blobs.sweep_blob_garbage()

    assert _unlocks(conn) == 1
    assert not conn.closed


@pytest.mark.asyncio
async def test_a_cancelled_sweep_still_releases_the_lock(conn):
    started = asyncio.Event()

    async def _condemn(*_a, **_kw):
        started.set()
        await asyncio.sleep(3600)

    with (
        patch.object(blobs, "condemn_orphan_blobs", _condemn),
        patch.object(blobs, "reap_condemned_blobs", AsyncMock()),
    ):
        task = asyncio.create_task(blobs.sweep_blob_garbage())
        await started.wait()
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

    assert _unlocks(conn) == 1
    assert not conn.closed


@pytest.mark.asyncio
async def test_an_unconfirmed_release_closes_the_session_so_the_lock_dies_with_it(conn):
    conn.refuse_unlock = True
    with (
        patch.object(blobs, "condemn_orphan_blobs", AsyncMock(return_value=0)),
        patch.object(blobs, "reap_condemned_blobs", AsyncMock(return_value=(0, 0))),
    ):
        with pytest.raises(RuntimeError, match="connection lost"):
            await blobs.sweep_blob_garbage()

    assert conn.closed


@pytest.mark.asyncio
async def test_a_reap_cancelled_twice_keeps_its_row_lock_until_the_delete_settles(conn):
    """The delete thread cannot be interrupted and the transaction's row lock
    is what keeps a writer from reviving the digest under it. A cancellation
    re-delivered at the next await must not roll the transaction back first."""
    import threading
    import time

    started = threading.Event()

    def _slow_delete(_key):
        started.set()
        time.sleep(0.2)
        conn.executed.append("DELETED")
        return True

    conn.fetch_responses = [(1,), None]  # still condemned, not referenced
    with patch.object(blobs, "_storage_delete_object", _slow_delete):
        task = asyncio.create_task(blobs._reap_one(conn, "u", "a" * 64, 1))
        while not started.is_set():
            await asyncio.sleep(0.01)
        task.cancel()
        await asyncio.sleep(0.02)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

    assert conn.executed.index("DELETED") < conn.executed.index("END")


@pytest.mark.asyncio
async def test_a_cancellation_during_the_grant_releases_the_session(conn):
    granted = asyncio.Event()
    original = _Cursor.execute

    async def _execute(self, sql, params=None):
        await original(self, sql, params)
        if "pg_try_advisory_lock(" in " ".join(sql.split()):
            # The grant lands server-side; the reply is what gets cancelled.
            granted.set()
            await asyncio.sleep(3600)

    with patch.object(_Cursor, "execute", _execute):
        task = asyncio.create_task(blobs.sweep_blob_garbage())
        await granted.wait()
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

    assert _unlocks(conn) == 1
    assert not conn.closed
