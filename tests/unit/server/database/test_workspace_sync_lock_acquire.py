"""A session lock granted under a cancelled acquisition must not reach the pool."""

from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from unittest.mock import patch

import pytest

from src.server.database import workspace_file as wf


class _Cursor:
    def __init__(self, conn) -> None:
        self._conn = conn

    async def execute(self, sql: str, params=None) -> None:
        flat = " ".join(sql.split())
        self._conn.executed.append(flat)
        if "pg_advisory_lock(" in flat:
            # The grant lands server-side; the reply is what gets cancelled.
            self._conn.granted.set()
            await asyncio.sleep(3600)


class _Conn:
    def __init__(self) -> None:
        self.executed: list[str] = []
        self.granted = asyncio.Event()
        self.closed = False

    @asynccontextmanager
    async def cursor(self, **_kw):
        yield _Cursor(self)

    @asynccontextmanager
    async def transaction(self):
        yield

    async def close(self) -> None:
        self.closed = True


@pytest.mark.asyncio
async def test_a_cancellation_during_the_grant_releases_the_session():
    conn = _Conn()

    @asynccontextmanager
    async def _acquire(existing=None):
        yield conn

    async def _hold():
        async with wf.workspace_sync_lock("ws-1"):
            pass  # pragma: no cover

    with patch.object(wf, "get_db_connection", _acquire):
        task = asyncio.create_task(_hold())
        await conn.granted.wait()
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

    assert any("pg_advisory_unlock" in s for s in conn.executed)
    assert not conn.closed
