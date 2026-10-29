"""Which SQL may run as a prepared statement.

A prepared ``SELECT *`` / ``RETURNING *`` fails with "cached plan must not
change result type" once a migration changes its columns, and a blue/green
deploy migrates while the old version still serves. So the app pool never
prepares, and the run's START/finalize SQL opts out on the pinned writer
session, which stays prepared for LangGraph's checkpoint writes.
"""

from unittest.mock import AsyncMock, MagicMock

import pytest

from src.server.database.pool import _configure_postgres_connection
from src.server.database.runs.lifecycle import _lifecycle_connection


class _Conn:
    prepare_threshold = 0


@pytest.mark.asyncio
async def test_app_pool_never_prepares():
    conn = MagicMock()
    conn.set_autocommit = AsyncMock()
    await _configure_postgres_connection(conn)
    assert conn.prepare_threshold is None


@pytest.mark.asyncio
async def test_pinned_session_runs_turn_sql_unprepared_then_restores():
    conn = _Conn()
    async with _lifecycle_connection(conn) as yielded:
        assert yielded is conn
        assert conn.prepare_threshold is None
    assert conn.prepare_threshold == 0


@pytest.mark.asyncio
async def test_pinned_session_restores_after_a_failed_statement():
    conn = _Conn()
    with pytest.raises(RuntimeError):
        async with _lifecycle_connection(conn):
            raise RuntimeError("finalize failed")
    assert conn.prepare_threshold == 0
