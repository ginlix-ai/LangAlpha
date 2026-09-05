"""Integration tests for the connection-state cleanup inside ``get_db_connection``.

Locks the contract for a connection whose async context was interrupted while a
query was still on the wire: the server is told to stop working on it, and the
connection goes back to the pool for replacement. The interesting case is the
one production actually hits -- an anyio cancel scope, which re-delivers
``CancelledError`` at every await and silently kills an unshielded cleanup.
"""

from __future__ import annotations

import asyncio
import logging
import uuid

import anyio
import psycopg
import pytest
from psycopg.conninfo import conninfo_to_dict

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]

ACTIVE = psycopg.pq.TransactionStatus.ACTIVE


@pytest.fixture
def app_pool_env(test_db_uri, monkeypatch):
    """Point the app's own pool at the test database.

    The cleanup under test lives inside ``get_db_connection``, so this cannot
    use ``patched_get_db_connection`` -- that fixture replaces the very
    function being tested.
    """
    from src.server.database import pool as pool_mod

    parts = conninfo_to_dict(test_db_uri)
    monkeypatch.setenv("DB_HOST", str(parts.get("host", "localhost")))
    monkeypatch.setenv("DB_PORT", str(parts.get("port", "5432")))
    monkeypatch.setenv("DB_NAME", str(parts.get("dbname", "postgres")))
    monkeypatch.setenv("DB_USER", str(parts.get("user", "postgres")))
    monkeypatch.setenv("DB_PASSWORD", str(parts.get("password", "postgres")))
    monkeypatch.setattr(pool_mod, "_conversation_db_pool_cache", {})


@pytest.fixture
def pool_logs() -> list[logging.LogRecord]:
    """Collect the pool logger's own records for the whole test.

    Not ``caplog``: it captures through a handler on the *root* logger, which
    makes it hostage to global logging state -- ``configure_logging`` calls
    ``basicConfig(force=True)``, which removes every root handler. A record
    then gets emitted and silently not captured, which would also make the "no
    cleanup error" assertion below pass vacuously, the one thing it must never
    do. A handler on the pool logger itself is unaffected.
    """
    from src.server.database import pool as pool_mod

    logger = logging.getLogger(pool_mod.__name__)
    records: list[logging.LogRecord] = []

    class _Collector(logging.Handler):
        def emit(self, record: logging.LogRecord) -> None:
            records.append(record)

    handler = _Collector(level=logging.DEBUG)
    previous_level, previous_disabled = logger.level, logger.disabled
    logger.setLevel(logging.DEBUG)
    logger.disabled = False
    logger.addHandler(handler)
    try:
        yield records
    finally:
        logger.removeHandler(handler)
        logger.setLevel(previous_level)
        logger.disabled = previous_disabled


@pytest.fixture
def cancel_calls(monkeypatch) -> list[dict]:
    """Record cancel_safe calls.

    ``pg_stat_activity`` alone cannot tell our cancel apart from the pool
    closing the connection out from under the query, and it would not notice a
    regression to the synchronous ``cancel()`` that blocks the event loop.
    """
    calls: list[dict] = []
    original = psycopg.AsyncConnection.cancel_safe

    async def spy(self, **kwargs):
        calls.append(kwargs)
        return await original(self, **kwargs)

    monkeypatch.setattr(psycopg.AsyncConnection, "cancel_safe", spy)
    return calls


async def _queries_running(test_db_uri: str, tag: str) -> int:
    """Server-side truth -- the connection object cannot tell us this."""
    async with await psycopg.AsyncConnection.connect(test_db_uri) as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                "SELECT count(*) FROM pg_stat_activity "
                "WHERE query LIKE %s AND state = 'active' AND pid <> pg_backend_pid()",
                (f"%{tag}%",),
            )
            return (await cur.fetchone())[0]


def _strand_query(conn, tag: str) -> None:
    """Leave a query on the wire with its result unconsumed.

    This is what an interrupted context leaves behind; psycopg's own
    ``CancelledError`` handler recovers a normally-cancelled ``execute()``
    before the pool ever sees it.
    """
    conn.pgconn.send_query(f"SELECT pg_sleep(10) /* {tag} */".encode())


async def _stranded_caller(tag: str) -> None:
    """A caller interrupted while a query is still on the wire."""
    from src.server.database.pool import get_db_connection

    async with get_db_connection() as conn:
        _strand_query(conn, tag)
        await asyncio.sleep(0.2)
        assert conn.info.transaction_status == ACTIVE
        await asyncio.sleep(30)  # park here so the cancellation lands mid-context


async def test_active_query_is_cancelled_when_the_caller_is_cancelled(
    app_pool_env, test_db_uri, cancel_calls, pool_logs
):
    from src.server.database.pool import get_or_create_pool

    tag = f"cleanup-{uuid.uuid4().hex[:8]}"
    pool = get_or_create_pool()
    await pool.open(wait=True, timeout=10)
    try:
        task = asyncio.create_task(_stranded_caller(tag))
        await asyncio.sleep(0.5)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

        await asyncio.sleep(0.3)
        assert await _queries_running(test_db_uri, tag) == 0
        assert {"timeout": 2.0} in cancel_calls

        messages = [r.getMessage() for r in pool_logs]
        # Assert the positive first: it proves capture is working, so the
        # negative below cannot pass just because nothing was recorded.
        assert any("cancelling pending query" in m for m in messages)
        assert not any("Error during connection state cleanup" in m for m in messages)
    finally:
        await pool.close()


async def test_cancel_survives_an_anyio_cancel_scope(app_pool_env, test_db_uri):
    """Every SSE response body runs inside an anyio cancel scope.

    Such a scope re-delivers ``CancelledError`` at every await, so an unshielded
    cleanup dies at its first one -- and ``CancelledError`` is not an
    ``Exception``, so nothing logs it. Without the shield this leaves the query
    running on the server.
    """
    from src.server.database.pool import get_or_create_pool

    tag = f"cleanup-{uuid.uuid4().hex[:8]}"
    pool = get_or_create_pool()
    await pool.open(wait=True, timeout=10)

    try:
        async with anyio.create_task_group() as tg:
            tg.start_soon(_stranded_caller, tag)
            await asyncio.sleep(0.5)
            tg.cancel_scope.cancel()

        await asyncio.sleep(0.3)
        assert await _queries_running(test_db_uri, tag) == 0
    finally:
        await pool.close()
