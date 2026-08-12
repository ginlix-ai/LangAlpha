"""rebase_sse_events: row-locked replace of an agent set's archive rows.

The read (FOR UPDATE), strip, append, and write happen in ONE transaction,
so a concurrent ``append_sse_event`` serializes on the row lock instead of
landing between a composer's snapshot and its replacement write (where the
replacement would erase it).
"""

from contextlib import asynccontextmanager
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.server.database.conversation import rebase_sse_events

MAIN = [{"event": "message_chunk", "data": {"agent": "ptc", "content": "m"}}]
STALE_TASK_ROW = {
    "event": "artifact",
    "data": {"agent": "task:abc", "artifact_type": "todo_list"},
}
CW_ROW = {"event": "context_window", "data": {"agent": "ptc"}}
CAPTURED = [
    {"event": "message_chunk", "data": {"agent": "task:abc", "content": "t"}}
]


@pytest.fixture
def mock_cursor():
    cursor = AsyncMock()
    cursor.execute = AsyncMock()
    cursor.fetchone = AsyncMock(return_value=None)
    return cursor


@pytest.fixture
def mock_connection(mock_cursor):
    conn = MagicMock()

    @asynccontextmanager
    async def _cursor_cm(**kwargs):
        yield mock_cursor

    @asynccontextmanager
    async def _tx_cm():
        yield

    conn.cursor = _cursor_cm
    conn.transaction = _tx_cm
    return conn


@pytest.fixture
def rebase_db(mock_connection):
    @asynccontextmanager
    async def _fake_pool():
        yield mock_connection

    with patch(
        "src.server.database.pool.get_db_connection", new=_fake_pool
    ):
        yield mock_connection


@pytest.mark.asyncio
async def test_locked_read_strips_and_appends_on_same_conn(
    rebase_db, mock_cursor
):
    """Concurrent-append survival: the context_window row that landed after
    the collector's compose is preserved, the collected agent's stale row is
    replaced (not duplicated), and the write happens inside the same locked
    transaction."""
    mock_cursor.fetchone = AsyncMock(
        side_effect=[
            {"sse_events": MAIN + [STALE_TASK_ROW, CW_ROW]},
            {"conversation_thread_id": "t1", "turn_index": 0},
        ]
    )

    ok = await rebase_sse_events(
        "resp-1", {"task:abc"}, CAPTURED, fallback_base=[]
    )

    assert ok is True
    read_sql = mock_cursor.execute.await_args_list[0].args[0]
    assert "FOR UPDATE" in read_sql
    write_sql, write_params = mock_cursor.execute.await_args_list[1].args
    assert "SET sse_events" in write_sql
    assert "RETURNING" in write_sql
    assert write_params[0].obj == MAIN + [CW_ROW] + CAPTURED
    assert write_params[1] == "resp-1"


@pytest.mark.asyncio
async def test_missing_row_composes_from_fallback(rebase_db, mock_cursor):
    """No row yet: the fallback (pre-compose main chunks) seeds the blob;
    the write updates nothing, so False propagates and the caller treats
    it as a failed persist."""
    mock_cursor.fetchone = AsyncMock(return_value=None)

    ok = await rebase_sse_events(
        "resp-1", {"task:abc"}, CAPTURED, fallback_base=list(MAIN)
    )

    assert ok is False
    _, write_params = mock_cursor.execute.await_args_list[1].args
    assert write_params[0].obj == MAIN + CAPTURED
