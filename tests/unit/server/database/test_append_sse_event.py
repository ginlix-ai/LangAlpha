"""append_sse_event: atomic server-side JSONB append to the latest response.

Replaces _persist_context_window_event's read-modify-write of the whole
sse_events blob with a single ``sse_events || event`` UPDATE scoped to the
thread's most-recent response — no full-blob round-trip, race-free against
concurrent appenders.
"""

from contextlib import asynccontextmanager
from unittest.mock import patch

import pytest
from psycopg.types.json import Json

from src.server.database.conversation import append_sse_event

THREAD_ID = "thread-1"
EVENT = {"event": "context_window", "data": {"action": "summarize"}}


@pytest.mark.asyncio
async def test_single_jsonb_append_to_latest_response(mock_connection, mock_cursor):
    mock_cursor.rowcount = 1
    ok = await append_sse_event(THREAD_ID, EVENT, conn=mock_connection)
    assert ok is True

    mock_cursor.execute.assert_awaited_once()
    sql, params = mock_cursor.execute.call_args.args
    # In-place concat, not a full-blob overwrite.
    assert "||" in sql
    assert "SET sse_events =" in sql
    # Scoped to the most-recent response by turn_index.
    assert "ORDER BY turn_index DESC" in sql
    assert "LIMIT 1" in sql
    # Payload bound as a one-element JSONB array (the concat operand).
    json_binds = [p for p in params if isinstance(p, Json)]
    assert len(json_binds) == 1
    assert json_binds[0].obj == [EVENT]
    assert THREAD_ID in params


@pytest.mark.asyncio
async def test_returns_false_when_no_response_row(mock_connection, mock_cursor):
    mock_cursor.rowcount = 0
    ok = await append_sse_event(THREAD_ID, EVENT, conn=mock_connection)
    assert ok is False


@pytest.mark.asyncio
async def test_uses_pool_when_no_conn(mock_connection, mock_cursor):
    mock_cursor.rowcount = 1

    @asynccontextmanager
    async def _fake_pool():
        yield mock_connection

    with patch("src.server.database.conversation.get_db_connection", new=_fake_pool):
        ok = await append_sse_event(THREAD_ID, EVENT)
    assert ok is True
    mock_cursor.execute.assert_awaited_once()
