"""A delayed hook job is written first due after its delay."""

from unittest.mock import AsyncMock

import pytest

from src.server.database.runs.hook_jobs import HookJob
from src.server.database.runs.outbox import enqueue_hooks


@pytest.mark.asyncio
async def test_the_delay_rides_into_next_retry_at(mock_connection, mock_cursor):
    mock_cursor.execute = AsyncMock()
    await enqueue_hooks(
        mock_connection,
        run_id="run-1",
        thread_id="thread-1",
        jobs=[
            HookJob(hook_type="push", idempotency_key="run-1:push", delay_seconds=8.0),
            HookJob(hook_type="user_feed", idempotency_key="run-1:user_feed"),
        ],
    )
    (push_sql, push_params), (feed_sql, feed_params) = [
        c.args for c in mock_cursor.execute.await_args_list
    ]
    assert "next_retry_at" in push_sql
    assert "clock_timestamp()" in push_sql and "make_interval" in push_sql
    assert push_params[-1] == 8.0
    # Without a delay the interval is NULL, so the row is due at once, as before.
    assert feed_params[-1] is None
