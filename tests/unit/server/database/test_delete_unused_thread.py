"""A refused comment send deletes the thread it made, and only a thread no
turn ever started in. The row lock comes before the checks, in statements of
their own, so a START racing the delete either committed its turn rows first
and keeps the thread, or waits on the lock and fails on the missing row; its
rows never cascade away under it. A copy of the send another worker admitted
under the same key, as a run or a steer, keeps the thread as well."""

from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest

from src.server.database.conversation.threads_write import (
    delete_unused_thread,
    thread_exists_key,
)

_HAS_RUN = "src.server.database.runs.lifecycle.request_key_has_run"
_STEER_TAKEN = "src.server.database.runs.outbox.steer_key_taken"


@pytest.fixture
def key_free():
    with (
        patch(_HAS_RUN, AsyncMock(return_value=False)) as has_run,
        patch(_STEER_TAKEN, AsyncMock(return_value=False)) as steer_taken,
    ):
        yield has_run, steer_taken


@pytest.mark.asyncio
async def test_the_lock_comes_before_the_checks(mock_db_connection, mock_cursor):
    mock_cursor.fetchone.return_value = (1,)
    mock_cursor.rowcount = 1
    order: list[str] = []

    async def execute(sql, *args):
        order.append("lock" if "FOR UPDATE" in sql else "delete")

    async def key_read(*args):
        order.append("key")
        return False

    mock_cursor.execute.side_effect = execute
    with (
        patch(_HAS_RUN, AsyncMock(side_effect=key_read)) as has_run,
        patch(_STEER_TAKEN, AsyncMock(side_effect=key_read)),
    ):
        assert await delete_unused_thread("t-1", request_key="k-1")

    assert order == ["lock", "key", "key", "delete"]
    assert has_run.await_args.args[1] == "k-1"
    delete = mock_cursor.execute.call_args_list[-1].args[0]
    assert "DELETE FROM conversation_threads" in delete
    assert "NOT EXISTS" in delete and "conversation_queries" in delete
    assert "conversation_responses" in delete


@pytest.mark.asyncio
async def test_a_thread_already_gone_is_left_alone(
    mock_db_connection, mock_cursor, key_free
):
    mock_cursor.fetchone.return_value = None

    assert not await delete_unused_thread("t-1", request_key="k-1")
    assert mock_cursor.execute.await_count == 1
    key_free[0].assert_not_awaited()


@pytest.mark.asyncio
async def test_a_thread_a_turn_started_in_stays(
    mock_db_connection, mock_cursor, key_free
):
    mock_cursor.fetchone.return_value = (1,)
    mock_cursor.rowcount = 0

    assert not await delete_unused_thread("t-1", request_key="k-1")


@pytest.mark.asyncio
@pytest.mark.parametrize("taken_by", ["run", "steer"])
async def test_a_copy_admitted_under_the_key_keeps_the_thread(
    mock_db_connection, mock_cursor, taken_by
):
    mock_cursor.fetchone.return_value = (1,)
    with (
        patch(_HAS_RUN, AsyncMock(return_value=taken_by == "run")),
        patch(_STEER_TAKEN, AsyncMock(return_value=taken_by == "steer")),
    ):
        assert not await delete_unused_thread("t-1", request_key="k-1")

    (lock,) = (c.args[0] for c in mock_cursor.execute.call_args_list)
    assert "FOR UPDATE" in lock


@pytest.mark.asyncio
async def test_a_removed_threads_existence_leaves_the_cache_after_the_commit(
    mock_db_connection, mock_cursor, key_free
):
    """A retry under this thread id would otherwise skip making the row."""
    mock_cursor.fetchone.return_value = (1,)
    mock_cursor.rowcount = 1
    cache = SimpleNamespace(
        enabled=True, client=SimpleNamespace(delete=AsyncMock())
    )

    with patch("src.utils.cache.redis_cache.get_cache_client", return_value=cache):
        assert await delete_unused_thread("t-1", request_key="k-1")

    cache.client.delete.assert_awaited_once_with(thread_exists_key("t-1"))


@pytest.mark.asyncio
async def test_a_thread_that_stays_keeps_its_cached_existence(
    mock_db_connection, mock_cursor, key_free
):
    mock_cursor.fetchone.return_value = (1,)
    mock_cursor.rowcount = 0
    cache = SimpleNamespace(
        enabled=True, client=SimpleNamespace(delete=AsyncMock())
    )

    with patch("src.utils.cache.redis_cache.get_cache_client", return_value=cache):
        assert not await delete_unused_thread("t-1", request_key="k-1")

    cache.client.delete.assert_not_awaited()
