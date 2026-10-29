"""Locks the v2 mux admission row-truth contract.

The client decides a task's terminal outcome by voting channel outcomes
ordered by server-declared run start. A task lane admitted without that
start order would vote as older-than-everything and could crown a stale
predecessor's outcome — so an admission whose row read fails is deferred
(the rescan retries), never opened blind. Drain requires positive terminal
evidence; an unknown row stays live.
"""

import datetime
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.server.handlers.chat.thread_stream_mux_v2 import (
    _PENDING_TTL_S,
    _admission_meta,
    _load_pending,
    _settled_runs_of_open_turn,
    _store_pending,
)
from tests.unit.redis_mock_pipeline import attach_pipeline

SR_DB = "src.server.database.runs.subagent_runs"
STARTED = datetime.datetime(2026, 7, 1, tzinfo=datetime.timezone.utc)
STARTED_MS = STARTED.timestamp() * 1000.0


def _patched(row=None, error=None):
    getter = AsyncMock(return_value=row)
    if error is not None:
        getter.side_effect = error
    return patch(f"{SR_DB}.get_task_run", new=getter)


@pytest.mark.asyncio
async def test_unreadable_row_defers_task_admission():
    with _patched(error=RuntimeError("db down")):
        assert await _admission_meta("run-1", "task:abc", False, None) is None


@pytest.mark.asyncio
async def test_terminal_row_drains_with_start_order():
    row = {"status": "completed", "started_at": STARTED}
    with _patched(row=row):
        meta = await _admission_meta("run-1", "task:abc", False, None)
    assert meta == (True, STARTED_MS)


@pytest.mark.asyncio
async def test_open_row_stays_live_with_start_order():
    row = {"status": "in_progress", "started_at": STARTED}
    with _patched(row=row):
        meta = await _admission_meta("run-1", "task:abc", False, None)
    assert meta == (False, STARTED_MS)


@pytest.mark.asyncio
async def test_caller_supplied_meta_skips_the_row_read():
    with _patched(error=RuntimeError("must not be called")):
        meta = await _admission_meta("run-1", "task:abc", True, STARTED_MS)
    assert meta == (True, STARTED_MS)


@pytest.mark.asyncio
async def test_pending_store_writes_hash_and_refreshes_ttl():
    # The durable copy is what lets a replacement socket inherit a deferral
    # whose socket died before its retry succeeded.
    cache = MagicMock()
    cache.client.hset = AsyncMock()
    cache.client.expire = AsyncMock()
    # Both commands ride one pipeline now; the assertions below still pin the
    # commands themselves, which is the contract that matters.
    attach_pipeline(cache.client)
    await _store_pending(cache, "t-1", "run-1", "task:abc")
    cache.client.hset.assert_awaited_once_with(
        "mux2:pending:t-1", "run-1", "task:abc"
    )
    cache.client.expire.assert_awaited_once_with(
        "mux2:pending:t-1", _PENDING_TTL_S
    )


@pytest.mark.asyncio
async def test_pending_load_decodes_bytes_and_fails_closed():
    cache = MagicMock()
    cache.client.hgetall = AsyncMock(
        return_value={b"run-1": b"task:abc", "run-2": "main"}
    )
    assert await _load_pending(cache, "t-1") == {
        "run-1": "task:abc",
        "run-2": "main",
    }
    cache.client.hgetall = AsyncMock(return_value={})
    assert await _load_pending(cache, "t-1") == {}
    # An error is UNKNOWN debt, never absent debt — {} here would silently
    # strand every deferral this socket was meant to inherit.
    cache.client.hgetall = AsyncMock(side_effect=RuntimeError("redis down"))
    assert await _load_pending(cache, "t-1") is None


@pytest.mark.asyncio
async def test_main_lane_admits_without_start_order():
    # Main-lane outcomes are not voted by start order; a missing row must
    # not block the root channel.
    with patch(
        "src.server.database.runs.lifecycle.get_run",
        new=AsyncMock(side_effect=RuntimeError("db down")),
    ):
        meta = await _admission_meta("run-1", "main", False, None)
    assert meta == (False, None)


@pytest.mark.asyncio
async def test_open_turn_failures_read_as_unserved():
    # The client stops asking for the open turn once it hears ``attached``, so
    # a failed discovery or deferral must be distinguishable from "nothing to
    # drain" for attach to withhold the ack.
    with patch(
        f"{SR_DB}.list_settled_runs_of_unseen_turns",
        new=AsyncMock(side_effect=RuntimeError("db down")),
    ):
        assert await _settled_runs_of_open_turn("t-1", None, 10) is None
    settled = [{"task_run_id": "run-1", "task_id": "abc"}]
    with patch(
        f"{SR_DB}.list_settled_runs_of_unseen_turns",
        new=AsyncMock(return_value=settled),
    ) as listed:
        assert await _settled_runs_of_open_turn("t-1", "root-1", 10) == settled
    listed.assert_awaited_once_with("t-1", "root-1", 10)

    cache = MagicMock()
    cache.client.hset = AsyncMock(side_effect=RuntimeError("redis down"))
    cache.client.expire = AsyncMock()
    attach_pipeline(cache.client)
    assert await _store_pending(cache, "t-1", "run-1", "task:abc") is False


class TestRowTruthCloseCarriesFinish:
    """A task channel closed from row truth names when the ledger settled the
    run (``finished``, epoch ms), as the ``run_end`` it never got would have,
    so a live card ends its run there rather than whenever the close reached
    the browser."""

    SETTLED = datetime.datetime(2026, 7, 1, 0, 3, tzinfo=datetime.timezone.utc)

    @pytest.mark.asyncio
    async def test_row_truth_close_takes_finalized_at(self):
        from src.server.handlers.chat.thread_stream_mux_v2 import _run_row_close

        with _patched(
            row={"status": "error", "started_at": STARTED, "finalized_at": self.SETTLED}
        ):
            close = await _run_row_close("r1", "task:t1")
        assert close == {"outcome": "error", "finished": self.SETTLED.timestamp() * 1000}

    @pytest.mark.asyncio
    async def test_open_row_does_not_close(self):
        from src.server.handlers.chat.thread_stream_mux_v2 import _run_row_close

        with _patched(row={"status": "in_progress", "started_at": STARTED}):
            assert await _run_row_close("r1", "task:t1") is None
