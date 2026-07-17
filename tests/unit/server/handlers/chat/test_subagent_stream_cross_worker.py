"""Locks the cross-worker per-task SSE consumer contract.

A GET /threads/{tid}/tasks/{task_id} landing on a worker that doesn't own
the task must NOT stall on the local registry or close the stream as if
the task finished. Liveness comes from the cross-worker signals: the Redis
meta hash and the task's namespace advisory lock. A local entry counts as
the writer only when it carries the live asyncio task.
"""

from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.server.handlers.chat.stream_from_log import (
    _subagent_writer_settled,
    _wait_for_subagent_task,
    stream_subagent_from_log,
)

_MOD = "src.server.handlers.chat.stream_from_log"


# ---------------------------------------------------------------------------
# _subagent_writer_settled — the remote terminal predicate
# ---------------------------------------------------------------------------


class TestWriterSettled:
    async def _settled(self, meta, held) -> bool:
        with (
            patch(f"{_MOD}.read_task_meta", new=AsyncMock(return_value=meta)),
            patch(
                "src.server.services.writer_guard.held_task_namespaces",
                new=AsyncMock(return_value=held),
            ),
        ):
            return await _subagent_writer_settled("t1", "aaa111")

    @pytest.mark.asyncio
    async def test_lock_held_keeps_tailing_whatever_meta_says(self):
        # Lock-first: a resume may hold the lock before rewriting terminal
        # meta, and a spawn's meta publication may have failed — the lock
        # is authoritative.
        for meta in (None, {"status": "completed"}, {"status": "running"}):
            assert await self._settled(meta, {"aaa111"}) is False

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "meta", [None, {"status": "completed"}, {"status": "running"}]
    )
    async def test_lock_free_is_settled(self, meta):
        # Free lock = settled or owner died; meta "running" without the
        # lock is a crashed worker's leftover.
        assert await self._settled(meta, set()) is True

    @pytest.mark.asyncio
    async def test_probe_failure_falls_back_to_meta_running(self):
        assert await self._settled({"status": "running"}, None) is False

    @pytest.mark.asyncio
    @pytest.mark.parametrize("meta", [None, {"status": "cancelled"}])
    async def test_probe_failure_with_no_live_meta_settles(self, meta):
        assert await self._settled(meta, None) is True


# ---------------------------------------------------------------------------
# _wait_for_subagent_task — no 30s stall when the task is known cross-worker
# ---------------------------------------------------------------------------


def _store_with(registry):
    store = MagicMock()
    store.get_registry = AsyncMock(return_value=registry)
    return store


class TestWaitForSubagentTask:
    @pytest.mark.asyncio
    async def test_local_task_returned(self):
        task = SimpleNamespace(task_id="aaa111")
        registry = MagicMock()
        registry.get_task_by_task_id = AsyncMock(return_value=task)
        with patch(
            f"{_MOD}.BackgroundRegistryStore.get_instance",
            return_value=_store_with(registry),
        ):
            assert await _wait_for_subagent_task("t1", "aaa111") is task

    @pytest.mark.asyncio
    async def test_cross_worker_meta_short_circuits_without_sleeping(self):
        sleep = AsyncMock()
        with (
            patch(
                f"{_MOD}.BackgroundRegistryStore.get_instance",
                return_value=_store_with(None),
            ),
            patch(
                f"{_MOD}.read_task_meta",
                new=AsyncMock(return_value={"status": "running"}),
            ),
            patch(f"{_MOD}.asyncio.sleep", new=sleep),
        ):
            assert await _wait_for_subagent_task("t1", "aaa111") is None
        sleep.assert_not_awaited()


# ---------------------------------------------------------------------------
# Branch selection — who gets the remote predicate
# ---------------------------------------------------------------------------


async def _captured_terminal_check(local_task):
    """Run the consumer with a stubbed inner loop; return the terminal_check
    it was wired with."""
    captured = {}

    async def _fake_inner(**kwargs):
        captured.update(kwargs)
        return
        yield  # pragma: no cover — makes this an async generator

    with (
        patch(f"{_MOD}._wait_for_subagent_task", new=AsyncMock(return_value=local_task)),
        patch(f"{_MOD}._stream_from_redis_log", new=_fake_inner),
    ):
        async for _ in stream_subagent_from_log("t1", "aaa111"):
            pass
    return captured["terminal_check"]


class TestBranchSelection:
    @pytest.mark.asyncio
    async def test_no_local_entry_uses_remote_predicate(self):
        check = await _captured_terminal_check(None)
        with patch(
            f"{_MOD}._subagent_writer_settled", new=AsyncMock(return_value=False)
        ) as settled:
            assert await check() is False
        settled.assert_awaited_once_with("t1", "aaa111")

    @pytest.mark.asyncio
    async def test_inert_placeholder_is_not_trusted(self):
        # completed=True with no asyncio task: a peer-worker placeholder,
        # not the writer — must consult the cross-worker signals.
        inert = SimpleNamespace(task_id="aaa111", completed=True, asyncio_task=None)
        check = await _captured_terminal_check(inert)
        with patch(
            f"{_MOD}._subagent_writer_settled", new=AsyncMock(return_value=False)
        ):
            assert await check() is False

    @pytest.mark.asyncio
    async def test_done_outer_task_is_not_trusted(self):
        # A double-cancel can leave the shielded inner handler emitting
        # after the outer asyncio task is done — the namespace lock (still
        # held in that state) must decide, not the local done() flag.
        done_outer = MagicMock()
        done_outer.asyncio_task = MagicMock()
        done_outer.asyncio_task.done = MagicMock(return_value=True)
        check = await _captured_terminal_check(done_outer)
        with patch(
            f"{_MOD}._subagent_writer_settled", new=AsyncMock(return_value=False)
        ) as settled:
            assert await check() is False
        settled.assert_awaited_once_with("t1", "aaa111")

    @pytest.mark.asyncio
    async def test_live_local_writer_uses_local_signals(self):
        running = MagicMock()
        running.completed = False
        running.asyncio_task = MagicMock()
        running.asyncio_task.done = MagicMock(return_value=False)
        running.sse_consumer_count = 0
        check = await _captured_terminal_check(running)
        with patch(
            f"{_MOD}._subagent_writer_settled",
            new=AsyncMock(side_effect=AssertionError("must not probe remotely")),
        ):
            assert await check() is False
            running.completed = True
            assert await check() is True
