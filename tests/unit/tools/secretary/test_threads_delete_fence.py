"""Secretary thread deletion goes through the exclusive-T mutation fence.

Review F2 (v4 2.4): the agent-facing delete must take the same
``ThreadMutationRunner.exclusive(thread_id, "delete")`` fence as the HTTP
endpoint and run the DELETE on the fenced session — an unfenced
``delete_thread`` would cascade away a live run's ledger rows out from under
a writer on any worker.
"""

from __future__ import annotations

import json
from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.server.services.thread_mutation import (
    MutationConflict,
    MutationUnavailable,
)
from src.tools.secretary import tools as sec_tools

_FENCED_CONN = object()


def _runner(enter_exc: Exception | None = None) -> MagicMock:
    @asynccontextmanager
    async def _exclusive(thread_id, verb):
        assert verb == "delete"
        if enter_exc is not None:
            raise enter_exc
        yield SimpleNamespace(conn=_FENCED_CONN)

    runner = MagicMock()
    runner.exclusive = _exclusive
    return runner


def _content(cmd) -> dict:
    return json.loads(cmd.update["messages"][0].content)


def _patches(runner, delete_thread):
    disabled_cache = MagicMock(enabled=False, client=None)
    return (
        patch.object(sec_tools, "_verify_thread_owner", AsyncMock(return_value=None)),
        patch.object(sec_tools, "_hitl_confirm", MagicMock(return_value=(True, None))),
        patch(
            "src.server.services.thread_mutation.ThreadMutationRunner.get_instance",
            return_value=runner,
        ),
        patch("src.server.database.conversation.delete_thread", delete_thread),
        patch(
            "src.utils.cache.redis_cache.get_cache_client",
            return_value=disabled_cache,
        ),
    )


@pytest.mark.asyncio
async def test_delete_runs_on_the_fenced_session():
    delete_thread = AsyncMock()
    p1, p2, p3, p4, p5 = _patches(_runner(), delete_thread)
    with p1, p2, p3, p4, p5:
        cmd = await sec_tools._threads_delete("u-1", "t-1", "tc-1")

    delete_thread.assert_awaited_once_with("t-1", conn=_FENCED_CONN)
    assert _content(cmd)["success"] is True


@pytest.mark.asyncio
async def test_delete_refused_while_thread_is_busy():
    """MutationConflict (live run / rival mutation holds the guard) surfaces
    as an error command and the DELETE never executes."""
    delete_thread = AsyncMock()
    conflict = MutationConflict(
        "busy_thread", "delete", "Thread has an active run"
    )
    p1, p2, p3, p4, p5 = _patches(_runner(enter_exc=conflict), delete_thread)
    with p1, p2, p3, p4, p5:
        cmd = await sec_tools._threads_delete("u-1", "t-1", "tc-1")

    delete_thread.assert_not_awaited()
    body = _content(cmd)
    assert body["success"] is False
    assert body["error"] == "Thread has an active run"


@pytest.mark.asyncio
async def test_delete_unavailable_surfaces_retriable_error():
    delete_thread = AsyncMock()
    p1, p2, p3, p4, p5 = _patches(
        _runner(enter_exc=MutationUnavailable("no session")), delete_thread
    )
    with p1, p2, p3, p4, p5:
        cmd = await sec_tools._threads_delete("u-1", "t-1", "tc-1")

    delete_thread.assert_not_awaited()
    body = _content(cmd)
    assert body["success"] is False
    assert "retry" in body["error"].lower()
