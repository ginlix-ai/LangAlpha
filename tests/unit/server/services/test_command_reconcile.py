"""Which jobs a connect ends: ones whose rows are over, and ones with no row
that no call has waited on for a few chunks. A job whose row still runs is
the watcher's, and one with no row that a call waits on is a live call's,
whichever worker runs it."""

from __future__ import annotations

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest

from ptc_agent.core.sandbox.jobs import WAITER_GONE_S, UnendedJob
from src.server.services.commands import reconcile
from src.server.services.computer_manager._bringup import BringUpMixin


def _job(command_id: str, waited_ago: int | None = 5) -> UnendedJob:
    return UnendedJob(command_id, 1_800_000_000, waited_ago)


def test_a_job_whose_command_is_over_or_whose_waiter_is_gone_is_ended():
    listed = [
        _job("bg-finished"),
        _job("bg-stopped"),
        _job("bg-running", waited_ago=WAITER_GONE_S * 10),
        _job("bg-live-call", waited_ago=WAITER_GONE_S - 1),
        _job("bg-waiter-gone", waited_ago=WAITER_GONE_S + 1),
        _job("bg-from-before-stamps", waited_ago=None),
    ]
    statuses = {"bg-finished": "failed", "bg-stopped": "stopped", "bg-running": "running"}
    assert reconcile.stale_jobs(listed, statuses) == [
        "bg-finished",
        "bg-stopped",
        "bg-waiter-gone",
    ]


@pytest.mark.asyncio
async def test_the_connect_lists_once_and_ends_only_the_stale():
    runtime = AsyncMock()
    listed = [_job("bg-a"), _job("bg-b")]
    with (
        patch.object(reconcile.jobs, "list_unended_jobs", new=AsyncMock(return_value=listed)) as ls,
        patch.object(
            reconcile.cr_db, "statuses_on", new=AsyncMock(return_value={"bg-a": "failed", "bg-b": "running"})
        ) as st,
        patch.object(reconcile.jobs, "end_jobs", new=AsyncMock(return_value=1)) as end,
    ):
        assert await reconcile.end_stale_jobs("c-1", runtime, "/home/workspace") == ["bg-a"]
    assert ls.await_args.args[1] == "/home/workspace/.system/jobs"
    st.assert_awaited_once_with("c-1", ["bg-a", "bg-b"])
    end.assert_awaited_once_with(runtime.exec, "/home/workspace/.system/jobs", ["bg-a"], mark=True)


@pytest.mark.asyncio
async def test_a_computer_with_nothing_unended_reads_no_rows():
    with (
        patch.object(reconcile.jobs, "list_unended_jobs", new=AsyncMock(return_value=[])),
        patch.object(reconcile.cr_db, "statuses_on", new=AsyncMock()) as st,
    ):
        assert await reconcile.end_stale_jobs("c-1", AsyncMock(), "/r") == []
    st.assert_not_awaited()


@pytest.mark.asyncio
async def test_a_lazy_reconnect_is_awaited_before_the_listing():
    """A restart that reconnects lazily hands over a sandbox still starting;
    the listing waits for it rather than finding no runtime."""
    ready = asyncio.Event()
    sandbox = SimpleNamespace(runtime=None, working_dir="/home/workspace")

    async def wait_ready():
        await ready.wait()
        sandbox.runtime = AsyncMock()

    sandbox.wait_ready = wait_ready
    manager = BringUpMixin.__new__(BringUpMixin)
    manager._prune_tasks = set()
    with patch.object(reconcile, "end_stale_jobs", new=AsyncMock(return_value=[])) as end:
        manager.end_stale_jobs_soon("c-1", sandbox)
        await asyncio.sleep(0)
        end.assert_not_awaited()
        ready.set()
        await asyncio.gather(*manager._prune_tasks)
    end.assert_awaited_once_with("c-1", sandbox.runtime, "/home/workspace")
