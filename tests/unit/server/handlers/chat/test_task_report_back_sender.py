"""A task's report-back turn posts as the sender whose turn started the task.

The task's run row names that sender (``subagent_runs.user_id``). It is the
thread's owner for every turn that runs today, so the run row and the thread
are given two different accounts to show which one the turn posts as.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, patch

import pytest

from src.server.services.report_back.subagent import execute_task_report_back
from tests.unit.server.handlers.chat.test_task_report_back import _arb_env, _arb_job

SENDER = "u-sender"
OWNER = "u-owner"


async def _post_for(run_row: dict) -> AsyncMock:
    post = AsyncMock(return_value=("dispatched", "rb-run-1"))
    job = _arb_job()
    del job["payload"]["user_id"], job["payload"]["workspace_id"]
    with (
        _arb_env(run_row=run_row, latest_statuses=["completed"], post=post),
        patch(
            "src.server.database.conversation.get_thread_auth_meta",
            new=AsyncMock(return_value={"user_id": OWNER, "workspace_id": "w9"}),
        ),
    ):
        await execute_task_report_back(job)
    return post


@pytest.mark.asyncio
async def test_the_report_posts_as_the_runs_sender_into_the_threads_workspace():
    post = await _post_for({"status": "completed", "result_delivered_at": None, "user_id": SENDER})

    assert post.await_args.kwargs["user_id"] == SENDER
    assert post.await_args.kwargs["body"]["workspace_id"] == "w9"


@pytest.mark.asyncio
async def test_a_run_row_naming_no_one_posts_as_the_threads_owner():
    """A row from before the column was written names no one."""
    post = await _post_for({"status": "completed", "result_delivered_at": None, "user_id": None})

    assert post.await_args.kwargs["user_id"] == OWNER
