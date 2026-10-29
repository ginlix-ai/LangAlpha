"""The steer executor: what a steer no live turn read becomes.

A chat gateway resubmits what its run missed, so its row is returned rather
than answered twice. A web or MarketView steer becomes a turn of its own,
under the surface it was sent from, carrying ``steer_job_id`` so START claims
the row. After an error, a user steer waits for the user's next move.
"""

from __future__ import annotations

from contextlib import ExitStack
from unittest.mock import AsyncMock, patch

import pytest

from src.server.services.report_back import steer_executor

_OUTBOX = "src.server.database.runs.outbox"
_RUNS = "src.server.database.runs.lifecycle"
_NOTIFY = "src.server.services.report_back.notify_turn"

THREAD_ID = "t-1"
JOB_ID = "job-1"


def _job(attempts: int = 1) -> dict:
    return {
        "hook_outbox_id": JOB_ID,
        "attempts": attempts,
        "idempotency_key": "steer:key-1",
        "conversation_thread_id": THREAD_ID,
    }


def _row(payload: dict, attempts: int = 1) -> dict:
    return {
        "id": JOB_ID,
        "status": "claimed",
        "attempts": attempts,
        "run_id": "run-ended",
        "payload": payload,
    }


def _payload(**over) -> dict:
    return {
        "target": "thread",
        "sender": "user",
        "content": "also cover bonds",
        "user_id": "u-1",
        "workspace_id": "ws-1",
        "platform": "web",
        "chat_app": False,
        "request_key": "rk-1",
        **over,
    }


async def _execute(payload: dict, *, latest_status: str = "completed", post=None):
    mocks = {
        "finish": AsyncMock(return_value=True),
        "defer": AsyncMock(return_value="pending"),
        "post": post or AsyncMock(return_value=("dispatched", "run-new")),
        "park": AsyncMock(return_value=False),
    }
    with ExitStack() as stack:
        enter = stack.enter_context
        enter(patch(f"{_OUTBOX}.get_steer_job", AsyncMock(return_value=_row(payload))))
        enter(patch(f"{_OUTBOX}.finish_claimed_steer", mocks["finish"]))
        enter(patch(f"{_OUTBOX}.defer_claimed_job", mocks["defer"]))
        enter(patch(f"{_OUTBOX}.extend_job_lease", AsyncMock(return_value=True)))
        enter(
            patch(
                f"{_RUNS}.get_latest_attempt",
                AsyncMock(return_value={"status": latest_status}),
            )
        )
        enter(patch(f"{_RUNS}.get_run", AsyncMock(return_value={"id": "run-new"})))
        enter(patch(f"{_NOTIFY}.park_while_thread_busy", mocks["park"]))
        enter(patch(f"{_NOTIFY}.post_notification_turn", mocks["post"]))
        await steer_executor.execute_steer(_job())
    return mocks


@pytest.mark.asyncio
async def test_a_chat_app_steer_is_returned_not_answered():
    mocks = await _execute(_payload(platform="telegram", chat_app=True))

    mocks["post"].assert_not_awaited()
    mocks["finish"].assert_awaited_once_with(
        JOB_ID,
        attempts=1,
        outcome="returned",
        patch={"returned_run_id": "run-ended"},
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("platform", ["web", "market_view:AAPL"])
async def test_a_user_steer_becomes_its_own_turn_under_its_surface(platform):
    mocks = await _execute(_payload(platform=platform))

    body = mocks["post"].await_args.kwargs["body"]
    assert body["messages"] == [{"role": "user", "content": "also cover bonds"}]
    assert body["platform"] == platform
    assert body["steer_job_id"] == JOB_ID
    assert body["request_key"] == "rk-1"
    assert mocks["post"].await_args.kwargs["user_id"] == "u-1"
    # START committed the run, so the row closes as that turn.
    mocks["finish"].assert_awaited_once_with(
        JOB_ID,
        attempts=1,
        outcome="turn",
        patch={"dispatched_run_id": "run-new"},
    )


@pytest.mark.asyncio
async def test_a_user_steer_without_a_key_gets_one_stable_per_job():
    """A re-post after a lost answer must dedup on the same key."""
    mocks = await _execute(_payload(request_key=None))

    body = mocks["post"].await_args.kwargs["body"]
    assert body["request_key"] == steer_executor.steer_request_key(JOB_ID)


@pytest.mark.asyncio
async def test_a_user_steer_after_an_error_waits_for_the_users_next_move():
    """A turn of its own would fail the same way and make Retry stale."""
    mocks = await _execute(_payload(), latest_status="error")

    mocks["defer"].assert_awaited_once()
    mocks["post"].assert_not_awaited()
    mocks["park"].assert_not_awaited()
    mocks["finish"].assert_not_awaited()


@pytest.mark.asyncio
async def test_a_turn_that_took_the_steer_first_leaves_the_row_alone():
    """START refused the executor's turn: a live turn's hold owns the row."""
    mocks = await _execute(
        _payload(), post=AsyncMock(return_value=("taken", None))
    )

    mocks["finish"].assert_not_awaited()
    mocks["defer"].assert_not_awaited()


@pytest.mark.asyncio
async def test_a_busy_cap_at_the_ceiling_settles_through_the_defer():
    """The executor parks as always; at the attempts ceiling the defer itself
    settles the row per sender (``outbox._exhaust_steers``), so no outcome is
    written here."""
    post = AsyncMock(return_value=("cap", None))
    mocks = await _execute(_payload(), post=post)

    mocks["defer"].assert_awaited_once()
    mocks["finish"].assert_not_awaited()


@pytest.mark.asyncio
async def test_the_exhaust_followup_hands_a_task_steer_to_its_refresh():
    job = {"hook_outbox_id": JOB_ID, "payload": {"target": "task", "task_run_id": "tr-1"}}
    refresh = AsyncMock()
    with patch(
        "src.server.services.report_back.task_steer.refresh_exhausted", refresh
    ):
        await steer_executor._on_exhausted(job)

    refresh.assert_awaited_once_with(job)
