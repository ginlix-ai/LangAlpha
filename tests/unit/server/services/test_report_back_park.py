"""park_while_thread_busy: the one park both report-back executors take while
the thread they report to is live or interrupted, and the release a claim
takes itself when a turn ended while it held its job."""

from __future__ import annotations

import contextlib
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.server.services.report_back.notify_turn import park_while_thread_busy


@contextlib.contextmanager
def _env(statuses, *, parked="pending"):
    latest = AsyncMock(
        side_effect=[{"status": s} if s else None for s in statuses]
    )
    defer = AsyncMock(return_value=parked)
    release = AsyncMock(return_value=1)
    drainer = MagicMock()
    with (
        patch("src.server.database.runs.lifecycle.get_latest_attempt", new=latest),
        patch("src.server.database.runs.outbox.defer_claimed_job", new=defer),
        patch(
            "src.server.database.runs.outbox.release_deferred_notifications",
            new=release,
        ),
        patch("src.server.database.pool.get_db_connection"),
        patch(
            "src.server.services.hook_outbox.HookOutboxDrainer.get_instance",
            return_value=drainer,
        ),
    ):
        yield latest, defer, release, drainer


async def _park(**kwargs) -> bool:
    return await park_while_thread_busy(
        "job-1", 2, "t-1", log_prefix="[TEST]", subject="task Task-1", **kwargs
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("status", [None, "completed", "error", "cancelled"])
async def test_an_idle_thread_lets_the_caller_post(status):
    with _env([status]) as (_, defer, release, _):
        assert await _park() is False
    defer.assert_not_awaited()
    release.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("status", ["in_progress", "interrupted"])
async def test_a_busy_thread_parks_the_job(status):
    with _env([status, status]) as (_, defer, release, drainer):
        assert await _park() is True
    defer.assert_awaited_once_with("job-1", attempts=2, max_attempts=5)
    release.assert_not_awaited()
    drainer.nudge.assert_not_called()


@pytest.mark.asyncio
async def test_a_finalize_between_the_read_and_the_park_is_caught():
    """That finalize's release pass ran before the job was parked, so it saw
    nothing to release; the re-read releases the job itself and wakes the
    drainer, else it waits at infinity for a turn that may never come."""
    with _env(["in_progress", "completed"]) as (_, defer, release, drainer):
        assert await _park() is True
    defer.assert_awaited_once()
    release.assert_awaited_once()
    assert release.await_args.args[1] == "t-1"
    drainer.nudge.assert_called_once()


@pytest.mark.asyncio
async def test_the_re_read_waits_out_a_finalize_in_flight():
    """A finalize whose release pass ran before the park still reads live
    to a plain read until it commits, which strands the job; the re-read
    waits on the row that finalize holds."""
    with _env(["in_progress", "completed"]) as (latest, _, release, _):
        assert await _park() is True
    assert latest.await_args_list[1].kwargs == {"wait_for_finalize": True}
    release.assert_awaited_once()


@pytest.mark.asyncio
async def test_an_alert_held_at_the_ceiling_is_released_by_the_re_read_too():
    """A market-target alert parked at the attempts ceiling is held rather
    than dead, still parked for the finalize release the re-read stands in
    for."""
    with _env(["in_progress", "completed"], parked="exhausted") as (
        _, _, release, drainer
    ):
        assert await _park(parks_at_ceiling=True) is True
    release.assert_awaited_once()
    drainer.nudge.assert_called_once()


@pytest.mark.asyncio
async def test_a_steer_settled_at_the_ceiling_releases_nothing():
    """A steer's defer at the ceiling settles it (returned, dropped, closed,
    or parked for the user) rather than parking it for a finalize's release,
    so the park has nothing to stand in for."""
    with _env(["in_progress", "completed"], parked="exhausted") as (
        latest, _, release, drainer
    ):
        assert await _park() is True
    assert latest.await_count == 1
    release.assert_not_awaited()
    drainer.nudge.assert_not_called()


@pytest.mark.asyncio
@pytest.mark.parametrize("parked", ["dead", None])
async def test_a_job_not_parked_pending_is_not_released(parked):
    """Dead at the attempts ceiling, or the fence lost: either way the caller
    stops and there is no parked row of its own to release."""
    with _env(["in_progress"], parked=parked) as (latest, _, release, _):
        assert await _park() is True
    assert latest.await_count == 1
    release.assert_not_awaited()


# release_if_a_turn_ended: the look a turn's ending buys a job its finalize
# skipped because the job was claimed.


def _attempt(run_id: str, status: str) -> dict:
    return {"conversation_response_id": run_id, "status": status}


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("since", "now"),
    [
        (_attempt("r-0", "completed"), _attempt("r-1", "completed")),
        (_attempt("r-0", "in_progress"), _attempt("r-0", "error")),
        (None, _attempt("r-1", "cancelled")),
    ],
    ids=["a-later-turn", "the-turn-running-then", "the-first-turn"],
)
async def test_a_turn_ended_since_the_claim_releases_the_thread(since, now):
    from src.server.services.report_back.notify_turn import (
        release_if_a_turn_ended,
        run_marker,
    )

    with _env([]) as (latest, _, release, drainer):
        latest.side_effect = None
        latest.return_value = now
        assert await release_if_a_turn_ended("t-1", run_marker(since)) is True
    # Behind a finalize in flight, which a plain read would take for live.
    assert latest.await_args.kwargs == {"wait_for_finalize": True}
    release.assert_awaited_once()
    assert release.await_args.args[1] == "t-1"
    drainer.nudge.assert_called_once()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("since", "now"),
    [
        # Nothing ended since: a refusal on every look must not loop.
        (_attempt("r-1", "completed"), _attempt("r-1", "completed")),
        # A turn live or awaiting the user releases it when it ends.
        (_attempt("r-0", "completed"), _attempt("r-1", "in_progress")),
        (_attempt("r-0", "completed"), _attempt("r-1", "interrupted")),
        (None, None),
    ],
    ids=["no-new-turn", "live", "interrupted", "no-turn-yet"],
)
async def test_no_ending_since_the_claim_releases_nothing(since, now):
    from src.server.services.report_back.notify_turn import (
        release_if_a_turn_ended,
        run_marker,
    )

    with _env([]) as (latest, _, release, drainer):
        latest.side_effect = None
        latest.return_value = now
        assert await release_if_a_turn_ended("t-1", run_marker(since)) is False
    release.assert_not_awaited()
    drainer.nudge.assert_not_called()
