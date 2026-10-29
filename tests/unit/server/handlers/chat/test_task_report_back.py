"""Locks the task report-back contract.

Jobs are born on the run ledger's terminal CAS; the EXECUTOR arbitrates at
claim time against the durable row (result_delivered_at → drop; live or
interrupted parent → park until the thread's next completed finalize).
The notification turn announces the outcome; the result rides the durable
archive, never the payload.
"""

import contextlib
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, patch

import pytest

from src.server.services.report_back.subagent import (
    execute_task_report_back,
    publish_cleared_wake_if_no_open_job,
    task_report,
    task_report_message,
)

WAKE_MOD = "src.server.services.report_back.handoff.wake"


# ---------------------------------------------------------------------------
# task_report_message
# ---------------------------------------------------------------------------


def _payload(**over) -> dict:
    base = {
        "task_id": "abc123",
        "display_id": "Task-abc123",
        "subagent_type": "research",
        "description": "look things up",
        "style": "pointer",
    }
    base.update(over)
    return base


class TestTaskReport:
    def test_a_failed_run_without_a_display_id_names_the_task(self):
        report = task_report(
            _payload(display_id=None, description="", final_status="error")
        )
        assert report.model_dump(exclude_none=True) == {
            "kind": "task",
            "task_id": "abc123",
            "display_id": "Task-abc123",
            "failed": True,
            "subagent_type": "research",
        }

    def test_the_row_gets_the_role(self):
        report = task_report(_payload(final_status="completed"))
        assert report.subagent_type == "research"


class TestBuildNotificationMessage:
    def test_carries_only_the_event(self):
        """Posted, it names no time: a task that settled after the turn's last
        model call is posted too, so "after your previous turn ended" could be
        false."""
        msg = task_report_message(task_report(_payload()), "research")
        assert msg == (
            "<system>\n"
            "Background subagent Task-abc123 (research) completed.\n"
            "Task description: look things up\n"
            "</system>"
        )

    def test_a_posted_failure_says_so(self):
        report = task_report(_payload(final_status="error"))
        msg = task_report_message(report, "research")
        assert "Background subagent Task-abc123 (research) FAILED.\n" in msg

    def test_a_failure_taken_in_mid_turn_says_so(self):
        report = task_report(_payload(final_status="error"))
        msg = task_report_message(report, "research", steered=True)
        assert "Task-abc123 (research) FAILED while you were working." in msg


# ---------------------------------------------------------------------------
# settled-watch reconciliation (jobs are born at the run's terminal CAS)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_settled_wake_publishes_when_no_open_job():
    wake = AsyncMock()
    with (
        patch(
            "src.server.database.runs.outbox.list_open_notification_jobs",
            new=AsyncMock(return_value=[]),
        ),
        patch(f"{WAKE_MOD}.publish_wake", new=wake),
        patch("src.utils.cache.redis_cache.get_cache_client"),
    ):
        await publish_cleared_wake_if_no_open_job("t1")

    assert wake.await_args.kwargs.get("cleared") is True


@pytest.mark.asyncio
async def test_settled_wake_skipped_while_a_job_is_open():
    """An open job means the executor's own outcome (run_id wake or cleared)
    is the signal — a premature cleared wake would drop the pending chip
    while a notification turn is still owed."""
    wake = AsyncMock()
    with (
        patch(
            "src.server.database.runs.outbox.list_open_notification_jobs",
            new=AsyncMock(return_value=[{"hook_outbox_id": "j1"}]),
        ),
        patch(f"{WAKE_MOD}.publish_wake", new=wake),
        patch("src.utils.cache.redis_cache.get_cache_client"),
    ):
        await publish_cleared_wake_if_no_open_job("t1")

    wake.assert_not_awaited()


# ---------------------------------------------------------------------------
# executor arbitration against the ledger row
# ---------------------------------------------------------------------------


def _arb_job(**payload_over) -> dict:
    payload = {
        "task_id": "abc123",
        "task_run_id": "run-1",
        "display_id": "Task-abc123",
        "subagent_type": "research",
        "description": "look things up",
        "style": "pointer",
        "workspace_id": "w1",
        "user_id": "u1",
    }
    payload.update(payload_over)
    return {
        "hook_outbox_id": "0f1e2d3c-0000-0000-0000-000000000001",
        "conversation_thread_id": "t1",
        "attempts": 0,
        "payload": payload,
    }


@contextlib.contextmanager
def _arb_env(
    *,
    run_row,
    latest_statuses,
    post=None,
    defer=None,
    release=None,
    wake=None,
    parent_run=None,
):
    """Patch set for the executor's pre-dispatch arbitration. ``parent_run``
    is the attempt row the task's ``parent_run_id`` names."""
    # A plain string is the status alone; a dict is the whole attempt row, for
    # the arms that read more of it than its status.
    rows = [
        s if isinstance(s, dict) else ({"status": s} if s else None)
        for s in latest_statuses
    ]
    # An owed report reads its claim marker before the park's reads; the
    # first row stands for that read too.
    latest = AsyncMock(side_effect=rows[:1] + rows)
    patches = (
        patch(
            "src.server.database.runs.subagent_runs.get_task_run",
            new=AsyncMock(return_value=run_row),
        ),
        patch(
            "src.server.database.runs.lifecycle.get_latest_attempt", new=latest
        ),
        patch(
            "src.server.database.runs.lifecycle.get_run",
            new=AsyncMock(return_value=parent_run),
        ),
        patch(
            "src.server.services.report_back.notify_turn.post_notification_turn",
            new=post or AsyncMock(return_value=("dispatched", "rb-run-1")),
        ),
        patch(
            "src.server.services.report_back.notify_turn.await_run_terminal",
            new=AsyncMock(return_value="done"),
        ),
        patch(
            "src.server.database.runs.outbox.merge_job_payload",
            new=AsyncMock(return_value=True),
        ),
        patch(
            "src.server.database.runs.outbox.defer_claimed_job",
            new=defer or AsyncMock(return_value="pending"),
        ),
        patch(
            "src.server.database.runs.outbox.release_deferred_notifications",
            new=release or AsyncMock(return_value=1),
        ),
        patch(
            f"{WAKE_MOD}.publish_wake",
            new=wake or AsyncMock(),
        ),
        patch("src.utils.cache.redis_cache.get_cache_client"),
        patch("src.server.database.pool.get_db_connection"),
        patch("src.server.services.hook_outbox.HookOutboxDrainer.get_instance"),
    )
    with contextlib.ExitStack() as stack:
        for p in patches:
            stack.enter_context(p)
        yield


@pytest.mark.asyncio
async def test_dispatch_drops_when_result_already_delivered():
    """TaskOutput delivered after the CAS: nothing is owed — no POST, and a
    cleared wake reconciles watchers riding the pending chip."""
    post, wake = AsyncMock(), AsyncMock()
    with _arb_env(
        run_row={"status": "completed", "result_delivered_at": "now"},
        latest_statuses=["completed"],
        post=post,
        wake=wake,
    ):
        await execute_task_report_back(_arb_job())

    post.assert_not_awaited()
    assert wake.await_args.kwargs.get("cleared") is True


@pytest.mark.asyncio
async def test_dispatch_drops_when_run_row_gone():
    post, wake = AsyncMock(), AsyncMock()
    with _arb_env(
        run_row=None, latest_statuses=["completed"], post=post, wake=wake
    ):
        await execute_task_report_back(_arb_job())

    post.assert_not_awaited()
    wake.assert_not_awaited()


@pytest.mark.asyncio
async def test_dispatch_parks_on_live_parent():
    """Jobs are born mid-turn (at the run's CAS); a live parent may still
    fetch the result itself, so the executor parks instead of busy-waiting
    a POST into the running turn."""
    post, defer, release = (
        AsyncMock(),
        AsyncMock(return_value="pending"),
        AsyncMock(),
    )
    with _arb_env(
        run_row={"status": "completed", "result_delivered_at": None},
        latest_statuses=["in_progress", "in_progress"],
        post=post,
        defer=defer,
        release=release,
    ):
        await execute_task_report_back(_arb_job())

    post.assert_not_awaited()
    defer.assert_awaited_once()
    release.assert_not_awaited()


@pytest.mark.asyncio
async def test_park_self_releases_when_parent_finalized_meanwhile():
    """The parent's completed finalize releases deferred jobs — but only
    ones already parked. A finalize landing between the status read and the
    park must be caught by the post-park re-read, which releases the job
    itself (else it waits at infinity for a turn that may never come)."""
    defer, release = AsyncMock(return_value="pending"), AsyncMock()
    with _arb_env(
        run_row={"status": "completed", "result_delivered_at": None},
        latest_statuses=["in_progress", "completed"],
        defer=defer,
        release=release,
    ):
        await execute_task_report_back(_arb_job())

    defer.assert_awaited_once()
    release.assert_awaited_once()


@pytest.mark.asyncio
async def test_dispatch_proceeds_for_undelivered_run_on_settled_parent():
    post = AsyncMock(return_value=("dispatched", "rb-run-1"))
    with _arb_env(
        run_row={"status": "completed", "result_delivered_at": None},
        latest_statuses=["completed"],
        post=post,
    ):
        await execute_task_report_back(_arb_job())

    post.assert_awaited_once()


@pytest.mark.asyncio
@pytest.mark.parametrize("meta", [{"user_id": "u9", "workspace_id": "w9"}, None])
async def test_a_ledger_job_posts_as_its_threads_owner_or_not_at_all(meta):
    """Ledger-enqueued jobs carry task identity only: the turn posts as the
    thread's owner into its workspace, and a thread that is gone owes none."""
    post, wake = AsyncMock(return_value=("dispatched", "rb-run-1")), AsyncMock()
    job = _arb_job()
    del job["payload"]["user_id"], job["payload"]["workspace_id"]
    with (
        _arb_env(
            run_row={"status": "completed", "result_delivered_at": None},
            latest_statuses=["completed"],
            post=post,
            wake=wake,
        ),
        patch(
            "src.server.database.conversation.get_thread_auth_meta",
            new=AsyncMock(return_value=meta),
        ),
    ):
        await execute_task_report_back(job)

    if meta is None:
        post.assert_not_awaited()
        wake.assert_not_awaited()
        return
    assert post.await_args.kwargs["user_id"] == "u9"
    assert post.await_args.kwargs["body"]["workspace_id"] == "w9"


@pytest.mark.asyncio
async def test_a_payload_naming_no_task_still_announces_unlabelled():
    """The label would name nothing, and the turn is owed all the same."""
    post = AsyncMock(return_value=("dispatched", "rb-run-1"))
    with _arb_env(
        run_row={"status": "completed", "result_delivered_at": None},
        latest_statuses=["completed"],
        post=post,
    ):
        await execute_task_report_back(_arb_job(task_id=None, display_id=None))

    post.assert_awaited_once()
    assert "report_source" not in post.await_args.kwargs["body"]


# ---------------------------------------------------------------------------
# dispatch: the notification turn points at TaskOutput
# ---------------------------------------------------------------------------


def _job() -> dict:
    payload = _payload()
    payload.update({"workspace_id": "w1", "user_id": "u1"})
    return {
        "hook_outbox_id": "0f1e2d3c-0000-0000-0000-000000000001",
        "conversation_thread_id": "t1",
        "attempts": 0,
        "payload": payload,
    }


@pytest.mark.asyncio
async def test_dispatch_posts_the_report_as_its_own_turn():
    posted: list[dict] = []

    async def _post(*, thread_id, body, **kwargs):
        posted.append(body)
        return "dispatched", "rb-run-1"

    with (
        patch(
            "src.server.database.runs.lifecycle.get_latest_attempt",
            new=AsyncMock(return_value={"status": "completed"}),
        ),
        patch(
            "src.server.services.report_back.notify_turn.post_notification_turn",
            new=AsyncMock(side_effect=_post),
        ),
        patch(
            "src.server.services.report_back.notify_turn.await_run_terminal",
            new=AsyncMock(return_value="done"),
        ),
        patch(
            "src.server.database.runs.outbox.merge_job_payload",
            new=AsyncMock(return_value=True),
        ),
        patch(
            f"{WAKE_MOD}.publish_wake",
            new=AsyncMock(),
        ),
        patch("src.utils.cache.redis_cache.get_cache_client"),
    ):
        await execute_task_report_back(_job())

    assert len(posted) == 1
    content = posted[0]["messages"][0]["content"]
    assert "Background subagent Task-abc123 (research) completed.\n" in content
    # The message is hidden, so history labels the turn from this.
    assert posted[0]["report_source"] == {
        "kind": "task",
        "task_id": "abc123",
        "display_id": "Task-abc123",
        "description": "look things up",
        "failed": False,
        "subagent_type": "research",
    }


@pytest.mark.asyncio
async def test_dispatch_nacks_when_pointer_persist_fails():
    """merge_job_payload failure must propagate (drainer nack): an acked DONE
    row without dispatched_run_id vanishes from the recents ledger, erasing
    the notification from wake-miss recovery."""
    wake = AsyncMock()
    with (
        patch(
            "src.server.database.runs.lifecycle.get_latest_attempt",
            new=AsyncMock(return_value={"status": "completed"}),
        ),
        patch(
            "src.server.services.report_back.notify_turn.post_notification_turn",
            new=AsyncMock(return_value=("dispatched", "rb-run-1")),
        ),
        patch(
            "src.server.database.runs.outbox.merge_job_payload",
            new=AsyncMock(side_effect=RuntimeError("db down")),
        ),
        patch(
            f"{WAKE_MOD}.publish_wake",
            new=wake,
        ),
        patch("src.utils.cache.redis_cache.get_cache_client"),
    ):
        with pytest.raises(RuntimeError, match="db down"):
            await execute_task_report_back(_job())

    wake.assert_not_awaited()


# ---------------------------------------------------------------------------
# recents: drained notification runs stay discoverable via DONE outbox rows
# ---------------------------------------------------------------------------


def _slice_env(recents):
    """Patches for read_task_report_back_status with a stubbed recents read."""
    return (
        patch(
            "src.server.database.runs.outbox.list_open_notification_jobs",
            new=AsyncMock(return_value=[]),
        ),
        patch(
            "src.server.services.subagent_liveness.get_active_task_ids",
            new=AsyncMock(return_value=[]),
        ),
        patch(
            "src.server.database.runs.outbox.get_recent_notification_run_ids",
            new=recents,
        ),
        patch(
            "src.server.database.runs.command_runs.list_running",
            new=AsyncMock(return_value=[]),
        ),
    )


@pytest.mark.asyncio
async def test_status_slice_lists_recent_notification_runs():
    from src.server.services.report_back.subagent import (
        read_task_report_back_status,
    )

    recents = AsyncMock(return_value=["rb-2", "rb-1"])
    p1, p2, p3, p4 = _slice_env(recents)
    with p1, p2, p3, p4:
        out = await read_task_report_back_status("t1")

    assert out["recent_report_back_run_ids"] == ["rb-2", "rb-1"]
    # A background command's report drains on the same chain, so it is
    # recent the same way.
    assert recents.await_args.args == (
        "t1",
        ("task_report_back", "command_report_back"),
    )


@pytest.mark.asyncio
async def test_status_slice_recents_read_failure_reports_empty():
    from src.server.services.report_back.subagent import (
        read_task_report_back_status,
    )

    recents = AsyncMock(side_effect=RuntimeError("db down"))
    p1, p2, p3, p4 = _slice_env(recents)
    with p1, p2, p3, p4:
        out = await read_task_report_back_status("t1")

    # Recents failure degrades to an empty list plus UNKNOWN pendingness:
    # recents are the wake-miss recovery channel, so their outage must not
    # let an otherwise-idle slice authorize the client's teardown.
    assert out["recent_report_back_run_ids"] == []
    assert out["pending_report_back"] is None


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "unread",
    [
        "src.server.services.subagent_liveness.get_active_task_ids",
        "src.server.database.runs.command_runs.list_running",
    ],
    ids=["tasks", "commands"],
)
async def test_an_unread_writer_list_leaves_pendingness_unknown(unread):
    """A subagent or command still running has no job yet, so a list that
    could not be read must not let an empty outbox read as drained."""
    from src.server.services.report_back.subagent import (
        read_task_report_back_status,
    )

    p1, p2, p3, p4 = _slice_env(AsyncMock(return_value=[]))
    with p1, p2, p3, p4, patch(unread, new=AsyncMock(side_effect=RuntimeError("db down"))):
        out = await read_task_report_back_status("t1")

    assert out["pending_report_back"] is None
    assert out["active_tasks"] == out["active_commands"] == []


# ---------------------------------------------------------------------------
# post-finalize/pre-ack window: a job stays open through the executor's
# terminal wait and ack, but is owed only until its dispatched run ends. An
# ended run joins recents at read time (terminal ⇒ persisted ⇒ replayable),
# so a reloading client that replayed the turn never re-attaches it.
# ---------------------------------------------------------------------------


def _open_job(run_id=None, hook_type="task_report_back"):
    return {"hook_type": hook_type, "payload": {"dispatched_run_id": run_id} if run_id else {}}


def _open_job_env(recents, statuses, jobs=None):
    jobs = jobs if jobs is not None else [_open_job("run-x")]
    return (
        patch(
            "src.server.database.runs.outbox.list_open_notification_jobs",
            new=AsyncMock(return_value=jobs),
        ),
        patch(
            "src.server.services.subagent_liveness.get_active_task_ids",
            new=AsyncMock(return_value=[]),
        ),
        patch(
            "src.server.database.runs.outbox.get_recent_notification_run_ids",
            new=AsyncMock(return_value=recents),
        ),
        patch("src.server.database.runs.lifecycle.get_run_statuses", new=statuses),
    )


@pytest.mark.asyncio
async def test_an_ended_notification_run_is_no_longer_owed():
    """The tip under a delivered notification must drop with the run, not
    after the executor's terminal poll and ack."""
    from src.server.services.report_back.subagent import (
        read_task_report_back_status,
    )

    statuses = AsyncMock(return_value={"run-x": "completed"})
    p1, p2, p3, p4 = _open_job_env(["rb-1"], statuses)
    with p1, p2, p3, p4:
        out = await read_task_report_back_status("t1")

    assert out["pending_report_back"] is False
    assert out["report_back_run_id"] is None
    assert out["recent_report_back_run_ids"] == ["run-x", "rb-1"]


@pytest.mark.asyncio
async def test_open_jobs_live_run_stays_out_of_recents():
    from src.server.services.report_back.subagent import (
        read_task_report_back_status,
    )

    statuses = AsyncMock(return_value={"run-x": "in_progress"})
    p1, p2, p3, p4 = _open_job_env(["rb-1"], statuses)
    with p1, p2, p3, p4:
        out = await read_task_report_back_status("t1")

    assert out["pending_report_back"] is True
    assert out["recent_report_back_run_ids"] == ["rb-1"]
    assert out["report_back_run_id"] == "run-x"


@pytest.mark.asyncio
async def test_a_dispatched_run_without_a_row_is_live():
    """Admission returns the run id before its row commits."""
    from src.server.services.report_back.subagent import (
        read_task_report_back_status,
    )

    statuses = AsyncMock(return_value={})
    p1, p2, p3, p4 = _open_job_env(["rb-1"], statuses)
    with p1, p2, p3, p4:
        out = await read_task_report_back_status("t1")

    assert out["pending_report_back"] is True
    assert out["report_back_run_id"] == "run-x"


@pytest.mark.asyncio
async def test_an_ended_head_names_the_next_owed_job():
    from src.server.services.report_back.subagent import (
        read_task_report_back_status,
    )

    jobs = [_open_job("run-x"), _open_job("run-y"), _open_job()]
    statuses = AsyncMock(return_value={"run-x": "completed", "run-y": "in_progress"})
    p1, p2, p3, p4 = _open_job_env(["rb-1"], statuses, jobs)
    with p1, p2, p3, p4:
        out = await read_task_report_back_status("t1")

    assert out["pending_report_back"] is True
    assert out["report_back_run_id"] == "run-y"
    assert out["recent_report_back_run_ids"] == ["run-x", "rb-1"]


@pytest.mark.asyncio
async def test_the_slice_names_whose_reports_are_owed():
    """A client names what it waits on: a command's notice needs no wait, a
    subagent's report does. An ended run's job is owed no more."""
    from src.server.services.report_back.subagent import (
        read_task_report_back_status,
    )

    jobs = [_open_job("run-x"), _open_job("run-y", "command_report_back"), _open_job(None, "command_report_back")]
    statuses = AsyncMock(return_value={"run-x": "completed", "run-y": "in_progress"})
    p1, p2, p3, p4 = _open_job_env([], statuses, jobs)
    with p1, p2, p3, p4:
        out = await read_task_report_back_status("t1")
    assert out["owed_report_kinds"] == ["command"]

    failing = patch(
        "src.server.database.runs.outbox.list_open_notification_jobs",
        new=AsyncMock(side_effect=RuntimeError("db down")),
    )
    p1, p2, p3, p4 = _open_job_env([], statuses, jobs)
    with p1, failing, p2, p3, p4:
        out = await read_task_report_back_status("t1")
    assert out["pending_report_back"] is None and out["owed_report_kinds"] is None


@pytest.mark.asyncio
async def test_an_ended_run_already_in_recents_is_not_duplicated():
    from src.server.services.report_back.subagent import (
        read_task_report_back_status,
    )

    statuses = AsyncMock(return_value={"run-x": "completed"})
    p1, p2, p3, p4 = _open_job_env(["run-x", "rb-1"], statuses)
    with p1, p2, p3, p4:
        out = await read_task_report_back_status("t1")

    assert out["recent_report_back_run_ids"] == ["run-x", "rb-1"]


@pytest.mark.asyncio
async def test_a_failed_run_status_read_keeps_the_job_owed():
    from src.server.services.report_back.subagent import (
        read_task_report_back_status,
    )

    statuses = AsyncMock(side_effect=RuntimeError("db down"))
    p1, p2, p3, p4 = _open_job_env(["rb-1"], statuses)
    with p1, p2, p3, p4:
        out = await read_task_report_back_status("t1")

    assert out["recent_report_back_run_ids"] == ["rb-1"]
    assert out["pending_report_back"] is True
    assert out["report_back_run_id"] == "run-x"


# ---------------------------------------------------------------------------
# a cancel releases every parked job on the thread — but a stop must not be
# the thing that starts a turn. Read off the task's own parent attempt.
# ---------------------------------------------------------------------------

_STOPPED_AT = datetime(2026, 8, 1, 12, 0, tzinfo=timezone.utc)
_STOPPED_RUN = "attempt-stopped"


def _cancelled_attempt(**over) -> dict:
    return {
        "status": "cancelled",
        "cancel_requested_at": _STOPPED_AT,
        "conversation_response_id": _STOPPED_RUN,
        "metadata": {"cancelled_by_user": True},
        **over,
    }


def _settled_run(*, settled_at, parent_run_id=_STOPPED_RUN) -> dict:
    return {
        "status": "completed",
        "result_delivered_at": None,
        "parent_run_id": parent_run_id,
        "finalized_at": settled_at,
    }


@pytest.mark.asyncio
async def test_dispatch_drops_when_the_users_own_stop_released_it():
    """The result was already settled and parked when they pressed stop, and
    the cancel finalize is what released it. Posting here would make the stop
    itself open a billable turn a second later.
    """
    post, wake = AsyncMock(), AsyncMock()
    with _arb_env(
        run_row=_settled_run(settled_at=_STOPPED_AT - timedelta(seconds=30)),
        parent_run=_cancelled_attempt(),
        latest_statuses=[_cancelled_attempt()],
        post=post,
        wake=wake,
    ):
        await execute_task_report_back(_arb_job())

    post.assert_not_awaited()
    assert wake.await_args.kwargs.get("cleared") is True


@pytest.mark.asyncio
async def test_the_stop_still_covers_it_once_the_user_starts_another_turn():
    """Whether the drainer reached the job before the user's next turn
    started must not decide whether the stop opens a turn: the drop is read
    off the task's own parent, so it holds while the next turn runs (no park
    behind it) and after it ends."""
    post, defer, wake = AsyncMock(), AsyncMock(return_value="pending"), AsyncMock()
    with _arb_env(
        run_row=_settled_run(settled_at=_STOPPED_AT - timedelta(seconds=30)),
        parent_run=_cancelled_attempt(),
        latest_statuses=[{"status": "in_progress", "conversation_response_id": "next"}],
        post=post,
        defer=defer,
        wake=wake,
    ):
        await execute_task_report_back(_arb_job())

    post.assert_not_awaited()
    defer.assert_not_awaited()
    assert wake.await_args.kwargs.get("cleared") is True


@pytest.mark.asyncio
async def test_dispatch_announces_a_task_the_stopped_turn_did_not_launch():
    """The task belongs to an earlier turn and settled while a later one ran,
    so its job was parked. Cancelling that later turn releases it — but the
    stop was never about this result, and a timestamp-only test would read the
    earlier settle as covered and lose the notification for good.
    """
    post = AsyncMock(return_value=("dispatched", "rb-run-1"))
    with _arb_env(
        run_row=_settled_run(
            settled_at=_STOPPED_AT - timedelta(seconds=30),
            parent_run_id="attempt-earlier",
        ),
        parent_run={"status": "completed", "conversation_response_id": "attempt-earlier"},
        latest_statuses=[_cancelled_attempt()],
        post=post,
    ):
        await execute_task_report_back(_arb_job())

    post.assert_awaited_once()


@pytest.mark.asyncio
async def test_dispatch_still_announces_a_result_that_landed_after_the_stop():
    """News the stop cannot have been about. A blanket "this thread was
    cancelled once" test would silence every later completion on it — the
    exact job this pipeline exists to do.
    """
    post = AsyncMock(return_value=("dispatched", "rb-run-1"))
    with _arb_env(
        run_row=_settled_run(settled_at=_STOPPED_AT + timedelta(minutes=5)),
        parent_run=_cancelled_attempt(),
        latest_statuses=[_cancelled_attempt()],
        post=post,
    ):
        await execute_task_report_back(_arb_job())

    post.assert_awaited_once()


@pytest.mark.asyncio
async def test_dispatch_announces_when_the_stop_carries_no_timestamp():
    """Undecidable without both stamps, and silence is the worse failure —
    the pipeline's stated posture is a duplicate over a lost notification.
    """
    post = AsyncMock(return_value=("dispatched", "rb-run-1"))
    with _arb_env(
        run_row=_settled_run(settled_at=_STOPPED_AT - timedelta(seconds=30)),
        parent_run=_cancelled_attempt(cancel_requested_at=None),
        latest_statuses=[_cancelled_attempt(cancel_requested_at=None)],
        post=post,
    ):
        await execute_task_report_back(_arb_job())

    post.assert_awaited_once()


@pytest.mark.asyncio
async def test_dispatch_announces_a_result_a_shutdown_cancel_released():
    """A shutdown or recovery cancels the turn and stamps the same time, but
    nobody asked for silence, so the result still opens a turn."""
    post = AsyncMock(return_value=("dispatched", "rb-run-1"))
    with _arb_env(
        run_row=_settled_run(settled_at=_STOPPED_AT - timedelta(seconds=30)),
        parent_run=_cancelled_attempt(metadata={}),
        latest_statuses=[_cancelled_attempt(metadata={})],
        post=post,
    ):
        await execute_task_report_back(_arb_job())

    post.assert_awaited_once()
