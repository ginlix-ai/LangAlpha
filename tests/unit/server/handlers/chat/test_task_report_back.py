"""Locks the task report-back contract.

Jobs are born on the run ledger's terminal CAS; the EXECUTOR arbitrates at
claim time against the durable row (result_delivered_at → drop; live or
interrupted parent → park until the thread's next completed finalize).
``style`` is carried in the durable payload and honored at dispatch: inline
embeds the result and gates subagent tooling off; pointer announces and
keeps TaskOutput available. Unknown styles must fall back to inline — a
durable job may outlive the code that enqueued it.
"""

import contextlib
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.server.handlers.chat.task_report_back import (
    TASK_RB_RESULT_CAP,
    _build_notification_message,
    enqueue_task_report_backs,
    execute_task_report_back,
)


# ---------------------------------------------------------------------------
# _build_notification_message
# ---------------------------------------------------------------------------


def _payload(**over) -> dict:
    base = {
        "task_id": "abc123",
        "display_id": "Task-abc123",
        "subagent_type": "research",
        "description": "look things up",
        "result_text": "the findings",
        "result_truncated": False,
        "result_total_chars": 12,
        "style": "inline",
    }
    base.update(over)
    return base


class TestBuildNotificationMessage:
    def test_inline_embeds_result(self):
        msg = _build_notification_message(_payload())
        assert '<task_result id="Task-abc123" subagent="research">' in msg
        assert "the findings" in msg
        assert "Review the output below" in msg
        assert "TaskOutput" not in msg

    def test_inline_truncation_note(self):
        msg = _build_notification_message(
            _payload(result_truncated=True, result_total_chars=99999)
        )
        assert f"[truncated: showing {TASK_RB_RESULT_CAP} of 99999 chars]" in msg

    def test_inline_missing_result_never_points_at_taskoutput(self):
        # Inline turns have no TaskOutput tool; the recovery hint must not
        # reference it.
        msg = _build_notification_message(_payload(result_text=None))
        assert "workspace files" in msg
        assert "TaskOutput" not in msg

    def test_pointer_directs_to_taskoutput(self):
        msg = _build_notification_message(_payload(style="pointer"))
        assert 'TaskOutput(task_id="abc123")' in msg
        assert "<task_result" not in msg
        assert "Retrieve it" in msg

    def test_unknown_style_renders_inline(self):
        msg = _build_notification_message(_payload(style="carrier-pigeon"))
        assert "<task_result" in msg
        assert "the findings" in msg


# ---------------------------------------------------------------------------
# settled-watch reconciliation (jobs are born at the run's terminal CAS)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_settled_wake_publishes_when_no_open_job():
    wake = AsyncMock()
    with (
        patch(
            "src.server.database.hook_outbox.get_open_notification_job",
            new=AsyncMock(return_value=None),
        ),
        patch("src.server.handlers.chat.report_back.publish_wake", new=wake),
        patch("src.utils.cache.redis_cache.get_cache_client"),
    ):
        await enqueue_task_report_backs(thread_id="t1", all_settled=True)

    assert wake.await_args.kwargs.get("cleared") is True


@pytest.mark.asyncio
async def test_settled_wake_skipped_while_a_job_is_open():
    """An open job means the executor's own outcome (run_id wake or cleared)
    is the signal — a premature cleared wake would drop the pending chip
    while a notification turn is still owed."""
    wake = AsyncMock()
    with (
        patch(
            "src.server.database.hook_outbox.get_open_notification_job",
            new=AsyncMock(return_value={"hook_outbox_id": "j1"}),
        ),
        patch("src.server.handlers.chat.report_back.publish_wake", new=wake),
        patch("src.utils.cache.redis_cache.get_cache_client"),
    ):
        await enqueue_task_report_backs(thread_id="t1", all_settled=True)

    wake.assert_not_awaited()


@pytest.mark.asyncio
async def test_not_settled_is_a_noop():
    probe = AsyncMock()
    with patch(
        "src.server.database.hook_outbox.get_open_notification_job", new=probe
    ):
        await enqueue_task_report_backs(thread_id="t1", all_settled=False)

    probe.assert_not_awaited()


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
):
    """Patch set for the executor's pre-dispatch arbitration."""
    latest = AsyncMock(
        side_effect=[{"status": s} if s else None for s in latest_statuses]
    )
    patches = (
        patch(
            "src.server.database.subagent_runs.get_task_run",
            new=AsyncMock(return_value=run_row),
        ),
        patch(
            "src.server.database.turn_lifecycle.get_latest_attempt", new=latest
        ),
        patch(
            "src.server.handlers.chat.notify_turn.post_notification_turn",
            new=post or AsyncMock(return_value=("dispatched", "rb-run-1")),
        ),
        patch(
            "src.server.handlers.chat.notify_turn.await_run_terminal",
            new=AsyncMock(return_value="done"),
        ),
        patch(
            "src.server.database.hook_outbox.merge_job_payload",
            new=AsyncMock(return_value=True),
        ),
        patch(
            "src.server.database.hook_outbox.defer_claimed_job",
            new=defer or AsyncMock(return_value="pending"),
        ),
        patch(
            "src.server.database.hook_outbox.release_deferred_jobs",
            new=release or AsyncMock(return_value=1),
        ),
        patch(
            "src.server.handlers.chat.report_back.publish_wake",
            new=wake or AsyncMock(),
        ),
        patch("src.utils.cache.redis_cache.get_cache_client"),
        patch("src.server.database.conversation.get_db_connection"),
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


# ---------------------------------------------------------------------------
# dispatch: the recursion gate follows the style
# ---------------------------------------------------------------------------


def _job(style: str) -> dict:
    payload = _payload(style=style)
    payload.update({"workspace_id": "w1", "user_id": "u1"})
    if style == "pointer":
        payload.pop("result_text")
    return {
        "hook_outbox_id": "0f1e2d3c-0000-0000-0000-000000000001",
        "conversation_thread_id": "t1",
        "attempts": 0,
        "payload": payload,
    }


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "style,expected_gate", [("inline", True), ("pointer", False)]
)
async def test_dispatch_gates_subagents_by_style(style, expected_gate):
    posted: list[dict] = []

    async def _post(*, thread_id, body, **kwargs):
        posted.append(body)
        return "dispatched", "rb-run-1"

    with (
        patch(
            "src.server.database.turn_lifecycle.get_latest_attempt",
            new=AsyncMock(return_value={"status": "completed"}),
        ),
        patch(
            "src.server.handlers.chat.notify_turn.post_notification_turn",
            new=AsyncMock(side_effect=_post),
        ),
        patch(
            "src.server.handlers.chat.notify_turn.await_run_terminal",
            new=AsyncMock(return_value="done"),
        ),
        patch(
            "src.server.database.hook_outbox.merge_job_payload",
            new=AsyncMock(return_value=True),
        ),
        patch(
            "src.server.handlers.chat.report_back.publish_wake",
            new=AsyncMock(),
        ),
        patch("src.utils.cache.redis_cache.get_cache_client"),
    ):
        await execute_task_report_back(_job(style))

    assert len(posted) == 1
    assert posted[0]["disable_subagents"] is expected_gate
    content = posted[0]["messages"][0]["content"]
    if style == "pointer":
        assert 'TaskOutput(task_id="abc123")' in content
    else:
        assert "<task_result" in content


@pytest.mark.asyncio
async def test_dispatch_nacks_when_pointer_persist_fails():
    """merge_job_payload failure must propagate (drainer nack): an acked DONE
    row without dispatched_run_id vanishes from the recents ledger, erasing
    the notification from wake-miss recovery."""
    wake = AsyncMock()
    with (
        patch(
            "src.server.database.turn_lifecycle.get_latest_attempt",
            new=AsyncMock(return_value={"status": "completed"}),
        ),
        patch(
            "src.server.handlers.chat.notify_turn.post_notification_turn",
            new=AsyncMock(return_value=("dispatched", "rb-run-1")),
        ),
        patch(
            "src.server.database.hook_outbox.merge_job_payload",
            new=AsyncMock(side_effect=RuntimeError("db down")),
        ),
        patch(
            "src.server.handlers.chat.report_back.publish_wake",
            new=wake,
        ),
        patch("src.utils.cache.redis_cache.get_cache_client"),
    ):
        with pytest.raises(RuntimeError, match="db down"):
            await execute_task_report_back(_job("pointer"))

    wake.assert_not_awaited()


# ---------------------------------------------------------------------------
# recents: drained notification runs stay discoverable via DONE outbox rows
# ---------------------------------------------------------------------------


def _slice_env(recents):
    """Patches for read_task_report_back_status with a stubbed recents read."""
    btm = MagicMock()
    btm.get_live_task_info = AsyncMock(return_value={"active_tasks": []})
    return (
        patch(
            "src.server.database.hook_outbox.get_open_notification_job",
            new=AsyncMock(return_value=None),
        ),
        patch(
            "src.server.services.background_task_manager."
            "BackgroundTaskManager.get_instance",
            return_value=btm,
        ),
        patch(
            "src.server.database.hook_outbox.get_recent_notification_run_ids",
            new=recents,
        ),
    )


@pytest.mark.asyncio
async def test_status_slice_lists_recent_notification_runs():
    from src.server.handlers.chat.task_report_back import (
        read_task_report_back_status,
    )

    recents = AsyncMock(return_value=["rb-2", "rb-1"])
    p1, p2, p3 = _slice_env(recents)
    with p1, p2, p3:
        out = await read_task_report_back_status("t1")

    assert out["recent_report_back_run_ids"] == ["rb-2", "rb-1"]
    assert recents.await_args.args == ("t1", "task_report_back")


@pytest.mark.asyncio
async def test_status_slice_recents_read_failure_reports_empty():
    from src.server.handlers.chat.task_report_back import (
        read_task_report_back_status,
    )

    recents = AsyncMock(side_effect=RuntimeError("db down"))
    p1, p2, p3 = _slice_env(recents)
    with p1, p2, p3:
        out = await read_task_report_back_status("t1")

    # Recents failure degrades to an empty list plus UNKNOWN pendingness:
    # recents are the wake-miss recovery channel, so their outage must not
    # let an otherwise-idle slice authorize the client's teardown.
    assert out["recent_report_back_run_ids"] == []
    assert out["pending_report_back"] is None
