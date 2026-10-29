"""A background command's notice: the approved wording, who decides it is owed,
and the two ways it reaches the agent.

The literals below are copied from the approved design page; a change to one
is a change to prompt surface and needs the same review the page had.
"""

from __future__ import annotations

from contextlib import ExitStack, contextmanager
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, patch

import pytest

from src.server.services.report_back import command as cmd
from src.server.services.report_back.notify_turn import NotOwed

_ID = "bg-3f9a1c2e7d10"
_LOG = ".agents/threads/3f9a1c2e/commands/bg-3f9a1c2e7d10.log"
_T0 = datetime(2026, 10, 24, 14, 0, tzinfo=timezone.utc)

# --- approved on the design page, verbatim --------------------------------

APPROVED_COMPLETED_STEERED = (
    "<system>\n"
    "Background command bg-3f9a1c2e7d10 (Run 10-year backtest) completed with exit "
    "code 0 after 23m while you were working. Output: "
    ".agents/threads/3f9a1c2e/commands/bg-3f9a1c2e7d10.log\n"
    "</system>"
)
APPROVED_AFTER_TURN_LIMIT = (
    "<system>\n"
    "Background command bg-3f9a1c2e7d10 (Run 10-year backtest) FAILED after 1h52m: "
    "it was stopped an hour after your turn ended, with no wakeup checking on it. "
    "What it wrote is in .agents/threads/3f9a1c2e/commands/bg-3f9a1c2e7d10.log. If "
    "the user still needs it, rerun it; it resumes where it stopped only if it saved "
    "its progress.\n"
    "</system>"
)


def _row(**over):
    row = {
        "command_id": _ID,
        "conversation_thread_id": "11111111-1111-1111-1111-111111111111",
        "user_id": "u-1",
        "workspace_id": "22222222-2222-2222-2222-222222222222",
        "run_id": None,
        "tool_call_id": "toolu_1",
        "tool": "Bash",
        "description": "Run 10-year backtest",
        "log_path": _LOG,
        "status": "completed",
        "failure_reason": None,
        "exit_code": 0,
        "started_at": _T0,
        "ended_at": _T0 + timedelta(minutes=23, seconds=4),
        "result_delivered_at": None,
    }
    row.update(over)
    return row


def _ran(minutes: int, seconds: int = 0) -> dict:
    return {"ended_at": _T0 + timedelta(minutes=minutes, seconds=seconds)}


# --- verdict -----------------------------------------------------------------


def _job(hook_type="command_report_back", **payload):
    return {
        "hook_outbox_id": "00000000-0000-0000-0000-0000000000aa",
        "hook_type": hook_type,
        "conversation_thread_id": "11111111-1111-1111-1111-111111111111",
        "attempts": 1,
        "payload": {"command_id": _ID, **payload},
    }


def _notice(row, *, no_turn=False):
    """``read_notice``'s answer for ``row``: ``owed`` as ``COMMAND_OWED_SQL``
    reads it (the integration suite pins the SQL itself)."""
    if row is None:
        return None
    reports = row["status"] in ("completed", "failed") or (
        row["status"] == "stopped" and row.get("stopped_by") == "user"
    )
    owed = reports and not row.get("result_delivered_at")
    return {**row, "owed": owed, "no_turn": no_turn, "thread_workspace_id": "ws-thread"}


@contextmanager
def _patches(*ps):
    with ExitStack() as stack:
        yield [stack.enter_context(p) for p in ps]


def _verdict_env(row, *, run=None, wakeups=(), no_turn=False, reads=None):
    """The row's one read, its turn's run, and the thread's pending wakeups."""
    return _patches(
        patch(
            "src.server.database.runs.command_runs.read_notice",
            new=reads or AsyncMock(return_value=_notice(row, no_turn=no_turn)),
        ),
        patch("src.server.database.runs.lifecycle.get_run", new=AsyncMock(return_value=run)),
        patch(
            "src.server.database.wakeups.read.list_pending",
            new=AsyncMock(return_value=list(wakeups)),
        ),
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "row, wake",
    [
        (None, False),
        (_row(status="stopped", stopped_by="agent"), True),
        (_row(status="running"), True),
        (_row(result_delivered_at=_T0 + timedelta(hours=1)), True),
    ],
)
async def test_nothing_is_owed_for_a_gone_stopped_or_seen_command(row, wake):
    with _verdict_env(row):
        verdict = await cmd.command_report_verdict(_job())
    assert isinstance(verdict, NotOwed)
    assert verdict.wake is wake


@pytest.mark.asyncio
async def test_nothing_is_owed_on_a_thread_no_turn_will_come_to():
    """Archived, or its workspace deleted: as a wakeup's fire is dropped
    there; an unarchive does not bring the notice back."""
    with _verdict_env(_row(), no_turn=True):
        verdict = await cmd.command_report_verdict(_job())
    assert isinstance(verdict, NotOwed)
    assert verdict.wake is True


@pytest.mark.asyncio
async def test_a_stop_after_the_command_ended_drops_its_notice():
    row = _row(run_id="33333333-3333-3333-3333-333333333333")
    by_user = {"cancelled_by_user": True}
    stopped = {
        "status": "cancelled",
        "cancel_requested_at": row["ended_at"] + timedelta(seconds=5),
        "metadata": by_user,
    }
    with _verdict_env(row, run=stopped):
        assert isinstance(await cmd.command_report_verdict(_job()), NotOwed)

    earlier = {**stopped, "cancel_requested_at": row["ended_at"] - timedelta(minutes=5)}
    with _verdict_env(row, run=earlier):
        assert not isinstance(await cmd.command_report_verdict(_job()), NotOwed)


@pytest.mark.asyncio
async def test_a_shutdown_cancel_after_the_command_ended_still_owes_its_notice():
    row = _row(run_id="33333333-3333-3333-3333-333333333333")
    shut_down = {
        "status": "cancelled",
        "cancel_requested_at": row["ended_at"] + timedelta(seconds=5),
        "metadata": {},
    }
    with _verdict_env(row, run=shut_down):
        assert not isinstance(await cmd.command_report_verdict(_job()), NotOwed)


@pytest.mark.asyncio
async def test_an_owed_notice_names_only_time_wakeups():
    wakeups = [
        {"short_id": "w_1", "label": "Check", "kind": "at"},
        {"short_id": "w_2", "label": "Every hour", "kind": "cron"},
        {"short_id": "w_3", "label": "NVDA dip", "kind": "price"},
        {"short_id": "w_4", "label": "Fill", "kind": "order"},
    ]
    with _verdict_env(_row(), wakeups=wakeups):
        verdict = await cmd.command_report_verdict(_job())
    assert [w["short_id"] for w in verdict.wakeups] == ["w_1", "w_2"]


@pytest.mark.asyncio
async def test_a_failed_wakeup_read_still_delivers_the_notice():
    with _verdict_env(_row()), patch(
        "src.server.database.wakeups.read.list_pending",
        new=AsyncMock(side_effect=RuntimeError("db")),
    ):
        verdict = await cmd.command_report_verdict(_job())
    assert verdict.wakeups == []


# --- the live-turn offer ---------------------------------------------------------


@pytest.mark.asyncio
async def test_a_live_turn_takes_the_notice_with_its_caption():
    from src.server.services.report_back import steer

    find = AsyncMock(return_value=None)
    with _verdict_env(_row()), patch(
        "src.server.database.runs.lifecycle.find_run_by_request_key", new=find
    ):
        offer = await steer._decide(_job(), "t")
    assert offer.content == APPROVED_COMPLETED_STEERED
    assert offer.shown["report_source"]["kind"] == "command_report"
    find.assert_awaited_once()


@pytest.mark.asyncio
async def test_a_live_turn_leaves_a_notice_whose_own_turn_was_admitted():
    from src.server.services.report_back import steer

    with _verdict_env(_row()), patch(
        "src.server.database.runs.lifecycle.find_run_by_request_key",
        new=AsyncMock(return_value={"conversation_response_id": "r"}),
    ):
        assert await steer._decide(_job(), "t") is None


@pytest.mark.asyncio
async def test_the_next_turn_takes_a_held_notice_without_looking_for_its_own_turn():
    from src.server.services.report_back import steer

    row = _row(status="failed", failure_reason="after_turn_limit", exit_code=None, **_ran(112, 30))
    find = AsyncMock()
    with _verdict_env(row), patch(
        "src.server.database.runs.lifecycle.find_run_by_request_key", new=find
    ):
        offer = await steer._decide(_job("command_notice"), "t")
    assert offer.content == APPROVED_AFTER_TURN_LIMIT
    find.assert_not_awaited()


@pytest.mark.asyncio
async def test_a_live_turn_closes_a_notice_nothing_is_owed_for():
    from src.server.services.report_back import steer

    with _verdict_env(_row(result_delivered_at=_T0)), patch(
        "src.server.database.runs.lifecycle.find_run_by_request_key",
        new=AsyncMock(return_value=None),
    ):
        assert isinstance(await steer._decide(_job(), "t"), NotOwed)


# --- the executor ---------------------------------------------------------------


_STAMP = "src.server.database.runs.command_runs.stamp_delivered"


def _idle_thread_env(post, order=None):
    """The edges of a claim on an idle thread, entered in this order (merge
    third); ``order`` records the save of the run id and the delivery stamp."""
    order = order if order is not None else []
    return _patches(
        patch(
            "src.server.database.runs.lifecycle.get_latest_attempt",
            new=AsyncMock(return_value={"status": "completed", "conversation_response_id": "r0"}),
        ),
        patch("src.server.services.report_back.notify_turn.post_notification_turn", new=post),
        patch(
            "src.server.database.runs.outbox.merge_job_payload",
            new=AsyncMock(side_effect=lambda *a: order.append("merge")),
        ),
        patch(_STAMP, new=AsyncMock(side_effect=lambda ids: order.append(f"stamp {ids}") or ids)),
        patch(
            "src.server.services.report_back.notify_turn.await_run_terminal",
            new=AsyncMock(return_value="terminal"),
        ),
        patch("src.server.services.report_back.handoff.wake.publish_wake", new=AsyncMock()),
        patch(
            "src.server.services.report_back.subagent.publish_cleared_wake", new=AsyncMock()
        ),
    )


@pytest.mark.asyncio
async def test_an_idle_thread_gets_a_system_turn_captioned_by_the_command():
    post = AsyncMock(return_value=("dispatched", "run-9"))
    order: list[str] = []
    with _verdict_env(_row()), _idle_thread_env(post, order) as mocks:
        await cmd.execute_command_report_back(_job())

    body = post.await_args.kwargs["body"]
    assert body["query_type"] == "system"
    # The workspace its thread is in now.
    assert body["workspace_id"] == "ws-thread"
    assert body["messages"] == [
        {"role": "user", "content": APPROVED_COMPLETED_STEERED.replace(" while you were working", "")}
    ]
    assert body["request_key"] == cmd.COMMAND_PIPELINE.request_key(_job()["hook_outbox_id"])
    assert body["report_source"]["kind"] == "command_report"
    assert post.await_args.kwargs["user_id"] == "u-1"
    mocks[2].assert_awaited_once_with(_job()["hook_outbox_id"], {"dispatched_run_id": "run-9"})
    # The turn's input carries the notice, so its admission is the delivery;
    # stamped after the run id is saved, so a failed save re-posts into the
    # same turn rather than reading the end as seen.
    assert order == ["merge", f"stamp {[_ID]}"]


@pytest.mark.asyncio
async def test_a_resumed_notice_turn_stamps_the_end_again():
    """The claim that dispatched may have died before its stamp."""
    order: list[str] = []
    post = AsyncMock()
    with _idle_thread_env(post, order):
        await cmd.execute_command_report_back(_job(dispatched_run_id="run-9"))
    post.assert_not_awaited()
    assert order == [f"stamp {[_ID]}"]


@pytest.mark.asyncio
async def test_an_end_seen_while_the_post_waited_opens_no_turn():
    """A turn that ran while the thread was busy showed the end (ThreadStatus);
    the POST asks again before it goes."""
    seen = _row(result_delivered_at=_T0 + timedelta(hours=1))
    reads = AsyncMock(side_effect=[_notice(_row()), _notice(seen)])

    async def post(**kw):
        return ("dispatched", "run-9") if await kw["still_due"]() else ("superseded", None)

    order: list[str] = []
    with _verdict_env(_row(), reads=reads), _idle_thread_env(post, order) as mocks:
        await cmd.execute_command_report_back(_job())
    assert order == []
    mocks[-1].assert_awaited_once()


@pytest.mark.asyncio
async def test_a_steered_notice_stamps_the_end_before_its_job_closes():
    """The confirm of the step that read it; a failed stamp still closes the
    job, which is what keeps the notice from being told twice."""
    from src.server.services.report_back import steer

    for hook_type in ("command_report_back", "command_notice"):
        held = [{**_job(hook_type, steered_run_id="run-A"), "hook_outbox_id": "job-1"}]
        order: list[str] = []
        close = AsyncMock(side_effect=lambda ids, **k: order.append("close") or ids)
        for stamp in (
            AsyncMock(side_effect=lambda ids: order.append(f"stamp {ids}") or ids),
            AsyncMock(side_effect=RuntimeError("db down")),
        ):
            order.clear()
            with patch(
                "src.server.database.runs.outbox.held_notice_jobs",
                new=AsyncMock(return_value=held),
            ), patch(_STAMP, new=stamp), patch(
                "src.server.database.runs.outbox.confirm_jobs", new=close
            ):
                await steer.SteeredReportInbox().confirm("run-A", ["job-1"])
            stamp.assert_awaited_once_with([_ID])
            assert order[-1] == "close"
        assert close.await_count == 2


@pytest.mark.asyncio
async def test_a_busy_thread_parks_the_notice_without_posting():
    post = AsyncMock()
    defer = AsyncMock(return_value="pending")
    with _verdict_env(_row()), patch(
        "src.server.database.runs.lifecycle.get_latest_attempt",
        new=AsyncMock(return_value={"status": "in_progress", "conversation_response_id": "r0"}),
    ), patch("src.server.database.runs.outbox.defer_claimed_job", new=defer), patch(
        "src.server.services.report_back.notify_turn.post_notification_turn", new=post
    ):
        await cmd.execute_command_report_back(_job())
    defer.assert_awaited_once()
    post.assert_not_awaited()


def test_the_request_key_namespace_is_its_own():
    from src.server.services.report_back.subagent import TASK_PIPELINE

    job_id = "00000000-0000-0000-0000-0000000000aa"
    assert cmd.COMMAND_PIPELINE.request_key(job_id) != TASK_PIPELINE.request_key(job_id)
    assert cmd.COMMAND_PIPELINE.request_key(job_id) == cmd.COMMAND_PIPELINE.request_key(job_id)


def test_the_executor_and_its_quota_are_registered():
    from src.server.services import hook_outbox

    cmd.register_outbox_executors()
    assert hook_outbox._EXECUTORS["command_report_back"] is cmd._exec_command_report_back
    assert hook_outbox._INFLIGHT_QUOTAS["command_report_back"] == 3
