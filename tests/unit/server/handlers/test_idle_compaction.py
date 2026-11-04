"""Firing a claimed idle compaction (``handlers.idle_compaction``): a claim
past its deadline makes no call, a user who comes back takes the thread from
the summary, and the install lands only on the checkpoint it summarized.
"""

from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest
from fastapi import HTTPException

from src.server.handlers import idle_compaction as handler
from src.server.handlers.idle_compaction import _install, _precheck, _Skip, _unless_returned, fire
from src.server.utils.checkpoint_helpers import INTERRUPT_CHANNEL

ROW = {
    "thread_id": "t-1",
    "kind": "I1",
    "base_checkpoint_id": "cp-1",
    "armed_by_run_id": "run-1",
    "user_id": "u-1",
    "deadline": datetime.now(UTC) + timedelta(days=1),
}


@pytest.mark.asyncio
async def test_a_claim_past_its_deadline_is_dropped_before_anything_is_read(monkeypatch, caplog):
    caplog.set_level("INFO")
    precheck = AsyncMock()
    monkeypatch.setattr(handler, "_precheck", precheck)

    await fire({**ROW, "deadline": datetime.now(UTC) - timedelta(seconds=1)})

    precheck.assert_not_awaited()
    assert "idle=discard reason=past_deadline kind=I1 thread_id=t-1" in caplog.text


@pytest.mark.asyncio
async def test_the_kind_logged_installed_is_the_one_that_ran(monkeypatch, caplog):
    """A deploy since the arm makes a summary keeping the tail a rebuild."""
    caplog.set_level("INFO")
    monkeypatch.setattr(handler, "_precheck", AsyncMock())
    monkeypatch.setattr(handler, "_summarize_and_install", AsyncMock(return_value=SimpleNamespace(preserved=[])))

    await fire({**ROW, "kind": "I2"})

    assert "idle=installed reason=summarized kind=I1 thread_id=t-1" in caplog.text


@pytest.mark.asyncio
async def test_a_summary_past_the_compaction_timeout_is_discarded_as_such(monkeypatch, caplog):
    caplog.set_level("INFO")
    monkeypatch.setattr(handler, "_precheck", AsyncMock())
    monkeypatch.setattr(handler, "_summarize_and_install", AsyncMock(side_effect=TimeoutError()))

    await fire(ROW)

    assert "idle=discard reason=timeout kind=I1 thread_id=t-1" in caplog.text


@pytest.mark.asyncio
async def test_a_run_since_the_arm_drops_the_claim_before_anything_is_built():
    """A run that ended before it wrote anything leaves the pointer where it
    was, and armed nothing of its own."""
    with (
        patch(
            "src.server.database.conversation.threads_read.get_thread_branch_read",
            AsyncMock(return_value=SimpleNamespace(checkpoint_id="cp-1")),
        ),
        patch("src.server.database.runs.lifecycle.get_latest_run_id", AsyncMock(return_value="run-2")),
        patch("src.server.utils.checkpoint_helpers.get_checkpointer") as saver,
    ):
        with pytest.raises(_Skip) as skip:
            await _precheck(ROW)

    assert (skip.value.event, skip.value.reason) == ("skip", "superseded")
    saver.assert_not_called()


@pytest.mark.asyncio
async def test_an_archived_thread_is_left_as_it_is():
    """Nobody reopens it, and its summary would spend the user's plan."""
    with (
        patch(
            "src.server.database.conversation.threads_read.get_thread_branch_read",
            AsyncMock(return_value=SimpleNamespace(checkpoint_id="cp-1")),
        ),
        patch("src.server.database.runs.lifecycle.get_latest_run_id", AsyncMock(return_value="run-1")),
        patch(
            "src.server.database.conversation.get_thread_by_id",
            AsyncMock(return_value={"archived_at": datetime.now(UTC)}),
        ),
        patch("src.server.utils.checkpoint_helpers.get_checkpointer") as saver,
    ):
        with pytest.raises(_Skip) as skip:
            await _precheck(ROW)

    assert skip.value.reason == "archived"
    saver.assert_not_called()


@pytest.mark.asyncio
async def test_a_firing_cancelled_by_a_stopping_worker_is_logged_as_such(monkeypatch, caplog):
    caplog.set_level("INFO")
    monkeypatch.setattr(handler, "_precheck", AsyncMock())
    monkeypatch.setattr(handler, "_summarize_and_install", AsyncMock(side_effect=asyncio.CancelledError()))

    with pytest.raises(asyncio.CancelledError):
        await fire(ROW)

    assert "idle=discard reason=shutdown kind=I1 thread_id=t-1" in caplog.text


@pytest.fixture
def ledger(monkeypatch):
    """The thread's latest run, as the summary polls it."""
    monkeypatch.setattr(handler, "POLL_S", 0.01)
    latest = AsyncMock(return_value="run-1")
    with (
        patch("src.server.database.runs.lifecycle.get_latest_run_id", latest),
        patch("src.config.settings.get_compaction_timeout", return_value=5),
    ):
        yield latest


@pytest.mark.asyncio
async def test_a_run_started_on_the_thread_cancels_the_summary(ledger):
    cancelled = asyncio.Event()

    async def summary():
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            cancelled.set()
            raise

    ledger.side_effect = ["run-1", "run-2"]

    with pytest.raises(_Skip) as skip:
        await _unless_returned(summary, "t-1", "run-1")

    assert (skip.value.event, skip.value.reason) == ("discard", "user_returned")
    assert cancelled.is_set()


@pytest.mark.asyncio
async def test_with_no_run_started_the_summary_runs_to_its_answer(ledger):
    async def summary():
        await asyncio.sleep(0.05)
        return "compaction"

    assert await _unless_returned(summary, "t-1", "run-1") == "compaction"


@pytest.mark.asyncio
async def test_a_run_started_while_the_agent_was_built_skips_the_call(ledger):
    """The call's first step would export the transcript the summary reads,
    labelled with the thread's newest checkpoint, over that run's."""
    ledger.return_value = "run-2"
    summary = AsyncMock()

    with pytest.raises(_Skip) as skip:
        await _unless_returned(summary, "t-1", "run-1")

    assert (skip.value.event, skip.value.reason) == ("skip", "user_returned")
    summary.assert_not_called()


# -- the install ----------------------------------------------------------------


@pytest.fixture
def fence():
    """The install's reads under a fence that grants; ``pointer`` is where
    the thread points by then, ``pending`` what is set to report back."""
    state = SimpleNamespace(pointer="cp-1", latest="run-1", pending=None, refused=None, pending_writes=[])
    state.saver = SimpleNamespace(
        aget_tuple=AsyncMock(side_effect=lambda config: SimpleNamespace(pending_writes=state.pending_writes))
    )

    @asynccontextmanager
    async def hold(thread_id, verb):
        if state.refused is not None:
            raise state.refused
        yield SimpleNamespace(saver=state.saver)

    with (
        patch("src.server.handlers.thread_maintenance.compaction_write", AsyncMock(return_value={"messages": []})),
        patch("src.server.handlers.thread_maintenance._hold_thread_mutation", hold),
        patch(
            "src.server.database.conversation.threads_read.get_thread_branch_read",
            AsyncMock(side_effect=lambda thread_id: SimpleNamespace(checkpoint_id=state.pointer)),
        ),
        patch("src.tools.thread_status.tool.pending_work", AsyncMock(side_effect=lambda *a, **k: state.pending)),
        patch(
            "src.server.database.runs.lifecycle.get_latest_run_id",
            AsyncMock(side_effect=lambda thread_id: state.latest),
        ),
    ):
        yield state


THREAD = SimpleNamespace(meta={}, route=SimpleNamespace(role="analyst"))


def _agent(written: bool = True) -> SimpleNamespace:
    return SimpleNamespace(write=AsyncMock(return_value=written))


@pytest.mark.asyncio
async def test_the_install_writes_through_the_fence_on_the_checkpoint_it_summarized(fence):
    agent = _agent()

    await _install(agent, object(), ROW, THREAD)

    agent.write.assert_awaited_once_with({"messages": []}, "idle_compact", saver=fence.saver)


@pytest.mark.parametrize(
    ("change", "reason"),
    [
        ({"pointer": "cp-2"}, "moved"),
        ({"latest": "run-2"}, "superseded"),
        ({"pending": "wakeup"}, "pending_wakeup"),
        ({"pending_writes": [("task", INTERRUPT_CHANNEL, SimpleNamespace(id="i-1", value={}))]}, "waiting_on_user"),
        ({"refused": HTTPException(status_code=409)}, "fence_refused"),
        ({"refused": HTTPException(status_code=504)}, "write_failed"),
    ],
    ids=["moved", "superseded", "pending", "waiting", "refused", "write_failed"],
)
@pytest.mark.asyncio
async def test_a_thread_that_changed_under_the_summary_keeps_its_context(fence, change, reason):
    for name, value in change.items():
        setattr(fence, name, value)
    agent = _agent()

    with pytest.raises(_Skip) as skip:
        await _install(agent, object(), ROW, THREAD)

    assert (skip.value.event, skip.value.reason) == ("discard", reason)
    agent.write.assert_not_awaited()


@pytest.mark.asyncio
async def test_a_write_whose_pointer_did_not_advance_is_discarded(fence):
    """Left beside the pointer, it would still be read by the next turn."""
    with pytest.raises(_Skip) as skip:
        await _install(_agent(written=False), object(), ROW, THREAD)

    assert skip.value.reason == "moved"
