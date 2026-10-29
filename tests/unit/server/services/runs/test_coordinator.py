"""The transcript export rides the post-finalize tail.

A thread's transcript renders after every won finalize, whatever the
terminal status and whoever finalized: the CAS stamps the checkpoint it reads,
and another thread may read this one's transcript before the next turn ends.
Exporting only on a completed turn left a stopped, failed or recovered turn's
steps out of it until some later trigger.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, call, patch

import pytest

from src.server.database.runs.lifecycle import NewestCheckpoint
from src.server.services.runs.coordinator import (
    FinalizeResult,
    RunCoordinator,
    RunHandle,
    RunOutcome,
)

LIFECYCLE = "src.server.database.runs.lifecycle.finalize_run"
NEWEST = NewestCheckpoint("cp-1", "run-1")
EXPORT = "src.server.services.transcripts.schedule_thread_export"


def _row(status: str) -> dict:
    return {
        "conversation_response_id": "run-1",
        "conversation_thread_id": "thread-1",
        "status": status,
        "metadata": {"msg_type": "ptc", "workspace_id": "ws-1"},
    }


def _coordinator() -> RunCoordinator:
    coordinator = RunCoordinator()
    coordinator._schedule_replay_refresh = MagicMock()
    coordinator._nudge_hook_drainer = MagicMock()
    coordinator._newest_checkpoint = AsyncMock(return_value=NEWEST)
    return coordinator


def _handle() -> RunHandle:
    return RunHandle(
        run_id="run-1", thread_id="thread-1", turn_index=0, attempt_no=1,
    )


async def _owner_finalize(status: str, result: FinalizeResult):
    order = []

    async def finalize(**kwargs):
        order.append(("finalize", kwargs["newest"]))
        return result

    with patch(LIFECYCLE, side_effect=finalize), \
         patch(EXPORT, side_effect=lambda *a: order.append(("export", *a))) as export:
        await _coordinator().finalize_run(_handle(), RunOutcome(status=status))
    return export, order


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "status", ["completed", "interrupted", "error", "cancelled"]
)
async def test_an_owner_finalize_exports_at_every_terminal_status(status):
    export, order = await _owner_finalize(
        status, FinalizeResult(applied=True, run=_row(status))
    )

    export.assert_called_once_with("thread-1", "ws-1")
    assert order == [("finalize", NEWEST), ("export", "thread-1", "ws-1")]


@pytest.mark.asyncio
async def test_a_lost_cas_does_not_export():
    export, _ = await _owner_finalize(
        "completed", FinalizeResult(applied=False, run=_row("cancelled"))
    )

    export.assert_not_called()


@pytest.mark.asyncio
async def test_a_detached_finalize_exports():
    executor = MagicMock()
    executor.append_run_end_event = AsyncMock()
    with patch(
        LIFECYCLE,
        new_callable=AsyncMock,
        return_value=FinalizeResult(applied=True, run=_row("error")),
    ), patch(EXPORT) as export, patch(
        "src.server.services.runs.executor.LocalRunExecutor"
    ) as executor_cls:
        executor_cls.get_instance.return_value = executor
        await _coordinator().finalize_detached_run(
            "thread-1", "run-1", RunOutcome(status="error")
        )

    export.assert_called_once_with("thread-1", "ws-1")


def test_a_run_end_asks_for_its_threads_archive_prune():
    """An archived thread keeps its scratchpad while a run writes to it, and
    nothing else prunes it once that run ends."""
    with patch(EXPORT), patch(
        "src.server.services.workspace_manager.prune_if_archived_soon"
    ) as prune:
        _coordinator().post_finalize_tail("thread-1", _row("completed"))

    assert prune.call_args_list == [call("thread-1")]


@pytest.mark.asyncio
async def test_a_turns_usage_row_names_the_full_agent():
    """The turn's usage row lands as 'ptc' with no producer on the outcome."""
    handle = RunHandle(
        run_id="run-1", thread_id="thread-1", turn_index=0, attempt_no=1,
        workspace_id="ws-1", user_id="user-1",
    )
    outcome = RunOutcome(status="completed", metadata={"is_byok": False})
    write = _coordinator()._build_usage_writer(handle, outcome)

    with patch(
        "src.server.database.conversation.create_usage_record", new=AsyncMock()
    ) as insert:
        await write(MagicMock(), "completed")

    assert insert.await_args.args[0]["msg_type"] == "ptc"


@pytest.mark.asyncio
async def test_an_export_that_fails_to_schedule_only_logs():
    coordinator = _coordinator()
    with patch(EXPORT, side_effect=RuntimeError("no loop")):
        coordinator.post_finalize_tail("thread-1", _row("completed"))

    coordinator._schedule_replay_refresh.assert_called_once_with("thread-1")
    coordinator._nudge_hook_drainer.assert_called_once()


CONFIRM = "src.server.services.report_back.steer.confirm_checkpointed_holds"


@pytest.mark.asyncio
async def test_an_owner_finalize_confirms_checkpointed_holds_before_its_cas():
    """A stop between the step that took a steer in and the model call that
    confirms it leaves the steer in the checkpoint; the CAS's settle would
    take it as unread."""
    order = []

    async def finalize(**kwargs):
        order.append("finalize")
        return FinalizeResult(applied=False, run=_row("cancelled"))

    async def confirm(thread_id, run_id):
        order.append(("confirm", thread_id, run_id))
        return []

    with patch(LIFECYCLE, side_effect=finalize), patch(CONFIRM, side_effect=confirm):
        await _coordinator().finalize_run(_handle(), RunOutcome(status="cancelled"))

    assert order == [("confirm", "thread-1", "run-1"), "finalize"]


@pytest.mark.asyncio
async def test_a_detached_finalize_confirms_checkpointed_holds_before_its_cas():
    order = []

    async def finalize(**kwargs):
        order.append("finalize")
        return FinalizeResult(applied=False, run=None)

    async def confirm(thread_id, run_id):
        order.append(("confirm", thread_id, run_id))
        return []

    with patch(LIFECYCLE, side_effect=finalize), patch(CONFIRM, side_effect=confirm):
        await _coordinator().finalize_detached_run(
            "thread-1", "run-1", RunOutcome(status="error")
        )

    assert order == [("confirm", "thread-1", "run-1"), "finalize"]
