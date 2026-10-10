"""The transcript export rides the post-finalize tail.

A PTC thread's transcript renders after every won finalize, whatever the
terminal status and whoever finalized: the CAS stamps the checkpoint it reads,
and another thread may read this one's transcript before the next turn ends.
Exporting only on a completed turn left a stopped, failed or recovered turn's
steps out of it until some later trigger.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, call, patch

import pytest

from src.server.services.runs.coordinator import (
    FinalizeResult,
    RunCoordinator,
    RunHandle,
    RunOutcome,
)

LIFECYCLE = "src.server.database.runs.lifecycle.finalize_run"
EXPORT = "src.server.services.transcripts.schedule_thread_export"


def _row(status: str, msg_type: str = "ptc") -> dict:
    return {
        "conversation_response_id": "run-1",
        "conversation_thread_id": "thread-1",
        "status": status,
        "metadata": {"msg_type": msg_type, "workspace_id": "ws-1"},
    }


def _coordinator() -> RunCoordinator:
    coordinator = RunCoordinator()
    coordinator._schedule_replay_refresh = MagicMock()
    coordinator._nudge_hook_drainer = MagicMock()
    coordinator._latest_checkpoint_id = AsyncMock(return_value="cp-1")
    return coordinator


def _handle() -> RunHandle:
    return RunHandle(
        run_id="run-1", thread_id="thread-1", turn_index=0, attempt_no=1,
        msg_type="ptc",
    )


async def _owner_finalize(status: str, result: FinalizeResult):
    order = []

    async def finalize(**kwargs):
        order.append(("finalize", kwargs["checkpoint_id"]))
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
    assert order == [("finalize", "cp-1"), ("export", "thread-1", "ws-1")]


@pytest.mark.asyncio
async def test_a_lost_cas_does_not_export():
    export, _ = await _owner_finalize(
        "completed", FinalizeResult(applied=False, run=_row("cancelled"))
    )

    export.assert_not_called()


@pytest.mark.asyncio
async def test_a_flash_run_does_not_export():
    export, _ = await _owner_finalize(
        "cancelled", FinalizeResult(applied=True, run=_row("cancelled", "flash"))
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
            "thread-1", "run-1", RunOutcome(status="error"), checkpoint_id="cp-2"
        )

    export.assert_called_once_with("thread-1", "ws-1")


@pytest.mark.parametrize(("msg_type", "asked"), [("ptc", True), ("flash", False)])
def test_a_ptc_run_end_asks_for_its_threads_archive_prune(msg_type, asked):
    """An archived thread keeps its scratchpad while a run writes to it, and
    nothing else prunes it once that run ends."""
    with patch(EXPORT), patch(
        "src.server.services.workspace_manager.prune_if_archived_soon"
    ) as prune:
        _coordinator().post_finalize_tail("thread-1", _row("completed", msg_type))

    assert prune.call_args_list == ([call("thread-1")] if asked else [])


@pytest.mark.asyncio
async def test_an_export_that_fails_to_schedule_only_logs():
    coordinator = _coordinator()
    with patch(EXPORT, side_effect=RuntimeError("no loop")):
        coordinator.post_finalize_tail("thread-1", _row("completed"))

    coordinator._schedule_replay_refresh.assert_called_once_with("thread-1")
    coordinator._nudge_hook_drainer.assert_called_once()
