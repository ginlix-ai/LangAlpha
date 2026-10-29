"""A run that failed or was stopped before its graph took the input is stamped
at finalize.

The executor's failure path knows nothing of the input, so the finalize reads
the thread's newest step: none of the run's means the input never reached a
checkpoint, and ``/retry`` must send it again. A read that fails decides
nothing, since stamping a run whose graph did take the input would send its
message twice. The recovery scanner decides the same way for a run whose
worker died, from the stamp START kept on the run's row.
"""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.server.database.runs.lifecycle import NewestCheckpoint
from src.server.models.chat import ChatRequest
from src.server.services.runs.coordinator import (
    FinalizeResult,
    RunCoordinator,
    RunHandle,
    RunOutcome,
)
from src.server.services.runs.pending_input import (
    PENDING_INPUT_KEY,
    executor_input_stamp,
    untaken_input_metadata,
)
from src.server.services.runs.recovery import RecoveryScanner

LIFECYCLE = "src.server.database.runs.lifecycle.finalize_run"
STAMP = {PENDING_INPUT_KEY: {"checkpoint_id": "cp-base"}}


async def _finalized_metadata(status: str, newest) -> dict:
    coordinator = RunCoordinator()
    coordinator._schedule_replay_refresh = MagicMock()
    coordinator._nudge_hook_drainer = MagicMock()
    coordinator._newest_checkpoint = (
        AsyncMock(side_effect=newest)
        if isinstance(newest, Exception)
        else AsyncMock(return_value=newest)
    )
    handle = RunHandle(
        run_id="run-1",
        thread_id="thread-1",
        turn_index=3,
        attempt_no=1,
        pending_input=STAMP,
    )
    seen = {}

    async def finalize(**kwargs):
        seen.update(kwargs["outcome"].metadata)
        return FinalizeResult(applied=False, run=None)

    with (
        patch(LIFECYCLE, side_effect=finalize),
        patch("src.server.services.transcripts.schedule_thread_export"),
    ):
        await coordinator.finalize_run(handle, RunOutcome(status=status))
    return seen


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("newest", "stamped"),
    [
        (NewestCheckpoint("cp-old", "run-0"), True),
        (None, True),
        (NewestCheckpoint("cp-new", "run-1"), False),
        (RuntimeError("checkpointer down"), False),
    ],
    ids=["older-run", "no-checkpoint", "own-step", "unread"],
)
async def test_a_failed_run_is_stamped_only_when_no_step_is_its_own(newest, stamped):
    metadata = await _finalized_metadata("error", newest)

    assert (PENDING_INPUT_KEY in metadata) is stamped


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("newest", "stamped"),
    [
        (NewestCheckpoint("cp-old", "run-0"), True),
        (NewestCheckpoint("cp-new", "run-1"), False),
        (RuntimeError("checkpointer down"), False),
    ],
    ids=["older-run", "own-step", "unread"],
)
async def test_a_stopped_run_is_stamped_as_a_failed_one_is(newest, stamped):
    """A stop before the graph took the input leaves it on no branch, so the
    turn's regenerate sends the message again from the stamp."""
    metadata = await _finalized_metadata("cancelled", newest)

    assert (PENDING_INPUT_KEY in metadata) is stamped


@pytest.mark.asyncio
@pytest.mark.parametrize("status", ["completed", "interrupted"])
async def test_a_run_that_ended_on_its_own_terms_is_not_stamped(status):
    metadata = await _finalized_metadata(status, NewestCheckpoint("cp-old", "run-0"))

    assert PENDING_INPUT_KEY not in metadata


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("newest", "stamped"),
    [
        (NewestCheckpoint("cp-old", "run-0"), True),
        (None, True),
        (NewestCheckpoint("cp-new", "run-1"), False),
        (RuntimeError("checkpointer down"), False),
    ],
    ids=["older-run", "no-checkpoint", "own-step", "unread"],
)
async def test_a_dead_workers_run_is_stamped_only_when_no_step_is_its_own(
    newest, stamped
):
    read = (
        AsyncMock(side_effect=newest)
        if isinstance(newest, Exception)
        else AsyncMock(return_value=newest)
    )
    run = {"metadata": {"msg_type": "ptc", **untaken_input_metadata(STAMP)}}
    with (
        patch(
            "src.server.app.setup.checkpointer",
            SimpleNamespace(aget_tuple=AsyncMock(return_value=None)),
        ),
        patch("src.server.utils.checkpoint_helpers.newest_checkpoint", read),
    ):
        status, *_, metadata = await RecoveryScanner()._classify(
            run, "run-1", "thread-1"
        )

    assert status == "error"
    assert (metadata == STAMP) is stamped
    assert metadata in ({}, STAMP)


@pytest.mark.asyncio
async def test_a_dead_workers_stopped_run_is_stamped_from_its_row():
    run = {
        "cancel_requested_at": "2026-10-10T00:00:00Z",
        "metadata": {"msg_type": "ptc", **untaken_input_metadata(STAMP)},
    }
    with (
        patch(
            "src.server.app.setup.checkpointer",
            SimpleNamespace(aget_tuple=AsyncMock(return_value=None)),
        ),
        patch(
            "src.server.utils.checkpoint_helpers.newest_checkpoint",
            AsyncMock(return_value=NewestCheckpoint("cp-old", "run-0")),
        ),
    ):
        status, *_, metadata = await RecoveryScanner()._classify(
            run, "run-1", "thread-1"
        )

    assert (status, metadata) == ("cancelled", STAMP)



@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("newest", "stamped"),
    [
        (NewestCheckpoint("cp-old", "run-0"), True),
        (None, True),
        (NewestCheckpoint("cp-new", "run-1"), False),
        (RuntimeError("checkpointer down"), False),
    ],
    ids=["older-run", "no-checkpoint", "own-step", "unread"],
)
async def test_an_orphaned_dispatch_is_stamped_only_when_no_step_is_its_own(
    newest, stamped
):
    """The dispatch consumer settles its run with no handle, from the row and
    the checkpoint as the scanner does, and hands the finalize the newest
    checkpoint it read so a step of the run's own moves the pointer."""
    read = (
        AsyncMock(side_effect=newest)
        if isinstance(newest, Exception)
        else AsyncMock(return_value=newest)
    )
    run = {"metadata": {"msg_type": "ptc", **untaken_input_metadata(STAMP)}}
    seen = {}

    async def finalize(**kwargs):
        seen.update(metadata=kwargs["outcome"].metadata, newest=kwargs["newest"])
        return FinalizeResult(applied=False, run={"status": "error"})

    with (
        patch(
            "src.server.database.runs.lifecycle.get_run", AsyncMock(return_value=run)
        ),
        patch("src.server.app.setup.checkpointer", SimpleNamespace()),
        patch("src.server.utils.checkpoint_helpers.newest_checkpoint", read),
        patch(LIFECYCLE, side_effect=finalize),
        patch(
            "src.server.services.runs.executor.LocalRunExecutor.get_instance",
            return_value=SimpleNamespace(append_run_end_event=AsyncMock()),
        ),
    ):
        status = await RunCoordinator().reconcile_orphaned_dispatch(
            "thread-1", "run-1"
        )

    assert status == "error"
    assert (PENDING_INPUT_KEY in seen["metadata"]) is stamped
    assert seen["newest"] == (None if isinstance(newest, Exception) else newest)

def test_a_resume_leaves_its_checkpoint_to_be_replayed():
    """Replaying an unanswered pause raises it again for the user to answer."""
    resume = ChatRequest(messages=[], hitl_response={"i-1": {"decisions": []}})
    edit = ChatRequest(
        messages=[{"role": "user", "content": "edited"}], checkpoint_id="cp-base"
    )

    assert executor_input_stamp(resume) == {}
    assert executor_input_stamp(edit) == STAMP
