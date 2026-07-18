"""Fork truncation vs. live background subagent runs (M4-C).

A fork deletes conversation_responses at or past the cut, and subagent_runs
cascades from those rows. The pre-existing gate only sees live ROOT runs, but
a background subagent outlives the turn that dispatched it — so a fork can
arrive with no live response row and still be about to erase a running task's
ledger row out from under its executor.

Two orderings are load-bearing and pinned here:

- the guard runs BEFORE the truncation, so a refusal costs nothing;
- the repair runs AFTER it, inside the same transaction, so no window exists
  in which a resume can read the emptied latest_run_id.
"""

from unittest.mock import AsyncMock, patch

import pytest

from src.server.database.subagent_runs import TaskRunSlotBusyError
from src.server.database.turn_lifecycle import ForkSpec, TurnLifecycleError, start_run

QR_DB = "src.server.database.turn_lifecycle.qr_db"
SR_DB = "src.server.database.turn_lifecycle.sr_db"

THREAD_ID = "11111111-1111-1111-1111-111111111111"
RUN_ID = "22222222-2222-2222-2222-222222222222"
TASK_RUN_ID = "33333333-3333-3333-3333-333333333333"

FORK = ForkSpec(from_turn=2, checkpoint_id="ckpt-1")


def _clean_thread(mock_cursor):
    """No duplicate request_key, no live root run — the two probes the fork
    path makes before it reaches the subagent guard."""
    mock_cursor.fetchone.side_effect = [None, None]


async def _start(conn):
    return await start_run(
        run_id=RUN_ID,
        thread_id=THREAD_ID,
        request_key="req-1",
        fork=FORK,
        conn=conn,
    )


@pytest.mark.asyncio
async def test_live_task_run_refuses_the_fork_before_truncating(
    mock_connection, mock_cursor
):
    _clean_thread(mock_cursor)
    live = {"task_run_id": TASK_RUN_ID, "task_id": "task-a", "status": "in_progress"}
    truncate = AsyncMock(return_value=0)

    with (
        patch(f"{SR_DB}.find_open_run_from_turn", new=AsyncMock(return_value=live)),
        patch(f"{QR_DB}.truncate_thread_from_turn", new=truncate),
    ):
        with pytest.raises(TaskRunSlotBusyError) as exc:
            await _start(mock_connection)

    # Nothing was deleted — the refusal is free, and the caller can retry
    # once the background task settles.
    truncate.assert_not_awaited()
    assert exc.value.task_id == "task-a"
    assert TASK_RUN_ID in str(exc.value)


@pytest.mark.asyncio
async def test_guard_is_scoped_to_the_truncated_turns(mock_connection, mock_cursor):
    """A background task under a turn the fork KEEPS is untouched by the
    cascade, so it must not block the fork."""
    _clean_thread(mock_cursor)
    probe = AsyncMock(return_value=None)

    with (
        patch(f"{SR_DB}.find_open_run_from_turn", new=probe),
        patch(f"{QR_DB}.truncate_thread_from_turn", new=AsyncMock(return_value=3)),
        patch(f"{SR_DB}.repair_task_chains", new=AsyncMock()),
        patch(f"{QR_DB}.update_thread_checkpoint_id", new=AsyncMock(return_value=False)),
    ):
        # Fails at the checkpoint pin, well past the guard.
        with pytest.raises(TurnLifecycleError):
            await _start(mock_connection)

    probe.assert_awaited_once_with(THREAD_ID, FORK.from_turn, conn=mock_connection)


@pytest.mark.asyncio
async def test_repair_follows_the_truncation_in_the_same_transaction(
    mock_connection, mock_cursor
):
    _clean_thread(mock_cursor)
    calls: list[str] = []

    async def _truncate(*args, **kwargs):
        calls.append("truncate")
        return 3

    async def _repair(*args, **kwargs):
        calls.append("repair")
        return {"rewound": 1, "deleted": 1}

    async def _pin(*args, **kwargs):
        calls.append("pin")
        return False  # aborts the START right after the repair

    with (
        patch(f"{SR_DB}.find_open_run_from_turn", new=AsyncMock(return_value=None)),
        patch(f"{QR_DB}.truncate_thread_from_turn", new=_truncate),
        patch(f"{SR_DB}.repair_task_chains", new=_repair),
        patch(f"{QR_DB}.update_thread_checkpoint_id", new=_pin),
    ):
        with pytest.raises(TurnLifecycleError):
            await _start(mock_connection)

    # Repair must not trail the transaction: the cascade and the re-anchor
    # commit together or not at all.
    assert calls == ["truncate", "repair", "pin"]


@pytest.mark.asyncio
async def test_repair_runs_on_the_forks_own_session(mock_connection, mock_cursor):
    _clean_thread(mock_cursor)
    repair = AsyncMock(return_value={"rewound": 0, "deleted": 0})

    with (
        patch(f"{SR_DB}.find_open_run_from_turn", new=AsyncMock(return_value=None)),
        patch(f"{QR_DB}.truncate_thread_from_turn", new=AsyncMock(return_value=1)),
        patch(f"{SR_DB}.repair_task_chains", new=repair),
        patch(f"{QR_DB}.update_thread_checkpoint_id", new=AsyncMock(return_value=False)),
    ):
        with pytest.raises(TurnLifecycleError):
            await _start(mock_connection)

    repair.assert_awaited_once_with(THREAD_ID, conn=mock_connection)
