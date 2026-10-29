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

from src.server.database.runs.subagent_runs import TaskRunSlotBusyError
from src.server.database.runs.lifecycle import ForkSpec, TurnLifecycleError, start_run

QR_DB = "src.server.database.runs.lifecycle.qr_db"
SR_DB = "src.server.database.runs.lifecycle.sr_db"
TT_DB = "src.server.database.runs.lifecycle.tt_db"
REPAIR = "src.server.database.runs.lifecycle.subagent_repair"

THREAD_ID = "11111111-1111-1111-1111-111111111111"
RUN_ID = "22222222-2222-2222-2222-222222222222"
TASK_RUN_ID = "33333333-3333-3333-3333-333333333333"

FORK = ForkSpec(from_turn=2, checkpoint_id="ckpt-1", branch_tip="ckpt-tip")


@pytest.fixture(autouse=True)
def _no_steers_to_keep():
    """The fork's steer lift and the move fence read the cursor; these tests
    script it for the guards alone."""
    with (
        patch(
            "src.server.database.workspace.workspace_is_moving",
            new=AsyncMock(return_value=False),
        ),
        patch(
            "src.server.database.runs.lifecycle.lift_fork_steers",
            new=AsyncMock(return_value=([], None)),
        ),
        patch("src.server.database.runs.lifecycle.land_fork_steers", new=AsyncMock()),
    ):
        yield


def _clean_thread(mock_cursor):
    """No duplicate request_key, a tip still where the fork read it, no live
    root run: the checks the fork path makes before the subagent guard."""
    mock_cursor.fetchone.side_effect = [{"workspace_id": "ws-1", "status": "running"}, None, None]
    mock_cursor.rowcount = 1


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
        patch(f"{REPAIR}.repair_task_chains", new=AsyncMock()),
        patch(f"{TT_DB}.drop_rewound", new=AsyncMock(side_effect=TurnLifecycleError)),
    ):
        # Fails at the transcript drop, well past the guard.
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

    async def _drop(*args, **kwargs):
        calls.append("drop")
        raise TurnLifecycleError  # aborts the START right after the repair

    with (
        patch(f"{SR_DB}.find_open_run_from_turn", new=AsyncMock(return_value=None)),
        patch(f"{QR_DB}.truncate_thread_from_turn", new=_truncate),
        patch(f"{REPAIR}.repair_task_chains", new=_repair),
        patch(f"{TT_DB}.drop_rewound", new=_drop),
    ):
        with pytest.raises(TurnLifecycleError):
            await _start(mock_connection)

    # Repair must not trail the transaction: the cascade and the re-anchor
    # commit together or not at all.
    assert calls == ["truncate", "repair", "drop"]


@pytest.mark.asyncio
async def test_repair_runs_on_the_forks_own_session(mock_connection, mock_cursor):
    _clean_thread(mock_cursor)
    repair = AsyncMock(return_value={"rewound": 0, "deleted": 0})

    with (
        patch(f"{SR_DB}.find_open_run_from_turn", new=AsyncMock(return_value=None)),
        patch(f"{QR_DB}.truncate_thread_from_turn", new=AsyncMock(return_value=1)),
        patch(f"{REPAIR}.repair_task_chains", new=repair),
        patch(f"{TT_DB}.drop_rewound", new=AsyncMock(side_effect=TurnLifecycleError)),
    ):
        with pytest.raises(TurnLifecycleError):
            await _start(mock_connection)

    repair.assert_awaited_once_with(THREAD_ID, conn=mock_connection)
