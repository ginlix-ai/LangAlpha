"""Fork START vs. orders still waiting under the turns it deletes.

An interrupted turn holding a proposal is not a live run, so a fork over it is
admitted, and the ledger keeps no foreign key to the response rows the
truncation deletes. Two orderings are load-bearing and pinned here:

- the refusal runs AFTER the tip check and both live-run guards, so a refused
  fork refuses no order;
- it runs BEFORE the truncation, inside the START transaction on the fork's own
  session, so its subquery still sees the rows and a rollback undoes it.
"""

from contextlib import asynccontextmanager
from unittest.mock import AsyncMock, patch

import pytest

from src.server.database.runs.lifecycle import (
    ForkSpec,
    RunSlotBusyError,
    StaleForkError,
    TurnLifecycleError,
    start_run,
)
from src.server.database.runs.subagent_runs import TaskRunSlotBusyError

OA_DB = "src.server.database.runs.lifecycle.oa_db"
QR_DB = "src.server.database.runs.lifecycle.qr_db"
SR_DB = "src.server.database.runs.lifecycle.sr_db"
TT_DB = "src.server.database.runs.lifecycle.tt_db"
REPAIR = "src.server.database.runs.lifecycle.subagent_repair"

THREAD_ID = "11111111-1111-1111-1111-111111111111"
RUN_ID = "22222222-2222-2222-2222-222222222222"
LIVE_RUN_ID = "33333333-3333-3333-3333-333333333333"
TASK_RUN_ID = "44444444-4444-4444-4444-444444444444"

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


async def _start(conn):
    return await start_run(
        run_id=RUN_ID,
        thread_id=THREAD_ID,
        request_key="req-1",
        fork=FORK,
        conn=conn,
    )


@pytest.mark.asyncio
async def test_refusal_follows_the_guards_and_precedes_the_truncation(
    mock_connection, mock_cursor
):
    # No duplicate request_key, the tip where the fork read it, no live run.
    mock_cursor.fetchone.side_effect = [{"workspace_id": "ws-1", "status": "running"}, None, None]
    mock_cursor.rowcount = 1
    calls: list[str] = []

    @asynccontextmanager
    async def _transaction(*args, **kwargs):
        calls.append("begin")
        try:
            yield
        finally:
            calls.append("end")

    async def _task_guard(*args, **kwargs):
        calls.append("task_guard")
        return None

    async def _refuse(*args, **kwargs):
        calls.append("refuse")
        return []

    async def _truncate(*args, **kwargs):
        calls.append("truncate")
        return 3

    mock_connection.transaction = _transaction
    refuse = AsyncMock(side_effect=_refuse)

    with (
        patch(f"{SR_DB}.find_open_run_from_turn", new=_task_guard),
        patch(f"{OA_DB}.refuse_attempts_on_fork", new=refuse),
        patch(f"{QR_DB}.truncate_thread_from_turn", new=_truncate),
        patch(f"{REPAIR}.repair_task_chains", new=AsyncMock()),
        # Aborts the START past the truncation, inside the same transaction.
        patch(f"{TT_DB}.drop_rewound", new=AsyncMock(side_effect=TurnLifecycleError)),
    ):
        with pytest.raises(TurnLifecycleError):
            await _start(mock_connection)

    assert calls == ["begin", "task_guard", "refuse", "truncate", "end"]
    refuse.assert_awaited_once_with(THREAD_ID, FORK.from_turn, conn=mock_connection)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("tip_moved", "live_root", "live_task", "refusal"),
    [
        pytest.param(True, None, None, StaleForkError, id="moved-branch-tip"),
        pytest.param(
            False,
            {"conversation_response_id": LIVE_RUN_ID, "status": "in_progress"},
            None,
            RunSlotBusyError,
            id="live-root-run",
        ),
        pytest.param(
            False,
            None,
            {"task_run_id": TASK_RUN_ID, "task_id": "task-a", "status": "in_progress"},
            TaskRunSlotBusyError,
            id="live-task-run",
        ),
    ],
)
async def test_a_refused_fork_refuses_no_order(
    mock_connection, mock_cursor, tip_moved, live_root, live_task, refusal
):
    mock_cursor.fetchone.side_effect = [{"workspace_id": "ws-1", "status": "running"}, None, live_root]
    mock_cursor.rowcount = 0 if tip_moved else 1
    refuse = AsyncMock(return_value=[])
    truncate = AsyncMock(return_value=0)

    with (
        patch(
            f"{SR_DB}.find_open_run_from_turn", new=AsyncMock(return_value=live_task)
        ),
        patch(f"{OA_DB}.refuse_attempts_on_fork", new=refuse),
        patch(f"{QR_DB}.truncate_thread_from_turn", new=truncate),
    ):
        with pytest.raises(refusal):
            await _start(mock_connection)

    # A refused fork changes nothing, so the order still waits for the
    # verdict a later run may yet ask for.
    refuse.assert_not_awaited()
    truncate.assert_not_awaited()
