"""Task-chain repair + the truncation guard (M4-C).

Deleting conversation_responses cascades their subagent_runs away. Two damage
shapes follow: a subagent_tasks row whose latest_run_id no longer resolves
(the FK nulls it), and a task row with no runs left at all. Either one makes a
later resume read an empty pointer and start an unchained run — no
predecessor, no start pin. What is pinned here:

- rewind re-anchors a dangling pointer to the NEWEST surviving run, and only
  ever touches rows that are actually damaged;
- task rows with zero surviving runs are deleted, not left as ghosts;
- the guard finds live runs whose dispatching response sits at or past the
  fork cut, so the truncation can refuse before it cascades;
- the global sweep does the same work thread-agnostically, age-scoped because
  it runs unfenced alongside live START transactions.

SQL is asserted by substring against the mocked cursor — the house pattern for
this layer; whole-query equality is whitespace-brittle.
"""

from contextlib import asynccontextmanager
from unittest.mock import patch

import pytest

from src.server.database.subagent_runs import (
    count_open_runs_for_thread,
    find_open_run_from_turn,
    repair_dangling_task_chains,
    repair_task_chains,
)

THREAD_ID = "11111111-1111-1111-1111-111111111111"
RESPONSE_A = "22222222-2222-2222-2222-222222222222"
RESPONSE_B = "33333333-3333-3333-3333-333333333333"
TASK_RUN_ID = "44444444-4444-4444-4444-444444444444"

POOL = "src.server.database.conversation.get_db_connection"


def _rowcounts(cursor, values):
    """Make successive execute() calls report different rowcounts, so a
    two-statement repair can be read as two distinct counts."""
    seq = iter(values)

    async def _exec(*args, **kwargs):
        cursor.rowcount = next(seq)

    cursor.execute.side_effect = _exec


def _pool_of(connection):
    @asynccontextmanager
    async def _fake():
        yield connection

    return patch(POOL, new=_fake)


def _sql_at(cursor, index):
    return cursor.execute.call_args_list[index][0][0]


def _params_at(cursor, index):
    return cursor.execute.call_args_list[index][0][1]


# ---------------------------------------------------------------- rewind
# --------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_rewind_picks_the_newest_surviving_run(mock_connection, mock_cursor):
    _rowcounts(mock_cursor, [1, 0])

    await repair_task_chains(THREAD_ID, conn=mock_connection)

    sql = _sql_at(mock_cursor, 0)
    assert "UPDATE subagent_tasks" in sql
    # Newest survivor, not the first one found.
    assert "ORDER BY r.started_at DESC" in sql
    assert "LIMIT 1" in sql
    # Scoped to the task's own run chain.
    assert "r.thread_id = t.thread_id" in sql
    assert "r.task_id = t.task_id" in sql
    assert THREAD_ID in _params_at(mock_cursor, 0)


@pytest.mark.asyncio
async def test_rewind_only_touches_damaged_rows(mock_connection, mock_cursor):
    """A healthy pointer must not be rewritten — otherwise the counts lie and
    every repair call churns updated_at across the whole thread."""
    _rowcounts(mock_cursor, [0, 0])

    await repair_task_chains(THREAD_ID, conn=mock_connection)

    sql = _sql_at(mock_cursor, 0)
    # Dangling means "nothing answers to latest_run_id" — which is also true
    # when the FK has nulled it, so both damage shapes match this one clause.
    assert "NOT EXISTS" in sql
    assert "r.task_run_id = t.latest_run_id" in sql
    # ...and there has to be something left to rewind TO.
    assert "EXISTS" in sql


@pytest.mark.asyncio
async def test_noop_on_a_healthy_thread(mock_connection, mock_cursor):
    _rowcounts(mock_cursor, [0, 0])

    result = await repair_task_chains(THREAD_ID, conn=mock_connection)

    assert result == {"rewound": 0, "deleted": 0}


# ------------------------------------------------------------ ghost rows
# --------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_task_rows_with_zero_runs_are_deleted(mock_connection, mock_cursor):
    _rowcounts(mock_cursor, [0, 2])

    result = await repair_task_chains(THREAD_ID, conn=mock_connection)

    sql = _sql_at(mock_cursor, 1)
    assert "DELETE FROM subagent_tasks" in sql
    assert "NOT" in sql and "EXISTS" in sql
    assert THREAD_ID in _params_at(mock_cursor, 1)
    assert result["deleted"] == 2


@pytest.mark.asyncio
async def test_rewind_and_delete_are_counted_separately(
    mock_connection, mock_cursor
):
    _rowcounts(mock_cursor, [3, 1])

    result = await repair_task_chains(THREAD_ID, conn=mock_connection)

    assert result == {"rewound": 3, "deleted": 1}
    assert mock_cursor.execute.await_count == 2


@pytest.mark.asyncio
async def test_repair_runs_on_the_pinned_session(mock_connection, mock_cursor):
    """The truncation calls this inside its own transaction; taking a second
    pool connection would put the repair outside that atomicity."""
    _rowcounts(mock_cursor, [1, 1])

    @asynccontextmanager
    async def _explode():  # pragma: no cover - must never be entered
        raise AssertionError("repair_task_chains took a pool connection")
        yield

    with patch(POOL, new=_explode):
        result = await repair_task_chains(THREAD_ID, conn=mock_connection)

    assert result == {"rewound": 1, "deleted": 1}


# ------------------------------------------------------------ global sweep
# --------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_sweep_heals_dangling_pointers_thread_agnostically(
    mock_connection, mock_cursor
):
    _rowcounts(mock_cursor, [2, 1])

    with _pool_of(mock_connection):
        result = await repair_dangling_task_chains()

    assert result == {"rewound": 2, "deleted": 1}
    rewind_sql = _sql_at(mock_cursor, 0)
    assert "UPDATE subagent_tasks" in rewind_sql
    assert "r.task_run_id = t.latest_run_id" in rewind_sql
    # No thread predicate — this is the retroactive, whole-ledger form.
    assert "t.thread_id = %s" not in rewind_sql


@pytest.mark.asyncio
async def test_sweep_ignores_rows_younger_than_the_age_floor(
    mock_connection, mock_cursor
):
    """The sweep runs unfenced. A task row is legitimately run-less for the
    width of a START transaction, so both statements skip fresh rows."""
    _rowcounts(mock_cursor, [0, 0])

    with _pool_of(mock_connection):
        await repair_dangling_task_chains(min_age_seconds=900)

    for index in (0, 1):
        assert "t.updated_at <" in _sql_at(mock_cursor, index)
        assert "MAKE_INTERVAL" in _sql_at(mock_cursor, index)
        assert 900 in _params_at(mock_cursor, index)


# ------------------------------------------------------------------ guard
# --------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_guard_finds_live_run_under_the_fork_cut(
    mock_connection, mock_cursor
):
    mock_cursor.fetchone.return_value = {
        "task_run_id": TASK_RUN_ID,
        "task_id": "task-a",
        "status": "in_progress",
    }

    row = await find_open_run_from_turn(THREAD_ID, 4, conn=mock_connection)

    assert row["task_run_id"] == TASK_RUN_ID
    sql = _sql_at(mock_cursor, 0)
    assert "status = 'in_progress'" in sql
    # Only runs dispatched by responses the truncation will actually delete.
    assert "turn_index >= %s" in sql
    assert "parent_run_id IN" in sql
    assert _params_at(mock_cursor, 0) == (THREAD_ID, THREAD_ID, 4)


@pytest.mark.asyncio
async def test_guard_passes_when_nothing_is_live(mock_connection, mock_cursor):
    mock_cursor.fetchone.return_value = None

    assert await find_open_run_from_turn(THREAD_ID, 0, conn=mock_connection) is None


@pytest.mark.asyncio
async def test_thread_guard_counts_every_live_run(mock_connection, mock_cursor):
    """Full delete cascades from the thread, so which response dispatched a
    run is irrelevant — including runs whose parent_run_id is already null."""
    mock_cursor.fetchone.return_value = (1,)

    with _pool_of(mock_connection):
        count = await count_open_runs_for_thread(THREAD_ID)

    assert count == 1
    sql = _sql_at(mock_cursor, 0)
    assert "status = 'in_progress'" in sql
    assert "parent_run_id" not in sql
    assert _params_at(mock_cursor, 0) == (THREAD_ID,)
