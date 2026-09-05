"""The credit gate's SQL surface — the guards that keep a live spend figure honest.

Every statement here runs against rows that are also being written by finalize,
by the collector, and by the recovery sweep, so each one carries guards that are
the whole reason it is safe to run concurrently with them. None of those guards
has a runtime assertion behind it: drop one and the query still executes, just
against the wrong rows. What is pinned:

- the heartbeat is monotone, and refuses a row that is terminal or already
  billed — a late beat racing finalize must not resurrect billed spend;
- the degraded sweep waits out a grace interval for task runs and not for
  responses, because only one of the two lanes can prove no settle is coming;
- both sweeps keep the predicate that matches the partial in-flight index,
  since they run every scan interval on every worker;
- the predecessor walk terminates and settles inside the caller's transaction;
- the resume query keeps all four filters that make it mean "stopped for money,
  under the attempt being resumed".

SQL is asserted by substring against the mocked cursor — the house pattern for
this layer; whole-query equality is whitespace-brittle.
"""

from contextlib import asynccontextmanager
from decimal import Decimal
from unittest.mock import AsyncMock, patch

import pytest

from src.server.contracts.status import (
    CREDIT_STOP_ERROR_TYPE,
    INTERRUPT_REASON_CREDIT_PAUSE,
)
from src.server.database.runs import credit_ledger, subagent_runs

THREAD_ID = "11111111-1111-1111-1111-111111111111"
RUN_ID = "22222222-2222-2222-2222-222222222222"
TASK_RUN_ID = "33333333-3333-3333-3333-333333333333"

POOL = "src.server.database.pool.get_db_connection"


def _pool_of(connection):
    @asynccontextmanager
    async def _fake():
        yield connection

    return patch(POOL, new=_fake)


def _sql(cursor):
    return cursor.execute.call_args_list[-1][0][0]


def _params(cursor):
    return cursor.execute.call_args_list[-1][0][1]


def _executable(sql: str) -> str:
    """SQL with its -- comments stripped, for assertions about what actually
    runs. These statements carry comments that quote the very spellings some
    of the assertions below rule out."""
    return "\n".join(line.split("--")[0] for line in sql.splitlines())


# ------------------------------------------------------------------ heartbeat


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "kind,table,key",
    [
        ("run", "conversation_responses", "conversation_response_id"),
        ("task", "subagent_runs", "task_run_id"),
    ],
)
async def test_the_heartbeat_is_monotone_and_guarded(
    mock_connection, mock_cursor, kind, table, key
):
    mock_cursor.rowcount = 1
    with _pool_of(mock_connection):
        assert await credit_ledger.heartbeat(kind, RUN_ID, 12.5) is True

    sql = _sql(mock_cursor)
    assert f"UPDATE {table}" in sql
    assert f"{key} = %s" in sql
    # Monotone: a reordered or duplicate flush must never lower the figure.
    assert "GREATEST(in_flight_credits" in sql
    # ...and a beat that arrives after finalize must not revive billed spend.
    assert "status = 'in_progress'" in sql
    assert "usage_settled_at IS NULL" in sql


@pytest.mark.asyncio
async def test_the_heartbeat_reports_a_row_it_could_not_write(
    mock_connection, mock_cursor
):
    """False is what tells the lane its ledger row is closed, so it can stop."""
    mock_cursor.rowcount = 0
    with _pool_of(mock_connection):
        assert await credit_ledger.heartbeat("run", RUN_ID, 12.5) is False


@pytest.mark.asyncio
async def test_the_heartbeat_binds_exact_decimal_not_float(
    mock_connection, mock_cursor
):
    """GREATEST(numeric, float8) resolves in double precision: a float here
    would round the stored column value through binary float on every beat."""
    mock_cursor.rowcount = 1
    with _pool_of(mock_connection):
        await credit_ledger.heartbeat("run", RUN_ID, 0.07999999999999999)

    bound = _params(mock_cursor)[0]
    assert isinstance(bound, Decimal)
    assert bound == Decimal("0.07999999999999999")


# ------------------------------------------------------------ degraded sweep


@pytest.mark.asyncio
async def test_a_task_sweep_waits_out_the_grace_and_a_response_sweep_does_not(
    mock_connection, mock_cursor
):
    """A terminal response proves no settle is coming; a terminal task run does
    not, because the collector may still be about to claim it."""
    with _pool_of(mock_connection):
        await credit_ledger.settle_abandoned("task")
        task_sql = _sql(mock_cursor)
        await credit_ledger.settle_abandoned("run")
        run_sql = _sql(mock_cursor)

    assert "finalized_at < NOW() - INTERVAL '30 minutes'" in task_sql
    assert "INTERVAL" not in run_sql
    assert "UPDATE subagent_runs" in task_sql
    assert "UPDATE conversation_responses" in run_sql


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["run", "task"])
async def test_both_sweeps_match_the_partial_in_flight_index(
    mock_connection, mock_cursor, kind
):
    """These fire on every worker every scan interval; off the index they are
    sequential scans of the two hottest tables in the schema."""
    with _pool_of(mock_connection):
        await credit_ledger.settle_abandoned(kind)

    sql = _sql(mock_cursor)
    assert "usage_settled_at IS NULL" in sql
    assert "in_flight_credits > 0" in sql
    assert "status != 'in_progress'" in sql
    # The one the planner needs to prove the index covers every matching row.
    # Without it the other three still read as a match and the sweep still
    # scans: verified on Postgres, where it will not pick the index even with
    # enable_seqscan off.
    assert "user_id IS NOT NULL" in sql


# -------------------------------------------------------------- chain settle


@pytest.mark.asyncio
async def test_the_predecessor_walk_terminates_and_uses_the_callers_transaction(
    mock_connection, mock_cursor
):
    """The stamp has to commit with the usage row or not at all, so it must
    never open a connection of its own."""
    mock_cursor.rowcount = 1

    def _explode():
        raise AssertionError("settle_task_run opened its own connection")

    with patch(POOL, new=_explode):
        assert await credit_ledger.settle_task_run(TASK_RUN_ID, conn=mock_connection)

    sql = _sql(mock_cursor)
    assert "WITH RECURSIVE chain" in sql
    assert "predecessor_run_id" in sql
    # UNION, never UNION ALL: dedup is what stops the walk if a predecessor
    # ever points back into the chain, and it runs inside an open write txn.
    assert "UNION" in _executable(sql)
    assert "UNION ALL" not in _executable(sql)
    assert "usage_settled_at IS NULL" in sql


# ------------------------------------------------------ terminal vs settled


@pytest.mark.asyncio
@pytest.mark.parametrize("status", ["error", "cancelled", "completed"])
async def test_finalizing_a_task_run_never_stamps_it_billed(
    mock_connection, mock_cursor, status
):
    """Terminating is not being billed, whatever the terminal is.

    Settling here would drop the run's heartbeated spend out of the in-flight
    aggregate before the collector inserts its usage row, and a turn admitted
    in that window is measured against an understated balance. A credit stop
    settles ``cancelled`` and lands in that window by construction, which is
    exactly when a resume is about to be admitted.
    """
    mock_cursor.fetchone = AsyncMock(
        return_value={"status": status, "task_run_id": TASK_RUN_ID}
    )

    with _pool_of(mock_connection):
        await subagent_runs.finalize_task_run(
            task_run_id=TASK_RUN_ID, status=status
        )

    sql = _executable(_sql(mock_cursor))
    assert "finalized_at = NOW()" in sql
    assert "usage_settled_at" not in sql, (
        "finalize stamped the settle; only the collector's usage transaction "
        "and the abandoned sweep may do that"
    )


# -------------------------------------------------------------- resume query


@pytest.mark.asyncio
async def test_the_resume_query_keeps_every_filter_that_gives_it_meaning(
    mock_connection, mock_cursor
):
    with _pool_of(mock_connection):
        await credit_ledger.list_credit_stopped_for_resume(THREAD_ID, RUN_ID)

    sql = _sql(mock_cursor)
    # Anchored on the thread's latest run, not the newest pause: a resume opens
    # a new run, so "newest pause" would re-announce the same stops forever.
    assert "ORDER BY run_seq DESC" in sql
    assert "LIMIT 1" in sql
    assert "r.status = 'interrupted'" in sql
    assert "r.interrupt_reason = %s" in sql
    assert "s.status = 'cancelled'" in sql
    assert "s.failure->>'error_type' = %s" in sql
    # Redundant against the join, but it is what keeps this an index scan.
    assert "s.thread_id = %s" in sql

    params = _params(mock_cursor)
    assert INTERRUPT_REASON_CREDIT_PAUSE in params
    assert CREDIT_STOP_ERROR_TYPE in params


@pytest.mark.asyncio
async def test_the_resume_query_excludes_the_whole_attempt_chain(
    mock_connection, mock_cursor
):
    """The regression. The resuming run's row is inserted in the START
    transaction, long before this runs, so "the thread's latest run" is that
    row — in_progress, never the pause — and every later predicate misses.
    Excluding it is what makes the announcement land at all.

    The CHAIN rather than the one id, because a resume that fails retryably is
    retried as a NEW row chained by ``retry_of_run_id`` while the pause stays
    two rows back. Excluding only the resuming run would then anchor on the
    failed attempt, whose status is 'error', and the retry that finally
    succeeds would announce no stopped subagents at all.
    """
    with _pool_of(mock_connection):
        await credit_ledger.list_credit_stopped_for_resume(THREAD_ID, RUN_ID)

    sql = _executable(_sql(mock_cursor))
    assert "WITH RECURSIVE attempt_chain" in sql
    assert "retry_of_run_id" in sql
    assert "conversation_response_id NOT IN" in sql
    # Terminates: dedup is what stops the walk if a retry ever points back
    # into its own chain.
    assert "UNION" in sql
    assert "UNION ALL" not in sql
    # The chain is anchored on the resuming run, so it must be bound first.
    assert _params(mock_cursor)[0] == RUN_ID


@pytest.mark.asyncio
async def test_a_task_sweep_leaves_rows_whose_parent_turn_is_still_live(
    mock_connection, mock_cursor
):
    """Age alone cannot prove no settle is coming for a task run.

    The collector is spawned only once the PARENT turn goes terminal, so a
    subagent that finished early under a long turn is stamped billed 30
    minutes in and drops out of the in-flight aggregate for the rest of that
    turn — understating the balance every admission in that window is
    measured against, which is the one thing this ledger exists to prevent.
    """
    with _pool_of(mock_connection):
        await credit_ledger.settle_abandoned("task")

    sql = _executable(_sql(mock_cursor))
    assert "NOT EXISTS" in sql
    assert "conversation_responses r" in sql
    assert "subagent_runs.parent_run_id" in sql
    assert "r.status = 'in_progress'" in sql


@pytest.mark.asyncio
async def test_a_task_sweep_waits_out_the_grace_from_the_parent_going_terminal(
    mock_connection, mock_cursor
):
    """The parent's STATUS is not enough, only the instant it changed.

    A child that finished long before its parent has already served its own
    grace, so a bare "parent is not in_progress" opens the instant the parent
    commits — which is before the collector that commit spawns has inserted
    anything. The sweep would stamp the row settled out from under a collector
    about to bill it. The parent's ``usage_settled_at`` is the terminal instant
    (the finalize CAS stamps it in the same statement) and NULL on a parent
    finalized by pre-gate code, which must read as "chance not started" rather
    than as "chance over".
    """
    with _pool_of(mock_connection):
        await credit_ledger.settle_abandoned("task")

    sql = " ".join(_executable(_sql(mock_cursor)).split())
    assert "r.usage_settled_at IS NULL" in sql
    assert "r.usage_settled_at > NOW() - INTERVAL '30 minutes'" in sql


@pytest.mark.asyncio
async def test_a_response_sweep_has_no_parent_to_wait_on(
    mock_connection, mock_cursor
):
    """The complement: responses ARE the parents, so the guard would be
    nonsense there — and a correlated subquery on the hottest table in the
    schema is not something to add for nothing."""
    with _pool_of(mock_connection):
        await credit_ledger.settle_abandoned("run")

    assert "NOT EXISTS" not in _executable(_sql(mock_cursor))
