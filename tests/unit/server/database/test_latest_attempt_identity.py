"""Pins "latest attempt" to the one monotonic ordering: run_seq.

turn_index is reused by retries and LOWERED by branch rewinds, so a
turn_index-ordered "latest" diverges from the run_seq one exactly there;
created_at ties under clock skew. Three queries define "latest" and they must
agree. Among the stakes is the report-back park: an executor parks a report
while ``get_latest_attempt``'s row reads live or interrupted, so a divergent
definition would either post into a live turn or park a report behind a turn
that already ended.
"""

import uuid

import pytest

from src.server.database.runs import lifecycle as tl_db


def _executed_sql(mock_cursor) -> str:
    return mock_cursor.execute.call_args.args[0]


@pytest.mark.asyncio
async def test_get_latest_attempt_orders_by_run_seq_alone(
    mock_db_connection, mock_cursor
):
    await tl_db.get_latest_attempt("t-1")

    sql = _executed_sql(mock_cursor)
    assert "ORDER BY run_seq DESC" in sql
    assert "LIMIT 1" in sql
    assert "turn_index" not in sql
    assert "created_at" not in sql


@pytest.mark.asyncio
async def test_a_fork_predecessor_is_the_latest_attempt_of_an_earlier_turn(
    mock_db_connection, mock_cursor
):
    await tl_db.get_latest_attempt("t-1", before_turn=4)

    sql = _executed_sql(mock_cursor)
    params = mock_cursor.execute.call_args.args[1]
    assert "turn_index < %s" in sql
    assert "ORDER BY run_seq DESC" in sql
    assert params == ("t-1", 4)


@pytest.mark.asyncio
async def test_batch_variant_picks_per_thread_row_by_run_seq(
    mock_db_connection, mock_cursor
):
    await tl_db.get_latest_attempts_for_threads([str(uuid.uuid4())], "u-1")

    sql = _executed_sql(mock_cursor)
    # DISTINCT ON keeps the first row per thread, so the trailing sort key IS
    # the per-thread pick — it must be run_seq, same as the single-row query.
    assert "DISTINCT ON (cr.conversation_thread_id)" in sql
    assert "ORDER BY cr.conversation_thread_id, cr.run_seq DESC" in sql
    assert "turn_index" not in sql


@pytest.mark.asyncio
async def test_projection_healer_uses_the_same_latest_definition(
    mock_db_connection, mock_cursor
):
    await tl_db.heal_stale_thread_projections()

    sql = _executed_sql(mock_cursor)
    assert "ORDER BY cr.run_seq DESC" in sql
    assert "turn_index" not in sql


# A report names the runs it continued by walking back over the thread's
# attempts in the same order, so an attempt before the reported run is the
# one before it by run_seq.
_THREAD = str(uuid.uuid4())
_LEGS = [str(uuid.uuid4()) for _ in range(4)]  # oldest first; the last reports


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("statuses", "chain"),
    [
        (["completed", "interrupted", "interrupted", "completed"], _LEGS[1:]),
        (["interrupted", "completed", "interrupted", "completed"], _LEGS[2:]),
        (None, []),
    ],
    ids=["interrupted-chain", "broken-by-a-completed-run", "missing-run"],
)
async def test_a_reported_run_names_the_interrupted_runs_it_continued(
    mock_db_connection, mock_cursor, statuses, chain
):
    """Oldest first, back to the first attempt that did not follow an
    interrupt. A run that is not on the thread reads no rows."""
    mock_cursor.fetchall.return_value = (
        []
        if statuses is None
        else [
            {"conversation_response_id": uuid.UUID(run), "status": status}
            for run, status in zip(reversed(_LEGS), reversed(statuses))
        ]
    )

    got = await tl_db.get_continued_runs(_THREAD, _LEGS[-1].upper())

    assert got == chain
    sql = _executed_sql(mock_cursor)
    assert "ORDER BY run_seq DESC" in sql
    assert "LIMIT %s" in sql
    assert "turn_index" not in sql
    assert mock_cursor.execute.call_args.args[1][:3] == (_THREAD, _LEGS[-1], _THREAD)
