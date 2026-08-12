"""Pins "latest attempt" to the one monotonic ordering: run_seq.

turn_index is reused by retries and LOWERED by branch rewinds, so a
turn_index-ordered "latest" diverges from the run_seq one exactly there;
created_at ties under clock skew. Three queries define "latest" and they must
agree — the post-#339 stakes are the stop path: ``_stop_already_covered``
compares a parked report-back against ``get_latest_attempt``'s row, so a
divergent definition would either drop a result the stop was never about or
open a billable synthetic turn after the user asked for none.
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
