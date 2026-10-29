"""How the card stamp's read of the run ledger is bound.

It takes ids off stored artifacts, so it is scoped to its thread and drops
malformed ids before they reach a ``uuid`` column.
"""

from __future__ import annotations

import re

import pytest

from src.server.database.runs import subagent_runs

THREAD = "11111111-1111-4111-8111-111111111111"
RUN = "22222222-2222-4222-8222-222222222222"
OTHER_RUN = "33333333-3333-4333-8333-333333333333"


def _sql(mock_cursor) -> str:
    return re.sub(r"\s+", " ", mock_cursor.execute.call_args.args[0])


@pytest.mark.asyncio
async def test_a_malformed_run_id_cannot_fail_the_telemetry_batch(
    mock_db_connection, mock_cursor
):
    await subagent_runs.get_task_run_telemetry(
        THREAD, [RUN, "not-a-uuid", f"urn:uuid:{OTHER_RUN.upper()}", "", "7"]
    )

    sql = _sql(mock_cursor)
    assert "WHERE thread_id = %s AND task_run_id = ANY(%s)" in sql
    assert mock_cursor.execute.call_args.args[1] == (THREAD, [RUN, OTHER_RUN])


@pytest.mark.asyncio
async def test_only_malformed_ids_read_nothing(mock_db_connection, mock_cursor):
    assert await subagent_runs.get_task_run_telemetry(THREAD, ["not-a-uuid"]) == {}
    mock_cursor.execute.assert_not_awaited()
