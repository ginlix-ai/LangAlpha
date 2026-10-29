"""The outbox statements behind a live turn taking report-backs in: one
steerable predicate, a hold fenced to its run's liveness, held exactly when
stamped, and the confirm and unowed closes kept apart."""

from __future__ import annotations

import re

import pytest

from src.server.database.runs import outbox

_RUN = "00000000-0000-0000-0000-00000000000a"
_JOB = "00000000-0000-0000-0000-00000000000b"


def _sql(mock_cursor) -> str:
    return re.sub(r"\s+", " ", mock_cursor.execute.call_args.args[0])


def _params(mock_cursor) -> tuple:
    return mock_cursor.execute.call_args.args[1]


@pytest.mark.asyncio
async def test_the_hold_takes_effect_only_while_its_run_is_in_progress(
    mock_db_connection, mock_cursor
):
    """Its run's finalize commits the terminal UPDATE and the release of
    parked reports in one transaction. Locking the run row FOR SHARE in the
    hold's own statement orders the two: a hold that commits first is
    released by that finalize, and one that waits behind it re-reads a
    terminal row and takes nothing."""
    mock_cursor.fetchall.return_value = []

    assert await outbox.hold_steered_report_jobs([_JOB], run_id=_RUN) == []

    sql = _sql(mock_cursor)
    assert (
        "WITH owner AS ( SELECT 1 FROM conversation_responses "
        "WHERE conversation_response_id = %s::uuid AND status = 'in_progress' "
        "FOR SHARE )"
    ) in sql
    assert "AND EXISTS (SELECT 1 FROM owner)" in sql
    assert _params(mock_cursor) == (_RUN, _RUN, [_JOB])


@pytest.mark.asyncio
async def test_hold_and_list_share_one_steerable_predicate(
    mock_db_connection, mock_cursor
):
    predicate = re.sub(r"\s+", " ", outbox._STEERABLE_REPORT_SQL)

    await outbox.list_steerable_report_jobs("t-1")
    assert predicate in _sql(mock_cursor)
    await outbox.hold_steered_report_jobs([_JOB], run_id=_RUN)
    assert predicate in _sql(mock_cursor)


@pytest.mark.asyncio
async def test_an_empty_hold_or_close_touches_nothing(mock_db_connection, mock_cursor):
    assert await outbox.hold_steered_report_jobs([], run_id=_RUN) == []
    assert await outbox.confirm_steered_report_jobs([], run_id=_RUN) == []
    assert await outbox.close_unowed_report_jobs([]) == []
    assert await outbox.release_steered_report_jobs([], run_id=_RUN) == []
    mock_cursor.execute.assert_not_awaited()


@pytest.mark.asyncio
async def test_a_release_drops_the_hold_with_the_park(mock_db_connection, mock_cursor):
    """Held exactly when stamped: a released report is takeable by the next
    live turn at once, not after its executor's claim and defer."""
    await outbox.release_deferred_notifications(mock_db_connection, "t-1")

    sql = _sql(mock_cursor)
    assert (
        "SET next_retry_at = CASE WHEN o.hook_type IN ('command_notice')"
        " THEN o.next_retry_at ELSE NOW() END,"
        " payload = o.payload - 'steered_run_id'"
    ) in sql
    assert "o.next_retry_at = 'infinity'::timestamptz" in sql


@pytest.mark.asyncio
async def test_a_release_never_makes_a_next_turn_command_notice_due(
    mock_db_connection, mock_cursor
):
    """A command's notice held for the next turn has no executor that may post
    it: a turn's ending drops only a hold it left on one, and touches no
    other."""
    await outbox.release_deferred_notifications(mock_db_connection, "t-1")

    sql = _sql(mock_cursor)
    assert "'command_report_back'" in sql and "'command_notice'" in sql
    assert (
        "AND (o.hook_type NOT IN ('command_notice') OR o.payload ? 'steered_run_id')"
    ) in sql


@pytest.mark.asyncio
async def test_a_live_turn_lists_command_notices_on_both_their_keys(
    mock_db_connection, mock_cursor
):
    """A command's notice due now sits on the thread's chain; one held for the
    next turn on a key of its own, which no claim ever makes a head of."""
    await outbox.list_steerable_report_jobs("t-1")

    sql = _sql(mock_cursor)
    assert "hook_type IN ('task_report_back', 'report_back', 'command_report_back')" in sql
    assert "'command_notice'" in sql
    thread, keys, _ = _params(mock_cursor)
    assert thread == "t-1" and "command_notice:t-1" in keys


@pytest.mark.asyncio
async def test_a_turn_gives_back_only_what_it_holds_and_leaves_it_parked(
    mock_db_connection, mock_cursor
):
    """No teardown and no claim: the run's finalize releases it with the
    thread's other parked reports, to an executor that drops it as superseded."""
    mock_cursor.fetchall.return_value = [(_JOB,)]

    assert await outbox.release_steered_report_jobs([_JOB], run_id=_RUN) == [_JOB]

    sql = _sql(mock_cursor)
    assert "SET payload = payload - 'steered_run_id'" in sql
    assert "status = 'pending' AND payload->>'steered_run_id' = %s::text" in sql
    assert "next_retry_at" not in sql and "watch_clear" not in sql
    assert _params(mock_cursor) == ([_JOB], _RUN)


@pytest.mark.asyncio
async def test_confirm_closes_only_what_its_own_run_holds(
    mock_db_connection, mock_cursor
):
    mock_cursor.fetchall.return_value = [(_JOB,)]

    assert await outbox.confirm_steered_report_jobs([_JOB], run_id=_RUN) == [_JOB]

    sql = _sql(mock_cursor)
    assert "status = 'pending' AND payload->>'steered_run_id' = %s::text" in sql
    assert "'watch_clear'" in sql and ":steered_clear" in sql
    assert "'error_wake', false" in sql
    # A held row is parked: no executor can have claimed it or its pointer.
    assert "refuse_if_pointer" not in sql
    assert _params(mock_cursor) == ([_JOB], _RUN)


@pytest.mark.asyncio
async def test_an_unowed_close_takes_only_an_unheld_row_and_stamps_nothing(
    mock_db_connection, mock_cursor
):
    await outbox.close_unowed_report_jobs([_JOB])

    sql = _sql(mock_cursor)
    assert re.sub(r"\s+", " ", outbox._STEERABLE_REPORT_SQL) in sql
    assert "jsonb_build_object('steered_run_id'" not in sql
    assert "'error_wake', false" in sql and ":steered_clear" in sql
    # An unheld row may have been claimed before, its run pointer with it.
    assert "'refuse_if_pointer', true" in sql
    assert _params(mock_cursor) == ([_JOB],)


def test_the_dead_compensation_is_the_shared_clear_with_an_error_wake():
    sql = re.sub(r"\s+", " ", outbox._DEAD_REPORT_BACK_COMPENSATION_SQL)
    assert "FROM dead_rows" in sql
    assert "'error_wake', true" in sql
    assert "'refuse_if_pointer', true" in sql
    assert "hook_outbox_id || ':dead_clear', created_at" in sql
