"""The outbox statements behind an alert (a market target's or a wakeup's
notice) held for its thread's next turn: every way out of a claim at the ceiling holds it rather than
dead-lettering it, a held one blocks no later wake, and the sweeps that
revive and close it are single guarded statements every worker may run.

``tests/integration/test_market_alert_hold_db.py`` runs them against real
Postgres; these pin the shape a refactor could quietly lose."""

from __future__ import annotations

import re

import pytest

from src.server.database.runs import outbox

_JOB = "00000000-0000-0000-0000-00000000000b"


def _flat(sql: str) -> str:
    return re.sub(r"\s+", " ", sql).strip()


def _statements(mock_cursor) -> list[str]:
    return [_flat(c.args[0]) for c in mock_cursor.execute.call_args_list]


_HELD = {"hook_outbox_id": _JOB, "hook_type": "market_target", "status": "pending"}


@pytest.mark.asyncio
@pytest.mark.parametrize("call", ["nack", "defer"])
async def test_a_claim_out_of_attempts_is_held_not_dead(
    mock_db_connection, mock_cursor, call
):
    # The steer settle finds nothing; the alert hold takes the row.
    mock_cursor.fetchall.side_effect = [[], [_HELD]]
    fn = outbox.nack_outbox_job if call == "nack" else outbox.defer_claimed_job

    assert await fn(_JOB, attempts=5, max_attempts=5) == "exhausted"

    statements = _statements(mock_cursor)
    hold = statements[-1]
    assert hold.startswith("WITH held AS ( UPDATE hook_outbox o SET status = 'pending'")
    assert "next_retry_at = 'infinity'::timestamptz" in hold
    assert "jsonb_build_object('exhausted', true)" in hold
    assert "o.hook_type IN ('market_target', 'wakeup', 'command_notice')" in hold
    # A wakeup's riders are held with their head, keeping the head they rode.
    assert "'exhausted', true, 'rode', r.payload->>'rides')" in hold
    assert _flat(outbox._AT_CEILING_SQL) in hold
    # No dead transition ran after the hold.
    assert not any("'dead'" in s for s in statements)


@pytest.mark.asyncio
async def test_the_park_sweep_holds_alerts_and_dead_letters_the_rest(
    mock_db_connection, mock_cursor
):
    mock_cursor.fetchall.side_effect = [[], [_HELD], []]

    assert await outbox.park_exhausted_jobs(max_attempts=5) == [_HELD]

    hold, dead = _statements(mock_cursor)[-2:]
    assert "jsonb_build_object('exhausted', true)" in hold
    assert "o.lease_expires_at <= NOW()" in hold
    assert "hook_type NOT IN ('steer', 'market_target', 'wakeup', 'command_notice')" in dead


@pytest.mark.asyncio
async def test_the_executors_hold_is_fenced_to_its_lease(
    mock_db_connection, mock_cursor
):
    mock_cursor.fetchall.return_value = []

    assert await outbox.exhaust_alert(_JOB, attempts=2) is False

    sql = _statements(mock_cursor)[-1]
    assert (
        "o.hook_outbox_id = %(job_id)s::uuid AND o.status = 'claimed'"
        " AND o.attempts = %(attempts)s"
    ) in sql
    assert mock_cursor.execute.call_args.args[1] == {"job_id": _JOB, "attempts": 2}


def test_a_waiting_alert_blocks_no_later_job_in_either_claim_phase():
    import inspect

    source = _flat(inspect.getsource(outbox.claim_outbox_jobs))
    blocker = '(p.status = \'pending\' AND NOT {waiting_alert_sql("p")})'
    assert source.count(blocker) == 2


@pytest.mark.asyncio
async def test_the_revive_sweep_makes_an_owed_dead_alert_due_a_bounded_number_of_times(
    mock_db_connection, mock_cursor
):
    """Due, so this build's executor tries the wake an older build never
    did; not held, which would wait on a turn an idle thread never gets."""
    await outbox.revive_dead_alerts()

    sql = _statements(mock_cursor)[-1]
    head, where = sql.split(" WHERE ", 1)
    assert "next_retry_at = NOW()" in head
    assert "lease_expires_at = NULL" in head
    assert "completed_at = NULL" in head
    # Neither held nor marked spent: ``exhausted`` survives only if set.
    assert "infinity" not in head
    assert "'exhausted'" not in head
    assert f"jsonb_build_object('revived', {_flat(outbox._ALERT_REVIVALS_SQL)} + 1)" in head
    # The fence token survives revival.
    assert "attempts" not in head

    # Only an alert that wakes its thread, one that never does is never due,
    # and a command's report, a type a build before commands kills.
    assert where.startswith(
        "o.status = 'dead' AND o.hook_type IN ('market_target', 'wakeup', 'command_report_back')"
    )
    assert f"{_flat(outbox._ALERT_REVIVALS_SQL)} < %(max)s" in where
    assert _flat(outbox._REVIVED_OWED_SQL) in where
    assert mock_cursor.execute.call_args.args[1] == {
        "cooldown": 600,
        "window": 86400,
        "max": 2,
    }


@pytest.mark.asyncio
async def test_the_revive_scan_ranges_on_the_terminal_age_index(
    mock_db_connection, mock_cursor
):
    """``idx_hook_outbox_terminal_age`` indexes this exact expression over
    done and dead rows; dead for the cool-down, within the window."""
    await outbox.revive_dead_alerts()

    sql = _statements(mock_cursor)[-1]
    assert (
        "COALESCE(o.completed_at, o.created_at)"
        " <= NOW() - make_interval(secs => %(cooldown)s::float)"
    ) in sql
    assert (
        "COALESCE(o.completed_at, o.created_at)"
        " > NOW() - make_interval(secs => %(window)s::float)"
    ) in sql


def test_a_revival_count_of_any_other_type_reads_as_none():
    count = _flat(outbox._ALERT_REVIVALS_SQL)
    assert "jsonb_typeof(o.payload->'revived') = 'number'" in count
    assert "ELSE 0" in count


@pytest.mark.asyncio
async def test_the_close_sweep_ends_unheld_alerts_no_turn_will_take(
    mock_db_connection, mock_cursor
):
    await outbox.close_unowed_waiting_alerts()

    sql = _statements(mock_cursor)[-1]
    assert "SET status = 'done'" in sql
    assert (
        "WHERE o.hook_type IN ('market_target', 'wakeup', 'command_notice')"
        " AND o.status = 'pending' AND NOT (o.payload ? 'steered_run_id')"
    ) in sql
    # Not owed: only a waiting one, which nothing else would close.
    assert (
        f"({_flat(outbox.waiting_alert_sql('o'))} AND NOT {_flat(outbox._ALERT_OWED_SQL)})"
    ) in sql
    # No turn will come: any pending one, held or merely parked, a command's
    # notice held for the next turn among them, which no executor claims.
    assert "LEFT JOIN workspaces w ON w.workspace_id = c.workspace_id" in sql
    assert "AND (c.archived_at IS NOT NULL OR w.status = 'deleted')" in sql
    # Except a wakeup's notice still owed, which its executor must end first.
    assert f"(o.hook_type <> 'wakeup' OR NOT {_flat(outbox._ALERT_OWED_SQL)})" in sql
    due = _statements(mock_cursor)[-2]
    assert "SET next_retry_at = NOW()" in due
    assert f"AND {_flat(outbox.NO_TURN_SQL)} AND {_flat(outbox.WAKEUP_OWED_SQL)}" in due
    assert "ORDER BY (o.payload ? 'rides'), o.hook_outbox_id FOR UPDATE SKIP LOCKED" in due


def test_the_owed_predicate_reads_each_notice_as_delivery_does():
    owed = _flat(outbox._ALERT_OWED_SQL)
    assert "WHEN 'hit' THEN t.hit_delivered_at IS NULL" in owed
    assert (
        "WHEN 'expiring' THEN t.warn_delivered_at IS NULL"
        " AND t.status = 'armed' AND t.expires_at > NOW()"
    ) in owed
    assert "ELSE false" in owed
