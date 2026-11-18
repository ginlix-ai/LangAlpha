"""A suspension's stop of a turn, against real Postgres.

The stop is recorded where the cancel is stamped and read back from the row
the finalize settles, whichever worker took the cancel and whatever its
local flags said, so only the rows the real stamp and finalize write show
that it reads as the suspension, a user's stop as the user's, and an agent's
as it always has.
"""

from __future__ import annotations

import uuid

import pytest
import pytest_asyncio

pytestmark = [pytest.mark.integration, pytest.mark.asyncio(loop_scope="session")]


@pytest_asyncio.fixture(loop_scope="session")
async def thread_id(seed_workspace, patched_get_db_connection):
    from src.server.database.conversation import create_thread

    tid = str(uuid.uuid4())
    await create_thread(
        conversation_thread_id=tid,
        workspace_id=str(seed_workspace["workspace_id"]),
        current_status="in_progress",
        msg_type="ptc",
    )
    return tid


async def _run(thread_id: str, user_id: str | None = None) -> str:
    from src.server.database.runs.lifecycle import start_run

    run_id = str(uuid.uuid4())
    await start_run(
        run_id=run_id,
        thread_id=thread_id,
        request_key=str(uuid.uuid4()),
        metadata={
            "msg_type": "ptc",
            "automation_execution_id": str(uuid.uuid4()),
        },
        user_id=user_id,
    )
    return run_id


async def _settle(thread_id: str, run_id: str, metadata: dict) -> dict:
    from src.server.database.runs.lifecycle import RunOutcome, finalize_run

    result = await finalize_run(
        run_id=run_id,
        thread_id=thread_id,
        outcome=RunOutcome(status="cancelled", metadata=metadata),
    )
    assert result.applied
    return result.run


# The executor's finalize after a stop it was signalled for: the nudge and
# the reconcile signal as a user's stop, so its own flag says the user's. A
# turn stopped while it waited for its computer settles through ``fail_open``,
# which says nothing.
EXECUTOR = {"cancelled_by_user": True}
WAITING = {}


@pytest.mark.parametrize("settled_by", [EXECUTOR, WAITING], ids=["executor", "waiting"])
async def test_a_suspensions_stop_settles_as_the_suspension(thread_id, settled_by):
    from src.server.contracts.status import (
        bills_subagents,
        is_restart_interrupted,
        is_suspension_stop,
        is_user_stop,
    )
    from src.server.database.runs.lifecycle import request_run_cancel
    from src.server.services.automation_outcomes import Outcome, ledger_outcome

    run_id = await _run(thread_id)
    intent = await request_run_cancel(run_id, thread_id=thread_id, by="suspension")
    assert intent["state"] == "requested"

    row = await _settle(thread_id, run_id, {**settled_by, "interrupted_by_restart": True})

    assert row["status"] == "cancelled"
    assert row["metadata"]["cancelled_by_user"] is False
    assert "stopped_by_agent" not in row["metadata"]
    assert is_suspension_stop(row) and not is_user_stop(row)
    assert not is_restart_interrupted(row)
    assert ledger_outcome(row) is Outcome.SUSPENDED
    # The work before the stop is the account's, as at any stop it asked for.
    assert bills_subagents(row)


@pytest.mark.parametrize("settled_by", [EXECUTOR, WAITING], ids=["executor", "waiting"])
async def test_a_users_stop_stays_the_users(thread_id, settled_by):
    from src.server.contracts.status import is_suspension_stop, is_user_stop
    from src.server.database.runs.lifecycle import request_run_cancel
    from src.server.services.automation_outcomes import Outcome, ledger_outcome

    run_id = await _run(thread_id)
    await request_run_cancel(run_id, thread_id=thread_id)

    row = await _settle(thread_id, run_id, settled_by)

    assert is_user_stop(row) and not is_suspension_stop(row)
    assert ledger_outcome(row) is Outcome.STOPPED


async def test_an_agents_stop_stays_what_it_was(thread_id):
    from src.server.contracts.status import is_suspension_stop, is_user_stop
    from src.server.database.runs.lifecycle import request_run_cancel
    from src.server.services.automation_outcomes import Outcome, ledger_outcome

    run_id = await _run(thread_id)
    await request_run_cancel(run_id, thread_id=thread_id, by="agent")

    row = await _settle(thread_id, run_id, EXECUTOR)

    assert row["metadata"]["stopped_by_agent"] is True
    assert is_user_stop(row) and not is_suspension_stop(row)
    assert ledger_outcome(row) is Outcome.STOPPED


@pytest.mark.parametrize("first", ["user", "suspension"])
async def test_the_first_stop_stamped_is_the_one_recorded(thread_id, first):
    from src.server.contracts.status import is_suspension_stop, is_user_stop
    from src.server.database.runs.lifecycle import request_run_cancel

    run_id = await _run(thread_id)
    for by in (first, "suspension" if first == "user" else "user"):
        await request_run_cancel(run_id, thread_id=thread_id, by=by)

    row = await _settle(thread_id, run_id, EXECUTOR)

    assert is_user_stop(row) is (first == "user")
    assert is_suspension_stop(row) is (first == "suspension")


async def test_a_report_settled_before_a_suspensions_stop_is_covered_by_it(thread_id):
    from datetime import timedelta

    from src.server.database.runs.lifecycle import get_run, request_run_cancel
    from src.server.services.report_back.notify_turn import stop_already_covered

    run_id = await _run(thread_id)
    await request_run_cancel(run_id, thread_id=thread_id, by="suspension")
    await _settle(thread_id, run_id, WAITING)

    row = await get_run(run_id)
    stopped_at = row["cancel_requested_at"]
    assert stop_already_covered(row, stopped_at - timedelta(seconds=1))
    assert not stop_already_covered(row, stopped_at + timedelta(seconds=1))


async def test_a_suspensions_stop_reads_only_turns_started_before_its_notice(
    thread_id, seed_workspace
):
    from datetime import datetime, timedelta, timezone

    from src.server.database.runs.lifecycle import open_runs_of_user

    user_id = seed_workspace["user_id"]
    run_id = await _run(thread_id, user_id)
    now = datetime.now(timezone.utc)

    def ids(rows):
        return {str(r["conversation_response_id"]) for r in rows}

    assert run_id in ids(await open_runs_of_user(user_id))
    assert run_id in ids(await open_runs_of_user(user_id, started_before=now))
    assert run_id not in ids(
        await open_runs_of_user(user_id, started_before=now - timedelta(minutes=1))
    )
