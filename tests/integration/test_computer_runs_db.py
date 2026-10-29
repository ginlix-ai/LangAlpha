"""The usage run ledger against a real Postgres.

A run opens and closes inside the status writes themselves, guarded on the
computer's committed status, and the sweep's reconcile and metric claim are
single statements any worker may repeat. Only a real database shows that a
repeated or concurrent hook leaves one open run, that a late close changes
nothing, and that two folds of one run count nothing twice.
"""

from __future__ import annotations

import asyncio
import uuid
from datetime import datetime, timedelta, timezone
from unittest.mock import patch

import pytest

from src.server.database import computer_runs as runs_db
from src.server.database.computer import (
    create_computer,
    try_bind_computer_provider_ref,
    try_claim_computer_for_start,
    update_computer_status,
)
from src.server.database.session_lock import try_own_session_lock
from src.server.database.workspace import (
    create_workspace_on_computer,
    update_workspace_status,
)
from src.server.services.computer_usage import reconcile_runs
from src.server.services.computer_usage import transitions

pytestmark = [pytest.mark.integration, pytest.mark.asyncio(loop_scope="session")]


async def _computer(seed_user, *, kind="daytona", status="stopped", ref="sb-1"):
    computer = await create_computer(
        seed_user["user_id"], kind=kind, name=f"{kind} box {uuid.uuid4().hex[:8]}",
        status=status,
        provider_ref=ref,
    )
    return str(computer["computer_id"])


async def _runs(pool, computer_id):
    async with pool.connection() as conn:
        result = await conn.execute(
            "SELECT * FROM computer_runs WHERE computer_id = %s ORDER BY started_at",
            (computer_id,),
        )
        return await result.fetchall()


async def _raw(pool, sql, *params):
    async with pool.connection() as conn:
        await conn.execute(sql, params)


async def test_a_run_spans_running_and_stopping_and_ends_at_stopped(
    seed_user, test_db_pool
):
    computer_id = await _computer(seed_user)
    with patch.object(transitions, "resolve_spec", return_value=(2.0, 4.0, 5.0)):
        await update_computer_status(computer_id, "running")

    [run] = await _runs(test_db_pool, computer_id)
    assert run["ended_at"] is None
    assert (run["provider_kind"], run["provider_sandbox_id"]) == ("daytona", "sb-1")
    assert run["user_id"] == seed_user["user_id"]
    assert (run["resource_tier"], run["cpu"], run["memory_gb"], run["disk_gb"]) == (
        "standard", 2.0, 4.0, 5.0
    )
    assert run["metrics_final"] is False

    await update_computer_status(
        computer_id, "stopping", expected="running", run_end_reason="idle"
    )
    [run] = await _runs(test_db_pool, computer_id)
    # The sandbox bills through the stop's backup.
    assert run["ended_at"] is None and run["end_reason"] == "idle"

    await update_computer_status(computer_id, "stopped", expected="stopping")
    [run] = await _runs(test_db_pool, computer_id)
    assert run["ended_at"] is not None
    assert run["end_reason"] == "idle"


async def test_a_stop_that_aborts_keeps_the_same_run_and_forgets_its_reason(
    seed_user, test_db_pool
):
    computer_id = await _computer(seed_user)
    await update_computer_status(computer_id, "running")
    await update_computer_status(computer_id, "stopping", run_end_reason="idle")
    await update_computer_status(computer_id, "running", expected="stopping")

    [run] = await _runs(test_db_pool, computer_id)
    assert run["ended_at"] is None and run["end_reason"] is None


@pytest.mark.parametrize("kind, reason", [("daytona", "stopped"), ("e2b", "paused")])
async def test_a_plain_stop_says_what_the_provider_did(
    seed_user, test_db_pool, kind, reason
):
    computer_id = await _computer(seed_user, kind=kind)
    await update_computer_status(computer_id, "running")
    await update_computer_status(computer_id, "stopping")
    await update_computer_status(computer_id, "stopped")

    [run] = await _runs(test_db_pool, computer_id)
    assert run["end_reason"] == reason


async def test_repeated_and_concurrent_opens_leave_one_open_run(seed_user, test_db_pool):
    computer_id = await _computer(seed_user)
    row = await update_computer_status(computer_id, "running")

    await asyncio.gather(
        *(transitions.note_computer_status(row, "running") for _ in range(10))
    )
    await update_computer_status(computer_id, "running")

    runs = await _runs(test_db_pool, computer_id)
    assert len(runs) == 1 and runs[0]["ended_at"] is None


async def test_a_close_that_lands_after_the_machine_runs_again_ends_nothing(
    seed_user, test_db_pool
):
    computer_id = await _computer(seed_user)
    row = await update_computer_status(computer_id, "running")

    await transitions.note_computer_status(row, "stopped")
    await transitions.note_computer_status(row, "error")

    [run] = await _runs(test_db_pool, computer_id)
    assert run["ended_at"] is None


async def test_error_and_delete_close_the_run(seed_user, test_db_pool):
    erred = await _computer(seed_user, ref="sb-err")
    await update_computer_status(erred, "running")
    await update_computer_status(erred, "error")
    deleted = await _computer(seed_user, ref="sb-del")
    await update_computer_status(deleted, "running")
    await update_computer_status(deleted, "deleted")

    assert (await _runs(test_db_pool, erred))[0]["end_reason"] == "error"
    assert (await _runs(test_db_pool, deleted))[0]["end_reason"] == "deleted"


async def test_a_delete_after_a_lost_stop_ends_the_run_at_the_stop(
    seed_user, test_db_pool
):
    computer_id = await _computer(seed_user)
    await update_computer_status(computer_id, "running")
    stopped_at = datetime.now(timezone.utc) + timedelta(seconds=1)
    await _raw(
        test_db_pool,
        "UPDATE computers SET status = 'stopped', stopped_at = %s"
        " WHERE computer_id = %s",
        stopped_at, computer_id,
    )

    await update_computer_status(computer_id, "deleted")

    [run] = await _runs(test_db_pool, computer_id)
    assert run["ended_at"] == stopped_at
    assert run["end_reason"] == "deleted"


async def test_the_owners_own_machine_has_no_runs(seed_user, test_db_pool):
    computer_id = await _computer(seed_user, kind="ssh")
    await update_computer_status(computer_id, "running")
    assert await _runs(test_db_pool, computer_id) == []


async def test_docker_runs_are_billed_but_never_polled(seed_user, test_db_pool):
    computer_id = await _computer(seed_user, kind="docker")
    await update_computer_status(computer_id, "running")
    [run] = await _runs(test_db_pool, computer_id)
    assert run["metrics_final"] is True


async def test_a_replacement_sandbox_starts_a_run_of_its_own(seed_user, test_db_pool):
    computer_id = await _computer(seed_user, status="running")
    await update_computer_status(computer_id, "running")

    # A spec change claims the running machine, then binds the new sandbox.
    assert await try_claim_computer_for_start(computer_id, from_status="running")
    bound = await try_bind_computer_provider_ref(
        computer_id, provider_ref="sb-2", expected_previous_provider_ref="sb-1",
        platform_secret_version=1,
    )
    assert bound is not None

    old, new = await _runs(test_db_pool, computer_id)
    assert (old["provider_sandbox_id"], old["end_reason"]) == ("sb-1", "replaced")
    assert old["ended_at"] is not None
    assert (new["provider_sandbox_id"], new["ended_at"]) == ("sb-2", None)


async def test_a_bind_while_running_closes_the_run_on_the_old_sandbox(
    seed_user, test_db_pool
):
    computer_id = await _computer(seed_user)
    await update_computer_status(computer_id, "running")

    await try_bind_computer_provider_ref(
        computer_id, provider_ref="sb-rebuilt", expected_previous_provider_ref="sb-1",
        platform_secret_version=1,
    )

    old, new = await _runs(test_db_pool, computer_id)
    assert old["end_reason"] == "replaced"
    assert new["provider_sandbox_id"] == "sb-rebuilt" and new["ended_at"] is None


async def test_the_workspace_mirror_moves_the_run_only_when_it_reaches_the_machine(
    seed_user, test_db_pool
):
    computer_id = await _computer(seed_user)
    workspace = await create_workspace_on_computer(
        seed_user["user_id"], computer_id=computer_id, name="Research"
    )
    workspace_id = str(workspace["workspace_id"])

    await update_workspace_status(workspace_id, "running")
    [run] = await _runs(test_db_pool, computer_id)
    assert run["ended_at"] is None

    await update_workspace_status(workspace_id, "stopped")
    [run] = await _runs(test_db_pool, computer_id)
    assert run["end_reason"] == "stopped"


async def test_a_hook_in_the_callers_transaction_rolls_back_with_it(
    seed_user, test_db_pool
):
    computer_id = await _computer(seed_user)
    async with test_db_pool.connection() as conn:
        try:
            async with conn.transaction():
                await update_computer_status(computer_id, "running", conn=conn)
                assert len(await _runs_on(conn, computer_id)) == 1
                raise RuntimeError("caller aborts")
        except RuntimeError:
            pass
    assert await _runs(test_db_pool, computer_id) == []


async def _runs_on(conn, computer_id):
    result = await conn.execute(
        "SELECT run_id FROM computer_runs WHERE computer_id = %s", (computer_id,)
    )
    return await result.fetchall()


async def test_reconcile_closes_orphans_at_the_stop_and_opens_what_is_missing(
    seed_user, test_db_pool
):
    orphaned = await _computer(seed_user, ref="sb-orphan")
    await update_computer_status(orphaned, "running")
    stopped_at = datetime.now(timezone.utc) + timedelta(seconds=1)
    # A stop whose hook was lost (an older build wrote it, or the hook failed).
    await _raw(
        test_db_pool,
        "UPDATE computers SET status = 'stopped', stopped_at = %s"
        " WHERE computer_id = %s",
        stopped_at, orphaned,
    )
    unrecorded = await _computer(seed_user, ref="sb-unrecorded")
    await _raw(
        test_db_pool,
        "UPDATE computers SET status = 'running' WHERE computer_id = %s",
        unrecorded,
    )
    ssh = await _computer(seed_user, kind="ssh", ref="host")
    await _raw(
        test_db_pool, "UPDATE computers SET status = 'running' WHERE computer_id = %s",
        ssh,
    )

    assert await reconcile_runs() == (1, 1)

    [closed] = await _runs(test_db_pool, orphaned)
    assert closed["ended_at"] == stopped_at
    assert closed["end_reason"] == "reconciled"
    [opened] = await _runs(test_db_pool, unrecorded)
    assert opened["ended_at"] is None
    assert await _runs(test_db_pool, ssh) == []
    # Nothing left to do: reconcile is safe on every worker every cycle.
    assert await reconcile_runs() == (0, 0)


async def test_reconcile_moves_a_run_left_on_a_replaced_sandbox(
    seed_user, test_db_pool
):
    computer_id = await _computer(seed_user, ref="sb-old")
    await update_computer_status(computer_id, "running")
    # The replacement's bind, whose hook was lost.
    await _raw(
        test_db_pool,
        "UPDATE computers SET provider_ref = 'sb-new' WHERE computer_id = %s",
        computer_id,
    )

    assert await reconcile_runs() == (0, 1)

    old, new = await _runs(test_db_pool, computer_id)
    assert (old["provider_sandbox_id"], old["end_reason"]) == ("sb-old", "replaced")
    assert (new["provider_sandbox_id"], new["ended_at"]) == ("sb-new", None)
    assert await reconcile_runs() == (0, 0)


async def test_reconcile_ends_a_run_at_its_stop_after_a_later_delete(
    seed_user, test_db_pool
):
    computer_id = await _computer(seed_user)
    await update_computer_status(computer_id, "running")
    stopped_at = datetime.now(timezone.utc) + timedelta(seconds=1)
    # A stop and then a delete, both written without their hooks.
    await _raw(
        test_db_pool,
        "UPDATE computers SET status = 'deleted', stopped_at = %s"
        " WHERE computer_id = %s",
        stopped_at, computer_id,
    )

    assert await reconcile_runs() == (1, 0)

    [run] = await _runs(test_db_pool, computer_id)
    assert run["ended_at"] == stopped_at


async def test_a_run_the_machine_stopped_during_is_closed_when_it_runs_again(
    seed_user, test_db_pool
):
    computer_id = await _computer(seed_user)
    await update_computer_status(computer_id, "running")
    stopped_at = datetime.now(timezone.utc) + timedelta(seconds=1)
    await _raw(
        test_db_pool,
        "UPDATE computers SET status = 'running', stopped_at = %s"
        " WHERE computer_id = %s",
        stopped_at, computer_id,
    )

    await update_computer_status(computer_id, "running")

    old, new = await _runs(test_db_pool, computer_id)
    assert old["ended_at"] == stopped_at
    assert new["ended_at"] is None


async def test_a_run_is_claimed_once_per_cycle_and_folded_behind_its_cursor(
    seed_user, test_db_pool
):
    computer_id = await _computer(seed_user)
    await update_computer_status(computer_id, "running")
    kinds = ("daytona", "e2b")

    [claimed] = await runs_db.claim_runs_for_metrics(
        kinds=kinds, poll_every_seconds=240, limit=10
    )
    assert str(claimed["computer_id"]) == computer_id
    assert claimed["provider_config"] == {}
    fresh = await runs_db.get_computer_run(str(claimed["run_id"]))
    assert fresh["run_id"] == claimed["run_id"] and fresh["metrics_final"] is False
    # Another worker in the same cycle finds nothing due.
    assert await runs_db.claim_runs_for_metrics(
        kinds=kinds, poll_every_seconds=240, limit=10
    ) == []
    # A stop's fold names the computer and is not held off by the cycle.
    assert len(await runs_db.claim_runs_for_metrics(
        kinds=kinds, poll_every_seconds=240, limit=10, computer_id=computer_id
    )) == 1

    through = datetime.now(timezone.utc)
    aggregates = {"sample_count": 3, "cpu_avg_pct": 12.5,
                  "cpu_pct_histogram": [3] + [0] * 19, "metrics_through": through}
    run_id = claimed["run_id"]
    assert await runs_db.store_run_metrics(
        run_id, expected_through=None, aggregates=aggregates, final=False
    )
    # A fold that read the old cursor lost the race and writes nothing.
    assert not await runs_db.store_run_metrics(
        run_id, expected_through=None, aggregates=aggregates, final=False
    )
    await runs_db.store_run_metrics_error(run_id, "TimeoutError", final=False)

    [run] = await _runs(test_db_pool, computer_id)
    assert run["sample_count"] == 3
    assert run["metrics_through"] == through
    assert run["cpu_pct_histogram"] == [3] + [0] * 19
    assert run["metrics_error"] == "TimeoutError"


async def test_a_closed_run_is_due_once_for_its_last_fold(seed_user, test_db_pool):
    computer_id = await _computer(seed_user)
    await update_computer_status(computer_id, "running")
    await update_computer_status(computer_id, "stopped")
    kinds = ("daytona",)

    [claimed] = await runs_db.claim_runs_for_metrics(
        kinds=kinds, poll_every_seconds=0, limit=10
    )
    assert claimed["ended_at"] is not None
    await runs_db.store_run_metrics_error(claimed["run_id"], "gone", final=True)

    assert await runs_db.claim_runs_for_metrics(
        kinds=kinds, poll_every_seconds=0, limit=10
    ) == []


async def test_a_sigkill_counts_against_the_open_run_on_that_sandbox(
    seed_user, test_db_pool
):
    computer_id = await _computer(seed_user)
    await update_computer_status(computer_id, "running")

    assert await runs_db.record_oom_kill("daytona", "sb-1")
    assert await runs_db.record_oom_kill("daytona", "sb-1")
    assert not await runs_db.record_oom_kill("e2b", "sb-1")

    [run] = await _runs(test_db_pool, computer_id)
    assert run["oom_kills"] == 2


async def test_a_replacement_claim_keeps_the_run_until_the_new_sandbox_binds(
    seed_user, test_db_pool
):
    computer_id = await _computer(seed_user)
    await update_computer_status(computer_id, "running")

    # The old sandbox runs on while the claim backs it up; a released claim
    # goes back to the same run.
    assert await try_claim_computer_for_start(computer_id, from_status="running")
    [run] = await _runs(test_db_pool, computer_id)
    assert run["ended_at"] is None
    await update_computer_status(computer_id, "running", expected="starting")
    [same] = await _runs(test_db_pool, computer_id)
    assert same["run_id"] == run["run_id"] and same["ended_at"] is None


async def test_a_replacement_ends_the_old_run_at_its_delete_not_at_the_bind(
    seed_user, test_db_pool
):
    computer_id = await _computer(seed_user, ref="sb-old")
    await update_computer_status(computer_id, "running")
    assert await try_claim_computer_for_start(computer_id, from_status="running")

    await transitions.note_sandbox_replaced(computer_id, "sb-old")
    deleted_at = datetime.now(timezone.utc)
    await asyncio.sleep(0.2)  # the new sandbox's restore
    assert await try_bind_computer_provider_ref(
        computer_id, provider_ref="sb-new", expected_previous_provider_ref="sb-old",
        platform_secret_version=1,
    )
    await update_computer_status(computer_id, "running", expected="starting")

    old, new = await _runs(test_db_pool, computer_id)
    assert (old["provider_sandbox_id"], old["end_reason"]) == ("sb-old", "replaced")
    assert old["ended_at"] <= deleted_at < new["started_at"]
    assert (new["provider_sandbox_id"], new["ended_at"]) == ("sb-new", None)


async def test_a_stop_reason_lands_only_while_the_machine_is_stopping(
    seed_user, test_db_pool
):
    computer_id = await _computer(seed_user)
    await update_computer_status(computer_id, "running")

    await transitions.note_computer_status(
        {"computer_id": computer_id}, "stopping", reason="idle"
    )
    [run] = await _runs(test_db_pool, computer_id)
    assert run["end_reason"] is None


async def test_a_stop_that_ends_in_error_reads_error_whatever_it_was_for(
    seed_user, test_db_pool
):
    computer_id = await _computer(seed_user)
    await update_computer_status(computer_id, "running")
    await update_computer_status(computer_id, "stopping", run_end_reason="idle")

    await update_computer_status(computer_id, "error")

    [run] = await _runs(test_db_pool, computer_id)
    assert run["end_reason"] == "error"


async def test_a_closed_run_waits_out_the_settle_window_for_its_last_fold(
    seed_user, test_db_pool
):
    computer_id = await _computer(seed_user)
    await update_computer_status(computer_id, "running")
    await update_computer_status(computer_id, "stopped")
    kinds = ("daytona", "e2b")

    assert await runs_db.claim_runs_for_metrics(
        kinds=kinds, poll_every_seconds=240, settle_seconds=120, limit=10
    ) == []
    # A delete's read names the computer and does not wait.
    assert len(await runs_db.claim_runs_for_metrics(
        kinds=kinds, poll_every_seconds=240, settle_seconds=120, limit=10,
        computer_id=computer_id,
    )) == 1
    await _raw(
        test_db_pool,
        "UPDATE computer_runs SET ended_at = ended_at - interval '5 minutes',"
        " started_at = started_at - interval '10 minutes',"
        " metrics_polled_at = NULL WHERE computer_id = %s",
        computer_id,
    )
    [claimed] = await runs_db.claim_runs_for_metrics(
        kinds=kinds, poll_every_seconds=240, settle_seconds=120, limit=10
    )
    assert str(claimed["computer_id"]) == computer_id


async def test_one_worker_collects_and_the_close_hands_the_lock_on(
    patched_get_db_connection,
):
    async with try_own_session_lock("computer_usage:test_collector") as first:
        async with try_own_session_lock("computer_usage:test_collector") as second:
            assert (first, second) == (True, False)
    async with try_own_session_lock("computer_usage:test_collector") as next_cycle:
        assert next_cycle is True
