"""The usage run ledger against a real Postgres.

A run opens and closes inside the status writes themselves, guarded on the
computer's committed status, and the sweep's reconcile and metric claim are
single statements any worker may repeat. Only a real database shows that a
repeated or concurrent hook leaves one open run, that a late close changes
nothing, and that two folds of one run count nothing twice.
"""

from __future__ import annotations

import asyncio
import json
import time
import uuid
from datetime import datetime, timedelta, timezone
from unittest.mock import patch

import pytest

from ptc_agent.core.sandbox.runtime import RuntimeState, SandboxAllocation
from src.observability import event_log
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
from src.server.services.computer_usage.causes import computer_cause

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
        run_id, expected_through=None, aggregates=aggregates, final=False,
        ended_at=None,
    )
    # A fold that read the old cursor lost the race and writes nothing.
    assert not await runs_db.store_run_metrics(
        run_id, expected_through=None, aggregates=aggregates, final=False,
        ended_at=None,
    )
    await runs_db.store_run_metrics_error(
        run_id, "TimeoutError", final=False, ended_at=None
    )

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
    await runs_db.store_run_metrics_error(
        claimed["run_id"], "gone", final=True, ended_at=claimed["ended_at"]
    )

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


# -- Each run's provider-reported size, and the disk hold after it (122) ----


def _alloc(state, *, archive_after_s=7 * 86400):
    """What Daytona reports for an old standard sandbox: 1 vCPU, 1 GiB, 3 GiB."""
    return SandboxAllocation(
        state=RuntimeState(state), cpu=1.0, memory_gb=1.0, disk_gb=3.0,
        archive_after_s=archive_after_s,
    )


async def _answer(kind, sandbox, found, *, asked_at=None):
    """The provider's answer, read just now unless *asked_at* says earlier."""
    await runs_db.store_allocation(
        kind, sandbox, found,
        asked_at=time.monotonic() if asked_at is None else asked_at,
    )


async def _gone(kind, sandbox, *, archive_after_s):
    await runs_db.store_sandbox_gone(
        kind, sandbox, archive_after_s=archive_after_s, asked_at=time.monotonic()
    )


async def _stop(computer_id):
    await update_computer_status(computer_id, "stopping")
    await update_computer_status(computer_id, "stopped")


async def _age_runs(pool, computer_id, days):
    await _raw(
        pool,
        "UPDATE computer_runs SET started_at = started_at - make_interval(days => %s),"
        " ended_at = ended_at - make_interval(days => %s),"
        " allocation_checked_at = allocation_checked_at - make_interval(days => %s)"
        " WHERE computer_id = %s",
        days, days, days, computer_id,
    )


async def test_the_providers_size_replaces_the_tiers_and_a_restart_copies_it(
    seed_user, test_db_pool
):
    computer_id = await _computer(seed_user)
    with patch.object(transitions, "resolve_spec", return_value=(1.0, 2.0, 5.0)):
        await update_computer_status(computer_id, "running")
        await _stop(computer_id)
        await _answer("daytona", "sb-1", _alloc("stopped"))
        await update_computer_status(computer_id, "running")

    await runs_db.settle_holds()
    first, second = await _runs(test_db_pool, computer_id)
    for run in (first, second):
        assert (run["cpu"], run["memory_gb"], run["disk_gb"]) == (1.0, 1.0, 3.0)
        assert run["size_source"] == "provider"
        assert run["archive_after_s"] == 7 * 86400
    # The stop's hold on the disk ended when the sandbox ran again.
    assert first["held_end"] == "restarted"
    assert first["held_until"] == second["started_at"]


async def test_a_restart_after_the_archive_holds_the_disk_only_until_it(
    seed_user, test_db_pool
):
    computer_id = await _computer(seed_user)
    await update_computer_status(computer_id, "running")
    await _stop(computer_id)
    await _answer(
        "daytona", "sb-1", _alloc("stopped", archive_after_s=86400)
    )
    await _age_runs(test_db_pool, computer_id, 30)
    await update_computer_status(computer_id, "running")

    await runs_db.settle_holds()
    first, _second = await _runs(test_db_pool, computer_id)
    assert first["held_end"] == "archived"
    assert first["held_until"] - first["ended_at"] == timedelta(days=1)


async def test_a_hold_waits_for_the_archive_moment_until_the_provider_is_asked(
    seed_user, test_db_pool
):
    computer_id = await _computer(seed_user)
    await update_computer_status(computer_id, "running")
    await _stop(computer_id)
    await _age_runs(test_db_pool, computer_id, 30)
    await update_computer_status(computer_id, "running")

    first, _second = await _runs(test_db_pool, computer_id)
    assert first["held_until"] is None

    await _answer("daytona", "sb-1", _alloc("running"))
    first, _second = await _runs(test_db_pool, computer_id)
    assert first["held_end"] == "archived"
    assert first["held_until"] - first["ended_at"] == timedelta(days=7)


async def test_the_sweep_settles_a_hold_once_the_archive_interval_has_passed(
    seed_user, test_db_pool
):
    computer_id = await _computer(seed_user)
    await update_computer_status(computer_id, "running")
    await _stop(computer_id)
    await _answer("daytona", "sb-1", _alloc("stopped"))
    assert await runs_db.settle_holds() == 0

    await _age_runs(test_db_pool, computer_id, 8)
    assert await runs_db.settle_holds() == 1
    [run] = await _runs(test_db_pool, computer_id)
    assert run["held_end"] == "archived"
    assert run["held_until"] - run["ended_at"] == timedelta(days=7)


async def test_deleting_a_stopped_computer_ends_its_hold_at_the_delete(
    seed_user, test_db_pool
):
    computer_id = await _computer(seed_user)
    await update_computer_status(computer_id, "running")
    await _answer("daytona", "sb-1", _alloc("running"))
    await _stop(computer_id)
    await update_computer_status(computer_id, "deleted")

    await runs_db.settle_holds()
    [run] = await _runs(test_db_pool, computer_id)
    assert run["held_end"] == "deleted"
    assert run["held_until"] >= run["ended_at"]


async def test_a_delete_after_a_lost_stop_holds_the_disk_from_the_stop(
    seed_user, test_db_pool
):
    computer_id = await _computer(seed_user)
    await update_computer_status(computer_id, "running")
    await _answer("daytona", "sb-1", _alloc("running"))
    await _raw(
        test_db_pool,
        "UPDATE computers SET stopped_at = NOW() - interval '2 hours'"
        " WHERE computer_id = %s",
        computer_id,
    )
    await _raw(
        test_db_pool,
        "UPDATE computer_runs SET started_at = NOW() - interval '5 hours'"
        " WHERE computer_id = %s",
        computer_id,
    )
    await update_computer_status(computer_id, "deleted")

    await runs_db.settle_holds()
    [run] = await _runs(test_db_pool, computer_id)
    assert run["held_end"] == "deleted"
    assert timedelta(hours=1, minutes=59) < run["held_until"] - run["ended_at"]


async def test_a_gone_sandbox_keeps_the_tiers_size_and_ends_its_hold(
    seed_user, test_db_pool
):
    computer_id = await _computer(seed_user)
    with patch.object(transitions, "resolve_spec", return_value=(1.0, 2.0, 5.0)):
        await update_computer_status(computer_id, "running")
    await _stop(computer_id)
    await _age_runs(test_db_pool, computer_id, 30)

    await _gone(
        "daytona", "sb-1", archive_after_s=7 * 86400
    )

    [run] = await _runs(test_db_pool, computer_id)
    assert run["size_source"] == "tier" and run["disk_gb"] == 5.0
    # Never asked in time to learn its own interval: the configured one bounds it.
    assert run["held_end"] == "archived"
    assert run["held_until"] - run["ended_at"] == timedelta(days=7)


async def test_a_replacement_holds_no_disk_after_the_old_sandbox(
    seed_user, test_db_pool
):
    computer_id = await _computer(seed_user)
    await update_computer_status(computer_id, "running")
    await _answer("daytona", "sb-1", _alloc("running"))
    await transitions.note_sandbox_replaced(computer_id, "sb-1")

    [run] = await _runs(test_db_pool, computer_id)
    assert run["end_reason"] == "replaced" and run["held_end"] == "deleted"
    assert run["held_until"] == run["ended_at"]


async def test_the_sweep_asks_about_unsized_and_still_held_sandboxes_only(
    seed_user, test_db_pool
):
    held = await _computer(seed_user, ref="sb-held")
    await update_computer_status(held, "running")
    await _stop(held)
    sized = await _computer(seed_user, ref="sb-sized")
    await update_computer_status(sized, "running")
    await _answer(
        "daytona", "sb-sized", _alloc("running", archive_after_s=None)
    )

    due = await runs_db.get_sandboxes_due_allocation(
        kinds=["daytona"], size_every_seconds=240, held_every_seconds=3600, limit=50
    )

    assert {row["provider_sandbox_id"] for row in due} >= {"sb-held"}
    assert "sb-sized" not in {row["provider_sandbox_id"] for row in due}


async def test_a_stop_the_provider_never_finished_runs_until_a_check_finds_it_down(
    seed_user, test_db_pool
):
    computer_id = await _computer(seed_user)
    await update_computer_status(computer_id, "running")
    await _answer("daytona", "sb-1", _alloc("running"))
    await _stop(computer_id)
    [stopped] = await _runs(test_db_pool, computer_id)

    # Closed after its last check, so the sweep asks at once.
    due = await runs_db.get_sandboxes_due_allocation(
        kinds=["daytona"], size_every_seconds=240, held_every_seconds=3600, limit=50
    )
    assert "sb-1" in {row["provider_sandbox_id"] for row in due}

    await asyncio.sleep(0.05)
    await _answer("daytona", "sb-1", _alloc("stopping"))
    [still_up] = await _runs(test_db_pool, computer_id)
    assert still_up["ended_at"] > stopped["ended_at"]
    assert still_up["held_until"] is None
    # Asked again a cycle on.
    await _raw(
        test_db_pool,
        "UPDATE computer_runs SET allocation_checked_at = NOW() - interval '5 minutes'"
        " WHERE computer_id = %s",
        computer_id,
    )
    due = await runs_db.get_sandboxes_due_allocation(
        kinds=["daytona"], size_every_seconds=240, held_every_seconds=3600, limit=50
    )
    assert "sb-1" in {row["provider_sandbox_id"] for row in due}

    await _answer("daytona", "sb-1", _alloc("stopped"))
    [down] = await _runs(test_db_pool, computer_id)
    assert down["ended_at"] == still_up["ended_at"]
    due = await runs_db.get_sandboxes_due_allocation(
        kinds=["daytona"], size_every_seconds=240, held_every_seconds=3600, limit=50
    )
    assert "sb-1" not in {row["provider_sandbox_id"] for row in due}


async def test_a_hold_whose_archive_moment_is_unknown_settles_once_asked(
    seed_user, test_db_pool
):
    computer_id = await _computer(seed_user)
    await update_computer_status(computer_id, "running")
    await _stop(computer_id)
    await update_computer_status(computer_id, "deleted")
    [run] = await _runs(test_db_pool, computer_id)
    assert run["held_until"] is None

    # The sandbox went with the computer; the delete, a stored time, ends it.
    await _gone("daytona", "sb-1", archive_after_s=7 * 86400)

    [run] = await _runs(test_db_pool, computer_id)
    assert run["size_source"] == "tier" and run["held_end"] == "deleted"


async def test_a_docker_runs_hold_ends_at_its_next_start_unasked(
    seed_user, test_db_pool
):
    computer_id = await _computer(seed_user, kind="docker", ref="ctr-1")
    await update_computer_status(computer_id, "running")
    await _stop(computer_id)
    await update_computer_status(computer_id, "running")

    await runs_db.settle_holds()
    first, second = await _runs(test_db_pool, computer_id)
    assert first["size_source"] == second["size_source"] == "tier"
    assert first["held_end"] == "restarted"
    assert first["held_until"] == second["started_at"]


async def test_a_later_answer_completes_a_size_the_provider_left_out(
    seed_user, test_db_pool
):
    computer_id = await _computer(seed_user, kind="e2b", ref="e2b-1")
    with patch.object(transitions, "resolve_spec", return_value=(1.0, 2.0, 5.0)):
        await update_computer_status(computer_id, "running")
    # A sandbox that runs on, and whose disk sample did not come back.
    running = SandboxAllocation(state=RuntimeState.RUNNING, cpu=1.0, memory_gb=2.0)
    await _answer("e2b", "e2b-1", running)
    [run] = await _runs(test_db_pool, computer_id)
    assert run["disk_gb"] == 5.0 and run["size_source"] == "partial"

    await _raw(
        test_db_pool,
        "UPDATE computer_runs SET allocation_checked_at = NOW() - interval '5 minutes'"
        " WHERE computer_id = %s",
        computer_id,
    )
    due = await runs_db.get_sandboxes_due_allocation(
        kinds=["e2b"], size_every_seconds=240, held_every_seconds=3600, limit=50
    )
    assert "e2b-1" in {row["provider_sandbox_id"] for row in due}

    await _answer(
        "e2b", "e2b-1", SandboxAllocation(
            state=RuntimeState.RUNNING, cpu=1.0, memory_gb=2.0, disk_gb=20.0
        )
    )
    [run] = await _runs(test_db_pool, computer_id)
    assert run["disk_gb"] == 20.0 and run["size_source"] == "provider"


async def test_a_sandbox_starting_again_does_not_stretch_the_last_run(
    seed_user, test_db_pool
):
    computer_id = await _computer(seed_user)
    await update_computer_status(computer_id, "running")
    await _answer("daytona", "sb-1", _alloc("running"))
    await _stop(computer_id)
    [stopped] = await _runs(test_db_pool, computer_id)

    # The start brings the sandbox up before the run opens at 'running'.
    assert await try_claim_computer_for_start(computer_id, from_status="stopped")
    await _answer("daytona", "sb-1", _alloc("starting"))

    [run] = await _runs(test_db_pool, computer_id)
    assert run["ended_at"] == stopped["ended_at"]


@pytest.mark.parametrize("checked_before_stop", [True, False])
async def test_a_fresh_stop_is_asked_about_ahead_of_unsized_history(
    seed_user, test_db_pool, checked_before_stop
):
    for i in range(3):
        old = await _computer(seed_user, ref=f"sb-old-{i}")
        await update_computer_status(old, "running")
        if i:
            await _stop(old)
    fresh = await _computer(seed_user, ref="sb-fresh")
    await update_computer_status(fresh, "running")
    if checked_before_stop:
        await _answer("daytona", "sb-fresh", _alloc("running"))
    await _stop(fresh)

    [first] = await runs_db.get_sandboxes_due_allocation(
        kinds=["daytona"], size_every_seconds=240, held_every_seconds=3600, limit=1
    )

    assert first["provider_sandbox_id"] == "sb-fresh"


async def test_no_archive_ends_a_hold_until_a_check_finds_the_sandbox_down(
    seed_user, test_db_pool
):
    computer_id = await _computer(seed_user)
    await update_computer_status(computer_id, "running")
    await _answer("daytona", "sb-1", _alloc("running"))
    await _stop(computer_id)
    await _age_runs(test_db_pool, computer_id, 8)

    # The stop may never have finished: neither the archive moment nor a
    # check that failed ends the hold.
    await runs_db.mark_allocation_checked("daytona", "sb-1")
    await runs_db.settle_holds()
    [run] = await _runs(test_db_pool, computer_id)
    assert run["held_until"] is None

    await _answer("daytona", "sb-1", _alloc("stopped"))
    [run] = await _runs(test_db_pool, computer_id)
    assert run["held_end"] == "archived"
    assert run["held_until"] - run["ended_at"] == timedelta(days=7)


async def test_a_run_extended_past_its_stop_is_read_again_for_the_time_added(
    seed_user, test_db_pool
):
    computer_id = await _computer(seed_user)
    await update_computer_status(computer_id, "running")
    await _answer("daytona", "sb-1", _alloc("running"))
    await _stop(computer_id)
    [claimed] = await runs_db.claim_runs_for_metrics(
        kinds=("daytona",), poll_every_seconds=0, limit=10
    )

    await asyncio.sleep(0.05)
    await _answer("daytona", "sb-1", _alloc("stopping"))
    # The fold claimed before the check read only to the old end.
    assert await runs_db.store_run_metrics(
        claimed["run_id"], expected_through=None,
        aggregates={"metrics_through": claimed["ended_at"]}, final=True,
        ended_at=claimed["ended_at"],
    )

    [run] = await _runs(test_db_pool, computer_id)
    assert run["ended_at"] > claimed["ended_at"]
    assert run["metrics_final"] is False
    assert run["metrics_through"] == claimed["ended_at"]


async def test_a_replacement_still_closes_its_run_when_the_hold_update_fails(
    seed_user, test_db_pool
):
    computer_id = await _computer(seed_user)
    await update_computer_status(computer_id, "running")

    async def conflicting(cur, *args, **kwargs):
        await cur.execute("SELECT 1 / 0")

    with patch.object(runs_db, "_end_holds", conflicting):
        await transitions.note_sandbox_replaced(computer_id, "sb-1")

    [run] = await _runs(test_db_pool, computer_id)
    assert run["end_reason"] == "replaced" and run["ended_at"] is not None
    assert run["held_until"] is None


async def test_an_answer_read_before_a_stop_says_nothing_of_it(seed_user, test_db_pool):
    computer_id = await _computer(seed_user)
    await update_computer_status(computer_id, "running")
    await _answer("daytona", "sb-1", _alloc("running"))
    read = time.monotonic()
    await asyncio.sleep(0.05)
    await _stop(computer_id)
    [stopped] = await _runs(test_db_pool, computer_id)

    # Read while the sandbox still ran, and written after the stop: it
    # neither stretches the run nor confirms the stop.
    await _answer("daytona", "sb-1", _alloc("running"), asked_at=read)
    # Read as archived before a restart and stop another worker made.
    await _answer("daytona", "sb-1", _alloc("archived"), asked_at=read)

    [run] = await _runs(test_db_pool, computer_id)
    assert run["ended_at"] == stopped["ended_at"]
    assert run["held_until"] is None
    assert run["seen_down_at"] < run["ended_at"]


async def test_a_gone_vm_keeps_holding_its_disk_with_no_archive(
    seed_user, test_db_pool
):
    computer_id = await _computer(seed_user)
    await update_computer_status(computer_id, "running")
    await _stop(computer_id)
    await _answer("daytona", "sb-1", _alloc("stopped", archive_after_s=None))
    await _age_runs(test_db_pool, computer_id, 30)

    await _gone("daytona", "sb-1", archive_after_s=7 * 86400)

    [run] = await _runs(test_db_pool, computer_id)
    assert run["archive_after_s"] is None and run["held_end"] == "gone"
    assert run["held_until"] - run["ended_at"] > timedelta(days=29)


async def test_a_sandbox_found_up_as_its_computer_starts_confirms_no_stop(
    seed_user, test_db_pool
):
    computer_id = await _computer(seed_user)
    await update_computer_status(computer_id, "running")
    await _answer("daytona", "sb-1", _alloc("running"))
    await _stop(computer_id)
    await _age_runs(test_db_pool, computer_id, 8)

    # A start claim keeps the run from stretching; the stop is still unknown.
    assert await try_claim_computer_for_start(computer_id, from_status="stopped")
    await _answer("daytona", "sb-1", _alloc("running"))

    [run] = await _runs(test_db_pool, computer_id)
    assert run["held_until"] is None and run["seen_down_at"] is None


async def test_stops_whose_lookups_keep_failing_take_turns(seed_user, test_db_pool):
    for ref in ("sb-a", "sb-b"):
        computer_id = await _computer(seed_user, ref=ref)
        await update_computer_status(computer_id, "running")
        await _stop(computer_id)
    await _raw(
        test_db_pool,
        # Asked once since each stop, ten minutes ago, and failed.
        "UPDATE computer_runs SET size_source = 'provider',"
        " started_at = started_at - interval '10 minutes',"
        " ended_at = ended_at - interval '10 minutes',"
        " allocation_checked_at = ended_at - interval '10 minutes'"
        " + interval '1 second'",
    )

    def ask():
        return runs_db.get_sandboxes_due_allocation(
            kinds=["daytona"], size_every_seconds=0, held_every_seconds=3600, limit=1
        )

    [first] = await ask()
    await runs_db.mark_allocation_checked("daytona", first["provider_sandbox_id"])
    [second] = await ask()

    assert {first["provider_sandbox_id"], second["provider_sandbox_id"]} == {
        "sb-a", "sb-b"
    }


async def test_a_stop_found_down_is_not_undone_by_a_later_answer_finding_it_up(
    seed_user, test_db_pool
):
    computer_id = await _computer(seed_user)
    await update_computer_status(computer_id, "running")
    await _stop(computer_id)
    await _answer("daytona", "sb-1", _alloc("stopped"))
    [stopped] = await _runs(test_db_pool, computer_id)

    # Brought up by a start that failed before its run opened.
    await update_computer_status(computer_id, "error")
    await _answer("daytona", "sb-1", _alloc("running"))

    [run] = await _runs(test_db_pool, computer_id)
    assert run["ended_at"] == stopped["ended_at"]


async def test_first_down_is_the_first_answer_that_found_the_stop_and_only_it(
    seed_user, test_db_pool
):
    computer_id = await _computer(seed_user)
    await update_computer_status(computer_id, "running")
    await _stop(computer_id)
    await _answer("daytona", "sb-1", _alloc("stopped"))
    [run] = await _runs(test_db_pool, computer_id)
    assert run["first_down_at"] is not None
    assert run["first_down_at"] >= run["ended_at"] - timedelta(seconds=1)

    # As a run that ended before the column existed: an earlier answer saw it
    # down, so a later one is not its first.
    await _raw(
        test_db_pool,
        "UPDATE computer_runs SET first_down_at = NULL WHERE computer_id = %s",
        computer_id,
    )
    await _answer("daytona", "sb-1", _alloc("stopped"))
    [run] = await _runs(test_db_pool, computer_id)
    assert run["first_down_at"] is None


async def test_a_run_records_the_cause_of_the_start_that_opened_it(
    seed_user, test_db_pool
):
    computer_id = await _computer(seed_user)
    with computer_cause("warm", client="desktop", detail="composer"):
        await update_computer_status(computer_id, "running")

    [run] = await _runs(test_db_pool, computer_id)
    assert (run["start_cause"], run["start_client"]) == ("warm", "desktop")


async def test_each_status_write_reaching_the_machine_is_an_event(
    seed_user, test_db_pool, tmp_path, monkeypatch
):
    monkeypatch.setenv("EVENT_LOG_DIR", str(tmp_path))
    computer_id = await _computer(seed_user)
    with computer_cause("button", client="browser"):
        await update_computer_status(computer_id, "starting")

    event_log.flush()
    [path] = tmp_path.glob("computer-events-*.jsonl")
    [event] = [json.loads(line) for line in path.read_text().splitlines()]
    assert (event["computer_id"], event["status"], event["cause"], event["client"]) == (
        computer_id, "starting", "button", "browser"
    )
    assert (event["kind"], event["sandbox"]) == ("daytona", "sb-1")
