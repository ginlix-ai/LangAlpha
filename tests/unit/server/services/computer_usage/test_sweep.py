"""The usage sweep: what a provider read costs a run, and what it never costs the sweep.

The claim and cursor SQL is pinned against Postgres in
``tests/integration/test_computer_runs_db.py``; this pins the Python around
it with the database layer patched out.
"""

from __future__ import annotations

import asyncio
import time
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from unittest.mock import ANY, AsyncMock, MagicMock, patch

import pytest

from ptc_agent.core.sandbox.runtime import (
    MetricSample,
    RuntimeState,
    SandboxAllocation,
    SandboxFailureKind,
    SandboxRateLimitedError,
)
from src.server.services.computer_usage import sweep

pytestmark = pytest.mark.asyncio

T0 = datetime(2026, 10, 23, 12, 0, tzinfo=timezone.utc)


def _run(**overrides):
    run = {
        "run_id": "run-1",
        "computer_id": "c-1",
        "provider_kind": "daytona",
        "provider_sandbox_id": "sb-1",
        "started_at": T0,
        "ended_at": None,
        "metrics_through": None,
        "sample_count": None,
    }
    run.update(overrides)
    return run


@pytest.fixture
def runs_db():
    with patch.object(sweep, "runs_db") as db:
        db.store_run_metrics = AsyncMock(return_value=True)
        db.store_run_metrics_error = AsyncMock()
        # The claim filters ``db.claimable`` as the SQL does, by kind and,
        # for a delete's read, computer; the periodic sweep claims per kind.
        db.claimable = []

        def claim(*, kinds, computer_id=None, limit, **_):
            due = [
                r
                for r in db.claimable
                if r["provider_kind"] in kinds
                and (computer_id is None or r["computer_id"] == computer_id)
            ]
            return due[:limit]

        db.claim_runs_for_metrics = AsyncMock(side_effect=claim)
        db.close_orphaned_runs = AsyncMock(return_value=0)
        db.get_running_computers_without_run = AsyncMock(return_value=[])
        db.settle_holds = AsyncMock(return_value=0)
        db.get_sandboxes_due_allocation = AsyncMock(return_value=[])
        db.store_allocation = AsyncMock()
        db.store_sandbox_gone = AsyncMock()
        db.mark_allocation_checked = AsyncMock()
        yield db


def _provider(samples=(), error=None, kind=SandboxFailureKind.UNKNOWN):
    provider = MagicMock()
    provider.metrics_since = AsyncMock(return_value=list(samples), side_effect=error)
    provider.classify_error = MagicMock(return_value=kind)
    return provider


def _lock(held: bool):
    @asynccontextmanager
    async def lock(key):
        yield held

    return lock


async def test_an_open_run_reads_from_its_start_and_stores_behind_the_cursor(runs_db):
    sample = MetricSample(timestamp=T0 + timedelta(seconds=5), cpu_used_pct=40.0)
    provider = _provider([sample])

    assert await sweep.fold_run(_run(), provider) is True

    provider.metrics_since.assert_awaited_once_with("sb-1", T0, until=None)
    call = runs_db.store_run_metrics.await_args
    assert call.args == ("run-1",)
    assert call.kwargs["expected_through"] is None
    assert call.kwargs["final"] is False
    assert call.kwargs["aggregates"]["sample_count"] == 1
    assert call.kwargs["aggregates"]["metrics_through"] == sample.timestamp


async def test_a_run_resumes_from_the_last_sample_folded(runs_db):
    through = T0 + timedelta(minutes=5)
    provider = _provider()
    await sweep.fold_run(_run(metrics_through=through, sample_count=60), provider)

    provider.metrics_since.assert_awaited_once_with("sb-1", through, until=None)
    assert runs_db.store_run_metrics.await_args.kwargs["expected_through"] == through


async def test_a_closed_run_reads_up_to_its_end_and_is_final(runs_db):
    ended = T0 + timedelta(hours=1)
    provider = _provider()
    await sweep.fold_run(_run(ended_at=ended), provider)

    provider.metrics_since.assert_awaited_once_with("sb-1", T0, until=ended)
    assert runs_db.store_run_metrics.await_args.kwargs["final"] is True


@pytest.mark.parametrize("closed", [False, True])
async def test_a_failed_read_records_the_error_and_keeps_the_aggregates(
    runs_db, closed
):
    # Relative to now: a run closed over a day ago gives up instead.
    now = datetime.now(timezone.utc)
    ended_at = now - timedelta(hours=1) if closed else None
    provider = _provider(error=RuntimeError("telemetry unavailable"))

    run = _run(started_at=now - timedelta(hours=2), ended_at=ended_at)
    assert await sweep.fold_run(run, provider) is False

    runs_db.store_run_metrics.assert_not_called()
    runs_db.store_run_metrics_error.assert_awaited_once_with(
        "run-1", "RuntimeError: telemetry unavailable", final=False,
        ended_at=ended_at,
    )


async def test_a_closed_run_is_read_again_after_a_transient_failure(runs_db):
    recent = datetime.now(timezone.utc) - timedelta(hours=1)
    provider = _provider(error=RuntimeError("telemetry unavailable"))

    await sweep.fold_run(_run(ended_at=recent), provider)

    assert runs_db.store_run_metrics_error.await_args.kwargs["final"] is False


async def test_a_closed_run_whose_sandbox_is_gone_is_final(runs_db):
    recent = datetime.now(timezone.utc) - timedelta(hours=1)
    provider = _provider(
        error=RuntimeError("not found"), kind=SandboxFailureKind.SANDBOX_GONE
    )

    assert await sweep.fold_run(_run(ended_at=recent), provider) is False

    assert runs_db.store_run_metrics_error.await_args.kwargs["final"] is True


async def test_a_closed_run_that_names_no_sandbox_is_final(runs_db):
    recent = datetime.now(timezone.utc) - timedelta(hours=1)
    provider = _provider()

    await sweep.fold_run(_run(ended_at=recent, provider_sandbox_id=None), provider)

    provider.metrics_since.assert_not_called()
    assert runs_db.store_run_metrics_error.await_args.kwargs["final"] is True


async def test_a_closed_run_whose_read_fails_to_parse_is_read_again(runs_db):
    recent = datetime.now(timezone.utc) - timedelta(hours=1)
    provider = _provider(error=ValueError("Invalid JSON"))

    await sweep.fold_run(_run(ended_at=recent), provider)

    assert runs_db.store_run_metrics_error.await_args.kwargs["final"] is False


async def test_a_run_closed_past_the_give_up_window_is_final_on_any_error(runs_db):
    long_ago = datetime.now(timezone.utc) - sweep.FINAL_GIVE_UP - timedelta(hours=1)
    provider = _provider(error=RuntimeError("telemetry unavailable"))

    await sweep.fold_run(
        _run(started_at=long_ago - timedelta(hours=1), ended_at=long_ago), provider
    )

    assert runs_db.store_run_metrics_error.await_args.kwargs["final"] is True


async def test_a_closed_runs_rate_limit_refusal_is_not_final(runs_db):
    recent = datetime.now(timezone.utc) - timedelta(hours=1)
    provider = _provider(
        error=SandboxRateLimitedError("slow down"),
        kind=SandboxFailureKind.SANDBOX_GONE,
    )

    with pytest.raises(SandboxRateLimitedError):
        await sweep.fold_run(_run(ended_at=recent), provider)

    assert runs_db.store_run_metrics_error.await_args.kwargs["final"] is False


async def test_a_closed_runs_timeout_is_not_final(runs_db):
    recent = datetime.now(timezone.utc) - timedelta(hours=1)
    provider = _provider(
        error=TimeoutError("read stalled"), kind=SandboxFailureKind.SANDBOX_GONE
    )

    assert await sweep.fold_run(_run(ended_at=recent), provider) is False

    assert runs_db.store_run_metrics_error.await_args.kwargs["final"] is False


async def test_an_open_run_is_never_final_on_error(runs_db):
    provider = _provider(
        error=RuntimeError("not found"), kind=SandboxFailureKind.SANDBOX_GONE
    )

    await sweep.fold_run(_run(), provider)

    assert runs_db.store_run_metrics_error.await_args.kwargs["final"] is False


async def test_a_rate_limit_refusal_is_recorded_then_re_raised(runs_db):
    provider = _provider(error=SandboxRateLimitedError("slow down"))

    with pytest.raises(SandboxRateLimitedError):
        await sweep.fold_run(_run(), provider)

    runs_db.store_run_metrics.assert_not_called()
    runs_db.store_run_metrics_error.assert_awaited_once_with(
        "run-1", "SandboxRateLimitedError: slow down", final=False, ended_at=None
    )


async def test_a_slow_provider_times_out_rather_than_holding_the_sweep(runs_db):
    async def hang(*args, **kwargs):
        await asyncio.sleep(60)

    provider = MagicMock()
    provider.metrics_since = hang
    with patch.object(sweep, "FETCH_TIMEOUT_SECONDS", 0.01):
        assert await sweep.fold_run(_run(), provider) is False
    error = runs_db.store_run_metrics_error.await_args.args[1]
    assert error.startswith("TimeoutError")


async def test_a_kinds_reads_are_one_at_a_time_and_one_failing_run_spoils_no_other(
    runs_db,
):
    runs_db.claimable = [_run(run_id=f"run-{i}") for i in range(20)]
    in_flight = 0
    peak = 0

    async def read(sandbox_id, since, until=None):
        nonlocal in_flight, peak
        in_flight += 1
        peak = max(peak, in_flight)
        await asyncio.sleep(0.001)
        in_flight -= 1
        return []

    good = MagicMock()
    good.metrics_since = read

    def provider_for(run):
        if run["run_id"] == "run-3":
            raise ValueError("unreadable provider settings")
        return good

    with patch.object(sweep, "READ_INTERVAL_SECONDS", {"daytona": 0}):
        done = await sweep.collect_metrics(provider_for)

    assert done == 19
    assert peak == 1
    claimed_kinds = [
        call.kwargs["kinds"] for call in runs_db.claim_runs_for_metrics.await_args_list
    ]
    assert sorted(claimed_kinds) == [("daytona",), ("e2b",)]


async def test_a_refused_kind_backs_off_then_gives_up_its_cycle_alone(runs_db):
    runs_db.claimable = [
        _run(run_id=f"d-{i}", provider_kind="daytona") for i in range(5)
    ] + [_run(run_id="e-1", provider_kind="e2b")]
    starts: list[float] = []

    async def refuse(sandbox_id, since, until=None):
        starts.append(time.monotonic())
        raise SandboxRateLimitedError("slow down")

    daytona = MagicMock()
    daytona.metrics_since = refuse
    daytona.classify_error = MagicMock(return_value=SandboxFailureKind.UNKNOWN)
    e2b = _provider()

    with (
        patch.object(sweep, "READ_INTERVAL_SECONDS", {"daytona": 0.05}),
        patch.object(sweep, "DEFAULT_READ_INTERVAL_SECONDS", 0),
    ):
        done = await sweep.collect_metrics(
            lambda run: daytona if run["provider_kind"] == "daytona" else e2b
        )

    assert done == 1
    assert len(starts) == sweep.MAX_REFUSALS
    gaps = [b - a for a, b in zip(starts, starts[1:])]
    assert all(gap >= 0.05 * sweep.REFUSAL_BACKOFF_INTERVALS - 0.01 for gap in gaps)
    e2b.metrics_since.assert_awaited_once()


async def test_a_read_after_a_refusal_that_succeeds_resets_the_count(runs_db):
    runs_db.claimable = [_run(run_id=f"d-{i}") for i in range(5)]
    answers = iter(
        [
            SandboxRateLimitedError("slow down"),
            [],
            SandboxRateLimitedError("slow down"),
            [],
            [],
        ]
    )

    async def read(sandbox_id, since, until=None):
        answer = next(answers)
        if isinstance(answer, Exception):
            raise answer
        return answer

    provider = MagicMock()
    provider.metrics_since = read
    provider.classify_error = MagicMock(return_value=SandboxFailureKind.UNKNOWN)
    with patch.object(sweep, "READ_INTERVAL_SECONDS", {"daytona": 0.01}):
        assert await sweep.collect_metrics(lambda run: provider) == 3


async def test_a_kind_claims_only_the_runs_its_cycle_can_read(runs_db):
    with patch.object(sweep, "READ_INTERVAL_SECONDS", {"daytona": 15.0}):
        await sweep.collect_metrics(lambda run: _provider())

    limits = {
        call.kwargs["kinds"]: call.kwargs["limit"]
        for call in runs_db.claim_runs_for_metrics.await_args_list
    }
    assert limits[("daytona",)] == int(sweep.CYCLE_BUDGET_SECONDS // 15) + 1
    assert limits[("e2b",)] == sweep.CLAIM_LIMIT


async def test_a_cycle_starts_no_read_past_its_budget(runs_db):
    runs_db.claimable = [_run(run_id=f"d-{i}") for i in range(10)]
    provider = _provider()
    with (
        # Reads start at 0, 0.2 and 0.4; the fourth would start past 0.5,
        # with a tenth of a second to spare either way for a loaded machine.
        patch.object(sweep, "READ_INTERVAL_SECONDS", {"daytona": 0.2}),
        patch.object(sweep, "CYCLE_BUDGET_SECONDS", 0.5),
        patch.object(sweep, "CLAIM_LIMIT", 10),
    ):
        await sweep.collect_metrics(lambda run: provider)

    assert provider.metrics_since.await_count == 3


async def _read_starts(runs_db, computer_id):
    runs_db.claimable = [_run(run_id=f"run-{i}") for i in range(3)]
    starts: list[float] = []

    async def read(sandbox_id, since, until=None):
        starts.append(time.monotonic())
        return []

    provider = MagicMock()
    provider.metrics_since = read
    with patch.object(sweep, "READ_INTERVAL_SECONDS", {"daytona": 0.05}):
        assert (
            await sweep.collect_metrics(lambda run: provider, computer_id=computer_id)
            == 3
        )
    return [b - a for a, b in zip(starts, starts[1:])]


async def test_the_periodic_sweep_spaces_the_start_of_each_read(runs_db):
    gaps = await _read_starts(runs_db, None)

    assert all(gap >= 0.045 for gap in gaps)


async def test_a_stop_time_fold_of_a_named_computer_is_not_spaced(runs_db):
    gaps = await _read_starts(runs_db, "c-1")

    assert all(gap < 0.04 for gap in gaps)


async def test_a_failed_claim_ends_the_pass_quietly(runs_db):
    runs_db.claim_runs_for_metrics.side_effect = RuntimeError("pool closed")
    assert await sweep.collect_metrics(lambda run: _provider()) == 0


async def test_reconcile_opens_through_the_same_hook_as_a_status_write(runs_db):
    computer = {"computer_id": "c-9", "kind": "daytona"}
    runs_db.close_orphaned_runs.return_value = 2
    runs_db.get_running_computers_without_run.return_value = [computer]

    with patch.object(sweep, "note_computer_status", AsyncMock()) as hook:
        assert await sweep.reconcile_runs() == (2, 1)

    hook.assert_awaited_once_with(computer, "running")
    kinds = runs_db.get_running_computers_without_run.await_args.args[0]
    assert "ssh" not in kinds and "daytona" in kinds


async def test_a_failed_reconcile_still_collects(runs_db):
    runs_db.close_orphaned_runs.side_effect = RuntimeError("down")
    with (
        patch.object(sweep, "try_own_session_lock", _lock(True)),
        patch.object(sweep, "collect_metrics", AsyncMock()) as collect,
    ):
        await sweep.run_usage_sweep(lambda run: _provider())
    collect.assert_awaited_once()


async def test_a_sweep_collects_nothing_while_another_worker_holds_the_lock(runs_db):
    with (
        patch.object(sweep, "try_own_session_lock", _lock(False)),
        patch.object(sweep, "collect_metrics", AsyncMock()) as collect,
    ):
        await sweep.run_usage_sweep(lambda run: _provider())

    runs_db.close_orphaned_runs.assert_awaited_once()
    collect.assert_not_called()


async def test_a_failing_collector_lock_is_logged_not_raised(runs_db):
    @asynccontextmanager
    async def broken(key):
        raise RuntimeError("pool closed")
        yield

    with (
        patch.object(sweep, "try_own_session_lock", broken),
        patch.object(sweep, "collect_metrics", AsyncMock()) as collect,
    ):
        await sweep.run_usage_sweep(lambda run: _provider())

    collect.assert_not_called()


async def test_the_delete_time_read_is_one_unspaced_read_of_that_computer(runs_db):
    runs_db.claimable = [_run()]
    provider = _provider()

    await sweep.fold_before_delete(lambda run: provider, "c-1")

    claim = runs_db.claim_runs_for_metrics.await_args.kwargs
    assert claim["computer_id"] == "c-1"
    provider.metrics_since.assert_awaited_once_with("sb-1", T0, until=None)


async def test_a_hanging_delete_time_read_gives_up_without_raising(runs_db):
    async def hang(*args, **kwargs):
        await asyncio.sleep(60)
        return []

    runs_db.claim_runs_for_metrics.side_effect = hang
    with patch.object(sweep, "DELETE_FOLD_TIMEOUT_SECONDS", 0.01):
        await sweep.fold_before_delete(lambda run: _provider(), "c-1")


async def test_a_delete_waits_out_a_whole_provider_read():
    # Cut short, the delete-time read is lost with the sandbox's history.
    assert sweep.DELETE_FOLD_TIMEOUT_SECONDS > sweep.FETCH_TIMEOUT_SECONDS


async def test_the_sweep_reads_an_open_run_only_up_to_the_settle_window(runs_db):
    provider = _provider()
    start = datetime.now(timezone.utc) - timedelta(hours=1)

    await sweep.fold_run(_run(started_at=start), provider, settle=timedelta(minutes=2))

    until = provider.metrics_since.await_args.kwargs["until"]
    assert until <= datetime.now(timezone.utc) - timedelta(minutes=2)


async def test_an_open_run_with_nothing_settled_yet_is_not_read(runs_db):
    provider = _provider()
    just_now = datetime.now(timezone.utc)

    assert (
        await sweep.fold_run(
            _run(started_at=just_now), provider, settle=timedelta(minutes=2)
        )
        is True
    )
    provider.metrics_since.assert_not_called()


async def test_a_closed_run_read_through_its_end_is_final_without_a_read(runs_db):
    ended = T0 + timedelta(minutes=10)
    provider = _provider()

    await sweep.fold_run(
        _run(ended_at=ended, metrics_through=ended, sample_count=3), provider
    )

    provider.metrics_since.assert_not_called()
    assert runs_db.store_run_metrics.await_args.kwargs["final"] is True


async def test_a_provider_that_cannot_be_built_is_recorded(runs_db):
    runs_db.claimable = [_run()]

    def provider_for(run):
        raise ValueError("unreadable provider settings")

    with patch.object(sweep, "READ_INTERVAL_SECONDS", {"daytona": 0}):
        assert await sweep.collect_metrics(provider_for) == 0
    runs_db.store_run_metrics_error.assert_awaited_once()


async def test_a_delete_time_read_that_lost_the_cursor_reads_on_from_the_winner(
    runs_db,
):
    winner_through = T0 + timedelta(minutes=8)
    runs_db.store_run_metrics.side_effect = [False, True]
    runs_db.get_computer_run = AsyncMock(
        return_value={
            "run_id": "run-1",
            "metrics_through": winner_through,
            "sample_count": 96,
            "metrics_final": False,
        }
    )
    late = MetricSample(
        timestamp=winner_through + timedelta(minutes=1), cpu_used_pct=9.0
    )
    provider = _provider([late])

    assert await sweep.fold_run(_run(), provider) is True

    first, second = provider.metrics_since.await_args_list
    assert first.args[1] == T0 and second.args[1] == winner_through
    assert (
        runs_db.store_run_metrics.await_args.kwargs["expected_through"]
        == winner_through
    )


async def test_a_sweep_read_that_lost_the_cursor_leaves_it_to_the_winner(runs_db):
    runs_db.store_run_metrics.return_value = False
    runs_db.get_computer_run = AsyncMock()
    provider = _provider()
    start = datetime.now(timezone.utc) - timedelta(hours=1)

    assert (
        await sweep.fold_run(
            _run(started_at=start), provider, settle=timedelta(minutes=2)
        )
        is True
    )

    runs_db.get_computer_run.assert_not_called()
    provider.metrics_since.assert_awaited_once()


def _due(sandbox_id="sb-1", kind="daytona"):
    return {
        "provider_kind": kind,
        "provider_sandbox_id": sandbox_id,
        "computer_id": "c-1",
        "provider_config": None,
        "root_dir": None,
    }


def _allocating(answer=None, error=None, kind=SandboxFailureKind.UNKNOWN):
    provider = MagicMock()
    provider.allocation = AsyncMock(return_value=answer, side_effect=error)
    provider.classify_error = MagicMock(return_value=kind)
    return provider


async def test_a_providers_answer_is_written_over_the_sandboxs_runs(runs_db):
    runs_db.get_sandboxes_due_allocation.return_value = [_due()]
    answer = SandboxAllocation(
        state=RuntimeState.STOPPED, cpu=1.0, memory_gb=1.0, disk_gb=3.0,
        archive_after_s=604800,
    )
    provider = _allocating(answer)

    assert await sweep.check_allocations(lambda row: provider) == 1

    provider.allocation.assert_awaited_once_with("sb-1")
    runs_db.store_allocation.assert_awaited_once_with(
        "daytona", "sb-1", answer, asked_at=ANY
    )


async def test_a_sandbox_the_provider_no_longer_has_is_stored_as_gone(runs_db):
    runs_db.get_sandboxes_due_allocation.return_value = [_due()]
    provider = _allocating(
        error=RuntimeError("404"), kind=SandboxFailureKind.SANDBOX_GONE
    )
    provider.default_archive_after_s = 604800

    assert await sweep.check_allocations(lambda row: provider) == 1

    runs_db.store_sandbox_gone.assert_awaited_once_with(
        "daytona", "sb-1", archive_after_s=604800, asked_at=ANY
    )
    runs_db.store_allocation.assert_not_called()


async def test_a_failed_lookup_stores_nothing_and_spoils_no_other(runs_db):
    runs_db.get_sandboxes_due_allocation.return_value = [_due("sb-1"), _due("sb-2")]
    answer = SandboxAllocation(state=RuntimeState.RUNNING, cpu=2.0, memory_gb=4.0)

    def provider_for(row):
        if row["provider_sandbox_id"] == "sb-1":
            return _allocating(error=RuntimeError("503"))
        return _allocating(answer)

    assert await sweep.check_allocations(provider_for) == 1

    [call] = runs_db.store_allocation.await_args_list
    assert call.args == ("daytona", "sb-2", answer)
    # Stamped, so a sandbox that always fails waits its turn behind the rest.
    runs_db.mark_allocation_checked.assert_awaited_once_with("daytona", "sb-1")


async def test_a_slow_lookup_times_out_rather_than_holding_the_sweep(runs_db):
    runs_db.get_sandboxes_due_allocation.return_value = [_due()]

    async def hang(sandbox_id):
        await asyncio.sleep(60)

    provider = _allocating()
    provider.allocation = hang
    with patch.object(sweep, "FETCH_TIMEOUT_SECONDS", 0.01):
        assert await sweep.check_allocations(lambda row: provider) == 0

    runs_db.store_allocation.assert_not_called()


async def test_lookups_that_hang_start_none_past_the_cycle_budget(runs_db):
    runs_db.get_sandboxes_due_allocation.return_value = [
        _due(f"sb-{i}") for i in range(5)
    ]
    asked = []

    async def hang(sandbox_id):
        asked.append(sandbox_id)
        await asyncio.sleep(0.3)
        raise TimeoutError

    provider = _allocating()
    provider.allocation = hang
    # One at a time: lookups start at 0 and 0.3; the next would at 0.6, past
    # the 0.45 budget, and the rest wait for the next cycle unstamped.
    with (
        patch.object(sweep, "ALLOCATION_CONCURRENCY", 1),
        patch.object(sweep, "CYCLE_BUDGET_SECONDS", 0.45),
    ):
        assert await sweep.check_allocations(lambda row: provider) == 0

    assert asked == ["sb-0", "sb-1"]
    assert runs_db.mark_allocation_checked.await_count == 2


async def test_the_lock_holder_looks_up_allocations_beside_the_metrics(runs_db):
    with (
        patch.object(sweep, "try_own_session_lock", _lock(True)),
        patch.object(sweep, "collect_metrics", AsyncMock()) as collect,
        patch.object(sweep, "check_allocations", AsyncMock()) as check,
    ):
        await sweep.run_usage_sweep(lambda run: _provider())

    collect.assert_awaited_once()
    check.assert_awaited_once()
    runs_db.settle_holds.assert_awaited_once()


async def test_holds_are_settled_even_when_reconcile_fails(runs_db):
    runs_db.close_orphaned_runs.side_effect = RuntimeError("down")
    with patch.object(sweep, "try_own_session_lock", _lock(False)):
        await sweep.run_usage_sweep(lambda run: _provider())

    runs_db.settle_holds.assert_awaited_once()


async def test_a_slow_read_starts_no_read_past_the_budget(runs_db):
    runs_db.claimable = [_run(run_id=f"d-{i}") for i in range(5)]
    reads = 0

    async def slow(sandbox_id, since, until=None):
        nonlocal reads
        reads += 1
        await asyncio.sleep(0.4)
        return []

    provider = MagicMock()
    provider.metrics_since = slow
    # Reads start at 0 and 0.4 (the second waits out the first); the third
    # is due at 0.5 by its slot but the clock reads 0.8, past the 0.6 budget.
    with (
        patch.object(sweep, "READ_INTERVAL_SECONDS", {"daytona": 0.1}),
        patch.object(sweep, "CYCLE_BUDGET_SECONDS", 0.6),
        patch.object(sweep, "CLAIM_LIMIT", 5),
    ):
        await sweep.collect_metrics(lambda run: provider)

    assert reads == 2
