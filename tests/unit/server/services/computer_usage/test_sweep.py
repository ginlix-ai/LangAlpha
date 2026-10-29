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
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from ptc_agent.core.sandbox.runtime import (
    MetricSample,
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
        db.claim_runs_for_metrics = AsyncMock(return_value=[])
        db.close_orphaned_runs = AsyncMock(return_value=0)
        db.get_running_computers_without_run = AsyncMock(return_value=[])
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
        "run-1", "RuntimeError: telemetry unavailable", final=False
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
        "run-1", "SandboxRateLimitedError: slow down", final=False
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


async def test_collection_is_bounded_and_one_failing_run_spoils_no_other(runs_db):
    runs_db.claim_runs_for_metrics.return_value = [
        _run(run_id=f"run-{i}") for i in range(20)
    ]
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

    with (
        patch.object(sweep, "CONCURRENCY", 4),
        patch.object(sweep, "READ_INTERVAL_SECONDS", 0),
    ):
        done = await sweep.collect_metrics(provider_for)

    assert done == 19
    assert peak <= 4
    assert runs_db.claim_runs_for_metrics.await_args.kwargs["kinds"] == ("daytona", "e2b")


async def test_a_throttled_kind_is_not_read_again_but_another_kind_is(runs_db):
    runs_db.claim_runs_for_metrics.return_value = [
        _run(run_id="d-1", provider_kind="daytona"),
        _run(run_id="d-2", provider_kind="daytona"),
        _run(run_id="d-3", provider_kind="daytona"),
        _run(run_id="e-1", provider_kind="e2b"),
    ]
    daytona = _provider(error=SandboxRateLimitedError("slow down"))
    e2b = _provider()

    with patch.object(sweep, "READ_INTERVAL_SECONDS", 0.05):
        done = await sweep.collect_metrics(
            lambda run: daytona if run["provider_kind"] == "daytona" else e2b
        )

    assert done == 1
    daytona.metrics_since.assert_awaited_once()
    e2b.metrics_since.assert_awaited_once()


async def _read_starts(runs_db, computer_id):
    runs_db.claim_runs_for_metrics.return_value = [
        _run(run_id=f"run-{i}") for i in range(3)
    ]
    starts: list[float] = []

    async def read(sandbox_id, since, until=None):
        starts.append(time.monotonic())
        return []

    provider = MagicMock()
    provider.metrics_since = read
    with patch.object(sweep, "READ_INTERVAL_SECONDS", 0.05):
        assert await sweep.collect_metrics(
            lambda run: provider, computer_id=computer_id
        ) == 3
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
    runs_db.claim_runs_for_metrics.return_value = [_run()]
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

    assert await sweep.fold_run(
        _run(started_at=just_now), provider, settle=timedelta(minutes=2)
    ) is True
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
    runs_db.claim_runs_for_metrics.return_value = [_run()]

    def provider_for(run):
        raise ValueError("unreadable provider settings")

    with patch.object(sweep, "READ_INTERVAL_SECONDS", 0):
        assert await sweep.collect_metrics(provider_for) == 0
    runs_db.store_run_metrics_error.assert_awaited_once()


async def test_a_delete_time_read_that_lost_the_cursor_reads_on_from_the_winner(runs_db):
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
    late = MetricSample(timestamp=winner_through + timedelta(minutes=1), cpu_used_pct=9.0)
    provider = _provider([late])

    assert await sweep.fold_run(_run(), provider) is True

    first, second = provider.metrics_since.await_args_list
    assert first.args[1] == T0 and second.args[1] == winner_through
    assert runs_db.store_run_metrics.await_args.kwargs["expected_through"] == winner_through


async def test_a_sweep_read_that_lost_the_cursor_leaves_it_to_the_winner(runs_db):
    runs_db.store_run_metrics.return_value = False
    runs_db.get_computer_run = AsyncMock()
    provider = _provider()
    start = datetime.now(timezone.utc) - timedelta(hours=1)

    assert await sweep.fold_run(
        _run(started_at=start), provider, settle=timedelta(minutes=2)
    ) is True

    runs_db.get_computer_run.assert_not_called()
    provider.metrics_since.assert_awaited_once()
