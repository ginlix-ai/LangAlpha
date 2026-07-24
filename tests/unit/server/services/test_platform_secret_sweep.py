"""Platform-secret migration sweep: lock/skip/scrub dispatch tests."""

from __future__ import annotations

import hashlib
from contextlib import asynccontextmanager
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.server.services.platform_secret_rollout import (
    PlatformSecretRollout,
    PlatformSecretRolloutSet,
)
from src.server.services.platform_secret_sweep import PlatformSecretSweeper


PLACEHOLDER = "dtn_secret_opaque"

_SWEEP = "src.server.services.platform_secret_sweep"
_ROLLOUT = "src.server.services.platform_secret_rollout"
_MECHANICS = "ptc_agent.core.sandbox.platform_secrets"


def _rollout_set(generation: int = 1) -> PlatformSecretRolloutSet:
    rollout = PlatformSecretRollout(
        secret_key="FMP_API_KEY",
        provider="daytona",
        secret_name="prod-platform-fmp-api-key",
        provider_secret_id="secret-id",
        placeholder_sha256=hashlib.sha256(PLACEHOLDER.encode()).hexdigest(),
        current_credential_sha256="current",
        generation=generation,
    )
    return PlatformSecretRolloutSet(rollouts=(rollout,), generation=generation)


def _row(*, version: int = 0, always_on: bool = False) -> dict:
    return {
        "workspace_id": "ws",
        "sandbox_id": "sb",
        "platform_secret_version": version,
        "is_always_on": always_on,
    }


def _runtime(*, autostop: bool = True) -> MagicMock:
    runtime = MagicMock()
    runtime.capabilities = {"autostop"} if autostop else set()
    runtime.set_autostop_interval = AsyncMock()
    return runtime


def _executor(*, busy: bool) -> MagicMock:
    executor = MagicMock()
    executor.has_active_tasks_for_workspace = AsyncMock(return_value=busy)
    return executor


@pytest.fixture(autouse=True)
def _fresh_sweeper():
    PlatformSecretSweeper.reset_instance()
    yield
    PlatformSecretSweeper.reset_instance()


@asynccontextmanager
async def _fake_conn(acquired: bool):
    cur = MagicMock()
    cur.fetchone = AsyncMock(return_value=(acquired,))
    conn = MagicMock()
    conn.execute = AsyncMock(return_value=cur)
    yield conn


@pytest.mark.asyncio
async def test_legacy_row_gets_scrub_restart_and_always_on_reassert():
    sweeper = PlatformSecretSweeper()
    rollout_set = _rollout_set()
    runtime = _runtime()
    provider = MagicMock()
    provider.get = AsyncMock(return_value=runtime)
    converge = AsyncMock()
    stamp = AsyncMock()
    drop = AsyncMock()

    with (
        patch(f"{_MECHANICS}.converge_sandbox_platform_secrets", converge),
        patch(f"{_ROLLOUT}.stamp_workspace_platform_secret_version", stamp),
        patch(
            "src.server.services.runs.executor.LocalRunExecutor.get_instance",
            return_value=_executor(busy=False),
        ),
        patch.object(sweeper, "_drop_local_session", drop),
    ):
        converged = await sweeper._converge_locked(
            "ws", "sb", _row(always_on=True), rollout_set, provider
        )

    assert converged is True
    converge.assert_awaited_once_with(
        runtime,
        expected=rollout_set.placeholders,
        bindings=rollout_set.bindings,
    )
    runtime.set_autostop_interval.assert_awaited_once_with(0)
    stamp.assert_awaited_once_with(
        "ws", expected_sandbox_id="sb", rollout_set=rollout_set
    )
    # The restart killed the cached session's exec processes.
    drop.assert_awaited_once_with("ws")


@pytest.mark.asyncio
async def test_active_turn_defers_the_scrub_to_the_next_cycle():
    sweeper = PlatformSecretSweeper()
    provider = MagicMock()
    provider.get = AsyncMock()
    converge = AsyncMock()

    with (
        patch(f"{_MECHANICS}.converge_sandbox_platform_secrets", converge),
        patch(
            "src.server.services.runs.executor.LocalRunExecutor.get_instance",
            return_value=_executor(busy=True),
        ),
    ):
        converged = await sweeper._converge_locked(
            "ws", "sb", _row(), _rollout_set(), provider
        )

    assert converged is False
    provider.get.assert_not_awaited()
    converge.assert_not_awaited()
    assert sweeper._busy_skips["ws"] == 1


@pytest.mark.asyncio
async def test_certified_behind_row_hot_swaps_without_restart():
    # 0 < version < generation: placeholders throughout — hot remount + verify,
    # never a scrub (and no busy gate: nothing destructive happens).
    sweeper = PlatformSecretSweeper()
    rollout_set = _rollout_set(generation=2)
    runtime = _runtime()
    provider = MagicMock()
    provider.get = AsyncMock(return_value=runtime)
    converge = AsyncMock()
    remount = AsyncMock()
    verify = AsyncMock()
    stamp = AsyncMock()
    drop = AsyncMock()

    with (
        patch(f"{_MECHANICS}.converge_sandbox_platform_secrets", converge),
        patch(f"{_MECHANICS}.remount_platform_secret_bindings", remount),
        patch(f"{_MECHANICS}.verify_runtime_platform_secrets", verify),
        patch(f"{_ROLLOUT}.stamp_workspace_platform_secret_version", stamp),
        patch.object(sweeper, "_drop_local_session", drop),
    ):
        converged = await sweeper._converge_locked(
            "ws", "sb", _row(version=1), rollout_set, provider
        )

    assert converged is True
    converge.assert_not_awaited()
    remount.assert_awaited_once_with(
        runtime,
        expected=rollout_set.placeholders,
        bindings=rollout_set.bindings,
    )
    verify.assert_awaited_once_with(runtime, expected=rollout_set.placeholders)
    stamp.assert_awaited_once()
    drop.assert_not_awaited()


@pytest.mark.asyncio
async def test_convergence_failure_is_contained_and_retried_next_pass():
    sweeper = PlatformSecretSweeper()
    provider = MagicMock()
    provider.get = AsyncMock(side_effect=RuntimeError("sandbox gone"))
    stamp = AsyncMock()

    with (
        patch(f"{_ROLLOUT}.stamp_workspace_platform_secret_version", stamp),
        patch(
            "src.server.services.runs.executor.LocalRunExecutor.get_instance",
            return_value=_executor(busy=False),
        ),
    ):
        converged = await sweeper._converge_locked(
            "ws", "sb", _row(), _rollout_set(), provider
        )

    assert converged is False
    stamp.assert_not_awaited()


@pytest.mark.asyncio
async def test_lock_loser_skips_without_touching_the_workspace():
    sweeper = PlatformSecretSweeper()
    provider = MagicMock()
    provider.get = AsyncMock()
    locked = AsyncMock()

    with (
        patch(
            "src.server.database.pool.get_db_connection",
            lambda: _fake_conn(acquired=False),
        ),
        patch.object(sweeper, "_converge_locked", locked),
    ):
        converged = await sweeper._sweep_one(_row(), _rollout_set(), provider)

    assert converged is False
    locked.assert_not_awaited()


@pytest.mark.asyncio
async def test_lock_winner_converges_and_releases():
    sweeper = PlatformSecretSweeper()
    provider = MagicMock()
    locked = AsyncMock(return_value=True)
    executed: list[str] = []

    @asynccontextmanager
    async def _conn_cm():
        cur = MagicMock()
        cur.fetchone = AsyncMock(return_value=(True,))

        async def _execute(sql, params=None):
            executed.append(sql)
            return cur

        conn = MagicMock()
        conn.execute = AsyncMock(side_effect=_execute)
        yield conn

    with (
        patch("src.server.database.pool.get_db_connection", _conn_cm),
        patch.object(sweeper, "_converge_locked", locked),
    ):
        converged = await sweeper._sweep_one(_row(), _rollout_set(), provider)

    assert converged is True
    locked.assert_awaited_once()
    assert any("pg_try_advisory_lock" in sql for sql in executed)
    assert any("pg_advisory_unlock" in sql for sql in executed)


@pytest.mark.asyncio
async def test_sweep_once_is_inert_without_a_registered_rollout():
    sweeper = PlatformSecretSweeper()
    sweeper._config = MagicMock()
    list_behind = AsyncMock()

    with (
        patch(
            f"{_ROLLOUT}.get_platform_secret_rollouts",
            AsyncMock(side_effect=RuntimeError("not initialized")),
        ),
        patch(f"{_ROLLOUT}.list_workspaces_behind_platform_secret", list_behind),
    ):
        assert await sweeper.sweep_once() == 0

    list_behind.assert_not_awaited()


@pytest.mark.asyncio
async def test_start_is_inert_when_platform_secrets_inactive():
    sweeper = PlatformSecretSweeper()
    with patch(
        f"{_MECHANICS}.platform_secrets_active", return_value=False
    ):
        sweeper.start(MagicMock())
    assert sweeper._loop_task is None


@pytest.mark.asyncio
async def test_start_and_stop_lifecycle():
    sweeper = PlatformSecretSweeper(interval=0.01)
    with (
        patch(f"{_MECHANICS}.platform_secrets_active", return_value=True),
        patch.object(sweeper, "sweep_once", AsyncMock(return_value=0)),
    ):
        sweeper.start(MagicMock())
        assert sweeper._loop_task is not None
        await sweeper.stop()
    assert sweeper._loop_task is None
