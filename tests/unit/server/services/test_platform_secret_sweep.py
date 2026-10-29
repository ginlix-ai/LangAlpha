"""Platform-secret migration sweep: lock/skip/scrub dispatch tests."""

from __future__ import annotations

import hashlib
from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.server.services.platform_secret_rollout import (
    PlatformSecretRollout,
    PlatformSecretRolloutSet,
)
from src.server.services.platform_secret_sweep import PlatformSecretSweeper


PLACEHOLDER = "dtn_secret_opaque"
COMPUTER_ID = "11111111-1111-4111-8111-111111111111"

_SWEEP = "src.server.services.platform_secret_sweep"
_ROLLOUT = "src.server.services.platform_secret_rollout"
_MECHANICS = "ptc_agent.core.sandbox.platform_secrets"
_LIVEFS = "src.server.services.livefs.mount"


def _rollout_set(
    generation: int = 1, *, provider: str = "daytona"
) -> PlatformSecretRolloutSet:
    rollout = PlatformSecretRollout(
        secret_key="FMP_API_KEY",
        provider=provider,
        secret_name="prod-platform-fmp-api-key",
        provider_secret_id="secret-id",
        placeholder_sha256=hashlib.sha256(PLACEHOLDER.encode()).hexdigest(),
        current_credential_sha256="current",
        generation=generation,
    )
    return PlatformSecretRolloutSet(
        provider=provider, rollouts=(rollout,), generation=generation
    )


def _sweep_config(*, e2b_key: str = "e2b-key", catalog: bool = True) -> SimpleNamespace:
    """What the sweep reads off the deployment config: the catalog and keys."""
    sandbox = SimpleNamespace(
        provider="daytona",
        platform_secrets=("FMP_API_KEY",) if catalog else (),
        daytona=SimpleNamespace(api_key="daytona-key"),
        e2b=SimpleNamespace(api_key=e2b_key),
        docker=SimpleNamespace(),
    )
    sandbox.settings_for = lambda kind: getattr(sandbox, kind)
    return SimpleNamespace(sandbox=sandbox)


def _row(
    *,
    version: int = 0,
    always_on: bool = False,
    workspace_id: str | None = "ws",
    kind: str = "daytona",
) -> dict:
    # One row per computer, as list_workspaces_behind_platform_secret selects;
    # the project is the machine's oldest live one, or None when it has none.
    return {
        "computer_id": COMPUTER_ID,
        "workspace_id": workspace_id,
        "sandbox_id": "sb",
        "kind": kind,
        "platform_secret_version": version,
        "is_always_on": always_on,
    }


def _runtime(*, autostop: bool = True) -> MagicMock:
    runtime = MagicMock()
    runtime.autostops = autostop
    runtime.set_autostop_interval = AsyncMock()
    return runtime


def _executor(*, busy: bool) -> MagicMock:
    executor = MagicMock()
    executor.has_active_tasks_for_computer = AsyncMock(return_value=busy)
    return executor


@pytest.fixture(autouse=True)
def _fresh_sweeper():
    PlatformSecretSweeper.reset_instance()
    yield
    PlatformSecretSweeper.reset_instance()


@pytest.fixture(autouse=True)
def mount_restarted():
    """The file mount's record of the scrub's restart."""
    with patch(f"{_LIVEFS}.restarted", AsyncMock()) as restarted:
        yield restarted


@asynccontextmanager
async def _fake_conn(acquired: bool):
    cur = MagicMock()
    cur.fetchone = AsyncMock(return_value=(acquired,))
    conn = MagicMock()
    conn.execute = AsyncMock(return_value=cur)
    yield conn


@pytest.mark.asyncio
async def test_legacy_row_gets_scrub_restart_and_always_on_reassert(mount_restarted):
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
        patch(f"{_ROLLOUT}.stamp_platform_secret_version", stamp),
        patch(
            "src.server.services.runs.executor.LocalRunExecutor.get_instance",
            return_value=_executor(busy=False),
        ),
        patch.object(sweeper, "_drop_local_session", drop),
    ):
        converged = await sweeper._converge_locked(
            COMPUTER_ID, "sb", _row(always_on=True), rollout_set, provider
        )

    assert converged is True
    converge.assert_awaited_once_with(
        runtime,
        expected=rollout_set.placeholders,
        bindings=rollout_set.bindings,
    )
    runtime.set_autostop_interval.assert_awaited_once_with(0)
    stamp.assert_awaited_once_with(
        computer_id=COMPUTER_ID, expected_sandbox_id="sb", rollout_set=rollout_set
    )
    # The restart killed the cached session's exec processes, and the mount.
    drop.assert_awaited_once_with(COMPUTER_ID)
    mount_restarted.assert_awaited_once_with(COMPUTER_ID, "sb")


@pytest.mark.parametrize("scrub_fails", [False, True], ids=["scrubbed", "scrub-failed"])
@pytest.mark.asyncio
async def test_a_scrub_whose_mount_record_fails_is_not_stamped(
    mount_restarted, scrub_fails
):
    """Unstamped, the next cycle scrubs it again and records the restart
    again; one that failed part way may have restarted it all the same."""
    sweeper = PlatformSecretSweeper()
    provider = MagicMock()
    provider.get = AsyncMock(return_value=_runtime())
    converge = AsyncMock(side_effect=RuntimeError("stuck") if scrub_fails else None)
    stamp = AsyncMock()
    mount_restarted.side_effect = None if scrub_fails else ConnectionError("unreachable")

    with (
        patch(f"{_MECHANICS}.converge_sandbox_platform_secrets", converge),
        patch(f"{_ROLLOUT}.stamp_platform_secret_version", stamp),
        patch(
            "src.server.services.runs.executor.LocalRunExecutor.get_instance",
            return_value=_executor(busy=False),
        ),
    ):
        converged = await sweeper._converge_locked(
            COMPUTER_ID, "sb", _row(), _rollout_set(), provider
        )

    assert converged is False
    stamp.assert_not_awaited()
    mount_restarted.assert_awaited_once_with(COMPUTER_ID, "sb")


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
            COMPUTER_ID, "sb", _row(), _rollout_set(), provider
        )

    assert converged is False
    provider.get.assert_not_awaited()
    converge.assert_not_awaited()
    assert sweeper._busy_skips[COMPUTER_ID] == 1


@pytest.mark.asyncio
async def test_certified_behind_row_hot_swaps_without_restart(mount_restarted):
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
        patch(f"{_ROLLOUT}.stamp_platform_secret_version", stamp),
        patch.object(sweeper, "_drop_local_session", drop),
    ):
        converged = await sweeper._converge_locked(
            COMPUTER_ID, "sb", _row(version=1), rollout_set, provider
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
    mount_restarted.assert_not_awaited()


@pytest.mark.asyncio
async def test_convergence_failure_is_contained_and_retried_next_pass():
    sweeper = PlatformSecretSweeper()
    provider = MagicMock()
    provider.get = AsyncMock(side_effect=RuntimeError("sandbox gone"))
    stamp = AsyncMock()

    with (
        patch(f"{_ROLLOUT}.stamp_platform_secret_version", stamp),
        patch(
            "src.server.services.runs.executor.LocalRunExecutor.get_instance",
            return_value=_executor(busy=False),
        ),
    ):
        converged = await sweeper._converge_locked(
            COMPUTER_ID, "sb", _row(), _rollout_set(), provider
        )

    assert converged is False
    stamp.assert_not_awaited()


@pytest.mark.asyncio
async def test_lock_loser_skips_without_touching_the_machine():
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
    from src.server.database.computer import computer_advisory_key

    sweeper = PlatformSecretSweeper()
    provider = MagicMock()
    locked = AsyncMock(return_value=True)
    keys: list[int] = []
    unlocked: list[int] = []

    with (
        patch(
            "src.server.database.pool.get_db_connection",
            _lock_conn([True], keys, unlocked),
        ),
        patch.object(sweeper, "_converge_locked", locked),
    ):
        converged = await sweeper._sweep_one(_row(), _rollout_set(), provider)

    assert converged is True
    locked.assert_awaited_once()
    assert keys == unlocked == [computer_advisory_key(COMPUTER_ID)]


@pytest.mark.asyncio
async def test_sweep_once_is_inert_without_a_registered_rollout():
    sweeper = PlatformSecretSweeper()
    sweeper._config = _sweep_config()
    list_behind = AsyncMock()

    with (
        patch(f"{_ROLLOUT}.get_all_platform_secret_rollouts", AsyncMock(return_value={})),
        patch.object(sweeper, "_register_missing_kinds", AsyncMock(return_value=False)),
        patch(f"{_ROLLOUT}.list_workspaces_behind_platform_secret", list_behind),
    ):
        assert await sweeper.sweep_once() == 0

    list_behind.assert_not_awaited()


@pytest.mark.asyncio
async def test_start_is_inert_without_a_catalog():
    sweeper = PlatformSecretSweeper()
    sweeper.start(_sweep_config(catalog=False))
    assert sweeper._loop_task is None


@pytest.mark.asyncio
async def test_start_and_stop_lifecycle():
    """A catalog alone starts it: a deployment on a provider without platform
    Secrets can still hold computers on one that has them."""
    sweeper = PlatformSecretSweeper(interval=0.01)
    config = _sweep_config()
    config.sandbox.provider = "docker"
    with patch.object(sweeper, "sweep_once", AsyncMock(return_value=0)):
        sweeper.start(config)
        assert sweeper._loop_task is not None
        await sweeper.stop()
    assert sweeper._loop_task is None


# ---------------------------------------------------------------------------
# The sweep converges a sandbox, and the sandbox belongs to a computer
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_the_lock_is_the_machines():
    """Two projects on one computer each holding their own key would scrub the
    same sandbox at the same time."""
    from src.server.database.computer import computer_advisory_key

    sweeper = PlatformSecretSweeper()
    keys: list[int] = []

    @asynccontextmanager
    async def _conn_cm():
        cur = MagicMock()
        cur.fetchone = AsyncMock(return_value=(True,))

        async def _execute(sql, params=None):
            if params:
                keys.append(params[0])
            return cur

        conn = MagicMock()
        conn.execute = AsyncMock(side_effect=_execute)
        yield conn

    locked = AsyncMock(return_value=True)
    with (
        patch("src.server.database.pool.get_db_connection", _conn_cm),
        patch.object(sweeper, "_converge_locked", locked),
    ):
        await sweeper._sweep_one(_row(), _rollout_set(), MagicMock())

    assert set(keys) == {computer_advisory_key(COMPUTER_ID)}
    assert locked.await_args.args[0] == COMPUTER_ID


@pytest.mark.asyncio
async def test_a_busy_sibling_defers_the_scrub():
    """The scrub force-stops the sandbox, which every project on the machine is
    using, so one idle project does not settle it."""
    sweeper = PlatformSecretSweeper()
    provider = MagicMock()
    executor = MagicMock()
    executor.has_active_tasks_for_computer = AsyncMock(return_value=True)
    stamp = AsyncMock()

    with (
        patch(
            "src.server.services.runs.executor.LocalRunExecutor.get_instance",
            MagicMock(return_value=executor),
        ),
        patch(f"{_ROLLOUT}.stamp_platform_secret_version", stamp),
    ):
        converged = await sweeper._converge_locked(
            COMPUTER_ID, "sb", _row(), _rollout_set(), provider
        )

    assert converged is False
    executor.has_active_tasks_for_computer.assert_awaited_once_with(
        COMPUTER_ID, workspace_id="ws"
    )
    stamp.assert_not_awaited()


@pytest.mark.asyncio
async def test_a_machine_with_no_project_left_still_converges():
    """The source query LEFT JOINs projects: a running sandbox with every
    project deleted is behind the fleet all the same, and the stamp and the
    session retirement are the machine's, so no representative is needed."""
    sweeper = PlatformSecretSweeper()
    rollout_set = _rollout_set()
    provider = MagicMock()
    provider.get = AsyncMock(return_value=_runtime())
    executor = _executor(busy=False)
    stamp = AsyncMock()
    drop = AsyncMock()

    with (
        patch(f"{_MECHANICS}.converge_sandbox_platform_secrets", AsyncMock()),
        patch(f"{_ROLLOUT}.stamp_platform_secret_version", stamp),
        patch(
            "src.server.services.runs.executor.LocalRunExecutor.get_instance",
            MagicMock(return_value=executor),
        ),
        patch.object(sweeper, "_drop_local_session", drop),
    ):
        converged = await sweeper._converge_locked(
            COMPUTER_ID, "sb", _row(workspace_id=None), rollout_set, provider
        )

    assert converged is True
    executor.has_active_tasks_for_computer.assert_awaited_once_with(
        COMPUTER_ID, workspace_id=None
    )
    stamp.assert_awaited_once_with(
        computer_id=COMPUTER_ID, expected_sandbox_id="sb", rollout_set=rollout_set
    )
    drop.assert_awaited_once_with(COMPUTER_ID)


@pytest.mark.asyncio
async def test_sweep_uses_each_computers_provider_and_closes_clients():
    from src.server.services.computer_manager import ComputerManager

    sweeper = PlatformSecretSweeper()
    sweeper._config = _sweep_config()
    rows = [{**_row(), "kind": "daytona", "provider_ref": "sb",
             "root_dir": "/custom", "provider_config": {"auto_stop_interval": value}}
            for value in (600, 1200)]
    manager = MagicMock()
    manager._binding_from_computer.side_effect = ComputerManager._binding_from_computer
    providers = [MagicMock(close=AsyncMock()), MagicMock(close=AsyncMock())]
    manager._provider_for.side_effect = providers
    with (
        patch(
            f"{_ROLLOUT}.get_all_platform_secret_rollouts",
            AsyncMock(return_value={"daytona": _rollout_set()}),
        ),
        patch.object(sweeper, "_register_missing_kinds", AsyncMock(return_value=False)),
        patch(f"{_ROLLOUT}.list_workspaces_behind_platform_secret", AsyncMock(return_value=rows)),
        patch.object(ComputerManager, "get_instance", return_value=manager),
        patch.object(sweeper, "_sweep_one", AsyncMock(return_value=True)) as sweep,
    ):
        assert await sweeper.sweep_once() == 2
    bindings = [call.args[0] for call in manager._provider_for.call_args_list]
    assert [b.provider_config for b in bindings] == [r["provider_config"] for r in rows]
    assert all(b.kind == "daytona" and b.root_dir == "/custom" for b in bindings)
    assert [call.args[2] for call in sweep.await_args_list] == providers
    for provider in providers:
        provider.close.assert_awaited_once()


@pytest.mark.asyncio
async def test_a_never_certified_e2b_sandbox_is_verified_not_scrubbed():
    # A restart cannot change a fixed env, so there is no scrub: a clean one
    # is certified as it stands.
    sweeper = PlatformSecretSweeper()
    provider = MagicMock()
    provider.get = AsyncMock(return_value=_runtime())
    converge = AsyncMock()
    verify = AsyncMock()
    stamp = AsyncMock()

    with (
        patch(f"{_MECHANICS}.converge_sandbox_platform_secrets", converge),
        patch(f"{_MECHANICS}.remount_platform_secret_bindings", AsyncMock()),
        patch(f"{_MECHANICS}.verify_runtime_platform_secrets", verify),
        patch(f"{_ROLLOUT}.stamp_platform_secret_version", stamp),
    ):
        converged = await sweeper._converge_locked(
            COMPUTER_ID, "sb", _row(kind="e2b"), _rollout_set(), provider
        )

    assert converged is True
    converge.assert_not_awaited()
    verify.assert_awaited_once()
    stamp.assert_awaited_once()


def _needs_replacing():
    from ptc_agent.core.sandbox.platform_secrets import (
        PlatformSecretReplacementRequired,
    )

    return AsyncMock(side_effect=PlatformSecretReplacementRequired("FMP_API_KEY"))


@pytest.mark.asyncio
@pytest.mark.parametrize("version", [0, 1])
async def test_an_e2b_sandbox_holding_plaintext_is_rebuilt(version):
    from src.server.services.computer_manager import ComputerManager

    sweeper = PlatformSecretSweeper()
    provider = MagicMock()
    provider.get = AsyncMock(return_value=_runtime())
    manager = MagicMock()
    manager.replace_sandbox_for_platform_secrets = AsyncMock(return_value=True)
    stamp = AsyncMock()

    with (
        patch(f"{_MECHANICS}.remount_platform_secret_bindings", AsyncMock()),
        patch(f"{_MECHANICS}.verify_runtime_platform_secrets", _needs_replacing()),
        patch(f"{_ROLLOUT}.stamp_platform_secret_version", stamp),
        patch.object(ComputerManager, "get_instance", return_value=manager),
    ):
        converged = await sweeper._converge_locked(
            COMPUTER_ID, "sb", _row(kind="e2b", version=version),
            _rollout_set(generation=2), provider,
        )

    assert converged is True
    manager.replace_sandbox_for_platform_secrets.assert_awaited_once_with(
        COMPUTER_ID, "sb"
    )
    # The old sandbox is never stamped; the new one is as it binds.
    stamp.assert_not_awaited()


@pytest.mark.asyncio
async def test_a_rebuild_waits_out_a_turn_and_backs_off_after_a_failure(monkeypatch):
    from src.server.services import platform_secret_sweep
    from src.server.services.computer_errors import MachineBusyError
    from src.server.services.computer_manager import ComputerManager

    sweeper = PlatformSecretSweeper()
    provider = MagicMock()
    provider.get = AsyncMock(return_value=_runtime())
    replace = AsyncMock(side_effect=MachineBusyError("turn running"))
    manager = MagicMock(replace_sandbox_for_platform_secrets=replace)
    clock = [1000.0]
    monkeypatch.setattr(platform_secret_sweep.time, "monotonic", lambda: clock[0])

    async def converge() -> bool:
        return await sweeper._converge_locked(
            COMPUTER_ID, "sb", _row(kind="e2b", version=1),
            _rollout_set(generation=2), provider,
        )

    with (
        patch(f"{_MECHANICS}.remount_platform_secret_bindings", AsyncMock()),
        patch(f"{_MECHANICS}.verify_runtime_platform_secrets", _needs_replacing()),
        patch(f"{_ROLLOUT}.stamp_platform_secret_version", AsyncMock()),
        patch.object(ComputerManager, "get_instance", return_value=manager),
    ):
        # Busy: deferred to the next pass, with no backoff.
        assert await converge() is False
        assert sweeper._busy_skips[COMPUTER_ID] == 1
        replace.side_effect = RuntimeError("backup incomplete")
        assert await converge() is False
        assert replace.await_count == 2
        # A failed rebuild is not retried every pass.
        clock[0] += 60
        assert await converge() is False
        assert replace.await_count == 2
        clock[0] += platform_secret_sweep.REBUILD_RETRY_S
        replace.side_effect = None
        replace.return_value = True
        assert await converge() is True
        assert replace.await_count == 3


# ---------------------------------------------------------------------------
# One rollout per provider kind
# ---------------------------------------------------------------------------


def _kind_row(computer_id: str, kind: str) -> dict:
    return {**_row(kind=kind, version=1), "computer_id": computer_id,
            "provider_ref": "sb", "root_dir": None, "provider_config": None}


@pytest.mark.asyncio
async def test_each_computer_converges_against_its_own_kinds_rollout():
    from src.server.services.computer_manager import ComputerManager

    sweeper = PlatformSecretSweeper()
    sweeper._config = _sweep_config()
    sets = {
        "daytona": _rollout_set(3, provider="daytona"),
        "e2b": _rollout_set(8, provider="e2b"),
    }
    rows = [_kind_row("c-1", "daytona"), _kind_row("c-2", "e2b")]
    list_behind = AsyncMock(return_value=rows)
    manager = MagicMock()
    manager._provider_for.return_value = MagicMock(close=AsyncMock())

    with (
        patch(f"{_ROLLOUT}.get_all_platform_secret_rollouts", AsyncMock(return_value=sets)),
        patch.object(sweeper, "_register_missing_kinds", AsyncMock(return_value=False)),
        patch(f"{_ROLLOUT}.list_workspaces_behind_platform_secret", list_behind),
        patch.object(ComputerManager, "get_instance", return_value=manager),
        patch.object(sweeper, "_sweep_one", AsyncMock(return_value=True)) as sweep,
    ):
        assert await sweeper.sweep_once() == 2

    assert list_behind.await_args.args[0] == sets
    assert [
        (call.args[0]["computer_id"], call.args[1].provider)
        for call in sweep.await_args_list
    ] == [("c-1", "daytona"), ("c-2", "e2b")]


@pytest.mark.asyncio
async def test_a_kind_without_credentials_is_not_listed():
    """Nothing could reach its sandboxes, and its computers cannot start."""
    sweeper = PlatformSecretSweeper()
    sweeper._config = _sweep_config(e2b_key="")
    sets = {"daytona": _rollout_set(provider="daytona"), "e2b": _rollout_set(provider="e2b")}
    list_behind = AsyncMock(return_value=[])

    with (
        patch(f"{_ROLLOUT}.get_all_platform_secret_rollouts", AsyncMock(return_value=sets)),
        patch.object(sweeper, "_register_missing_kinds", AsyncMock(return_value=False)),
        patch(f"{_ROLLOUT}.list_workspaces_behind_platform_secret", list_behind),
    ):
        assert await sweeper.sweep_once() == 0

    assert sorted(list_behind.await_args.args[0]) == ["daytona"]


@pytest.mark.asyncio
async def test_a_pass_that_registers_a_kind_converges_onto_it_at_once():
    sweeper = PlatformSecretSweeper()
    sweeper._config = _sweep_config()
    after = {"daytona": _rollout_set(provider="daytona"), "e2b": _rollout_set(9, provider="e2b")}
    get_all = AsyncMock(side_effect=[{"daytona": after["daytona"]}, after])
    list_behind = AsyncMock(return_value=[])

    with (
        patch(f"{_ROLLOUT}.get_all_platform_secret_rollouts", get_all),
        patch.object(sweeper, "_register_missing_kinds", AsyncMock(return_value=True)),
        patch(f"{_ROLLOUT}.list_workspaces_behind_platform_secret", list_behind),
    ):
        await sweeper.sweep_once()

    assert list_behind.await_args.args[0] == after


@pytest.mark.asyncio
async def test_registration_skips_kinds_registered_or_without_credentials():
    sweeper = PlatformSecretSweeper()
    sweeper._config = _sweep_config(e2b_key="")
    register = AsyncMock(return_value=True)

    with (
        patch(
            f"{_ROLLOUT}.served_platform_secret_kinds",
            AsyncMock(return_value={"daytona": 1, "e2b": 2}),
        ),
        patch.object(sweeper, "_register_kind", register),
    ):
        assert await sweeper._register_missing_kinds(
            {"daytona": _rollout_set()}
        ) is False

    register.assert_not_awaited()


def _lock_conn(acquired: list[bool], keys: list[str], unlocked: list[str]):
    @asynccontextmanager
    async def _conn_cm():
        async def _execute(sql, params=None):
            cur = MagicMock()
            if "pg_try_advisory_lock" in sql:
                keys.append(params[0])
                cur.fetchone = AsyncMock(return_value=(acquired.pop(0),))
            return cur

        async def _unlock(sql, params=None):
            if "pg_advisory_unlock" in sql:
                unlocked.append(params[0])

        conn = MagicMock()
        conn.execute = AsyncMock(side_effect=_execute)
        conn.close = AsyncMock()
        unlock_cur = MagicMock()
        unlock_cur.execute = AsyncMock(side_effect=_unlock)
        conn.cursor.return_value.__aenter__.return_value = unlock_cur
        yield conn

    return _conn_cm


@pytest.mark.asyncio
async def test_registration_retry_runs_under_the_kinds_lock_and_backs_off(
    monkeypatch,
):
    from src.server.services import platform_secret_sweep
    from src.server.services.platform_secret_rollout import (
        PlatformSecretReadinessError,
    )

    sweeper = PlatformSecretSweeper()
    sweeper._config = _sweep_config()
    clock = [1000.0]
    monkeypatch.setattr(platform_secret_sweep.time, "monotonic", lambda: clock[0])
    acquired = [False, True, True]
    keys: list[str] = []
    unlocked: list[str] = []
    reconcile = AsyncMock(side_effect=ConnectionError("e2b down"))

    async def register() -> bool:
        return await sweeper._register_missing_kinds({"daytona": _rollout_set()})

    with (
        patch(
            "src.server.database.pool.get_db_connection",
            _lock_conn(acquired, keys, unlocked),
        ),
        patch(
            f"{_ROLLOUT}.served_platform_secret_kinds",
            AsyncMock(return_value={"daytona": 1, "e2b": 1}),
        ),
        patch(
            f"{_ROLLOUT}.get_platform_secret_rollouts",
            AsyncMock(side_effect=PlatformSecretReadinessError("not initialized")),
        ),
        patch(f"{_ROLLOUT}.reconcile_and_register_kind", reconcile),
    ):
        # A sibling holds the kind's lock: this worker leaves it alone.
        assert await register() is False
        reconcile.assert_not_awaited()
        # Its turn, and the provider is down: retried only after the backoff.
        assert await register() is False
        assert reconcile.await_count == 1
        clock[0] += 60
        assert await register() is False
        assert reconcile.await_count == 1
        clock[0] += platform_secret_sweep.REGISTER_RETRY_S
        reconcile.side_effect = None
        reconcile.return_value = _rollout_set(provider="e2b")
        assert await register() is True

    assert reconcile.await_count == 2
    assert reconcile.await_args.args[1] == "e2b"
    key = platform_secret_sweep.registration_lock_key("e2b")
    assert keys == [key] * 3
    # Released after the failed attempt and after the one that landed.
    assert unlocked == [key, key]
    assert "e2b" not in sweeper._register_retry_at


@pytest.mark.asyncio
async def test_registration_leaves_a_kind_a_sibling_just_registered():
    from src.server.services.platform_secret_sweep import registration_lock_key

    sweeper = PlatformSecretSweeper()
    sweeper._config = _sweep_config()
    reconcile = AsyncMock()
    unlocked: list[str] = []

    with (
        patch(
            "src.server.database.pool.get_db_connection",
            _lock_conn([True], [], unlocked),
        ),
        patch(
            f"{_ROLLOUT}.get_platform_secret_rollouts",
            AsyncMock(return_value=_rollout_set(provider="e2b")),
        ),
        patch(f"{_ROLLOUT}.reconcile_and_register_kind", reconcile),
    ):
        assert await sweeper._register_kind("e2b") is False

    reconcile.assert_not_awaited()
    assert unlocked == [registration_lock_key("e2b")]
