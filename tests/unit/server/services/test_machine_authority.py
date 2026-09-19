"""The machine is the authority for the two identity transitions and for stop.

``computers.provider_ref`` is where a provisioned sandbox is published and
``computers.status`` is where a start is claimed, so the manager's bind and
claim have to enter through the computer and let the project's own columns ride
along as the shadow the same statement writes. What these lock is the choice of
statement, because both shapes return a workspace row and a call routed at the
project would look correct until two projects on one machine start racing: a
workspace-keyed CAS would let each of them provision its own sandbox for the
same computer, and a workspace-keyed claim would let a sibling restart a
machine that is already coming up.

Stop is the same argument from the other side. It is asked of one project but
takes the sandbox away from every project on the machine, so the activity gate
it consults has to be the machine's.
"""

from __future__ import annotations

import uuid
from contextlib import contextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from ptc_agent.config.core import FilesystemConfig
from src.server.services.computer_manager import ComputerBinding, ComputerManager
from src.server.services.workspace_manager import WorkspaceManager
from tests.computer_manager_patch import cm_patch

COMPUTER_ID = "11111111-1111-4111-8111-111111111111"

_LIFECYCLE = "src.server.services.computer_manager._lifecycle"
_MACHINES = "src.server.services.computer_manager._machines"
_PROVISIONING = "src.server.services.computer_manager._provisioning"


def _make_config():
    config = MagicMock()
    config.sandbox = SimpleNamespace(provider="daytona")
    config.filesystem = SimpleNamespace(working_directory="/home/workspace")
    config.to_core_config.return_value = SimpleNamespace(
        sandbox=SimpleNamespace(
            provider="daytona",
            daytona=SimpleNamespace(api_key="test-key"),
            platform_secrets={},
        ),
        filesystem=FilesystemConfig(working_directory="/home/workspace"),
    )
    return config


def _workspace(workspace_id, *, computer_id=COMPUTER_ID, status="running"):
    return {
        "workspace_id": workspace_id,
        "user_id": "user-1",
        "computer_id": computer_id,
        "sandbox_id": "sandbox-abc",
        "status": status,
    }


def _computer(*, shadowed, status="running", provider_ref="sandbox-abc"):
    return {
        "computer_id": COMPUTER_ID,
        "status": status,
        "provider_ref": provider_ref,
        "shadowed_workspace_ids": shadowed,
    }


def _binding(workspace_id="", *, provider_ref="sandbox-abc"):
    return ComputerBinding(
        workspace_id=workspace_id,
        computer_id=COMPUTER_ID,
        kind="daytona",
        root_dir="/home/workspace",
        provider_ref=provider_ref,
    )


class _Base:
    def setup_method(self):
        ComputerManager.reset_instance()

    def teardown_method(self):
        ComputerManager.reset_instance()

    def _manager(self):
        return WorkspaceManager.get_instance(config=_make_config())


def _patch_statements(*, computer_row, workspace_row):
    """Patch both identity statements and the project read-back they share."""
    return (
        cm_patch(
            "try_bind_computer_provider_ref",
            AsyncMock(return_value=computer_row),
        ),
        cm_patch(
            "try_claim_computer_for_start",
            AsyncMock(return_value=computer_row),
        ),
        cm_patch(
            "db_get_workspace",
            AsyncMock(return_value=workspace_row),
        ),
    )


@contextmanager
def _patch_stop_status(status):
    with patch(f"{_MACHINES}.update_computer_status", status):
        yield status


class TestBindingThroughTheMachine(_Base):
    @pytest.mark.asyncio
    async def test_a_project_on_a_computer_binds_the_computer(self):
        manager = self._manager()
        ws_id = str(uuid.uuid4())
        row = _workspace(ws_id)
        bind, _claim, read = _patch_statements(
            computer_row=_computer(shadowed=[ws_id]), workspace_row=row
        )
        with bind as machine_bind, read:
            bound = await manager._bind_machine_identity(
                _binding(ws_id),
                sandbox_id="sandbox-abc",
                expected_previous_sandbox_id=None,
                platform_secret_version=4,
            )

        machine_bind.assert_awaited_once_with(
            COMPUTER_ID,
            provider_ref="sandbox-abc",
            expected_previous_provider_ref=None,
            platform_secret_version=4,
        )
        assert bound is row

    @pytest.mark.asyncio
    async def test_a_lost_machine_race_publishes_nothing(self):
        """The caller unwinds the sandbox it built, so a read-back here would
        hand it a row that still names the winner's sandbox."""
        manager = self._manager()
        ws_id = str(uuid.uuid4())
        bind, _claim, read = _patch_statements(
            computer_row=None, workspace_row=_workspace(ws_id)
        )
        with bind, read as read_back:
            bound = await manager._bind_machine_identity(
                _binding(ws_id),
                sandbox_id="sandbox-abc",
                expected_previous_sandbox_id=None,
                platform_secret_version=0,
            )

        assert bound is None
        read_back.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_a_project_the_shadow_missed_is_logged_not_raised(self, caplog):
        """The machine's row is the authority and the project reconciles on its
        next write, so a drifted shadow must not fail the provision."""
        manager = self._manager()
        ws_id = str(uuid.uuid4())
        bind, _claim, read = _patch_statements(
            computer_row=_computer(shadowed=[str(uuid.uuid4())]),
            workspace_row=_workspace(ws_id),
        )
        with bind, read, caplog.at_level("WARNING"):
            bound = await manager._bind_machine_identity(
                _binding(ws_id),
                sandbox_id="sandbox-abc",
                expected_previous_sandbox_id=None,
                platform_secret_version=0,
            )

        assert bound is not None
        assert "unshadowed" in caplog.text


class TestClaimingTheStart(_Base):
    @pytest.mark.asyncio
    async def test_a_project_on_a_computer_claims_the_computer(self):
        manager = self._manager()
        ws_id = str(uuid.uuid4())
        row = _workspace(ws_id, status="starting")
        _bind, claim, read = _patch_statements(
            computer_row=_computer(shadowed=[ws_id], status="starting"),
            workspace_row=row,
        )
        with claim as machine_claim, read:
            claimed = await manager._claim_machine_for_start(
                _binding(ws_id), from_status="stopped"
            )

        machine_claim.assert_awaited_once_with(COMPUTER_ID, from_status="stopped")
        assert claimed is row

    @pytest.mark.asyncio
    async def test_the_claim_fences_on_the_state_the_caller_saw(self):
        """A machine backfilled at 'creating' is startable, and a caller that
        saw it there must not CAS from 'stopped' and lose every time."""
        manager = self._manager()
        ws_id = str(uuid.uuid4())
        _bind, claim, read = _patch_statements(
            computer_row=_computer(shadowed=[ws_id], status="starting"),
            workspace_row=_workspace(ws_id, status="starting"),
        )
        with claim as machine_claim, read:
            await manager._claim_machine_for_start(
                _binding(ws_id), from_status="creating"
            )

        machine_claim.assert_awaited_once_with(COMPUTER_ID, from_status="creating")

    @pytest.mark.asyncio
    async def test_a_lost_claim_reads_nothing_back(self):
        """A loser that read the row back would see the winner's 'starting' and
        could not tell it from its own."""
        manager = self._manager()
        ws_id = str(uuid.uuid4())
        _bind, claim, read = _patch_statements(
            computer_row=None, workspace_row=_workspace(ws_id)
        )
        with claim, read as read_back:
            claimed = await manager._claim_machine_for_start(
                _binding(ws_id), from_status="stopped"
            )

        assert claimed is None
        read_back.assert_not_awaited()


class TestStoppingAMachineWithWorkOnIt(_Base):
    """Stop is refused while any project on the machine still has work.

    Verified end to end against a live stack as well: with a real in_progress
    run on a sibling-free computer the route answers 400 and the same POST
    succeeds once the run is finalized.
    """

    @staticmethod
    def _executor(*, busy_machine=False, busy_project=False):
        executor = MagicMock()
        executor.has_active_tasks_for_computer = AsyncMock(return_value=busy_machine)
        executor.has_active_tasks_for_workspace = AsyncMock(return_value=busy_project)
        return patch(
            f"{_LIFECYCLE}.LocalRunExecutor.get_instance",
            MagicMock(return_value=executor),
        ), executor

    @pytest.mark.asyncio
    async def test_a_busy_machine_refuses_before_the_stopping_write(self):
        manager = self._manager()
        ws_id = str(uuid.uuid4())
        gate, executor = self._executor(busy_machine=True)
        status = AsyncMock()
        with (
            patch(
                f"{_LIFECYCLE}.get_computer",
                AsyncMock(return_value=_computer(shadowed=[ws_id])),
            ),
            _patch_stop_status(status),
            gate,
        ):
            with pytest.raises(RuntimeError, match="still has work running"):
                await manager._stop_machine(COMPUTER_ID, workspace_id=ws_id)

        executor.has_active_tasks_for_computer.assert_awaited_once_with(
            COMPUTER_ID, workspace_id=ws_id
        )
        # Nothing was written, so a refused stop leaves the machine running
        # rather than parked in 'stopping' with a live sandbox under it.
        status.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_an_idle_sibling_does_not_settle_it(self):
        """The probe is the machine's and names the asking project only to
        exclude it, which is what makes a sibling's run count."""
        manager = self._manager()
        ws_id = str(uuid.uuid4())
        gate, executor = self._executor(busy_machine=True, busy_project=False)
        with (
            patch(
                f"{_LIFECYCLE}.get_computer",
                AsyncMock(return_value=_computer(shadowed=[ws_id])),
            ),
            patch(f"{_MACHINES}.update_computer_status", AsyncMock()),
            gate,
        ):
            with pytest.raises(RuntimeError, match="still has work running"):
                await manager._stop_machine(COMPUTER_ID, workspace_id=ws_id)

        executor.has_active_tasks_for_workspace.assert_not_awaited()


class TestStopEntersThroughTheMachine(_Base):
    """Which row is allowed to refuse a stop, and which write performs it.

    Asking one project to stop takes the sandbox away from every project on the
    machine, so the machine's own status is the precondition and the transition
    is a compare-and-set on it. Reading the project's shadow here is what let a
    single lagging row wedge the idle sweep for good.
    """

    @pytest.mark.asyncio
    async def test_a_lagging_project_row_cannot_refuse(self):
        ws_id = str(uuid.uuid4())
        manager = self._manager()
        manager.backup_project_files = AsyncMock()
        manager._detached_sandbox_teardown = AsyncMock()
        manager._machine_has_active_tasks = AsyncMock(return_value=False)
        status = AsyncMock(return_value=_computer(shadowed=[ws_id], status="stopping"))
        with (
            cm_patch(
                "db_get_workspace",
                AsyncMock(return_value=_workspace(ws_id, status="stopped")),
            ),
            patch(
                f"{_PROVISIONING}.get_live_workspace_ids_for_computer",
                AsyncMock(return_value=[ws_id]),
            ),
            patch(
                f"{_LIFECYCLE}.get_computer",
                AsyncMock(return_value=_computer(shadowed=[ws_id])),
            ),
            patch(
                f"{_MACHINES}.get_computer",
                AsyncMock(return_value=_computer(shadowed=[ws_id])),
            ),
            _patch_stop_status(status),
        ):
            await manager._stop_machine(COMPUTER_ID, workspace_id=ws_id)

        assert [c.args for c in status.await_args_list] == [
            (COMPUTER_ID, "stopping"),
            (COMPUTER_ID, "stopping"),
            (COMPUTER_ID, "stopped"),
        ]
        assert [c.kwargs.get("expected") for c in status.await_args_list] == [
            "running",
            "stopping",
            "stopping",
        ]

    @pytest.mark.asyncio
    async def test_a_machine_someone_else_is_already_stopping_is_left_alone(self):
        """Losing the claim is another worker doing the work, not a failure to
        report: raising here is what made the sweep log every cycle.

        The machine is read twice, once to gate the stop and once inside the
        claim, so a peer moving it out of 'running' between them is the shape a
        lost claim actually arrives in."""
        ws_id = str(uuid.uuid4())
        manager = self._manager()
        manager.backup_project_files = AsyncMock()
        manager._detached_sandbox_teardown = AsyncMock()
        manager._machine_has_active_tasks = AsyncMock(return_value=False)
        status = AsyncMock()
        with (
            cm_patch(
                "db_get_workspace",
                AsyncMock(return_value=_workspace(ws_id)),
            ),
            patch(
                f"{_LIFECYCLE}.get_computer",
                AsyncMock(return_value=_computer(shadowed=[ws_id])),
            ),
            patch(
                f"{_MACHINES}.get_computer",
                AsyncMock(return_value=_computer(shadowed=[ws_id], status="stopping")),
            ),
            patch(f"{_MACHINES}.update_computer_status", status),
        ):
            row = await manager._stop_machine(COMPUTER_ID, workspace_id=ws_id)

        assert row["workspace_id"] == ws_id
        status.assert_not_awaited()
        manager._detached_sandbox_teardown.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_archive_reads_the_machines_status_too(self):
        """Same authority, and no transition to make: a project row saying
        'stopped' under a running machine must not archive a live sandbox."""
        ws_id = str(uuid.uuid4())
        manager = self._manager()
        manager._machine_has_active_tasks = AsyncMock(return_value=False)
        with (
            cm_patch(
                "db_get_workspace",
                AsyncMock(return_value=_workspace(ws_id, status="stopped")),
            ),
            patch(
                f"{_MACHINES}.get_computer",
                AsyncMock(return_value=_computer(shadowed=[ws_id])),
            ),
        ):
            with pytest.raises(RuntimeError, match="Cannot archive computer"):
                await manager._archive_machine(COMPUTER_ID, workspace_id=ws_id)


class TestStopOwnershipFence(_Base):
    @pytest.mark.asyncio
    async def test_a_live_heartbeat_prevents_stopping_recovery(self):
        manager = self._manager()
        manager._settle_machine_stop = AsyncMock()
        manager._detached_runtime = MagicMock()

        with patch(
            f"{_LIFECYCLE}.computer_stop_heartbeat_present",
            AsyncMock(return_value=True),
        ):
            await manager._correct_stuck_stopping(_binding("ws-1"))

        manager._detached_runtime.assert_not_called()
        manager._settle_machine_stop.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_lost_postgres_fence_aborts_the_provider_stop(self):
        manager = self._manager()
        session = MagicMock()
        session.sandbox.sandbox_id = "sandbox-abc"
        session.stop = AsyncMock()
        manager._machine(COMPUTER_ID).session = session
        manager._backup_machine_files_to_db = AsyncMock(return_value=2)
        manager._machine_has_active_tasks = AsyncMock(return_value=False)
        manager._settle_machine_stop = AsyncMock()
        manager._stop_result = AsyncMock(return_value={"status": "running"})

        with patch(
            f"{_MACHINES}.update_computer_status",
            AsyncMock(return_value=None),
        ) as status:
            result = await manager._finish_claimed_stop(
                _binding("ws-1"),
                workspace_id="ws-1",
                durable_sandbox_id="sandbox-abc",
            )

        assert result == {"status": "running"}
        status.assert_awaited_once_with(COMPUTER_ID, "stopping", expected="stopping")
        session.stop.assert_not_awaited()
        manager._settle_machine_stop.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_work_started_during_backup_aborts_the_provider_stop(self):
        manager = self._manager()
        session = MagicMock()
        session.sandbox.sandbox_id = "sandbox-abc"
        session.stop = AsyncMock()
        manager._machine(COMPUTER_ID).session = session
        manager._backup_machine_files_to_db = AsyncMock(return_value=2)
        manager._machine_has_active_tasks = AsyncMock(return_value=True)
        manager._settle_machine_stop = AsyncMock()
        manager._stop_result = AsyncMock(return_value={"status": "running"})

        with patch(
            f"{_MACHINES}.update_computer_status", new_callable=AsyncMock
        ) as status:
            result = await manager._finish_claimed_stop(
                _binding("ws-1"),
                workspace_id="ws-1",
                durable_sandbox_id="sandbox-abc",
            )

        assert result == {"status": "running"}
        status.assert_not_awaited()
        session.stop.assert_not_awaited()
        manager._settle_machine_stop.assert_awaited_once_with(COMPUTER_ID, "running")


class TestStopMirrorsEveryProjectOnTheMachine(_Base):
    """A stop is asked of one project and ends the runtime for every one of them.

    The file mirror is per project but the sandbox is the machine's, so the last
    mirror has to be taken for each project before the runtime goes away. A
    folder changed outside a turn (the file-write route, or a sibling's own turn
    since its last sync) exists only in the sandbox, and the next recreation
    restores whatever the mirror holds.
    """

    def _machine(self, *workspace_ids):
        """The statements a bound stop reads, with every project live on it."""
        computer = _computer(shadowed=list(workspace_ids))
        status = AsyncMock(return_value={**computer, "status": "stopping"})
        return (
            cm_patch(
                "db_get_workspace",
                AsyncMock(return_value=_workspace(workspace_ids[0])),
            ),
            patch(
                f"{_PROVISIONING}.get_live_workspace_ids_for_computer",
                AsyncMock(return_value=list(workspace_ids)),
            ),
            patch(f"{_LIFECYCLE}.get_computer", AsyncMock(return_value=computer)),
            patch(f"{_MACHINES}.get_computer", AsyncMock(return_value=computer)),
            _patch_stop_status(status),
        )

    def _stoppable(self, manager, backup):
        manager.backup_project_files = backup
        manager._detached_sandbox_teardown = AsyncMock()
        manager._machine_has_active_tasks = AsyncMock(return_value=False)

    @pytest.mark.asyncio
    async def test_the_borrowed_handles_siblings_are_mirrored_too(self):
        """The handle is whichever project the caller happened to name."""
        handle, sibling = str(uuid.uuid4()), str(uuid.uuid4())
        manager = self._manager()
        backup = AsyncMock()
        self._stoppable(manager, backup)
        rows, live, lifecycle, machine, status = self._machine(handle, sibling)
        with rows, live, lifecycle, machine, status:
            await manager._stop_machine(COMPUTER_ID, workspace_id=handle)

        assert [c.args[0] for c in backup.await_args_list] == [handle, sibling]
        # The machine's published ref fences every project, not its own shadow
        # column: one lagging row must not skip its own last mirror.
        assert [c.kwargs["expected_sandbox_id"] for c in backup.await_args_list] == [
            "sandbox-abc",
            "sandbox-abc",
        ]

    @pytest.mark.asyncio
    async def test_one_projects_failure_does_not_strand_its_siblings(self):
        """Best effort is per project, as it has always been per stop: the
        sandbox is going away either way, so the siblings still get their turn
        and the machine still settles."""
        handle, sibling = str(uuid.uuid4()), str(uuid.uuid4())
        manager = self._manager()
        backup = AsyncMock(side_effect=[RuntimeError("scan failed"), None])
        self._stoppable(manager, backup)
        rows, live, lifecycle, machine, status = self._machine(handle, sibling)
        with rows, live, lifecycle, machine, status as moved:
            await manager._stop_machine(COMPUTER_ID, workspace_id=handle)

        assert [c.args[0] for c in backup.await_args_list] == [handle, sibling]
        assert [c.args for c in moved.await_args_list] == [
            (COMPUTER_ID, "stopping"),
            (COMPUTER_ID, "stopping"),
            (COMPUTER_ID, "stopped"),
        ]

    @pytest.mark.asyncio
    async def test_a_project_this_worker_never_saw_reaches_the_machines_session(self):
        """Naming the machine is what makes the sibling's mirror possible at all.

        ``backup_project_files`` reaches the session through the computer id it is
        handed, so a sibling this worker has never served still finds the handle
        the machine's starter installed."""
        handle, sibling = str(uuid.uuid4()), str(uuid.uuid4())
        manager = self._manager()
        backup = AsyncMock()
        manager.backup_project_files = backup
        with patch(
            f"{_PROVISIONING}.get_live_workspace_ids_for_computer",
            AsyncMock(return_value=[handle, sibling]),
        ):
            await manager._backup_machine_files_to_db(
                COMPUTER_ID, workspace_id=handle, expected_sandbox_id="sandbox-abc"
            )

        assert [c.args[0] for c in backup.await_args_list] == [handle, sibling]
        assert [c.kwargs["computer_id"] for c in backup.await_args_list] == [
            COMPUTER_ID,
            COMPUTER_ID,
        ]

    @pytest.mark.asyncio
    async def test_a_strict_caller_still_tries_every_project_before_refusing(self):
        """Recreate paths destroy the sandbox, so an unsaved project aborts them
        - but the siblings' mirrors are taken first, since the abort is not
        guaranteed to leave the runtime alone forever."""
        handle, sibling = str(uuid.uuid4()), str(uuid.uuid4())
        manager = self._manager()
        backup = AsyncMock(side_effect=[RuntimeError("scan failed"), None])
        manager.backup_project_files = backup
        with patch(
            f"{_PROVISIONING}.get_live_workspace_ids_for_computer",
            AsyncMock(return_value=[handle, sibling]),
        ):
            with pytest.raises(RuntimeError, match="unmirrored"):
                await manager._backup_machine_files_to_db(
                    COMPUTER_ID,
                    workspace_id=handle,
                    expected_sandbox_id="sandbox-abc",
                    strict=True,
                )

        assert [c.args[0] for c in backup.await_args_list] == [handle, sibling]

    @pytest.mark.asyncio
    async def test_a_lost_sibling_list_still_mirrors_the_handle(self):
        """A failed read makes the stop less complete, never less safe."""
        ws_id = str(uuid.uuid4())
        manager = self._manager()
        backup = AsyncMock()
        manager.backup_project_files = backup
        with patch(
            f"{_PROVISIONING}.get_live_workspace_ids_for_computer",
            AsyncMock(side_effect=RuntimeError("pool exhausted")),
        ):
            await manager._backup_machine_files_to_db(
                COMPUTER_ID, workspace_id=ws_id, expected_sandbox_id="sandbox-abc"
            )

        assert [c.args[0] for c in backup.await_args_list] == [ws_id]
