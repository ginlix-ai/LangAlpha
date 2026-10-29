"""Shared stubs for the workspace-identity fencing the manager tests exercise.

Both workspace_manager suites patch the same symbols. Kept here so a rename
touches one copy, not two that had already drifted apart in their docstrings.
"""

from contextlib import ExitStack, contextmanager, nullcontext
from unittest.mock import AsyncMock, patch

import pytest

from tests.computer_manager_patch import cm_patch


@pytest.fixture(autouse=True)
def _no_folders_to_settle():
    """Every acquisition settles folders first, and its first step is a DB read.

    No row needs moving here; the settle's own cases are pinned in the
    planner and move-script suites. A test that drives a settle patches
    ``read_folder_rows`` itself, inside this one.
    """
    with patch(
        "src.server.services.computer_manager._folders.read_folder_rows",
        AsyncMock(return_value=(None, [])),
    ) as rows:
        yield rows


@pytest.fixture(autouse=True)
def _folder_hold_is_free():
    """Every backup and attachment holds its folder against a settle, an advisory
    lock these pool-less tests cannot take. A test of the hold patches it inside this one."""
    with ExitStack() as stack:
        for module in (
            "src.server.services.workspace_layout",
            "src.server.services.workspace_manager",
        ):
            stack.enter_context(
                patch(
                    f"{module}.workspace_folder_in_use",
                    lambda _workspace_id: nullcontext(),
                )
            )
        yield


@pytest.fixture(autouse=True)
def _no_workspace_moving():
    """Acquiring a session and deleting a workspace both refuse one that is
    moving, and a delete one with a live run, before it ends its commands:
    row reads these pool-less tests cannot make. A test of a refusal patches
    it inside this one."""
    with patch(
        "src.server.services.workspace_manager.workspace_is_moving",
        AsyncMock(return_value=False),
    ) as moving, patch(
        "src.server.services.workspace_manager.workspace_has_active_work",
        AsyncMock(return_value=False),
    ):
        yield moving


@pytest.fixture(autouse=True)
def _no_backup_stamp():
    """Every backup stamps the time once it completes: a row write these
    pool-less tests cannot make."""
    with patch(
        "src.server.services.persistence.backup.mark_files_backed_up",
        AsyncMock(),
    ):
        yield


@pytest.fixture(autouse=True)
def _no_workspaces_to_resync():
    """A restart on the sandbox the machine stopped on queues every workspace
    on it for its bring-up, listed by a row read these pool-less tests cannot
    make. A test of that queue patches it inside this one."""
    with patch(
        "src.server.services.computer_manager._bringup.get_live_workspace_ids_for_computer",
        AsyncMock(return_value=[]),
    ) as live:
        yield live


@pytest.fixture(autouse=True)
def _folder_allocation_is_free():
    """An adoption picks its folder under a transaction-scoped advisory lock
    these pool-less tests cannot take; the reads and the bind under it are patched."""
    with patch(
        "src.server.services.computer_manager._machines.folder_allocation",
        lambda computer_id, conn=None: nullcontext(None),
    ):
        yield


def _patch_identity(workspace):
    """Stub the narrow identity read every cached-session return validates against.

    Returns the workspace's own status/sandbox_id, i.e. "the cache agrees with
    Postgres" — the baseline the staleness tests deviate from deliberately.
    """
    return cm_patch(
        "db_get_workspace_identity",
        AsyncMock(
            return_value={
                "status": workspace["status"],
                "sandbox_id": workspace["sandbox_id"],
            }
        ),
    )


@contextmanager
def _patch_machine_bind(workspace, *, computer_id):
    """Stub the machine-authority CAS a project with an indexed computer binds through.

    ``computers.provider_ref`` is the authority, so the statement returns the
    computer row plus the projects its shadow write reached, and the manager
    re-reads the project row afterwards -- that read-back is what the caller
    gets. Yields the CAS mock. ``workspace=None`` means another provisioner
    won the machine.
    """
    computer = None
    bound = None
    if workspace is not None:
        computer = {
            "computer_id": computer_id,
            "provider_ref": workspace.get("sandbox_id"),
            "status": workspace.get("status"),
            "shadowed_workspace_ids": [str(workspace["workspace_id"])],
        }
        # The read-back names its machine, the way a bound row does: the
        # manager re-indexes off whatever this returns, so a row missing the
        # column would silently demote the project to its own machine.
        bound = {**workspace, "computer_id": computer_id}
    with ExitStack() as stack:
        cas = stack.enter_context(
            cm_patch(
                "try_bind_computer_provider_ref",
                AsyncMock(return_value=computer),
            )
        )
        stack.enter_context(
            cm_patch(
                "db_get_workspace",
                AsyncMock(return_value=bound),
            )
        )
        yield cas
