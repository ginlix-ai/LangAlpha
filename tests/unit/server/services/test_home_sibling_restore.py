"""A turn restores the other workspaces on its computer that a recreate left empty.

A sandbox recreate restores only the workspace that asked for it and flags the
rest, which otherwise rejoin on their own next open. The Chief of Staff reads
and edits those folders, so a Home turn brings each flagged one back first.
"""

from contextlib import asynccontextmanager
from functools import partial
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest

from src.server.services.computer_manager._provisioning import ProvisioningMixin
from src.server.services.computer_manager._types import ComputerBinding

_MOD = "src.server.services.computer_manager._provisioning"


@asynccontextmanager
async def _held(workspace_id):
    yield


@pytest.mark.asyncio
async def test_home_restores_each_flagged_sibling_on_the_sandbox_it_holds():
    """A sibling already attached here at its current files costs no restore,
    one attached before a move brought it new files is restored all the same,
    and one whose restore fails is left to its own next open and fails nothing
    else."""
    home = ComputerBinding(
        workspace_id="home",
        computer_id="comp-1",
        kind="daytona",
        dir_name="Home",
        root_dir="/home/workspace",
        provider_ref="sbx-1",
    )
    sandbox = object()
    restored = []

    async def _restore(binding, sb, *, urgent):
        assert sb is sandbox and urgent is False
        if binding.workspace_id == "broken":
            raise RuntimeError("manifest unreadable")
        restored.append(binding)
        return True

    manager = SimpleNamespace(
        resolve_binding=AsyncMock(return_value=home),
        _session_sandbox_id=lambda session: "sbx-1",
        _projects_attached={("attached", "sbx-1"): "e1", ("moved-in", "sbx-1"): "e1"},
        _ensure_workspace_dirs=AsyncMock(),
        _maybe_restore_files=AsyncMock(side_effect=_restore),
        get_session_for_workspace=AsyncMock(),
    )
    manager.restore_owed_folders = partial(ProvisioningMixin.restore_owed_folders, manager)
    # The owed list is the database's: a clean sibling is never in it.
    owed_read = AsyncMock(
        return_value={
            "home": None,
            "attached": "e1",
            "owed": None,
            "broken": None,
            "moved-in": "e2",
        }
    )

    with (
        patch(f"{_MOD}.get_restore_owed_workspaces_for_computer", owed_read),
        patch(
            f"{_MOD}.db_get_workspace_dir_name",
            AsyncMock(side_effect=lambda ws: f"{ws}-dir"),
        ),
        patch(f"{_MOD}.workspace_folder_in_use", _held),
    ):
        await ProvisioningMixin.restore_sibling_folders(
            manager, SimpleNamespace(sandbox=sandbox), "home"
        )

    manager.resolve_binding.assert_awaited_once_with("home")
    owed_read.assert_awaited_once_with("comp-1")
    owed, moved_in = sorted(restored, key=lambda b: b.workspace_id != "owed")
    assert moved_in.workspace_id == "moved-in"
    assert (owed.workspace_id, owed.dir_name, owed.computer_id, owed.root_dir) == (
        "owed",
        "owed-dir",
        "comp-1",
        "/home/workspace",
    )
    assert sorted(c.args[0] for c in manager._ensure_workspace_dirs.await_args_list) == [
        "broken",
        "moved-in",
        "owed",
    ]
    # Only the sandbox Home holds: no other workspace's machine is acquired.
    manager.get_session_for_workspace.assert_not_awaited()
