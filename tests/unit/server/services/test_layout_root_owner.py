"""Which folder the v3 to v4 layout move sweeps a machine's root into.

The step moves every loose entry at the sandbox root into one workspace folder,
and it is triggered by whichever project starts first. On a machine that
consolidation folded siblings onto, those root files are the original project's
work, so a sibling starting first would carry them into its own folder and the
owner would find its results gone. ``computers.origin_workspace_id`` records
that owner at backfill; what these lock is that the runner reads it instead of
the folder of the project that happened to ask.
"""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from ptc_agent.config.core import FilesystemConfig
from src.server.services.computer_manager import ComputerBinding, ComputerManager

_PROVISIONING = "src.server.services.computer_manager._provisioning"

COMPUTER_ID = "11111111-1111-4111-8111-111111111111"
ORIGIN_ID = "22222222-2222-4222-8222-222222222222"
SIBLING_ID = "33333333-3333-4333-8333-333333333333"
FOLDERS = {ORIGIN_ID: "origin-ab12", SIBLING_ID: "sibling-cd34"}


def _make_config():
    config = MagicMock()
    config.sandbox = SimpleNamespace(provider="daytona")
    config.filesystem = SimpleNamespace(working_directory="/home/workspace")
    config.skills = SimpleNamespace(
        enabled=False,
        sandbox_skills_base="/home/workspace/.agents/skills",
        local_skill_dirs_with_sandbox=lambda: None,
    )
    config.to_core_config.return_value = SimpleNamespace(
        sandbox=SimpleNamespace(
            provider="daytona",
            daytona=SimpleNamespace(api_key="test-key"),
            platform_secrets={},
        ),
        filesystem=FilesystemConfig(working_directory="/home/workspace"),
    )
    return config


def _binding(workspace_id):
    """The machine the asking project is on; the sync is driven off this alone."""
    return ComputerBinding(
        workspace_id=workspace_id,
        computer_id=COMPUTER_ID,
        dir_name=FOLDERS.get(workspace_id),
        kind="daytona",
        root_dir="/home/workspace",
        provider_ref="sandbox-abc",
    )


def _machine(*, origin_workspace_id):
    return {
        "computer_id": COMPUTER_ID,
        "status": "running",
        "provider_ref": "sandbox-abc",
        "root_dir": "/home/workspace",
        "origin_workspace_id": origin_workspace_id,
    }


class TestTheRootOwnerReachesTheSync:
    def setup_method(self):
        ComputerManager.reset_instance()

    def teardown_method(self):
        ComputerManager.reset_instance()

    def _manager(self):
        manager = ComputerManager.get_instance(config=_make_config())
        manager.push_vault_secrets = AsyncMock()
        manager._vault_payloads = AsyncMock(
            side_effect=lambda workspace_id, user_id: (
                user_id,
                {"_root": {}, str(workspace_id): {}},
            )
        )
        manager._stamp_layout_version = AsyncMock()
        manager._stamp_mcp_config_version = AsyncMock()
        return manager

    @staticmethod
    async def _sync(manager, workspace_id, computer) -> dict:
        sandbox = MagicMock()
        sandbox.sync_sandbox_assets = AsyncMock(
            return_value=SimpleNamespace(layout_version=4)
        )
        folder = AsyncMock(side_effect=lambda wid, **kw: FOLDERS.get(str(wid)))
        with (
            patch(f"{_PROVISIONING}.get_computer", AsyncMock(return_value=computer)),
            patch(f"{_PROVISIONING}.db_get_workspace_dir_name", folder),
            patch(
                f"{_PROVISIONING}.sandbox_skill_sync_params",
                AsyncMock(return_value={}),
            ),
        ):
            await manager._sync_sandbox_assets(
                _binding(workspace_id), "user-1", sandbox
            )
        return sandbox.sync_sandbox_assets.await_args.kwargs

    @pytest.mark.asyncio
    async def test_a_sibling_hands_the_move_the_origins_folder_not_its_own(self):
        manager = self._manager()

        kwargs = await self._sync(
            manager, SIBLING_ID, _machine(origin_workspace_id=ORIGIN_ID)
        )

        assert kwargs["root_owner_dir_name"] == FOLDERS[ORIGIN_ID]
        # Its own folder still drives the tool overlay and the mkdir; only the
        # move's target belongs to the machine rather than to the caller.
        assert kwargs["project"].dir_name == FOLDERS[SIBLING_ID]

    @pytest.mark.asyncio
    async def test_a_machine_with_no_origin_moves_into_the_callers_folder(self):
        """Every machine minted after the split is born at v4, so the move
        never runs for it and the old behaviour is the right fallback."""
        manager = self._manager()

        kwargs = await self._sync(
            manager, SIBLING_ID, _machine(origin_workspace_id=None)
        )

        assert kwargs["root_owner_dir_name"] == FOLDERS[SIBLING_ID]
        assert kwargs["project"].dir_name == FOLDERS[SIBLING_ID]

    @pytest.mark.asyncio
    async def test_a_machine_with_no_row_moves_into_the_callers_folder(self):
        """A machine the read cannot answer for must not park a target: nothing
        is cached, so the next sibling asks again instead of inheriting the
        guess. The caller's own folder is what every fresh machine answers."""
        manager = self._manager()

        kwargs = await self._sync(manager, SIBLING_ID, None)

        assert kwargs["root_owner_dir_name"] == FOLDERS[SIBLING_ID]
        assert not manager._machine(COMPUTER_ID).root_owner_read

    @pytest.mark.asyncio
    async def test_the_origin_starting_itself_is_its_own_owner(self):
        manager = self._manager()

        kwargs = await self._sync(
            manager, ORIGIN_ID, _machine(origin_workspace_id=ORIGIN_ID)
        )

        assert kwargs["root_owner_dir_name"] == FOLDERS[ORIGIN_ID]

    @pytest.mark.asyncio
    async def test_a_tombstoned_origin_still_owns_the_root(self):
        """Its folder read ignores the tombstone on purpose: an orphaned folder
        can be recovered, files merged into a living sibling cannot."""
        manager = self._manager()
        deleted_origin = "44444444-4444-4444-8444-444444444444"

        sandbox = MagicMock()
        sandbox.sync_sandbox_assets = AsyncMock(
            return_value=SimpleNamespace(layout_version=4)
        )
        folder = AsyncMock(
            side_effect=lambda wid, **kw: (
                "gone-ef56" if str(wid) == deleted_origin else FOLDERS.get(str(wid))
            )
        )
        with (
            patch(
                f"{_PROVISIONING}.get_computer",
                AsyncMock(return_value=_machine(origin_workspace_id=deleted_origin)),
            ),
            patch(f"{_PROVISIONING}.db_get_workspace_dir_name", folder),
            patch(
                f"{_PROVISIONING}.sandbox_skill_sync_params",
                AsyncMock(return_value={}),
            ),
        ):
            await manager._sync_sandbox_assets(_binding(SIBLING_ID), "user-1", sandbox)

        kwargs = sandbox.sync_sandbox_assets.await_args.kwargs
        assert kwargs["root_owner_dir_name"] == "gone-ef56"

    @pytest.mark.asyncio
    async def test_the_owner_is_read_once_per_machine(self):
        """Every sibling's every sync would otherwise pay for the lookup, and
        the answer cannot change for the life of the machine."""
        manager = self._manager()
        await self._sync(manager, SIBLING_ID, _machine(origin_workspace_id=ORIGIN_ID))
        assert manager._machine(COMPUTER_ID).root_owner_read is True
        assert manager._machine(COMPUTER_ID).root_owner_dir == FOLDERS[ORIGIN_ID]

        machine = AsyncMock(return_value=_machine(origin_workspace_id=ORIGIN_ID))
        sandbox = MagicMock()
        sandbox.sync_sandbox_assets = AsyncMock(
            return_value=SimpleNamespace(layout_version=4)
        )
        with (
            patch(f"{_PROVISIONING}.get_computer", machine),
            patch(
                f"{_PROVISIONING}.db_get_workspace_dir_name",
                AsyncMock(side_effect=lambda wid, **kw: FOLDERS.get(str(wid))),
            ),
            patch(
                f"{_PROVISIONING}.sandbox_skill_sync_params",
                AsyncMock(return_value={}),
            ),
        ):
            await manager._sync_sandbox_assets(_binding(SIBLING_ID), "user-1", sandbox)

        assert (
            sandbox.sync_sandbox_assets.await_args.kwargs["root_owner_dir_name"]
            == FOLDERS[ORIGIN_ID]
        )
        machine.assert_not_awaited()
