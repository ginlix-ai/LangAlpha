"""The layout version's two seams: the sandbox manifest, and the computer row.

A layout migration moves files without moving any module hash, so the asset
sync's "nothing changed" exit would return before the manifest write and the
migration would run again on every later sync. These pin the fall-through, the
folder the migration is told about, and that a settled sandbox still takes the
fast path.
"""

from __future__ import annotations

import hashlib
import json
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from ptc_agent.config.core import (
    CoreConfig,
    DaytonaConfig,
    FilesystemConfig,
    LoggingConfig,
    MCPConfig,
    SandboxConfig,
    SecurityConfig,
)
from ptc_agent.core.project_context import ProjectContext
from ptc_agent.core.sandbox.assets import publish_vault_secrets, sync_sandbox_assets
from ptc_agent.core.sandbox.migration import CURRENT_LAYOUT_VERSION
from ptc_agent.core.sandbox.runtime import ExecResult, SandboxRuntime
from tests.unit.core.sandbox.fake_provider import fake_provider

WORK_DIR = "/home/workspace"
DIR_NAME = "acme-ab12"
OWNER_DIR = "origin-cd34"
PROJECT = ProjectContext("ws-1", DIR_NAME)


def _make_sandbox():
    from ptc_agent.core.sandbox.ptc_sandbox import PTCSandbox

    config = CoreConfig(
        sandbox=SandboxConfig(daytona=DaytonaConfig(api_key="test-key")),
        security=SecurityConfig(),
        mcp=MCPConfig(servers=[]),
        logging=LoggingConfig(),
        filesystem=FilesystemConfig(),
    )
    with patch(
        "ptc_agent.core.sandbox.ptc_sandbox.create_provider",
        return_value=fake_provider(),
    ):
        sandbox = PTCSandbox(config=config)
    sandbox._work_dir = WORK_DIR
    sandbox.mcp_registry = None
    runtime = AsyncMock(spec=SandboxRuntime)
    runtime.working_dir = WORK_DIR
    runtime.fetch_working_dir = AsyncMock(return_value=WORK_DIR)
    sandbox.runtime = runtime
    sandbox._wait_ready = AsyncMock()
    sandbox.ensure_sandbox_ready = AsyncMock()
    sandbox._prune_disabled_tool_modules = AsyncMock()
    sandbox._install_tool_modules = AsyncMock()
    sandbox._start_internal_mcp_servers = AsyncMock()
    sandbox._write_unified_manifest = AsyncMock()
    sandbox._cleanup_legacy_manifests = AsyncMock()
    sandbox._upload_mcp_server_files_impl = AsyncMock()
    sandbox._upload_internal_packages = AsyncMock()
    return sandbox


async def _settled_manifest(sandbox, *, layout_version: int, skill_roots=None):
    """The manifest this sandbox would write, stamped at *layout_version*."""
    manifest = await sandbox._compute_sandbox_manifest(skill_roots=skill_roots)
    return {**manifest, "layout_version": layout_version}


class TestTheMigrationStampsItself:
    @pytest.mark.asyncio
    async def test_failed_migration_aborts_sync_without_publishing_a_manifest(self):
        sandbox = _make_sandbox()
        sandbox._read_unified_manifest = AsyncMock(
            return_value=await _settled_manifest(sandbox, layout_version=3)
        )
        with patch(
            "ptc_agent.core.sandbox.assets.run_layout_migrations",
            AsyncMock(side_effect=RuntimeError("move failed")),
        ):
            with pytest.raises(RuntimeError, match="move failed"):
                await sandbox.sync_sandbox_assets(reusing_sandbox=True, project=PROJECT)
        sandbox._write_unified_manifest.assert_not_awaited()
        sandbox._install_tool_modules.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_a_moved_layout_writes_the_manifest_with_nothing_else_changed(self):
        sandbox = _make_sandbox()
        remote = await _settled_manifest(sandbox, layout_version=3)
        sandbox._read_unified_manifest = AsyncMock(return_value=remote)

        with patch(
            "ptc_agent.core.sandbox.assets.run_layout_migrations",
            AsyncMock(return_value=CURRENT_LAYOUT_VERSION),
        ):
            result = await sandbox.sync_sandbox_assets(
                reusing_sandbox=True, project=PROJECT
            )

        assert result.refreshed_modules == []
        assert result.layout_version == CURRENT_LAYOUT_VERSION
        # Without this write the sandbox keeps claiming v3 and migrates again
        # on every sync for the rest of its life.
        written = sandbox._write_unified_manifest.await_args.args[0]
        assert written["layout_version"] == CURRENT_LAYOUT_VERSION

    @pytest.mark.asyncio
    async def test_the_folder_reaches_the_migration(self):
        sandbox = _make_sandbox()
        sandbox._read_unified_manifest = AsyncMock(
            return_value=await _settled_manifest(sandbox, layout_version=3)
        )
        migrate = AsyncMock(return_value=CURRENT_LAYOUT_VERSION)

        with patch("ptc_agent.core.sandbox.assets.run_layout_migrations", migrate):
            await sandbox.sync_sandbox_assets(reusing_sandbox=True, project=PROJECT)

        assert migrate.await_args.kwargs["dir_name"] == DIR_NAME

    @pytest.mark.asyncio
    async def test_the_root_owner_outranks_the_folder_that_asked(self):
        """v3 to v4 sweeps the root into one folder, and on a computer folded
        from several sandboxes that folder is the machine's original project's,
        whichever sibling starts first."""
        sandbox = _make_sandbox()
        sandbox._read_unified_manifest = AsyncMock(
            return_value=await _settled_manifest(sandbox, layout_version=3)
        )
        migrate = AsyncMock(return_value=CURRENT_LAYOUT_VERSION)

        with patch("ptc_agent.core.sandbox.assets.run_layout_migrations", migrate):
            await sandbox.sync_sandbox_assets(
                reusing_sandbox=True,
                project=PROJECT,
                root_owner_dir_name=OWNER_DIR,
            )

        assert migrate.await_args.kwargs["dir_name"] == OWNER_DIR
        # Only the move's target moves: the caller still gets its own folder
        # created and its own tool overlay.
        made = " ".join(c.args[0] for c in sandbox.runtime.exec.await_args_list)
        assert f"{WORK_DIR}/{DIR_NAME}/data" in made
        assert OWNER_DIR not in made

    @pytest.mark.asyncio
    async def test_a_settled_sandbox_neither_migrates_nor_rewrites(self):
        # The real driver, so "did not re-run" is the absence of shell, not the
        # absence of a mock call.
        sandbox = _make_sandbox()
        sandbox._read_unified_manifest = AsyncMock(
            return_value=await _settled_manifest(
                sandbox, layout_version=CURRENT_LAYOUT_VERSION
            )
        )

        result = await sandbox.sync_sandbox_assets(
            reusing_sandbox=True, project=PROJECT
        )

        assert result.refreshed_modules == []
        assert result.layout_version == CURRENT_LAYOUT_VERSION
        # The only shell a settled sync runs is the idempotent mkdir of the
        # workspace tier; nothing moves.
        commands = [c.args[0] for c in sandbox.runtime.exec.await_args_list]
        assert all(c.startswith("mkdir -p ") for c in commands), commands
        sandbox._write_unified_manifest.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_the_skills_cache_still_folds_in_the_agents_own_installs(self):
        # The fall-through must not skip the lock read the fast path does, or a
        # one-time migration would drop agent-installed skills from the cached
        # view for the life of the process.
        sandbox = _make_sandbox()
        sandbox._read_unified_manifest = AsyncMock(
            return_value=await _settled_manifest(
                sandbox, layout_version=3, skill_roots=["/local/skills"]
            )
        )
        sandbox._download_skills_lock = AsyncMock(return_value={"agent-skill": {}})
        sandbox._build_complete_skills_cache = MagicMock()

        with patch(
            "ptc_agent.core.sandbox.assets.run_layout_migrations",
            AsyncMock(return_value=CURRENT_LAYOUT_VERSION),
        ):
            await sandbox.sync_sandbox_assets(
                reusing_sandbox=True,
                project=PROJECT,
                skill_dirs=["/local/skills"],
            )

        sandbox._build_complete_skills_cache.assert_called_once()


class TestAMissingStampOnAMachineBornLater:
    """A missing manifest reads as v1, whose steps delete and move folders by
    name. On a machine the owner keeps their own files on, that would take
    them, so a provider whose machines were never laid out that way floors
    the stamp at the first layout it made."""

    @pytest.mark.asyncio
    async def test_ssh_starts_from_its_first_layout(self):
        from ptc_agent.core.sandbox.providers.ssh import SSHProvider

        sandbox = _make_sandbox()
        sandbox.provider = object.__new__(SSHProvider)
        sandbox._read_unified_manifest = AsyncMock(return_value=None)
        migrate = AsyncMock(side_effect=lambda _rt, _wd, version, **_kw: version)

        with patch("ptc_agent.core.sandbox.assets.run_layout_migrations", migrate):
            await sandbox.sync_sandbox_assets(reusing_sandbox=True, project=PROJECT)

        assert migrate.await_args.args[2] == SSHProvider.first_layout_version

    @pytest.mark.asyncio
    async def test_other_providers_still_start_from_v1(self):
        sandbox = _make_sandbox()
        sandbox._read_unified_manifest = AsyncMock(return_value=None)
        migrate = AsyncMock(return_value=CURRENT_LAYOUT_VERSION)

        with patch("ptc_agent.core.sandbox.assets.run_layout_migrations", migrate):
            await sandbox.sync_sandbox_assets(reusing_sandbox=True, project=PROJECT)

        assert migrate.await_args.args[2] == 1


VAULT_FILE = f"{WORK_DIR}/_internal/.vault_secrets.json"


class TestVaultPublication:
    """The root vault file is rewritten exactly when what it holds on disk
    differs from the owner's vault. A digest recorded beside it would be
    trusted after the earlier version rewrote the path, or after anything
    removed it."""

    @staticmethod
    def _digest(payload):
        encoded = json.dumps(
            payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False
        ).encode("utf-8")
        return hashlib.sha256(encoded).hexdigest()

    @classmethod
    def _on_disk(cls, sandbox, payload, *, exit_code=0):
        """Answer the digest read as a file holding *payload* would."""
        if payload is None:
            stdout = "absent\n"
        else:
            stdout = f"{cls._digest(payload)}  {VAULT_FILE}\n"

        async def run(command, *args, **kwargs):
            if "sha256sum" in command:
                return ExecResult(stdout=stdout, stderr="", exit_code=exit_code)
            return ExecResult(stdout="", stderr="", exit_code=0)

        sandbox.runtime.exec = AsyncMock(side_effect=run)

    @staticmethod
    async def _settle(sandbox):
        sandbox._read_unified_manifest = AsyncMock(
            return_value=await _settled_manifest(
                sandbox, layout_version=CURRENT_LAYOUT_VERSION
            )
        )

    @staticmethod
    def _commands(sandbox):
        return [call.args[0] for call in sandbox.runtime.exec.await_args_list]

    @pytest.mark.asyncio
    async def test_a_file_already_holding_the_vault_is_left_alone(self):
        sandbox = _make_sandbox()
        payload = {"API_KEY": "secret"}
        await self._settle(sandbox)
        self._on_disk(sandbox, payload)

        await sync_sandbox_assets(
            sandbox, reusing_sandbox=True, project=PROJECT, vault_secrets=payload
        )

        sandbox.runtime.upload_file.assert_not_awaited()
        sandbox._write_unified_manifest.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_a_file_holding_other_content_is_rewritten(self):
        sandbox = _make_sandbox()
        await self._settle(sandbox)
        self._on_disk(sandbox, {"API_KEY": "old-secret"})

        await sync_sandbox_assets(
            sandbox,
            reusing_sandbox=True,
            project=PROJECT,
            vault_secrets={"API_KEY": "new-secret"},
        )

        assert sandbox.runtime.upload_file.await_args.args == (
            b'{"API_KEY":"new-secret"}',
            VAULT_FILE,
        )
        # Nothing about the vault lives in the manifest any more.
        sandbox._write_unified_manifest.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_a_file_gone_from_disk_is_written_again(self):
        sandbox = _make_sandbox()
        payload = {"API_KEY": "secret"}
        await self._settle(sandbox)
        self._on_disk(sandbox, None)

        await sync_sandbox_assets(
            sandbox, reusing_sandbox=True, project=PROJECT, vault_secrets=payload
        )

        sandbox.runtime.upload_file.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_an_unreadable_file_is_written_rather_than_trusted(self):
        sandbox = _make_sandbox()
        payload = {"API_KEY": "secret"}
        await self._settle(sandbox)
        self._on_disk(sandbox, payload, exit_code=1)

        await sync_sandbox_assets(
            sandbox, reusing_sandbox=True, project=PROJECT, vault_secrets=payload
        )

        sandbox.runtime.upload_file.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_an_empty_vault_removes_the_file(self):
        sandbox = _make_sandbox()
        await self._settle(sandbox)
        self._on_disk(sandbox, {"API_KEY": "old-secret"})

        await sync_sandbox_assets(
            sandbox, reusing_sandbox=True, project=PROJECT, vault_secrets={}
        )

        assert f"rm -f {VAULT_FILE}" in self._commands(sandbox)
        sandbox.runtime.upload_file.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_an_empty_vault_with_no_file_does_nothing(self):
        sandbox = _make_sandbox()
        await self._settle(sandbox)
        self._on_disk(sandbox, None)

        await sync_sandbox_assets(
            sandbox, reusing_sandbox=True, project=PROJECT, vault_secrets={}
        )

        assert not any(c.startswith("rm ") for c in self._commands(sandbox))
        sandbox.runtime.upload_file.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_a_sync_with_nothing_to_say_does_not_read_the_file(self):
        sandbox = _make_sandbox()
        await self._settle(sandbox)
        self._on_disk(sandbox, None)

        await sync_sandbox_assets(sandbox, reusing_sandbox=True, project=PROJECT)

        assert not any("sha256sum" in c for c in self._commands(sandbox))
        sandbox.runtime.upload_file.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_a_direct_push_is_gated_on_the_file(self):
        sandbox = _make_sandbox()
        payload = {"API_KEY": "secret"}
        sandbox._read_unified_manifest = AsyncMock()
        self._on_disk(sandbox, {"API_KEY": "old-secret"})

        assert await publish_vault_secrets(sandbox, payload) is True
        sandbox.runtime.upload_file.assert_awaited_once()

        self._on_disk(sandbox, payload)
        sandbox.runtime.upload_file.reset_mock()
        assert await publish_vault_secrets(sandbox, payload) is False
        sandbox.runtime.upload_file.assert_not_awaited()
        sandbox._read_unified_manifest.assert_not_awaited()
        sandbox._write_unified_manifest.assert_not_awaited()


class TestTheWorkspaceTierIsCreated:
    """Workspace N's folder has no other origin.

    A computer gains folders long after its sandbox was built, and the
    reconnect path skips directory setup because the computer's own
    directories are already there, so the sync is where a folder that never
    existed comes from.
    """

    @staticmethod
    def _mkdirs(sandbox):
        commands = [c.args[0] for c in sandbox.runtime.exec.await_args_list]
        return [c for c in commands if c.startswith("mkdir -p ")]

    @pytest.mark.asyncio
    async def test_a_sync_creates_the_folder_it_was_told_about(self):
        sandbox = _make_sandbox()
        sandbox._read_unified_manifest = AsyncMock(
            return_value=await _settled_manifest(
                sandbox, layout_version=CURRENT_LAYOUT_VERSION
            )
        )

        await sandbox.sync_sandbox_assets(reusing_sandbox=True, project=PROJECT)

        made = " ".join(self._mkdirs(sandbox))
        ws = f"{WORK_DIR}/{DIR_NAME}"
        for expected in (
            f"{ws}/data",
            f"{ws}/.agents/skills",
            f"{ws}/.agents/memory",
            f"{ws}/.agents/tools",
        ):
            assert expected in made, made

    @pytest.mark.asyncio
    async def test_without_a_folder_the_tier_folds_onto_the_root(self):
        sandbox = _make_sandbox()
        sandbox._read_unified_manifest = AsyncMock(
            return_value=await _settled_manifest(
                sandbox, layout_version=CURRENT_LAYOUT_VERSION
            )
        )

        await sandbox.sync_sandbox_assets(reusing_sandbox=True)

        made = " ".join(self._mkdirs(sandbox))
        assert f"{WORK_DIR}/data" in made, made
        assert f"{WORK_DIR}//" not in made, made
