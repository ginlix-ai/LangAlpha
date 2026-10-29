"""A new sandbox's first sync diffs against what setup recorded.

Setup uploads the MCP server files and the shared packages while the registry
connects, and its manifest records exactly those. The first sync then sends
only what is missing (the tool overlay, the skills).
"""

from __future__ import annotations

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
from ptc_agent.core.sandbox.migration import CURRENT_LAYOUT_VERSION
from ptc_agent.core.sandbox.runtime import SandboxRuntime
from tests.unit.core.sandbox.fake_provider import fake_provider

WORK_DIR = "/home/workspace"
PROJECT = ProjectContext("ws-1", "acme-ab12")


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
    sandbox.mcp_registry = MagicMock()
    sandbox._compute_tool_schema_hash = MagicMock(return_value="schema")
    runtime = AsyncMock(spec=SandboxRuntime)
    runtime.working_dir = WORK_DIR
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
    sandbox._download_skills_lock = AsyncMock(return_value=None)
    sandbox._prune_remote_skills = AsyncMock()
    sandbox._upload_skills = AsyncMock(return_value=({}, set()))
    return sandbox


def _skill_dirs(tmp_path):
    skill = tmp_path / "skills" / "charting"
    skill.mkdir(parents=True)
    (skill / "SKILL.md").write_text("---\nname: charting\n---\nDraw.\n")
    return [str(tmp_path / "skills")]


async def _setup_manifest(sandbox) -> dict:
    await sandbox.setup_tools_and_mcp()
    return sandbox._write_unified_manifest.await_args.args[0]


class TestSetupRecordsWhatIsOnDisk:
    @pytest.mark.asyncio
    async def test_setup_records_its_uploads_and_not_the_overlay(self):
        manifest = await _setup_manifest(_make_sandbox())

        assert set(manifest["modules"]) == {"mcp_servers", "internal_packages"}


class TestTheFirstSyncDiffs:
    @pytest.mark.asyncio
    @pytest.mark.parametrize("project", [PROJECT, None], ids=["folder", "root"])
    async def test_the_first_sync_sends_only_what_setup_did_not(self, tmp_path, project):
        sandbox = _make_sandbox()
        remote = await _setup_manifest(sandbox)
        sandbox._read_unified_manifest = AsyncMock(return_value=remote)
        for done_by_setup in (
            sandbox._write_unified_manifest,
            sandbox._upload_mcp_server_files_impl,
            sandbox._upload_internal_packages,
        ):
            done_by_setup.reset_mock()

        result = await sandbox.sync_sandbox_assets(
            reusing_sandbox=False, project=project, skill_dirs=_skill_dirs(tmp_path)
        )

        assert sorted(result.refreshed_modules) == ["skills", "tool_modules"]
        sandbox._upload_mcp_server_files_impl.assert_not_awaited()
        sandbox._upload_internal_packages.assert_not_awaited()
        sandbox._install_tool_modules.assert_awaited_once()
        sandbox._start_internal_mcp_servers.assert_awaited_once()
        sandbox._upload_skills.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_an_unreadable_manifest_still_sends_everything(self, tmp_path):
        sandbox = _make_sandbox()
        sandbox._read_unified_manifest = AsyncMock(return_value=None)

        with patch(
            "ptc_agent.core.sandbox.assets.run_layout_migrations",
            AsyncMock(return_value=CURRENT_LAYOUT_VERSION),
        ):
            result = await sandbox.sync_sandbox_assets(
                reusing_sandbox=False, project=PROJECT, skill_dirs=_skill_dirs(tmp_path)
            )

        assert set(result.refreshed_modules) == {
            "mcp_servers",
            "internal_packages",
            "skills",
            "tool_modules",
        }

