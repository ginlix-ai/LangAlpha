"""One upload failing must not leave the MCP config running files that are gone.

The server files, the shared packages and the skills upload in parallel, and
the MCP config is installed after them. A skill failure used to skip that
install after the server files had already been pruned, so servers moved off
stdio kept launching deleted files on every acquire.
"""

from __future__ import annotations

import asyncio
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
from ptc_agent.core.sandbox import assets
from ptc_agent.core.sandbox.migration import CURRENT_LAYOUT_VERSION
from ptc_agent.core.sandbox.runtime import SandboxRuntime
from tests.unit.core.sandbox.fake_provider import fake_provider

WORK_DIR = "/home/workspace"
PROJECT = ProjectContext("ws-1", "acme-ab12")
EXPECTED = {"price_data_mcp_server.py", "_bootstrap.py"}


def _make_sandbox(order: list[str]):
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
    # An unreadable manifest sends every module.
    sandbox._read_unified_manifest = AsyncMock(return_value=None)
    sandbox._install_tool_modules = AsyncMock(
        side_effect=lambda **_: order.append("install")
    )
    sandbox._start_internal_mcp_servers = AsyncMock()
    sandbox._write_unified_manifest = AsyncMock()
    sandbox._cleanup_legacy_manifests = AsyncMock()
    sandbox._upload_mcp_server_files_impl = AsyncMock(return_value=EXPECTED)
    sandbox._prune_mcp_server_files = AsyncMock(
        side_effect=lambda expected: order.append("prune")
    )
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


async def _sync(sandbox, tmp_path):
    with patch(
        "ptc_agent.core.sandbox.assets.run_layout_migrations",
        AsyncMock(return_value=CURRENT_LAYOUT_VERSION),
    ):
        return await sandbox.sync_sandbox_assets(
            reusing_sandbox=True, project=PROJECT, skill_dirs=_skill_dirs(tmp_path)
        )


class TestASkillFailure:
    @pytest.mark.asyncio
    async def test_still_installs_the_config_and_then_fails_the_sync(self, tmp_path):
        order: list[str] = []
        sandbox = _make_sandbox(order)
        sandbox._upload_skills.side_effect = OSError(18, "Invalid cross-device link")

        with pytest.raises(OSError, match="cross-device"):
            await _sync(sandbox, tmp_path)

        assert order == ["install", "prune"]
        # Unwritten, so the next acquire retries the skills.
        sandbox._write_unified_manifest.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_a_cancelled_skills_upload_is_not_swallowed(self, tmp_path):
        order: list[str] = []
        sandbox = _make_sandbox(order)
        sandbox._upload_skills.side_effect = asyncio.CancelledError()

        with pytest.raises(asyncio.CancelledError):
            await _sync(sandbox, tmp_path)

        assert order == []
        sandbox._write_unified_manifest.assert_not_awaited()


class TestAnMcpInputFailure:
    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "upload", ["_upload_mcp_server_files_impl", "_upload_internal_packages"]
    )
    async def test_stops_before_the_install_and_keeps_the_old_files(
        self, tmp_path, upload
    ):
        order: list[str] = []
        sandbox = _make_sandbox(order)
        getattr(sandbox, upload).side_effect = RuntimeError("upload failed")

        with pytest.raises(RuntimeError, match="upload failed"):
            await _sync(sandbox, tmp_path)

        # No prune: the config still installed may launch any of those files.
        assert order == []
        sandbox._write_unified_manifest.assert_not_awaited()


class TestASuccessfulSync:
    @pytest.mark.asyncio
    async def test_prunes_after_the_install_with_what_the_upload_shipped(
        self, tmp_path
    ):
        order: list[str] = []
        sandbox = _make_sandbox(order)

        result = await _sync(sandbox, tmp_path)

        assert order == ["install", "prune"]
        sandbox._prune_mcp_server_files.assert_awaited_once_with(EXPECTED)
        assert set(result.refreshed_modules) == {
            "mcp_servers",
            "internal_packages",
            "skills",
            "tool_modules",
        }
        sandbox._write_unified_manifest.assert_awaited_once()


class TestTheUploadPrunesNothing:
    @pytest.mark.asyncio
    async def test_only_the_prune_removes_a_file_no_longer_shipped(self):
        sandbox = _make_sandbox([])
        stale = f"{WORK_DIR}/mcp_servers/price_data_mcp_server.py"
        sandbox.als_directory = AsyncMock(
            return_value=[
                {"name": "price_data_mcp_server.py", "path": stale, "is_dir": False}
            ]
        )

        expected = await assets._upload_mcp_server_files_impl(sandbox)
        commands = [c.args[0] for c in sandbox.runtime.exec.await_args_list]
        assert not any(c.startswith("rm") for c in commands)

        await assets._prune_mcp_server_files(sandbox, expected)
        assert sandbox.runtime.exec.await_args.args[0] == f"rm -f {stale}"
