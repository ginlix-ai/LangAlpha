"""Tests for the two paths in PTCSandbox that can rm -rf a skill dir.

_prune_remote_skills never removes user-installed skills, removes stale
platform ones, and preserves everything when the lock is unavailable or has no
entry for a directory. _upload_skills' clean-slate rm answers to the same
signal: a lock it could not read means it does not know what it is deleting.
"""

from __future__ import annotations

import json
import os
import tempfile
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
from ptc_agent.core.sandbox.runtime import (
    ExecResult,
    RuntimeState,
    SandboxProvider,
    SandboxRuntime,
)


def _make_config(**overrides) -> CoreConfig:
    defaults = dict(
        sandbox=SandboxConfig(daytona=DaytonaConfig(api_key="test-key")),
        security=SecurityConfig(),
        mcp=MCPConfig(),
        logging=LoggingConfig(),
        filesystem=FilesystemConfig(),
    )
    defaults.update(overrides)
    return CoreConfig(**defaults)


def _dir_entry(name: str, path: str) -> dict:
    return {"name": name, "path": path, "is_dir": True}


@pytest.fixture
def mock_runtime():
    runtime = AsyncMock(spec=SandboxRuntime)
    runtime.id = "mock-runtime-1"
    runtime.working_dir = "/home/workspace"
    # The lock merge runs through an exec'd script whose stdout is parsed as
    # JSON; rm/mkdir ignore stdout, so one ok-payload default serves them all.
    runtime.exec = AsyncMock(
        return_value=ExecResult(
            json.dumps({"status": "ok", "skills": {}, "skipped": []}), "", 0
        )
    )
    runtime.get_state = AsyncMock(return_value=RuntimeState.RUNNING)
    runtime.list_files = AsyncMock(return_value=[])
    return runtime


@pytest.fixture
def mock_provider(mock_runtime):
    provider = AsyncMock(spec=SandboxProvider)
    provider.create = AsyncMock(return_value=mock_runtime)
    provider.get = AsyncMock(return_value=mock_runtime)
    provider.close = AsyncMock()
    provider.is_transient_error = MagicMock(return_value=False)
    return provider


SANDBOX_BASE = "/home/workspace/.agents/skills"


class TestPruneRemoteSkills:
    """Unit tests for PTCSandbox._prune_remote_skills()."""

    @patch("ptc_agent.core.sandbox.ptc_sandbox.create_provider")
    @pytest.mark.asyncio
    async def test_prune_skips_user_owned_skill(
        self, mock_create_provider, mock_provider, mock_runtime
    ):
        """Skill not in platform list but owner='user' in lock -> survives."""
        from ptc_agent.core.sandbox.ptc_sandbox import PTCSandbox

        mock_create_provider.return_value = mock_provider
        sandbox = PTCSandbox(config=_make_config())
        sandbox.runtime = mock_runtime

        # Sandbox has a user-installed skill "my-custom-skill"
        mock_runtime.list_files = AsyncMock(
            return_value=[
                _dir_entry(
                    "my-custom-skill",
                    f"{SANDBOX_BASE}/my-custom-skill",
                ),
            ]
        )

        lock = {
            "my-custom-skill": {"owner": "user", "name": "my-custom-skill"},
        }

        # Platform set does NOT include "my-custom-skill"
        await sandbox._prune_remote_skills(
            SANDBOX_BASE, local_skill_names=set(), existing_lock=lock
        )

        # rm -rf should NOT have been called
        mock_runtime.exec.assert_not_called()

    @patch("ptc_agent.core.sandbox.ptc_sandbox.create_provider")
    @pytest.mark.asyncio
    async def test_prune_removes_stale_platform_skill(
        self, mock_create_provider, mock_provider, mock_runtime
    ):
        """Skill not in platform list and owner='platform' -> pruned."""
        from ptc_agent.core.sandbox.ptc_sandbox import PTCSandbox

        mock_create_provider.return_value = mock_provider
        sandbox = PTCSandbox(config=_make_config())
        sandbox.runtime = mock_runtime

        mock_runtime.list_files = AsyncMock(
            return_value=[
                _dir_entry(
                    "old-platform-skill",
                    f"{SANDBOX_BASE}/old-platform-skill",
                ),
            ]
        )

        lock = {
            "old-platform-skill": {
                "owner": "platform",
                "name": "old-platform-skill",
            },
        }

        await sandbox._prune_remote_skills(
            SANDBOX_BASE, local_skill_names=set(), existing_lock=lock
        )

        # rm -rf SHOULD have been called for the stale platform skill
        mock_runtime.exec.assert_called_once()
        call_args = mock_runtime.exec.call_args[0][0]
        assert "rm -rf" in call_args
        assert "old-platform-skill" in call_args

    @patch("ptc_agent.core.sandbox.ptc_sandbox.create_provider")
    @pytest.mark.asyncio
    async def test_prune_skips_unknown_skill_safe_default(
        self, mock_create_provider, mock_provider, mock_runtime
    ):
        """Skill dir not in lock at all -> preserved (safe default)."""
        from ptc_agent.core.sandbox.ptc_sandbox import PTCSandbox

        mock_create_provider.return_value = mock_provider
        sandbox = PTCSandbox(config=_make_config())
        sandbox.runtime = mock_runtime

        mock_runtime.list_files = AsyncMock(
            return_value=[
                _dir_entry(
                    "mystery-skill",
                    f"{SANDBOX_BASE}/mystery-skill",
                ),
            ]
        )

        # Lock exists but has no entry for "mystery-skill"
        lock = {
            "other-skill": {"owner": "platform", "name": "other-skill"},
        }

        await sandbox._prune_remote_skills(
            SANDBOX_BASE, local_skill_names=set(), existing_lock=lock
        )

        # Unknown origin -> preserved; rm -rf should NOT be called
        mock_runtime.exec.assert_not_called()

    @patch("ptc_agent.core.sandbox.ptc_sandbox.create_provider")
    @pytest.mark.asyncio
    async def test_prune_skips_all_when_lock_unavailable(
        self, mock_create_provider, mock_provider, mock_runtime
    ):
        """Lock is None -> no skills pruned (safe default)."""
        from ptc_agent.core.sandbox.ptc_sandbox import PTCSandbox

        mock_create_provider.return_value = mock_provider
        sandbox = PTCSandbox(config=_make_config())
        sandbox.runtime = mock_runtime

        mock_runtime.list_files = AsyncMock(
            return_value=[
                _dir_entry(
                    "skill-a",
                    f"{SANDBOX_BASE}/skill-a",
                ),
                _dir_entry(
                    "skill-b",
                    f"{SANDBOX_BASE}/skill-b",
                ),
            ]
        )

        await sandbox._prune_remote_skills(
            SANDBOX_BASE, local_skill_names=set(), existing_lock=None
        )

        # No lock -> everything preserved
        mock_runtime.exec.assert_not_called()

    @patch("ptc_agent.core.sandbox.ptc_sandbox.create_provider")
    @pytest.mark.asyncio
    async def test_prune_cleans_stale_lock_entries(
        self, mock_create_provider, mock_provider, mock_runtime
    ):
        """Only platform skills NOT in local_skill_names are pruned;
        user skills and current platform skills survive."""
        from ptc_agent.core.sandbox.ptc_sandbox import PTCSandbox

        mock_create_provider.return_value = mock_provider
        sandbox = PTCSandbox(config=_make_config())
        sandbox.runtime = mock_runtime

        mock_runtime.list_files = AsyncMock(
            return_value=[
                _dir_entry("current-skill", f"{SANDBOX_BASE}/current-skill"),
                _dir_entry("stale-skill", f"{SANDBOX_BASE}/stale-skill"),
                _dir_entry("user-skill", f"{SANDBOX_BASE}/user-skill"),
            ]
        )

        lock = {
            "current-skill": {"owner": "platform", "name": "current-skill"},
            "stale-skill": {"owner": "platform", "name": "stale-skill"},
            "user-skill": {"owner": "user", "name": "user-skill"},
        }

        # "current-skill" is still in the local platform set
        await sandbox._prune_remote_skills(
            SANDBOX_BASE,
            local_skill_names={"current-skill"},
            existing_lock=lock,
        )

        # Only "stale-skill" should be pruned
        assert mock_runtime.exec.call_count == 1
        call_args = mock_runtime.exec.call_args[0][0]
        assert "rm -rf" in call_args
        assert "stale-skill" in call_args


def _local_skill_tree(tmpdir: str, name: str) -> str:
    skill_dir = os.path.join(tmpdir, name)
    os.makedirs(skill_dir)
    with open(os.path.join(skill_dir, "SKILL.md"), "w") as f:
        f.write(f"---\nname: {name}\ndescription: d\n---\n# {name}\n")
    return tmpdir


class TestUploadOwnershipGuard:
    """_upload_skills only writes a name it can prove it owns.

    Ownership is read from the lock, and `_download_skills_lock` returns None
    both for a fresh sandbox and for a failed read. On that signal every name
    already on disk is treated as someone else's: a fresh sandbox has no dirs
    and uploads everything, a failed read defers instead of clearing and
    rewriting an agent's files. Prune preserves everything on the same signal.
    """

    async def _upload(self, sandbox, tmpdir, *, existing_lock):
        return await sandbox._upload_skills(
            [(tmpdir, SANDBOX_BASE)],
            manifest={"files": {"x": "y"}, "skills": {}},
            existing_lock=existing_lock,
        )

    def _writes(self, mock_runtime) -> list[str]:
        return [
            c[0][0]
            for c in mock_runtime.exec.call_args_list
            if c[0] and ("rm -rf" in c[0][0] or "mkdir -p" in c[0][0])
        ]

    @patch("ptc_agent.core.sandbox.ptc_sandbox.create_provider")
    @pytest.mark.asyncio
    async def test_lock_unavailable_and_the_name_is_taken(
        self, mock_create_provider, mock_provider, mock_runtime
    ):
        """Nothing is removed, nothing is written, and the name is reported as
        a collision so the caller's version stamp retries it."""
        from ptc_agent.core.sandbox.ptc_sandbox import PTCSandbox

        mock_create_provider.return_value = mock_provider
        sandbox = PTCSandbox(config=_make_config())
        sandbox.runtime = mock_runtime
        mock_runtime.list_files = AsyncMock(
            return_value=[_dir_entry("some-skill", f"{SANDBOX_BASE}/some-skill")]
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            _local_skill_tree(tmpdir, "some-skill")
            _, collisions = await self._upload(sandbox, tmpdir, existing_lock=None)

        assert collisions == {"some-skill"}
        assert self._writes(mock_runtime) == []
        mock_runtime.upload_files.assert_not_called()

    @patch("ptc_agent.core.sandbox.ptc_sandbox.create_provider")
    @pytest.mark.asyncio
    async def test_fresh_sandbox_uploads_everything(
        self, mock_create_provider, mock_provider, mock_runtime
    ):
        """No lock and no dirs is a fresh sandbox, not a failed read."""
        from ptc_agent.core.sandbox.ptc_sandbox import PTCSandbox

        mock_create_provider.return_value = mock_provider
        sandbox = PTCSandbox(config=_make_config())
        sandbox.runtime = mock_runtime
        mock_runtime.list_files = AsyncMock(return_value=[])

        with tempfile.TemporaryDirectory() as tmpdir:
            _local_skill_tree(tmpdir, "some-skill")
            _, collisions = await self._upload(sandbox, tmpdir, existing_lock=None)

        assert collisions == set()
        assert any("some-skill" in w for w in self._writes(mock_runtime))

    @patch("ptc_agent.core.sandbox.ptc_sandbox.create_provider")
    @pytest.mark.asyncio
    async def test_lock_read_succeeded_keeps_the_clean_slate(
        self, mock_create_provider, mock_provider, mock_runtime
    ):
        """A readable lock proves ownership, so the dir is cleared before the
        rewrite even though it is already on disk."""
        from ptc_agent.core.sandbox.ptc_sandbox import PTCSandbox

        mock_create_provider.return_value = mock_provider
        sandbox = PTCSandbox(config=_make_config())
        sandbox.runtime = mock_runtime
        mock_runtime.list_files = AsyncMock(
            return_value=[_dir_entry("some-skill", f"{SANDBOX_BASE}/some-skill")]
        )

        lock = {"unrelated": {"owner": "platform", "name": "unrelated"}}
        with tempfile.TemporaryDirectory() as tmpdir:
            _local_skill_tree(tmpdir, "some-skill")
            _, collisions = await self._upload(sandbox, tmpdir, existing_lock=lock)

        assert collisions == set()
        rm = [w for w in self._writes(mock_runtime) if "rm -rf" in w]
        assert len(rm) == 1 and "some-skill" in rm[0]

    @patch("ptc_agent.core.sandbox.ptc_sandbox.create_provider")
    @pytest.mark.asyncio
    async def test_lock_read_succeeded_still_skips_an_owned_name(
        self, mock_create_provider, mock_provider, mock_runtime
    ):
        """The lock-based guard is unchanged: an agent-installed name is left
        alone even though the sandbox listing is never consulted here."""
        from ptc_agent.core.sandbox.ptc_sandbox import PTCSandbox

        mock_create_provider.return_value = mock_provider
        sandbox = PTCSandbox(config=_make_config())
        sandbox.runtime = mock_runtime

        lock = {"some-skill": {"owner": "user", "name": "some-skill"}}
        with tempfile.TemporaryDirectory() as tmpdir:
            _local_skill_tree(tmpdir, "some-skill")
            _, collisions = await self._upload(sandbox, tmpdir, existing_lock=lock)

        assert collisions == {"some-skill"}
        assert self._writes(mock_runtime) == []
