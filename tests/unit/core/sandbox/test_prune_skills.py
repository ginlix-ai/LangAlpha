"""Tests for the two paths in PTCSandbox that can rm -rf a skill dir.

_prune_remote_skills never removes user-installed skills, removes stale
platform ones, and preserves everything when the lock is unavailable or has no
entry for a directory. _upload_skills' staged swap answers to the same
signal: a lock it could not read means it does not know what it is replacing.
"""

from __future__ import annotations

import ast
import io
import json
import os
import shlex
import tarfile
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
            local_skill_names=set(), existing_lock=lock
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
            local_skill_names=set(), existing_lock=lock
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
            local_skill_names=set(), existing_lock=lock
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
            local_skill_names=set(), existing_lock=None
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
            [tmpdir],
            manifest={"files": {"x": "y"}, "skills": {}},
            existing_lock=existing_lock,
        )

    def _writes(self, mock_runtime) -> list[str]:
        return [
            c[0][0]
            for c in mock_runtime.exec.call_args_list
            if c[0] and ("rm -rf" in c[0][0] or "mkdir -p" in c[0][0])
        ]

    def _archived_files(self, mock_runtime) -> list[str]:
        """The sandbox paths of the files in the one archive uploaded."""
        (call,) = [
            c for c in mock_runtime.upload_file.call_args_list
            if c.args[1].endswith(".tar.gz")
        ]
        with tarfile.open(fileobj=io.BytesIO(call[0][0]), mode="r:gz") as tar:
            return ["/" + m.name for m in tar.getmembers() if m.isfile()]

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
        mock_runtime.upload_file.assert_not_called()
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
        # The whole tier goes up as one archive, never a request per file.
        mock_runtime.upload_files.assert_not_called()
        assert [p.rsplit("/", 2)[-2:] for p in self._archived_files(mock_runtime)] == [
            ["some-skill", "SKILL.md"]
        ]

    def _script_args(self, mock_runtime) -> list[dict]:
        """The decoded ARGS of every skill-sync script exec, in call order."""
        out = []
        uploads = {c.args[1]: c.args[0] for c in mock_runtime.upload_file.call_args_list}
        for c in mock_runtime.exec.call_args_list:
            argv = shlex.split(c.args[0])
            if argv[:2] != ["python3", "-I"]:
                continue
            script = ast.parse(uploads[argv[2]])
            args = next(
                node.value for node in script.body
                if isinstance(node, ast.Assign)
                and any(isinstance(t, ast.Name) and t.id == "ARGS" for t in node.targets)
            )
            out.append(json.loads(ast.literal_eval(args.args[0])))
        return out

    @patch("ptc_agent.core.sandbox.ptc_sandbox.create_provider")
    @pytest.mark.asyncio
    async def test_lock_read_succeeded_swaps_in_a_staged_copy(
        self, mock_create_provider, mock_provider, mock_runtime
    ):
        """A readable lock proves ownership, so the dir is replaced even though
        it is already on disk: staged beside it and swapped in, never cleared
        first."""
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
        assert not [w for w in self._writes(mock_runtime) if "rm -rf" in w]
        (install,) = self._script_args(mock_runtime)
        assert install["mode"] == "install"
        assert install["base"] == SANDBOX_BASE
        assert install["names"] == ["some-skill"]
        assert install["staging"].startswith(f"{SANDBOX_BASE}/.staging/sync-")
        assert self._archived_files(mock_runtime) == [
            f"{install['staging']}/some-skill/SKILL.md"
        ]

    @patch("ptc_agent.core.sandbox.ptc_sandbox.create_provider")
    @pytest.mark.asyncio
    async def test_skills_land_under_the_computers_own_root(
        self, mock_create_provider, mock_provider, mock_runtime
    ):
        """A computer whose root is not the default gets its skills under that
        root: the default is a folder its user may not be able to create."""
        from ptc_agent.core.sandbox.ptc_sandbox import PTCSandbox

        root = "/home/someone/langalpha"
        mock_create_provider.return_value = mock_provider
        sandbox = PTCSandbox(
            config=_make_config(filesystem=FilesystemConfig(working_directory=root))
        )
        sandbox.runtime = mock_runtime

        with tempfile.TemporaryDirectory() as tmpdir:
            _local_skill_tree(tmpdir, "some-skill")
            await self._upload(sandbox, tmpdir, existing_lock=None)

        (install,) = self._script_args(mock_runtime)
        assert install["base"] == f"{root}/.agents/skills"
        assert install["staging"].startswith(f"{root}/.agents/skills/.staging/")
        assert all(p.startswith(f"{root}/") for p in self._archived_files(mock_runtime))

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
