"""A set of files reaches the sandbox as one archive and one command.

The archive carries no owner, so the files belong to whoever runs commands on
the provider, as a direct upload left them; files are 0644 and folders 0755.
The command clears nothing, since a caller replacing a folder unpacks beside it
and swaps, and it fails loudly rather than leave a partial tier looking
complete.
"""

from __future__ import annotations

import asyncio
import io
import os
import tarfile
from pathlib import Path
from types import SimpleNamespace

import pytest

from ptc_agent.core.sandbox.runtime import ExecResult
from ptc_agent.core.sandbox.tree_upload import (
    TreeUploadError,
    build_tar_gz,
    upload_tree,
)


class _LocalRuntime:
    """``exec`` is bash on this machine, so the real ``tar`` unpacks."""

    def __init__(self) -> None:
        self.commands: list[str] = []
        self.uploads: list[str] = []

    async def exec(self, command: str, timeout: int = 60) -> ExecResult:
        self.commands.append(command)
        proc = await asyncio.create_subprocess_exec(
            "bash",
            "-c",
            command,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.STDOUT,
        )
        out, _ = await asyncio.wait_for(proc.communicate(), timeout)
        return ExecResult(out.decode(), "", proc.returncode or 0)

    async def upload_file(self, content: bytes, path: str) -> None:
        self.uploads.append(path)
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        Path(path).write_bytes(content)


def _sandbox(root: Path) -> SimpleNamespace:
    async def runtime_call(func, *args, retry_policy, **kwargs):
        return await func(*args, **kwargs)

    return SimpleNamespace(
        runtime=_LocalRuntime(),
        layout=SimpleNamespace(internal=str(root / "_internal")),
        _runtime_call=runtime_call,
    )


def _members(archive: bytes) -> dict[str, tarfile.TarInfo]:
    with tarfile.open(fileobj=io.BytesIO(archive), mode="r:gz") as tar:
        return {m.name: m for m in tar.getmembers()}


class TestTheArchive:
    def test_members_are_ownerless_with_upload_modes(self, tmp_path):
        script = tmp_path / "run.sh"
        script.write_text("echo hi\n")
        script.chmod(0o755)

        members = _members(
            build_tar_gz(
                [(script, "/home/workspace/.agents/skills/a/scripts/run.sh")],
                dirs=["/home/workspace/.agents/skills/a"],
            )
        )

        folder = members["home/workspace/.agents/skills/a"]
        file = members["home/workspace/.agents/skills/a/scripts/run.sh"]
        assert folder.isdir() and folder.mode == 0o755
        # An upload never carried the host's exec bit, so neither does this.
        assert file.isfile() and file.mode == 0o644
        assert (file.uid, file.gid, file.uname, file.gname) == (0, 0, "", "")

    def test_a_path_that_climbs_out_is_refused(self):
        with pytest.raises(ValueError):
            build_tar_gz([(b"x", "/home/workspace/../etc/passwd")])
        with pytest.raises(ValueError):
            build_tar_gz([(b"x", "relative/path")])


class TestTheUpload:
    @pytest.mark.asyncio
    async def test_one_upload_and_one_command_write_the_tree(self, tmp_path):
        base = tmp_path / "skills"
        kept = base / "other" / "SKILL.md"
        kept.parent.mkdir(parents=True)
        kept.write_text("not ours")
        sandbox = _sandbox(tmp_path)

        await upload_tree(
            sandbox,
            [(b"new", f"{base}/a/SKILL.md"), (b"ref", f"{base}/a/ref/notes.md")],
            dirs=[f"{base}/a", f"{base}/a/ref"],
        )

        assert len(sandbox.runtime.uploads) == 1
        assert len(sandbox.runtime.commands) == 1
        assert (base / "a" / "SKILL.md").read_text() == "new"
        assert (base / "a" / "ref" / "notes.md").read_text() == "ref"
        assert kept.read_text() == "not ours"
        # The archive does not outlive the command.
        assert os.listdir(tmp_path / "_internal") == []

    @pytest.mark.asyncio
    async def test_a_root_with_a_space_unpacks_and_cleans_up(self, tmp_path):
        """A computer the user brings works in a folder they may have named."""
        root = tmp_path / "my data"
        sandbox = _sandbox(root)

        await upload_tree(sandbox, [(b"x", f"{root}/s/a/SKILL.md")])

        assert (root / "s" / "a" / "SKILL.md").read_text() == "x"
        assert os.listdir(root / "_internal") == []

    @pytest.mark.asyncio
    async def test_a_failed_unpack_raises(self, tmp_path):
        sandbox = _sandbox(tmp_path)

        async def broken(command: str, timeout: int = 60) -> ExecResult:
            return ExecResult("", "tar: short read", 2)

        sandbox.runtime.exec = broken
        with pytest.raises(TreeUploadError, match="exit 2"):
            await upload_tree(sandbox, [(b"x", f"{tmp_path}/s/a/SKILL.md")])

    @pytest.mark.asyncio
    async def test_a_failed_unpack_fails_a_start_as_a_server_fault(
        self, tmp_path, caplog
    ):
        """A sandbox whose command user cannot write the skills tier (Daytona's
        base image) fails the start as the per-file upload did: a logged 500,
        not a 400 that hands the sandbox's tar output to the client."""
        from fastapi import HTTPException

        from src.server.app.computers import _computer_action_errors

        sandbox = _sandbox(tmp_path)

        async def denied(command: str, timeout: int = 60) -> ExecResult:
            return ExecResult("tar: home/workspace/.agents: Cannot mkdir", "", 2)

        sandbox.runtime.exec = denied
        with pytest.raises(HTTPException) as answered:
            async with _computer_action_errors("start", "c1"):
                await upload_tree(sandbox, [(b"x", "/home/workspace/.agents/s/SKILL.md")])

        assert answered.value.status_code == 500
        assert "Cannot mkdir" not in answered.value.detail
        assert "Cannot mkdir" in caplog.text

    @pytest.mark.asyncio
    async def test_a_rerun_after_the_archive_is_gone_fails(self, tmp_path):
        """A retried command whose first attempt already ran cannot tell
        what that attempt did, so it fails for the caller to redo."""
        target = tmp_path / "s" / "a"
        sandbox = _sandbox(tmp_path)
        await upload_tree(sandbox, [(b"x", f"{target}/SKILL.md")])

        rerun = await sandbox.runtime.exec(sandbox.runtime.commands[0])

        assert rerun.exit_code != 0
        assert (target / "SKILL.md").read_text() == "x"
