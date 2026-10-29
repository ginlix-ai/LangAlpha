from __future__ import annotations

import pytest

pytestmark = [pytest.mark.integration, pytest.mark.asyncio(loop_scope="class")]


class TestExecuteBashCommand:
    """PTCSandbox.execute_bash_command() -- shell command execution."""

    async def test_bash_simple(self, shared_sandbox):
        wd = shared_sandbox._work_dir
        result = await shared_sandbox.execute_bash_command("echo hello bash", working_dir=wd)
        assert result["success"] is True
        assert "hello bash" in result["stdout"]
        assert result["exit_code"] == 0

    async def test_bash_returns_metadata(self, shared_sandbox):
        wd = shared_sandbox._work_dir
        result = await shared_sandbox.execute_bash_command("echo test", working_dir=wd)
        assert "bash_id" in result
        assert "command_hash" in result

    async def test_bash_error(self, shared_sandbox):
        wd = shared_sandbox._work_dir
        result = await shared_sandbox.execute_bash_command("exit 1", working_dir=wd)
        assert result["success"] is False
        assert result["exit_code"] == 1

    async def test_bash_with_pipe(self, shared_sandbox):
        wd = shared_sandbox._work_dir
        result = await shared_sandbox.execute_bash_command("echo 'a b c' | wc -w", working_dir=wd)
        assert result["success"] is True
        assert "3" in result["stdout"]

    async def test_bash_creates_files(self, shared_sandbox):
        wd = shared_sandbox._work_dir
        await shared_sandbox.execute_bash_command(f"echo 'bash content' > {wd}/bash_file.txt", working_dir=wd)
        content = await shared_sandbox.adownload_file_bytes(f"{wd}/bash_file.txt")
        assert content is not None
        assert b"bash content" in content
