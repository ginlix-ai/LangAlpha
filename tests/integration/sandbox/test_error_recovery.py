from __future__ import annotations
import pytest

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]


class TestErrorRecovery:
    async def test_code_run_after_crash(self, sandbox_runtime):
        """After code that crashes, the runtime should still be usable."""
        await sandbox_runtime.code_run("import sys; sys.exit(1)")
        result = await sandbox_runtime.code_run("print('recovered')")
        assert result.exit_code == 0
        assert "recovered" in result.stdout

    async def test_all_ops_after_stop_start(self, sandbox_runtime):
        """All operations should work after a stop/start cycle."""
        wd = await sandbox_runtime.fetch_working_dir()
        await sandbox_runtime.stop()
        await sandbox_runtime.start()
        assert (await sandbox_runtime.exec("echo ok")).exit_code == 0
        assert (await sandbox_runtime.code_run("print('ok')")).exit_code == 0
        await sandbox_runtime.upload_file(b"test", f"{wd}/recovery.txt")
        assert await sandbox_runtime.download_file(f"{wd}/recovery.txt") == b"test"

    async def test_exec_after_long_command(self, sandbox_runtime):
        """After a slow command completes, subsequent commands work."""
        result = await sandbox_runtime.exec("sleep 0.1 && echo done")
        assert result.exit_code == 0
        assert "done" in result.stdout
        # Subsequent command should work immediately
        result2 = await sandbox_runtime.exec("echo fast")
        assert result2.exit_code == 0
        assert "fast" in result2.stdout
