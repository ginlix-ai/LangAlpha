from __future__ import annotations
import os
import pytest

pytestmark = [pytest.mark.integration, pytest.mark.asyncio, pytest.mark.slow]


class TestLargeFileIO:
    @pytest.mark.parametrize("size_mb", [1, 5, 10])
    async def test_large_file_roundtrip(self, sandbox_runtime, timed, size_mb):
        wd = await sandbox_runtime.fetch_working_dir()
        data = os.urandom(size_mb * 1024 * 1024)
        async with timed("stress", f"upload_{size_mb}mb") as t:
            t.bytes_transferred = len(data)
            await sandbox_runtime.upload_file(data, f"{wd}/large_{size_mb}mb.bin")
        async with timed("stress", f"download_{size_mb}mb") as t:
            t.bytes_transferred = len(data)
            downloaded = await sandbox_runtime.download_file(
                f"{wd}/large_{size_mb}mb.bin"
            )
        assert len(downloaded) == len(data)


class TestRapidExecution:
    async def test_rapid_exec_50(self, sandbox_runtime, timed):
        """50 sequential exec calls."""
        async with timed("stress", "rapid_exec_50"):
            for i in range(50):
                result = await sandbox_runtime.exec(f"echo {i}")
                assert result.exit_code == 0

    async def test_rapid_code_run_20(self, sandbox_runtime, timed):
        """20 sequential code_run calls."""
        async with timed("stress", "rapid_code_run_20"):
            for i in range(20):
                result = await sandbox_runtime.code_run(f"print({i})")
                assert result.exit_code == 0


class TestBatchFileIO:
    async def test_batch_upload_100(self, sandbox_runtime, timed):
        """Upload 100 small files, then list them."""
        wd = await sandbox_runtime.fetch_working_dir()
        files = [
            (f"file_{i}".encode(), f"{wd}/batch/file_{i}.txt")
            for i in range(100)
        ]
        async with timed("stress", "batch_upload_100"):
            await sandbox_runtime.upload_files(files)
        async with timed("stress", "list_100"):
            entries = await sandbox_runtime.list_files(f"{wd}/batch")
        assert len(entries) >= 100
