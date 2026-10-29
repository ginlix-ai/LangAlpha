"""Contract tests for data persistence across stop/start cycles."""

from __future__ import annotations

import logging
import shlex

import pytest

logger = logging.getLogger(__name__)

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]


async def _stop_start(runtime, *, stop_timeout: int = 180) -> None:
    """Stop and restart a runtime, tolerating slow provider stop operations."""
    try:
        await runtime.stop(timeout=stop_timeout)
    except Exception as exc:
        # Daytona stop() can timeout under load — retry once after a brief wait
        logger.warning("stop() failed (%s), retrying once", exc)
        import asyncio

        await asyncio.sleep(5)
        await runtime.stop(timeout=stop_timeout)
    await runtime.start()


class TestDataPersistenceAcrossCycles:
    """Verify files survive stop -> start cycles (provider contract)."""

    async def test_files_persist_across_stop_start(self, sandbox_runtime, timed):
        wd = await sandbox_runtime.fetch_working_dir()
        await sandbox_runtime.upload_file(b"persistent data", f"{wd}/persist.txt")

        async with timed("persistence", "stop_start_persist"):
            await _stop_start(sandbox_runtime)

        content = await sandbox_runtime.download_file(f"{wd}/persist.txt")
        assert content == b"persistent data"

    async def test_python_output_files_persist(self, sandbox_runtime, timed):
        wd = await sandbox_runtime.fetch_working_dir()
        code = f"with open('{wd}/code_file.txt', 'w') as f: f.write('from python')"
        await sandbox_runtime.exec(f"python3 -c {shlex.quote(code)}")

        async with timed("persistence", "stop_start_code_persist"):
            await _stop_start(sandbox_runtime)

        content = await sandbox_runtime.download_file(f"{wd}/code_file.txt")
        assert content == b"from python"

    async def test_multiple_stop_start_cycles(self, sandbox_runtime, timed):
        wd = await sandbox_runtime.fetch_working_dir()
        await sandbox_runtime.upload_file(b"cycle_data", f"{wd}/cycle.txt")

        async with timed("persistence", "multiple_cycles"):
            for i in range(3):
                await _stop_start(sandbox_runtime)
                content = await sandbox_runtime.download_file(f"{wd}/cycle.txt")
                assert content == b"cycle_data", f"Failed on cycle {i}"
