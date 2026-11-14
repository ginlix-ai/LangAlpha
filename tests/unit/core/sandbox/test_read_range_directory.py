"""A Read of a directory, run through a real shell as the sandbox runs it.

sed fails on a directory, and the fallback's download then got a 400 that was
classified as the sandbox failing: the agent was told the sandbox was
unreachable and the run was marked failed for a path it named wrongly.
"""

import subprocess
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from ptc_agent.core.sandbox import files


async def _exec(command: str, timeout: int = 60):
    done = subprocess.run(["/bin/sh", "-c", command], capture_output=True, text=True)
    return SimpleNamespace(stdout=done.stdout, stderr=done.stderr, exit_code=done.returncode)


def _sandbox() -> SimpleNamespace:
    async def _call(fn, *args, retry_policy=None, **kwargs):
        return await fn(*args, **kwargs)

    return SimpleNamespace(
        _wait_ready=AsyncMock(),
        normalize_path=lambda path: path,
        runtime=SimpleNamespace(exec=_exec),
        _runtime_call=_call,
        _aread_file_range_fallback=AsyncMock(return_value=None),
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("suffix", ["", "/"])
async def test_a_directory_is_named_as_one(tmp_path: Path, suffix: str):
    sandbox = _sandbox()
    with pytest.raises(IsADirectoryError):
        await files.aread_file_range(sandbox, f"{tmp_path}{suffix}", 0, 10)
    sandbox._aread_file_range_fallback.assert_not_awaited()


@pytest.mark.asyncio
async def test_a_file_reads_its_range(tmp_path: Path):
    target = tmp_path / "a file.txt"
    target.write_text("a\nb\nc\n")
    assert await files.aread_file_range(_sandbox(), str(target), 1, 1) == "b\n"
