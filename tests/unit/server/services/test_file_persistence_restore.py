"""
Unit tests for FilePersistenceService.restore_to_sandbox parallel path.

The restore path previously ran files through a batch-of-10
``asyncio.gather`` loop and issued one ``mkdir -p`` per file. The new
path:

1. Collects unique parent directories and issues a single bulk mkdir.
2. Uploads all files through a semaphore-bounded worker pool so the
   next upload starts the instant any slot frees up — no per-batch
   wait for the slowest file.

These tests pin that behavior so regressions of the restore latency
budget are visible in CI.
"""

from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.server.services.persistence.file import FilePersistenceService


def _file(path: str, text: str = "hello") -> dict:
    return {
        "file_path": path,
        "is_binary": False,
        "content_binary": None,
        "content_text": text,
    }


def _mock_sandbox() -> MagicMock:
    sandbox = MagicMock()
    sandbox.working_dir = "/workspace"
    sandbox.acreate_directories = AsyncMock(return_value=True)
    sandbox.acreate_directory = AsyncMock(return_value=True)
    sandbox.aupload_file_bytes = AsyncMock(return_value=True)
    return sandbox


@pytest.mark.asyncio
@patch("src.server.services.persistence.file.update_file_mtime", new_callable=AsyncMock)
@patch("src.server.services.persistence.file.get_files_for_workspace", new_callable=AsyncMock)
async def test_restore_bulk_creates_unique_dirs_once(mock_get, mock_update):
    """All unique parent dirs are created in a SINGLE acreate_directories
    call. 50 files across 3 unique parent dirs → 1 bulk mkdir, not 50."""
    files = (
        [_file(f"data/a/file_{i}.txt") for i in range(20)]
        + [_file(f"data/b/file_{i}.txt") for i in range(20)]
        + [_file(f"reports/file_{i}.txt") for i in range(10)]
    )
    mock_get.return_value = files

    sandbox = _mock_sandbox()
    # short-circuit post-restore mtime sync to keep the test focused
    with patch.object(
        FilePersistenceService, "list_sandbox_files", new=AsyncMock(return_value={})
    ):
        result = await FilePersistenceService.restore_to_sandbox("ws-1", sandbox)

    assert result == {"restored": 50, "errors": 0}

    # Exactly one bulk-mkdir call, with all three unique dirs.
    sandbox.acreate_directories.assert_awaited_once()
    dirs_arg = sandbox.acreate_directories.await_args.args[0]
    dirs_set = set(dirs_arg)
    assert dirs_set == {"/workspace/data/a", "/workspace/data/b", "/workspace/reports"}

    # Per-file mkdir is NOT invoked on the happy bulk-success path.
    sandbox.acreate_directory.assert_not_awaited()

    # Upload is called once per file (+1 for the sync marker at the end).
    upload_paths = [c.args[0] for c in sandbox.aupload_file_bytes.await_args_list]
    file_uploads = [p for p in upload_paths if not p.endswith(".file_sync_marker")]
    assert len(file_uploads) == 50


@pytest.mark.asyncio
@patch("src.server.services.persistence.file.get_files_for_workspace", new_callable=AsyncMock)
async def test_restore_falls_back_to_per_dir_when_bulk_fails(mock_get):
    """If acreate_directories returns False (e.g. command line too long
    for huge workspaces), restore falls back to parallel per-dir creates
    before uploads start."""
    mock_get.return_value = [_file("a/x.txt"), _file("b/y.txt")]
    sandbox = _mock_sandbox()
    sandbox.acreate_directories = AsyncMock(return_value=False)

    with patch.object(
        FilePersistenceService, "list_sandbox_files", new=AsyncMock(return_value={})
    ):
        result = await FilePersistenceService.restore_to_sandbox("ws-1", sandbox)

    assert result == {"restored": 2, "errors": 0}
    sandbox.acreate_directories.assert_awaited_once()
    # Fallback fires acreate_directory per unique parent dir.
    dirs_from_fallback = {
        call.args[0] for call in sandbox.acreate_directory.await_args_list
    }
    assert dirs_from_fallback == {"/workspace/a", "/workspace/b"}
    upload_paths = [c.args[0] for c in sandbox.aupload_file_bytes.await_args_list]
    file_uploads = [p for p in upload_paths if not p.endswith(".file_sync_marker")]
    assert len(file_uploads) == 2


@pytest.mark.asyncio
@patch("src.server.services.persistence.file.get_files_for_workspace", new_callable=AsyncMock)
async def test_restore_isolates_per_file_failures(mock_get):
    """A single failed upload doesn't block the rest. 5 files, one
    raises, one returns False — healthy 3 still restore, error count
    tallied correctly, method does not raise."""
    mock_get.return_value = [_file(f"f_{i}.txt") for i in range(5)]
    sandbox = _mock_sandbox()

    call_count = {"n": 0}

    async def flaky_upload(_path, _content):
        call_count["n"] += 1
        if call_count["n"] == 2:
            raise RuntimeError("transient network blip")
        if call_count["n"] == 3:
            return False
        return True

    sandbox.aupload_file_bytes = AsyncMock(side_effect=flaky_upload)

    with patch.object(
        FilePersistenceService, "list_sandbox_files", new=AsyncMock(return_value={})
    ):
        result = await FilePersistenceService.restore_to_sandbox("ws-1", sandbox)

    assert result["restored"] == 3
    assert result["errors"] == 2
    # 5 file uploads + 1 sync-marker upload (sync marker runs in a best-effort
    # try/except so it runs even when some files errored).
    assert sandbox.aupload_file_bytes.await_count == 6


@pytest.mark.asyncio
@patch("src.server.services.persistence.file.get_files_for_workspace", new_callable=AsyncMock)
async def test_restore_caps_concurrency_at_semaphore_size(mock_get):
    """Worker pool semaphore caps concurrent uploads at 16. With 40
    files each taking a tick, peak in-flight must never exceed 16."""
    mock_get.return_value = [_file(f"f_{i}.txt") for i in range(40)]
    sandbox = _mock_sandbox()

    inflight = 0
    peak = 0

    async def tracking_upload(_path, _content):
        nonlocal inflight, peak
        inflight += 1
        peak = max(peak, inflight)
        await asyncio.sleep(0.002)
        inflight -= 1
        return True

    sandbox.aupload_file_bytes = AsyncMock(side_effect=tracking_upload)

    with patch.object(
        FilePersistenceService, "list_sandbox_files", new=AsyncMock(return_value={})
    ):
        await FilePersistenceService.restore_to_sandbox("ws-1", sandbox)

    assert peak <= 16, f"Concurrency cap breached: peak={peak}"
    # Sanity: parallelism actually happened (not forced to 1).
    assert peak > 1, f"Expected parallel uploads but peak={peak}"


@pytest.mark.asyncio
@patch("src.server.services.persistence.file.get_files_for_workspace", new_callable=AsyncMock)
async def test_restore_empty_file_list_is_noop(mock_get):
    """Zero files → no sandbox calls, no errors."""
    mock_get.return_value = []
    sandbox = _mock_sandbox()

    result = await FilePersistenceService.restore_to_sandbox("ws-1", sandbox)

    assert result == {"restored": 0, "errors": 0}
    sandbox.acreate_directories.assert_not_awaited()
    sandbox.aupload_file_bytes.assert_not_awaited()
