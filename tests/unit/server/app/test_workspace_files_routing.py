"""
Integration-style tests for the ``starting`` status routing in
``src/server/app/workspace_files.py``.

These pin the incident-day invariant: while a workspace is in the
intermediate ``starting`` state (lazy init in flight, or Phase 2 failed),
a concurrent ``GET /files`` call must route to the DB fallback. Before
Fix 1 the DB read side only checked ``stopped``/``stopping`` so a
concurrent request during lazy init went straight to live sandbox
acquisition and collided with the in-flight Daytona restore — 503 storm.

Plan item #8 (``investigate-backend-1-info-zesty-sketch.md``): request A
fails lazy init, request B calls ``/files`` while status is
``starting`` → B returns DB fallback, not 503.
"""

from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, patch

import pytest

from ptc_agent.core.sandbox.runtime import SandboxTransientError
from src.server.app.workspace_files import list_workspace_files


def _workspace(ws_id: str, user_id: str, status: str) -> dict:
    return {
        "workspace_id": ws_id,
        "user_id": user_id,
        "status": status,
        "config": None,
        "sandbox_id": "sb-existing",
    }


@pytest.mark.asyncio
@patch("src.server.app.workspace_files._get_work_dir", return_value="/home/workspace")
@patch("src.server.app.workspace_files.FilePersistenceService")
@patch("src.server.app.workspace_files.db_get_workspace")
async def test_starting_status_routes_to_db_fallback(
    mock_get_ws, mock_fp, _mock_wd,
):
    """Happy path: status='starting' → handler returns the DB file tree
    with ``source: 'database'``, never touching the sandbox."""
    ws_id = "ws-starting"
    mock_get_ws.return_value = _workspace(ws_id, "user-1", status="starting")
    mock_fp.get_file_tree = AsyncMock(
        return_value=[
            {"path": "results/summary.md"},
            {"path": "data/daily.csv"},
        ]
    )

    result = await list_workspace_files(
        workspace_id=ws_id,
        x_user_id="user-1",
        path=".",
        include_system=False,
        pattern="**/*",
        wait_for_sandbox=False,
        auto_start=False,
    )

    assert result["source"] == "database"
    assert result["sandbox_ready"] is False
    assert set(result["files"]) == {"results/summary.md", "data/daily.csv"}
    mock_fp.get_file_tree.assert_awaited_once_with(ws_id)


@pytest.mark.asyncio
@patch("src.server.app.workspace_files._get_work_dir", return_value="/home/workspace")
@patch("src.server.app.workspace_files.FilePersistenceService")
@patch("src.server.app.workspace_files.db_get_workspace")
async def test_files_during_concurrent_failing_lazy_init(
    mock_get_ws, mock_fp, _mock_wd,
):
    """Plan item #8: A (``get_session_for_workspace`` with failing Phase 2)
    races against B (``list_workspace_files``). As long as the DB row reads
    ``status='starting'`` when B arrives, B must route to the DB fallback
    and not raise 503 — even if A is mid-failure."""
    ws_id = "ws-racing"
    mock_get_ws.return_value = _workspace(ws_id, "user-1", status="starting")
    mock_fp.get_file_tree = AsyncMock(return_value=[{"path": "results/x.txt"}])

    async def failing_request_a() -> Exception:
        """Simulate ``WorkspaceManager.get_session_for_workspace`` raising
        a SandboxTransientError from Phase 2 while B is reading /files."""
        await asyncio.sleep(0.005)
        raise SandboxTransientError("phase 2 init exhausted retries")

    async def request_b() -> dict:
        # B intentionally has no knowledge of A — it just reads /files.
        return await list_workspace_files(
            workspace_id=ws_id,
            x_user_id="user-1",
            path=".",
            include_system=False,
            pattern="**/*",
            wait_for_sandbox=False,
            auto_start=False,
        )

    outcomes = await asyncio.gather(
        failing_request_a(),
        request_b(),
        return_exceptions=True,
    )

    a_outcome, b_outcome = outcomes
    assert isinstance(a_outcome, SandboxTransientError)  # A failed, as expected
    assert isinstance(b_outcome, dict), f"B should not raise 503 / error: {b_outcome!r}"
    assert b_outcome["source"] == "database"
    assert b_outcome["files"] == ["results/x.txt"]
