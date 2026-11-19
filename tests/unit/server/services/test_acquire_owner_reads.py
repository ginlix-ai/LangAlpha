"""An acquire gives the computer what its owner has, whoever acquires it.

The caller and the computer's owner are one account on every path that runs
today, so these hand the acquire a different caller to show which read takes
which: what the computer is given (connectors, vault, skills) is read for the
binding's owner, and the caller is passed on only for the tokens.
"""

from __future__ import annotations

import uuid
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.server.services.workspace_manager import WorkspaceManager
from tests.computer_manager_patch import cm_patch
from tests.unit.server.services.test_workspace_manager_mcp import (
    _make_binding,
    _make_config,
    _make_session,
)

SENDER = "user-sender"
OWNER = "user-owner"


@pytest.fixture(autouse=True)
def _fresh_manager():
    WorkspaceManager.reset_instance()
    yield
    WorkspaceManager.reset_instance()


@pytest.mark.asyncio
async def test_a_warm_acquire_resolves_the_computer_for_its_owner_not_the_caller():
    wm = WorkspaceManager.get_instance(config=_make_config())
    ws_id, computer_id = str(uuid.uuid4()), str(uuid.uuid4())
    wm.resolve_binding = AsyncMock(return_value=_make_binding(ws_id, computer_id, user_id=OWNER))
    wm._ensure_project_attached = AsyncMock()
    wm._apply_session_mcp = AsyncMock(return_value=None)
    wm._keep_computer_tokens = AsyncMock()
    wm._settle_folders = AsyncMock()
    session = _make_session(version=0, summary="cached", config_owner=ws_id)
    wm._machine(computer_id).session = session
    wm._record_sync(computer_id)  # inside the cooldown: the warm return

    with cm_patch(
        "db_get_workspace_identity",
        AsyncMock(return_value={"status": "running", "sandbox_id": "sb-1"}),
    ):
        assert await wm.get_session_for_workspace(ws_id, user_id=SENDER) is session

    assert wm._apply_session_mcp.await_args.args[1] == OWNER
    # The folder's attach: its tool overlay, skill links and file mount.
    assert wm._ensure_project_attached.await_args.kwargs["user_id"] == OWNER
    # The tokens are kept for the caller (``_keep_computer_tokens``).
    assert wm._keep_computer_tokens.await_args.args[1] == SENDER


@pytest.mark.asyncio
async def test_a_skill_reconcile_reads_the_workspace_rows_owner():
    wm = WorkspaceManager.get_instance(config=_make_config())
    wm.get_session_if_ready_for_work = AsyncMock(return_value=MagicMock(sandbox="sandbox"))
    wm._reconcile_skills = AsyncMock()
    row = {"workspace_id": "ws-1", "user_id": OWNER, "sandbox_id": "sb-1"}
    with patch(
        "src.server.services.computer_manager._mcp.db_get_workspace", AsyncMock(return_value=row)
    ):
        await wm.reconcile_skills_if_running("ws-1", SENDER, source="post_turn")

    assert wm.get_session_if_ready_for_work.await_args.kwargs["owner_id"] == OWNER
    assert wm._reconcile_skills.await_args.args[:2] == ("ws-1", OWNER)
