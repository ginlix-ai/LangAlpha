"""``/api/v1/sessions`` reads the live sessions off ComputerManager directly."""

from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.server.app.sessions import get_sessions
from src.server.services.computer_manager import ComputerManager, SessionMetadata


def _manager(meta: dict) -> MagicMock:
    manager = MagicMock(spec=ComputerManager)
    manager.idle_timeout = 900
    manager.cleanup_interval = 60
    manager._machines = {}
    for computer_id, session_meta in meta.items():
        ComputerManager._machine(manager, computer_id).meta = session_meta
    manager.live_session_stats = lambda: ComputerManager.live_session_stats(manager)
    return manager


@pytest.mark.asyncio
async def test_reports_the_managers_live_sessions():
    created = datetime(2026, 1, 1, tzinfo=timezone.utc)
    meta = SessionMetadata(
        workspace_id="ws-1",
        computer_id="comp-1",
        created_at=created,
        last_active=created,
        sandbox_id="sandbox-abc",
        request_count=3,
    )
    with (
        patch.object(
            ComputerManager, "current", return_value=_manager({"comp-1": meta})
        ),
        patch(
            "src.server.app.sessions.get_computers_for_user",
            AsyncMock(return_value=[{"computer_id": "comp-1"}]),
        ),
    ):
        stats = await get_sessions("user-1")

    assert stats["active_sessions"] == 1
    assert stats["idle_timeout"] == 900
    assert stats["cleanup_interval"] == 60
    assert stats["workspaces"] == [
        {
            "workspace_id": "ws-1",
            "computer_id": "comp-1",
            "created_at": created.isoformat(),
            "last_active": created.isoformat(),
            "request_count": 3,
            "sandbox_id": "sandbox-abc",
        }
    ]


@pytest.mark.asyncio
async def test_answers_empty_without_a_manager():
    """Before the manager is constructed, and after a reset, the endpoint
    reports zero rather than raising."""
    with patch.object(ComputerManager, "current", return_value=None):
        stats = await get_sessions("user-1")
    assert stats["active_sessions"] == 0
    assert "workspaces" not in stats


@pytest.mark.asyncio
async def test_lists_only_the_callers_machines():
    """A workspace id is the credential for the public file route, so another
    user's live session must not be readable here."""
    created = datetime(2026, 1, 1, tzinfo=timezone.utc)
    mine = SessionMetadata(
        workspace_id="ws-1",
        computer_id="comp-1",
        created_at=created,
        last_active=created,
        sandbox_id="sandbox-abc",
        request_count=1,
    )
    theirs = SessionMetadata(
        workspace_id="ws-2",
        computer_id="comp-2",
        created_at=created,
        last_active=created,
        sandbox_id="sandbox-def",
        request_count=1,
    )
    with (
        patch.object(
            ComputerManager,
            "current",
            return_value=_manager({"comp-1": mine, "comp-2": theirs}),
        ),
        patch(
            "src.server.app.sessions.get_computers_for_user",
            AsyncMock(return_value=[{"computer_id": "comp-1"}]),
        ),
    ):
        stats = await get_sessions("user-1")

    assert stats["active_sessions"] == 1
    assert [s["workspace_id"] for s in stats["workspaces"]] == ["ws-1"]
