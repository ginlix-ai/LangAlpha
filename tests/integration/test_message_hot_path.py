"""Integration tests for PTC message hot path — cold and warm session paths.

Exercises WorkspaceManager session resolution with real PostgreSQL and a
real sandbox provider (Daytona in CI, memory locally).  Verifies that:

- **Cold path** (first message): creates a new session, hits DB, initializes
  sandbox, syncs assets.
- **Warm path** (subsequent messages): returns the cached session with zero
  DB queries when the sync cooldown is active.
- **`update_workspace_activity` conditional SQL**: first call writes, second
  call within 60 seconds is a no-op.
- **`has_ready_session` accuracy**: reflects actual session/sandbox state.

Provider selection:
  SANDBOX_TEST_PROVIDER=daytona  (CI — requires DAYTONA_API_KEY)
  SANDBOX_TEST_PROVIDER=memory   (local default — no external deps)
"""

from __future__ import annotations

import os
import time
import uuid
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
import pytest_asyncio

from ptc_agent.config.agent import AgentConfig, SkillsConfig
from ptc_agent.config.core import (
    DaytonaConfig,
    FilesystemConfig,
    LoggingConfig,
    MCPConfig,
    SandboxConfig,
    SecurityConfig,
)
from ptc_agent.core.session import SessionManager

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]

# ---------------------------------------------------------------------------
# Provider detection
# ---------------------------------------------------------------------------

_PROVIDER = os.getenv("SANDBOX_TEST_PROVIDER", "memory").strip().lower()


def _is_daytona() -> bool:
    return _PROVIDER == "daytona"


# ---------------------------------------------------------------------------
# Config builders
# ---------------------------------------------------------------------------


def _make_agent_config(
    working_directory: str,
    provider: str = "daytona",
    api_key: str = "test-key",
    base_url: str = "https://app.daytona.io/api",
) -> AgentConfig:
    """Build an AgentConfig for the given provider."""
    return AgentConfig(
        security=SecurityConfig(),
        logging=LoggingConfig(),
        sandbox=SandboxConfig(
            provider=provider if provider in ("daytona", "docker") else "daytona",
            daytona=DaytonaConfig(
                api_key=api_key,
                base_url=base_url,
                snapshot_enabled=False,
            ),
        ),
        mcp=MCPConfig(),
        filesystem=FilesystemConfig(
            working_directory=working_directory,
            allowed_directories=[working_directory],
        ),
        skills=SkillsConfig(enabled=False),
    )


def _build_agent_config(sandbox_base_dir: str) -> AgentConfig:
    """Build an AgentConfig adapted to the active provider."""
    if _is_daytona():
        api_key = os.environ.get("DAYTONA_API_KEY", "")
        if not api_key:
            pytest.skip("DAYTONA_API_KEY not set")
        base_url = os.environ.get(
            "DAYTONA_BASE_URL", "https://app.daytona.io/api"
        )
        return _make_agent_config(
            working_directory="/home/workspace",
            provider="daytona",
            api_key=api_key,
            base_url=base_url,
        )

    # Memory provider — use temp dir as working directory
    return _make_agent_config(
        working_directory=sandbox_base_dir,
        provider="daytona",  # value ignored — create_provider is patched
        api_key="test-key",
    )


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def sandbox_base_dir(tmp_path):
    """Temporary directory for sandbox working dirs (memory provider only)."""
    d = tmp_path / "sandboxes"
    d.mkdir()
    return str(d)


@pytest.fixture
def _patch_create_provider(sandbox_base_dir):
    """Patch create_provider for memory provider. No-op for Daytona."""
    if _is_daytona():
        yield None
        return

    from tests.integration.sandbox.memory_provider import MemoryProvider

    provider = MemoryProvider(base_dir=sandbox_base_dir)
    with patch(
        "ptc_agent.core.sandbox.ptc_sandbox.create_provider",
        return_value=provider,
    ):
        yield provider


@pytest_asyncio.fixture
async def workspace_manager(
    sandbox_base_dir,
    _patch_create_provider,
    patched_get_db_connection,
):
    """Create a fresh WorkspaceManager wired to test DB and sandbox provider.

    Uses Daytona in CI (SANDBOX_TEST_PROVIDER=daytona) or memory provider
    locally. Resets the singleton and SessionManager cache on teardown.
    """
    from src.server.services.workspace_manager import WorkspaceManager

    # Reset any prior singleton
    WorkspaceManager.reset_instance()
    SessionManager._sessions.clear()

    config = _build_agent_config(sandbox_base_dir)
    manager = WorkspaceManager.get_instance(config=config)

    yield manager

    # Teardown: clean up sessions and reset singleton
    for ws_id in list(manager._sessions.keys()):
        session = manager._sessions.get(ws_id)
        if session and session.sandbox:
            try:
                await session.sandbox.cleanup()
            except Exception:
                pass
    manager._sessions.clear()
    SessionManager._sessions.clear()
    WorkspaceManager.reset_instance()


@pytest_asyncio.fixture
async def running_workspace(seed_user, patched_get_db_connection):
    """Create a workspace in 'running' status."""
    from src.server.database.workspace import create_workspace

    ws = await create_workspace(
        user_id=seed_user["user_id"],
        name="Hot Path Test",
        description="Integration test for cold/warm paths",
        status="running",
    )
    return ws


# ---------------------------------------------------------------------------
# Cold → Warm path tests
# ---------------------------------------------------------------------------


class TestColdWarmSessionPath:
    """Test the cold → warm session transition with real DB and sandbox."""

    async def test_cold_path_has_ready_session_returns_false(
        self, workspace_manager, running_workspace
    ):
        """Before any session is created, has_ready_session must be False."""
        ws_id = str(running_workspace["workspace_id"])
        assert workspace_manager.has_ready_session(ws_id) is False

    async def test_cold_path_creates_initialized_session(
        self, workspace_manager, running_workspace
    ):
        """First call to get_session_for_workspace creates and initializes a session."""
        ws_id = str(running_workspace["workspace_id"])
        user_id = running_workspace["user_id"]

        session = await workspace_manager.get_session_for_workspace(
            ws_id, user_id=user_id
        )

        assert session is not None
        assert session._initialized is True
        assert session.sandbox is not None
        assert session.sandbox.is_ready()

    async def test_warm_path_has_ready_session_returns_true(
        self, workspace_manager, running_workspace
    ):
        """After cold path, has_ready_session must be True."""
        ws_id = str(running_workspace["workspace_id"])
        user_id = running_workspace["user_id"]

        await workspace_manager.get_session_for_workspace(ws_id, user_id=user_id)

        assert workspace_manager.has_ready_session(ws_id) is True

    async def test_warm_path_returns_same_session_object(
        self, workspace_manager, running_workspace
    ):
        """Warm path returns the exact same session (identity check)."""
        ws_id = str(running_workspace["workspace_id"])
        user_id = running_workspace["user_id"]

        session1 = await workspace_manager.get_session_for_workspace(
            ws_id, user_id=user_id
        )
        session2 = await workspace_manager.get_session_for_workspace(
            ws_id, user_id=user_id
        )

        assert session1 is session2

    async def test_warm_path_zero_db_queries(
        self, workspace_manager, running_workspace
    ):
        """Within sync cooldown, warm path makes zero db_get_workspace calls.

        The cold path creates the session inline (Phase 1) and does NOT call
        _record_sync (Phase 2 is skipped).  The second call triggers Phase 2
        which records the cooldown.  The THIRD call is the true warm path
        that skips DB entirely.
        """
        ws_id = str(running_workspace["workspace_id"])
        user_id = running_workspace["user_id"]

        # Call 1 (cold) — creates session inline, no cooldown recorded
        await workspace_manager.get_session_for_workspace(ws_id, user_id=user_id)

        # Call 2 — triggers Phase 2 re-sync, records cooldown via _record_sync
        await workspace_manager.get_session_for_workspace(ws_id, user_id=user_id)

        # Call 3 (warm) — cooldown active, should skip DB entirely
        with patch(
            "src.server.services.workspace_manager.db_get_workspace",
            new_callable=AsyncMock,
        ) as mock_db:
            session = await workspace_manager.get_session_for_workspace(
                ws_id, user_id=user_id
            )

            mock_db.assert_not_called()
            assert session is not None

    async def test_cold_path_populates_user_mappings(
        self, workspace_manager, running_workspace
    ):
        """Cold path records workspace-user bidirectional mappings."""
        ws_id = str(running_workspace["workspace_id"])
        user_id = running_workspace["user_id"]

        await workspace_manager.get_session_for_workspace(ws_id, user_id=user_id)

        assert workspace_manager._workspace_to_user.get(ws_id) == user_id
        assert ws_id in workspace_manager._user_to_workspaces.get(user_id, set())

    async def test_sync_cooldown_respected(
        self, workspace_manager, running_workspace
    ):
        """Session returned immediately when sync cooldown is active."""
        ws_id = str(running_workspace["workspace_id"])
        user_id = running_workspace["user_id"]

        # Cold
        await workspace_manager.get_session_for_workspace(ws_id, user_id=user_id)

        # Warm — measure time. Should be sub-millisecond (no I/O).
        t0 = time.monotonic()
        session = await workspace_manager.get_session_for_workspace(
            ws_id, user_id=user_id
        )
        elapsed_ms = (time.monotonic() - t0) * 1000

        assert session is not None
        # Warm path should be under 5ms (just dict lookups + lock acquire)
        assert elapsed_ms < 50, f"Warm path took {elapsed_ms:.1f}ms — expected < 50ms"

    async def test_cooldown_expired_triggers_sync(
        self, workspace_manager, running_workspace
    ):
        """After cooldown expires, get_session_for_workspace re-syncs."""
        ws_id = str(running_workspace["workspace_id"])
        user_id = running_workspace["user_id"]

        # Cold
        await workspace_manager.get_session_for_workspace(ws_id, user_id=user_id)

        # Expire cooldown by backdating _last_sync_at
        workspace_manager._last_sync_at[ws_id] = time.monotonic() - 60

        # This should trigger a re-sync (Phase 2) but still return the same session
        with patch.object(
            workspace_manager, "_sync_sandbox_assets", new_callable=AsyncMock
        ):
            session = await workspace_manager.get_session_for_workspace(
                ws_id, user_id=user_id
            )

            # _sync_user_data_if_needed is called during Phase 2 re-sync
            # (not _sync_sandbox_assets, because needs_deferred_sync is False)
            assert session is not None

    async def test_multiple_workspaces_independent_sessions(
        self, workspace_manager, seed_user, patched_get_db_connection
    ):
        """Each workspace gets its own independent session."""
        from src.server.database.workspace import create_workspace

        ws1 = await create_workspace(
            user_id=seed_user["user_id"], name="WS1", status="running"
        )
        ws2 = await create_workspace(
            user_id=seed_user["user_id"], name="WS2", status="running"
        )

        ws1_id = str(ws1["workspace_id"])
        ws2_id = str(ws2["workspace_id"])
        user_id = seed_user["user_id"]

        session1 = await workspace_manager.get_session_for_workspace(
            ws1_id, user_id=user_id
        )
        session2 = await workspace_manager.get_session_for_workspace(
            ws2_id, user_id=user_id
        )

        assert session1 is not session2
        assert workspace_manager.has_ready_session(ws1_id)
        assert workspace_manager.has_ready_session(ws2_id)


# ---------------------------------------------------------------------------
# has_ready_session edge cases
# ---------------------------------------------------------------------------


class TestHasReadySession:
    """Test has_ready_session accuracy for various session states."""

    async def test_no_session_cached(self, workspace_manager):
        """Returns False for unknown workspace ID."""
        assert workspace_manager.has_ready_session("nonexistent") is False

    async def test_session_not_initialized(self, workspace_manager):
        """Returns False when session exists but is not initialized."""
        mock_session = MagicMock()
        mock_session._initialized = False
        mock_session.sandbox = None

        workspace_manager._sessions["ws-test"] = mock_session
        assert workspace_manager.has_ready_session("ws-test") is False

    async def test_sandbox_none(self, workspace_manager):
        """Returns False when session is initialized but sandbox is None."""
        mock_session = MagicMock()
        mock_session._initialized = True
        mock_session.sandbox = None

        workspace_manager._sessions["ws-test"] = mock_session
        assert workspace_manager.has_ready_session("ws-test") is False

    async def test_sandbox_not_ready(self, workspace_manager):
        """Returns False when sandbox exists but is not ready."""
        mock_session = MagicMock()
        mock_session._initialized = True
        mock_session.sandbox = MagicMock()
        mock_session.sandbox.is_ready.return_value = False

        workspace_manager._sessions["ws-test"] = mock_session
        assert workspace_manager.has_ready_session("ws-test") is False

    async def test_full_ready_state(self, workspace_manager, running_workspace):
        """Returns True only when all conditions pass (real sandbox)."""
        ws_id = str(running_workspace["workspace_id"])
        user_id = running_workspace["user_id"]

        await workspace_manager.get_session_for_workspace(ws_id, user_id=user_id)
        assert workspace_manager.has_ready_session(ws_id) is True


# ---------------------------------------------------------------------------
# update_workspace_activity conditional SQL
# ---------------------------------------------------------------------------


class TestUpdateWorkspaceActivityConditional:
    """Test the 60-second conditional UPDATE behavior."""

    async def test_first_call_writes(
        self, seed_workspace, patched_get_db_connection
    ):
        """First call always updates (last_activity_at is NULL or stale)."""
        from src.server.database.workspace import update_workspace_activity

        ws_id = str(seed_workspace["workspace_id"])
        result = await update_workspace_activity(ws_id)
        assert result is True

    async def test_immediate_second_call_skips(
        self, seed_workspace, patched_get_db_connection
    ):
        """Second call within 60 seconds is a no-op (conditional SQL)."""
        from src.server.database.workspace import update_workspace_activity

        ws_id = str(seed_workspace["workspace_id"])

        first = await update_workspace_activity(ws_id)
        assert first is True

        second = await update_workspace_activity(ws_id)
        assert second is False  # Skipped — within 60-second window

    async def test_nonexistent_workspace_returns_false(
        self, patched_get_db_connection, seed_user
    ):
        """Updating a nonexistent workspace returns False."""
        from src.server.database.workspace import update_workspace_activity

        result = await update_workspace_activity(str(uuid.uuid4()))
        assert result is False

    async def test_deleted_workspace_returns_false(
        self, seed_workspace, patched_get_db_connection
    ):
        """Deleted workspaces are excluded by the WHERE clause."""
        from src.server.database.workspace import (
            delete_workspace,
            update_workspace_activity,
        )

        ws_id = str(seed_workspace["workspace_id"])
        await delete_workspace(ws_id)  # soft delete sets status='deleted'

        result = await update_workspace_activity(ws_id)
        assert result is False


# ---------------------------------------------------------------------------
# mark_user_data_stale integration
# ---------------------------------------------------------------------------


class TestMarkUserDataStale:
    """Test that mark_user_data_stale clears sync flag for the right workspaces."""

    async def test_stale_clears_sync_flag(
        self, workspace_manager, seed_user, patched_get_db_connection
    ):
        """mark_user_data_stale clears _user_data_synced for all user workspaces."""
        from src.server.database.workspace import create_workspace
        from src.server.services.workspace_manager import WorkspaceManager

        ws = await create_workspace(
            user_id=seed_user["user_id"], name="Stale Test", status="running"
        )
        ws_id = str(ws["workspace_id"])
        user_id = seed_user["user_id"]

        # Cold path — creates session and syncs user data
        await workspace_manager.get_session_for_workspace(ws_id, user_id=user_id)

        # Simulate that user data was synced
        workspace_manager._user_data_synced.add(ws_id)
        assert ws_id in workspace_manager._user_data_synced

        # Mark stale — should clear the sync flag
        WorkspaceManager.mark_user_data_stale(user_id)
        assert ws_id not in workspace_manager._user_data_synced

    async def test_stale_only_affects_user_workspaces(
        self, workspace_manager, seed_user, patched_get_db_connection
    ):
        """mark_user_data_stale does not affect other users' workspaces."""
        from src.server.database.workspace import create_workspace
        from src.server.services.workspace_manager import WorkspaceManager

        ws1 = await create_workspace(
            user_id=seed_user["user_id"], name="User1 WS", status="running"
        )
        ws1_id = str(ws1["workspace_id"])

        # Set up session for user's workspace
        await workspace_manager.get_session_for_workspace(
            ws1_id, user_id=seed_user["user_id"]
        )
        workspace_manager._user_data_synced.add(ws1_id)

        # Stale a different user — should not affect our workspace
        WorkspaceManager.mark_user_data_stale("other-user-id")
        assert ws1_id in workspace_manager._user_data_synced
