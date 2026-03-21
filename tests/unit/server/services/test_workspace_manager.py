"""
Tests for WorkspaceManager service.

Tests workspace lifecycle: creation, session retrieval, stop, delete,
idle cleanup, singleton pattern, and background cleanup tasks.
"""

import asyncio
import time
import uuid
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.server.services.workspace_manager import WorkspaceManager


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_config():
    """Create a minimal mock AgentConfig."""
    config = MagicMock()
    config.to_core_config.return_value = MagicMock()
    config.daytona = MagicMock(api_key="test-key", base_url="https://daytona.test")
    config.sandbox = MagicMock(provider="daytona")
    config.filesystem = MagicMock(working_directory="/home/workspace")
    config.skills = MagicMock(enabled=False)
    return config


def _make_workspace(
    workspace_id=None,
    user_id="user-1",
    status="running",
    sandbox_id="sandbox-abc",
    **overrides,
):
    now = datetime.now(timezone.utc)
    data = {
        "workspace_id": workspace_id or str(uuid.uuid4()),
        "user_id": user_id,
        "name": "Test Workspace",
        "description": None,
        "sandbox_id": sandbox_id,
        "status": status,
        "mode": "ptc",
        "sort_order": 0,
        "created_at": now,
        "updated_at": now,
        "last_activity_at": now,
    }
    data.update(overrides)
    return data


def _make_mock_session(initialized=True, has_sandbox=True):
    session = MagicMock()
    session._initialized = initialized
    session.sandbox = MagicMock() if has_sandbox else None
    if has_sandbox:
        session.sandbox.sandbox_id = "sandbox-abc"
        session.sandbox.is_ready = MagicMock(return_value=True)
        session.sandbox.ensure_sandbox_ready = AsyncMock()
        session.sandbox.sync_sandbox_assets = AsyncMock()
    session.initialize = AsyncMock()
    session.initialize_lazy = AsyncMock()
    session.stop = AsyncMock()
    session.cleanup = AsyncMock()
    return session


# ---------------------------------------------------------------------------
# Singleton
# ---------------------------------------------------------------------------

class TestSingleton:
    """Test WorkspaceManager singleton pattern."""

    def setup_method(self):
        WorkspaceManager.reset_instance()

    def teardown_method(self):
        WorkspaceManager.reset_instance()

    def test_get_instance_requires_config_on_first_call(self):
        with pytest.raises(ValueError, match="config is required"):
            WorkspaceManager.get_instance()

    def test_get_instance_creates_singleton(self):
        config = _make_config()
        instance = WorkspaceManager.get_instance(config=config)
        assert instance is not None
        assert isinstance(instance, WorkspaceManager)

    def test_get_instance_returns_same_instance(self):
        config = _make_config()
        first = WorkspaceManager.get_instance(config=config)
        second = WorkspaceManager.get_instance()
        assert first is second

    def test_reset_instance_clears_singleton(self):
        config = _make_config()
        WorkspaceManager.get_instance(config=config)
        WorkspaceManager.reset_instance()
        with pytest.raises(ValueError, match="config is required"):
            WorkspaceManager.get_instance()


# ---------------------------------------------------------------------------
# Init and stats
# ---------------------------------------------------------------------------

class TestInitAndStats:
    """Test initialization and statistics."""

    def setup_method(self):
        WorkspaceManager.reset_instance()

    def teardown_method(self):
        WorkspaceManager.reset_instance()

    def test_init_sets_defaults(self):
        config = _make_config()
        wm = WorkspaceManager(config, idle_timeout=600, cleanup_interval=60)
        assert wm.idle_timeout == 600
        assert wm.cleanup_interval == 60
        assert wm._sessions == {}
        assert wm._shutdown is False

    def test_get_stats_empty(self):
        config = _make_config()
        wm = WorkspaceManager(config)
        stats = wm.get_stats()
        assert stats["cached_sessions"] == 0
        assert stats["cached_workspace_ids"] == []
        assert stats["idle_timeout"] == 1800

    def test_get_stats_with_sessions(self):
        config = _make_config()
        wm = WorkspaceManager(config)
        wm._sessions["ws-1"] = _make_mock_session()
        wm._sessions["ws-2"] = _make_mock_session()
        stats = wm.get_stats()
        assert stats["cached_sessions"] == 2
        assert set(stats["cached_workspace_ids"]) == {"ws-1", "ws-2"}


# ---------------------------------------------------------------------------
# create_workspace
# ---------------------------------------------------------------------------

class TestCreateWorkspace:
    """Test workspace creation."""

    def setup_method(self):
        WorkspaceManager.reset_instance()

    def teardown_method(self):
        WorkspaceManager.reset_instance()

    @pytest.mark.asyncio
    @patch("src.server.services.workspace_manager.update_workspace_status", new_callable=AsyncMock)
    @patch("src.server.services.workspace_manager.db_create_workspace", new_callable=AsyncMock)
    @patch("src.server.services.workspace_manager.SessionManager")
    @patch("src.server.services.workspace_manager.sync_user_data_to_sandbox", new_callable=AsyncMock)
    async def test_create_workspace_success(
        self, mock_sync_user, mock_sm, mock_db_create, mock_update_status
    ):
        ws_id = str(uuid.uuid4())
        created_ws = _make_workspace(workspace_id=ws_id, status="creating")
        updated_ws = _make_workspace(workspace_id=ws_id, status="running")

        mock_db_create.return_value = created_ws
        mock_update_status.return_value = updated_ws

        mock_session = _make_mock_session(initialized=False)
        mock_sm.get_session.return_value = mock_session

        config = _make_config()
        wm = WorkspaceManager(config)

        result = await wm.create_workspace(
            user_id="user-1", name="Test", description="desc"
        )

        assert result["status"] == "running"
        mock_db_create.assert_awaited_once()
        mock_session.initialize.assert_awaited_once()
        assert ws_id in wm._sessions

    @pytest.mark.asyncio
    @patch("src.server.services.workspace_manager.update_workspace_status", new_callable=AsyncMock)
    @patch("src.server.services.workspace_manager.db_create_workspace", new_callable=AsyncMock)
    @patch("src.server.services.workspace_manager.SessionManager")
    @patch("src.server.services.workspace_manager.sync_user_data_to_sandbox", new_callable=AsyncMock)
    async def test_create_workspace_sandbox_failure_marks_error(
        self, mock_sync_user, mock_sm, mock_db_create, mock_update_status
    ):
        ws_id = str(uuid.uuid4())
        created_ws = _make_workspace(workspace_id=ws_id, status="creating")
        mock_db_create.return_value = created_ws

        mock_session = _make_mock_session(initialized=False)
        mock_session.initialize.side_effect = RuntimeError("sandbox failed")
        mock_sm.get_session.return_value = mock_session

        config = _make_config()
        wm = WorkspaceManager(config)

        with pytest.raises(RuntimeError, match="sandbox failed"):
            await wm.create_workspace(user_id="user-1", name="Test")

        # Should have called update_workspace_status with error
        mock_update_status.assert_awaited()
        error_call = [
            c for c in mock_update_status.call_args_list
            if c.kwargs.get("status") == "error" or (len(c.args) > 1 and c.args[1] == "error")
        ]
        assert len(error_call) > 0


# ---------------------------------------------------------------------------
# stop_workspace
# ---------------------------------------------------------------------------

class TestStopWorkspace:
    """Test workspace stopping."""

    def setup_method(self):
        WorkspaceManager.reset_instance()

    def teardown_method(self):
        WorkspaceManager.reset_instance()

    @pytest.mark.asyncio
    @patch("src.server.services.workspace_manager.update_workspace_status", new_callable=AsyncMock)
    @patch("src.server.services.workspace_manager.db_get_workspace", new_callable=AsyncMock)
    @patch("src.server.services.workspace_manager.FilePersistenceService")
    async def test_stop_running_workspace(
        self, mock_file_svc, mock_db_get, mock_update_status
    ):
        ws_id = str(uuid.uuid4())
        mock_db_get.return_value = _make_workspace(workspace_id=ws_id, status="running")
        mock_file_svc.sync_to_db = AsyncMock()
        stopped_ws = _make_workspace(workspace_id=ws_id, status="stopped")
        mock_update_status.return_value = stopped_ws

        config = _make_config()
        wm = WorkspaceManager(config)
        mock_session = _make_mock_session()
        wm._sessions[ws_id] = mock_session
        wm._user_data_synced.add(ws_id)
        wm._last_sync_at[ws_id] = time.monotonic()

        result = await wm.stop_workspace(ws_id)

        assert result["status"] == "stopped"
        mock_session.stop.assert_awaited_once()
        assert ws_id not in wm._sessions
        assert ws_id not in wm._user_data_synced
        assert ws_id not in wm._last_sync_at

    @pytest.mark.asyncio
    @patch("src.server.services.workspace_manager.db_get_workspace", new_callable=AsyncMock)
    async def test_stop_workspace_not_found_raises(self, mock_db_get):
        mock_db_get.return_value = None
        config = _make_config()
        wm = WorkspaceManager(config)

        with pytest.raises(ValueError, match="not found"):
            await wm.stop_workspace("nonexistent")

    @pytest.mark.asyncio
    @patch("src.server.services.workspace_manager.db_get_workspace", new_callable=AsyncMock)
    async def test_stop_non_running_workspace_raises(self, mock_db_get):
        ws_id = str(uuid.uuid4())
        mock_db_get.return_value = _make_workspace(workspace_id=ws_id, status="stopped")
        config = _make_config()
        wm = WorkspaceManager(config)

        with pytest.raises(RuntimeError, match="Cannot stop"):
            await wm.stop_workspace(ws_id)


# ---------------------------------------------------------------------------
# delete_workspace
# ---------------------------------------------------------------------------

class TestDeleteWorkspace:
    """Test workspace deletion."""

    def setup_method(self):
        WorkspaceManager.reset_instance()

    def teardown_method(self):
        WorkspaceManager.reset_instance()

    @pytest.mark.asyncio
    @patch("src.server.services.workspace_manager.db_delete_workspace", new_callable=AsyncMock)
    @patch("src.server.services.workspace_manager.SessionManager")
    @patch("src.server.services.workspace_manager.db_get_workspace", new_callable=AsyncMock)
    @patch("src.server.services.workspace_manager.FilePersistenceService")
    async def test_delete_workspace_success(
        self, mock_file_svc, mock_db_get, mock_sm, mock_db_delete
    ):
        ws_id = str(uuid.uuid4())
        mock_db_get.return_value = _make_workspace(workspace_id=ws_id, status="running")
        mock_file_svc.sync_to_db = AsyncMock()
        mock_sm.cleanup_session = AsyncMock()

        config = _make_config()
        wm = WorkspaceManager(config)
        mock_session = _make_mock_session()
        wm._sessions[ws_id] = mock_session
        wm._user_data_synced.add(ws_id)

        result = await wm.delete_workspace(ws_id)

        assert result is True
        # Cleanup goes through SessionManager (single path, no double-cleanup)
        mock_sm.cleanup_session.assert_awaited_once_with(ws_id)
        mock_db_delete.assert_awaited_once_with(ws_id)
        assert ws_id not in wm._sessions
        assert ws_id not in wm._user_data_synced

    @pytest.mark.asyncio
    @patch("src.server.services.workspace_manager.db_get_workspace", new_callable=AsyncMock)
    async def test_delete_workspace_not_found_raises(self, mock_db_get):
        mock_db_get.return_value = None
        config = _make_config()
        wm = WorkspaceManager(config)

        with pytest.raises(ValueError, match="not found"):
            await wm.delete_workspace("nonexistent")


# ---------------------------------------------------------------------------
# cleanup_idle_workspaces
# ---------------------------------------------------------------------------

class TestCleanupIdle:
    """Test idle workspace cleanup."""

    def setup_method(self):
        WorkspaceManager.reset_instance()

    def teardown_method(self):
        WorkspaceManager.reset_instance()

    @pytest.mark.asyncio
    @patch("src.server.services.workspace_manager.get_workspaces_by_status", new_callable=AsyncMock)
    async def test_cleanup_idle_stops_old_workspaces(self, mock_get_by_status):
        ws_id = str(uuid.uuid4())
        old_time = datetime.now(timezone.utc) - timedelta(hours=2)
        mock_get_by_status.return_value = [
            _make_workspace(workspace_id=ws_id, last_activity_at=old_time),
        ]

        config = _make_config()
        wm = WorkspaceManager(config, idle_timeout=1800)

        with patch.object(wm, "stop_workspace", new_callable=AsyncMock) as mock_stop:
            count = await wm.cleanup_idle_workspaces()

        assert count == 1
        mock_stop.assert_awaited_once_with(ws_id)

    @pytest.mark.asyncio
    @patch("src.server.services.workspace_manager.get_workspaces_by_status", new_callable=AsyncMock)
    async def test_cleanup_idle_skips_active_workspaces(self, mock_get_by_status):
        now = datetime.now(timezone.utc)
        mock_get_by_status.return_value = [
            _make_workspace(last_activity_at=now),
        ]

        config = _make_config()
        wm = WorkspaceManager(config, idle_timeout=1800)

        with patch.object(wm, "stop_workspace", new_callable=AsyncMock) as mock_stop:
            count = await wm.cleanup_idle_workspaces()

        assert count == 0
        mock_stop.assert_not_awaited()

    @pytest.mark.asyncio
    @patch("src.server.services.workspace_manager.get_workspaces_by_status", new_callable=AsyncMock)
    async def test_cleanup_idle_skips_no_activity(self, mock_get_by_status):
        mock_get_by_status.return_value = [
            _make_workspace(last_activity_at=None),
        ]

        config = _make_config()
        wm = WorkspaceManager(config, idle_timeout=1800)

        with patch.object(wm, "stop_workspace", new_callable=AsyncMock) as mock_stop:
            count = await wm.cleanup_idle_workspaces()

        assert count == 0
        mock_stop.assert_not_awaited()


# ---------------------------------------------------------------------------
# shutdown
# ---------------------------------------------------------------------------

class TestShutdown:
    """Test workspace manager shutdown."""

    def setup_method(self):
        WorkspaceManager.reset_instance()

    def teardown_method(self):
        WorkspaceManager.reset_instance()

    @pytest.mark.asyncio
    async def test_shutdown_clears_state(self):
        config = _make_config()
        wm = WorkspaceManager(config)
        wm._sessions["ws-1"] = _make_mock_session()
        wm._user_data_synced.add("ws-1")
        wm._pending_lazy_sync.add("ws-1")
        wm._last_sync_at["ws-1"] = time.monotonic()
        wm._workspace_locks["ws-1"] = asyncio.Lock()

        await wm.shutdown()

        assert wm._sessions == {}
        assert len(wm._user_data_synced) == 0
        assert len(wm._pending_lazy_sync) == 0
        assert wm._last_sync_at == {}
        assert wm._workspace_locks == {}
        assert wm._shutdown is True

    @pytest.mark.asyncio
    async def test_shutdown_cancels_cleanup_task(self):
        config = _make_config()
        wm = WorkspaceManager(config, cleanup_interval=1)

        # Start cleanup task
        await wm.start_cleanup_task()
        assert wm._cleanup_task is not None

        # Shutdown
        await wm.shutdown()
        assert wm._cleanup_task is None


# ---------------------------------------------------------------------------
# Sync cooldown
# ---------------------------------------------------------------------------

class TestSyncCooldown:
    """Test sync cooldown logic."""

    def setup_method(self):
        WorkspaceManager.reset_instance()

    def teardown_method(self):
        WorkspaceManager.reset_instance()

    def test_sync_cooldown_no_previous_sync(self):
        config = _make_config()
        wm = WorkspaceManager(config)
        assert wm._sync_cooldown_ok("ws-1") is False

    def test_sync_cooldown_recent_sync(self):
        config = _make_config()
        wm = WorkspaceManager(config)
        wm._record_sync("ws-1")
        assert wm._sync_cooldown_ok("ws-1") is True

    def test_sync_cooldown_expired(self):
        config = _make_config()
        wm = WorkspaceManager(config)
        # Set sync time to well past the cooldown
        wm._last_sync_at["ws-1"] = time.monotonic() - wm._SYNC_COOLDOWN_SECONDS - 10
        assert wm._sync_cooldown_ok("ws-1") is False


# ---------------------------------------------------------------------------
# _seed_agent_md
# ---------------------------------------------------------------------------

class TestSeedAgentMd:
    """Test agent.md seeding."""

    @pytest.mark.asyncio
    async def test_seed_agent_md_writes_to_sandbox(self):
        sandbox = AsyncMock()
        sandbox.awrite_file_text = AsyncMock(return_value=True)

        await WorkspaceManager._seed_agent_md(sandbox, "My Workspace", "A description")

        sandbox.awrite_file_text.assert_awaited_once()
        call_args = sandbox.awrite_file_text.call_args
        assert call_args[0][0] == "agent.md"
        content = call_args[0][1]
        assert "My Workspace" in content
        assert "A description" in content

    @pytest.mark.asyncio
    async def test_seed_agent_md_none_sandbox_noop(self):
        # Should not raise when sandbox is None
        await WorkspaceManager._seed_agent_md(None, "Name")

    @pytest.mark.asyncio
    async def test_seed_agent_md_handles_write_failure(self):
        sandbox = AsyncMock()
        sandbox.awrite_file_text = AsyncMock(side_effect=Exception("write failed"))

        # Should not raise
        await WorkspaceManager._seed_agent_md(sandbox, "Name")
