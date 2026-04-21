"""
Tests for WorkspaceManager service.

Tests workspace lifecycle: creation, session retrieval, stop, delete,
idle cleanup, singleton pattern, and background cleanup tasks.
"""

import asyncio
import time
import uuid
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from ptc_agent.core.sandbox.runtime import SandboxGoneError
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


# ---------------------------------------------------------------------------
# SandboxGoneError
# ---------------------------------------------------------------------------

class TestSandboxGoneError:
    """Test SandboxGoneError exception class."""

    def test_attributes_and_message(self):
        err = SandboxGoneError("sandbox-123", "not found: 404")
        assert err.sandbox_id == "sandbox-123"
        assert "sandbox-123" in str(err)
        assert "not found: 404" in str(err)

    def test_is_runtime_error(self):
        err = SandboxGoneError("sandbox-123")
        assert isinstance(err, RuntimeError)

    def test_empty_message(self):
        err = SandboxGoneError("sandbox-123")
        assert str(err) == "Sandbox sandbox-123 is gone"


# ---------------------------------------------------------------------------
# PTCSandbox.has_failed() state matrix
# ---------------------------------------------------------------------------

class TestHasFailed:
    """Test PTCSandbox.has_failed() distinguishes 'init failed' from 'still initializing'."""

    def test_no_lazy_init(self):
        """Non-lazy sandbox: _ready_event is None → has_failed() returns False."""
        sandbox = MagicMock()
        sandbox._ready_event = None
        sandbox._init_error = None
        # Call the real has_failed logic
        from ptc_agent.core.sandbox.ptc_sandbox import PTCSandbox
        result = PTCSandbox.has_failed(sandbox)
        assert result is False

    def test_still_initializing(self):
        """Lazy init in progress: event not set → has_failed() returns False."""
        sandbox = MagicMock()
        sandbox._ready_event = asyncio.Event()
        sandbox._init_error = None
        from ptc_agent.core.sandbox.ptc_sandbox import PTCSandbox
        result = PTCSandbox.has_failed(sandbox)
        assert result is False

    def test_success(self):
        """Lazy init succeeded: event set, no error → has_failed() returns False."""
        sandbox = MagicMock()
        sandbox._ready_event = asyncio.Event()
        sandbox._ready_event.set()
        sandbox._init_error = None
        from ptc_agent.core.sandbox.ptc_sandbox import PTCSandbox
        result = PTCSandbox.has_failed(sandbox)
        assert result is False

    def test_with_error(self):
        """Lazy init failed: event set + error → has_failed() returns True."""
        sandbox = MagicMock()
        sandbox._ready_event = asyncio.Event()
        sandbox._ready_event.set()
        sandbox._init_error = SandboxGoneError("sb-1", "not found")
        from ptc_agent.core.sandbox.ptc_sandbox import PTCSandbox
        result = PTCSandbox.has_failed(sandbox)
        assert result is True


# ---------------------------------------------------------------------------
# has_ready_session
# ---------------------------------------------------------------------------

class TestHasReadySession:
    """Test WorkspaceManager.has_ready_session() quick pre-check."""

    def setup_method(self):
        WorkspaceManager.reset_instance()

    def teardown_method(self):
        WorkspaceManager.reset_instance()

    def test_has_ready_session_no_cache(self):
        """workspace_id not in _sessions returns False."""
        config = _make_config()
        wm = WorkspaceManager(config)
        assert wm.has_ready_session("ws-nonexistent") is False

    def test_has_ready_session_ready(self):
        """Initialized session with ready sandbox returns True."""
        config = _make_config()
        wm = WorkspaceManager(config)
        session = _make_mock_session(initialized=True, has_sandbox=True)
        session.sandbox.is_ready = MagicMock(return_value=True)
        wm._sessions["ws-1"] = session
        assert wm.has_ready_session("ws-1") is True

    def test_has_ready_session_not_ready(self):
        """Initialized session with non-ready sandbox returns False."""
        config = _make_config()
        wm = WorkspaceManager(config)
        session = _make_mock_session(initialized=True, has_sandbox=True)
        session.sandbox.is_ready = MagicMock(return_value=False)
        wm._sessions["ws-1"] = session
        assert wm.has_ready_session("ws-1") is False


# ---------------------------------------------------------------------------
# Sandbox recovery — Gap 1 & Gap 2 fixes
# ---------------------------------------------------------------------------

class TestSandboxRecovery:
    """Test sandbox recovery when lazy init fails with sandbox-gone error."""

    def setup_method(self):
        WorkspaceManager.reset_instance()

    def teardown_method(self):
        WorkspaceManager.reset_instance()

    def _make_manager(self):
        config = _make_config()
        return WorkspaceManager.get_instance(config=config)

    def _make_failed_session(self, error=None):
        """Create a session whose sandbox has a failed lazy init."""
        session = _make_mock_session()
        session.sandbox.is_ready = MagicMock(return_value=False)
        session.sandbox.has_failed = MagicMock(return_value=True)
        session.sandbox.init_error = error or SandboxGoneError("sb-old", "not found")
        return session

    def _make_initializing_session(self):
        """Create a session whose sandbox is still lazy-initializing."""
        session = _make_mock_session()
        session.sandbox.is_ready = MagicMock(return_value=False)
        session.sandbox.has_failed = MagicMock(return_value=False)
        return session

    @pytest.mark.asyncio
    @patch("src.server.services.workspace_manager.db_get_workspace")
    @patch("src.server.services.workspace_manager.SessionManager")
    @patch("src.server.services.workspace_manager.update_workspace_status")
    @patch("src.server.services.workspace_manager.update_workspace_activity")
    async def test_cache_hit_failed_lazy_sandbox_gone_recovers(
        self, mock_activity, mock_status, mock_session_mgr, mock_get_ws
    ):
        """Gap 1: cached session with SandboxGoneError → _recover_sandbox called."""
        manager = self._make_manager()
        ws_id = str(uuid.uuid4())
        workspace = _make_workspace(workspace_id=ws_id, status="running")
        mock_get_ws.return_value = workspace

        # Place broken session in cache
        broken_session = self._make_failed_session()
        manager._sessions[ws_id] = broken_session

        # Mock recovery: SessionManager.get_session returns a new working session
        new_session = _make_mock_session()
        new_session.sandbox.sandbox_id = "sb-new"
        mock_session_mgr.get_session.return_value = new_session
        mock_session_mgr.cleanup_session = AsyncMock()

        result = await manager.get_session_for_workspace(ws_id, user_id="user-1")

        # Broken session should be proactively cleaned up (MCP + provider)
        mock_session_mgr.cleanup_session.assert_awaited_with(ws_id)
        # Recovery creates a new session
        new_session.initialize.assert_called_once()
        # Status updated
        assert result is not None

    @pytest.mark.asyncio
    @patch("src.server.services.workspace_manager.db_get_workspace")
    @patch("src.server.services.workspace_manager.SessionManager")
    @patch("src.server.services.workspace_manager.update_workspace_status")
    @patch("src.server.services.workspace_manager.update_workspace_activity")
    async def test_cache_hit_failed_lazy_other_error_clears(
        self, mock_activity, mock_status, mock_session_mgr, mock_get_ws
    ):
        """Gap 1: cached session with non-SandboxGoneError → clears session, falls through."""
        manager = self._make_manager()
        ws_id = str(uuid.uuid4())
        workspace = _make_workspace(workspace_id=ws_id, status="running")
        mock_get_ws.return_value = workspace

        # Broken session with a non-SandboxGoneError
        broken_session = self._make_failed_session(
            error=RuntimeError("network timeout")
        )
        manager._sessions[ws_id] = broken_session

        # Fall-through: SessionManager.get_session returns a new session for reconnect
        new_session = _make_mock_session()
        mock_session_mgr.get_session.return_value = new_session
        mock_session_mgr.cleanup_session = AsyncMock()

        result = await manager.get_session_for_workspace(ws_id, user_id="user-1")

        # Broken session proactively cleaned up (MCP + provider)
        mock_session_mgr.cleanup_session.assert_awaited_with(ws_id)
        # Falls through to status-based handling (reconnect)
        assert result is not None

    @pytest.mark.asyncio
    @patch("src.server.services.workspace_manager.db_get_workspace")
    async def test_cache_hit_still_initializing_returns(self, mock_get_ws):
        """Sandbox still initializing → returns session immediately, no recovery."""
        manager = self._make_manager()
        ws_id = str(uuid.uuid4())
        workspace = _make_workspace(workspace_id=ws_id, status="running")
        mock_get_ws.return_value = workspace

        session = self._make_initializing_session()
        manager._sessions[ws_id] = session

        result = await manager.get_session_for_workspace(ws_id, user_id="user-1")

        # Same session returned, no recovery triggered
        assert result is session

    @pytest.mark.asyncio
    @patch("src.server.services.workspace_manager.db_get_workspace")
    @patch("src.server.services.workspace_manager.SessionManager")
    @patch("src.server.services.workspace_manager.update_workspace_status")
    @patch("src.server.services.workspace_manager.update_workspace_activity")
    async def test_phase2_sandbox_gone_recovers(
        self, mock_activity, mock_status, mock_session_mgr, mock_get_ws
    ):
        """Gap 2: ensure_sandbox_ready raises SandboxGoneError → recovery in Phase 2."""
        manager = self._make_manager()
        ws_id = str(uuid.uuid4())
        workspace = _make_workspace(workspace_id=ws_id, status="running")
        mock_get_ws.return_value = workspace

        # Ready session but ensure_sandbox_ready fails (sandbox gone after cooldown)
        session = _make_mock_session()
        session.sandbox.ensure_sandbox_ready = AsyncMock(
            side_effect=SandboxGoneError("sb-old", "not found")
        )
        manager._sessions[ws_id] = session
        # Force sync by clearing cooldown
        manager._last_sync_at = {}

        # Mock recovery
        new_session = _make_mock_session()
        new_session.sandbox.sandbox_id = "sb-new"
        mock_session_mgr.get_session.return_value = new_session
        mock_session_mgr.cleanup_session = AsyncMock()

        result = await manager.get_session_for_workspace(ws_id, user_id="user-1")

        # Recovery triggered
        mock_session_mgr.cleanup_session.assert_awaited_with(ws_id)
        new_session.initialize.assert_called_once()
        assert result is not None

    @pytest.mark.asyncio
    @patch("src.server.services.workspace_manager.db_get_workspace")
    @patch("src.server.services.workspace_manager.SessionManager")
    async def test_phase2_concurrent_recovery_skips(
        self, mock_session_mgr, mock_get_ws
    ):
        """Gap 2: SandboxGoneError but session already recovered → uses existing."""
        manager = self._make_manager()
        ws_id = str(uuid.uuid4())
        workspace = _make_workspace(workspace_id=ws_id, status="running")
        mock_get_ws.return_value = workspace

        # Session with sandbox-gone error in Phase 2
        broken_session = _make_mock_session()
        broken_session.sandbox.ensure_sandbox_ready = AsyncMock(
            side_effect=SandboxGoneError("sb-old", "not found")
        )
        manager._sessions[ws_id] = broken_session
        manager._last_sync_at = {}

        # Simulate concurrent recovery: when we re-acquire the lock,
        # another request has already placed a working session in the cache.
        already_recovered = _make_mock_session()
        already_recovered.sandbox.is_ready = MagicMock(return_value=True)

        original_acquire = manager._acquire_workspace_lock

        @asynccontextmanager
        async def mock_acquire(wid, timeout=60.0):
            # Before yielding the lock, simulate concurrent recovery
            manager._sessions[wid] = already_recovered
            async with original_acquire(wid, timeout=timeout):
                yield

        manager._acquire_workspace_lock = mock_acquire

        result = await manager.get_session_for_workspace(ws_id, user_id="user-1")

        # Should return the already-recovered session, not create a new one
        assert result is already_recovered

    @pytest.mark.asyncio
    @patch("src.server.services.workspace_manager.db_get_workspace")
    async def test_phase2_other_error_logs_warning(self, mock_get_ws):
        """Phase 2: non-SandboxGoneError → logs warning, returns session."""
        manager = self._make_manager()
        ws_id = str(uuid.uuid4())
        workspace = _make_workspace(workspace_id=ws_id, status="running")
        mock_get_ws.return_value = workspace

        session = _make_mock_session()
        session.sandbox.ensure_sandbox_ready = AsyncMock(
            side_effect=RuntimeError("network blip")
        )
        manager._sessions[ws_id] = session
        manager._last_sync_at = {}

        result = await manager.get_session_for_workspace(ws_id, user_id="user-1")

        # Same session returned (broken, but we don't know it's sandbox-gone)
        assert result is session

    @pytest.mark.asyncio
    @patch("src.server.services.workspace_manager.db_get_workspace")
    @patch("src.server.services.workspace_manager.SessionManager")
    @patch("src.server.services.workspace_manager.update_workspace_status")
    @patch("src.server.services.workspace_manager.update_workspace_activity")
    async def test_running_reconnect_sandbox_gone_recovers(
        self, mock_activity, mock_status, mock_session_mgr, mock_get_ws
    ):
        """Existing path: status=running, initialize raises SandboxGoneError → recovery."""
        manager = self._make_manager()
        ws_id = str(uuid.uuid4())
        workspace = _make_workspace(workspace_id=ws_id, status="running")
        mock_get_ws.return_value = workspace

        # First session fails to initialize (sandbox gone)
        failing_session = _make_mock_session(initialized=False)
        failing_session.initialize = AsyncMock(
            side_effect=SandboxGoneError("sb-old", "not found")
        )

        # Recovery session
        recovered_session = _make_mock_session()
        recovered_session.sandbox.sandbox_id = "sb-new"

        mock_session_mgr.get_session.side_effect = [failing_session, recovered_session]
        mock_session_mgr.cleanup_session = AsyncMock()

        result = await manager.get_session_for_workspace(ws_id, user_id="user-1")

        # Recovery triggered
        mock_session_mgr.cleanup_session.assert_awaited_with(ws_id)
        recovered_session.initialize.assert_called_once()
        assert result is not None

    @pytest.mark.asyncio
    @patch("src.server.services.workspace_manager.db_get_workspace")
    @patch("src.server.services.workspace_manager.SessionManager")
    @patch("src.server.services.workspace_manager.update_workspace_status")
    @patch("src.server.services.workspace_manager.update_workspace_activity")
    async def test_stopped_workspace_lazy_init_sandbox_gone_recovers(
        self, mock_activity, mock_status, mock_session_mgr, mock_get_ws
    ):
        """REGRESSION: First request to a stopped workspace whose sandbox is deleted.

        Previously, _restart_workspace(lazy_init=True) returned a session
        with a pending background reconnect. The reconnect failed with
        SandboxGoneError but the error only surfaced when the chat handler
        called _wait_ready(). Now, the stopped path falls through to Phase 2
        which waits for lazy init and handles SandboxGoneError.
        """
        manager = self._make_manager()
        ws_id = str(uuid.uuid4())
        workspace = _make_workspace(workspace_id=ws_id, status="stopped")
        mock_get_ws.return_value = workspace

        # _restart_workspace returns a session whose sandbox will fail in Phase 2
        lazy_session = _make_mock_session()
        lazy_session.sandbox.ensure_sandbox_ready = AsyncMock(
            side_effect=SandboxGoneError("sb-old", "not found")
        )

        # Recovery session
        recovered_session = _make_mock_session()
        recovered_session.sandbox.sandbox_id = "sb-new"

        # First call: _restart_workspace gets lazy_session
        # Second call: _recover_sandbox gets recovered_session
        mock_session_mgr.get_session.side_effect = [lazy_session, recovered_session]
        mock_session_mgr.cleanup_session = AsyncMock()

        # Patch _restart_workspace to return the lazy session directly
        # (simulates the real lazy init path)
        async def mock_restart(workspace, user_id, lazy_init=True, on_state_observed=None):
            session = lazy_session
            manager._sessions[ws_id] = session
            manager._pending_lazy_sync.add(ws_id)
            return session

        with patch.object(manager, "_restart_workspace", side_effect=mock_restart):
            result = await manager.get_session_for_workspace(ws_id, user_id="user-1")

        # Phase 2 caught SandboxGoneError and triggered recovery
        mock_session_mgr.cleanup_session.assert_awaited_with(ws_id)
        assert result is not None

    @pytest.mark.asyncio
    @patch("src.server.services.workspace_manager.SessionManager")
    @patch("src.server.services.workspace_manager.update_workspace_status")
    @patch("src.server.services.workspace_manager.update_workspace_activity")
    async def test_restart_workspace_stamps_activity_after_status(
        self, mock_activity, mock_status, mock_session_mgr
    ):
        """REGRESSION: _restart_workspace must await update_workspace_activity
        after flipping status to 'running'. Without this, an idle sweep firing
        during the sandbox restore reads a stale last_activity_at and stops the
        workspace mid-request, surfacing to the user as
        'Session for workspace ... is not properly initialized'.
        Mirrors _recover_sandbox.
        """
        manager = self._make_manager()
        ws_id = str(uuid.uuid4())
        workspace = _make_workspace(workspace_id=ws_id, status="stopped")

        session = _make_mock_session()
        mock_session_mgr.get_session.return_value = session

        # Patch non-focus internals so execution reaches the final
        # status + activity block on the happy reconnect path.
        manager._sync_sandbox_assets = AsyncMock()
        manager._maybe_restore_files = AsyncMock()
        manager._maybe_migrate_sandbox = AsyncMock(return_value=None)

        # Record relative order of the two awaited writes.
        call_order: list[str] = []

        async def record_status(**kwargs):
            call_order.append("status")

        async def record_activity(workspace_id):
            call_order.append("activity")

        mock_status.side_effect = record_status
        mock_activity.side_effect = record_activity

        result = await manager._restart_workspace(
            workspace, user_id="user-1", lazy_init=False
        )

        assert result is session

        mock_status.assert_awaited_once()
        status_kwargs = mock_status.await_args.kwargs
        assert status_kwargs["status"] == "running"
        assert status_kwargs["workspace_id"] == ws_id
        mock_activity.assert_awaited_once_with(ws_id)

        # Ordering must match _recover_sandbox: status flip first, then
        # activity stamp. Reversing the order would leave a larger window
        # where cleanup_idle_workspaces could stop the workspace.
        assert call_order == ["status", "activity"]


# ---------------------------------------------------------------------------
# on_state_observed forwarding — pin the kwarg threads through every
# session init branch so a silent typo in any one call site fails CI.
# ---------------------------------------------------------------------------


class TestOnStateObservedForwarding:
    """Lock in that on_state_observed is passed to session.initialize /
    initialize_lazy at every call site in workspace_manager.py. A typo
    or missing kwarg in any branch would silently drop the archived
    refinement event on the chat SSE stream."""

    def setup_method(self):
        WorkspaceManager.reset_instance()

    def teardown_method(self):
        WorkspaceManager.reset_instance()

    def _make_manager(self):
        manager = WorkspaceManager.get_instance(config=_make_config())
        manager._sync_sandbox_assets = AsyncMock()
        manager._maybe_migrate_sandbox = AsyncMock(return_value=None)
        return manager

    @pytest.mark.asyncio
    @patch("src.server.services.workspace_manager.db_get_workspace")
    @patch("src.server.services.workspace_manager.SessionManager")
    @patch("src.server.services.workspace_manager.update_workspace_activity")
    async def test_running_path_forwards_callback_to_initialize(
        self, mock_activity, mock_session_mgr, mock_get_ws
    ):
        """status=running + no cache → session.initialize(..., on_state_observed=sentinel)."""
        manager = self._make_manager()
        ws_id = str(uuid.uuid4())
        mock_get_ws.return_value = _make_workspace(workspace_id=ws_id, status="running")
        session = _make_mock_session(initialized=False)
        mock_session_mgr.get_session.return_value = session

        def sentinel(_s: str) -> None:
            return None

        await manager.get_session_for_workspace(
            ws_id, user_id="user-1", on_state_observed=sentinel
        )

        session.initialize.assert_awaited_once()
        assert session.initialize.await_args.kwargs.get("on_state_observed") is sentinel

    @pytest.mark.asyncio
    @patch("src.server.services.workspace_manager.db_get_workspace")
    @patch("src.server.services.workspace_manager.SessionManager")
    @patch("src.server.services.workspace_manager.update_workspace_activity")
    @patch("src.server.services.workspace_manager.update_workspace_status")
    async def test_stopped_path_forwards_callback_to_initialize_lazy(
        self, mock_status, mock_activity, mock_session_mgr, mock_get_ws
    ):
        """status=stopped + matching config hash → _restart_workspace keeps
        lazy_init=True → session.initialize_lazy(..., on_state_observed=sentinel)."""
        manager = self._make_manager()
        ws_id = str(uuid.uuid4())
        # Make config hash match so _restart_workspace keeps lazy_init=True.
        manager._compute_sandbox_config_hash = MagicMock(return_value="matching-hash")
        workspace = _make_workspace(
            workspace_id=ws_id,
            status="stopped",
            config={"sandbox_config_hash": "matching-hash"},
        )
        mock_get_ws.return_value = workspace
        session = _make_mock_session(initialized=False)
        # Simulate lazy init leaving sandbox ready so Phase 2 doesn't retry.
        session.sandbox.is_ready = MagicMock(return_value=True)
        session.sandbox.has_failed = MagicMock(return_value=False)
        mock_session_mgr.get_session.return_value = session

        def sentinel(_s: str) -> None:
            return None

        await manager.get_session_for_workspace(
            ws_id, user_id="user-1", on_state_observed=sentinel
        )

        session.initialize_lazy.assert_awaited_once()
        assert (
            session.initialize_lazy.await_args.kwargs.get("on_state_observed")
            is sentinel
        )
        # Lazy path must not have touched the eager initialize.
        session.initialize.assert_not_awaited()

    @pytest.mark.asyncio
    @patch("src.server.services.workspace_manager.db_get_workspace")
    @patch("src.server.services.workspace_manager.SessionManager")
    @patch("src.server.services.workspace_manager.update_workspace_activity")
    @patch("src.server.services.workspace_manager.update_workspace_status")
    async def test_restart_forced_non_lazy_forwards_callback_to_initialize(
        self, mock_status, mock_activity, mock_session_mgr, mock_get_ws
    ):
        """Config hash mismatch inside _restart_workspace forces lazy_init=False
        → session.initialize(..., on_state_observed=sentinel) instead of initialize_lazy."""
        manager = self._make_manager()
        ws_id = str(uuid.uuid4())
        manager._compute_sandbox_config_hash = MagicMock(return_value="new-hash")
        workspace = _make_workspace(
            workspace_id=ws_id,
            status="stopped",
            config={"sandbox_config_hash": "old-hash"},
        )
        mock_get_ws.return_value = workspace
        session = _make_mock_session(initialized=False)
        session.sandbox.is_ready = MagicMock(return_value=True)
        session.sandbox.has_failed = MagicMock(return_value=False)
        mock_session_mgr.get_session.return_value = session

        def sentinel(_s: str) -> None:
            return None

        await manager.get_session_for_workspace(
            ws_id, user_id="user-1", on_state_observed=sentinel
        )

        session.initialize.assert_awaited_once()
        assert session.initialize.await_args.kwargs.get("on_state_observed") is sentinel
        session.initialize_lazy.assert_not_awaited()

    @pytest.mark.asyncio
    @patch("src.server.services.workspace_manager.db_get_workspace")
    @patch("src.server.services.workspace_manager.SessionManager")
    @patch("src.server.services.workspace_manager.update_workspace_activity")
    async def test_warm_cached_session_does_not_call_any_initialize(
        self, mock_activity, mock_session_mgr, mock_get_ws
    ):
        """Initialized cached session → no initialize / initialize_lazy call
        even when on_state_observed is passed. The callback simply has no
        path to fire on the warm hit and must not leak into any init path."""
        manager = self._make_manager()
        ws_id = str(uuid.uuid4())
        mock_get_ws.return_value = _make_workspace(workspace_id=ws_id, status="running")
        cached = _make_mock_session(initialized=True)
        cached.sandbox.is_ready = MagicMock(return_value=True)
        cached.sandbox.has_failed = MagicMock(return_value=False)
        manager._sessions[ws_id] = cached

        def sentinel(_s: str) -> None:
            return None

        await manager.get_session_for_workspace(
            ws_id, user_id="user-1", on_state_observed=sentinel
        )

        cached.initialize.assert_not_awaited()
        cached.initialize_lazy.assert_not_awaited()


# ---------------------------------------------------------------------------
# Phase 2 error narrowing + _clear_session helper (Fix 2)
# ---------------------------------------------------------------------------

from ptc_agent.core.sandbox.runtime import SandboxTransientError  # noqa: E402


class TestPhase2ErrorNarrowing:
    """Phase 2 now distinguishes a failed lazy init (has_failed() == True,
    clear + re-raise) from a post-init transient (has_failed() == False,
    swallow + retry next request). Generic Exception keeps the legacy
    best-effort-retry behavior — regression-guarded here."""

    def setup_method(self):
        WorkspaceManager.reset_instance()

    def teardown_method(self):
        WorkspaceManager.reset_instance()

    def _make_manager(self):
        return WorkspaceManager.get_instance(config=_make_config())

    @pytest.mark.asyncio
    @patch("src.server.services.workspace_manager.db_get_workspace")
    @patch("src.server.services.workspace_manager.SessionManager")
    @patch("src.server.services.workspace_manager.update_workspace_status")
    @patch("src.server.services.workspace_manager.update_workspace_activity")
    async def test_phase2_transient_init_failure_clears_and_raises(
        self, mock_activity, mock_status, mock_session_mgr, mock_get_ws
    ):
        """Phase 2 SandboxTransientError + has_failed() True → _clear_session
        is called and the error propagates for handle_workflow_error to catch."""
        manager = self._make_manager()
        ws_id = str(uuid.uuid4())
        mock_get_ws.return_value = _make_workspace(
            workspace_id=ws_id, status="running"
        )

        session = _make_mock_session()
        session.sandbox.ensure_sandbox_ready = AsyncMock(
            side_effect=SandboxTransientError("transport failed after retries")
        )
        session.sandbox.has_failed = MagicMock(return_value=True)
        manager._sessions[ws_id] = session
        manager._last_sync_at = {}
        mock_session_mgr.cleanup_session = AsyncMock()

        with pytest.raises(SandboxTransientError):
            await manager.get_session_for_workspace(ws_id, user_id="user-1")

        mock_session_mgr.cleanup_session.assert_awaited_with(ws_id)
        assert ws_id not in manager._sessions

    @pytest.mark.asyncio
    @patch("src.server.services.workspace_manager.db_get_workspace")
    @patch("src.server.services.workspace_manager.SessionManager")
    @patch("src.server.services.workspace_manager.update_workspace_status")
    @patch("src.server.services.workspace_manager.update_workspace_activity")
    async def test_phase2_transient_post_init_swallowed(
        self, mock_activity, mock_status, mock_session_mgr, mock_get_ws
    ):
        """SandboxTransientError after sandbox is ready (e.g., asset sync)
        leaves the healthy session in cache and is best-effort retried next
        request. has_failed() == False is the discriminator."""
        manager = self._make_manager()
        ws_id = str(uuid.uuid4())
        mock_get_ws.return_value = _make_workspace(
            workspace_id=ws_id, status="running"
        )

        session = _make_mock_session()
        session.sandbox.has_failed = MagicMock(return_value=False)
        session.sandbox.ensure_sandbox_ready = AsyncMock()

        # Post-init transient: raise from a later sync step via patched method.
        manager._sync_sandbox_assets = AsyncMock(
            side_effect=SandboxTransientError("sync blip")
        )
        manager._maybe_restore_files = AsyncMock()
        manager._pending_lazy_sync.add(ws_id)  # force deferred-sync branch
        manager._sessions[ws_id] = session
        manager._last_sync_at = {}
        mock_session_mgr.cleanup_session = AsyncMock()

        result = await manager.get_session_for_workspace(ws_id, user_id="user-1")

        assert result is session  # still cached, no clear
        mock_session_mgr.cleanup_session.assert_not_awaited()
        assert ws_id in manager._sessions

    @pytest.mark.asyncio
    @patch("src.server.services.workspace_manager.db_get_workspace")
    @patch("src.server.services.workspace_manager.SessionManager")
    async def test_phase2_generic_exception_not_cleared(
        self, mock_session_mgr, mock_get_ws
    ):
        """REGRESSION: plain Exception in Phase 2 keeps the legacy
        'log and retry next request' behavior. Do not broaden the clear."""
        manager = self._make_manager()
        ws_id = str(uuid.uuid4())
        mock_get_ws.return_value = _make_workspace(
            workspace_id=ws_id, status="running"
        )

        session = _make_mock_session()
        session.sandbox.ensure_sandbox_ready = AsyncMock(
            side_effect=RuntimeError("some non-sandbox runtime error")
        )
        manager._sessions[ws_id] = session
        manager._last_sync_at = {}
        mock_session_mgr.cleanup_session = AsyncMock()

        result = await manager.get_session_for_workspace(ws_id, user_id="user-1")

        assert result is session
        mock_session_mgr.cleanup_session.assert_not_awaited()


class TestClearSessionHelper:
    """WorkspaceManager._clear_session proactively awaits cleanup_session
    (closes MCP + provider) and clears local caches. Must be resilient when
    cleanup_session raises and idempotent when workspace not present."""

    def setup_method(self):
        WorkspaceManager.reset_instance()

    def teardown_method(self):
        WorkspaceManager.reset_instance()

    @pytest.mark.asyncio
    @patch("src.server.services.workspace_manager.SessionManager")
    async def test_clear_session_happy_path(self, mock_sm):
        """Awaits cleanup_session, pops from _sessions, discards from
        _pending_lazy_sync."""
        config = _make_config()
        manager = WorkspaceManager.get_instance(config=config)
        ws_id = str(uuid.uuid4())
        manager._sessions[ws_id] = _make_mock_session()
        manager._pending_lazy_sync.add(ws_id)
        mock_sm.cleanup_session = AsyncMock()

        await manager._clear_session(ws_id)

        mock_sm.cleanup_session.assert_awaited_once_with(ws_id)
        assert ws_id not in manager._sessions
        assert ws_id not in manager._pending_lazy_sync

    @pytest.mark.asyncio
    @patch("src.server.services.workspace_manager.SessionManager")
    async def test_clear_session_idempotent_when_absent(self, mock_sm):
        """Workspace not tracked — no KeyError; cleanup still attempted."""
        config = _make_config()
        manager = WorkspaceManager.get_instance(config=config)
        ws_id = str(uuid.uuid4())
        mock_sm.cleanup_session = AsyncMock()

        await manager._clear_session(ws_id)  # must not raise

        mock_sm.cleanup_session.assert_awaited_once_with(ws_id)

    @pytest.mark.asyncio
    @patch("src.server.services.workspace_manager.SessionManager")
    async def test_clear_session_survives_cleanup_exception(self, mock_sm):
        """If cleanup_session raises, local caches still clear — the caller
        must not see the exception bleed out of this helper."""
        config = _make_config()
        manager = WorkspaceManager.get_instance(config=config)
        ws_id = str(uuid.uuid4())
        manager._sessions[ws_id] = _make_mock_session()
        manager._pending_lazy_sync.add(ws_id)
        mock_sm.cleanup_session = AsyncMock(
            side_effect=RuntimeError("MCP stuck")
        )

        await manager._clear_session(ws_id)  # must swallow

        assert ws_id not in manager._sessions
        assert ws_id not in manager._pending_lazy_sync


# ---------------------------------------------------------------------------
# Intermediate "starting" status (Fix 1)
# ---------------------------------------------------------------------------


class TestIntermediateStartingStatus:
    """Lazy restart: status transitions stopped → starting → running.
    The activity stamp moves with the running promotion (not the starting
    flip) — cleanup_idle_workspaces only queries status=running, so rows
    in "starting" are immune to the idle sweep regardless."""

    def setup_method(self):
        WorkspaceManager.reset_instance()

    def teardown_method(self):
        WorkspaceManager.reset_instance()

    def _make_manager(self):
        manager = WorkspaceManager.get_instance(config=_make_config())
        manager._sync_sandbox_assets = AsyncMock()
        manager._maybe_restore_files = AsyncMock()
        manager._maybe_migrate_sandbox = AsyncMock(return_value=None)
        # Stable config hash so lazy_init is not force-flipped to False
        # by the config-migration guard in _restart_workspace.
        manager._compute_sandbox_config_hash = MagicMock(return_value="stable")
        return manager

    @staticmethod
    def _lazy_workspace(workspace_id, status="stopped"):
        return _make_workspace(
            workspace_id=workspace_id,
            status=status,
            config={"sandbox_config_hash": "stable"},
        )

    @pytest.mark.asyncio
    @patch("src.server.services.workspace_manager.SessionManager")
    @patch("src.server.services.workspace_manager.update_workspace_status")
    @patch("src.server.services.workspace_manager.update_workspace_activity")
    async def test_lazy_restart_sets_starting_without_activity_stamp(
        self, mock_activity, mock_status, mock_session_mgr
    ):
        """lazy_init=True flips status → "starting" and does NOT stamp
        activity (sweep never sees "starting")."""
        manager = self._make_manager()
        ws_id = str(uuid.uuid4())
        workspace = self._lazy_workspace(ws_id, status="stopped")

        session = _make_mock_session()
        mock_session_mgr.get_session.return_value = session

        await manager._restart_workspace(
            workspace, user_id="user-1", lazy_init=True
        )

        mock_status.assert_awaited_once()
        kwargs = mock_status.await_args.kwargs
        assert kwargs["status"] == "starting"
        assert kwargs["workspace_id"] == ws_id
        mock_activity.assert_not_awaited()

    @pytest.mark.asyncio
    @patch("src.server.services.workspace_manager.db_get_workspace")
    @patch("src.server.services.workspace_manager.SessionManager")
    @patch("src.server.services.workspace_manager.update_workspace_status")
    @patch("src.server.services.workspace_manager.update_workspace_activity")
    async def test_phase2_success_promotes_starting_to_running_and_stamps(
        self, mock_activity, mock_status, mock_session_mgr, mock_get_ws
    ):
        """When Phase 2 finishes the deferred sync, DB is promoted to
        running AND activity is stamped in that order (mirrors PR #152's
        invariant for the lazy path)."""
        manager = self._make_manager()
        ws_id = str(uuid.uuid4())
        mock_get_ws.return_value = self._lazy_workspace(ws_id, status="stopped")

        lazy_session = _make_mock_session()
        lazy_session.sandbox.ensure_sandbox_ready = AsyncMock()
        mock_session_mgr.get_session.return_value = lazy_session

        call_order: list[tuple[str, dict]] = []

        async def record_status(**kwargs):
            call_order.append(("status", kwargs))

        async def record_activity(workspace_id):
            call_order.append(("activity", {"workspace_id": workspace_id}))

        mock_status.side_effect = record_status
        mock_activity.side_effect = record_activity

        await manager.get_session_for_workspace(ws_id, user_id="user-1")

        # Expected sequence:
        #   1. _restart_workspace: status=starting (no activity stamp yet)
        #   2. Phase 2: status=running, then activity
        names = [c[0] for c in call_order]
        assert names == ["status", "status", "activity"], names
        assert call_order[0][1]["status"] == "starting"
        assert call_order[1][1]["status"] == "running"
        assert call_order[2][1]["workspace_id"] == ws_id

    @pytest.mark.asyncio
    @patch("src.server.services.workspace_manager.db_get_workspace")
    @patch("src.server.services.workspace_manager.SessionManager")
    @patch("src.server.services.workspace_manager.update_workspace_status")
    @patch("src.server.services.workspace_manager.update_workspace_activity")
    async def test_phase2_failure_leaves_status_at_starting(
        self, mock_activity, mock_status, mock_session_mgr, mock_get_ws
    ):
        """Phase 2 exhausts retries → status stays "starting" (never flipped
        to running), next request re-enters the stopped/starting branch."""
        manager = self._make_manager()
        ws_id = str(uuid.uuid4())
        mock_get_ws.return_value = self._lazy_workspace(ws_id, status="stopped")

        lazy_session = _make_mock_session()
        lazy_session.sandbox.ensure_sandbox_ready = AsyncMock(
            side_effect=SandboxTransientError("exhausted retries")
        )
        lazy_session.sandbox.has_failed = MagicMock(return_value=True)
        mock_session_mgr.get_session.return_value = lazy_session
        mock_session_mgr.cleanup_session = AsyncMock()

        with pytest.raises(SandboxTransientError):
            await manager.get_session_for_workspace(ws_id, user_id="user-1")

        status_calls = [
            c.kwargs for c in mock_status.await_args_list
        ]
        assert {c["status"] for c in status_calls} == {"starting"}

    @pytest.mark.asyncio
    @patch("src.server.services.workspace_manager.db_get_workspace")
    @patch("src.server.services.workspace_manager.SessionManager")
    @patch("src.server.services.workspace_manager.update_workspace_status")
    @patch("src.server.services.workspace_manager.update_workspace_activity")
    async def test_status_starting_reenters_restart_flow(
        self, mock_activity, mock_status, mock_session_mgr, mock_get_ws
    ):
        """If a prior lazy init left the row in "starting" (e.g. failed and
        cleared), the next request with no cached session re-enters the
        restart flow just like "stopped" would."""
        manager = self._make_manager()
        ws_id = str(uuid.uuid4())
        mock_get_ws.return_value = self._lazy_workspace(ws_id, status="starting")

        lazy_session = _make_mock_session()
        lazy_session.sandbox.ensure_sandbox_ready = AsyncMock()
        mock_session_mgr.get_session.return_value = lazy_session

        await manager.get_session_for_workspace(ws_id, user_id="user-1")

        lazy_session.initialize_lazy.assert_awaited_once()


# ---------------------------------------------------------------------------
# Status-tuple parametrization for DB-fallback routing (Fix 1 consumers)
# ---------------------------------------------------------------------------


class TestStatusRoutesToDbFallback:
    """Smoke check: the status tuple the consumer modules (workspace_files,
    public) compare against contains all three of stopped/stopping/starting.
    A typo there would reproduce the 503 storm from the original incident."""

    @pytest.mark.parametrize("status", ["stopped", "stopping", "starting"])
    def test_workspace_files_tuple_includes_status(self, status):
        from src.server.app import workspace_files
        import inspect

        source = inspect.getsource(workspace_files)
        assert f'"{status}"' in source
        # The exact tuple must remain in sync with public.py; if this ever
        # has to change, grep for "stopped", "stopping", "starting" in both
        # files and update together.
        assert '"stopped", "stopping", "starting"' in source

    @pytest.mark.parametrize("status", ["stopped", "stopping", "starting"])
    def test_public_tuple_includes_status(self, status):
        from src.server.app import public
        import inspect

        source = inspect.getsource(public)
        assert f'"{status}"' in source
        assert '"stopped", "stopping", "starting"' in source
