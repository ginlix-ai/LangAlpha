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

        result = await manager.get_session_for_workspace(ws_id, user_id="user-1")

        # Broken session should be removed
        mock_session_mgr.remove_session.assert_called_with(ws_id)
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

        result = await manager.get_session_for_workspace(ws_id, user_id="user-1")

        # Broken session removed
        mock_session_mgr.remove_session.assert_called_with(ws_id)
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

        result = await manager.get_session_for_workspace(ws_id, user_id="user-1")

        # Recovery triggered
        mock_session_mgr.remove_session.assert_called_with(ws_id)
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

        result = await manager.get_session_for_workspace(ws_id, user_id="user-1")

        # Recovery triggered
        mock_session_mgr.remove_session.assert_called_with(ws_id)
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

        # Patch _restart_workspace to return the lazy session directly
        # (simulates the real lazy init path)
        async def mock_restart(workspace, user_id, lazy_init=True):
            session = lazy_session
            manager._sessions[ws_id] = session
            manager._pending_lazy_sync.add(ws_id)
            return session

        with patch.object(manager, "_restart_workspace", side_effect=mock_restart):
            result = await manager.get_session_for_workspace(ws_id, user_id="user-1")

        # Phase 2 caught SandboxGoneError and triggered recovery
        mock_session_mgr.remove_session.assert_called_with(ws_id)
        assert result is not None
