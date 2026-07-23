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
from types import SimpleNamespace
from unittest.mock import ANY, AsyncMock, MagicMock, patch

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


def _make_config_with_tiers():
    """Config whose daytona block exposes real resource tiers + auto-stop.

    ``set_workspace_spec`` reads ``resource_tiers`` (a real dict of tiers with a
    ``.disk`` attr) to validate the tier and compute downgrade-vs-upgrade, which
    the bare ``_make_config()`` MagicMock can't provide.
    """
    config = _make_config()
    config.sandbox = MagicMock(provider="daytona")
    config.sandbox.daytona = MagicMock(
        resource_tiers={
            "standard": SimpleNamespace(cpu=1, memory=1, disk=3),
            "performance": SimpleNamespace(cpu=2, memory=4, disk=5),
            "max": SimpleNamespace(cpu=4, memory=8, disk=10),
        },
        default_tier="standard",
        auto_stop_interval=3600,
    )
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
    async def test_create_workspace_success(
        self, mock_sm, mock_db_create, mock_update_status
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
    async def test_create_workspace_sandbox_failure_marks_error(
        self, mock_sm, mock_db_create, mock_update_status
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
        wm._last_sync_at[ws_id] = time.monotonic()

        result = await wm.stop_workspace(ws_id)

        assert result["status"] == "stopped"
        mock_session.stop.assert_awaited_once()
        assert ws_id not in wm._sessions
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
# _backup_files_to_db strict mode
# ---------------------------------------------------------------------------

class TestBackupFilesStrict:
    """strict=True turns the best-effort backup into a hard precondition for
    callers about to destroy the sandbox (spec-change recreate)."""

    def setup_method(self):
        WorkspaceManager.reset_instance()

    def teardown_method(self):
        WorkspaceManager.reset_instance()

    @pytest.mark.asyncio
    async def test_strict_raises_without_session(self):
        wm = WorkspaceManager(_make_config())
        ws_id = str(uuid.uuid4())

        with pytest.raises(RuntimeError, match="No attached session"):
            await wm._backup_files_to_db(ws_id, strict=True)

    @pytest.mark.asyncio
    @patch("src.server.services.workspace_manager.FilePersistenceService")
    async def test_strict_raises_on_sync_failure(self, mock_file_svc):
        wm = WorkspaceManager(_make_config())
        ws_id = str(uuid.uuid4())
        wm._sessions[ws_id] = _make_mock_session()
        mock_file_svc.sync_to_db = AsyncMock(side_effect=OSError("disk detached"))

        with pytest.raises(RuntimeError, match="aborting before sandbox teardown"):
            await wm._backup_files_to_db(ws_id, strict=True)

    @pytest.mark.asyncio
    @patch("src.server.services.workspace_manager.FilePersistenceService")
    async def test_default_stays_best_effort(self, mock_file_svc):
        wm = WorkspaceManager(_make_config())
        ws_id = str(uuid.uuid4())
        wm._sessions[ws_id] = _make_mock_session()
        mock_file_svc.sync_to_db = AsyncMock(side_effect=OSError("disk detached"))

        await wm._backup_files_to_db(ws_id)  # warns, does not raise


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

        result = await wm.delete_workspace(ws_id)

        assert result is True
        # Cleanup goes through SessionManager (single path, no double-cleanup)
        mock_sm.cleanup_session.assert_awaited_once_with(ws_id)
        mock_db_delete.assert_awaited_once_with(ws_id)
        assert ws_id not in wm._sessions

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

    @pytest.fixture(autouse=True)
    def _quiet_durable_probes(self):
        """The reaper's activity guard also reads the run ledgers; keep both
        quiet so these tests exercise only the idle-timeout mechanics."""
        with (
            patch(
                "src.server.database.runs.lifecycle.workspace_has_active_run",
                new=AsyncMock(return_value=False),
            ),
            patch(
                "src.server.database.runs.subagent_runs.count_open_runs_for_workspace",
                new=AsyncMock(return_value=0),
            ),
        ):
            yield

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

    # --- always-on entitlement reconciliation (bundled into the idle sweep) ---

    @pytest.mark.asyncio
    @patch("src.server.dependencies.usage_limits.always_on_entitlement_lost", new_callable=AsyncMock)
    @patch("src.server.services.workspace_manager.get_workspaces_by_status", new_callable=AsyncMock)
    async def test_cleanup_always_on_entitled_stays_exempt(
        self, mock_get_by_status, mock_lost
    ):
        """A long-idle always-on workspace whose owner is still entitled is
        neither disabled nor reaped — the idle exemption holds."""
        old_time = datetime.now(timezone.utc) - timedelta(hours=2)
        mock_get_by_status.return_value = [
            _make_workspace(is_always_on=True, last_activity_at=old_time),
        ]
        mock_lost.return_value = False

        wm = WorkspaceManager(_make_config(), idle_timeout=1800)
        with (
            patch.object(wm, "stop_workspace", new_callable=AsyncMock) as mock_stop,
            patch.object(wm, "set_workspace_always_on", new_callable=AsyncMock) as mock_disable,
        ):
            count = await wm.cleanup_idle_workspaces()

        assert count == 0
        mock_stop.assert_not_awaited()
        mock_disable.assert_not_awaited()

    @pytest.mark.asyncio
    @patch("src.server.dependencies.usage_limits.always_on_entitlement_lost", new_callable=AsyncMock)
    @patch("src.server.services.workspace_manager.get_workspaces_by_status", new_callable=AsyncMock)
    async def test_cleanup_always_on_lost_idle_disables_and_reaps(
        self, mock_get_by_status, mock_lost
    ):
        """Lost entitlement clears always-on, then the now-normal idle workspace
        is reaped in the same tick."""
        ws_id = str(uuid.uuid4())
        old_time = datetime.now(timezone.utc) - timedelta(hours=2)
        mock_get_by_status.return_value = [
            _make_workspace(
                workspace_id=ws_id, is_always_on=True, last_activity_at=old_time
            ),
        ]
        mock_lost.return_value = True

        wm = WorkspaceManager(_make_config(), idle_timeout=1800)
        with (
            patch.object(wm, "stop_workspace", new_callable=AsyncMock) as mock_stop,
            patch.object(wm, "set_workspace_always_on", new_callable=AsyncMock) as mock_disable,
        ):
            count = await wm.cleanup_idle_workspaces()

        mock_disable.assert_awaited_once_with(ws_id, False)
        mock_stop.assert_awaited_once_with(ws_id)
        assert count == 1

    @pytest.mark.asyncio
    @patch("src.server.dependencies.usage_limits.always_on_entitlement_lost", new_callable=AsyncMock)
    @patch("src.server.services.workspace_manager.get_workspaces_by_status", new_callable=AsyncMock)
    async def test_cleanup_always_on_lost_active_disables_without_stop(
        self, mock_get_by_status, mock_lost
    ):
        """Lost entitlement on an actively-used workspace clears always-on but
        does NOT stop it mid-use — it idle-stops on a later tick."""
        ws_id = str(uuid.uuid4())
        mock_get_by_status.return_value = [
            _make_workspace(
                workspace_id=ws_id,
                is_always_on=True,
                last_activity_at=datetime.now(timezone.utc),
            ),
        ]
        mock_lost.return_value = True

        wm = WorkspaceManager(_make_config(), idle_timeout=1800)
        with (
            patch.object(wm, "stop_workspace", new_callable=AsyncMock) as mock_stop,
            patch.object(wm, "set_workspace_always_on", new_callable=AsyncMock) as mock_disable,
        ):
            count = await wm.cleanup_idle_workspaces()

        mock_disable.assert_awaited_once_with(ws_id, False)
        mock_stop.assert_not_awaited()
        assert count == 0

    @pytest.mark.asyncio
    @patch("src.server.dependencies.usage_limits.always_on_entitlement_lost", new_callable=AsyncMock)
    @patch("src.server.services.workspace_manager.get_workspaces_by_status", new_callable=AsyncMock)
    async def test_cleanup_always_on_validate_memoized_per_user(
        self, mock_get_by_status, mock_lost
    ):
        """Two always-on workspaces for one user trigger a single platform
        validate, not one per workspace."""
        old_time = datetime.now(timezone.utc) - timedelta(hours=2)
        mock_get_by_status.return_value = [
            _make_workspace(
                workspace_id=str(uuid.uuid4()),
                user_id="user-1",
                is_always_on=True,
                last_activity_at=old_time,
            ),
            _make_workspace(
                workspace_id=str(uuid.uuid4()),
                user_id="user-1",
                is_always_on=True,
                last_activity_at=old_time,
            ),
        ]
        mock_lost.return_value = True

        wm = WorkspaceManager(_make_config(), idle_timeout=1800)
        with (
            patch.object(wm, "stop_workspace", new_callable=AsyncMock),
            patch.object(wm, "set_workspace_always_on", new_callable=AsyncMock) as mock_disable,
        ):
            await wm.cleanup_idle_workspaces()

        mock_lost.assert_awaited_once_with("user-1")
        assert mock_disable.await_count == 2


class TestReapStuckStarting:
    """reap_stuck_starting_workspaces reverts rows wedged in 'starting' past the
    reap_stuck_after window, but never reaps a start THIS worker is still running
    (it holds _pending_lazy_sync membership) and leaves fresh rows alone."""

    def setup_method(self):
        WorkspaceManager.reset_instance()

    def teardown_method(self):
        WorkspaceManager.reset_instance()

    @pytest.mark.asyncio
    @patch(
        "src.server.services.workspace_manager.update_workspace_status",
        new_callable=AsyncMock,
    )
    @patch(
        "src.server.services.workspace_manager.get_workspaces_by_status",
        new_callable=AsyncMock,
    )
    async def test_reaps_stale_starting_row(self, mock_get_by_status, mock_status):
        """A row wedged past the threshold with NO local membership is the
        cross-process case (a crashed/recycled worker left it 'starting'): no
        in-process owner will ever recover it, so the reaper reverts it."""
        manager = WorkspaceManager.get_instance(config=_make_config())
        ws_id = str(uuid.uuid4())
        stale = _make_workspace(
            workspace_id=ws_id,
            status="starting",
            updated_at=datetime.now(timezone.utc)
            - timedelta(seconds=manager.reap_stuck_after + 1),
        )
        mock_get_by_status.return_value = [stale]
        # No _pending_lazy_sync membership — no owner on this worker.

        reverted = await manager.reap_stuck_starting_workspaces()

        assert reverted == 1
        mock_status.assert_awaited_once_with(workspace_id=ws_id, status="stopped")

    @pytest.mark.asyncio
    @patch(
        "src.server.services.workspace_manager.update_workspace_status",
        new_callable=AsyncMock,
    )
    @patch(
        "src.server.services.workspace_manager.get_workspaces_by_status",
        new_callable=AsyncMock,
    )
    async def test_leaves_in_flight_lazy_owner_past_threshold(
        self, mock_get_by_status, mock_status
    ):
        """Even PAST the threshold, a row this worker is still starting (it holds
        _pending_lazy_sync) must NOT be reaped — the owner will promote on success
        or revert on failure. Reaping would discard the membership and no-op the
        owner's promotion, stranding a ready session behind a 'stopped' row. This
        guards the slow-archived-restore race independently of the threshold."""
        manager = WorkspaceManager.get_instance(config=_make_config())
        ws_id = str(uuid.uuid4())
        owned = _make_workspace(
            workspace_id=ws_id,
            status="starting",
            updated_at=datetime.now(timezone.utc)
            - timedelta(seconds=manager.reap_stuck_after + 1),
        )
        mock_get_by_status.return_value = [owned]
        manager._pending_lazy_sync.add(ws_id)

        reverted = await manager.reap_stuck_starting_workspaces()

        assert reverted == 0
        mock_status.assert_not_awaited()
        # Membership preserved so the owner's later promotion still fires.
        assert ws_id in manager._pending_lazy_sync

    @pytest.mark.asyncio
    @patch(
        "src.server.services.workspace_manager.update_workspace_status",
        new_callable=AsyncMock,
    )
    @patch(
        "src.server.services.workspace_manager.get_workspaces_by_status",
        new_callable=AsyncMock,
    )
    async def test_leaves_fresh_starting_row(self, mock_get_by_status, mock_status):
        """A start still within the wait window must NOT be reaped — that would
        yank a legitimately in-flight cold restore out from under its owner."""
        manager = WorkspaceManager.get_instance(config=_make_config())
        fresh = _make_workspace(
            status="starting",
            updated_at=datetime.now(timezone.utc) - timedelta(seconds=10),
        )
        mock_get_by_status.return_value = [fresh]

        reverted = await manager.reap_stuck_starting_workspaces()

        assert reverted == 0
        mock_status.assert_not_awaited()

    @pytest.mark.asyncio
    @patch(
        "src.server.services.workspace_manager.update_workspace_status",
        new_callable=AsyncMock,
    )
    @patch(
        "src.server.services.workspace_manager.get_workspaces_by_status",
        new_callable=AsyncMock,
    )
    async def test_leaves_slow_but_legit_restore(self, mock_get_by_status, mock_status):
        """A row older than start_wait_timeout but younger than reap_stuck_after
        is below the reap threshold and must NOT be reaped — even with no local
        membership (e.g. a cross-process start that is slow but not yet wedged).
        This isolates the threshold boundary from the in-process owner guard."""
        manager = WorkspaceManager.get_instance(config=_make_config())
        ws_id = str(uuid.uuid4())
        # Halfway between the two thresholds (e.g. ~450s with defaults).
        age = (manager.start_wait_timeout + manager.reap_stuck_after) / 2
        slow = _make_workspace(
            workspace_id=ws_id,
            status="starting",
            updated_at=datetime.now(timezone.utc) - timedelta(seconds=age),
        )
        mock_get_by_status.return_value = [slow]

        reverted = await manager.reap_stuck_starting_workspaces()

        assert reverted == 0
        mock_status.assert_not_awaited()


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
        wm._pending_lazy_sync.add("ws-1")
        wm._last_sync_at["ws-1"] = time.monotonic()
        wm._workspace_locks["ws-1"] = asyncio.Lock()

        await wm.shutdown()

        assert wm._sessions == {}
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
    @patch("src.server.services.workspace_manager.update_workspace_status")
    @patch("src.server.services.workspace_manager.SessionManager")
    @patch("src.server.services.workspace_manager.db_get_workspace")
    async def test_recover_sandbox_failure_destroys_orphan(
        self, mock_get_ws, mock_session_mgr, mock_status
    ):
        """A failure after the new sandbox is created tears it down via
        cleanup_session, so a half-built recreate never orphans a billed sandbox."""
        manager = self._make_manager()
        ws_id = str(uuid.uuid4())
        mock_get_ws.return_value = _make_workspace(
            workspace_id=ws_id, status="running", resource_tier="standard"
        )

        session = _make_mock_session()
        mock_session_mgr.get_session.return_value = session
        mock_session_mgr.cleanup_session = AsyncMock()

        manager._mint_sandbox_tokens = AsyncMock(return_value={})
        manager._apply_session_mcp = AsyncMock(return_value=None)
        manager._sync_sandbox_assets = AsyncMock()
        # Sandbox is created (initialize ok) but a later step fails.
        manager._restore_files = AsyncMock(side_effect=RuntimeError("restore boom"))

        with pytest.raises(RuntimeError, match="restore boom"):
            await manager._recover_sandbox(ws_id, "user-1", MagicMock())

        mock_session_mgr.cleanup_session.assert_awaited_once_with(ws_id)
        # Broken session not left in the cache.
        assert ws_id not in manager._sessions

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
    @patch("src.server.services.workspace_manager.try_claim_workspace_for_start", new_callable=AsyncMock)
    async def test_stopped_workspace_lazy_init_sandbox_gone_recovers(
        self, mock_claim, mock_activity, mock_status, mock_session_mgr, mock_get_ws
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
        # Cross-worker claim succeeds — this worker wins the start mutex.
        mock_claim.return_value = workspace

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

    @pytest.mark.asyncio
    @pytest.mark.parametrize("always_on", [True, False])
    @patch("src.server.services.workspace_manager.SessionManager")
    @patch("src.server.services.workspace_manager.update_workspace_status")
    @patch("src.server.services.workspace_manager.update_workspace_activity")
    async def test_restart_reseeds_autostop_in_both_directions(
        self, mock_activity, mock_status, mock_session_mgr, always_on
    ):
        """REGRESSION: a plain reconnect must reseed the live auto-stop interval
        from the current is_always_on flag in BOTH directions. Re-asserting only
        the enable direction left a workspace whose always-on was disabled while
        stopped pinned at interval 0 (never auto-stops) until a full recreate —
        the disable healed only on recreate, not on the cheaper reconnect.
        """
        manager = self._make_manager()
        ws_id = str(uuid.uuid4())
        workspace = _make_workspace(
            workspace_id=ws_id, status="stopped", is_always_on=always_on
        )

        session = _make_mock_session()
        mock_session_mgr.get_session.return_value = session

        # Patch non-focus internals so execution reaches the reconnect reseed.
        manager._sync_sandbox_assets = AsyncMock()
        manager._maybe_restore_files = AsyncMock()
        manager._maybe_migrate_sandbox = AsyncMock(return_value=None)
        manager._apply_autostop_for_always_on = AsyncMock()

        await manager._restart_workspace(
            workspace, user_id="user-1", lazy_init=False
        )

        # enabled tracks the flag exactly — True keeps interval 0, False
        # restores the default so the sandbox can auto-stop again. The non-lazy
        # reconnect reuses the live runtime, so it's passed through as ``runtime``.
        manager._apply_autostop_for_always_on.assert_awaited_once_with(
            "sandbox-abc", enabled=always_on, runtime=ANY
        )


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
    @patch("src.server.services.workspace_manager.try_claim_workspace_for_start", new_callable=AsyncMock)
    async def test_stopped_path_forwards_callback_to_initialize_lazy(
        self, mock_claim, mock_status, mock_activity, mock_session_mgr, mock_get_ws
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
        mock_claim.return_value = workspace
        session = _make_mock_session(initialized=False)
        # Simulate lazy init leaving sandbox ready so Phase 2 doesn't retry.
        session.sandbox.is_ready = MagicMock(return_value=True)
        session.sandbox.has_failed = MagicMock(return_value=False)
        mock_session_mgr.get_session.return_value = session

        observed: list[str] = []

        def sentinel(s: str) -> None:
            observed.append(s)

        await manager.get_session_for_workspace(
            ws_id, user_id="user-1", on_state_observed=sentinel
        )

        session.initialize_lazy.assert_awaited_once()
        forwarded = session.initialize_lazy.await_args.kwargs.get("on_state_observed")
        # The stopped (claim-owner) path wraps the caller's callback so it can
        # also broadcast the archived hint cross-worker — the wrapper must still
        # invoke the original observer.
        assert forwarded is not None
        forwarded("stopped")
        assert observed == ["stopped"]
        # Lazy path must not have touched the eager initialize.
        session.initialize.assert_not_awaited()

    @pytest.mark.asyncio
    @patch("src.server.services.workspace_manager.db_get_workspace")
    @patch("src.server.services.workspace_manager.SessionManager")
    @patch("src.server.services.workspace_manager.update_workspace_activity")
    @patch("src.server.services.workspace_manager.update_workspace_status")
    @patch("src.server.services.workspace_manager.try_claim_workspace_for_start", new_callable=AsyncMock)
    async def test_restart_forced_non_lazy_forwards_callback_to_initialize(
        self, mock_claim, mock_status, mock_activity, mock_session_mgr, mock_get_ws
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
        mock_claim.return_value = workspace
        session = _make_mock_session(initialized=False)
        session.sandbox.is_ready = MagicMock(return_value=True)
        session.sandbox.has_failed = MagicMock(return_value=False)
        mock_session_mgr.get_session.return_value = session

        observed: list[str] = []

        def sentinel(s: str) -> None:
            observed.append(s)

        await manager.get_session_for_workspace(
            ws_id, user_id="user-1", on_state_observed=sentinel
        )

        session.initialize.assert_awaited_once()
        forwarded = session.initialize.await_args.kwargs.get("on_state_observed")
        # Claim-owner path wraps the caller's callback; the wrapper must still
        # invoke the original observer even on the forced non-lazy branch.
        assert forwarded is not None
        forwarded("stopped")
        assert observed == ["stopped"]
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
    """Phase 2 distinguishes a failed lazy init (has_failed() == True,
    clear + re-raise) from a post-init transient (has_failed() == False).
    For an UNPROMOTED lazy start, a post-init transient reverts the row to
    'stopped' and re-raises so the caller can't return a sandbox behind a
    'stopped' row (split-brain). Generic Exception keeps the legacy
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
    async def test_phase2_transient_post_init_lazy_reverts_and_raises(
        self, mock_activity, mock_status, mock_session_mgr, mock_get_ws
    ):
        """A post-init transient (e.g. asset sync) on an UNPROMOTED lazy start
        reverts the row to 'stopped' and re-raises. has_failed() == False, so
        the sandbox is healthy — but returning the session here would hand back
        a sandbox the DB says is 'stopped', letting another worker spawn a
        second one (split-brain). The discriminator is _pending_lazy_sync
        membership; the deferred-sync asset step is reached only on that path."""
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
        manager._pending_lazy_sync.add(ws_id)  # unpromoted lazy start
        manager._sessions[ws_id] = session
        manager._last_sync_at = {}
        mock_session_mgr.cleanup_session = AsyncMock()

        with pytest.raises(SandboxTransientError):
            await manager.get_session_for_workspace(ws_id, user_id="user-1")

        # Row reverted so cross-worker losers re-claim immediately instead of
        # the caller returning a sandbox behind a 'stopped' row.
        mock_status.assert_any_await(workspace_id=ws_id, status="stopped")
        assert ws_id not in manager._pending_lazy_sync
        # has_failed() was False — the healthy session is left cached (not
        # cleared); the next request re-claims against the reverted row.
        mock_session_mgr.cleanup_session.assert_not_awaited()

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
    @patch("src.server.services.workspace_manager.try_claim_workspace_for_start", new_callable=AsyncMock)
    async def test_phase2_success_promotes_starting_to_running_and_stamps(
        self, mock_claim, mock_activity, mock_status, mock_session_mgr, mock_get_ws
    ):
        """When Phase 2 finishes the deferred sync, DB is promoted to
        running AND activity is stamped in that order (mirrors PR #152's
        invariant for the lazy path)."""
        manager = self._make_manager()
        ws_id = str(uuid.uuid4())
        workspace = self._lazy_workspace(ws_id, status="stopped")
        mock_get_ws.return_value = workspace
        mock_claim.return_value = workspace

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
    @patch("src.server.services.workspace_manager.try_claim_workspace_for_start", new_callable=AsyncMock)
    async def test_phase2_failure_reverts_status_to_stopped(
        self, mock_claim, mock_activity, mock_status, mock_session_mgr, mock_get_ws
    ):
        """Claim winner fails in Phase 2 → row is reverted "starting" → "stopped"
        (never promoted to running), so cross-worker losers can re-claim
        immediately instead of waiting out the full start_wait_timeout. The
        original exception still propagates."""
        manager = self._make_manager()
        ws_id = str(uuid.uuid4())
        workspace = self._lazy_workspace(ws_id, status="stopped")
        mock_get_ws.return_value = workspace
        mock_claim.return_value = workspace

        lazy_session = _make_mock_session()
        lazy_session.sandbox.ensure_sandbox_ready = AsyncMock(
            side_effect=SandboxTransientError("exhausted retries")
        )
        lazy_session.sandbox.has_failed = MagicMock(return_value=True)
        mock_session_mgr.get_session.return_value = lazy_session
        mock_session_mgr.cleanup_session = AsyncMock()

        with pytest.raises(SandboxTransientError):
            await manager.get_session_for_workspace(ws_id, user_id="user-1")

        status_calls = [c.kwargs for c in mock_status.await_args_list]
        # 'starting' from the claim/restart, then 'stopped' from the Phase 2
        # failure revert — and crucially never 'running'.
        assert {c["status"] for c in status_calls} == {"starting", "stopped"}
        assert status_calls[-1]["status"] == "stopped"
        # Pending-sync marker cleared so the workspace isn't wedged.
        assert ws_id not in manager._pending_lazy_sync

    @pytest.mark.asyncio
    @patch("src.server.services.workspace_manager.db_get_workspace")
    @patch("src.server.services.workspace_manager.SessionManager")
    @patch("src.server.services.workspace_manager.update_workspace_status")
    @patch("src.server.services.workspace_manager.update_workspace_activity")
    @patch("src.server.services.workspace_manager.try_claim_workspace_for_start", new_callable=AsyncMock)
    async def test_phase2_generic_failure_reverts_status_to_stopped(
        self, mock_claim, mock_activity, mock_status, mock_session_mgr, mock_get_ws
    ):
        """A generic Phase 2 failure on an unpromoted lazy start is the most
        dangerous path: it must revert the row to 'stopped' so losers re-claim
        immediately, AND re-raise rather than return the session — returning it
        would hand the agent a sandbox that never finished asset/file sync while
        the DB says 'stopped'."""
        manager = self._make_manager()
        ws_id = str(uuid.uuid4())
        workspace = self._lazy_workspace(ws_id, status="stopped")
        mock_get_ws.return_value = workspace
        mock_claim.return_value = workspace

        lazy_session = _make_mock_session()
        lazy_session.sandbox.ensure_sandbox_ready = AsyncMock(
            side_effect=RuntimeError("daytona hiccup")
        )
        mock_session_mgr.get_session.return_value = lazy_session

        # Unpromoted lazy start: the failure is surfaced, not swallowed.
        with pytest.raises(RuntimeError, match="daytona hiccup"):
            await manager.get_session_for_workspace(ws_id, user_id="user-1")

        status_calls = [c.kwargs for c in mock_status.await_args_list]
        assert {c["status"] for c in status_calls} == {"starting", "stopped"}
        assert status_calls[-1]["status"] == "stopped"
        assert ws_id not in manager._pending_lazy_sync

    @pytest.mark.asyncio
    @patch("src.server.services.workspace_manager.db_get_workspace")
    @patch("src.server.services.workspace_manager.SessionManager")
    @patch("src.server.services.workspace_manager.update_workspace_status")
    @patch("src.server.services.workspace_manager.update_workspace_activity")
    @patch(
        "src.server.services.workspace_manager.try_claim_workspace_for_start",
        new_callable=AsyncMock,
    )
    async def test_phase2_cancelled_reverts_status_to_stopped(
        self, mock_claim, mock_activity, mock_status, mock_session_mgr, mock_get_ws
    ):
        """A client disconnect / shutdown cancels Phase 2. CancelledError is a
        BaseException, so without an explicit handler it would bypass every
        revert and wedge the row in 'starting' forever. It must revert to
        'stopped' AND re-raise to preserve cancellation semantics."""
        manager = self._make_manager()
        ws_id = str(uuid.uuid4())
        workspace = self._lazy_workspace(ws_id, status="stopped")
        mock_get_ws.return_value = workspace
        mock_claim.return_value = workspace

        lazy_session = _make_mock_session()
        lazy_session.sandbox.ensure_sandbox_ready = AsyncMock(
            side_effect=asyncio.CancelledError()
        )
        mock_session_mgr.get_session.return_value = lazy_session

        with pytest.raises(asyncio.CancelledError):
            await manager.get_session_for_workspace(ws_id, user_id="user-1")

        status_calls = [c.kwargs for c in mock_status.await_args_list]
        assert status_calls[-1]["status"] == "stopped"
        assert ws_id not in manager._pending_lazy_sync

    @pytest.mark.asyncio
    @patch(
        "src.server.services.workspace_manager.publish_status_change",
        new_callable=AsyncMock,
    )
    @patch("src.server.services.workspace_manager.db_get_workspace")
    @patch("src.server.services.workspace_manager.SessionManager")
    @patch("src.server.services.workspace_manager.update_workspace_status")
    @patch("src.server.services.workspace_manager.update_workspace_activity")
    @patch(
        "src.server.services.workspace_manager.try_claim_workspace_for_start",
        new_callable=AsyncMock,
    )
    async def test_claim_owner_broadcasts_archived_state(
        self,
        mock_claim,
        mock_activity,
        mock_status,
        mock_session_mgr,
        mock_get_ws,
        mock_publish,
    ):
        """When the pre-start sandbox state is 'archived', the claim owner
        publishes it on the status channel so cross-worker consumers (the
        /events SSE, a losing worker's chat spinner) can show the slow-restore
        copy regardless of who owns the start."""
        manager = self._make_manager()
        ws_id = str(uuid.uuid4())
        workspace = self._lazy_workspace(ws_id, status="stopped")
        mock_get_ws.return_value = workspace
        mock_claim.return_value = workspace

        session = _make_mock_session(initialized=False)
        session.sandbox.is_ready = MagicMock(return_value=True)
        session.sandbox.has_failed = MagicMock(return_value=False)

        async def fake_init_lazy(*args, on_state_observed=None, **kwargs):
            if on_state_observed is not None:
                on_state_observed("archived")

        session.initialize_lazy = AsyncMock(side_effect=fake_init_lazy)
        mock_session_mgr.get_session.return_value = session

        await manager.get_session_for_workspace(ws_id, user_id="user-1")

        archived = [
            c
            for c in mock_publish.call_args_list
            if (c.kwargs.get("extra") or {}).get("sandbox_state") == "archived"
        ]
        assert archived, "claim owner did not broadcast archived sandbox_state"
        assert archived[0].args[1] == "starting"

    @pytest.mark.asyncio
    @patch("src.server.services.workspace_manager.db_get_workspace")
    @patch("src.server.services.workspace_manager.SessionManager")
    @patch("src.server.services.workspace_manager.update_workspace_status")
    @patch("src.server.services.workspace_manager.update_workspace_activity")
    async def test_status_starting_waits_for_other_worker_to_finish(
        self, mock_activity, mock_status, mock_session_mgr, mock_get_ws
    ):
        """Cross-worker safety: a request landing on a workspace already in
        'starting' MUST NOT restart (would double-start the sandbox in another
        worker). It waits for the in-flight start to flip status to 'running',
        then attaches to that session via the running path.

        Replaces the prior "re-enter restart flow" behavior, which was unsafe
        under multi-worker deployments — see ``try_claim_workspace_for_start``
        and ``_wait_for_start_completion``.
        """
        manager = self._make_manager()
        # Tighten polling so the test does not depend on default 300s/0.5s.
        manager.start_wait_timeout = 5.0
        manager.start_wait_poll_interval = 0.01
        ws_id = str(uuid.uuid4())
        starting_ws = self._lazy_workspace(ws_id, status="starting")
        running_ws = self._lazy_workspace(ws_id, status="running")
        # First read sees 'starting'; first poll-iteration read inside
        # _wait_for_start_completion sees 'running' (other worker finished).
        mock_get_ws.side_effect = [starting_ws, running_ws]

        session = _make_mock_session(initialized=True)
        session.sandbox.is_ready = MagicMock(return_value=True)
        session.sandbox.has_failed = MagicMock(return_value=False)
        mock_session_mgr.get_session.return_value = session

        result = await manager.get_session_for_workspace(ws_id, user_id="user-1")

        # Did NOT call initialize_lazy — we did not restart.
        session.initialize_lazy.assert_not_awaited()
        # Returned a usable session attached via the running path.
        assert result is session


# ---------------------------------------------------------------------------
# Status-tuple parametrization for DB-fallback routing (Fix 1 consumers)
# ---------------------------------------------------------------------------


class TestStatusRoutesToDbFallback:
    """Smoke check that the consumer modules route non-live workspaces to the
    DB fallback instead of waking a sandbox. ``workspace_files`` (authenticated)
    compares against the stopped/stopping/starting tuple; ``public``
    (unauthenticated) uses the stronger ``status == "running"`` +
    ``has_ready_session`` no-wake gate. A regression in either reproduces the
    503 storm from the original incident (or, for public, a denial-of-wallet)."""

    @pytest.mark.parametrize("status", ["stopped", "stopping", "starting"])
    def test_workspace_files_tuple_includes_status(self, status):
        from src.server.app.workspace_files import crud
        import inspect

        source = inspect.getsource(crud)
        assert f'"{status}"' in source
        # The authenticated routes compare against this exact tuple.
        assert '"stopped", "stopping", "starting"' in source

    def test_public_routes_gate_on_ready_session(self):
        """The unauthenticated shared file routes must read only a warm
        in-memory session via the no-wake ``get_session_if_ready`` accessor,
        never ``get_session_for_workspace`` which would attach/restart a Daytona
        sandbox for a share-token request (denial-of-wallet)."""
        from src.server.app import public
        import inspect

        source = inspect.getsource(public)
        # list/read/download each read the cached session through the single
        # no-wake accessor rather than acquiring (or waking) one.
        assert source.count("get_session_if_ready(workspace_id)") >= 3


# ---------------------------------------------------------------------------
# Multi-worker start mutex — cross-process race protection
# ---------------------------------------------------------------------------


class TestMultiWorkerStartMutex:
    """``try_claim_workspace_for_start`` atomically flips status='stopped' →
    'starting'; only the winner restarts. Losers wait via
    ``_wait_for_start_completion`` and attach via the running path. These
    tests cover that contract."""

    def setup_method(self):
        WorkspaceManager.reset_instance()

    def teardown_method(self):
        WorkspaceManager.reset_instance()

    def _make_manager(self):
        manager = WorkspaceManager.get_instance(config=_make_config())
        manager.start_wait_timeout = 5.0
        manager.start_wait_poll_interval = 0.01
        manager._sync_sandbox_assets = AsyncMock()
        manager._maybe_restore_files = AsyncMock()
        manager._maybe_migrate_sandbox = AsyncMock(return_value=None)
        return manager

    @pytest.mark.asyncio
    @patch("src.server.services.workspace_manager.db_get_workspace")
    @patch("src.server.services.workspace_manager.SessionManager")
    @patch("src.server.services.workspace_manager.update_workspace_status")
    @patch("src.server.services.workspace_manager.update_workspace_activity")
    @patch("src.server.services.workspace_manager.try_claim_workspace_for_start", new_callable=AsyncMock)
    async def test_winner_proceeds_with_restart(
        self, mock_claim, mock_activity, mock_status, mock_session_mgr, mock_get_ws
    ):
        """Worker that wins the claim restarts the sandbox normally."""
        manager = self._make_manager()
        ws_id = str(uuid.uuid4())
        # Provide a matching config hash so _restart_workspace keeps lazy_init=True.
        manager._compute_sandbox_config_hash = MagicMock(return_value="match")
        workspace = _make_workspace(
            workspace_id=ws_id,
            status="stopped",
            config={"sandbox_config_hash": "match"},
        )
        mock_get_ws.return_value = workspace
        # Claim succeeds — we own the start.
        mock_claim.return_value = workspace

        session = _make_mock_session()
        session.sandbox.ensure_sandbox_ready = AsyncMock()
        mock_session_mgr.get_session.return_value = session

        await manager.get_session_for_workspace(ws_id, user_id="user-1")

        mock_claim.assert_awaited_once_with(ws_id)
        session.initialize_lazy.assert_awaited_once()

    @pytest.mark.asyncio
    @patch("src.server.services.workspace_manager.db_get_workspace")
    @patch("src.server.services.workspace_manager.SessionManager")
    @patch("src.server.services.workspace_manager.update_workspace_status")
    @patch("src.server.services.workspace_manager.update_workspace_activity")
    @patch("src.server.services.workspace_manager.try_claim_workspace_for_start", new_callable=AsyncMock)
    async def test_loser_waits_then_attaches_via_running_path(
        self, mock_claim, mock_activity, mock_status, mock_session_mgr, mock_get_ws
    ):
        """Worker that loses the claim does NOT restart — it waits for the
        winner to finish, then attaches to the now-running session."""
        manager = self._make_manager()
        ws_id = str(uuid.uuid4())
        stopped_ws = _make_workspace(workspace_id=ws_id, status="stopped")
        running_ws = _make_workspace(workspace_id=ws_id, status="running")
        # Phase 1 DB read sees 'stopped'; subsequent poll inside the wait
        # sees 'running' (winner finished).
        mock_get_ws.side_effect = [stopped_ws, running_ws]
        # Claim returns None — another worker already claimed.
        mock_claim.return_value = None

        session = _make_mock_session(initialized=True)
        session.sandbox.is_ready = MagicMock(return_value=True)
        session.sandbox.has_failed = MagicMock(return_value=False)
        mock_session_mgr.get_session.return_value = session

        result = await manager.get_session_for_workspace(ws_id, user_id="user-1")

        mock_claim.assert_awaited_once_with(ws_id)
        # Critical: did not restart — no double-start across workers.
        session.initialize_lazy.assert_not_awaited()
        assert result is session

    @pytest.mark.asyncio
    @patch("src.server.services.workspace_manager.db_get_workspace")
    @patch("src.server.services.workspace_manager.SessionManager")
    @patch("src.server.services.workspace_manager.update_workspace_status")
    @patch("src.server.services.workspace_manager.update_workspace_activity")
    @patch("src.server.services.workspace_manager.try_claim_workspace_for_start", new_callable=AsyncMock)
    async def test_loser_raises_when_winner_errors(
        self, mock_claim, mock_activity, mock_status, mock_session_mgr, mock_get_ws
    ):
        """If the winning worker's start fails (status → 'error'), waiting
        losers surface a RuntimeError rather than hanging or silent success."""
        manager = self._make_manager()
        ws_id = str(uuid.uuid4())
        stopped_ws = _make_workspace(workspace_id=ws_id, status="stopped")
        error_ws = _make_workspace(workspace_id=ws_id, status="error")
        mock_get_ws.side_effect = [stopped_ws, error_ws]
        mock_claim.return_value = None

        with pytest.raises(RuntimeError, match="failed to start"):
            await manager.get_session_for_workspace(ws_id, user_id="user-1")

    @pytest.mark.asyncio
    @patch("src.server.services.workspace_manager.db_get_workspace")
    async def test_wait_helper_times_out_when_stuck(self, mock_get_ws):
        """If status sits in 'starting' past the timeout (winner died mid-
        start), the wait helper raises rather than waiting forever."""
        manager = self._make_manager()
        manager.start_wait_timeout = 0.1
        manager.start_wait_poll_interval = 0.02
        ws_id = str(uuid.uuid4())
        starting_ws = _make_workspace(workspace_id=ws_id, status="starting")
        mock_get_ws.return_value = starting_ws

        with pytest.raises(RuntimeError, match="stuck in 'starting'"):
            await manager._wait_for_start_completion(ws_id)

    @pytest.mark.asyncio
    @patch("src.server.services.workspace_manager.db_get_workspace")
    async def test_wait_helper_raises_on_deletion(self, mock_get_ws):
        """Workspace deleted while waiting → ValueError (caller must not
        keep polling a row that no longer exists)."""
        manager = self._make_manager()
        ws_id = str(uuid.uuid4())
        mock_get_ws.return_value = None

        with pytest.raises(ValueError, match="not found"):
            await manager._wait_for_start_completion(ws_id)

    @pytest.mark.asyncio
    @patch("src.server.services.workspace_manager.db_get_workspace")
    @patch("src.server.services.workspace_manager.SessionManager")
    @patch("src.server.services.workspace_manager.update_workspace_status")
    @patch("src.server.services.workspace_manager.update_workspace_activity")
    @patch(
        "src.server.services.workspace_manager.try_claim_workspace_for_start",
        new_callable=AsyncMock,
    )
    async def test_start_wait_does_not_hold_workspace_lock(
        self, mock_claim, mock_activity, mock_status, mock_session_mgr, mock_get_ws
    ):
        """Regression: the cross-worker start wait must run OUTSIDE the per-
        workspace lock. Otherwise a 60-300s archived cold-start head-of-line
        blocks every other op on that workspace (stop/delete/another get)
        behind the 60s lock-acquire ceiling."""
        manager = self._make_manager()
        ws_id = str(uuid.uuid4())
        starting_ws = _make_workspace(workspace_id=ws_id, status="starting")
        running_ws = _make_workspace(workspace_id=ws_id, status="running")
        mock_get_ws.side_effect = [starting_ws, running_ws]
        mock_claim.return_value = None  # 'starting' arrival skips the claim

        session = _make_mock_session(initialized=True)
        session.sandbox.is_ready = MagicMock(return_value=True)
        session.sandbox.has_failed = MagicMock(return_value=False)
        mock_session_mgr.get_session.return_value = session

        # Gate the wait so we can probe the lock while the caller is parked in it.
        release = asyncio.Event()

        async def _blocking_wait(workspace_id, *a, **k):
            await release.wait()
            return running_ws

        manager._wait_for_start_completion = AsyncMock(side_effect=_blocking_wait)

        waiter = asyncio.create_task(
            manager.get_session_for_workspace(ws_id, user_id="user-1")
        )
        await asyncio.sleep(0.05)  # let the waiter reach the gated wait
        assert not waiter.done()

        # The per-workspace lock MUST be free while the waiter waits. Short
        # timeout so a regression (wait-inside-lock) fails fast instead of
        # hanging the full 60s lock-acquire ceiling.
        async def _probe():
            async with manager._observed_lock(ws_id, "probe"):
                return True

        assert await asyncio.wait_for(_probe(), timeout=2.0) is True

        release.set()
        result = await waiter
        assert result is session

    @pytest.mark.asyncio
    @patch("src.server.services.workspace_manager.db_get_workspace")
    @patch("src.server.services.workspace_manager.SessionManager")
    @patch("src.server.services.workspace_manager.update_workspace_status")
    @patch("src.server.services.workspace_manager.update_workspace_activity")
    @patch(
        "src.server.services.workspace_manager.try_claim_workspace_for_start",
        new_callable=AsyncMock,
    )
    async def test_loser_retries_once_when_owner_reverts_to_stopped(
        self, mock_claim, mock_activity, mock_status, mock_session_mgr, mock_get_ws
    ):
        """If the winner fails and reverts 'starting'→'stopped', the waiting
        loser retries the start once and becomes the owner (restart runs)."""
        manager = self._make_manager()
        manager._compute_sandbox_config_hash = MagicMock(return_value="match")
        ws_id = str(uuid.uuid4())
        stopped_ws = _make_workspace(
            workspace_id=ws_id,
            status="stopped",
            config={"sandbox_config_hash": "match"},
        )
        mock_get_ws.return_value = stopped_ws  # both Phase 1 reads see 'stopped'
        # First claim loses; the post-revert retry wins.
        mock_claim.side_effect = [None, stopped_ws]
        # Owner failed and reverted the row back to 'stopped'.
        manager._wait_for_start_completion = AsyncMock(return_value=stopped_ws)

        session = _make_mock_session()
        session.sandbox.ensure_sandbox_ready = AsyncMock()
        mock_session_mgr.get_session.return_value = session

        await manager.get_session_for_workspace(ws_id, user_id="user-1")

        assert mock_claim.await_count == 2
        session.initialize_lazy.assert_awaited_once()


# ---------------------------------------------------------------------------
# _entitled_tier — lazy spec reclaim at (re)provision time. Keeps the persisted
# tier unless the platform confirms the owner's entitlement lapsed, in which
# case it persists back to standard — but keeps the elevated size when the check
# is inconclusive (OSS / no user) or the backed-up files won't fit standard.
# ---------------------------------------------------------------------------


class TestEntitledTier:
    def setup_method(self):
        WorkspaceManager.reset_instance()

    def teardown_method(self):
        WorkspaceManager.reset_instance()

    def _make_manager(self):
        return WorkspaceManager.get_instance(config=_make_config_with_tiers())

    _LOST = "src.server.dependencies.usage_limits.spec_entitlement_lost"
    _SET_TIER = "src.server.services.workspace_entitlements.db_set_workspace_resource_tier"

    @pytest.mark.asyncio
    async def test_standard_tier_short_circuits(self):
        """standard tier is never reclaimed — no entitlement check."""
        manager = self._make_manager()
        ws = _make_workspace(resource_tier="standard")

        with patch(self._LOST, new_callable=AsyncMock) as mock_lost:
            assert await manager._entitled_tier(ws, "user-1") == "standard"
        mock_lost.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_no_user_keeps_elevated_tier(self):
        """No owner to reconcile against → keep the elevated tier, no check."""
        manager = self._make_manager()
        ws = _make_workspace(resource_tier="max")

        with patch(self._LOST, new_callable=AsyncMock) as mock_lost:
            assert await manager._entitled_tier(ws, None) == "max"
        mock_lost.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_still_entitled_keeps_tier_no_write(self):
        """Entitlement held → keep the elevated tier, nothing persisted."""
        manager = self._make_manager()
        ws = _make_workspace(resource_tier="performance")

        with (
            patch(self._LOST, new_callable=AsyncMock, return_value=False),
            patch(self._SET_TIER) as mock_set_tier,
        ):
            assert await manager._entitled_tier(ws, "user-1") == "performance"
        mock_set_tier.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_lost_and_disk_fits_reclaims_to_standard(self):
        """Lost entitlement + files fit standard → persist and return standard."""
        manager = self._make_manager()
        ws = _make_workspace(workspace_id="ws-1", resource_tier="max")
        manager._assert_disk_fits = AsyncMock()

        with (
            patch(self._LOST, new_callable=AsyncMock, return_value=True),
            patch(self._SET_TIER) as mock_set_tier,
        ):
            assert await manager._entitled_tier(ws, "user-1") == "standard"
        mock_set_tier.assert_awaited_once_with("ws-1", "standard")

    @pytest.mark.asyncio
    async def test_lost_but_files_overflow_keeps_size(self):
        """Lost entitlement but files exceed the standard disk → keep the size
        (data safety over enforcement), nothing persisted."""
        manager = self._make_manager()
        ws = _make_workspace(workspace_id="ws-1", resource_tier="max")
        manager._assert_disk_fits = AsyncMock(
            side_effect=RuntimeError("Cannot downgrade")
        )

        with (
            patch(self._LOST, new_callable=AsyncMock, return_value=True),
            patch(self._SET_TIER) as mock_set_tier,
        ):
            assert await manager._entitled_tier(ws, "user-1") == "max"
        mock_set_tier.assert_not_awaited()


# ---------------------------------------------------------------------------
# _entitled_always_on — lazy always-on reclaim at (re)provision time. Mirrors
# _entitled_tier: keeps the persisted flag unless the platform confirms the
# always-on entitlement lapsed, in which case it clears the flag and returns
# False so the sandbox comes back auto-stop-enabled. The idle reaper only walks
# running rows, so this is what reconciles a workspace stopped when its plan
# lapsed. Fail-safe: keeps always-on when the check is inconclusive (OSS / no
# user).
# ---------------------------------------------------------------------------


class TestEntitledAlwaysOn:
    def setup_method(self):
        WorkspaceManager.reset_instance()

    def teardown_method(self):
        WorkspaceManager.reset_instance()

    def _make_manager(self):
        return WorkspaceManager.get_instance(config=_make_config_with_tiers())

    _LOST = "src.server.dependencies.usage_limits.always_on_entitlement_lost"
    _SET_AO = "src.server.services.workspace_entitlements.db_set_workspace_always_on"

    @pytest.mark.asyncio
    async def test_not_always_on_short_circuits(self):
        """A workspace that isn't always-on is never checked."""
        manager = self._make_manager()
        ws = _make_workspace(is_always_on=False)

        with patch(self._LOST, new_callable=AsyncMock) as mock_lost:
            assert await manager._entitled_always_on(ws, "user-1") is False
        mock_lost.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_no_user_keeps_flag(self):
        """No owner to reconcile against → keep always-on, no check."""
        manager = self._make_manager()
        ws = _make_workspace(is_always_on=True)

        with patch(self._LOST, new_callable=AsyncMock) as mock_lost:
            assert await manager._entitled_always_on(ws, None) is True
        mock_lost.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_still_entitled_keeps_flag_no_write(self):
        """Entitlement held → keep always-on, nothing persisted."""
        manager = self._make_manager()
        ws = _make_workspace(is_always_on=True)

        with (
            patch(self._LOST, new_callable=AsyncMock, return_value=False),
            patch(self._SET_AO) as mock_set_ao,
        ):
            assert await manager._entitled_always_on(ws, "user-1") is True
        mock_set_ao.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_lost_reclaims_and_clears_flag(self):
        """Lost entitlement → clear the flag and return False (auto-stop back on)."""
        manager = self._make_manager()
        ws = _make_workspace(workspace_id="ws-1", is_always_on=True)

        with (
            patch(self._LOST, new_callable=AsyncMock, return_value=True),
            patch(self._SET_AO, new_callable=AsyncMock) as mock_set_ao,
        ):
            assert await manager._entitled_always_on(ws, "user-1") is False
        mock_set_ao.assert_awaited_once_with("ws-1", False)


# ---------------------------------------------------------------------------
# Lazy spec reclaim at (re)provision — Phase-2 arm (_maybe_reclaim_lazy_tier)
# plus the _recover_sandbox seam that rebuilds at the reclaimed tier. The
# reclaim deliberately runs OUTSIDE the per-workspace lock (Phase 1 must stay
# fast); the wiring tests assert lock freedom directly.
# ---------------------------------------------------------------------------


class TestMaybeReclaimLazyTier:
    def setup_method(self):
        WorkspaceManager.reset_instance()

    def teardown_method(self):
        WorkspaceManager.reset_instance()

    def _make_manager(self):
        return WorkspaceManager.get_instance(config=_make_config())

    @pytest.mark.asyncio
    @patch("src.server.services.workspace_entitlements.db_get_workspace")
    async def test_lapsed_tier_destroys_and_recovers(self, mock_get_ws):
        """Lapsed entitlement → destroy the reconnected sandbox, clear the
        session (identity-guarded), and recover at the reclaimed tier."""
        manager = self._make_manager()
        ws_id = str(uuid.uuid4())
        mock_get_ws.return_value = _make_workspace(
            workspace_id=ws_id, status="starting", sandbox_id="sandbox-1",
            resource_tier="max",
        )
        session = _make_mock_session()
        manager._sessions[ws_id] = session

        manager._entitled_tier = AsyncMock(return_value="standard")
        manager._destroy_sandbox = AsyncMock()
        manager._clear_session = AsyncMock()
        recovered = _make_mock_session()
        manager._recover_sandbox = AsyncMock(return_value=recovered)

        result = await manager._maybe_reclaim_lazy_tier(ws_id, "user-1", session)

        assert result is recovered
        manager._destroy_sandbox.assert_awaited_once_with("sandbox-1")
        manager._clear_session.assert_awaited_once_with(ws_id, evict_session=session)
        manager._recover_sandbox.assert_awaited_once_with(ws_id, "user-1", ANY)
        assert ws_id not in manager._pending_lazy_sync

    @pytest.mark.asyncio
    @patch("src.server.services.workspace_entitlements.db_get_workspace")
    async def test_still_entitled_returns_none(self, mock_get_ws):
        """Still entitled → None (proceed on the existing sandbox), no teardown."""
        manager = self._make_manager()
        ws_id = str(uuid.uuid4())
        mock_get_ws.return_value = _make_workspace(
            workspace_id=ws_id, status="starting", sandbox_id="sandbox-1",
            resource_tier="max",
        )
        session = _make_mock_session()
        manager._entitled_tier = AsyncMock(return_value="max")
        manager._destroy_sandbox = AsyncMock()
        manager._recover_sandbox = AsyncMock()

        assert await manager._maybe_reclaim_lazy_tier(ws_id, "user-1", session) is None
        manager._destroy_sandbox.assert_not_awaited()
        manager._recover_sandbox.assert_not_awaited()

    @pytest.mark.asyncio
    @patch("src.server.services.workspace_entitlements.db_get_workspace")
    async def test_standard_tier_skips_entitlement_check(self, mock_get_ws):
        """standard tier → no platform round-trip at all."""
        manager = self._make_manager()
        ws_id = str(uuid.uuid4())
        mock_get_ws.return_value = _make_workspace(
            workspace_id=ws_id, status="starting"
        )
        session = _make_mock_session()
        manager._entitled_tier = AsyncMock()

        assert await manager._maybe_reclaim_lazy_tier(ws_id, "user-1", session) is None
        manager._entitled_tier.assert_not_awaited()

    @pytest.mark.asyncio
    @patch("src.server.services.workspace_entitlements.db_get_workspace")
    async def test_destroy_failure_still_recovers(self, mock_get_ws):
        """Destroying the outsized sandbox is best-effort — a failure logs and
        still proceeds to recover (Daytona reclaims the orphan later)."""
        manager = self._make_manager()
        ws_id = str(uuid.uuid4())
        mock_get_ws.return_value = _make_workspace(
            workspace_id=ws_id, status="starting", sandbox_id="sandbox-1",
            resource_tier="max",
        )
        session = _make_mock_session()
        manager._entitled_tier = AsyncMock(return_value="standard")
        manager._destroy_sandbox = AsyncMock(side_effect=RuntimeError("destroy boom"))
        manager._clear_session = AsyncMock()
        recovered = _make_mock_session()
        manager._recover_sandbox = AsyncMock(return_value=recovered)

        result = await manager._maybe_reclaim_lazy_tier(ws_id, "user-1", session)

        assert result is recovered
        manager._recover_sandbox.assert_awaited_once()

    @pytest.mark.asyncio
    @patch("src.server.services.workspace_entitlements.db_get_workspace")
    async def test_recovery_failure_rearms_pending_and_raises(self, mock_get_ws):
        """A failed recovery re-arms _pending_lazy_sync so Phase 2's generic
        handler reverts the claim row to 'stopped' and re-raises, instead of
        tolerating the failure as a warm re-sync hiccup."""
        manager = self._make_manager()
        ws_id = str(uuid.uuid4())
        mock_get_ws.return_value = _make_workspace(
            workspace_id=ws_id, status="starting", sandbox_id="sandbox-1",
            resource_tier="max",
        )
        session = _make_mock_session()
        manager._entitled_tier = AsyncMock(return_value="standard")
        manager._destroy_sandbox = AsyncMock()
        manager._clear_session = AsyncMock()
        manager._recover_sandbox = AsyncMock(side_effect=RuntimeError("recover boom"))

        with pytest.raises(RuntimeError, match="recover boom"):
            await manager._maybe_reclaim_lazy_tier(ws_id, "user-1", session)
        assert ws_id in manager._pending_lazy_sync


class TestPhase2TierReclaim:
    """Wiring: the reclaim runs in Phase 2 of get_session_for_workspace —
    after the lazy restart, with the per-workspace lock released."""

    def setup_method(self):
        WorkspaceManager.reset_instance()

    def teardown_method(self):
        WorkspaceManager.reset_instance()

    def _make_manager(self):
        manager = WorkspaceManager.get_instance(config=_make_config())
        manager._sync_sandbox_assets = AsyncMock()
        manager._maybe_restore_files = AsyncMock()
        manager._maybe_migrate_sandbox = AsyncMock(return_value=None)
        manager._apply_session_mcp = AsyncMock(return_value=None)
        manager._apply_autostop_for_always_on = AsyncMock()
        manager._compute_sandbox_config_hash = MagicMock(return_value="stable")
        return manager

    @staticmethod
    def _elevated_workspace(workspace_id, status="stopped"):
        return _make_workspace(
            workspace_id=workspace_id,
            status=status,
            sandbox_id="sandbox-1",
            resource_tier="max",
            config={"sandbox_config_hash": "stable"},
        )

    @pytest.mark.asyncio
    @patch("src.server.services.workspace_entitlements.db_get_workspace")
    @patch("src.server.services.workspace_manager.db_get_workspace")
    @patch("src.server.services.workspace_manager.SessionManager")
    @patch("src.server.services.workspace_manager.update_workspace_status")
    @patch("src.server.services.workspace_manager.update_workspace_activity")
    @patch(
        "src.server.services.workspace_manager.try_claim_workspace_for_start",
        new_callable=AsyncMock,
    )
    async def test_lapsed_tier_reclaims_off_the_lock(
        self, mock_claim, mock_activity, mock_status, mock_session_mgr,
        mock_get_ws, mock_ent_get_ws,
    ):
        """Lapsed entitlement on a lazy restart → recovery runs with the
        per-workspace lock FREE (Phase 2), and the caller gets the recovered
        session; the Phase-1 reconnect itself stayed lazy."""
        manager = self._make_manager()
        ws_id = str(uuid.uuid4())
        workspace = self._elevated_workspace(ws_id)
        mock_get_ws.return_value = workspace
        mock_ent_get_ws.return_value = workspace
        mock_claim.return_value = workspace
        mock_session_mgr.cleanup_session = AsyncMock()

        lazy_session = _make_mock_session()
        mock_session_mgr.get_session.return_value = lazy_session

        manager._entitled_tier = AsyncMock(return_value="standard")
        manager._destroy_sandbox = AsyncMock()
        recovered = _make_mock_session()

        lock_was_held: list[bool] = []

        async def recover_probe(workspace_id, user_id, core_config):
            lock = await manager._get_workspace_lock(workspace_id)
            lock_was_held.append(lock.locked())
            return recovered

        manager._recover_sandbox = AsyncMock(side_effect=recover_probe)

        result = await manager.get_session_for_workspace(ws_id, user_id="user-1")

        assert result is recovered
        lazy_session.initialize_lazy.assert_awaited_once()
        manager._destroy_sandbox.assert_awaited_once_with("sandbox-1")
        assert lock_was_held == [False]

    @pytest.mark.asyncio
    @patch("src.server.services.workspace_entitlements.db_get_workspace")
    @patch("src.server.services.workspace_manager.db_get_workspace")
    @patch("src.server.services.workspace_manager.SessionManager")
    @patch("src.server.services.workspace_manager.update_workspace_status")
    @patch("src.server.services.workspace_manager.update_workspace_activity")
    @patch(
        "src.server.services.workspace_manager.try_claim_workspace_for_start",
        new_callable=AsyncMock,
    )
    async def test_entitled_tier_promotes_normally(
        self, mock_claim, mock_activity, mock_status, mock_session_mgr,
        mock_get_ws, mock_ent_get_ws,
    ):
        """Still entitled → no destroy/recover; the lazy start promotes to
        'running' exactly as before the reclaim existed."""
        manager = self._make_manager()
        ws_id = str(uuid.uuid4())
        workspace = self._elevated_workspace(ws_id)
        mock_get_ws.return_value = workspace
        mock_ent_get_ws.return_value = workspace
        mock_claim.return_value = workspace

        lazy_session = _make_mock_session()
        mock_session_mgr.get_session.return_value = lazy_session

        manager._entitled_tier = AsyncMock(return_value="max")
        manager._destroy_sandbox = AsyncMock()
        manager._recover_sandbox = AsyncMock()

        result = await manager.get_session_for_workspace(ws_id, user_id="user-1")

        assert result is lazy_session
        manager._destroy_sandbox.assert_not_awaited()
        manager._recover_sandbox.assert_not_awaited()
        statuses = [c.kwargs.get("status") for c in mock_status.await_args_list]
        assert "running" in statuses

    @pytest.mark.asyncio
    @patch("src.server.services.workspace_entitlements.db_get_workspace")
    @patch("src.server.services.workspace_manager.db_get_workspace")
    @patch("src.server.services.workspace_manager.SessionManager")
    @patch("src.server.services.workspace_manager.update_workspace_status")
    @patch("src.server.services.workspace_manager.update_workspace_activity")
    @patch(
        "src.server.services.workspace_manager.try_claim_workspace_for_start",
        new_callable=AsyncMock,
    )
    async def test_reclaim_recovery_failure_reverts_to_stopped(
        self, mock_claim, mock_activity, mock_status, mock_session_mgr,
        mock_get_ws, mock_ent_get_ws,
    ):
        """A failed reclaim recovery reverts the claim row to 'stopped' (via the
        re-armed pending marker) and surfaces the failure to the caller."""
        manager = self._make_manager()
        ws_id = str(uuid.uuid4())
        workspace = self._elevated_workspace(ws_id)
        mock_get_ws.return_value = workspace
        mock_ent_get_ws.return_value = workspace
        mock_claim.return_value = workspace
        mock_session_mgr.cleanup_session = AsyncMock()

        lazy_session = _make_mock_session()
        mock_session_mgr.get_session.return_value = lazy_session

        manager._entitled_tier = AsyncMock(return_value="standard")
        manager._destroy_sandbox = AsyncMock()
        manager._recover_sandbox = AsyncMock(side_effect=RuntimeError("recover boom"))

        with pytest.raises(RuntimeError, match="recover boom"):
            await manager.get_session_for_workspace(ws_id, user_id="user-1")

        statuses = [c.kwargs.get("status") for c in mock_status.await_args_list]
        assert statuses[-1] == "stopped"


class TestRecoverSandboxEntitledTier:
    def setup_method(self):
        WorkspaceManager.reset_instance()

    def teardown_method(self):
        WorkspaceManager.reset_instance()

    def _make_manager(self):
        return WorkspaceManager.get_instance(config=_make_config())

    @pytest.mark.asyncio
    @patch("src.server.services.workspace_manager.update_workspace_activity")
    @patch("src.server.services.workspace_manager.update_workspace_status")
    @patch("src.server.services.workspace_manager.SessionManager")
    @patch("src.server.services.workspace_manager.db_get_workspace")
    async def test_provisions_at_entitled_tier(
        self, mock_get_ws, mock_session_mgr, mock_status, mock_activity
    ):
        """_recover_sandbox sizes the fresh sandbox to the tier _entitled_tier
        returns, not the raw persisted tier (a lapsed 'max' rebuilds at standard)."""
        manager = self._make_manager()
        ws_id = str(uuid.uuid4())
        mock_get_ws.return_value = _make_workspace(
            workspace_id=ws_id, status="stopped", resource_tier="max"
        )
        session = _make_mock_session()
        mock_session_mgr.get_session.return_value = session

        manager._entitled_tier = AsyncMock(return_value="standard")
        manager._mint_sandbox_tokens = AsyncMock(return_value={})
        manager._apply_session_mcp = AsyncMock(return_value=None)
        manager._sync_sandbox_assets = AsyncMock()
        manager._restore_files = AsyncMock()

        result = await manager._recover_sandbox(ws_id, "user-1", MagicMock())

        assert result is session
        manager._entitled_tier.assert_awaited_once()
        assert session.initialize.await_args.kwargs["tier"] == "standard"


# ---------------------------------------------------------------------------
# set_workspace_spec — tier change recreates the sandbox (hosted Daytona can't
# resize a snapshot sandbox). Persist-then-revert on failure; guards against
# tearing the sandbox out from under a live turn or a too-small disk.
# ---------------------------------------------------------------------------


class TestSetWorkspaceSpec:
    def setup_method(self):
        WorkspaceManager.reset_instance()

    def teardown_method(self):
        WorkspaceManager.reset_instance()

    def _make_manager(self):
        return WorkspaceManager.get_instance(config=_make_config_with_tiers())

    @pytest.mark.asyncio
    @patch("src.server.services.workspace_entitlements.db_get_workspace")
    async def test_unknown_tier_raises_before_db(self, mock_get_ws):
        """An unknown tier is rejected before any DB read."""
        manager = self._make_manager()
        with pytest.raises(ValueError, match="Unknown resource tier"):
            await manager.set_workspace_spec("ws-1", "titanium")
        mock_get_ws.assert_not_called()

    @pytest.mark.asyncio
    @patch("src.server.services.workspace_entitlements.db_set_workspace_resource_tier")
    @patch("src.server.services.workspace_entitlements.db_get_workspace")
    async def test_never_started_persists_tier_only(self, mock_get_ws, mock_set_tier):
        """No sandbox yet → just persist the tier; no recreate."""
        manager = self._make_manager()
        ws = _make_workspace(status="creating", sandbox_id=None, resource_tier="standard")
        mock_get_ws.return_value = ws
        manager._recover_sandbox = AsyncMock()

        await manager.set_workspace_spec(ws["workspace_id"], "performance")

        mock_set_tier.assert_awaited_once_with(ws["workspace_id"], "performance")
        manager._recover_sandbox.assert_not_awaited()

    @pytest.mark.asyncio
    @patch("src.server.services.workspace_entitlements.db_set_workspace_resource_tier")
    @patch("src.server.services.workspace_entitlements.db_get_workspace")
    async def test_already_at_tier_is_noop(self, mock_get_ws, mock_set_tier):
        """Same tier + live sandbox → early return, nothing persisted."""
        manager = self._make_manager()
        ws = _make_workspace(status="running", sandbox_id="sb-1", resource_tier="max")
        mock_get_ws.return_value = ws

        result = await manager.set_workspace_spec(ws["workspace_id"], "max")

        assert result is ws
        mock_set_tier.assert_not_awaited()

    @pytest.mark.asyncio
    @patch("src.server.services.workspace_entitlements.LocalRunExecutor")
    @patch("src.server.services.workspace_manager.SessionManager")
    @patch("src.server.services.workspace_entitlements.update_workspace_status")
    @patch("src.server.services.workspace_entitlements.db_set_workspace_resource_tier")
    @patch("src.server.services.workspace_entitlements.db_get_workspace")
    async def test_running_recreate_failure_marks_stopped_and_reverts_tier(
        self, mock_get_ws, mock_set_tier, mock_status, mock_session_mgr, mock_btm
    ):
        """A failed recreate flips the row to stopped (so the next start
        self-heals via claim -> restart -> SandboxGone -> recover) AND reverts
        the persisted tier."""
        manager = self._make_manager()
        ws = _make_workspace(status="running", sandbox_id="sb-1", resource_tier="standard")
        mock_get_ws.return_value = ws
        mock_btm.get_instance.return_value.has_active_tasks_for_workspace = AsyncMock(
            return_value=False
        )
        mock_session_mgr.cleanup_session = AsyncMock()
        manager._sessions[ws["workspace_id"]] = MagicMock(sandbox=MagicMock())
        manager._backup_files_to_db = AsyncMock()
        manager._destroy_sandbox = AsyncMock()
        manager._recover_sandbox = AsyncMock(side_effect=RuntimeError("snapshot build failed"))

        with pytest.raises(RuntimeError, match="snapshot build failed"):
            await manager.set_workspace_spec(ws["workspace_id"], "max", user_id="user-1")

        # Row marked stopped (not terminal 'error') so the next start recovers.
        mock_status.assert_awaited_once_with(workspace_id=ws["workspace_id"], status="stopped")
        # Tier persisted to the target first, then reverted on the outer except.
        assert mock_set_tier.await_args_list[0].args == (ws["workspace_id"], "max")
        assert mock_set_tier.await_args_list[-1].args == (ws["workspace_id"], "standard")

    @pytest.mark.asyncio
    @patch("src.server.services.workspace_entitlements.LocalRunExecutor")
    @patch("src.server.services.workspace_entitlements.db_set_workspace_resource_tier")
    @patch("src.server.services.workspace_entitlements.db_get_workspace")
    async def test_running_without_attached_session_refuses_and_reverts(
        self, mock_get_ws, mock_set_tier, mock_btm
    ):
        """Running on another replica (no local session): refuse before teardown —
        the backup would silently no-op and destroy the only copy of the files."""
        manager = self._make_manager()
        ws = _make_workspace(status="running", sandbox_id="sb-1", resource_tier="standard")
        mock_get_ws.return_value = ws
        mock_btm.get_instance.return_value.has_active_tasks_for_workspace = AsyncMock(
            return_value=False
        )
        manager._destroy_sandbox = AsyncMock()
        manager._recover_sandbox = AsyncMock()

        with pytest.raises(RuntimeError, match="not attached"):
            await manager.set_workspace_spec(ws["workspace_id"], "max", user_id="user-1")

        manager._destroy_sandbox.assert_not_awaited()
        manager._recover_sandbox.assert_not_awaited()
        assert mock_set_tier.await_args_list[-1].args == (ws["workspace_id"], "standard")

    @pytest.mark.asyncio
    @patch("src.server.services.workspace_entitlements.LocalRunExecutor")
    @patch("src.server.services.workspace_entitlements.db_set_workspace_resource_tier")
    @patch("src.server.services.workspace_entitlements.db_get_workspace")
    async def test_running_with_active_turn_refuses_and_reverts(
        self, mock_get_ws, mock_set_tier, mock_btm
    ):
        """An in-flight agent turn blocks the recreate (would abort execute_code)."""
        manager = self._make_manager()
        ws = _make_workspace(status="running", sandbox_id="sb-1", resource_tier="standard")
        mock_get_ws.return_value = ws
        mock_btm.get_instance.return_value.has_active_tasks_for_workspace = AsyncMock(
            return_value=True
        )
        manager._backup_files_to_db = AsyncMock()
        manager._recover_sandbox = AsyncMock()

        with pytest.raises(RuntimeError, match="agent turn is running"):
            await manager.set_workspace_spec(ws["workspace_id"], "max", user_id="user-1")

        # Refused before any teardown; tier reverted to the original.
        manager._backup_files_to_db.assert_not_awaited()
        manager._recover_sandbox.assert_not_awaited()
        assert mock_set_tier.await_args_list[-1].args == (ws["workspace_id"], "standard")

    @pytest.mark.asyncio
    @patch("src.server.services.workspace_entitlements.db_set_workspace_resource_tier")
    @patch("src.server.services.workspace_entitlements.db_get_workspace")
    async def test_stopped_destroys_sandbox_for_recreate_on_next_start(
        self, mock_get_ws, mock_set_tier
    ):
        """A stopped sandbox is destroyed so the next start rebuilds it at the new tier."""
        manager = self._make_manager()
        ws = _make_workspace(status="stopped", sandbox_id="sb-1", resource_tier="standard")
        mock_get_ws.return_value = ws
        manager._destroy_sandbox = AsyncMock()

        await manager.set_workspace_spec(ws["workspace_id"], "max", user_id="user-1")

        manager._destroy_sandbox.assert_awaited_once_with("sb-1")
        # Upgrade succeeds → tier stays at the target, never reverted.
        assert mock_set_tier.await_args_list[-1].args == (ws["workspace_id"], "max")

    @pytest.mark.asyncio
    @patch("src.server.services.workspace_entitlements.get_workspace_total_size")
    @patch("src.server.services.workspace_entitlements.db_set_workspace_resource_tier")
    @patch("src.server.services.workspace_entitlements.db_get_workspace")
    async def test_downgrade_rejected_when_files_exceed_target_disk(
        self, mock_get_ws, mock_set_tier, mock_size
    ):
        """A downgrade whose backed-up files overflow the smaller disk is refused."""
        manager = self._make_manager()
        # max (10 GiB) → standard (3 GiB); 100 GiB of files won't fit.
        ws = _make_workspace(status="stopped", sandbox_id="sb-1", resource_tier="max")
        mock_get_ws.return_value = ws
        mock_size.return_value = 100 * 1024**3
        manager._destroy_sandbox = AsyncMock()

        with pytest.raises(RuntimeError, match="Cannot downgrade"):
            await manager.set_workspace_spec(ws["workspace_id"], "standard", user_id="user-1")

        # Refused before teardown; tier reverted to the original.
        manager._destroy_sandbox.assert_not_awaited()
        assert mock_set_tier.await_args_list[-1].args == (ws["workspace_id"], "max")


# ---------------------------------------------------------------------------
# duplicate_workspace — copy files + carried tier into a fresh "<name> (copy)";
# re-check the spec entitlement (a duplicate is a new allocation); eager
# sandbox create; mark the new row error on create failure.
# ---------------------------------------------------------------------------


class TestDuplicateWorkspace:
    def setup_method(self):
        WorkspaceManager.reset_instance()

    def teardown_method(self):
        WorkspaceManager.reset_instance()

    def _make_manager(self):
        return WorkspaceManager.get_instance(config=_make_config())

    def _install_create_path(self, manager, session):
        """Stub the eager-create collaborators so the lock body runs clean."""
        manager._mint_sandbox_tokens = AsyncMock(return_value={})
        manager._apply_session_mcp = AsyncMock()
        manager._sync_sandbox_assets = AsyncMock()
        manager._restore_files = AsyncMock()
        manager._record_sync = MagicMock()
        manager._update_workspace_config_fields = AsyncMock()
        manager._sandbox_config_stamp = MagicMock(return_value={})

    @pytest.mark.asyncio
    @patch("src.server.services.workspace_entitlements.db_get_workspace")
    async def test_missing_source_rejected(self, mock_get_ws):
        manager = self._make_manager()
        mock_get_ws.return_value = None
        with pytest.raises(ValueError, match="not found"):
            await manager.duplicate_workspace("ws-x", "user-1")

    @pytest.mark.asyncio
    @patch("src.server.services.workspace_entitlements.db_get_workspace")
    async def test_not_owned_rejected(self, mock_get_ws):
        manager = self._make_manager()
        mock_get_ws.return_value = _make_workspace(user_id="someone-else")
        with pytest.raises(ValueError, match="not found"):
            await manager.duplicate_workspace("ws-x", "user-1")

    @pytest.mark.asyncio
    @patch("src.server.services.workspace_entitlements.db_get_workspace")
    async def test_flash_source_rejected(self, mock_get_ws):
        manager = self._make_manager()
        mock_get_ws.return_value = _make_workspace(status="flash", user_id="user-1")
        with pytest.raises(ValueError, match="flash"):
            await manager.duplicate_workspace("ws-x", "user-1")

    @pytest.mark.asyncio
    @patch("src.server.dependencies.usage_limits.assert_spec_allowed")
    @patch("src.server.services.workspace_manager.update_workspace_status")
    @patch("src.server.services.workspace_entitlements.copy_workspace_files")
    @patch("src.server.services.workspace_entitlements.db_set_workspace_resource_tier")
    @patch("src.server.services.workspace_entitlements.db_create_workspace")
    @patch("src.server.services.workspace_manager.SessionManager")
    @patch("src.server.services.workspace_entitlements.db_get_workspace")
    async def test_carries_tier_and_strips_sandbox_stamps(
        self, mock_get_ws, mock_session_mgr, mock_create, mock_set_tier,
        mock_copy, mock_status, mock_assert_spec,
    ):
        """Entitled duplicate of an elevated workspace carries the tier and drops
        the source's sandbox-identity stamps from the copied config."""
        manager = self._make_manager()
        source = _make_workspace(
            status="stopped",
            user_id="user-1",
            resource_tier="max",
            config={
                "custom": "keep-me",
                "sandbox_config_hash": "abc",
                "sandbox_provider": "daytona",
                "sandbox_working_dir": "/home/workspace",
            },
        )
        mock_get_ws.return_value = source
        new_id = str(uuid.uuid4())
        mock_create.return_value = {"workspace_id": new_id}
        mock_status.return_value = _make_workspace(workspace_id=new_id, status="running")
        mock_assert_spec.return_value = None  # entitled

        session = _make_mock_session()
        mock_session_mgr.get_session.return_value = session
        self._install_create_path(manager, session)

        result = await manager.duplicate_workspace(source["workspace_id"], "user-1")

        # A duplicate is a new allocation → re-check the carried tier's entitlement.
        mock_assert_spec.assert_awaited_once_with("user-1", "max", current_tier="standard")
        mock_set_tier.assert_awaited_once_with(new_id, "max")
        mock_copy.assert_awaited_once_with(source["workspace_id"], new_id)
        # Sandbox-identity stamps stripped; unrelated config keys preserved.
        assert mock_create.call_args.kwargs["config"] == {"custom": "keep-me"}
        # Copy's sandbox built at the carried tier (hosted Daytona can't resize later).
        assert session.initialize.await_args.kwargs["tier"] == "max"
        mock_status.assert_awaited_once_with(
            workspace_id=new_id, status="running", sandbox_id="sandbox-abc"
        )
        assert result["status"] == "running"

    @pytest.mark.asyncio
    @patch("src.server.dependencies.usage_limits.assert_spec_allowed")
    @patch("src.server.services.workspace_manager.update_workspace_status")
    @patch("src.server.services.workspace_entitlements.copy_workspace_files")
    @patch("src.server.services.workspace_entitlements.db_set_workspace_resource_tier")
    @patch("src.server.services.workspace_entitlements.db_create_workspace")
    @patch("src.server.services.workspace_manager.SessionManager")
    @patch("src.server.services.workspace_entitlements.db_get_workspace")
    async def test_entitlement_lost_downgrades_copy_to_standard(
        self, mock_get_ws, mock_session_mgr, mock_create, mock_set_tier,
        mock_copy, mock_status, mock_assert_spec,
    ):
        """If the user is no longer entitled to the source's tier, the copy is
        created at standard instead of failing the duplicate."""
        from fastapi import HTTPException

        manager = self._make_manager()
        source = _make_workspace(status="stopped", user_id="user-1", resource_tier="performance")
        mock_get_ws.return_value = source
        new_id = str(uuid.uuid4())
        mock_create.return_value = {"workspace_id": new_id}
        mock_status.return_value = _make_workspace(workspace_id=new_id, status="running")
        mock_assert_spec.side_effect = HTTPException(403, detail="Requires scope: workspace:spec:performance")

        session = _make_mock_session()
        mock_session_mgr.get_session.return_value = session
        self._install_create_path(manager, session)

        await manager.duplicate_workspace(source["workspace_id"], "user-1")

        # Fell back to standard → tier not carried, sandbox built at standard.
        mock_set_tier.assert_not_awaited()
        assert session.initialize.await_args.kwargs["tier"] == "standard"

    @pytest.mark.asyncio
    @patch("src.server.services.workspace_entitlements.update_workspace_status")
    @patch("src.server.services.workspace_entitlements.copy_workspace_files")
    @patch("src.server.services.workspace_entitlements.db_create_workspace")
    @patch("src.server.services.workspace_manager.SessionManager")
    @patch("src.server.services.workspace_entitlements.db_get_workspace")
    async def test_sandbox_create_failure_marks_new_row_error(
        self, mock_get_ws, mock_session_mgr, mock_create, mock_copy, mock_status
    ):
        """A failed sandbox create marks the new row error and re-raises."""
        manager = self._make_manager()
        source = _make_workspace(status="stopped", user_id="user-1", resource_tier="standard")
        mock_get_ws.return_value = source
        new_id = str(uuid.uuid4())
        mock_create.return_value = {"workspace_id": new_id}

        session = _make_mock_session()
        session.initialize = AsyncMock(side_effect=RuntimeError("sandbox boom"))
        mock_session_mgr.get_session.return_value = session
        mock_session_mgr.cleanup_session = AsyncMock()
        self._install_create_path(manager, session)

        with pytest.raises(RuntimeError, match="sandbox boom"):
            await manager.duplicate_workspace(source["workspace_id"], "user-1")

        mock_status.assert_awaited_once_with(workspace_id=new_id, status="error")
        # Orphaned sandbox is torn down, not left billing.
        mock_session_mgr.cleanup_session.assert_awaited_once_with(new_id)
