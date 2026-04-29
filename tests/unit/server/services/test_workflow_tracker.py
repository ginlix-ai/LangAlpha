"""
Tests for WorkflowTracker service.

Tests workflow status tracking via Redis cache: marking active/disconnected/
completed/cancelled/interrupted, cancel flags, retry counts, and graceful
degradation when Redis is unavailable.
"""

import inspect
import uuid
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.server.services.workflow_tracker import (
    RECONNECTABLE_STATUSES,
    TERMINAL_STATUSES,
    WorkflowStatus,
    WorkflowTracker,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_tracker(enabled=True):
    """Create a WorkflowTracker with mocked Redis cache client."""
    with patch("src.server.services.workflow_tracker.get_cache_client") as mock_get:
        mock_cache = AsyncMock()
        mock_cache.enabled = enabled
        mock_cache.get = AsyncMock(return_value=None)
        mock_cache.set = AsyncMock(return_value=True)
        mock_cache.delete = AsyncMock(return_value=True)
        mock_cache.exists = AsyncMock(return_value=False)
        mock_get.return_value = mock_cache

        tracker = WorkflowTracker()
        return tracker, mock_cache


# ---------------------------------------------------------------------------
# Singleton
# ---------------------------------------------------------------------------

class TestSingleton:
    """Test WorkflowTracker singleton pattern."""

    def teardown_method(self):
        WorkflowTracker._instance = None

    @patch("src.server.services.workflow_tracker.get_cache_client")
    def test_get_instance_creates_singleton(self, mock_get):
        mock_cache = MagicMock()
        mock_cache.enabled = True
        mock_get.return_value = mock_cache

        instance = WorkflowTracker.get_instance()
        assert instance is not None
        assert isinstance(instance, WorkflowTracker)

    @patch("src.server.services.workflow_tracker.get_cache_client")
    def test_get_instance_returns_same_instance(self, mock_get):
        mock_cache = MagicMock()
        mock_cache.enabled = True
        mock_get.return_value = mock_cache

        first = WorkflowTracker.get_instance()
        second = WorkflowTracker.get_instance()
        assert first is second


# ---------------------------------------------------------------------------
# mark_active
# ---------------------------------------------------------------------------

class TestMarkActive:
    """Test marking workflows as active."""

    def teardown_method(self):
        WorkflowTracker._instance = None

    @pytest.mark.asyncio
    async def test_mark_active_success(self):
        tracker, mock_cache = _make_tracker(enabled=True)
        thread_id = str(uuid.uuid4())

        result = await tracker.mark_active(
            thread_id=thread_id,
            workspace_id="ws-1",
            user_id="user-1",
        )

        assert result is True
        mock_cache.set.assert_awaited_once()
        call_args = mock_cache.set.call_args
        key = call_args[0][0]
        obj = call_args[0][1]
        assert key == f"workflow:status:{thread_id}"
        assert obj["status"] == WorkflowStatus.ACTIVE
        assert obj["workspace_id"] == "ws-1"
        assert obj["user_id"] == "user-1"

    @pytest.mark.asyncio
    async def test_mark_active_disabled(self):
        tracker, mock_cache = _make_tracker(enabled=False)

        result = await tracker.mark_active("t-1", "ws-1", "user-1")
        assert result is False
        mock_cache.set.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_mark_active_with_metadata(self):
        tracker, mock_cache = _make_tracker(enabled=True)
        thread_id = str(uuid.uuid4())

        result = await tracker.mark_active(
            thread_id=thread_id,
            workspace_id="ws-1",
            user_id="user-1",
            metadata={"model": "gpt-4"},
        )

        assert result is True
        obj = mock_cache.set.call_args[0][1]
        assert obj["metadata"]["model"] == "gpt-4"


# ---------------------------------------------------------------------------
# mark_completed / mark_disconnected / mark_interrupted / mark_cancelled
# ---------------------------------------------------------------------------

class TestMarkTransitions:
    """Test status transition methods."""

    def teardown_method(self):
        WorkflowTracker._instance = None

    @pytest.mark.asyncio
    async def test_mark_completed_sets_ttl(self):
        tracker, mock_cache = _make_tracker(enabled=True)
        thread_id = str(uuid.uuid4())

        result = await tracker.mark_completed(thread_id)

        assert result is True
        call_kwargs = mock_cache.set.call_args
        # TTL should be COMPLETED_TTL (3600)
        assert call_kwargs.kwargs.get("ttl") == 3600 or (
            len(call_kwargs.args) > 2 and call_kwargs.args[2] == 3600
        )

    @pytest.mark.asyncio
    async def test_mark_disconnected_no_ttl(self):
        tracker, mock_cache = _make_tracker(enabled=True)
        thread_id = str(uuid.uuid4())

        result = await tracker.mark_disconnected(thread_id)

        assert result is True
        mock_cache.set.assert_awaited()

    @pytest.mark.asyncio
    async def test_mark_interrupted_success(self):
        tracker, mock_cache = _make_tracker(enabled=True)
        thread_id = str(uuid.uuid4())

        result = await tracker.mark_interrupted(thread_id)

        assert result is True

    @pytest.mark.asyncio
    async def test_mark_cancelled_success(self):
        tracker, mock_cache = _make_tracker(enabled=True)
        thread_id = str(uuid.uuid4())

        result = await tracker.mark_cancelled(thread_id)

        assert result is True

    @pytest.mark.asyncio
    async def test_mark_failed_sets_ttl_and_status(self):
        tracker, mock_cache = _make_tracker(enabled=True)
        thread_id = str(uuid.uuid4())

        result = await tracker.mark_failed(thread_id, error="boom")

        assert result is True
        # Last positional/kwarg holds the persisted status object.
        call_args = mock_cache.set.call_args
        persisted = call_args.args[1] if len(call_args.args) > 1 else call_args.kwargs.get("value")
        assert persisted["status"] == "failed"
        assert persisted["metadata"]["error"] == "boom"
        # Bounded TTL (matches mark_completed/mark_cancelled).
        ttl = call_args.kwargs.get("ttl") or (
            call_args.args[2] if len(call_args.args) > 2 else None
        )
        assert ttl == 3600

    @pytest.mark.asyncio
    async def test_mark_failed_without_error_omits_metadata(self):
        tracker, mock_cache = _make_tracker(enabled=True)
        thread_id = str(uuid.uuid4())

        result = await tracker.mark_failed(thread_id)

        assert result is True
        call_args = mock_cache.set.call_args
        persisted = call_args.args[1] if len(call_args.args) > 1 else call_args.kwargs.get("value")
        # No metadata block when error is None — matches mark_cancelled.
        assert "metadata" not in persisted or persisted["metadata"] == {}

    @pytest.mark.asyncio
    async def test_mark_soft_interrupted_sets_ttl(self):
        tracker, mock_cache = _make_tracker(enabled=True)
        thread_id = str(uuid.uuid4())

        result = await tracker.mark_soft_interrupted(thread_id)

        assert result is True
        call_args = mock_cache.set.call_args
        persisted = call_args.args[1] if len(call_args.args) > 1 else call_args.kwargs.get("value")
        assert persisted["status"] == "soft_interrupted"
        ttl = call_args.kwargs.get("ttl") or (
            call_args.args[2] if len(call_args.args) > 2 else None
        )
        # Distinct from INTERRUPTED (which uses ttl=None) — bounded TTL.
        assert ttl == 3600

    @pytest.mark.asyncio
    async def test_all_methods_disabled(self):
        tracker, _ = _make_tracker(enabled=False)
        tid = "t-1"

        assert await tracker.mark_completed(tid) is False
        assert await tracker.mark_disconnected(tid) is False
        assert await tracker.mark_interrupted(tid) is False
        assert await tracker.mark_cancelled(tid) is False
        assert await tracker.mark_failed(tid, error="x") is False
        assert await tracker.mark_soft_interrupted(tid) is False


# ---------------------------------------------------------------------------
# Status set invariants
# ---------------------------------------------------------------------------

class TestStatusSetInvariants:
    """Pin TERMINAL_STATUSES / RECONNECTABLE_STATUSES against workflow_handler."""

    def test_terminal_disjoint_from_reconnectable(self):
        # If both sets share a state, ``can_reconnect`` would return True for a
        # terminal workflow — frontend would attach to a stream that never
        # produces events.
        assert TERMINAL_STATUSES.isdisjoint(RECONNECTABLE_STATUSES)

    def test_every_status_categorized(self):
        # Every WorkflowStatus is either terminal, reconnectable, or one of the
        # known intermediate/sentinel states. Adding a new status without
        # placing it in this partition fails the test.
        intermediate = {WorkflowStatus.INTERRUPTED, WorkflowStatus.UNKNOWN}
        partition = TERMINAL_STATUSES | RECONNECTABLE_STATUSES | intermediate
        assert set(WorkflowStatus) == partition

    def test_workflow_handler_uses_reconnectable_constant(self):
        # get_workflow_status reads from RECONNECTABLE_STATUSES so the
        # invariant above actually pins the production reconnect decision.
        from src.server.handlers import workflow_handler

        source = inspect.getsource(workflow_handler.get_workflow_status)
        assert "RECONNECTABLE_STATUSES" in source


# ---------------------------------------------------------------------------
# Cancel flag
# ---------------------------------------------------------------------------

class TestCancelFlag:
    """Test cancel flag operations."""

    def teardown_method(self):
        WorkflowTracker._instance = None

    @pytest.mark.asyncio
    async def test_set_cancel_flag(self):
        tracker, mock_cache = _make_tracker(enabled=True)
        thread_id = str(uuid.uuid4())

        result = await tracker.set_cancel_flag(thread_id)

        assert result is True
        call_args = mock_cache.set.call_args
        key = call_args[0][0]
        assert key == f"workflow:cancel:{thread_id}"

    @pytest.mark.asyncio
    async def test_is_cancelled_false(self):
        tracker, mock_cache = _make_tracker(enabled=True)
        mock_cache.exists.return_value = False

        result = await tracker.is_cancelled("t-1")
        assert result is False

    @pytest.mark.asyncio
    async def test_is_cancelled_true(self):
        tracker, mock_cache = _make_tracker(enabled=True)
        mock_cache.exists.return_value = True

        result = await tracker.is_cancelled("t-1")
        assert result is True

    @pytest.mark.asyncio
    async def test_cancel_flag_disabled(self):
        tracker, _ = _make_tracker(enabled=False)

        assert await tracker.set_cancel_flag("t-1") is False
        assert await tracker.is_cancelled("t-1") is False


# ---------------------------------------------------------------------------
# get_status / delete_status
# ---------------------------------------------------------------------------

class TestStatusOperations:
    """Test get and delete status operations."""

    def teardown_method(self):
        WorkflowTracker._instance = None

    @pytest.mark.asyncio
    async def test_get_status_found(self):
        tracker, mock_cache = _make_tracker(enabled=True)
        thread_id = str(uuid.uuid4())
        expected = {"status": "active", "thread_id": thread_id}
        mock_cache.get.return_value = expected

        result = await tracker.get_status(thread_id)
        assert result == expected

    @pytest.mark.asyncio
    async def test_get_status_not_found(self):
        tracker, mock_cache = _make_tracker(enabled=True)
        mock_cache.get.return_value = None

        result = await tracker.get_status("t-1")
        assert result is None

    @pytest.mark.asyncio
    async def test_get_status_disabled(self):
        tracker, _ = _make_tracker(enabled=False)
        result = await tracker.get_status("t-1")
        assert result is None

    @pytest.mark.asyncio
    async def test_delete_status_success(self):
        tracker, mock_cache = _make_tracker(enabled=True)
        thread_id = str(uuid.uuid4())

        result = await tracker.delete_status(thread_id)

        assert result is True
        # Should delete both status and cancel keys
        assert mock_cache.delete.await_count == 2

    @pytest.mark.asyncio
    async def test_delete_status_disabled(self):
        tracker, _ = _make_tracker(enabled=False)
        result = await tracker.delete_status("t-1")
        assert result is False


# ---------------------------------------------------------------------------
# Retry count tracking
# ---------------------------------------------------------------------------

class TestRetryCount:
    """Test retry count increment/get/reset."""

    def teardown_method(self):
        WorkflowTracker._instance = None

    @pytest.mark.asyncio
    async def test_increment_retry_count(self):
        tracker, mock_cache = _make_tracker(enabled=True)
        thread_id = str(uuid.uuid4())
        mock_cache.get.return_value = {
            "status": "active",
            "thread_id": thread_id,
            "retry_count": 1,
        }

        result = await tracker.increment_retry_count(thread_id)

        assert result == 2
        mock_cache.set.assert_awaited_once()
        saved_obj = mock_cache.set.call_args[0][1]
        assert saved_obj["retry_count"] == 2

    @pytest.mark.asyncio
    async def test_increment_retry_count_no_status(self):
        tracker, mock_cache = _make_tracker(enabled=True)
        mock_cache.get.return_value = None

        result = await tracker.increment_retry_count("t-1")
        assert result == 0

    @pytest.mark.asyncio
    async def test_get_retry_count(self):
        tracker, mock_cache = _make_tracker(enabled=True)
        mock_cache.get.return_value = {"retry_count": 3}

        result = await tracker.get_retry_count("t-1")
        assert result == 3

    @pytest.mark.asyncio
    async def test_get_retry_count_default_zero(self):
        tracker, mock_cache = _make_tracker(enabled=True)
        mock_cache.get.return_value = {"status": "active"}

        result = await tracker.get_retry_count("t-1")
        assert result == 0

    @pytest.mark.asyncio
    async def test_reset_retry_count(self):
        tracker, mock_cache = _make_tracker(enabled=True)
        thread_id = str(uuid.uuid4())
        mock_cache.get.return_value = {
            "status": "active",
            "retry_count": 5,
        }

        result = await tracker.reset_retry_count(thread_id)

        assert result is True
        saved_obj = mock_cache.set.call_args[0][1]
        assert saved_obj["retry_count"] == 0

    @pytest.mark.asyncio
    async def test_reset_retry_count_no_status(self):
        tracker, mock_cache = _make_tracker(enabled=True)
        mock_cache.get.return_value = None

        result = await tracker.reset_retry_count("t-1")
        assert result is False

    @pytest.mark.asyncio
    async def test_retry_disabled(self):
        tracker, _ = _make_tracker(enabled=False)

        assert await tracker.increment_retry_count("t-1") == 0
        assert await tracker.get_retry_count("t-1") == 0
        assert await tracker.reset_retry_count("t-1") is False


# ---------------------------------------------------------------------------
# WorkflowStatus enum
# ---------------------------------------------------------------------------

class TestWorkflowStatusEnum:
    """Test WorkflowStatus enum values."""

    def test_enum_values(self):
        assert WorkflowStatus.ACTIVE == "active"
        assert WorkflowStatus.DISCONNECTED == "disconnected"
        assert WorkflowStatus.COMPLETED == "completed"
        assert WorkflowStatus.INTERRUPTED == "interrupted"
        assert WorkflowStatus.CANCELLED == "cancelled"
        assert WorkflowStatus.UNKNOWN == "unknown"

    def test_enum_is_str(self):
        assert isinstance(WorkflowStatus.ACTIVE, str)
