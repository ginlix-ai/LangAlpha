"""
Tests for WorkflowTracker service.

Tests workflow status tracking via Redis cache: marking active/disconnected/
completed/cancelled/interrupted, status get/delete, and graceful degradation
when Redis is unavailable. (v4 dropped the Redis cancel flag and retry-count
counter — cancel intent is durable on the run row, retry counts come from the
attempt chain.)
"""

import json
import uuid
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.config.settings import get_redis_ttl_workflow_status
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
        # Status transitions run as one Lua CAS on the raw client.
        mock_cache.client.eval = AsyncMock(return_value=1)
        mock_get.return_value = mock_cache

        tracker = WorkflowTracker()
        return tracker, mock_cache


async def _call_get_workflow_status(status: WorkflowStatus, bg_status: str) -> dict:
    """Drive workflow_handler.get_workflow_status with the supplied tracker
    status, stubbing every other dependency. Returns the response dict."""
    from src.server.handlers import workflow_handler

    tracker = MagicMock()
    tracker.get_status = AsyncMock(return_value={
        "status": status,
        "last_update": None,
        "workspace_id": "ws-1",
        "user_id": "u-1",
    })
    tracker.mark_completed = AsyncMock(return_value=True)
    tracker.delete_status = AsyncMock(return_value=True)

    bg_manager = MagicMock()
    bg_manager.get_live_task_info = AsyncMock(return_value={
        "live": bg_status != "not_found",
        "active_tasks": [],
    })

    cache = MagicMock()
    cache.enabled = False
    cache.client = None

    with patch(
        "src.server.services.workflow_tracker.WorkflowTracker.get_instance",
        return_value=tracker,
    ), patch.object(
        workflow_handler, "get_checkpoint_tuple", new=AsyncMock(return_value=None)
    ), patch(
        "src.server.services.background_task_manager.BackgroundTaskManager.get_instance",
        return_value=bg_manager,
    ), patch(
        "src.server.database.conversation.get_thread_by_id",
        new=AsyncMock(return_value=None),
    ), patch(
        "src.utils.cache.redis_cache.get_cache_client",
        return_value=cache,
    ):
        return await workflow_handler.get_workflow_status("t-1")


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
# mark_completed / mark_interrupted / mark_cancelled
# ---------------------------------------------------------------------------

class TestMarkTransitions:
    """Test status transition methods."""

    def teardown_method(self):
        WorkflowTracker._instance = None

    @staticmethod
    def _eval_args(mock_cache):
        """(key, run_id_arg, patch, ttl) from the CAS eval call.

        eval(script, numkeys, key, run_id, patch_json, ttl, thread_id,
        started_at)."""
        args = mock_cache.client.eval.call_args.args
        return args[2], args[3], json.loads(args[4]), args[5]

    @pytest.mark.asyncio
    async def test_mark_completed_sets_ttl(self):
        tracker, mock_cache = _make_tracker(enabled=True)
        thread_id = str(uuid.uuid4())

        result = await tracker.mark_completed(thread_id)

        assert result is True
        key, _rid, patch, ttl = self._eval_args(mock_cache)
        assert key == f"workflow:status:{thread_id}"
        assert patch["status"] == "completed"
        assert ttl == get_redis_ttl_workflow_status()

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
        _key, _rid, patch, ttl = self._eval_args(mock_cache)
        assert patch["status"] == "failed"
        assert patch["metadata"]["error"] == "boom"
        # Bounded TTL (matches mark_completed/mark_cancelled).
        assert ttl == get_redis_ttl_workflow_status()

    @pytest.mark.asyncio
    async def test_mark_failed_without_error_omits_metadata(self):
        tracker, mock_cache = _make_tracker(enabled=True)
        thread_id = str(uuid.uuid4())

        result = await tracker.mark_failed(thread_id)

        assert result is True
        _key, _rid, patch, _ttl = self._eval_args(mock_cache)
        # _update_status_with_metadata only patches metadata when truthy —
        # match the implementation exactly so a regression to
        # ``{"metadata": {}}`` (which would clobber-merge nothing but still
        # create the key) would fail this test.
        assert "metadata" not in patch

    @pytest.mark.asyncio
    async def test_all_methods_disabled(self):
        tracker, _ = _make_tracker(enabled=False)
        tid = "t-1"

        assert await tracker.mark_completed(tid) is False
        assert await tracker.mark_interrupted(tid) is False
        assert await tracker.mark_cancelled(tid) is False
        assert await tracker.mark_failed(tid, error="x") is False

    @pytest.mark.asyncio
    async def test_stale_terminal_writer_cannot_overwrite_newer_marker(self):
        """Codex 2.3 round-10 F3: run G1's late terminal update must not
        replace run G2's freshly written ACTIVE marker. The pre-CAS
        read-modify-write let G1 pass the run_id gate before G2's
        mark_active and then clobber it — erasing the admission stamp the
        dispatch oracle keys on. Gate + write are now one Lua step (the
        fake exercises the same gate semantics)."""
        from tests.unit.server.handlers.chat.redis_fakes import FakeCache

        cache = FakeCache()
        with patch(
            "src.server.services.workflow_tracker.get_cache_client",
            return_value=cache,
        ):
            tracker = WorkflowTracker()

        await tracker.mark_active(
            "t-1", "ws-1", "u-1", run_id="G2",
            metadata={"origin_dispatch_gen": "gen-2"},
        )
        assert await tracker.mark_completed("t-1", run_id="G1") is False

        blob = cache.kv["workflow:status:t-1"]
        assert blob["run_id"] == "G2"
        assert blob["status"] == WorkflowStatus.ACTIVE
        assert blob["metadata"]["origin_dispatch_gen"] == "gen-2"

        # The current run's own terminal still lands, metadata merged.
        assert await tracker.mark_completed("t-1", run_id="G2") is True
        blob = cache.kv["workflow:status:t-1"]
        assert blob["status"] == "completed"
        assert blob["metadata"]["origin_dispatch_gen"] == "gen-2"

    @pytest.mark.asyncio
    async def test_status_gated_write_refuses_same_run_terminal(self):
        """Codex 2.3 round-12 F1: a healer that observed ACTIVE must not
        overwrite the SAME run's later terminal state — run G1 can go
        INTERRUPTED (HITL resumability) between the healer's read and its
        write, and the run_id gate alone cannot see that. The status-gated
        CAS refuses; an absent blob refuses too (nothing to heal)."""
        from tests.unit.server.handlers.chat.redis_fakes import FakeCache

        cache = FakeCache()
        with patch(
            "src.server.services.workflow_tracker.get_cache_client",
            return_value=cache,
        ):
            tracker = WorkflowTracker()

        await tracker.mark_active("t-1", "ws-1", "u-1", run_id="G1")
        await tracker.mark_interrupted(
            "t-1", run_id="G1", metadata={"interrupt_reason": "hitl"}
        )

        healed = await tracker.mark_completed(
            "t-1",
            run_id="G1",
            metadata={"healed": "stale_active_no_task"},
            expected_status=WorkflowStatus.ACTIVE,
        )
        assert healed is False
        blob = cache.kv["workflow:status:t-1"]
        assert blob["status"] == "interrupted"
        assert blob["metadata"]["interrupt_reason"] == "hitl"

        # Status-gated write against a genuinely ACTIVE blob still lands.
        await tracker.mark_active("t-2", "ws-1", "u-1", run_id="G1")
        assert (
            await tracker.mark_completed(
                "t-2", run_id="G1", expected_status=WorkflowStatus.ACTIVE
            )
            is True
        )

        # Absent blob: a status-gated heal has nothing to heal — refused.
        assert (
            await tracker.mark_completed(
                "t-gone", run_id="G1", expected_status=WorkflowStatus.ACTIVE
            )
            is False
        )

    @pytest.mark.asyncio
    async def test_receipt_gated_admission_refuses_resolved_generation(self):
        """Codex 2.3 round-13 P0: the orphan resolver receipts a generation
        it resolved as phantom; that generation's late admission must be
        refused ATOMICALLY at the marker write — admitting would run a turn
        whose watch state is already gone (report-back silently drops)."""
        from tests.unit.server.handlers.chat.redis_fakes import FakeCache

        cache = FakeCache()
        with patch(
            "src.server.services.workflow_tracker.get_cache_client",
            return_value=cache,
        ):
            tracker = WorkflowTracker()

        cache.client.sets["ptc_rb_resolved:t-1"] = {"g-PHANTOM"}

        marked = await tracker.mark_active(
            "t-1",
            "ws-1",
            "u-1",
            run_id="G1",
            metadata={"origin_dispatch_gen": "g-PHANTOM"},
            refuse_receipt_key="ptc_rb_resolved:t-1",
            receipt_member="g-PHANTOM",
        )
        assert marked is False
        assert "workflow:status:t-1" not in cache.kv

        # A different (un-receipted) generation admits normally through the
        # same gated path, and the marker carries its admission stamp.
        marked = await tracker.mark_active(
            "t-1",
            "ws-1",
            "u-1",
            run_id="G2",
            metadata={"origin_dispatch_gen": "g-FRESH"},
            refuse_receipt_key="ptc_rb_resolved:t-1",
            receipt_member="g-FRESH",
        )
        assert marked is True
        blob = cache.kv["workflow:status:t-1"]
        assert blob["status"] == "active"
        assert blob["metadata"]["origin_dispatch_gen"] == "g-FRESH"

    @pytest.mark.asyncio
    async def test_gated_admission_stamps_admitted_gen_on_exact_gen_origin(self):
        """Codex round-14 P0: the marker's terminal TTL (1h) is 23h shorter
        than the origin's — admission must leave a durable identity ON THE
        ORIGIN (KEEPTTL) so the resolver still reads 'admitted' after the
        marker expires. Only the exact-gen origin is stamped: a moved origin
        belongs to another reservation's lifecycle."""
        from tests.unit.server.handlers.chat.redis_fakes import FakeCache

        cache = FakeCache()
        with patch(
            "src.server.services.workflow_tracker.get_cache_client",
            return_value=cache,
        ):
            tracker = WorkflowTracker()

        cache.kv["ptc_origin:t-1"] = {"dispatch_gen": "g-1", "report_back": True}
        cache.client.ttls["ptc_origin:t-1"] = 86400
        marked = await tracker.mark_active(
            "t-1",
            "ws-1",
            "u-1",
            run_id="G1",
            metadata={"origin_dispatch_gen": "g-1"},
            refuse_receipt_key="ptc_rb_resolved:t-1",
            receipt_member="g-1",
        )
        assert marked is True
        assert cache.kv["ptc_origin:t-1"]["admitted_gen"] == "g-1"
        # KEEPTTL: the origin's remaining lifetime is untouched.
        assert cache.client.ttls["ptc_origin:t-1"] == 86400

        # Moved origin: admission still succeeds, stamp skipped.
        cache.kv["ptc_origin:t-2"] = {"dispatch_gen": "g-OTHER"}
        marked = await tracker.mark_active(
            "t-2",
            "ws-1",
            "u-1",
            run_id="G2",
            refuse_receipt_key="ptc_rb_resolved:t-2",
            receipt_member="g-2",
        )
        assert marked is True
        assert "admitted_gen" not in cache.kv["ptc_origin:t-2"]


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

    @pytest.mark.asyncio
    @pytest.mark.parametrize("status", sorted(RECONNECTABLE_STATUSES))
    async def test_get_workflow_status_reconnectable(self, status):
        # Reconnectable statuses must surface ``can_reconnect=True`` so the
        # frontend retries the SSE stream. Pins the actual decision. The wire
        # status is the public vocabulary (1.6).
        from src.server.services.status_vocabulary import to_public

        result = await _call_get_workflow_status(status, bg_status="active")
        assert result["can_reconnect"] is True
        assert result["status"] == to_public(status)

    @pytest.mark.asyncio
    @pytest.mark.parametrize("status", sorted(TERMINAL_STATUSES))
    async def test_get_workflow_status_terminal_blocks_reconnect(self, status):
        # Terminal statuses must surface ``can_reconnect=False`` so the
        # frontend stops attempting to reattach.
        result = await _call_get_workflow_status(status, bg_status="completed")
        assert result["can_reconnect"] is False
        assert result["status"] == status


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
        # v4: only the status key is deleted (the cancel flag key is gone —
        # cancel intent is durable on the run row, not a Redis flag).
        mock_cache.delete.assert_awaited_once_with(f"workflow:status:{thread_id}")

    @pytest.mark.asyncio
    async def test_delete_status_disabled(self):
        tracker, _ = _make_tracker(enabled=False)
        result = await tracker.delete_status("t-1")
        assert result is False


# ---------------------------------------------------------------------------
# WorkflowStatus enum
# ---------------------------------------------------------------------------

class TestWorkflowStatusEnum:
    """Test WorkflowStatus enum values."""

    def test_enum_values(self):
        assert WorkflowStatus.ACTIVE == "active"
        assert WorkflowStatus.COMPLETED == "completed"
        assert WorkflowStatus.INTERRUPTED == "interrupted"
        assert WorkflowStatus.CANCELLED == "cancelled"
        assert WorkflowStatus.UNKNOWN == "unknown"

    def test_enum_is_str(self):
        assert isinstance(WorkflowStatus.ACTIVE, str)


# ---------------------------------------------------------------------------
# get_statuses — batched MGET behind the liveness endpoint
# ---------------------------------------------------------------------------

class TestGetStatuses:
    def teardown_method(self):
        WorkflowTracker._instance = None

    @pytest.mark.asyncio
    async def test_batches_one_mget_and_decodes_found_keys(self):
        tracker, mock_cache = _make_tracker(enabled=True)
        mock_cache.client.mget = AsyncMock(return_value=[
            json.dumps({"status": "active", "run_id": "r-1", "user_id": "u-1"}),
            None,          # missing key -> omitted
            "not-json",    # undecodable -> omitted
        ])
        out = await tracker.get_statuses(["t-1", "t-2", "t-3"])
        assert out == {"t-1": {"status": "active", "run_id": "r-1", "user_id": "u-1"}}
        mock_cache.client.mget.assert_awaited_once_with(
            ["workflow:status:t-1", "workflow:status:t-2", "workflow:status:t-3"]
        )

    @pytest.mark.asyncio
    async def test_empty_ids_returns_empty_without_mget(self):
        tracker, mock_cache = _make_tracker(enabled=True)
        mock_cache.client.mget = AsyncMock()
        assert await tracker.get_statuses([]) == {}
        mock_cache.client.mget.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_disabled_returns_empty(self):
        tracker, _ = _make_tracker(enabled=False)
        assert await tracker.get_statuses(["t-1"]) == {}
