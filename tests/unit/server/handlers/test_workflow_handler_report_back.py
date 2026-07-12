"""Coverage for ``get_workflow_status``'s flash report-back resolution.

A flash thread polling ``/status`` must surface which report-back run to attach
to (``report_back_run_id``) and re-nudge the in-process consumer after a process
restart. The run is resolved from a live per-(flash, ptc) pointer — preferring
the head of the durable queue (the report-back currently being drained), then
any pending watch member with a pointer.
"""

from __future__ import annotations

import contextlib
import json

import pytest
from unittest.mock import AsyncMock, MagicMock, patch

from src.server.handlers import workflow_handler
from src.server.handlers.chat import report_back
from tests.unit.server.handlers.chat.redis_fakes import FakeCache as _FakeCache


def _seed(cache: _FakeCache, flash: str, queue: list[str], run_pointers: dict[str, str]) -> None:
    cache.client.sets[report_back.flash_watch_key(flash)] = set(queue)
    cache.client.lists[report_back.flash_rb_queue_key(flash)] = list(queue)
    # Pointers are read via client.mget (raw serialized JSON), matching prod.
    for ptc, run_id in run_pointers.items():
        cache.client.kv[report_back.flash_rb_run_key(flash, ptc)] = json.dumps(
            {"run_id": run_id}
        )


def _patches(
    cache: _FakeCache, ensure: MagicMock, latest_turn: int | None = None
) -> list:
    """Stub everything get_workflow_status touches except the report-back block."""
    tracker = MagicMock()
    # COMPLETED is terminal (not reconnectable) -> can_reconnect False.
    tracker.get_status = AsyncMock(
        return_value={
            "status": "completed",
            "last_update": None,
            "workspace_id": "ws-1",
            "user_id": "u-1",
        }
    )
    manager = MagicMock()
    # A live bg task skips the stale-clear branch regardless of can_reconnect.
    manager.get_live_task_info = AsyncMock(
        return_value={"live": True, "active_tasks": [], "run_id": "r1"}
    )
    return [
        patch.object(
            workflow_handler, "get_checkpoint_tuple", AsyncMock(return_value=None)
        ),
        patch(
            "src.server.services.workflow_tracker.WorkflowTracker.get_instance",
            return_value=tracker,
        ),
        patch(
            "src.server.services.background_task_manager.BackgroundTaskManager.get_instance",
            return_value=manager,
        ),
        patch(
            "src.server.database.conversation.get_thread_by_id",
            AsyncMock(return_value=None),
        ),
        patch(
            "src.server.database.conversation.get_latest_turn_index",
            AsyncMock(return_value=latest_turn),
        ),
        patch(
            "src.utils.cache.redis_cache.get_cache_client", return_value=cache
        ),
        patch.object(report_back, "ensure_rb_consumer", ensure),
    ]


@pytest.mark.asyncio
async def test_status_prefers_queue_head_run_id_and_nudges_consumer():
    cache = _FakeCache()
    flash = "flash-1"
    # Both head and member carry a pointer; the head's must win.
    _seed(
        cache,
        flash,
        ["ptc-head", "ptc-other"],
        {"ptc-head": "rb-head", "ptc-other": "rb-other"},
    )
    # A previously drained run rides along in the same status payload.
    cache.client.lists[report_back.flash_rb_done_key(flash)] = ["rb-done-1"]
    ensure = MagicMock()

    with contextlib.ExitStack() as stack:
        for p in _patches(cache, ensure):
            stack.enter_context(p)
        resp = await workflow_handler.get_workflow_status(flash)

    assert resp["pending_report_back"] is True
    assert resp["report_back_run_id"] == "rb-head"
    assert resp["recent_report_back_run_ids"] == ["rb-done-1"]
    # Durable queue is non-empty -> restart-nudge the consumer.
    ensure.assert_called_once_with(flash)


@pytest.mark.asyncio
async def test_status_falls_back_to_member_pointer_when_head_has_none():
    cache = _FakeCache()
    flash = "flash-1"
    # Head has no pointer yet; resolution falls through to the member that does.
    _seed(cache, flash, ["ptc-head", "ptc-other"], {"ptc-other": "rb-other"})
    ensure = MagicMock()

    with contextlib.ExitStack() as stack:
        for p in _patches(cache, ensure):
            stack.enter_context(p)
        resp = await workflow_handler.get_workflow_status(flash)

    assert resp["pending_report_back"] is True
    assert resp["report_back_run_id"] == "rb-other"
    ensure.assert_called_once_with(flash)


@pytest.mark.asyncio
async def test_status_no_pending_report_back_does_not_nudge():
    cache = _FakeCache()  # empty watch SET + empty queue
    ensure = MagicMock()

    with contextlib.ExitStack() as stack:
        for p in _patches(cache, ensure):
            stack.enter_context(p)
        resp = await workflow_handler.get_workflow_status("flash-x")

    assert resp["pending_report_back"] is False
    assert resp["report_back_run_id"] is None
    assert resp["recent_report_back_run_ids"] == []
    ensure.assert_not_called()


class _BoomClient:
    """A Redis client whose pipeline read blows up — a transient blip."""

    def pipeline(self, transaction: bool = False):
        raise RuntimeError("redis read failed")


class _BoomCache:
    enabled = True

    def __init__(self) -> None:
        self.client = _BoomClient()


@pytest.mark.asyncio
async def test_report_back_status_redis_error_returns_unknown_not_false():
    """Own-Redis-read failure -> ``None`` ('unknown'), never a false ``False``."""
    with patch(
        "src.utils.cache.redis_cache.get_cache_client", return_value=_BoomCache()
    ):
        resp = await report_back.read_report_back_status("flash-err")

    assert resp["pending_report_back"] is None
    assert resp["pending_report_back"] is not False
    assert resp["report_back_run_id"] is None
    assert resp["recent_report_back_run_ids"] == []  # never omitted, [] on failure


@pytest.mark.asyncio
async def test_report_back_status_success_returns_real_bool():
    """A successful read returns the real ``True``/``False``, not the None sentinel."""
    # Drained: empty watch SET + queue -> explicit False.
    empty = _FakeCache()
    with patch("src.utils.cache.redis_cache.get_cache_client", return_value=empty):
        drained = await report_back.read_report_back_status("flash-empty")
    assert drained["pending_report_back"] is False
    assert drained["report_back_run_id"] is None

    # Pending: a watch member with a live run pointer -> explicit True + run id.
    pending = _FakeCache()
    _seed(pending, "flash-pending", ["ptc-1"], {"ptc-1": "rb-1"})
    with (
        patch("src.utils.cache.redis_cache.get_cache_client", return_value=pending),
        patch.object(report_back, "ensure_rb_consumer", MagicMock()),
    ):
        live = await report_back.read_report_back_status("flash-pending")
    assert live["pending_report_back"] is True
    assert live["report_back_run_id"] == "rb-1"


# --- latest_turn_index (the cached-view terminal-staleness signal) -----------


@pytest.mark.asyncio
async def test_status_includes_latest_turn_index_for_terminal_thread():
    """A terminal thread's /status still carries the persisted-turn watermark.

    can_reconnect is false and there is no reconnectable run to compare, so
    latest_turn_index is the ONLY signal a cached frontend view has that whole
    turns completed while it was hidden.
    """
    cache = _FakeCache()
    ensure = MagicMock()

    with contextlib.ExitStack() as stack:
        for p in _patches(cache, ensure, latest_turn=3):
            stack.enter_context(p)
        resp = await workflow_handler.get_workflow_status("thread-1")

    assert resp["latest_turn_index"] == 3
    # Terminal per the tracker blob — the signal must not depend on liveness.
    assert resp["status"] == "completed"
    assert resp["can_reconnect"] is False


@pytest.mark.asyncio
async def test_status_latest_turn_index_none_when_thread_has_no_turns():
    """No persisted turns (or a failed read) surfaces as an explicit None."""
    cache = _FakeCache()
    ensure = MagicMock()

    with contextlib.ExitStack() as stack:
        for p in _patches(cache, ensure, latest_turn=None):
            stack.enter_context(p)
        resp = await workflow_handler.get_workflow_status("thread-1")

    assert "latest_turn_index" in resp
    assert resp["latest_turn_index"] is None


# --- Liveness read-model (the cheap dispatch-status primitive) ---------------


def test_liveness_from_blob_active_is_reconnectable():
    # Wire status is the PUBLIC vocabulary (1.6): tracker 'active' -> 'running'.
    out = workflow_handler.liveness_from_blob(
        "t-1", {"status": "active", "run_id": "r-1", "user_id": "u-1"}
    )
    assert out == {
        "thread_id": "t-1",
        "status": "running",
        "run_id": "r-1",
        "can_reconnect": True,
    }


def test_liveness_from_blob_completed_is_not_reconnectable():
    out = workflow_handler.liveness_from_blob(
        "t-2", {"status": "completed", "run_id": "r-2"}
    )
    assert out["can_reconnect"] is False
    assert out["run_id"] == "r-2"


def test_liveness_from_blob_missing_blob_is_idle():
    # Tracker's 'unknown' placeholder maps to public 'idle' — the internal
    # spelling never crosses the wire.
    out = workflow_handler.liveness_from_blob("t-3", None)
    assert out["status"] == "idle"
    assert out["run_id"] is None
    assert out["can_reconnect"] is False
