"""Coverage for ``get_workflow_status``'s flash report-back resolution.

A flash thread polling ``/status`` must surface which report-back run to attach
to (``report_back_run_id``), resolved from a live per-(flash, ptc) pointer of
any pending watch member. Execution progress lives in the durable outbox — no
in-process consumer remains to nudge.
"""

from __future__ import annotations

import contextlib
import json

import pytest
from unittest.mock import AsyncMock, MagicMock, patch

from src.server.handlers import workflow_handler
from src.server.handlers.chat import report_back
from tests.unit.server.handlers.chat.redis_fakes import FakeCache as _FakeCache


def _seed(cache: _FakeCache, flash: str, members: list[str], run_pointers: dict[str, str]) -> None:
    cache.client.sets[report_back.flash_watch_key(flash)] = set(members)
    # A live member always carries an origin (reserve writes both atomically);
    # the status read treats originless members as orphans and reaps them.
    for ptc in members:
        cache.client.kv[report_back.ptc_origin_key(ptc)] = json.dumps(
            {"flash_thread_id": flash, "report_back": True}
        )
    # Pointers are read via client.mget (raw serialized JSON), matching prod.
    for ptc, run_id in run_pointers.items():
        cache.client.kv[report_back.flash_rb_run_key(flash, ptc)] = json.dumps(
            {"run_id": run_id}
        )


def _patches(cache: _FakeCache, latest_turn: int | None = None) -> list:
    """Stub everything get_workflow_status touches except the report-back block.

    The ledger speaks status truth (v4 2.4): no active slot, latest attempt
    'completed' — a settled terminal thread (can_reconnect False).
    """
    from src.server.database import turn_lifecycle as tl_db

    manager = MagicMock()
    manager.get_live_task_info = AsyncMock(
        return_value={"live": False, "active_tasks": [], "run_id": None}
    )
    return [
        patch.object(
            workflow_handler, "get_checkpoint_tuple", AsyncMock(return_value=None)
        ),
        patch.object(tl_db, "get_active_run", AsyncMock(return_value=None)),
        patch.object(
            tl_db,
            "get_latest_attempt",
            AsyncMock(
                return_value={
                    "conversation_response_id": "r1",
                    "status": "completed",
                    "created_at": None,
                }
            ),
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
    ]


@pytest.mark.asyncio
async def test_status_surfaces_member_pointer_and_recent_runs():
    cache = _FakeCache()
    flash = "flash-1"
    _seed(cache, flash, ["ptc-1"], {"ptc-1": "rb-1"})
    # A previously drained run rides along in the same status payload.
    cache.client.lists[report_back.flash_rb_done_key(flash)] = ["rb-done-1"]

    with contextlib.ExitStack() as stack:
        for p in _patches(cache):
            stack.enter_context(p)
        resp = await workflow_handler.get_workflow_status(flash)

    assert resp["pending_report_back"] is True
    assert resp["report_back_run_id"] == "rb-1"
    assert resp["recent_report_back_run_ids"] == ["rb-done-1"]


@pytest.mark.asyncio
async def test_status_falls_back_to_any_member_with_a_pointer():
    cache = _FakeCache()
    flash = "flash-1"
    # One member has no pointer yet (not dispatched); the one that does wins.
    _seed(cache, flash, ["ptc-pending", "ptc-live"], {"ptc-live": "rb-live"})

    with contextlib.ExitStack() as stack:
        for p in _patches(cache):
            stack.enter_context(p)
        resp = await workflow_handler.get_workflow_status(flash)

    assert resp["pending_report_back"] is True
    assert resp["report_back_run_id"] == "rb-live"


@pytest.mark.asyncio
async def test_status_no_pending_report_back():
    cache = _FakeCache()  # empty watch SET

    with contextlib.ExitStack() as stack:
        for p in _patches(cache):
            stack.enter_context(p)
        resp = await workflow_handler.get_workflow_status("flash-x")

    assert resp["pending_report_back"] is False
    assert resp["report_back_run_id"] is None
    assert resp["recent_report_back_run_ids"] == []


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
    with patch(
        "src.utils.cache.redis_cache.get_cache_client", return_value=pending
    ):
        live = await report_back.read_report_back_status("flash-pending")
    assert live["pending_report_back"] is True
    assert live["report_back_run_id"] == "rb-1"


# --- Orphan members in the status read (Codex round-6 F1) --------------------


@pytest.mark.asyncio
async def test_status_excludes_and_reaps_originless_members():
    """An under-cap orphan (member whose origin lapsed) must not keep /status
    pending forever — reserve()'s cap-pressure reaper never fires below the
    cap while successful reserves keep refreshing the shared SET's TTL, so
    the read path itself filters and reaps."""
    cache = _FakeCache()
    flash = "flash-1"
    _seed(cache, flash, ["ptc-live", "ptc-orphan"], {"ptc-live": "rb-live"})
    del cache.client.kv[report_back.ptc_origin_key("ptc-orphan")]

    with patch("src.utils.cache.redis_cache.get_cache_client", return_value=cache):
        resp = await report_back.read_report_back_status(flash)

    assert resp["pending_report_back"] is True
    assert resp["report_back_run_id"] == "rb-live"
    # The orphan was reaped from the watch set, not just skipped.
    assert cache.client.sets[report_back.flash_watch_key(flash)] == {"ptc-live"}


@pytest.mark.asyncio
async def test_status_with_only_orphans_reads_drained():
    cache = _FakeCache()
    flash = "flash-1"
    _seed(cache, flash, ["ptc-orphan"], {})
    del cache.client.kv[report_back.ptc_origin_key("ptc-orphan")]

    with patch("src.utils.cache.redis_cache.get_cache_client", return_value=cache):
        resp = await report_back.read_report_back_status(flash)

    assert resp["pending_report_back"] is False
    assert resp["report_back_run_id"] is None
    assert not cache.client.sets.get(report_back.flash_watch_key(flash))


@pytest.mark.asyncio
async def test_status_orphan_reap_failure_still_filters_the_derivation():
    """The reap is best-effort: an eval failure must not degrade the read to
    unknown — the origin MGET already established the truth."""
    cache = _FakeCache()
    flash = "flash-1"
    _seed(cache, flash, ["ptc-orphan"], {})
    del cache.client.kv[report_back.ptc_origin_key("ptc-orphan")]

    async def _boom(*_a, **_k):
        raise RuntimeError("eval failed")

    cache.client.eval = _boom

    with patch("src.utils.cache.redis_cache.get_cache_client", return_value=cache):
        resp = await report_back.read_report_back_status(flash)

    assert resp["pending_report_back"] is False  # filtered, not None/unknown
    # The unreaped member stays for a later reap attempt.
    assert "ptc-orphan" in cache.client.sets[report_back.flash_watch_key(flash)]


# --- latest_turn_index (the cached-view terminal-staleness signal) -----------


@pytest.mark.asyncio
async def test_status_includes_latest_turn_index_for_terminal_thread():
    """A terminal thread's /status still carries the persisted-turn watermark.

    can_reconnect is false and there is no reconnectable run to compare, so
    latest_turn_index is the ONLY signal a cached frontend view has that whole
    turns completed while it was hidden.
    """
    cache = _FakeCache()
    with contextlib.ExitStack() as stack:
        for p in _patches(cache, latest_turn=3):
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
    with contextlib.ExitStack() as stack:
        for p in _patches(cache, latest_turn=None):
            stack.enter_context(p)
        resp = await workflow_handler.get_workflow_status("thread-1")

    assert "latest_turn_index" in resp
    assert resp["latest_turn_index"] is None
