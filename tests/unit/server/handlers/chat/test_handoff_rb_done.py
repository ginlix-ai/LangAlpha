"""Coverage for the recently-drained report-back record (``rb_done_key``).

``clear_handoff_report_back`` deletes the run pointer + watch membership
atomically, so a client that missed the pub/sub wake could never learn a
finished report-back turn's run id. After a successful clear the drained run id
is recorded on a bounded, TTL'd per-thread list that ``read_report_back_status``
surfaces as ``recent_report_back_run_ids`` (newest first) — best-effort, never
a reason to fail the clear, and skipped on the mass discard of a deleted Chief of Staff thread.
"""

from __future__ import annotations

from unittest.mock import patch

import pytest

from src.server.services.report_back.handoff import executor, keys, pointer, status
from tests.unit.server.handlers.chat.redis_fakes import (
    FakeCache as _FakeCache,
    seed_dispatched as _seed_dispatched,
)


def _seed_pointer(cache: _FakeCache, dispatcher: str, ptc: str, run_id: str) -> None:
    cache.kv[keys.rb_run_key(dispatcher, ptc)] = {"run_id": run_id}


# ---------------------------------------------------------------------------
# Recording on clear
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_clear_records_drained_run_newest_first_with_ttl():
    cache = _FakeCache()
    dispatcher = "cos-1"
    _seed_dispatched(cache, dispatcher, ["ptc-1", "ptc-2"])
    _seed_pointer(cache, dispatcher, "ptc-1", "rb-run-1")
    _seed_pointer(cache, dispatcher, "ptc-2", "rb-run-2")

    await pointer.clear_handoff_report_back(cache, "ptc-1", dispatcher)
    await pointer.clear_handoff_report_back(cache, "ptc-2", dispatcher)

    done_key = keys.rb_done_key(dispatcher)
    assert cache.client.lists[done_key] == ["rb-run-2", "rb-run-1"]  # newest first
    assert cache.client.ttls[done_key] == keys.RB_DONE_TTL


@pytest.mark.asyncio
async def test_clear_without_run_pointer_records_nothing():
    """No pointer -> nothing was ever dispatched for this pair; nothing to find."""
    cache = _FakeCache()
    dispatcher, ptc = "cos-1", "ptc-1"
    _seed_dispatched(cache, dispatcher, [ptc])

    await pointer.clear_handoff_report_back(cache, ptc, dispatcher)

    assert not cache.client.lists.get(keys.rb_done_key(dispatcher))
    # The teardown itself still ran.
    assert ptc not in cache.client.sets.get(keys.watch_key(dispatcher), set())
    assert f"ptc_origin:{ptc}" not in cache.kv


@pytest.mark.asyncio
async def test_done_list_bounded_at_max():
    cache = _FakeCache()
    dispatcher = "cos-1"
    count = keys.RB_DONE_MAX + 2
    ptcs = [f"ptc-{i}" for i in range(1, count + 1)]
    _seed_dispatched(cache, dispatcher, ptcs)

    for i, ptc in enumerate(ptcs, 1):
        _seed_pointer(cache, dispatcher, ptc, f"rb-run-{i}")
        await pointer.clear_handoff_report_back(cache, ptc, dispatcher)

    done = cache.client.lists[keys.rb_done_key(dispatcher)]
    assert len(done) == keys.RB_DONE_MAX
    assert done[0] == f"rb-run-{count}"  # newest kept
    assert "rb-run-1" not in done and "rb-run-2" not in done  # oldest trimmed


@pytest.mark.asyncio
async def test_retried_clear_does_not_duplicate_run_id():
    """A re-cleared pair (e.g. the consumer's belt-and-suspenders pointer
    re-assert racing the terminal) LREM-dedups: the run id appears once."""
    cache = _FakeCache()
    dispatcher, ptc = "cos-1", "ptc-1"
    _seed_dispatched(cache, dispatcher, [ptc])
    _seed_pointer(cache, dispatcher, ptc, "rb-run-1")

    await pointer.clear_handoff_report_back(cache, ptc, dispatcher)
    # Retry: the pointer was re-asserted after the first clear.
    _seed_pointer(cache, dispatcher, ptc, "rb-run-1")
    await pointer.clear_handoff_report_back(cache, ptc, dispatcher)

    assert cache.client.lists[keys.rb_done_key(dispatcher)] == ["rb-run-1"]


@pytest.mark.asyncio
async def test_recording_failure_does_not_break_clear():
    """Recording is best-effort: a Redis failure there must not fail the clear."""
    cache = _FakeCache()
    dispatcher, ptc = "cos-1", "ptc-1"
    _seed_dispatched(cache, dispatcher, [ptc])
    _seed_pointer(cache, dispatcher, ptc, "rb-run-1")

    async def _boom(key, value):
        raise RuntimeError("redis down")

    cache.client.lpush = _boom

    await pointer.clear_handoff_report_back(cache, ptc, dispatcher)  # must not raise

    # Teardown fully applied despite the failed record.
    assert ptc not in cache.client.sets.get(keys.watch_key(dispatcher), set())
    assert f"ptc_origin:{ptc}" not in cache.kv
    assert keys.rb_run_key(dispatcher, ptc) not in cache.kv
    assert not cache.client.lists.get(keys.rb_done_key(dispatcher))


@pytest.mark.asyncio
async def test_mass_discard_of_deleted_dispatcher_thread_records_nothing():
    """A 404'd Chief of Staff thread can never render these turns; don't advertise them."""
    cache = _FakeCache()
    dispatcher = "cos-1"
    _seed_dispatched(cache, dispatcher, ["ptc-1", "ptc-2"])
    _seed_pointer(cache, dispatcher, "ptc-1", "rb-run-1")
    _seed_pointer(cache, dispatcher, "ptc-2", "rb-run-2")

    await executor._discard_dispatcher_thread(cache, dispatcher)

    assert not cache.client.lists.get(keys.rb_done_key(dispatcher))
    assert not cache.client.sets.get(keys.watch_key(dispatcher))


# ---------------------------------------------------------------------------
# read_report_back_status surfaces the record
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_status_surfaces_recent_run_ids_newest_first():
    cache = _FakeCache()
    dispatcher = "cos-1"
    cache.client.lists[keys.rb_done_key(dispatcher)] = ["rb-run-2", "rb-run-1"]

    with patch("src.utils.cache.redis_cache.get_cache_client", return_value=cache):
        resp = await status.read_report_back_status(dispatcher, "user-1")

    assert resp["recent_report_back_run_ids"] == ["rb-run-2", "rb-run-1"]
    # Drained thread: no live members, but the recent list is still served.
    assert resp["pending_report_back"] is False
    assert resp["report_back_run_id"] is None


@pytest.mark.asyncio
async def test_status_recent_run_ids_empty_when_key_absent():
    cache = _FakeCache()

    with patch("src.utils.cache.redis_cache.get_cache_client", return_value=cache):
        resp = await status.read_report_back_status("cos-1", "user-1")

    assert resp["recent_report_back_run_ids"] == []
