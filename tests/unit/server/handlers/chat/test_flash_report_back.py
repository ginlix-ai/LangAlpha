"""Flash report-back serialization path (N-concurrent dispatch).

``_flash_report_back`` enqueues a completed PTC onto the durable per-flash FIFO;
``_rb_consumer_loop`` POSTs one report-back turn at a time, awaiting each
terminal (``clear_flash_report_back``) before advancing.
"""

from __future__ import annotations

import asyncio
from unittest.mock import patch

import pytest

from src.server.handlers.chat import report_back
from tests.unit.server.handlers.chat.redis_fakes import (
    FakeCache as _FakeCache,
    drain as _drain,
    origin as _origin,
    seed_dispatched as _seed_dispatched,
)


# ---------------------------------------------------------------------------
# clear_flash_report_back — full per-pair teardown + consumer wake
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_clear_tears_down_all_per_pair_state_and_sets_event():
    cache = _FakeCache()
    flash, ptc = "flash-1", "ptc-1"
    _seed_dispatched(cache, flash, [ptc])
    await cache.client.rpush(report_back.flash_rb_queue_key(flash), ptc)
    await cache.client.sadd(report_back.flash_rb_queued_key(flash), ptc)
    cache.kv[report_back.flash_rb_run_key(flash, ptc)] = {"run_id": "rb-1"}
    event = asyncio.Event()
    report_back._rb_terminal_events[(flash, ptc)] = event

    await report_back.clear_flash_report_back(cache, ptc, flash)

    assert f"ptc_origin:{ptc}" not in cache.kv
    assert report_back.flash_rb_run_key(flash, ptc) not in cache.kv
    assert ptc not in cache.client.sets.get(report_back.flash_watch_key(flash), set())
    assert ptc not in cache.client.sets.get("flash_user_pending:u-1", set())
    assert ptc not in cache.client.lists.get(report_back.flash_rb_queue_key(flash), [])
    assert ptc not in cache.client.sets.get(report_back.flash_rb_queued_key(flash), set())
    assert event.is_set()  # consumer waiting on this pair is woken


@pytest.mark.asyncio
async def test_clear_without_flash_thread_id_only_deletes_origin():
    cache = _FakeCache()
    cache.kv["ptc_origin:ptc-1"] = _origin("ptc-1")

    await report_back.clear_flash_report_back(cache, "ptc-1", None)

    assert "ptc_origin:ptc-1" not in cache.kv


@pytest.mark.asyncio
async def test_clear_runs_all_six_mutations_in_one_pipeline():
    """All six teardown mutations ride one transaction; the drained-run record
    follows in its own best-effort pipeline, never interleaved."""
    cache = _FakeCache()
    flash, ptc = "flash-1", "ptc-1"
    _seed_dispatched(cache, flash, [ptc])
    await cache.client.rpush(report_back.flash_rb_queue_key(flash), ptc)
    await cache.client.sadd(report_back.flash_rb_queued_key(flash), ptc)
    cache.kv[report_back.flash_rb_run_key(flash, ptc)] = {"run_id": "rb-1"}

    batches: list[list[str]] = []
    orig_pipeline = cache.client.pipeline

    def _recording_pipeline(transaction: bool = True):
        pipe = orig_pipeline(transaction=transaction)
        orig_execute = pipe.execute

        async def _execute():
            batches.append([op[0] for op in pipe._ops])
            return await orig_execute()

        pipe.execute = _execute
        return pipe

    cache.client.pipeline = _recording_pipeline

    await report_back.clear_flash_report_back(cache, ptc, flash)

    assert batches[0] == ["delete", "delete", "srem", "srem", "lrem", "srem"]
    assert batches[1] == ["lrem", "lpush", "ltrim", "expire"]
    assert len(batches) == 2


@pytest.mark.asyncio
async def test_clear_releases_cap_slot_via_explicit_user_id_when_origin_expired():
    """Origin TTL-expired: an explicit user_id still releases the per-user cap slot."""
    cache = _FakeCache()
    flash, ptc, user = "flash-1", "ptc-1", "u-1"
    cache.client.sets[report_back.flash_watch_key(flash)] = {ptc}
    cache.client.sets[f"flash_user_pending:{user}"] = {ptc}
    # ptc_origin intentionally absent (expired) -> can't be read for the user id.

    await report_back.clear_flash_report_back(cache, ptc, flash, user_id=user)

    assert ptc not in cache.client.sets.get(f"flash_user_pending:{user}", set())
    assert ptc not in cache.client.sets.get(report_back.flash_watch_key(flash), set())


@pytest.mark.asyncio
async def test_clear_warns_when_cap_slot_user_unresolvable():
    """No explicit user_id and no origin -> warn (leak observable) but still tear down."""
    cache = _FakeCache()
    flash, ptc = "flash-1", "ptc-1"
    cache.client.sets[report_back.flash_watch_key(flash)] = {ptc}

    with patch.object(report_back.logger, "warning") as warn:
        await report_back.clear_flash_report_back(cache, ptc, flash)

    assert warn.called
    assert ptc not in cache.client.sets.get(report_back.flash_watch_key(flash), set())


# ---------------------------------------------------------------------------
# _flash_report_back — enqueue-only + dedup + gating
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_enqueue_appends_once_and_dedups_duplicate_completion():
    cache = _FakeCache()
    flash, ptc = "flash-1", "ptc-1"
    _seed_dispatched(cache, flash, [ptc])

    with patch("src.utils.cache.redis_cache.get_cache_client", return_value=cache), patch.object(
        report_back, "ensure_rb_consumer"
    ) as ensure:
        await report_back._flash_report_back(ptc)
        await report_back._flash_report_back(ptc)  # at-least-once duplicate

    assert cache.client.lists[report_back.flash_rb_queue_key(flash)] == [ptc]
    # The duplicate completion returns at the dedup gate, before re-nudging.
    assert ensure.call_count == 1


@pytest.mark.asyncio
async def test_enqueue_skips_non_member_and_non_report_back():
    cache = _FakeCache()
    flash = "flash-1"
    # origin present but PTC was never a watch member (cap rollback / already cleared)
    cache.kv["ptc_origin:ptc-gone"] = _origin("ptc-gone", flash)
    # origin present, report_back disabled
    cache.kv["ptc_origin:ptc-noflag"] = _origin("ptc-noflag", flash)
    cache.kv["ptc_origin:ptc-noflag"]["report_back"] = False
    cache.client.sets[report_back.flash_watch_key(flash)] = {"ptc-noflag"}

    with patch("src.utils.cache.redis_cache.get_cache_client", return_value=cache), patch.object(
        report_back, "ensure_rb_consumer"
    ) as ensure:
        await report_back._flash_report_back("ptc-gone")
        await report_back._flash_report_back("ptc-noflag")

    assert report_back.flash_rb_queue_key(flash) not in cache.client.lists
    ensure.assert_not_called()


@pytest.mark.asyncio
async def test_enqueue_is_atomic_marker_and_queue_entry_together():
    """The atomic EVAL never sets the dedup marker without the FIFO entry (and
    a rejected enqueue leaves no orphan marker that would wedge a retry)."""
    cache = _FakeCache()
    flash, ptc = "flash-1", "ptc-1"
    _seed_dispatched(cache, flash, [ptc])

    with patch("src.utils.cache.redis_cache.get_cache_client", return_value=cache), patch.object(
        report_back, "ensure_rb_consumer"
    ):
        await report_back._flash_report_back(ptc)

    # Marker SET and FIFO entry both present — never one without the other.
    assert ptc in cache.client.sets[report_back.flash_rb_queued_key(flash)]
    assert cache.client.lists[report_back.flash_rb_queue_key(flash)] == [ptc]

    # A non-member leaves NO marker behind (no orphan that blocks a future enqueue).
    with patch("src.utils.cache.redis_cache.get_cache_client", return_value=cache), patch.object(
        report_back, "ensure_rb_consumer"
    ):
        cache.kv["ptc_origin:ptc-2"] = _origin("ptc-2", flash)  # origin but not a watch member
        await report_back._flash_report_back("ptc-2")

    assert "ptc-2" not in cache.client.sets.get(report_back.flash_rb_queued_key(flash), set())


# ---------------------------------------------------------------------------
# Consumer — ordered, non-overlapping drain
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_consumer_drains_in_completion_order_at_n():
    cache = _FakeCache()
    flash = "flash-1"
    ptcs = ["ptc-a", "ptc-b", "ptc-c", "ptc-d"]
    _seed_dispatched(cache, flash, ptcs)
    order: list[str] = []

    async def fake_post(c, f, ptc, origin):
        order.append(ptc)
        await report_back.clear_flash_report_back(c, ptc, f)  # terminal "immediately"
        return "dispatched", f"run-{ptc}"

    with patch("src.utils.cache.redis_cache.get_cache_client", return_value=cache), patch.object(
        report_back, "_post_report_back", side_effect=fake_post
    ):
        # completion order deliberately != lexical order
        for ptc in ["ptc-c", "ptc-a", "ptc-d", "ptc-b"]:
            await report_back._flash_report_back(ptc)
        await _drain(flash)

    assert order == ["ptc-c", "ptc-a", "ptc-d", "ptc-b"]
    assert not cache.client.sets.get(report_back.flash_watch_key(flash))
    assert not cache.client.lists.get(report_back.flash_rb_queue_key(flash))
    assert not cache.client.sets.get(report_back.flash_rb_queued_key(flash))


@pytest.mark.asyncio
async def test_consumer_skips_stale_head_whose_membership_is_gone():
    cache = _FakeCache()
    flash = "flash-1"
    _seed_dispatched(cache, flash, ["ptc-2"])  # only ptc-2 is a live member
    order: list[str] = []

    # ptc-1 sits at the queue head but its membership was already cleared.
    await cache.client.rpush(report_back.flash_rb_queue_key(flash), "ptc-1")
    await cache.client.sadd(report_back.flash_rb_queued_key(flash), "ptc-1")

    async def fake_post(c, f, ptc, origin):
        order.append(ptc)
        await report_back.clear_flash_report_back(c, ptc, f)
        return "dispatched", f"run-{ptc}"

    with patch("src.utils.cache.redis_cache.get_cache_client", return_value=cache), patch.object(
        report_back, "_post_report_back", side_effect=fake_post
    ):
        await report_back._flash_report_back("ptc-2")
        await _drain(flash)

    assert order == ["ptc-2"]  # stale ptc-1 skipped, never POSTed
    assert "ptc-1" not in cache.client.lists.get(report_back.flash_rb_queue_key(flash), [])


@pytest.mark.asyncio
async def test_consumer_drops_member_on_permanent_rejection_and_advances():
    cache = _FakeCache()
    flash = "flash-1"
    _seed_dispatched(cache, flash, ["ptc-1", "ptc-2"])
    order: list[str] = []

    async def fake_post(c, f, ptc, origin):
        order.append(ptc)
        if ptc == "ptc-1":
            return "drop", None  # permanent 4xx — no run created
        await report_back.clear_flash_report_back(c, ptc, f)
        return "dispatched", f"run-{ptc}"

    with patch("src.utils.cache.redis_cache.get_cache_client", return_value=cache), patch.object(
        report_back, "_post_report_back", side_effect=fake_post
    ):
        await report_back._flash_report_back("ptc-1")
        await report_back._flash_report_back("ptc-2")
        await _drain(flash)

    assert order == ["ptc-1", "ptc-2"]  # dropped ptc-1 still advanced to ptc-2
    assert not cache.client.sets.get(report_back.flash_watch_key(flash))
    assert not cache.client.lists.get(report_back.flash_rb_queue_key(flash))


@pytest.mark.asyncio
async def test_consumer_bounds_terminal_wait_and_force_clears_stuck_member(monkeypatch):
    """A report-back whose run never reaches terminal must not wedge the queue."""
    monkeypatch.setattr(report_back, "_RB_TERMINAL_WAIT_CAP", 0.0)
    cache = _FakeCache()
    flash = "flash-1"
    _seed_dispatched(cache, flash, ["ptc-1", "ptc-2"])
    order: list[str] = []

    async def fake_post(c, f, ptc, origin):
        order.append(ptc)
        return "dispatched", f"run-{ptc}"  # NO terminal ever fires

    with patch("src.utils.cache.redis_cache.get_cache_client", return_value=cache), patch.object(
        report_back, "_post_report_back", side_effect=fake_post
    ):
        await report_back._flash_report_back("ptc-1")
        await report_back._flash_report_back("ptc-2")
        await _drain(flash)

    # Deadline force-cleared each stuck member, so the whole queue drained.
    assert order == ["ptc-1", "ptc-2"]
    assert not cache.client.sets.get(report_back.flash_watch_key(flash))
    assert not cache.client.lists.get(report_back.flash_rb_queue_key(flash))
    assert not cache.client.sets.get(report_back.flash_rb_queued_key(flash))


@pytest.mark.asyncio
async def test_consumer_discards_whole_thread_on_404():
    cache = _FakeCache()
    flash = "flash-1"
    _seed_dispatched(cache, flash, ["ptc-1", "ptc-2", "ptc-3"])
    order: list[str] = []

    async def fake_post(c, f, ptc, origin):
        order.append(ptc)
        return "deleted", None  # flash thread gone

    with patch("src.utils.cache.redis_cache.get_cache_client", return_value=cache), patch.object(
        report_back, "_post_report_back", side_effect=fake_post
    ):
        await report_back._flash_report_back("ptc-1")
        await report_back._flash_report_back("ptc-2")
        await report_back._flash_report_back("ptc-3")
        await _drain(flash)

    assert order == ["ptc-1"]  # first 404 discards the rest without POSTing
    assert not cache.client.sets.get(report_back.flash_watch_key(flash))
    assert report_back.flash_rb_queue_key(flash) not in cache.client.lists
    assert report_back.flash_rb_queued_key(flash) not in cache.client.lists
    for ptc in ["ptc-1", "ptc-2", "ptc-3"]:
        assert f"ptc_origin:{ptc}" not in cache.kv


@pytest.mark.asyncio
async def test_consumer_resumes_existing_run_without_reposting():
    """A restarted consumer resumes a head with a live run pointer instead of
    POSTing a second summary turn at double cost."""
    cache = _FakeCache()
    flash, ptc = "flash-1", "ptc-1"
    _seed_dispatched(cache, flash, [ptc])
    # A prior drain dispatched this report-back before the consumer crashed; its
    # run pointer (and the un-LREM'd queue head) survive the restart.
    cache.kv[report_back.flash_rb_run_key(flash, ptc)] = {"run_id": "prior-run"}

    post_calls = 0

    async def fake_post(c, f, p, origin):
        nonlocal post_calls
        post_calls += 1
        return "dispatched", "new-run"

    with patch("src.utils.cache.redis_cache.get_cache_client", return_value=cache), patch.object(
        report_back, "_post_report_back", side_effect=fake_post
    ):
        await report_back._flash_report_back(ptc)  # enqueue + start consumer
        for _ in range(20):
            await asyncio.sleep(0)
        # Resumed on the existing run: parked on its terminal, never re-POSTed.
        assert post_calls == 0
        # The prior run finally completes -> terminal clear wakes the resumed wait.
        await report_back.clear_flash_report_back(cache, ptc, flash)
        await _drain(flash)

    assert post_calls == 0  # never re-dispatched, even after restart
    assert not cache.client.sets.get(report_back.flash_watch_key(flash))
    assert not cache.client.lists.get(report_back.flash_rb_queue_key(flash))
    assert report_back.flash_rb_run_key(flash, ptc) not in cache.kv


@pytest.mark.asyncio
async def test_reassert_skipped_when_membership_already_cleared():
    """A fast terminal during the POST must not have its deleted run pointer
    resurrected by the post-dispatch re-assert."""
    cache = _FakeCache()
    flash, ptc = "flash-1", "ptc-1"
    _seed_dispatched(cache, flash, [ptc])

    async def fake_post(c, f, p, origin):
        # Terminal fires during the POST: clears the run pointer AND the watch
        # membership before we return "dispatched".
        await report_back.clear_flash_report_back(c, p, f)
        return "dispatched", "rb-run"

    with patch("src.utils.cache.redis_cache.get_cache_client", return_value=cache), patch.object(
        report_back, "_post_report_back", side_effect=fake_post
    ):
        await report_back._flash_report_back(ptc)
        await _drain(flash)

    # Membership was gone at re-assert time, so the deleted pointer stays deleted.
    assert report_back.flash_rb_run_key(flash, ptc) not in cache.kv
    assert not cache.client.sets.get(report_back.flash_watch_key(flash))
    assert not cache.client.lists.get(report_back.flash_rb_queue_key(flash))


# ---------------------------------------------------------------------------
# Consumption clear (1.7): the completed report-back flash run enqueues a
# watch_clear outbox job gated on report_back_ptc_thread_id — no in-process
# completion hook remains.
# ---------------------------------------------------------------------------


def test_completed_flash_with_report_back_id_enqueues_consumption_clear():
    from src.server.database.hook_outbox import build_finalize_jobs

    jobs = build_finalize_jobs(
        run_id="run-1",
        thread_id="flash-1",
        msg_type="flash",
        user_id="u-1",
        report_back_ptc_thread_id="ptc-1",
    )("completed")

    clears = [j for j in jobs if j.hook_type == "watch_clear"]
    assert len(clears) == 1
    assert clears[0].payload["ptc_thread_id"] == "ptc-1"
    assert clears[0].payload["error_wake"] is False  # consumption, not failure
    assert clears[0].ordering_key == "ptc-1"


def test_completed_flash_without_report_back_id_skips_clear():
    from src.server.database.hook_outbox import build_finalize_jobs

    jobs = build_finalize_jobs(
        run_id="run-1",
        thread_id="flash-1",
        msg_type="flash",
        user_id="u-1",
    )("completed")

    assert not [j for j in jobs if j.hook_type == "watch_clear"]
