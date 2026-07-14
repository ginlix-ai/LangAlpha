"""Crash-path cleanup for `_consume_background_gen`.

When a dispatched background generator raises, the except branch tears down the
report-back watch keyed by the *PTC* thread id. Regression: the FLASH_DISPATCH
site (a report-back run) used the flash thread id as the origin key, so a
report-back run that crashed before its terminal handler fired left the durable
watch/pointer alive until TTL and `/status` kept reporting a stale pending run.
"""

from unittest.mock import AsyncMock, patch

import pytest

from src.server.app.threads import _consume_background_gen


async def _crashing_gen():
    raise RuntimeError("kaboom")
    yield  # unreachable — marks this as an async generator


class _FakeClient:
    def __init__(self):
        self.publish = AsyncMock()
        self.xadd = AsyncMock()
        self.xrevrange = AsyncMock(return_value=[])


class _FakeCache:
    def __init__(self, origin_map):
        self.enabled = True
        self.client = _FakeClient()
        self._origin = origin_map

    async def get(self, key):
        return self._origin.get(key)


def _patched(cache, clear):
    from src.server.database import turn_lifecycle as tl_db

    return (
        patch(
            "src.utils.cache.redis_cache.get_cache_client", return_value=cache
        ),
        patch(
            "src.server.handlers.chat.report_back.clear_flash_report_back", clear
        ),
        # Default: no ledger row — watch-clear tests stay hermetic (no DB).
        # The ledger-focused tests below re-patch get_run inside this scope.
        patch.object(tl_db, "get_run", AsyncMock(return_value=None)),
    )


@pytest.mark.asyncio
async def test_report_back_crash_clears_watch_via_ptc_thread_id():
    # report-back run: thread_id is the flash thread, but the origin lives under
    # the completed PTC thread named by report_back_ptc_thread_id.
    cache = _FakeCache({"ptc_origin:ptc-1": {"flash_thread_id": "flash-1"}})
    clear = AsyncMock()
    p1, p2, p3 = _patched(cache, clear)
    with p1, p2, p3:
        ok = await _consume_background_gen(
            _crashing_gen(),
            "FLASH_DISPATCH",
            "flash-1",
            "run-1",
            report_back_ptc_thread_id="ptc-1",
            user_id="user-1",
        )
    assert ok is False
    # The known owner is threaded through so the per-user cap slot is released
    # even when ptc_origin carries no user_id (would TTL-leak otherwise).
    clear.assert_awaited_once_with(
        cache, "ptc-1", "flash-1", user_id="user-1", expected_gen=None,
        refuse_if_pointer=True,
    )
    cache.client.publish.assert_awaited_once()
    assert cache.client.publish.call_args[0][0] == "thread:wake:flash-1"


@pytest.mark.asyncio
async def test_ordinary_flash_dispatch_crash_preserves_watch():
    # No report_back id: the origin lookup uses the flash thread id, misses, and
    # leaves a still-running dispatched PTC's keys intact for reload recovery.
    cache = _FakeCache({})  # ptc_origin:flash-1 absent
    clear = AsyncMock()
    p1, p2, p3 = _patched(cache, clear)
    with p1, p2, p3:
        ok = await _consume_background_gen(
            _crashing_gen(), "FLASH_DISPATCH", "flash-1", "run-1"
        )
    assert ok is False
    clear.assert_not_awaited()
    cache.client.publish.assert_not_awaited()


@pytest.mark.asyncio
async def test_ptc_dispatch_crash_clears_via_thread_id():
    # PTC_DISPATCH: thread_id IS the ptc thread, so the default origin key hits.
    cache = _FakeCache({"ptc_origin:ptc-9": {"flash_thread_id": "flash-9"}})
    clear = AsyncMock()
    p1, p2, p3 = _patched(cache, clear)
    with p1, p2, p3:
        ok = await _consume_background_gen(
            _crashing_gen(), "PTC_DISPATCH", "ptc-9", "run-9", user_id="user-9"
        )
    assert ok is False
    clear.assert_awaited_once_with(
        cache, "ptc-9", "flash-9", user_id="user-9", expected_gen=None,
        refuse_if_pointer=True,
    )
    assert cache.client.publish.call_args[0][0] == "thread:wake:flash-9"


@pytest.mark.asyncio
async def test_crash_clear_uses_the_requests_in_band_dispatch_gen():
    """The crashed request's own origin_dispatch_gen scopes the clear — no DB
    read needed, and a run-row read failure can never widen the clear to an
    incarnation the request didn't dispatch."""
    cache = _FakeCache({"ptc_origin:ptc-9": {"flash_thread_id": "flash-9"}})
    clear = AsyncMock()
    p1, p2, p3 = _patched(cache, clear)
    with p1, p2, p3:
        ok = await _consume_background_gen(
            _crashing_gen(), "PTC_DISPATCH", "ptc-9", "run-9",
            user_id="user-9", dispatch_gen="g-req",
        )
    assert ok is False
    clear.assert_awaited_once_with(
        cache, "ptc-9", "flash-9", user_id="user-9", expected_gen="g-req",
        refuse_if_pointer=True,
    )


@pytest.mark.asyncio
async def test_crash_with_a_ledger_row_skips_the_direct_watch_clear():
    """Round-19 P1: a run row means a finalize — the reconcile below, or the
    real owner's — enqueues the durable watch_clear on the flash ordering
    chain, which owns the pair teardown there. The direct clear runs
    OFF-CHAIN and can drain a summary admission's just-claimed pointer
    mid-flight, so it is reserved for ROWLESS crashes."""
    from src.server.database.turn_lifecycle import FinalizeResult

    cache = _FakeCache({"ptc_origin:ptc-1": {"flash_thread_id": "flash-1"}})
    clear = AsyncMock()
    p1, p2, _default_rowless = _patched(cache, clear)
    p3, p4 = _ledger_patches(
        _row("in_progress"),
        FinalizeResult(applied=False, run={"status": "error"}),
    )
    with p1, p2, p3, p4:
        ok = await _consume_background_gen(
            _crashing_gen(), "PTC_DISPATCH", "ptc-1", "run-1", user_id="u-1"
        )
    assert ok is False
    clear.assert_not_awaited()


@pytest.mark.asyncio
async def test_crash_with_unreadable_ledger_skips_the_direct_watch_clear():
    """Row state unknown -> leave the pair to its owner or the origin TTL;
    never tear down off-chain on a guess."""
    from src.server.database import turn_lifecycle as tl_db

    cache = _FakeCache({"ptc_origin:ptc-1": {"flash_thread_id": "flash-1"}})
    clear = AsyncMock()
    p1, p2, _default_rowless = _patched(cache, clear)
    with p1, p2, patch.object(
        tl_db, "get_run", AsyncMock(side_effect=ConnectionError("db down"))
    ):
        ok = await _consume_background_gen(
            _crashing_gen(), "PTC_DISPATCH", "ptc-1", "run-1", user_id="u-1"
        )
    assert ok is False
    clear.assert_not_awaited()


# --- last-resort finalize + verified run_end (I6) ---------------------------
#
# The dispatch-failure path may only emit run_end with a VERIFIED terminal
# status: it settles an orphaned in_progress row itself (one CAS, adopting
# durable cancel intent), reads an already-terminal row's real status, and
# emits NOTHING when the stream already closed or the run actually succeeded.


def _row(status):
    """A minimally-real ledger row: the fallback finalize builds its hook
    factory from these fields (build_finalize_jobs_from_run_row)."""
    return {
        "conversation_response_id": "run-1",
        "conversation_thread_id": "ptc-1",
        "status": status,
        "metadata": {"msg_type": "ptc", "user_id": "u-1"},
    }


def _ledger_patches(run_row, finalize_result=None):
    from src.server.database import turn_lifecycle as tl_db

    return (
        patch.object(tl_db, "get_run", AsyncMock(return_value=run_row)),
        patch.object(
            tl_db,
            "finalize_run_idempotent",
            AsyncMock(return_value=finalize_result),
        ),
    )


async def _run_crash(cache, run_row, finalize_result=None):
    from src.server.services.background_task_manager import BackgroundTaskManager

    order = []
    cache.client.xadd.side_effect = lambda *a, **kw: order.append("error_xadd")
    fake_manager = AsyncMock()
    fake_manager.is_run_live = AsyncMock(return_value=False)
    fake_manager.append_run_end_event.side_effect = (
        lambda *a, **kw: order.append("run_end")
    )
    clear = AsyncMock()
    p1, p2, p0 = _patched(cache, clear)
    p3, p4 = _ledger_patches(run_row, finalize_result)
    with p1, p2, p0, p3, p4, patch.object(
        BackgroundTaskManager,
        "get_instance",
        classmethod(lambda cls: fake_manager),
    ):
        ok = await _consume_background_gen(
            _crashing_gen(), "PTC_DISPATCH", "ptc-1", "run-1"
        )
    assert ok is False
    return order, fake_manager


@pytest.mark.asyncio
async def test_orphaned_in_progress_row_is_finalized_then_run_end():
    """An in_progress row with a dead generator gets the last-resort CAS;
    run_end carries the CAS-adopted status and follows the error frame."""
    from src.server.database.turn_lifecycle import FinalizeResult

    cache = _FakeCache({})
    order, manager = await _run_crash(
        cache,
        run_row=_row("in_progress"),
        finalize_result=FinalizeResult(applied=True, run={"status": "error"}),
    )
    assert order == ["error_xadd", "run_end"]
    manager.append_run_end_event.assert_awaited_once_with("ptc-1", "run-1", "error")


@pytest.mark.asyncio
async def test_durable_cancel_intent_adopts_cancelled_outcome():
    """An adopted cancel is not a failure to the client: run_end(cancelled)
    closes the stream, but no background_failure error frame appears."""
    from src.server.database.turn_lifecycle import FinalizeResult

    cache = _FakeCache({})
    order, manager = await _run_crash(
        cache,
        run_row=_row("in_progress"),
        finalize_result=FinalizeResult(applied=True, run={"status": "cancelled"}),
    )
    assert order == ["run_end"]
    manager.append_run_end_event.assert_awaited_once_with(
        "ptc-1", "run-1", "cancelled"
    )


@pytest.mark.asyncio
async def test_lost_finalize_cas_emits_nothing():
    """applied=False means the real owner finalized concurrently — it owns
    the terminal transport; a second error frame / run_end here would
    double-close the stream."""
    from src.server.database.turn_lifecycle import FinalizeResult

    cache = _FakeCache({})
    order, manager = await _run_crash(
        cache,
        run_row=_row("in_progress"),
        finalize_result=FinalizeResult(applied=False, run={"status": "error"}),
    )
    assert order == []
    manager.append_run_end_event.assert_not_awaited()


@pytest.mark.asyncio
async def test_no_ledger_row_emits_error_frame_without_run_end():
    """Pre-START crash: nothing durable exists, so no terminal may be claimed."""
    cache = _FakeCache({})
    order, manager = await _run_crash(cache, run_row=None)
    assert order == ["error_xadd"]
    manager.append_run_end_event.assert_not_awaited()


@pytest.mark.asyncio
async def test_completed_row_emits_nothing():
    """A run that really finished must not gain a misleading error frame."""
    cache = _FakeCache({})
    order, manager = await _run_crash(cache, run_row=_row("completed"))
    assert order == []
    manager.append_run_end_event.assert_not_awaited()


@pytest.mark.asyncio
async def test_terminal_error_row_gets_error_frame_and_its_real_status():
    """In-generator finalize (row already error, no run_end on the stream):
    the consumer path completes the transport close with the ROW's status."""
    cache = _FakeCache({})
    order, manager = await _run_crash(cache, run_row=_row("error"))
    assert order == ["error_xadd", "run_end"]
    manager.append_run_end_event.assert_awaited_once_with("ptc-1", "run-1", "error")


@pytest.mark.asyncio
async def test_stream_already_closed_with_run_end_emits_nothing():
    cache = _FakeCache({})
    cache.client.xrevrange = AsyncMock(
        return_value=[("1-0", {b"event": b"event: run_end\ndata: {}\n\n"})]
    )
    order, manager = await _run_crash(cache, run_row=_row("error"))
    assert order == []
    manager.append_run_end_event.assert_not_awaited()


@pytest.mark.asyncio
async def test_live_btm_executor_blocks_last_resort_finalize():
    """A dead tail must never steal the run from its live executor: with a
    not-done BTM task for this exact run, the fallback performs NO finalize,
    NO frames, and NO report-back teardown — the owner settles everything."""
    import asyncio

    from src.server.database import turn_lifecycle as tl_db
    from src.server.services.background_task_manager import BackgroundTaskManager

    class _FakeTask:
        def done(self):
            return False

    class _FakeManager:
        def __init__(self):
            self.task_lock = asyncio.Lock()
            info = AsyncMock()
            info.task = _FakeTask()
            info.inner_task = None
            info.status = None
            self.tasks = {("ptc-1", "run-1"): info}
            self.append_run_end_event = AsyncMock()

        # The real probe, run against the fake's tasks dict — pins the
        # "not-done task => live => hands off" semantics, not a stub's.
        is_run_live = BackgroundTaskManager.is_run_live

    fake_manager = _FakeManager()
    cache = _FakeCache({"ptc_origin:ptc-1": {"flash_thread_id": "flash-1"}})
    finalize = AsyncMock()
    clear = AsyncMock()
    p1, p2, p0 = _patched(cache, clear)
    with p1, p2, p0, patch.object(
        tl_db, "finalize_run_idempotent", finalize
    ), patch.object(
        BackgroundTaskManager,
        "get_instance",
        classmethod(lambda cls: fake_manager),
    ):
        ok = await _consume_background_gen(
            _crashing_gen(), "PTC_DISPATCH", "ptc-1", "run-1", user_id="user-1"
        )

    assert ok is False
    finalize.assert_not_awaited()
    cache.client.xadd.assert_not_awaited()
    fake_manager.append_run_end_event.assert_not_awaited()
    clear.assert_not_awaited()  # live dispatched run's watch keys stay in use


@pytest.mark.asyncio
async def test_failed_last_resort_finalize_withholds_run_end():
    """If the CAS itself fails the row stays in_progress — an error frame may
    inform, but run_end (a terminal claim) must not appear (I6)."""
    from src.server.database import turn_lifecycle as tl_db
    from src.server.services.background_task_manager import BackgroundTaskManager

    cache = _FakeCache({})
    order = []
    cache.client.xadd.side_effect = lambda *a, **kw: order.append("error_xadd")
    fake_manager = AsyncMock()
    fake_manager.is_run_live = AsyncMock(return_value=False)
    fake_manager.append_run_end_event.side_effect = (
        lambda *a, **kw: order.append("run_end")
    )
    clear = AsyncMock()
    p1, p2, p0 = _patched(cache, clear)
    with p1, p2, p0, patch.object(
        tl_db, "get_run", AsyncMock(return_value=_row("in_progress"))
    ), patch.object(
        tl_db,
        "finalize_run_idempotent",
        AsyncMock(side_effect=RuntimeError("db down")),
    ), patch.object(
        BackgroundTaskManager,
        "get_instance",
        classmethod(lambda cls: fake_manager),
    ):
        ok = await _consume_background_gen(
            _crashing_gen(), "PTC_DISPATCH", "ptc-1", "run-1"
        )

    assert ok is False
    assert order == ["error_xadd"]
    fake_manager.append_run_end_event.assert_not_awaited()
