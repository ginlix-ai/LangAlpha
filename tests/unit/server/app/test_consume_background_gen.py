"""Crash-path cleanup for `_consume_background_gen`.

When a dispatched background generator raises, the crash branch hands the
orphaned run to ``TurnCoordinator.reconcile_orphaned_dispatch`` — every
generator is primed past START (2.4c), so a ledger row always exists and the
reconcile (or the real owner's finalize) enqueues any watch_clear on the
flash ordering chain. The consumer itself never touches report-back state:
an off-chain clear can drain a summary admission's just-claimed pointer
mid-flight (round-19 P1).
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
        # Default: no ledger row — keeps tests hermetic (no DB). The
        # ledger-focused tests below re-patch get_run inside this scope.
        patch.object(tl_db, "get_run", AsyncMock(return_value=None)),
    )


@pytest.mark.asyncio
async def test_crash_never_clears_report_back_off_chain():
    """Round-19 P1, 2.4d form: the crash path performs NO direct report-back
    teardown — regardless of ledger state, pair teardown belongs to the
    durable watch_clear on the flash ordering chain (enqueued by reconcile
    or the owner's finalize). An off-chain clear here can drain a summary
    admission's just-claimed pointer mid-flight."""
    from src.server.database import turn_lifecycle as tl_db
    from src.server.database.turn_lifecycle import FinalizeResult
    from src.server.services.background_task_manager import BackgroundTaskManager

    for ledger in (
        patch.object(tl_db, "get_run", AsyncMock(return_value=None)),
        patch.object(
            tl_db,
            "get_run",
            AsyncMock(return_value=_row("in_progress")),
        ),
        patch.object(
            tl_db, "get_run", AsyncMock(side_effect=ConnectionError("db down"))
        ),
    ):
        cache = _FakeCache({"ptc_origin:ptc-1": {"flash_thread_id": "flash-1"}})
        clear = AsyncMock()
        p1, p2, _default = _patched(cache, clear)
        finalize = patch.object(
            tl_db,
            "finalize_run",
            AsyncMock(
                return_value=FinalizeResult(applied=False, run={"status": "error"})
            ),
        )
        fake_manager = AsyncMock()
        fake_manager.is_run_live = AsyncMock(return_value=False)
        with p1, p2, ledger, finalize, patch.object(
            BackgroundTaskManager,
            "get_instance",
            classmethod(lambda cls: fake_manager),
        ):
            ok = await _consume_background_gen(
                _crashing_gen(), "PTC_DISPATCH", "ptc-1", "run-1"
            )
        assert ok is False
        clear.assert_not_awaited()


# --- last-resort finalize + verified run_end (I6) ---------------------------
#
# The dispatch-failure path funnels through finalize_detached_run: one CAS
# (adopting durable cancel intent), then transport closure with the VERIFIED
# terminal status through the run_end gate — a lost CAS still closes the
# stream with the survivor's outcome (the gate keeps it exactly-once against
# a live emitter). All frames go through append_run_end_event; the consumer
# path never XADDs directly. When nothing durable can be established, an
# error frame informs without claiming a terminal (outcome=None).


def _row(status):
    """A minimally-real ledger row: the fallback finalize builds its hook
    factory from these fields (build_finalize_jobs_from_run_row)."""
    return {
        "conversation_response_id": "run-1",
        "conversation_thread_id": "ptc-1",
        "status": status,
        "metadata": {"msg_type": "ptc", "user_id": "u-1"},
    }


def _finalize_patch(result):
    from src.server.database import turn_lifecycle as tl_db

    mock = (
        AsyncMock(side_effect=result)
        if isinstance(result, BaseException) or (
            isinstance(result, type) and issubclass(result, BaseException)
        )
        else AsyncMock(return_value=result)
    )
    return patch.object(tl_db, "finalize_run", mock), mock


async def _run_crash(cache, finalize_result):
    from src.server.services.background_task_manager import BackgroundTaskManager

    closes = []
    fake_manager = AsyncMock()
    fake_manager.is_run_live = AsyncMock(return_value=False)
    fake_manager.append_run_end_event.side_effect = (
        lambda *a, **kw: closes.append((a, kw))
    )
    clear = AsyncMock()
    p1, p2, p0 = _patched(cache, clear)
    p3, finalize = _finalize_patch(finalize_result)
    with p1, p2, p0, p3, patch.object(
        BackgroundTaskManager,
        "get_instance",
        classmethod(lambda cls: fake_manager),
    ):
        ok = await _consume_background_gen(
            _crashing_gen(), "PTC_DISPATCH", "ptc-1", "run-1"
        )
    assert ok is False
    # ALL frame emission goes through the run_end gate — the consumer path
    # never writes to the stream directly.
    cache.client.xadd.assert_not_awaited()
    return closes


@pytest.mark.asyncio
async def test_orphaned_in_progress_row_is_finalized_then_closed():
    """The last-resort CAS wins: transport closes with the CAS-adopted
    status, and the failure story rides the gate as the error frame."""
    from src.server.database.turn_lifecycle import FinalizeResult

    closes = await _run_crash(
        _FakeCache({}),
        FinalizeResult(applied=True, run={"status": "error"}),
    )
    assert len(closes) == 1
    args, kwargs = closes[0]
    assert args == ("ptc-1", "run-1", "error")
    assert kwargs["error_frame"]["error_type"] == "background_failure"


@pytest.mark.asyncio
async def test_durable_cancel_intent_adopts_cancelled_outcome():
    """An adopted cancel is not a failure to the client: run_end(cancelled)
    closes the stream with no background_failure error frame."""
    from src.server.database.turn_lifecycle import FinalizeResult

    closes = await _run_crash(
        _FakeCache({}),
        FinalizeResult(applied=True, run={"status": "cancelled"}),
    )
    assert closes == [(("ptc-1", "run-1", "cancelled"), {"error_frame": None})]


@pytest.mark.asyncio
async def test_lost_finalize_cas_still_closes_with_survivor_status():
    """applied=False means the real owner finalized concurrently — but it
    may have died between its commit and its emission, so the transport is
    still closed with the SURVIVOR's outcome; the run_end gate keeps this
    exactly-once against a live owner."""
    from src.server.database.turn_lifecycle import FinalizeResult

    closes = await _run_crash(
        _FakeCache({}),
        FinalizeResult(applied=False, run={"status": "completed"}),
    )
    assert closes == [(("ptc-1", "run-1", "completed"), {"error_frame": None})]


@pytest.mark.asyncio
async def test_survivor_error_row_closes_with_error_frame():
    """In-generator finalize (row already error, transport maybe never
    closed): the consumer path completes the close with the ROW's status
    and the failure story — the gate suppresses it if the owner already
    told its own."""
    from src.server.database.turn_lifecycle import FinalizeResult

    closes = await _run_crash(
        _FakeCache({}),
        FinalizeResult(applied=False, run={"status": "error"}),
    )
    assert len(closes) == 1
    args, kwargs = closes[0]
    assert args == ("ptc-1", "run-1", "error")
    assert kwargs["error_frame"] is not None


@pytest.mark.asyncio
async def test_no_ledger_row_informs_without_claiming_terminal():
    """Pre-START crash (RunNotFoundError): nothing durable exists, so no
    terminal may be claimed — outcome=None hands the error frame to the
    gate without closing the stream."""
    from src.server.database.turn_lifecycle import RunNotFoundError

    closes = await _run_crash(_FakeCache({}), RunNotFoundError("run-1"))
    assert len(closes) == 1
    args, kwargs = closes[0]
    assert args == ("ptc-1", "run-1", None)
    assert kwargs["error_frame"]["error_type"] == "background_failure"


@pytest.mark.asyncio
async def test_failed_last_resort_finalize_withholds_terminal_claim():
    """If the CAS itself fails the row stays in_progress — an error frame
    may inform (outcome=None), but no run_end terminal claim appears (I6)."""
    closes = await _run_crash(_FakeCache({}), RuntimeError("db down"))
    assert len(closes) == 1
    args, kwargs = closes[0]
    assert args == ("ptc-1", "run-1", None)
    assert kwargs["error_frame"] is not None


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
        tl_db, "finalize_run", finalize
    ), patch.object(
        BackgroundTaskManager,
        "get_instance",
        classmethod(lambda cls: fake_manager),
    ):
        ok = await _consume_background_gen(
            _crashing_gen(), "PTC_DISPATCH", "ptc-1", "run-1"
        )

    assert ok is False
    finalize.assert_not_awaited()
    cache.client.xadd.assert_not_awaited()
    fake_manager.append_run_end_event.assert_not_awaited()
    clear.assert_not_awaited()  # live dispatched run's watch keys stay in use
