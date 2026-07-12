"""Hook outbox (v4 1.7, I5) — decision table, executors, drainer ack/nack.

``build_finalize_jobs`` is the single mapping from a run's CAS-adopted final
status to its durable post-commit effects; executors validate their own
applicability (ordinary runs no-op) and RAISE on transport failure so the
drainer's nack/backoff — not a swallowed log line — is the retry path.
"""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.server.database.hook_outbox import (
    build_finalize_jobs,
    build_finalize_jobs_from_run_row,
)
from src.server.services import hook_outbox
from src.server.services.hook_outbox import HookOutboxDrainer


# ---------------------------------------------------------------------------
# Decision table
# ---------------------------------------------------------------------------


def _jobs(final_status, **kw):
    defaults = dict(
        run_id="run-1",
        thread_id="ptc-1",
        msg_type="ptc",
        user_id="u-1",
        burst_slot_id="slot-1",
    )
    defaults.update(kw)
    return build_finalize_jobs(**defaults)(final_status)


def _types(jobs):
    return sorted(j.hook_type for j in jobs)


class TestBuildFinalizeJobs:
    def test_completed_ptc_gets_burst_release_and_report_back(self):
        jobs = _jobs("completed")
        assert _types(jobs) == ["burst_release", "report_back"]
        rb = next(j for j in jobs if j.hook_type == "report_back")
        assert rb.payload == {"ptc_thread_id": "ptc-1"}
        assert rb.ordering_key == "ptc-1"
        assert rb.idempotency_key == "run-1:report_back"

    def test_interrupted_ptc_gets_needs_input_wake(self):
        jobs = _jobs("interrupted")
        assert _types(jobs) == ["burst_release", "needs_input_wake"]
        wake = next(j for j in jobs if j.hook_type == "needs_input_wake")
        assert wake.payload == {"ptc_thread_id": "ptc-1"}
        assert wake.ordering_key == "ptc-1"

    @pytest.mark.parametrize("status", ["error", "cancelled"])
    def test_failed_run_gets_watch_clear_with_error_wake(self, status):
        jobs = _jobs(status)
        assert _types(jobs) == ["burst_release", "watch_clear"]
        wc = next(j for j in jobs if j.hook_type == "watch_clear")
        assert wc.payload["error_wake"] is True
        assert wc.payload["ptc_thread_id"] == "ptc-1"

    def test_failed_report_back_flash_clears_via_origin_id(self):
        """A report-back flash run resolves the watch by its origin PTC id,
        not by its own (flash) thread id."""
        jobs = _jobs(
            "error",
            thread_id="flash-1",
            msg_type="flash",
            report_back_ptc_thread_id="ptc-9",
        )
        wc = next(j for j in jobs if j.hook_type == "watch_clear")
        assert wc.payload["ptc_thread_id"] == "ptc-9"
        assert wc.ordering_key == "ptc-9"

    def test_completed_flash_never_reports_back(self):
        jobs = _jobs("completed", thread_id="flash-1", msg_type="flash")
        assert _types(jobs) == ["burst_release"]

    def test_interrupted_flash_never_wakes(self):
        jobs = _jobs("interrupted", thread_id="flash-1", msg_type="flash")
        assert _types(jobs) == ["burst_release"]

    def test_no_user_or_slot_skips_burst_release(self):
        assert "burst_release" not in _types(_jobs("completed", user_id=None))
        assert "burst_release" not in _types(
            _jobs("completed", burst_slot_id=None)
        )

    def test_idempotency_keys_are_run_scoped_and_stable(self):
        """A re-finalize (lost race, sweep retry) enqueues the same keys —
        ON CONFLICT DO NOTHING makes the effect exactly-once per run."""
        a = {j.idempotency_key for j in _jobs("error")}
        b = {j.idempotency_key for j in _jobs("error")}
        assert a == b == {"run-1:burst_release", "run-1:watch_clear"}

    def test_from_run_row_rebuilds_from_start_stamped_metadata(self):
        factory = build_finalize_jobs_from_run_row(
            {
                "conversation_response_id": "run-7",
                "conversation_thread_id": "flash-7",
                "metadata": {
                    "msg_type": "flash",
                    "user_id": "u-7",
                    "burst_slot_id": "slot-7",
                    "report_back_ptc_thread_id": "ptc-7",
                },
            }
        )
        jobs = factory("error")
        assert _types(jobs) == ["burst_release", "watch_clear"]
        wc = next(j for j in jobs if j.hook_type == "watch_clear")
        assert wc.payload["ptc_thread_id"] == "ptc-7"
        assert wc.payload["user_id"] == "u-7"


# ---------------------------------------------------------------------------
# watch_clear executor (replaces BTM._clear_report_back_watch)
# ---------------------------------------------------------------------------


def _cache_with_origin(origin):
    cache = MagicMock()
    cache.enabled = True
    cache.client = MagicMock()
    cache.client.publish = AsyncMock()
    cache.get_strict = AsyncMock(return_value=origin)
    return cache


class TestExecWatchClear:
    @pytest.mark.asyncio
    async def test_clears_via_origin_and_wakes_on_error(self):
        from src.server.handlers.chat.report_back_keys import (
            ptc_origin_key,
            thread_wake_key,
        )

        cache = _cache_with_origin({"flash_thread_id": "flash-1", "user_id": "u-1"})
        mock_clear = AsyncMock()
        with patch(
            "src.utils.cache.redis_cache.get_cache_client", return_value=cache
        ), patch(
            "src.server.handlers.chat.report_back.clear_flash_report_back",
            mock_clear,
        ):
            await hook_outbox._exec_watch_clear(
                {"ptc_thread_id": "ptc-1", "user_id": "u-1", "error_wake": True}
            )

        cache.get_strict.assert_awaited_once_with(ptc_origin_key("ptc-1"))
        mock_clear.assert_awaited_once_with(cache, "ptc-1", "flash-1", user_id="u-1")
        assert cache.client.publish.await_args.args[0] == thread_wake_key("flash-1")

    @pytest.mark.asyncio
    async def test_consumption_clear_skips_error_wake(self):
        cache = _cache_with_origin({"flash_thread_id": "flash-1", "user_id": "u-1"})
        mock_clear = AsyncMock()
        with patch(
            "src.utils.cache.redis_cache.get_cache_client", return_value=cache
        ), patch(
            "src.server.handlers.chat.report_back.clear_flash_report_back",
            mock_clear,
        ):
            await hook_outbox._exec_watch_clear(
                {"ptc_thread_id": "ptc-1", "user_id": "u-1", "error_wake": False}
            )

        mock_clear.assert_awaited_once()
        cache.client.publish.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_noop_without_origin(self):
        """Ordinary run, or an already-cleared watch (idempotent retry)."""
        cache = _cache_with_origin(None)
        mock_clear = AsyncMock()
        with patch(
            "src.utils.cache.redis_cache.get_cache_client", return_value=cache
        ), patch(
            "src.server.handlers.chat.report_back.clear_flash_report_back",
            mock_clear,
        ):
            await hook_outbox._exec_watch_clear(
                {"ptc_thread_id": "flash-1", "user_id": "u-1", "error_wake": True}
            )
        mock_clear.assert_not_called()
        cache.client.publish.assert_not_called()

    @pytest.mark.asyncio
    async def test_clear_failure_propagates_for_nack(self):
        """Inverted from the old best-effort hook: a failed clear must raise
        so the drainer nacks and retries — swallowing would ack a leaked
        watch + per-user cap slot."""
        cache = _cache_with_origin({"flash_thread_id": "flash-1"})
        with patch(
            "src.utils.cache.redis_cache.get_cache_client", return_value=cache
        ), patch(
            "src.server.handlers.chat.report_back.clear_flash_report_back",
            AsyncMock(side_effect=RuntimeError("redis down")),
        ):
            with pytest.raises(RuntimeError):
                await hook_outbox._exec_watch_clear(
                    {"ptc_thread_id": "ptc-1", "error_wake": True}
                )

    @pytest.mark.asyncio
    async def test_origin_read_failure_propagates_for_nack(self):
        """The origin lookup is a STRICT read: a Redis blip must raise (→
        drainer nack/retry), never degrade to "no origin" and ack a dropped
        clear."""
        cache = _cache_with_origin(None)
        cache.get_strict = AsyncMock(side_effect=ConnectionError("redis down"))
        with patch(
            "src.utils.cache.redis_cache.get_cache_client", return_value=cache
        ):
            with pytest.raises(ConnectionError):
                await hook_outbox._exec_watch_clear(
                    {"ptc_thread_id": "ptc-1", "error_wake": True}
                )
            with pytest.raises(ConnectionError):
                await hook_outbox._exec_needs_input_wake({"ptc_thread_id": "ptc-1"})

    @pytest.mark.asyncio
    async def test_runtime_disabled_cache_nacks_not_acks(self):
        """Codex round-4 probe: a failed startup connect flips enabled=False
        with a client still present; a drainer booted in that state must NACK
        pending jobs (real get_strict raises), never ack them as config-off
        no-ops — Redis may come back with the effects still owed."""
        from src.server.database import hook_outbox as outbox_db
        from src.utils.cache.redis_cache import RedisCacheClient

        cache = RedisCacheClient(url="redis://unit-test-never-connects:6379/0")
        cache.enabled = False  # what connect() leaves behind on failure
        cache.client = MagicMock()  # present but unusable

        drainer = HookOutboxDrainer()
        with patch(
            "src.utils.cache.redis_cache.get_cache_client", return_value=cache
        ), patch.object(outbox_db, "ack_outbox_job", AsyncMock()) as ack, patch.object(
            outbox_db, "nack_outbox_job", AsyncMock(return_value="pending")
        ) as nack:
            await drainer._execute(
                _job(hook_type="watch_clear", payload={"ptc_thread_id": "ptc-1"})
            )
            await drainer._execute(
                _job(hook_type="needs_input_wake", payload={"ptc_thread_id": "ptc-1"})
            )
        assert nack.await_count == 2
        ack.assert_not_awaited()


# ---------------------------------------------------------------------------
# Drainer execute: ack on success, nack on failure, unknown type nacked
# ---------------------------------------------------------------------------


def _job(hook_type="burst_release", payload=None):
    return {
        "hook_outbox_id": "job-1",
        "hook_type": hook_type,
        "payload": payload or {},
        "run_id": "run-1",
        "attempts": 1,
    }


class TestDrainerExecute:
    @pytest.mark.asyncio
    async def test_success_acks(self):
        from src.server.database import hook_outbox as outbox_db

        drainer = HookOutboxDrainer()
        with patch.dict(
            hook_outbox._EXECUTORS, {"burst_release": AsyncMock()}
        ), patch.object(outbox_db, "ack_outbox_job", AsyncMock()) as ack, patch.object(
            outbox_db, "nack_outbox_job", AsyncMock()
        ) as nack:
            await drainer._execute(_job())
        ack.assert_awaited_once_with("job-1")
        nack.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_executor_failure_nacks(self):
        from src.server.database import hook_outbox as outbox_db

        drainer = HookOutboxDrainer()
        with patch.dict(
            hook_outbox._EXECUTORS,
            {"burst_release": AsyncMock(side_effect=RuntimeError("boom"))},
        ), patch.object(outbox_db, "ack_outbox_job", AsyncMock()) as ack, patch.object(
            outbox_db, "nack_outbox_job", AsyncMock(return_value="pending")
        ) as nack:
            await drainer._execute(_job())
        nack.assert_awaited_once()
        ack.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_unknown_hook_type_nacks_toward_dead(self):
        from src.server.database import hook_outbox as outbox_db

        drainer = HookOutboxDrainer()
        with patch.object(outbox_db, "ack_outbox_job", AsyncMock()) as ack, patch.object(
            outbox_db, "nack_outbox_job", AsyncMock(return_value="dead")
        ) as nack:
            await drainer._execute(_job(hook_type="not_a_hook"))
        nack.assert_awaited_once()
        ack.assert_not_awaited()
