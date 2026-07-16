"""Hook outbox (v4 1.7, I5) — decision table, executors, drainer ack/nack.

``build_finalize_jobs`` is the single mapping from a run's CAS-adopted final
status to its durable post-commit effects; executors validate their own
applicability (ordinary runs no-op) and RAISE on transport failure so the
drainer's nack/backoff — not a swallowed log line — is the retry path.
"""

from unittest.mock import AsyncMock, MagicMock, call, patch

import pytest

from src.server.database.hook_outbox import (
    build_finalize_jobs,
    build_finalize_jobs_from_run_row,
)
from src.server.handlers.chat.report_back import ClearOutcome
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
        assert rb.payload == {"ptc_thread_id": "ptc-1", "dispatch_gen": None}
        # No flash origin stamped -> falls back to the run's own thread.
        assert rb.ordering_key == "ptc-1"
        assert rb.idempotency_key == "run-1:report_back"

    def test_origin_flash_thread_keys_the_whole_lifecycle_chain(self):
        """One ordering rule: every report-back-lifecycle job keys on the
        WATCHING flash thread, so N dispatched PTCs reporting into one flash
        thread serialize strictly across competing drainers."""
        for status, hook in [
            ("completed", "report_back"),
            ("interrupted", "needs_input_wake"),
            ("error", "watch_clear"),
        ]:
            jobs = _jobs(status, origin_flash_thread_id="flash-9")
            job = next(j for j in jobs if j.hook_type == hook)
            assert job.ordering_key == "flash-9", (status, hook)

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
        but orders on its own (flash) thread id — the same chain as the
        report_back job that dispatched it."""
        jobs = _jobs(
            "error",
            thread_id="flash-1",
            msg_type="flash",
            report_back_ptc_thread_id="ptc-9",
        )
        wc = next(j for j in jobs if j.hook_type == "watch_clear")
        assert wc.payload["ptc_thread_id"] == "ptc-9"
        assert wc.ordering_key == "flash-1"

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
                "conversation_thread_id": "ptc-7",
                "metadata": {
                    "msg_type": "ptc",
                    "user_id": "u-7",
                    "burst_slot_id": "slot-7",
                    "origin_flash_thread_id": "flash-7",
                },
            }
        )
        jobs = factory("error")
        assert _types(jobs) == ["burst_release", "watch_clear"]
        wc = next(j for j in jobs if j.hook_type == "watch_clear")
        assert wc.payload["ptc_thread_id"] == "ptc-7"
        assert wc.payload["user_id"] == "u-7"
        # START-stamped origin routes the chain to the watching flash thread.
        assert wc.ordering_key == "flash-7"


# ---------------------------------------------------------------------------
# watch_clear executor (replaces BTM._clear_report_back_watch)
# ---------------------------------------------------------------------------


def _cache_with_origin(origin, marker=None):
    """Fake cache keyed like the executor reads it: the ptc-1 origin blob and
    (optionally) the ptc-1 admission marker; everything else reads None."""
    from src.server.handlers.chat.report_back_keys import ptc_origin_key

    cache = MagicMock()
    cache.enabled = True
    cache.client = MagicMock()
    cache.client.publish = AsyncMock()
    values = {
        ptc_origin_key("ptc-1"): origin,
        "workflow:status:ptc-1": marker,
    }
    cache.get_strict = AsyncMock(side_effect=lambda key: values.get(key))
    return cache


def _guard(owned: bool = True):
    """Stand-in for fenced_job_guard yielding fixed ownership."""
    from contextlib import asynccontextmanager

    @asynccontextmanager
    async def guard(job_id, attempts):
        yield owned

    return patch.object(hook_outbox.outbox_db, "fenced_job_guard", guard)


class TestExecWatchClear:
    @pytest.mark.asyncio
    async def test_clears_via_origin_and_wakes_on_error(self):
        from src.server.handlers.chat.report_back_keys import (
            ptc_origin_key,
            thread_wake_key,
        )

        cache = _cache_with_origin({"flash_thread_id": "flash-1", "user_id": "u-1"})
        mock_clear = AsyncMock(return_value=ClearOutcome(True))
        with _guard(), patch(
            "src.utils.cache.redis_cache.get_cache_client", return_value=cache
        ), patch(
            "src.server.handlers.chat.report_back.clear_flash_report_back",
            mock_clear,
        ):
            await hook_outbox._exec_watch_clear(
                _job(
                    hook_type="watch_clear",
                    payload={"ptc_thread_id": "ptc-1", "user_id": "u-1", "error_wake": True},
                    ordering_key="flash-1",
                )
            )

        cache.get_strict.assert_awaited_once_with(ptc_origin_key("ptc-1"))
        mock_clear.assert_awaited_once_with(
            cache, "ptc-1", "flash-1", user_id="u-1", expected_gen=None,
            refuse_if_pointer=False,
        )
        assert cache.client.publish.await_args.args[0] == thread_wake_key("flash-1")

    @pytest.mark.asyncio
    async def test_stale_key_requeues_onto_flash_chain_without_teardown(self):
        """Codex round-18 P1: off the flash chain, this teardown — and the
        orphan resolver it can escalate to — runs CONCURRENTLY with the
        chain's report_back admission; a resolver settling the pair between
        the admission's atomic pointer claim and the route consummating it
        schedules an orphan summary behind a settled pair. A stale-keyed job
        must requeue onto the flash chain (where the open report_back lease
        serializes it past the admission window), touching nothing here."""
        cache = _cache_with_origin({"flash_thread_id": "flash-1", "user_id": "u-1"})
        mock_clear = AsyncMock()
        mock_requeue = AsyncMock(return_value="pending")
        with _guard(), patch(
            "src.utils.cache.redis_cache.get_cache_client", return_value=cache
        ), patch(
            "src.server.handlers.chat.report_back.clear_flash_report_back",
            mock_clear,
        ), patch.object(
            hook_outbox.outbox_db, "requeue_job_with_key", mock_requeue
        ):
            await hook_outbox._exec_watch_clear(
                _job(
                    hook_type="watch_clear",
                    payload={"ptc_thread_id": "ptc-1", "user_id": "u-1", "error_wake": True},
                    ordering_key="ptc-1",
                )
            )
        mock_requeue.assert_awaited_once_with(
            "job-1",
            attempts=1,
            ordering_key="flash-1",
            max_attempts=hook_outbox.MAX_ATTEMPTS,
        )
        mock_clear.assert_not_called()
        cache.client.publish.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_dead_compensation_threads_pointer_refusal(self):
        """Codex round-20 P1: a dead report_back's compensation clear becomes
        chain head the moment its source dies (a dead row holds no lease) —
        while the source may have stalled MID-ADMISSION with the run pointer
        already claimed. The compensation payload carries refuse_if_pointer
        and the executor threads it, so pointer-first refuses cleanup (the
        admission's own lifecycle owns teardown) and cleanup-first still
        410s the late claim via the membership gate."""
        cache = _cache_with_origin({"flash_thread_id": "flash-1", "user_id": "u-1"})
        mock_clear = AsyncMock(return_value=ClearOutcome(False, None))
        with _guard(), patch(
            "src.utils.cache.redis_cache.get_cache_client", return_value=cache
        ), patch(
            "src.server.handlers.chat.report_back.clear_flash_report_back",
            mock_clear,
        ):
            await hook_outbox._exec_watch_clear(
                _job(
                    hook_type="watch_clear",
                    payload={
                        "ptc_thread_id": "ptc-1",
                        "user_id": "u-1",
                        "error_wake": True,
                        "dispatch_gen": "g-1",
                        "refuse_if_pointer": True,
                    },
                    ordering_key="flash-1",
                )
            )
        assert mock_clear.await_args.kwargs["refuse_if_pointer"] is True
        # Refusal (falsy outcome, no fencer gen) short-circuits BOTH the wake
        # and the phantom-resolver escalation — the live admission settles it.
        cache.client.publish.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_origin_without_flash_thread_executes_in_place(self):
        """No flash chain to requeue onto -> no admission to race: execute
        here rather than requeueing onto a None key."""
        cache = _cache_with_origin({"user_id": "u-1"})
        mock_clear = AsyncMock(return_value=ClearOutcome(True))
        mock_requeue = AsyncMock()
        with _guard(), patch(
            "src.utils.cache.redis_cache.get_cache_client", return_value=cache
        ), patch(
            "src.server.handlers.chat.report_back.clear_flash_report_back",
            mock_clear,
        ), patch.object(
            hook_outbox.outbox_db, "requeue_job_with_key", mock_requeue
        ):
            await hook_outbox._exec_watch_clear(
                _job(
                    hook_type="watch_clear",
                    payload={"ptc_thread_id": "ptc-1", "user_id": "u-1", "error_wake": True},
                    ordering_key="ptc-1",
                )
            )
        mock_requeue.assert_not_awaited()
        mock_clear.assert_awaited_once_with(
            cache, "ptc-1", None, user_id="u-1", expected_gen=None,
            refuse_if_pointer=False,
        )

    @pytest.mark.asyncio
    async def test_fenced_clear_suppressed_resolution_stays_silent(self):
        """A fenced clear whose resolution the atomic resolver suppresses
        (fencer admitted / origin moved / live foreign run) must neither tear
        anything down nor misreport the live owner as failed."""
        cache = _cache_with_origin(
            {"flash_thread_id": "flash-1", "user_id": "u-1", "dispatch_gen": "g-NEW"},
        )
        mock_clear = AsyncMock(return_value=ClearOutcome(False, "g-NEW"))
        mock_resolve = AsyncMock(return_value=(False, None))  # e.g. admitted
        with _guard(), patch(
            "src.utils.cache.redis_cache.get_cache_client", return_value=cache
        ), patch(
            "src.server.handlers.chat.report_back.clear_flash_report_back",
            mock_clear,
        ), patch(
            "src.server.handlers.chat.report_back.resolve_orphaned_watch",
            mock_resolve,
        ):
            await hook_outbox._exec_watch_clear(
                _job(
                    hook_type="watch_clear",
                    payload={
                        "ptc_thread_id": "ptc-1",
                        "user_id": "u-1",
                        "error_wake": True,
                        "dispatch_gen": "g-OLD",
                    },
                    ordering_key="flash-1",
                )
            )
        assert mock_clear.await_args.kwargs["expected_gen"] == "g-OLD"
        # The resolver is consulted with the EXACT generation that fenced the
        # teardown — not a fresh origin read, which a newer reservation could
        # have replaced in the meantime.
        mock_resolve.assert_awaited_once_with(
            cache, "ptc-1", "flash-1", "u-1", fencer_gen="g-NEW", job_gen="g-OLD"
        )
        cache.client.publish.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_fenced_clear_by_unadmitted_reservation_still_wakes(self):
        """A fence held by a reservation that never achieved admission
        (lost-409 continuation) must NOT swallow the predecessor's only
        failure signal — the pair's client-facing state is durably resolved
        (and the phantom receipted) BEFORE the wake goes out, even though
        the teardown stays fenced."""
        from src.server.handlers.chat.report_back_keys import thread_wake_key

        cache = _cache_with_origin(
            {"flash_thread_id": "flash-1", "user_id": "u-1", "dispatch_gen": "g-PHANTOM"},
        )
        mock_clear = AsyncMock(return_value=ClearOutcome(False, "g-PHANTOM"))
        mock_resolve = AsyncMock(return_value=(True, "rb-1"))
        with _guard(), patch(
            "src.utils.cache.redis_cache.get_cache_client", return_value=cache
        ), patch(
            "src.server.handlers.chat.report_back.clear_flash_report_back",
            mock_clear,
        ), patch(
            "src.server.handlers.chat.report_back.resolve_orphaned_watch",
            mock_resolve,
        ):
            await hook_outbox._exec_watch_clear(
                _job(
                    hook_type="watch_clear",
                    payload={
                        "ptc_thread_id": "ptc-1",
                        "user_id": "u-1",
                        "error_wake": True,
                        "dispatch_gen": "g-OLD",
                    },
                    ordering_key="flash-1",
                )
            )
        # Durable resolution BEFORE the ephemeral wake: memberships/pointer
        # fall so even a dropped nudge degrades to a poll of resolved state.
        mock_resolve.assert_awaited_once_with(
            cache,
            "ptc-1",
            "flash-1",
            "u-1",
            fencer_gen="g-PHANTOM",
            job_gen="g-OLD",
        )
        assert cache.client.publish.await_args.args[0] == thread_wake_key("flash-1")

    @pytest.mark.asyncio
    async def test_fenced_clear_without_fencer_gen_stays_silent(self):
        """Defensive: a falsy ClearOutcome with no fencing generation (Redis-
        less degradation shapes) has nothing sound to resolve against —
        never guess."""
        cache = _cache_with_origin(
            {"flash_thread_id": "flash-1", "user_id": "u-1"},
        )
        mock_clear = AsyncMock(return_value=ClearOutcome(False, None))
        mock_resolve = AsyncMock()
        with _guard(), patch(
            "src.utils.cache.redis_cache.get_cache_client", return_value=cache
        ), patch(
            "src.server.handlers.chat.report_back.clear_flash_report_back",
            mock_clear,
        ), patch(
            "src.server.handlers.chat.report_back.resolve_orphaned_watch",
            mock_resolve,
        ):
            await hook_outbox._exec_watch_clear(
                _job(
                    hook_type="watch_clear",
                    payload={
                        "ptc_thread_id": "ptc-1",
                        "user_id": "u-1",
                        "error_wake": True,
                        "dispatch_gen": "g-OLD",
                    },
                    ordering_key="flash-1",
                )
            )
        mock_resolve.assert_not_awaited()
        cache.client.publish.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_fenced_resolve_failure_propagates_for_nack(self):
        """The resolver must not decide on missing state: an unavailable
        Redis nacks the job for retry instead of guessing."""
        cache = _cache_with_origin(
            {"flash_thread_id": "flash-1", "user_id": "u-1", "dispatch_gen": "g-NEW"},
        )
        mock_clear = AsyncMock(return_value=ClearOutcome(False, "g-NEW"))
        mock_resolve = AsyncMock(side_effect=ConnectionError("redis down"))
        with _guard(), patch(
            "src.utils.cache.redis_cache.get_cache_client", return_value=cache
        ), patch(
            "src.server.handlers.chat.report_back.clear_flash_report_back",
            mock_clear,
        ), patch(
            "src.server.handlers.chat.report_back.resolve_orphaned_watch",
            mock_resolve,
        ):
            with pytest.raises(ConnectionError):
                await hook_outbox._exec_watch_clear(
                    _job(
                        hook_type="watch_clear",
                        payload={
                            "ptc_thread_id": "ptc-1",
                            "user_id": "u-1",
                            "error_wake": True,
                            "dispatch_gen": "g-OLD",
                        },
                        ordering_key="flash-1",
                    )
                )
        cache.client.publish.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_consumption_clear_publishes_cleared_wake(self):
        """A landed consumption clear pushes the pending→idle transition so
        watchers drop the chip now instead of riding the 60s backstop. No
        error wake — the payload says ``cleared``, not ``error``."""
        cache = _cache_with_origin({"flash_thread_id": "flash-1", "user_id": "u-1"})
        mock_clear = AsyncMock(return_value=ClearOutcome(True))
        with _guard(), patch(
            "src.utils.cache.redis_cache.get_cache_client", return_value=cache
        ), patch(
            "src.server.handlers.chat.report_back.clear_flash_report_back",
            mock_clear,
        ):
            await hook_outbox._exec_watch_clear(
                _job(
                    hook_type="watch_clear",
                    payload={"ptc_thread_id": "ptc-1", "user_id": "u-1", "error_wake": False},
                    ordering_key="flash-1",
                )
            )

        mock_clear.assert_awaited_once()
        cache.client.publish.assert_awaited_once()
        published = cache.client.publish.await_args.args[1]
        assert '"cleared": true' in published
        assert "error" not in published

    @pytest.mark.asyncio
    async def test_fenced_consumption_clear_stays_silent(self):
        """Fenced (not cleared) = a newer incarnation owns the pair — the
        watcher is still legitimately pending, so no wake of any kind."""
        cache = _cache_with_origin({"flash_thread_id": "flash-1", "user_id": "u-1"})
        mock_clear = AsyncMock(return_value=ClearOutcome(False, "g-NEW"))
        with _guard(), patch(
            "src.utils.cache.redis_cache.get_cache_client", return_value=cache
        ), patch(
            "src.server.handlers.chat.report_back.clear_flash_report_back",
            mock_clear,
        ):
            await hook_outbox._exec_watch_clear(
                _job(
                    hook_type="watch_clear",
                    payload={"ptc_thread_id": "ptc-1", "user_id": "u-1", "error_wake": False},
                    ordering_key="flash-1",
                )
            )

        mock_clear.assert_awaited_once()
        cache.client.publish.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_noop_without_origin(self):
        """Ordinary run, or an already-cleared watch (idempotent retry)."""
        cache = _cache_with_origin(None)
        mock_clear = AsyncMock()
        with _guard(), patch(
            "src.utils.cache.redis_cache.get_cache_client", return_value=cache
        ), patch(
            "src.server.handlers.chat.report_back.clear_flash_report_back",
            mock_clear,
        ):
            await hook_outbox._exec_watch_clear(
                _job(
                    hook_type="watch_clear",
                    payload={"ptc_thread_id": "flash-1", "user_id": "u-1", "error_wake": True},
                )
            )
        mock_clear.assert_not_called()
        cache.client.publish.assert_not_called()

    @pytest.mark.asyncio
    async def test_clear_failure_propagates_for_nack(self):
        """Inverted from the old best-effort hook: a failed clear must raise
        so the drainer nacks and retries — swallowing would ack a leaked
        watch + per-user cap slot."""
        cache = _cache_with_origin({"flash_thread_id": "flash-1"})
        with _guard(), patch(
            "src.utils.cache.redis_cache.get_cache_client", return_value=cache
        ), patch(
            "src.server.handlers.chat.report_back.clear_flash_report_back",
            AsyncMock(side_effect=RuntimeError("redis down")),
        ):
            with pytest.raises(RuntimeError):
                await hook_outbox._exec_watch_clear(
                    _job(
                        hook_type="watch_clear",
                        payload={"ptc_thread_id": "ptc-1", "error_wake": True},
                        ordering_key="flash-1",
                    )
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
                    _job(
                        hook_type="watch_clear",
                        payload={"ptc_thread_id": "ptc-1", "error_wake": True},
                    )
                )
            with pytest.raises(ConnectionError):
                await hook_outbox._exec_needs_input_wake(
                    _job(
                        hook_type="needs_input_wake",
                        payload={"ptc_thread_id": "ptc-1"},
                    )
                )

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
# report_back executor — thin delegation, full job row (the executor needs the
# job id for lease heartbeats + the dispatched_run_id payload merge)
# ---------------------------------------------------------------------------


class TestExecReportBack:
    @pytest.mark.asyncio
    async def test_delegates_full_job_row(self):
        with patch(
            "src.server.handlers.chat.report_back.execute_report_back",
            AsyncMock(),
        ) as exec_rb:
            job = _job(hook_type="report_back", payload={"ptc_thread_id": "ptc-1"})
            await hook_outbox._exec_report_back(job)
        exec_rb.assert_awaited_once_with(job)


# ---------------------------------------------------------------------------
# Drainer execute: ack on success, nack on failure, unknown type nacked
# ---------------------------------------------------------------------------


def _job(hook_type="burst_release", payload=None, ordering_key=None):
    return {
        "hook_outbox_id": "job-1",
        "hook_type": hook_type,
        "payload": payload or {},
        "run_id": "run-1",
        "attempts": 1,
        "ordering_key": ordering_key,
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
        # Fenced ack: attempts is the lease-generation token.
        ack.assert_awaited_once_with("job-1", attempts=1)
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
        nack.assert_awaited_once_with(
            "job-1", attempts=1, max_attempts=hook_outbox.MAX_ATTEMPTS
        )
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

    # Dead-letter compensation is inserted ATOMICALLY inside
    # nack_outbox_job / park_exhausted_jobs (data-modifying CTE) — there is
    # deliberately no drainer-level compensation step to test; the SQL-level
    # contract is exercised by the live claim-gate verification script.


# ---------------------------------------------------------------------------
# Legacy FIFO migration sweep (one-shot at drainer start): folds pre-drainer
# Redis queues + pre-origin-stamping outbox rows into the ordering-key model.
# ---------------------------------------------------------------------------


class TestLegacyFifoMigration:
    def _cache(self):
        from tests.unit.server.handlers.chat.redis_fakes import (
            FakeCache,
            seed_dispatched,
        )

        cache = FakeCache()
        seed_dispatched(cache, "flash-1", ["ptc-a", "ptc-b"])
        return cache

    def _patches(self, cache, *, latest=None, pending=None):
        return [
            patch(
                "src.utils.cache.redis_cache.get_cache_client", return_value=cache
            ),
            patch(
                "src.server.database.turn_lifecycle.get_latest_attempt",
                AsyncMock(side_effect=latest or (lambda tid: None)),
            ),
            patch.object(
                hook_outbox.outbox_db, "enqueue_compensation_job", AsyncMock()
            ),
            patch.object(
                hook_outbox.outbox_db,
                "list_pending_jobs",
                AsyncMock(side_effect=lambda ht: (pending or {}).get(ht, [])),
            ),
            patch.object(
                hook_outbox.outbox_db,
                "set_job_ordering_key",
                AsyncMock(return_value=True),
            ),
        ]

    async def _run(self, cache, **kw):
        import contextlib

        with contextlib.ExitStack() as stack:
            mocks = [stack.enter_context(p) for p in self._patches(cache, **kw)]
            await hook_outbox._migrate_legacy_fifo_queues()
        return mocks

    @pytest.mark.asyncio
    async def test_queue_entries_become_outbox_rows_keyed_on_flash(self):
        cache = self._cache()
        cache.client.lists["flash_rb_queue:flash-1"] = ["ptc-a"]
        cache.client.sets["flash_rb_queued:flash-1"] = {"ptc-a"}

        async def _latest(tid):
            return {"conversation_response_id": f"run-{tid}"}

        _, _, comp, _, _ = await self._run(cache, latest=_latest)

        comp.assert_awaited_once_with(
            run_id="run-ptc-a",
            thread_id="ptc-a",
            hook_type="report_back",
            payload={"ptc_thread_id": "ptc-a"},
            ordering_key="flash-1",
            idempotency_key="legacy_fifo:flash-1:ptc-a",
            backdate_seconds=30 * 86400.0,
        )
        # Queue + dedup-marker keys removed only after the entry landed.
        assert "flash_rb_queue:flash-1" not in cache.client.lists
        assert "flash_rb_queued:flash-1" not in cache.client.sets

    @pytest.mark.asyncio
    async def test_entry_without_run_row_is_cleared_not_enqueued(self):
        """No run row -> no FK target and nothing will ever render the turn:
        release its watch/cap state instead of inserting a doomed job."""
        cache = self._cache()
        cache.client.lists["flash_rb_queue:flash-1"] = ["ptc-a"]

        _, _, comp, _, _ = await self._run(cache)

        comp.assert_not_awaited()
        from src.server.handlers.chat import report_back

        assert "ptc-a" not in cache.client.sets.get(
            report_back.flash_watch_key("flash-1"), set()
        )
        assert "ptc_origin:ptc-a" not in cache.kv

    @pytest.mark.asyncio
    async def test_entry_with_a_generated_origin_is_left_intact(self):
        """Round-19 P1: a queue entry proves only that a LEGACY delivery was
        owed. A generated origin belongs to a newer incarnation — possibly a
        rival worker's reservation whose START hasn't committed — so the
        sweep's legacy-scoped clear is fenced and touches nothing (that
        reservation's rollback consults the tombstone and completes it)."""
        from src.server.handlers.chat import report_back

        cache = self._cache()
        cache.client.lists["flash_rb_queue:flash-1"] = ["ptc-a"]
        cache.kv["ptc_origin:ptc-a"]["dispatch_gen"] = "g-FRESH"

        _, _, comp, _, _ = await self._run(cache)

        comp.assert_not_awaited()
        assert "ptc_origin:ptc-a" in cache.kv
        assert "ptc-a" in cache.client.sets[report_back.flash_watch_key("flash-1")]
        assert "__legacy__" in cache.client.sets["ptc_rb_tombstone:ptc-a"]

    @pytest.mark.asyncio
    async def test_pending_rows_rekeyed_onto_their_flash_chain(self):
        """Pre-origin-stamping rows are keyed on their own PTC thread; the
        sweep moves them onto the flash chain their execution serializes
        with — watch_clear rows included (round-18 P1: off-chain, their
        teardown races the chain's report_back admission)."""
        cache = self._cache()
        pending = {
            "report_back": [
                {  # wrong chain -> rekey
                    "hook_outbox_id": "job-1",
                    "ordering_key": "ptc-a",
                    "payload": {"ptc_thread_id": "ptc-a"},
                },
                {  # already correct -> untouched
                    "hook_outbox_id": "job-2",
                    "ordering_key": "flash-1",
                    "payload": {"ptc_thread_id": "ptc-b"},
                },
            ],
            "watch_clear": [
                {  # stale teardown row -> rekey too
                    "hook_outbox_id": "job-3",
                    "ordering_key": "ptc-b",
                    "payload": {"ptc_thread_id": "ptc-b"},
                },
            ],
        }
        _, _, _, _, rekey = await self._run(cache, pending=pending)

        assert rekey.await_args_list == [
            call("job-1", "flash-1"),
            call("job-3", "flash-1"),
        ]

    @pytest.mark.asyncio
    async def test_redis_disabled_is_a_noop(self):
        cache = self._cache()
        cache.enabled = False
        _, _, comp, listed, _ = await self._run(cache)
        comp.assert_not_awaited()
        listed.assert_not_awaited()
