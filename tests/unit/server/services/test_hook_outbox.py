"""Hook outbox (v4 1.7, I5) — decision table, executors, drainer ack/nack.

``build_finalize_jobs`` is the single mapping from a run's CAS-adopted final
status to its durable post-commit effects; executors validate their own
applicability (ordinary runs no-op) and RAISE on transport failure so the
drainer's nack/backoff — not a swallowed log line — is the retry path.
"""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.server.contracts.status import TERMINAL_STATUSES
from src.server.database.runs.outbox import (
    build_finalize_jobs,
    build_finalize_jobs_from_run_row,
)
from src.server.services.report_back.handoff import core
from src.server.services.report_back import subagent as task_report_back
from src.server.services.report_back.handoff.pointer import ClearOutcome
from src.server.services import hook_outbox
from src.server.services.hook_outbox import HookOutboxDrainer

EXECUTOR_MOD = "src.server.services.report_back.handoff.executor"
RESERVE_MOD = "src.server.services.report_back.handoff.reserve"
POINTER_MOD = "src.server.services.report_back.handoff.pointer"
WAKE_MOD = "src.server.services.report_back.handoff.wake"


@pytest.fixture(autouse=True)
def _registered_executors():
    """Mirror app setup: handler executors registered before any drainer use.

    Registration is idempotent for the same function, so autouse is safe."""
    core.register_outbox_executors()
    task_report_back.register_outbox_executors()


# ---------------------------------------------------------------------------
# Decision table
# ---------------------------------------------------------------------------


def _jobs(final_status, **kw):
    defaults = dict(
        run_id="run-1",
        thread_id="ptc-1",
        user_id="u-1",
        burst_slot_id="slot-1",
    )
    defaults.update(kw)
    return build_finalize_jobs(**defaults)(final_status)


def _types(jobs):
    return sorted(j.hook_type for j in jobs)


_RELEASES_ITS_PAIR = {
    "completed": "watch_clear",
    "error": "watch_clear",
    "cancelled": "watch_clear",
    "interrupted": None,
}


class TestBuildFinalizeJobs:
    def test_completed_ptc_gets_burst_release_and_report_back(self):
        jobs = _jobs("completed")
        assert _types(jobs) == ["burst_release", "report_back", "user_feed"]
        rb = next(j for j in jobs if j.hook_type == "report_back")
        assert rb.payload == {
            "ptc_thread_id": "ptc-1",
            "dispatch_gen": None,
            "final_status": "completed",
        }
        # No origin stamped -> falls back to the run's own thread.
        assert rb.ordering_key == "ptc-1"
        assert rb.idempotency_key == "run-1:report_back"

    def test_origin_dispatcher_thread_keys_the_whole_lifecycle_chain(self):
        """One ordering rule: every report-back-lifecycle job keys on the
        WATCHING Chief of Staff thread, so N dispatched PTCs reporting into one
        such thread serialize strictly across competing drainers."""
        for status, hook in [
            ("completed", "report_back"),
            ("interrupted", "needs_input_wake"),
            ("error", "report_back"),
            ("cancelled", "report_back"),
        ]:
            jobs = _jobs(status, dispatcher_thread_id="cos-9")
            job = next(j for j in jobs if j.hook_type == hook)
            assert job.ordering_key == "cos-9", (status, hook)

    def test_interrupted_ptc_gets_needs_input_wake(self):
        jobs = _jobs("interrupted")
        assert _types(jobs) == ["burst_release", "needs_input_wake", "user_feed"]
        wake = next(j for j in jobs if j.hook_type == "needs_input_wake")
        assert wake.payload == {"ptc_thread_id": "ptc-1"}
        assert wake.ordering_key == "ptc-1"

    @pytest.mark.parametrize("status", ["error", "cancelled"])
    def test_a_stopped_or_failed_ptc_run_reports_back_how_it_ended(self, status):
        """The watching thread promised the user a report; a stop or a
        failure is delivered like a result, and the summary's own completion
        releases the pair, so nothing tears it down here."""
        jobs = _jobs(status, origin_dispatch_gen="gen-3")
        assert _types(jobs) == ["burst_release", "report_back", "user_feed"]
        rb = next(j for j in jobs if j.hook_type == "report_back")
        assert rb.payload == {
            "ptc_thread_id": "ptc-1",
            "dispatch_gen": "gen-3",
            "final_status": status,
        }

    @pytest.mark.parametrize(
        ("role", "owed"),
        [
            (
                {},
                {
                    "completed": "report_back",
                    "error": "report_back",
                    "cancelled": "report_back",
                    "interrupted": "needs_input_wake",
                },
            ),
            (
                {"thread_id": "home-1", "report_back_ptc_thread_id": "ptc-9"},
                _RELEASES_ITS_PAIR,
            ),
        ],
        ids=["dispatched_ptc", "summary_on_home"],
    )
    def test_the_runs_role_decides_what_it_owes(self, role, owed):
        """A dispatched PTC run reports however it ends and wakes its watcher
        when it asks the user something. A summary run is the report-back
        itself, so it only releases its pair once over: if it owed another
        report, a failing summary would report on itself."""
        assert set(owed) == set(TERMINAL_STATUSES)
        for status, hook in owed.items():
            expected = ["burst_release", "user_feed", *([hook] if hook else [])]
            assert _types(_jobs(status, **role)) == sorted(expected), status

    @pytest.mark.parametrize("status", ["completed", "error", "cancelled"])
    def test_a_summary_releases_its_pair_by_the_origin_id(self, status):
        """A stopped or failed summary wakes the watcher so the card
        reconciles; a completed one is the consumption clear."""
        jobs = _jobs(
            status,
            thread_id="home-1",
            report_back_ptc_thread_id="ptc-9",
            origin_dispatch_gen="gen-3",
        )
        (wc,) = [j for j in jobs if j.hook_type == "watch_clear"]
        assert wc.payload == {
            "ptc_thread_id": "ptc-9",
            "user_id": "u-1",
            "error_wake": status != "completed",
            "dispatch_gen": "gen-3",
        }
        assert wc.idempotency_key == "run-1:watch_clear"
        # Ordered on its own thread: the summary runs on the watching thread,
        # the same chain as the report_back job that started it.
        assert wc.ordering_key == "home-1"

    def test_user_feed_rides_every_finalize_with_a_user(self):
        """run_settled is the at-least-once push behind the thread-lifecycle
        feed: PUBLIC status vocabulary, interrupt_reason only when
        interrupted, no ordering_key (independent per run)."""
        settled = next(
            j for j in _jobs("error", run_seq=42, workspace_id="ws-1")
            if j.hook_type == "user_feed"
        )
        assert settled.idempotency_key == "run-1:user_feed"
        assert settled.ordering_key is None
        assert settled.payload == {
            "user_id": "u-1",
            "thread_id": "ptc-1",
            "workspace_id": "ws-1",
            "run_id": "run-1",
            "run_seq": 42,
            "status": "failed",
            "interrupt_reason": None,
        }
        interrupted = next(
            j
            for j in _jobs("interrupted", interrupt_reason="ask_user")
            if j.hook_type == "user_feed"
        )
        assert interrupted.payload["status"] == "interrupted"
        assert interrupted.payload["interrupt_reason"] == "ask_user"
        # No user -> no feed to publish to.
        assert "user_feed" not in _types(_jobs("completed", user_id=None))

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
        assert a == b == {
            "run-1:burst_release",
            "run-1:user_feed",
            "run-1:report_back",
        }

    def test_from_run_row_rebuilds_from_start_stamped_metadata(self):
        factory = build_finalize_jobs_from_run_row(
            {
                "conversation_response_id": "run-7",
                "conversation_thread_id": "ptc-7",
                "metadata": {
                    "msg_type": "ptc",
                    "user_id": "u-7",
                    "burst_slot_id": "slot-7",
                    "origin_home_thread_id": "cos-7",
                },
            }
        )
        jobs = factory("error")
        assert _types(jobs) == ["burst_release", "report_back", "user_feed"]
        rb = next(j for j in jobs if j.hook_type == "report_back")
        assert rb.payload["ptc_thread_id"] == "ptc-7"
        assert rb.payload["final_status"] == "error"
        # START-stamped origin routes the chain to the watching Chief of Staff thread.
        assert rb.ordering_key == "cos-7"

    def test_an_automation_turn_settles_its_firing_on_every_terminal(self):
        """The firing ends as its run did, whichever worker finalized it. A
        run a concurrent one superseded at admission was never the firing's
        turn: the firing is back in line for the thread instead."""
        row = {
            "conversation_response_id": "run-7",
            "conversation_thread_id": "home-7",
            "metadata": {
                "msg_type": "ptc",
                "user_id": "u-7",
                "workspace_id": "ws-7",
                "automation_execution_id": "exec-7",
                "automation_id": "auto-7",
            },
        }
        for status in ("completed", "interrupted", "error", "cancelled"):
            jobs = build_finalize_jobs_from_run_row(row)(status)
            settle = next(j for j in jobs if j.hook_type == "automation_settle")
            assert settle.idempotency_key == "run-7:automation_settle"
            assert settle.payload == {
                "execution_id": "exec-7",
                "automation_id": "auto-7",
                "user_id": "u-7",
                "workspace_id": "ws-7",
            }
        superseded = {**row, "metadata": {**row["metadata"], "superseded": True}}
        assert "automation_settle" not in _types(
            build_finalize_jobs_from_run_row(superseded)("cancelled")
        )
        assert "automation_settle" not in _types(_jobs("completed"))


# ---------------------------------------------------------------------------
# watch_clear executor (replaces BTM._clear_report_back_watch)
# ---------------------------------------------------------------------------


def _cache_with_origin(origin, marker=None):
    """Fake cache keyed like the executor reads it: the ptc-1 origin blob and
    (optionally) the ptc-1 admission marker; everything else reads None."""
    from src.server.services.report_back.handoff.keys import ptc_origin_key

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
        from src.server.services.report_back.handoff.keys import (
            ptc_origin_key,
            thread_wake_key,
        )

        cache = _cache_with_origin({"home_thread_id": "cos-1", "user_id": "u-1"})
        mock_clear = AsyncMock(return_value=ClearOutcome(True))
        with _guard(), patch(
            "src.utils.cache.redis_cache.get_cache_client", return_value=cache
        ), patch(
            f"{POINTER_MOD}.clear_handoff_report_back",
            mock_clear,
        ):
            await core._exec_watch_clear(
                _job(
                    hook_type="watch_clear",
                    payload={"ptc_thread_id": "ptc-1", "user_id": "u-1", "error_wake": True},
                    ordering_key="cos-1",
                )
            )

        cache.get_strict.assert_awaited_once_with(ptc_origin_key("ptc-1"))
        mock_clear.assert_awaited_once_with(
            cache, "ptc-1", "cos-1", user_id="u-1", expected_gen=None,
            refuse_if_pointer=False,
        )
        assert cache.client.publish.await_args.args[0] == thread_wake_key("cos-1")

    @pytest.mark.asyncio
    async def test_stale_key_requeues_onto_dispatcher_chain_without_teardown(self):
        """Codex round-18 P1: off the dispatching thread's chain, this teardown — and the
        orphan resolver it can escalate to — runs CONCURRENTLY with the
        chain's report_back admission; a resolver settling the pair between
        the admission's atomic pointer claim and the route consummating it
        schedules an orphan summary behind a settled pair. A stale-keyed job
        must requeue onto that chain (where the open report_back lease
        serializes it past the admission window), touching nothing here."""
        cache = _cache_with_origin({"home_thread_id": "cos-1", "user_id": "u-1"})
        mock_clear = AsyncMock()
        mock_requeue = AsyncMock(return_value="pending")
        with _guard(), patch(
            "src.utils.cache.redis_cache.get_cache_client", return_value=cache
        ), patch(
            f"{POINTER_MOD}.clear_handoff_report_back",
            mock_clear,
        ), patch.object(
            hook_outbox.outbox_db, "requeue_job_with_key", mock_requeue
        ):
            await core._exec_watch_clear(
                _job(
                    hook_type="watch_clear",
                    payload={"ptc_thread_id": "ptc-1", "user_id": "u-1", "error_wake": True},
                    ordering_key="ptc-1",
                )
            )
        mock_requeue.assert_awaited_once_with(
            "job-1",
            attempts=1,
            ordering_key="cos-1",
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
        cache = _cache_with_origin({"home_thread_id": "cos-1", "user_id": "u-1"})
        mock_clear = AsyncMock(return_value=ClearOutcome(False, None))
        with _guard(), patch(
            "src.utils.cache.redis_cache.get_cache_client", return_value=cache
        ), patch(
            f"{POINTER_MOD}.clear_handoff_report_back",
            mock_clear,
        ):
            await core._exec_watch_clear(
                _job(
                    hook_type="watch_clear",
                    payload={
                        "ptc_thread_id": "ptc-1",
                        "user_id": "u-1",
                        "error_wake": True,
                        "dispatch_gen": "g-1",
                        "refuse_if_pointer": True,
                    },
                    ordering_key="cos-1",
                )
            )
        assert mock_clear.await_args.kwargs["refuse_if_pointer"] is True
        # Refusal (falsy outcome, no fencer gen) short-circuits BOTH the wake
        # and the phantom-resolver escalation — the live admission settles it.
        cache.client.publish.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_origin_without_dispatcher_thread_executes_in_place(self):
        """No dispatching thread chain to requeue onto -> no admission to race: execute
        here rather than requeueing onto a None key."""
        cache = _cache_with_origin({"user_id": "u-1"})
        mock_clear = AsyncMock(return_value=ClearOutcome(True))
        mock_requeue = AsyncMock()
        with _guard(), patch(
            "src.utils.cache.redis_cache.get_cache_client", return_value=cache
        ), patch(
            f"{POINTER_MOD}.clear_handoff_report_back",
            mock_clear,
        ), patch.object(
            hook_outbox.outbox_db, "requeue_job_with_key", mock_requeue
        ):
            await core._exec_watch_clear(
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
            {"home_thread_id": "cos-1", "user_id": "u-1", "dispatch_gen": "g-NEW"},
        )
        mock_clear = AsyncMock(return_value=ClearOutcome(False, "g-NEW"))
        mock_resolve = AsyncMock(return_value=(False, None))  # e.g. admitted
        with _guard(), patch(
            "src.utils.cache.redis_cache.get_cache_client", return_value=cache
        ), patch(
            f"{POINTER_MOD}.clear_handoff_report_back",
            mock_clear,
        ), patch(
            f"{RESERVE_MOD}.resolve_orphaned_watch",
            mock_resolve,
        ):
            await core._exec_watch_clear(
                _job(
                    hook_type="watch_clear",
                    payload={
                        "ptc_thread_id": "ptc-1",
                        "user_id": "u-1",
                        "error_wake": True,
                        "dispatch_gen": "g-OLD",
                    },
                    ordering_key="cos-1",
                )
            )
        assert mock_clear.await_args.kwargs["expected_gen"] == "g-OLD"
        # The resolver is consulted with the EXACT generation that fenced the
        # teardown — not a fresh origin read, which a newer reservation could
        # have replaced in the meantime.
        mock_resolve.assert_awaited_once_with(
            cache, "ptc-1", "cos-1", "u-1", fencer_gen="g-NEW", job_gen="g-OLD"
        )
        cache.client.publish.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_fenced_clear_by_unadmitted_reservation_still_wakes(self):
        """A fence held by a reservation that never achieved admission
        (lost-409 continuation) must NOT swallow the predecessor's only
        failure signal — the pair's client-facing state is durably resolved
        (and the phantom receipted) BEFORE the wake goes out, even though
        the teardown stays fenced."""
        from src.server.services.report_back.handoff.keys import thread_wake_key

        cache = _cache_with_origin(
            {"home_thread_id": "cos-1", "user_id": "u-1", "dispatch_gen": "g-PHANTOM"},
        )
        mock_clear = AsyncMock(return_value=ClearOutcome(False, "g-PHANTOM"))
        mock_resolve = AsyncMock(return_value=(True, "rb-1"))
        with _guard(), patch(
            "src.utils.cache.redis_cache.get_cache_client", return_value=cache
        ), patch(
            f"{POINTER_MOD}.clear_handoff_report_back",
            mock_clear,
        ), patch(
            f"{RESERVE_MOD}.resolve_orphaned_watch",
            mock_resolve,
        ):
            await core._exec_watch_clear(
                _job(
                    hook_type="watch_clear",
                    payload={
                        "ptc_thread_id": "ptc-1",
                        "user_id": "u-1",
                        "error_wake": True,
                        "dispatch_gen": "g-OLD",
                    },
                    ordering_key="cos-1",
                )
            )
        # Durable resolution BEFORE the ephemeral wake: memberships/pointer
        # fall so even a dropped nudge degrades to a poll of resolved state.
        mock_resolve.assert_awaited_once_with(
            cache,
            "ptc-1",
            "cos-1",
            "u-1",
            fencer_gen="g-PHANTOM",
            job_gen="g-OLD",
        )
        assert cache.client.publish.await_args.args[0] == thread_wake_key("cos-1")

    @pytest.mark.asyncio
    async def test_fenced_clear_without_fencer_gen_stays_silent(self):
        """Defensive: a falsy ClearOutcome with no fencing generation (Redis-
        less degradation shapes) has nothing sound to resolve against —
        never guess."""
        cache = _cache_with_origin(
            {"home_thread_id": "cos-1", "user_id": "u-1"},
        )
        mock_clear = AsyncMock(return_value=ClearOutcome(False, None))
        mock_resolve = AsyncMock()
        with _guard(), patch(
            "src.utils.cache.redis_cache.get_cache_client", return_value=cache
        ), patch(
            f"{POINTER_MOD}.clear_handoff_report_back",
            mock_clear,
        ), patch(
            f"{RESERVE_MOD}.resolve_orphaned_watch",
            mock_resolve,
        ):
            await core._exec_watch_clear(
                _job(
                    hook_type="watch_clear",
                    payload={
                        "ptc_thread_id": "ptc-1",
                        "user_id": "u-1",
                        "error_wake": True,
                        "dispatch_gen": "g-OLD",
                    },
                    ordering_key="cos-1",
                )
            )
        mock_resolve.assert_not_awaited()
        cache.client.publish.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_fenced_resolve_failure_propagates_for_nack(self):
        """The resolver must not decide on missing state: an unavailable
        Redis nacks the job for retry instead of guessing."""
        cache = _cache_with_origin(
            {"home_thread_id": "cos-1", "user_id": "u-1", "dispatch_gen": "g-NEW"},
        )
        mock_clear = AsyncMock(return_value=ClearOutcome(False, "g-NEW"))
        mock_resolve = AsyncMock(side_effect=ConnectionError("redis down"))
        with _guard(), patch(
            "src.utils.cache.redis_cache.get_cache_client", return_value=cache
        ), patch(
            f"{POINTER_MOD}.clear_handoff_report_back",
            mock_clear,
        ), patch(
            f"{RESERVE_MOD}.resolve_orphaned_watch",
            mock_resolve,
        ):
            with pytest.raises(ConnectionError):
                await core._exec_watch_clear(
                    _job(
                        hook_type="watch_clear",
                        payload={
                            "ptc_thread_id": "ptc-1",
                            "user_id": "u-1",
                            "error_wake": True,
                            "dispatch_gen": "g-OLD",
                        },
                        ordering_key="cos-1",
                    )
                )
        cache.client.publish.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_consumption_clear_publishes_cleared_wake(self):
        """A landed consumption clear pushes the pending→idle transition so
        watchers drop the chip now instead of riding the 60s backstop. No
        error wake — the payload says ``cleared``, not ``error``."""
        cache = _cache_with_origin({"home_thread_id": "cos-1", "user_id": "u-1"})
        mock_clear = AsyncMock(return_value=ClearOutcome(True))
        with _guard(), patch(
            "src.utils.cache.redis_cache.get_cache_client", return_value=cache
        ), patch(
            f"{POINTER_MOD}.clear_handoff_report_back",
            mock_clear,
        ):
            await core._exec_watch_clear(
                _job(
                    hook_type="watch_clear",
                    payload={"ptc_thread_id": "ptc-1", "user_id": "u-1", "error_wake": False},
                    ordering_key="cos-1",
                )
            )

        mock_clear.assert_awaited_once()
        cache.client.publish.assert_awaited_once()
        published = cache.client.publish.await_args.args[1]
        assert '"cleared": true' in published
        assert "error" not in published

    @pytest.mark.asyncio
    async def test_fenced_consumption_clear_owned_by_its_fencer_stays_silent(self):
        """Fenced by a generation the resolver finds admitted or live: the
        watcher is still legitimately pending, so no wake of any kind."""
        cache = _cache_with_origin({"home_thread_id": "cos-1", "user_id": "u-1"})
        mock_clear = AsyncMock(return_value=ClearOutcome(False, "g-NEW"))
        mock_resolve = AsyncMock(return_value=(False, None))
        with _guard(), patch(
            "src.utils.cache.redis_cache.get_cache_client", return_value=cache
        ), patch(
            f"{POINTER_MOD}.clear_handoff_report_back",
            mock_clear,
        ), patch(
            f"{RESERVE_MOD}.resolve_orphaned_watch",
            mock_resolve,
        ):
            await core._exec_watch_clear(
                _job(
                    hook_type="watch_clear",
                    payload={
                        "ptc_thread_id": "ptc-1",
                        "user_id": "u-1",
                        "error_wake": False,
                        "dispatch_gen": "g-OLD",
                    },
                    ordering_key="cos-1",
                )
            )

        mock_resolve.assert_awaited_once_with(
            cache, "ptc-1", "cos-1", "u-1", fencer_gen="g-NEW", job_gen="g-OLD"
        )
        cache.client.publish.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_fenced_consumption_clear_resolves_a_phantom_fencer(self):
        """A consumption clear fenced by a never-admitted reservation has no
        run left to drain the pair, so it resolves the phantom like a failure
        clear would. The summary landed, so the wake says ``cleared``."""
        cache = _cache_with_origin(
            {"home_thread_id": "cos-1", "user_id": "u-1", "dispatch_gen": "g-PHANTOM"},
        )
        mock_clear = AsyncMock(return_value=ClearOutcome(False, "g-PHANTOM"))
        mock_resolve = AsyncMock(return_value=(True, "rb-1"))
        with _guard(), patch(
            "src.utils.cache.redis_cache.get_cache_client", return_value=cache
        ), patch(
            f"{POINTER_MOD}.clear_handoff_report_back",
            mock_clear,
        ), patch(
            f"{RESERVE_MOD}.resolve_orphaned_watch",
            mock_resolve,
        ):
            await core._exec_watch_clear(
                _job(
                    hook_type="watch_clear",
                    payload={
                        "ptc_thread_id": "ptc-1",
                        "user_id": "u-1",
                        "error_wake": False,
                        "dispatch_gen": "g-OLD",
                    },
                    ordering_key="cos-1",
                )
            )

        mock_resolve.assert_awaited_once_with(
            cache, "ptc-1", "cos-1", "u-1", fencer_gen="g-PHANTOM", job_gen="g-OLD"
        )
        cache.client.publish.assert_awaited_once()
        published = cache.client.publish.await_args.args[1]
        assert '"cleared": true' in published
        assert "error" not in published

    @pytest.mark.asyncio
    async def test_noop_without_origin(self):
        """Ordinary run, or an already-cleared watch (idempotent retry)."""
        cache = _cache_with_origin(None)
        mock_clear = AsyncMock()
        with _guard(), patch(
            "src.utils.cache.redis_cache.get_cache_client", return_value=cache
        ), patch(
            f"{POINTER_MOD}.clear_handoff_report_back",
            mock_clear,
        ):
            await core._exec_watch_clear(
                _job(
                    hook_type="watch_clear",
                    payload={"ptc_thread_id": "cos-1", "user_id": "u-1", "error_wake": True},
                )
            )
        mock_clear.assert_not_called()
        cache.client.publish.assert_not_called()

    @pytest.mark.asyncio
    async def test_clear_failure_propagates_for_nack(self):
        """Inverted from the old best-effort hook: a failed clear must raise
        so the drainer nacks and retries — swallowing would ack a leaked
        watch + per-user cap slot."""
        cache = _cache_with_origin({"home_thread_id": "cos-1"})
        with _guard(), patch(
            "src.utils.cache.redis_cache.get_cache_client", return_value=cache
        ), patch(
            f"{POINTER_MOD}.clear_handoff_report_back",
            AsyncMock(side_effect=RuntimeError("redis down")),
        ):
            with pytest.raises(RuntimeError):
                await core._exec_watch_clear(
                    _job(
                        hook_type="watch_clear",
                        payload={"ptc_thread_id": "ptc-1", "error_wake": True},
                        ordering_key="cos-1",
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
                await core._exec_watch_clear(
                    _job(
                        hook_type="watch_clear",
                        payload={"ptc_thread_id": "ptc-1", "error_wake": True},
                    )
                )
            with pytest.raises(ConnectionError):
                await core._exec_needs_input_wake(
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
        from src.server.database.runs import outbox as outbox_db
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
            f"{EXECUTOR_MOD}.execute_report_back",
            AsyncMock(),
        ) as exec_rb:
            job = _job(hook_type="report_back", payload={"ptc_thread_id": "ptc-1"})
            await core._exec_handoff_report_back(job)
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
        from src.server.database.runs import outbox as outbox_db

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
        from src.server.database.runs import outbox as outbox_db

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
        from src.server.database.runs import outbox as outbox_db

        drainer = HookOutboxDrainer()
        with patch.object(outbox_db, "ack_outbox_job", AsyncMock()) as ack, patch.object(
            outbox_db, "nack_outbox_job", AsyncMock(return_value="dead")
        ) as nack:
            await drainer._execute(_job(hook_type="not_a_hook"))
        nack.assert_awaited_once()
        ack.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_a_steer_out_of_attempts_is_settled_not_dead_lettered(self):
        """The nack settles a steer at the ceiling per sender; the drainer
        runs its exhaust followup, never the dead-letter close."""
        from src.server.database.runs import outbox as outbox_db

        drainer = HookOutboxDrainer()
        job = _job(hook_type="steer", payload={"target": "task", "sender": "user"})
        closed, exhausted = AsyncMock(), AsyncMock()
        with patch.dict(
            hook_outbox._EXECUTORS, {"steer": AsyncMock(side_effect=RuntimeError("boom"))}
        ), patch.dict(hook_outbox._CLOSE_FOLLOWUPS, {"steer": closed}), patch.dict(
            hook_outbox._EXHAUST_FOLLOWUPS, {"steer": exhausted}
        ), patch.object(
            outbox_db, "nack_outbox_job", AsyncMock(return_value="exhausted")
        ), patch.object(hook_outbox.logger, "error") as error:
            await drainer._execute(job)
        exhausted.assert_awaited_once_with(job)
        closed.assert_not_awaited()
        error.assert_not_called()

    @pytest.mark.asyncio
    async def test_a_market_alert_out_of_attempts_is_held_not_dead_lettered(self):
        """The nack holds an alert at the ceiling for its thread's next turn;
        the drainer logs it as settled, never as a dead letter."""
        from src.server.database.runs import outbox as outbox_db

        drainer = HookOutboxDrainer()
        job = {**_job(hook_type="market_target"), "attempts": hook_outbox.MAX_ATTEMPTS}
        with patch.dict(
            hook_outbox._EXECUTORS,
            {"market_target": AsyncMock(side_effect=RuntimeError("boom"))},
        ), patch.object(
            outbox_db, "nack_outbox_job", AsyncMock(return_value="exhausted")
        ), patch.object(hook_outbox.logger, "error") as error, patch.object(
            drainer, "_close_followup", AsyncMock()
        ) as closed:
            await drainer._execute(job)
        closed.assert_not_awaited()
        error.assert_not_called()

    def test_the_steer_executor_registers_its_exhaust_followup(self):
        from src.server.services.report_back import steer_executor

        steer_executor.register_outbox_executors()
        assert hook_outbox._EXHAUST_FOLLOWUPS["steer"] is steer_executor._on_exhausted

    # Dead-letter compensation is inserted ATOMICALLY inside
    # nack_outbox_job / park_exhausted_jobs (data-modifying CTE) — there is
    # deliberately no drainer-level compensation step to test; the SQL-level
    # contract is exercised by the live claim-gate verification script.


# ---------------------------------------------------------------------------
# Executor registry: handlers register at startup; the outbox never imports
# them. Collisions are wiring bugs; re-registering the same function (reload,
# repeated setup) is a no-op.
# ---------------------------------------------------------------------------


class TestExecutorRegistry:
    def test_same_function_is_idempotent(self):
        core.register_outbox_executors()
        core.register_outbox_executors()
        assert (
            hook_outbox._EXECUTORS["watch_clear"]
            is core._exec_watch_clear
        )

    def test_different_executor_for_registered_type_raises(self):
        async def rogue(job):
            pass

        with pytest.raises(ValueError, match="report_back"):
            hook_outbox.register_hook_executor("report_back", rogue)

    @pytest.mark.asyncio
    async def test_ack_followup_runs_after_successful_ack(self):
        """task_report_back's post-ack cleared wake: the watch is push-driven,
        so the reconcile signal must land AFTER the row closes."""
        from src.server.database.runs import outbox as outbox_db

        drainer = HookOutboxDrainer()
        job = _job(hook_type="task_report_back", payload={"thread_id": "t-1"})
        wake_cache = MagicMock()
        with patch.dict(
            hook_outbox._EXECUTORS, {"task_report_back": AsyncMock()}
        ), patch.object(
            outbox_db, "ack_outbox_job", AsyncMock()
        ) as ack, patch(
            f"{WAKE_MOD}.publish_wake", AsyncMock()
        ) as wake, patch(
            "src.utils.cache.redis_cache.get_cache_client",
            return_value=wake_cache,
        ):
            await drainer._execute(job)
        ack.assert_awaited_once()
        wake.assert_awaited_once_with(wake_cache, "t-1", cleared=True)

    @pytest.mark.asyncio
    async def test_ack_followup_failure_never_unacks(self):
        from src.server.database.runs import outbox as outbox_db

        drainer = HookOutboxDrainer()
        job = _job(hook_type="task_report_back", payload={"thread_id": "t-1"})
        with patch.dict(
            hook_outbox._EXECUTORS, {"task_report_back": AsyncMock()}
        ), patch.object(
            outbox_db, "ack_outbox_job", AsyncMock()
        ) as ack, patch.object(
            outbox_db, "nack_outbox_job", AsyncMock()
        ) as nack, patch.dict(
            hook_outbox._CLOSE_FOLLOWUPS,
            {"task_report_back": AsyncMock(side_effect=RuntimeError("boom"))},
        ):
            await drainer._execute(job)
        ack.assert_awaited_once()
        nack.assert_not_awaited()

    @pytest.mark.asyncio
    @pytest.mark.parametrize(("nacked_to", "followed_up"), [("dead", True), ("pending", False)])
    async def test_close_followup_runs_when_a_job_dies(self, nacked_to, followed_up):
        """A dead row closes as surely as an acked one: the watch reading it
        as pending has no other signal until it recycles."""
        from src.server.database.runs import outbox as outbox_db

        drainer = HookOutboxDrainer()
        job = _job(hook_type="task_report_back", payload={"thread_id": "t-1"})
        followup = AsyncMock()
        with patch.dict(
            hook_outbox._EXECUTORS,
            {"task_report_back": AsyncMock(side_effect=RuntimeError("boom"))},
        ), patch.object(
            outbox_db, "nack_outbox_job", AsyncMock(return_value=nacked_to)
        ), patch.dict(
            hook_outbox._CLOSE_FOLLOWUPS, {"task_report_back": followup}
        ):
            await drainer._execute(job)
        assert followup.await_count == (1 if followed_up else 0)



class TestDrainerQuotas:
    @pytest.mark.asyncio
    async def test_a_burst_on_an_idle_drainer_stays_inside_its_quota(self):
        """The burst a quota exists for arrives while nothing is in flight, so
        the first claim must not be able to fill every slot with one type."""
        import asyncio

        from src.server.database.runs import outbox as outbox_db

        due = [_job(hook_type="market_target") for _ in range(12)]
        due += [_job(hook_type="steer") for _ in range(4)]

        async def claim(limit, *, excluded_hook_types=None, **_):
            skip = set(excluded_hook_types or [])
            picked = [j for j in due if j["hook_type"] not in skip][:limit]
            for job in picked:
                due.remove(job)
            return picked

        drainer = HookOutboxDrainer()
        hold = asyncio.Event()

        async def execute(job):
            await hold.wait()

        async def stop_waiting():
            drainer._stopping.set()

        with patch.object(
            outbox_db, "park_exhausted_jobs", AsyncMock(return_value=[])
        ), patch.object(
            outbox_db, "revive_dead_cleanup_jobs", AsyncMock(return_value=[])
        ), patch.object(
            outbox_db, "revive_dead_alerts", AsyncMock(return_value=[])
        ), patch.object(
            outbox_db, "close_unowed_waiting_alerts", AsyncMock(return_value=[])
        ), patch.object(
            outbox_db, "purge_terminal_jobs", AsyncMock(return_value=0)
        ), patch.object(
            outbox_db, "claim_outbox_jobs", side_effect=claim
        ), patch.object(drainer, "_execute", side_effect=execute), patch.object(
            drainer, "_wait_for_nudge", side_effect=stop_waiting
        ):
            await drainer._loop()
            running = list(drainer._inflight.values())
            hold.set()
            await asyncio.gather(*drainer._inflight)

        assert running.count("market_target") == hook_outbox.MARKET_TARGET_INFLIGHT_QUOTA
        assert running.count("steer") == 4


class TestAlertSweeps:
    """Market-target alerts: an older build's dead ones made due again,
    untakeable ones closed, each on its own drainer clock rather than every
    pass."""

    @staticmethod
    def _sweeps(**overrides):
        from src.server.database.runs import outbox as outbox_db

        mocks = {
            "park_exhausted_jobs": AsyncMock(return_value=[]),
            "revive_dead_cleanup_jobs": AsyncMock(return_value=[]),
            "revive_dead_alerts": AsyncMock(return_value=[]),
            "close_unowed_waiting_alerts": AsyncMock(return_value=[]),
            "purge_terminal_jobs": AsyncMock(return_value=0),
            "claim_outbox_jobs": AsyncMock(return_value=[]),
            **overrides,
        }
        return mocks, [patch.object(outbox_db, name, m) for name, m in mocks.items()]

    @staticmethod
    async def _passes(drainer, n: int):
        left = [n]

        async def wait():
            left[0] -= 1
            if left[0] <= 0:
                drainer._stopping.set()

        with patch.object(drainer, "_wait_for_nudge", side_effect=wait):
            await drainer._loop()

    @pytest.mark.asyncio
    async def test_both_sweeps_run_once_per_interval(self):
        import contextlib

        drainer = HookOutboxDrainer()
        mocks, patches = self._sweeps(
            revive_dead_alerts=AsyncMock(
                return_value=[{"hook_outbox_id": "a-1", "ordering_key": "k"}]
            ),
            close_unowed_waiting_alerts=AsyncMock(
                return_value=[{"hook_outbox_id": "a-2", "ordering_key": "k"}]
            ),
        )
        with contextlib.ExitStack() as stack:
            for p in patches:
                stack.enter_context(p)
            warning = stack.enter_context(patch.object(hook_outbox.logger, "warning"))
            await self._passes(drainer, 3)
        assert mocks["park_exhausted_jobs"].await_count == 3
        mocks["revive_dead_alerts"].assert_awaited_once()
        mocks["close_unowed_waiting_alerts"].assert_awaited_once()
        assert any("a-1" in str(c.args[0]) for c in warning.call_args_list)

    @pytest.mark.asyncio
    async def test_the_close_runs_on_a_shorter_clock_than_the_revive(self):
        """A dead alert must stay dead ten minutes before it qualifies, so
        the revive runs every ten minutes; the close keeps its minute."""
        import contextlib

        drainer = HookOutboxDrainer()
        mocks, patches = self._sweeps()
        passes = [0]

        async def wait():
            passes[0] += 1
            if passes[0] == 1:
                # A minute on: the close is due again, the revive is not.
                drainer._next_alert_close = 0.0
            else:
                drainer._stopping.set()

        with contextlib.ExitStack() as stack:
            for p in patches:
                stack.enter_context(p)
            stack.enter_context(patch.object(drainer, "_wait_for_nudge", side_effect=wait))
            await drainer._loop()
        assert mocks["close_unowed_waiting_alerts"].await_count == 2
        mocks["revive_dead_alerts"].assert_awaited_once()
        assert hook_outbox.ALERT_CLOSE_INTERVAL == 60.0
        assert hook_outbox.ALERT_REVIVE_INTERVAL == 600.0
        assert drainer._next_alert_revive - drainer._next_alert_close == pytest.approx(
            hook_outbox.ALERT_REVIVE_INTERVAL - hook_outbox.ALERT_CLOSE_INTERVAL, abs=5
        )

    @pytest.mark.asyncio
    async def test_a_failed_revive_does_not_skip_the_close(self):
        import contextlib

        drainer = HookOutboxDrainer()
        mocks, patches = self._sweeps(
            revive_dead_alerts=AsyncMock(side_effect=RuntimeError("db down"))
        )
        with contextlib.ExitStack() as stack:
            for p in patches:
                stack.enter_context(p)
            error = stack.enter_context(patch.object(hook_outbox.logger, "error"))
            await self._passes(drainer, 1)
        mocks["close_unowed_waiting_alerts"].assert_awaited_once()
        error.assert_called_once()

    @pytest.mark.asyncio
    async def test_an_alert_the_park_sweep_holds_is_not_dead_lettered(self):
        import contextlib

        drainer = HookOutboxDrainer()
        held = {
            "hook_outbox_id": "a-1",
            "hook_type": "market_target",
            "status": "pending",
            "attempts": hook_outbox.MAX_ATTEMPTS,
            "payload": {"exhausted": True},
        }
        _, patches = self._sweeps(park_exhausted_jobs=AsyncMock(return_value=[held]))
        with contextlib.ExitStack() as stack:
            for p in patches:
                stack.enter_context(p)
            error = stack.enter_context(patch.object(hook_outbox.logger, "error"))
            exhausted = stack.enter_context(
                patch.object(drainer, "_exhausted", AsyncMock())
            )
            closed = stack.enter_context(
                patch.object(drainer, "_close_followup", AsyncMock())
            )
            await self._passes(drainer, 1)
        exhausted.assert_awaited_once_with(held)
        closed.assert_not_awaited()
        error.assert_not_called()
