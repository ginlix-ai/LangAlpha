"""Hook-job decision table — what a settled run owes, as pure data.

``build_finalize_jobs`` is the one mapping from a run's CAS-adopted final
status to its post-commit effects; ``outbox.py`` writes the rows it returns on
the finalize transaction. Deliberately pool-free and I/O-free: the table runs
INSIDE that transaction, and keeping it importable without the DB pool is what
makes it testable as data.
"""

import uuid
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Mapping, NotRequired, Optional, TypedDict

from src.config import env
from src.server.contracts.status import is_restart_interrupted, to_public
from src.server.services.report_back.handoff.keys import ORIGIN_THREAD_META


@dataclass
class HookJob:
    """One outbox row: a post-commit effect that must survive a crash."""

    hook_type: str
    idempotency_key: str
    payload: Dict[str, Any] = field(default_factory=dict)
    ordering_key: Optional[str] = None
    #: Seconds before the row is first due; None is due at once.
    delay_seconds: Optional[float] = None


# Payload contracts between the decision table below and the executors in
# services.hook_outbox. Rows round-trip through JSONB, so these are the
# documented shape, not a runtime guarantee.


class BurstReleasePayload(TypedDict):
    user_id: str
    slot_id: str


class ReportBackPayload(TypedDict):
    ptc_thread_id: str
    dispatch_gen: Optional[str]
    # completed, error or cancelled. Absent on rows enqueued before the field
    # existed, when only completed runs were enqueued.
    final_status: NotRequired[str]


class NeedsInputWakePayload(TypedDict):
    ptc_thread_id: str


class WatchClearPayload(TypedDict):
    ptc_thread_id: str
    user_id: Optional[str]
    error_wake: bool
    dispatch_gen: Optional[str]


class AutomationSettlePayload(TypedDict):
    execution_id: str
    automation_id: str
    user_id: Optional[str]
    workspace_id: Optional[str]


#: A completed turn's outbox job arming its thread's idle compaction
#: (``services.idle_compaction``).
IDLE_COMPACT_HOOK = "idle_compact"


class IdleCompactPayload(TypedDict):
    thread_id: str
    run_id: str
    user_id: str
    workspace_id: Optional[str]


#: Work a graceful restart stopped on a thread, for the next process to resume
#: (``services.report_back.restart``).
RESTART_RESUME_HOOK = "restart_resume"

#: What a turn a restart stopped would owe as an ordinary stop, held for the
#: turn that takes its work over: the watching thread's report, a summary's
#: release and the automation firing's settle. A resume that will not come
#: writes them after all (``restart_held_jobs``).
RESTART_HELD_HOOKS = frozenset({"report_back", "watch_clear", "automation_settle"})


class RestartResumePayload(TypedDict):
    # The thread's latest run when the restart stopped its work: the turn it
    # interrupted, or the one before the subagents it stopped.
    run_id: str


def restart_resume_job(*, run_id: str, thread_id: str) -> HookJob:
    """One job per run, whichever of the run's finalize and the stop of a
    subagent it left running writes it first. Off the thread's report chain:
    the reports a restart left parked wait for the turn that resumes, which
    takes them in, so they must not hold its job back."""
    return HookJob(
        hook_type=RESTART_RESUME_HOOK,
        idempotency_key=f"{run_id}:{RESTART_RESUME_HOOK}",
        payload=RestartResumePayload(run_id=run_id),
        ordering_key=f"{RESTART_RESUME_HOOK}:{thread_id}",
    )


#: A firing handed to a process that is not draining, which starts it on the
#: thread its execution row records (``automation_executor.start_handed_off``).
AUTOMATION_HANDOFF_HOOK = "automation_handoff"


def automation_handoff_job(
    *, execution_id: str, automation_id: str, user_id: str
) -> HookJob:
    """A firing is handed on again each time a process drains before it
    starts, so each hand-off is a job of its own."""
    return HookJob(
        hook_type=AUTOMATION_HANDOFF_HOOK,
        idempotency_key=f"{execution_id}:{AUTOMATION_HANDOFF_HOOK}:{uuid.uuid4()}",
        payload={
            "execution_id": execution_id,
            "automation_id": automation_id,
            "user_id": user_id,
        },
    )


class UserFeedPayload(TypedDict):
    user_id: str
    thread_id: str
    workspace_id: Optional[str]
    run_id: str
    run_seq: Optional[int]
    status: str  # PUBLIC vocabulary (to_public of the CAS-adopted status)
    interrupt_reason: Optional[str]
    # Where the thread's seen pointer stands after this settle carried it,
    # over a wakeup's check that ended quietly with nothing changed. Absent
    # when the settle left it.
    last_seen_run_seq: NotRequired[int]


class SettleNewsPayload(TypedDict):
    """A settle the user may hear of. Whether they do, and what it says, the
    executor decides from the run row and the thread
    (``services/notices.settle_news``)."""

    user_id: str
    thread_id: str
    workspace_id: Optional[str]
    run_id: str
    run_seq: Optional[int]
    status: str  # the CAS-adopted run status: completed, error or interrupted
    interrupt_reason: Optional[str]


#: The news of a settle for the user's feed, which the desktop app notifies
#: from. Its own job, not the push's: a slow or failing APNs must not hold a
#: desktop notice back behind the push's quota and per-thread chain.
USER_NOTIFICATION_HOOK = "user_notification"

#: How long a finished turn's news waits for a client to mark the thread
#: seen, so a turn watched on the web or another phone does not buzz this one
#: or show on the desktop.
#: An interrupted run cannot be marked seen, so its news goes at once.
SETTLE_NEWS_GRACE_SECONDS = 8.0


#: Run metadata key for where an attempt ran ({"platform", ...}), stamped at
#: START by ``request_prep.surface_stamp``.
SURFACE_META = "surface"

#: Run metadata key for how a wake turn ended quietly: {"changed"}.
WAKEUP_QUIET_META = "wakeup_quiet"

#: Run metadata key, true when the turn ran for a user with the
#: ``idle_compaction`` feature on; stamped by the finalize.
IDLE_COMPACTION_META = "idle_compaction"


def is_unchanged_quiet(meta: Mapping[str, Any]) -> bool:
    """Whether the run was a wakeup's check that ended quietly and set or
    cancelled nothing: news to nobody, so it leaves the thread as seen as it
    was. A check that changed the plan is shown like any turn."""
    quiet = meta.get(WAKEUP_QUIET_META)
    return isinstance(quiet, dict) and quiet.get("changed") is False


def build_finalize_jobs(
    *,
    run_id: str,
    thread_id: str,
    user_id: Optional[str] = None,
    burst_slot_id: Optional[str] = None,
    report_back_ptc_thread_id: Optional[str] = None,
    dispatcher_thread_id: Optional[str] = None,
    origin_dispatch_gen: Optional[str] = None,
    workspace_id: Optional[str] = None,
    run_seq: Optional[int] = None,
    interrupt_reason: Optional[str] = None,
    automation_execution_id: Optional[str] = None,
    automation_id: Optional[str] = None,
    superseded: bool = False,
    last_seen_run_seq: Optional[int] = None,
    push_enabled: bool = False,
    restart_interrupted: bool = False,
    idle_compaction: bool = False,
) -> Callable[[str], List[HookJob]]:
    """The one decision table mapping a run's final status to its hook jobs.

    Returned callable is invoked inside the finalize transaction with the
    CAS-adopted final status (a durable cancel may have overridden the
    requested one). One ordering rule: every report-back-lifecycle job keys
    on the WATCHING Chief of Staff thread (``dispatcher_thread_id``, from
    the START-stamped ``ORIGIN_THREAD_META``; a report-back run IS that
    thread, so its own ``thread_id`` lands in the same chain): all
    completions reporting into one Chief of Staff thread serialize strictly,
    across any number of drainer workers, and a completed run's report_back
    can never be overtaken by a later watch_clear. Runs without a dispatching
    thread fall back to their own thread_id. Pure and synchronous — it runs
    inside the finalize transaction and must not do I/O.
    """

    def _jobs(final_status: str) -> List[HookJob]:
        jobs: List[HookJob] = []
        okey = dispatcher_thread_id or thread_id

        if user_id:
            # Every settle owed to a known owner publishes to their feed —
            # at-least-once via the outbox, so a crash between commit and
            # publish can't strand the sidebar on a stale spinner. No
            # ordering_key: the client's seq-aware merge tolerates reorder.
            # Deliberately NO dead-letter compensation or revive (cf. the
            # report_back CTE in outbox.py): the event is latency, not truth
            # — the feed endpoint re-reads DB truth on a 30s cadence inside
            # every live connection, which outruns any outbox revival path.
            feed = UserFeedPayload(
                user_id=user_id,
                thread_id=thread_id,
                workspace_id=workspace_id,
                run_id=run_id,
                run_seq=run_seq,
                status=to_public(final_status),
                interrupt_reason=(
                    interrupt_reason if final_status == "interrupted" else None
                ),
            )
            if last_seen_run_seq is not None:
                feed["last_seen_run_seq"] = last_seen_run_seq
            jobs.append(
                HookJob(
                    hook_type="user_feed",
                    idempotency_key=f"{run_id}:user_feed",
                    payload=feed,
                )
            )

        if user_id and not superseded and final_status != "cancelled":
            # A stop, or a run that was never the firing's turn, is news to
            # nobody. Which other settles are news is the executors' call
            # (``services/notices.settle_news``), from the run row.
            for hook in (USER_NOTIFICATION_HOOK, *(["push"] if push_enabled else [])):
                jobs.append(
                    HookJob(
                        hook_type=hook,
                        idempotency_key=f"{run_id}:{hook}",
                        payload=SettleNewsPayload(
                            user_id=user_id,
                            thread_id=thread_id,
                            workspace_id=workspace_id,
                            run_id=run_id,
                            run_seq=run_seq,
                            status=final_status,
                            interrupt_reason=(
                                interrupt_reason
                                if final_status == "interrupted"
                                else None
                            ),
                        ),
                        delay_seconds=(
                            None
                            if final_status == "interrupted"
                            else SETTLE_NEWS_GRACE_SECONDS
                        ),
                        # A thread's news reaches the feed, and APNs, in run
                        # order, so an older one sent slowly can't land after,
                        # and replace, a newer. Per hook, so a slow APNs holds
                        # back only the push.
                        ordering_key=f"{hook}:{thread_id}",
                    )
                )

        if user_id and final_status == "completed" and idle_compaction:
            # A completed turn may leave its thread idle on a warm prompt
            # cache, which an idle compaction summarizes before it expires.
            # Whether it does is read off the checkpoint the turn committed,
            # so the job runs after the commit. No ordering_key: the executor
            # arms only for the thread's latest run. With the feature off for
            # the turn's user no job is written, so a turn costs nothing.
            jobs.append(
                HookJob(
                    hook_type=IDLE_COMPACT_HOOK,
                    idempotency_key=f"{run_id}:{IDLE_COMPACT_HOOK}",
                    payload=IdleCompactPayload(
                        thread_id=thread_id,
                        run_id=run_id,
                        user_id=user_id,
                        workspace_id=workspace_id,
                    ),
                )
            )

        if automation_execution_id and automation_id and not superseded:
            # The firing that ran this turn ends as the run did. A run a
            # concurrent one superseded at admission was never its turn: the
            # firing is back in line for the thread instead.
            jobs.append(
                HookJob(
                    hook_type="automation_settle",
                    idempotency_key=f"{run_id}:automation_settle",
                    payload=AutomationSettlePayload(
                        execution_id=automation_execution_id,
                        automation_id=automation_id,
                        user_id=user_id,
                        workspace_id=workspace_id,
                    ),
                )
            )

        if user_id and burst_slot_id:
            jobs.append(
                HookJob(
                    hook_type="burst_release",
                    idempotency_key=f"{run_id}:burst_release",
                    payload=BurstReleasePayload(
                        user_id=user_id, slot_id=burst_slot_id
                    ),
                )
            )

        if report_back_ptc_thread_id:
            # A report-back summary run is the delivery itself, never a run
            # that owes one: once it ends it only releases its pair, and a
            # failed or stopped one wakes the watcher so the card reconciles.
            # An interrupted summary is waiting on the user, not over: the
            # next turn on its thread takes the pair over and owes the
            # release instead (see ``carried_pair``).
            if final_status != "interrupted":
                jobs.append(
                    HookJob(
                        hook_type="watch_clear",
                        idempotency_key=f"{run_id}:watch_clear",
                        payload=WatchClearPayload(
                            ptc_thread_id=report_back_ptc_thread_id,
                            user_id=user_id,
                            error_wake=final_status != "completed",
                            dispatch_gen=origin_dispatch_gen,
                        ),
                        ordering_key=okey,
                    )
                )
        elif final_status == "interrupted":
            jobs.append(
                HookJob(
                    hook_type="needs_input_wake",
                    idempotency_key=f"{run_id}:needs_input_wake",
                    payload=NeedsInputWakePayload(ptc_thread_id=thread_id),
                    ordering_key=okey,
                )
            )
        else:
            # A stop or a failure is reported like a result: the watching
            # thread told the user a report was coming, and a card alone
            # would leave that promise to be found by scrolling.
            jobs.append(
                HookJob(
                    hook_type="report_back",
                    idempotency_key=f"{run_id}:report_back",
                    payload=ReportBackPayload(
                        ptc_thread_id=thread_id,
                        dispatch_gen=origin_dispatch_gen,
                        final_status=final_status,
                    ),
                    ordering_key=okey,
                )
            )
        if restart_interrupted and final_status == "cancelled":
            # A restart's stop is not the end of the work: the turn that takes
            # it over owes what it holds (``carried_pair``, ``carried_firing``,
            # ``get_continued_runs``).
            jobs = [job for job in jobs if job.hook_type not in RESTART_HELD_HOOKS]
            jobs.append(restart_resume_job(run_id=run_id, thread_id=thread_id))
        return jobs

    return _jobs


def build_finalize_jobs_from_run_row(
    run: Dict[str, Any],
    *,
    last_seen_run_seq: Optional[int] = None,
    as_ordinary_stop: bool = False,
) -> Callable[[str], List[HookJob]]:
    """Factory from the durable row alone: everything the decision table
    needs was stamped into run metadata at START. This is finalize_run's
    DEFAULT: no finalize path can skip required terminal effects (I5).
    ``last_seen_run_seq`` is the seen pointer the finalize itself carried.
    ``as_ordinary_stop`` reads a run a restart stopped as any other stop."""
    meta = run.get("metadata") or {}
    return build_finalize_jobs(
        run_id=str(run["conversation_response_id"]),
        thread_id=str(run["conversation_thread_id"]),
        user_id=meta.get("user_id"),
        burst_slot_id=meta.get("burst_slot_id"),
        report_back_ptc_thread_id=meta.get("report_back_ptc_thread_id"),
        dispatcher_thread_id=meta.get(ORIGIN_THREAD_META),
        origin_dispatch_gen=meta.get("origin_dispatch_gen"),
        workspace_id=meta.get("workspace_id"),
        run_seq=run.get("run_seq"),
        # The post-CAS row's column: NULL unless the CAS adopted 'interrupted'.
        interrupt_reason=run.get("interrupt_reason"),
        automation_execution_id=meta.get("automation_execution_id"),
        automation_id=meta.get("automation_id"),
        # Stamped by the finalize itself, which the post-CAS row carries.
        superseded=bool(meta.get("superseded")),
        last_seen_run_seq=last_seen_run_seq,
        push_enabled=env.APNS_ENABLED,
        restart_interrupted=not as_ordinary_stop and is_restart_interrupted(run),
        idle_compaction=meta.get(IDLE_COMPACTION_META) is True,
    )


def restart_held_jobs(run: Dict[str, Any]) -> List[HookJob]:
    """What the finalize of ``run``, a turn a restart stopped, held back for
    the turn that takes it over (``RESTART_HELD_HOOKS``), as an ordinary
    stop writes it, for a resume that will not come."""
    return [
        job
        for job in build_finalize_jobs_from_run_row(run, as_ordinary_stop=True)("cancelled")
        if job.hook_type in RESTART_HELD_HOOKS
    ]
