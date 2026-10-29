"""Hook-job decision table — what a settled run owes, as pure data.

``build_finalize_jobs`` is the one mapping from a run's CAS-adopted final
status to its post-commit effects; ``outbox.py`` writes the rows it returns on
the finalize transaction. Deliberately pool-free and I/O-free: the table runs
INSIDE that transaction, and keeping it importable without the DB pool is what
makes it testable as data.
"""

from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Mapping, NotRequired, Optional, TypedDict

from src.server.contracts.status import to_public
from src.server.services.report_back.handoff.keys import ORIGIN_THREAD_META



@dataclass
class HookJob:
    """One outbox row: a post-commit effect that must survive a crash."""

    hook_type: str
    idempotency_key: str
    payload: Dict[str, Any] = field(default_factory=dict)
    ordering_key: Optional[str] = None


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


#: Run metadata key for how a wake turn ended quietly: {"changed"}.
WAKEUP_QUIET_META = "wakeup_quiet"


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
        return jobs

    return _jobs


def build_finalize_jobs_from_run_row(
    run: Dict[str, Any],
    *,
    last_seen_run_seq: Optional[int] = None,
) -> Callable[[str], List[HookJob]]:
    """Factory from the durable row alone: everything the decision table
    needs was stamped into run metadata at START. This is finalize_run's
    DEFAULT: no finalize path can skip required terminal effects (I5).
    ``last_seen_run_seq`` is the seen pointer the finalize itself carried."""
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
    )
