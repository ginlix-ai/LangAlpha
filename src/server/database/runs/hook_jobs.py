"""Hook-job decision table — what a settled run owes, as pure data.

``build_finalize_jobs`` is the one mapping from a run's CAS-adopted final
status to its post-commit effects; ``outbox.py`` writes the rows it returns on
the finalize transaction. Deliberately pool-free and I/O-free: the table runs
INSIDE that transaction, and keeping it importable without the DB pool is what
makes it testable as data.
"""

from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional, TypedDict

from src.server.contracts.status import to_public



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


class NeedsInputWakePayload(TypedDict):
    ptc_thread_id: str


class WatchClearPayload(TypedDict):
    ptc_thread_id: str
    user_id: Optional[str]
    error_wake: bool
    dispatch_gen: Optional[str]


class UserFeedPayload(TypedDict):
    user_id: str
    thread_id: str
    workspace_id: Optional[str]
    run_id: str
    run_seq: Optional[int]
    status: str  # PUBLIC vocabulary (to_public of the CAS-adopted status)
    interrupt_reason: Optional[str]


def build_finalize_jobs(
    *,
    run_id: str,
    thread_id: str,
    msg_type: str,
    user_id: Optional[str] = None,
    burst_slot_id: Optional[str] = None,
    report_back_ptc_thread_id: Optional[str] = None,
    origin_flash_thread_id: Optional[str] = None,
    origin_dispatch_gen: Optional[str] = None,
    workspace_id: Optional[str] = None,
    run_seq: Optional[int] = None,
    interrupt_reason: Optional[str] = None,
) -> Callable[[str], List[HookJob]]:
    """The one decision table mapping a run's final status to its hook jobs.

    Returned callable is invoked inside the finalize transaction with the
    CAS-adopted final status (a durable cancel may have overridden the
    requested one). One ordering rule: every report-back-lifecycle job keys
    on the WATCHING flash thread (START-stamped ``origin_flash_thread_id``;
    a report-back flash run IS that thread, so its own ``thread_id`` lands
    in the same chain) — all completions reporting into one flash thread
    serialize strictly, across any number of drainer workers, and a
    completed run's report_back can never be overtaken by a later
    watch_clear. Runs without a flash origin fall back to their own
    thread_id. Pure and synchronous — it runs inside the finalize
    transaction and must not do I/O.
    """

    def _jobs(final_status: str) -> List[HookJob]:
        jobs: List[HookJob] = []
        is_ptc = msg_type == "ptc"
        rb_ptc = report_back_ptc_thread_id or thread_id
        okey = origin_flash_thread_id or thread_id

        if user_id:
            # Every settle owed to a known owner publishes to their feed —
            # at-least-once via the outbox, so a crash between commit and
            # publish can't strand the sidebar on a stale spinner. No
            # ordering_key: the client's seq-aware merge tolerates reorder.
            # Deliberately NO dead-letter compensation or revive (cf. the
            # report_back CTE in outbox.py): the event is latency, not truth
            # — the feed endpoint re-reads DB truth on a 30s cadence inside
            # every live connection, which outruns any outbox revival path.
            jobs.append(
                HookJob(
                    hook_type="user_feed",
                    idempotency_key=f"{run_id}:user_feed",
                    payload=UserFeedPayload(
                        user_id=user_id,
                        thread_id=thread_id,
                        workspace_id=workspace_id,
                        run_id=run_id,
                        run_seq=run_seq,
                        status=to_public(final_status),
                        interrupt_reason=(
                            interrupt_reason
                            if final_status == "interrupted"
                            else None
                        ),
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

        if final_status == "completed" and is_ptc:
            jobs.append(
                HookJob(
                    hook_type="report_back",
                    idempotency_key=f"{run_id}:report_back",
                    payload=ReportBackPayload(
                        ptc_thread_id=thread_id,
                        dispatch_gen=origin_dispatch_gen,
                    ),
                    ordering_key=okey,
                )
            )
        elif final_status == "interrupted" and is_ptc:
            jobs.append(
                HookJob(
                    hook_type="needs_input_wake",
                    idempotency_key=f"{run_id}:needs_input_wake",
                    payload=NeedsInputWakePayload(ptc_thread_id=thread_id),
                    ordering_key=okey,
                )
            )

        if final_status in ("error", "cancelled") or (
            final_status == "completed"
            and not is_ptc
            and report_back_ptc_thread_id
        ):
            # error/cancelled: tear down any watch this run held open (a
            # dispatched PTC directly, a report-back flash run via its
            # origin id). completed flash WITH an origin id: consumption
            # clear — the report-back summary landed, release the watch.
            jobs.append(
                HookJob(
                    hook_type="watch_clear",
                    idempotency_key=f"{run_id}:watch_clear",
                    payload=WatchClearPayload(
                        ptc_thread_id=rb_ptc,
                        user_id=user_id,
                        error_wake=final_status in ("error", "cancelled"),
                        dispatch_gen=origin_dispatch_gen,
                    ),
                    ordering_key=okey,
                )
            )
        return jobs

    return _jobs


def build_finalize_jobs_from_run_row(
    run: Dict[str, Any],
) -> Callable[[str], List[HookJob]]:
    """Factory from the durable row alone: everything the decision table
    needs was stamped into run metadata at START. This is finalize_run's
    DEFAULT — no finalize path can skip required terminal effects (I5)."""
    meta = run.get("metadata") or {}
    return build_finalize_jobs(
        run_id=str(run["conversation_response_id"]),
        thread_id=str(run["conversation_thread_id"]),
        msg_type=meta.get("msg_type") or "ptc",
        user_id=meta.get("user_id"),
        burst_slot_id=meta.get("burst_slot_id"),
        report_back_ptc_thread_id=meta.get("report_back_ptc_thread_id"),
        origin_flash_thread_id=meta.get("origin_flash_thread_id"),
        origin_dispatch_gen=meta.get("origin_dispatch_gen"),
        workspace_id=meta.get("workspace_id"),
        run_seq=run.get("run_seq"),
        # The post-CAS row's column: NULL unless the CAS adopted 'interrupted'.
        interrupt_reason=run.get("interrupt_reason"),
    )
