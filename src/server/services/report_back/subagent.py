"""PTC←background-subagent report-backs, mirroring Chief of Staff←PTC hand-offs.

A subagent run that reaches a reportable outcome (``REPORT_BACK_STATUSES``)
bears a ``task_report_back`` outbox job in the same transaction as its
terminal CAS (``subagent_runs.finalize_task_run``) — the enqueue commits
with the CAS or not at all. Whether a notification is actually *owed* is
the EXECUTOR's decision at claim time, against the ledger: a run whose
``result_delivered_at`` is stamped (TaskOutput delivered it) is dropped
with a cleared wake; a live or interrupted parent parks the job until the
thread's next dead-turn finalize (completed, error or cancelled) releases
it, and a live turn may close it first by taking the report in as a steer
(``report_back.steer``). A run that had already settled when the user
stopped the turn that launched it drops too (``stop_already_covered``),
since the whole point of a stop is that no new turn starts. Both paths
decide through ``task_report_verdict``. The POST is a synthetic
notification turn via the shared ``notify_turn`` pipeline: it announces
the outcome and leaves TaskOutput to fetch the result from the durable
archive, so nothing volatile rides the payload.

Unlike the hand-off pipeline there is no Redis reserve state: the open outbox
row IS the pending-registry, owed from enqueue until the notification run
it dispatched ends (/status reads it via ``read_task_report_back_status``).

Delivery semantics are AT-LEAST-ONCE, never exactly-once: completed and
error runs enqueue (cancelled/interrupted never notify — cancellation is
intentful and carries its own UX). TaskOutput deliveries stamp
``result_delivered_at`` (stamp failures are swallowed in favor of a
duplicate over a lost notification); this executor never stamps — its
dedup is the claim-time drop against the stamp plus the per-run
idempotent job identity. Any change to this pipeline must preserve that
posture — the ledger stamp is the dedup, process memory never is.
"""

import logging
import uuid
from dataclasses import dataclass, replace
from typing import TYPE_CHECKING, Optional

from src.server.services.report_back.notify_turn import (
    NotOwed,
    ReportPipeline,
    run_report_back,
    stop_already_covered,
)

if TYPE_CHECKING:
    from src.server.models.chat import TaskReport

logger = logging.getLogger(__name__)

# The report-backs on a thread's own chain that the status slice counts: its
# subagents' and its background commands'. Both post a notification turn and
# drain in order on the thread's chain.
_THREAD_REPORT_HOOKS = ("task_report_back", "command_report_back")
# What each one is a report of, as ``owed_report_kinds`` names it.
_REPORT_KINDS = {"task_report_back": "task", "command_report_back": "command"}

async def publish_cleared_wake(thread_id: str) -> None:
    """Tell a thread's watchers to re-read its report-back status now.
    Best-effort: a dropped wake degrades to the watch's own backstop."""
    try:
        from src.server.services.report_back.handoff import wake
        from src.utils.cache.redis_cache import get_cache_client

        await wake.publish_wake(get_cache_client(), str(thread_id), cleared=True)
    except Exception:
        logger.debug(
            f"[TASK_REPORT_BACK] Cleared wake for {thread_id} failed", exc_info=True
        )


async def publish_cleared_wake_if_no_open_job(thread_id: str) -> None:
    """Settled-watch reconciliation for the subagent collectors.

    Report-back jobs are born on the run ledger's terminal CAS
    (``subagent_runs.finalize_task_run``), not here — when a collector's
    batch has fully settled and no report-back job is open for the thread,
    watchers are told to reconcile now instead of riding the status
    backstop. An open job means the executor's own outcome (run_id wake or
    cleared) is the signal to wait for. Never raises.
    """
    try:
        from src.server.database.runs import outbox as outbox_db
        from src.server.services.report_back.handoff import wake
        from src.utils.cache.redis_cache import get_cache_client

        open_jobs = await outbox_db.list_open_notification_jobs(
            thread_id, "task_report_back", limit=1
        )
        if not open_jobs:
            await wake.publish_wake(get_cache_client(), thread_id, cleared=True)
    except Exception:
        logger.warning(
            f"[TASK_REPORT_BACK] Settled-wake reconciliation failed for "
            f"thread {thread_id}",
            exc_info=True,
        )


async def read_task_report_back_status(thread_id: str) -> dict:
    """Report-back status slice for a PTC thread (same contract as hand-offs),
    its subagents' and its background commands' reports together.

    A non-deferred outbox job is owed until the notification run it
    dispatched ends (see ``ended_run_ids``). The first owed job's
    ``dispatched_run_id`` (present once the notification turn is POSTed) is
    the run to attach to. On a read failure ``pending_report_back`` is
    ``None`` (unknown — the frontend keeps watching). Drained notification
    runs are derived from recently DONE outbox rows — the only recovery for
    a wake published while the client held no subscription, and durable by
    construction (the ack that closes the job IS the ledger write) — plus
    the runs of open jobs that already ended, which are persisted and
    replayable before the ack lands. ``owed_report_kinds`` says whose reports
    are owed (``task``, ``command``), so a client names what it waits on; None
    when the read failed.
    """
    from src.server.database.runs import outbox as outbox_db
    from src.server.services.report_back.handoff.status import ended_run_ids

    pending: bool | None = False
    run_id = None
    ended: list[str] = []
    kinds: list[str] | None = []
    try:
        jobs = await outbox_db.list_open_notification_jobs(
            thread_id, _THREAD_REPORT_HOOKS
        )
        runs = [(job.get("payload") or {}).get("dispatched_run_id") for job in jobs]
        ended = await ended_run_ids(runs)
        owed = [(job, r) for job, r in zip(jobs, runs) if r not in ended]
        pending = bool(owed)
        run_id = owed[0][1] if owed else None
        kinds = sorted({_REPORT_KINDS[job["hook_type"]] for job, _ in owed})
    except Exception:
        logger.warning(
            f"Task report-back status read failed for {thread_id}; "
            f"reporting unknown",
            exc_info=True,
        )
        pending = None
        kinds = None
    # Producer-undecided signal: while a tail subagent still runs there is no
    # open outbox row yet, so ``pending=False`` alone would read as drained.
    # Listing the live writers lets the client keep its watch armed.
    active_tasks: list[str] = []
    try:
        from src.server.services import subagent_liveness

        active_tasks = await subagent_liveness.get_active_task_ids(thread_id)
    except Exception:
        logger.warning(
            f"Active-task read failed for {thread_id} report-back slice",
            exc_info=True,
        )
        # Unread, a writer still running would read as none: unknown keeps
        # the client armed until a read confirms it.
        if pending is False:
            pending = None
    # The same signal for commands: a running one reports when it ends, and
    # until then has no job.
    active_commands: list[str] = []
    try:
        from src.server.database.runs import command_runs as cr_db

        active_commands = [
            str(r["command_id"]) for r in await cr_db.list_running(thread_id=thread_id)
        ]
    except Exception:
        logger.warning(
            f"Running-command read failed for {thread_id} report-back slice",
            exc_info=True,
        )
        if pending is False:
            pending = None
    recent_run_ids: list[str] = []
    try:
        recent_run_ids = await outbox_db.get_recent_notification_run_ids(
            thread_id, _THREAD_REPORT_HOOKS
        )
    except Exception:
        logger.warning(
            f"Recents read failed for {thread_id} report-back slice",
            exc_info=True,
        )
        # Recents are the wake-miss recovery channel; without them an idle
        # answer would authorize teardown while a drained run may still be
        # unrendered. Degrade to unknown so the client stays armed.
        if pending is False:
            pending = None
    # Ended-but-unacked runs are newer than every acked one, since the jobs
    # drain in order on the thread's chain.
    recent_run_ids = list(dict.fromkeys([*ended, *recent_run_ids]))
    return {
        "thread_id": thread_id,
        "pending_report_back": pending,
        "report_back_run_id": run_id,
        "recent_report_back_run_ids": recent_run_ids,
        "active_tasks": active_tasks,
        "active_commands": active_commands,
        "owed_report_kinds": kinds,
    }


def task_report(payload: dict) -> "TaskReport":
    """The task a notification turn announces, read off its payload once: the
    turn's message names it from this, and history labels the turn with it."""
    from src.server.models.chat import TaskReport

    task_id = payload.get("task_id")
    return TaskReport(
        kind="task",
        task_id=str(task_id),
        display_id=payload.get("display_id") or f"Task-{task_id}",
        description=payload.get("description") or None,
        # Pre-final_status jobs (enqueued before the field existed) carry no
        # status; only completed runs were enqueued then.
        failed=payload.get("final_status") == "error",
        subagent_type=payload.get("subagent_type") or None,
    )


def task_report_message(
    report: "TaskReport", subagent_type: str | None, *, steered: bool = False
) -> str:
    """The report's content, as its own turn or as a steer into a live one.

    Only the event. Reading the result is the Task description's to say and
    never passing a failure off as finished work the system prompt's, so the
    report does not restate either. It need not say the result is unseen
    either: once TaskOutput serves a result, its report is dropped."""
    # Posted, it names no time: a report is posted whenever no live turn took
    # it in, which includes a task that settled after the turn's last model
    # call, so "after your previous turn ended" can be false.
    when = " while you were working" if steered else ""
    outcome = "FAILED" if report.failed else "completed"
    return (
        "<system>\n"
        f"Background subagent {report.display_id} ({subagent_type or 'subagent'}) "
        f"{outcome}{when}.\n"
        f"Task description: {report.description or ''}\n"
        "</system>"
    )


@dataclass(frozen=True)
class OwedTaskReport:
    """A task report still owed: what the reader is shown, the subagent type
    the model is told about, and the thread's owner and workspace a turn of
    its own posts as."""

    report: "TaskReport"
    subagent_type: str | None
    user_id: Optional[str] = None
    workspace_id: Optional[str] = None
    #: Whether the payload names its task: a label would name nothing.
    labelled: bool = True

    @property
    def report_source(self) -> Optional[dict]:
        return self.report.model_dump(exclude_none=True) if self.labelled else None

    def message(self, *, steered: bool) -> str:
        return task_report_message(self.report, self.subagent_type, steered=steered)


async def task_report_verdict(job: dict) -> OwedTaskReport | NotOwed:
    """Whether a ``task_report_back`` job is still owed, against the ledger.

    The one eligibility both its executor (at claim time) and a live turn
    taking it in as a steer decide by. TaskOutput deliveries stamp
    ``result_delivered_at``, so a stamped run owes nothing.
    """
    from src.server.database.runs import lifecycle as tl_db
    from src.server.database.runs import subagent_runs as sr_db

    payload = job.get("payload") or {}
    task_run_id = payload.get("task_run_id")
    if task_run_id:
        run_row = await sr_db.get_task_run(str(task_run_id))
        if run_row is None:
            return NotOwed("its run row is gone", wake=False)
        if run_row.get("result_delivered_at"):
            return NotOwed("its result was already delivered")
        parent_run_id = run_row.get("parent_run_id")
        if parent_run_id and stop_already_covered(
            await tl_db.get_run(str(parent_run_id)), run_row.get("finalized_at")
        ):
            return NotOwed("its turn was stopped after it settled")
    user_id, workspace_id = payload.get("user_id"), payload.get("workspace_id")
    return OwedTaskReport(
        task_report(payload),
        payload.get("subagent_type"),
        user_id=str(user_id) if user_id else None,
        workspace_id=str(workspace_id) if workspace_id else None,
        labelled=bool(payload.get("task_id")),
    )


async def _with_owner(job: dict, owed: OwedTaskReport) -> OwedTaskReport | NotOwed:
    """Ledger-enqueued jobs carry task identity only, so owner and workspace
    resolve from the thread, the durable source (older jobs carried both in
    the payload)."""
    from src.server.database import conversation as conv_db

    thread_id = job.get("conversation_thread_id") or (job.get("payload") or {}).get("thread_id")
    if (owed.user_id and owed.workspace_id) or not thread_id:
        return owed
    meta = await conv_db.get_thread_auth_meta(str(thread_id))
    if meta is None:
        return NotOwed("its thread is gone", wake=False)
    user_id = owed.user_id or meta.get("user_id")
    workspace_id = owed.workspace_id or meta.get("workspace_id")
    return replace(
        owed,
        user_id=str(user_id) if user_id else None,
        workspace_id=str(workspace_id) if workspace_id else None,
    )


TASK_PIPELINE = ReportPipeline(
    log_prefix="[TASK_REPORT_BACK]",
    # Apart from the hand-off namespace, so a job id never mints the same key
    # for both.
    request_ns=uuid.UUID("3d9c4e8a-1b6f-4a72-9e05-7f8a2c31d4b6"),
    # Short on purpose: a busy thread's cap does not drop the notification;
    # the job re-parks and the thread's next dead-turn finalize releases it.
    busy_cap=120.0,
    # The notification turn is a full turn with tools; the job holds the
    # thread's chain until it ends.
    terminal_cap=45 * 60.0,
    verdict=lambda job: task_report_verdict(job),
    subject=lambda job: _task_subject(job.get("payload") or {}),
    owner=_with_owner,
)


def _task_subject(payload: dict) -> str:
    return f"task {payload.get('display_id') or payload.get('task_id')}"


async def execute_task_report_back(job: dict) -> None:
    """Execute one ``task_report_back`` outbox job (``run_report_back``).
    It never stamps delivery: its dedup is the claim-time drop against
    TaskOutput's stamp plus the per-run job identity."""
    await run_report_back(job, TASK_PIPELINE)


# ---------------------------------------------------------------------------
# Outbox executor — registered with the drainer at startup
# (``register_outbox_executors``, called from app setup before the drainer
# starts) so the outbox service never imports this module.
# ---------------------------------------------------------------------------


async def _exec_subagent_report_back(job: dict) -> None:
    await execute_task_report_back(job)


async def _on_subagent_report_back_closed(job: dict) -> None:
    # Task report-back pendingness IS the open outbox row, and the frontend
    # watch is push-driven — a /status read that raced ahead of the ack, or
    # of a job parked dead, leaves the client armed with no later signal
    # until the ~30-min watch recycle. `cleared` only means "reconcile now":
    # the client re-reads /status, so a wake with another job still queued
    # is harmless. A dropped wake degrades to the recycle snapshot.
    thread_id = job.get("conversation_thread_id") or (
        job.get("payload") or {}
    ).get("thread_id")
    if thread_id:
        await publish_cleared_wake(thread_id)


def register_outbox_executors() -> None:
    from src.server.services.hook_outbox import register_hook_executor

    register_hook_executor(
        "task_report_back",
        _exec_subagent_report_back,
        on_closed=_on_subagent_report_back_closed,
    )
