"""PTC←background-subagent report-backs, mirroring flash←PTC.

Every completed subagent run bears a ``task_report_back`` outbox job in
the same transaction as its terminal CAS (``subagent_runs.
finalize_task_run``) — enqueue is unconditional and crash-safe. Whether a
notification is actually owed is the EXECUTOR's decision at claim time,
against the ledger: a run whose ``result_delivered_at`` is stamped
(TaskOutput delivered it) is dropped with a cleared wake; a live or
interrupted parent parks the job until the thread's next completed
finalize releases it. The POST is a synthetic notification turn via the
shared ``notify_turn`` machinery: it announces completion and leaves
TaskOutput to fetch the result from the durable archive — nothing
volatile rides the payload.

Unlike the flash pipeline there is no Redis reserve state: the open outbox
row IS the pending-registry (its open lifetime — enqueue through the
executor's terminal wait — is exactly the pending window, and /status
reads it via ``get_open_notification_job``).
"""

import logging
import uuid

logger = logging.getLogger(__name__)

# One deterministic request-key namespace per job id (separate from the
# flash RB namespace so a job id can never mint the same key for both).
TASK_RB_REQUEST_NS = uuid.UUID("3d9c4e8a-1b6f-4a72-9e05-7f8a2c31d4b6")

# POST defer-loop cap per lease chain. Short on purpose: a busy thread's
# cap exhaustion doesn't drop the notification — the executor re-parks the
# job as deferred and the thread's next completed finalize releases it.
_TASK_RB_BUSY_WAIT_CAP = 120.0
# The notification turn is a full PTC turn (sandbox, tools); hold the
# chain open up to this long before acking anyway.
_TASK_RB_TERMINAL_WAIT_CAP = 45 * 60.0


def _job_request_key(job_id: str) -> str:
    return str(uuid.uuid5(TASK_RB_REQUEST_NS, job_id))


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
        from src.server.database import hook_outbox as outbox_db
        from src.server.handlers.chat.report_back import publish_wake
        from src.utils.cache.redis_cache import get_cache_client

        job = await outbox_db.get_open_notification_job(
            thread_id, "task_report_back"
        )
        if job is None:
            await publish_wake(get_cache_client(), thread_id, cleared=True)
    except Exception:
        logger.warning(
            f"[TASK_REPORT_BACK] Settled-wake reconciliation failed for "
            f"thread {thread_id}",
            exc_info=True,
        )


async def read_task_report_back_status(thread_id: str) -> dict:
    """Report-back status slice for a PTC thread (same contract as flash).

    Pendingness IS the oldest open non-deferred outbox row; its
    ``dispatched_run_id`` (present once the notification turn is POSTed) is
    the run to attach to. On a read failure ``pending_report_back`` is
    ``None`` (unknown — the frontend keeps watching). Drained notification
    runs are derived from recently DONE outbox rows — the only recovery for
    a wake published while the client held no subscription, and durable by
    construction (the ack that closes the job IS the ledger write) — plus
    the open job's run when it is already terminal (post-finalize/pre-ack
    window: the turn is persisted and replayable before the ack lands).
    """
    from src.server.database import hook_outbox as outbox_db

    pending: bool | None = False
    run_id = None
    try:
        job = await outbox_db.get_open_notification_job(
            thread_id, "task_report_back"
        )
        if job is not None:
            pending = True
            run_id = (job.get("payload") or {}).get("dispatched_run_id")
    except Exception:
        logger.warning(
            f"Task report-back status read failed for {thread_id}; "
            f"reporting unknown",
            exc_info=True,
        )
        pending = None
    # Producer-undecided signal: while a tail subagent still runs there is no
    # open outbox row yet, so ``pending=False`` alone would read as drained.
    # Listing the live writers lets the client keep its watch armed.
    active_tasks: list[str] = []
    try:
        from src.server.services.background_task_manager import (
            BackgroundTaskManager,
        )

        active_tasks = await BackgroundTaskManager.get_instance().get_active_task_ids(
            thread_id
        )
    except Exception:
        logger.warning(
            f"Active-task read failed for {thread_id} report-back slice",
            exc_info=True,
        )
    recent_run_ids: list[str] = []
    try:
        recent_run_ids = await outbox_db.get_recent_notification_run_ids(
            thread_id, "task_report_back"
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
    # Post-finalize/pre-ack window: the dispatched run can already be terminal
    # (turn persisted, replayable) while the job is still open — recents would
    # otherwise be blind to it and a reloading client re-attaches the run.
    from src.server.handlers.chat.report_back import recents_with_terminal_pointer

    recent_run_ids = await recents_with_terminal_pointer(run_id, recent_run_ids)
    return {
        "thread_id": thread_id,
        "pending_report_back": pending,
        "report_back_run_id": run_id,
        "recent_report_back_run_ids": recent_run_ids,
        "active_tasks": active_tasks,
    }


def _build_notification_message(payload: dict) -> str:
    """The synthetic turn's content: announce completion and direct the
    agent to TaskOutput, which fetches the result from the durable archive."""
    display_id = payload.get("display_id") or f"Task-{payload.get('task_id')}"
    subagent_type = payload.get("subagent_type") or "subagent"
    description = payload.get("description") or ""
    return (
        "<system>\n"
        f"Background subagent {display_id} ({subagent_type}) finished after "
        f"your previous turn ended, and you have not seen its result. "
        f"Retrieve it and report the outcome to the user (integrate it with "
        f"your prior work where relevant).\n"
        f"Task description: {description}\n"
        "</system>"
        f"\n\nCall `TaskOutput(task_id=\"{payload.get('task_id')}\")` "
        "to see the result."
    )


async def execute_task_report_back(job: dict) -> None:
    """Execute one ``task_report_back`` outbox job.

    Sole caller is the hook-outbox drainer — raising nacks, returning acks.
    The job stays open (lease-heartbeated) until the notification run
    reaches terminal, so per-thread FIFO holds. Crash-safe resume via
    ``dispatched_run_id`` (merged atomically with the result-text scrub) and
    the job-deterministic ``request_key``. A thread found interrupted, or a
    busy-wait cap, re-parks the job as deferred instead of dropping — the
    thread's next completed finalize releases it.
    """
    from src.server.database import hook_outbox as outbox_db
    from src.server.database import turn_lifecycle as tl_db
    from src.server.handlers.chat.notify_turn import (
        await_run_terminal,
        post_notification_turn,
    )
    from src.server.handlers.chat.report_back import publish_wake
    from src.server.services.hook_outbox import LEASE_SECONDS, MAX_ATTEMPTS
    from src.utils.cache.redis_cache import get_cache_client

    payload = job.get("payload") or {}
    thread_id = job.get("conversation_thread_id") or payload.get("thread_id")
    user_id = payload.get("user_id")
    job_id = str(job["hook_outbox_id"])
    attempts = job["attempts"]
    subject = f"task {payload.get('display_id') or payload.get('task_id')}"

    if not thread_id:
        logger.warning(
            f"[TASK_REPORT_BACK] Job {job_id} missing thread; dropping"
        )
        return
    # psycopg returns uuid columns as UUID objects; downstream wants str
    # (wake payloads are json.dumps'd — UUID is not serializable).
    thread_id = str(thread_id)

    # Ledger-enqueued jobs carry task identity only — owner and workspace
    # resolve from the thread, the durable source (legacy collector jobs
    # carried both in the payload).
    workspace_id = payload.get("workspace_id")
    if not user_id or not workspace_id:
        from src.server.database import conversation as conv_db

        if not user_id:
            meta = await conv_db.get_thread_auth_meta(thread_id)
            user_id = str(meta["user_id"]) if meta and meta.get("user_id") else None
        if not workspace_id:
            row = await conv_db.get_thread_by_id(thread_id)
            workspace_id = (
                str(row["workspace_id"]) if row and row.get("workspace_id") else None
            )
    if not user_id:
        logger.warning(
            f"[TASK_REPORT_BACK] Job {job_id} on thread {thread_id}: owner "
            f"unresolvable (thread deleted?); dropping"
        )
        return

    async def _fence() -> bool:
        return await outbox_db.extend_job_lease(
            job_id, LEASE_SECONDS, attempts=attempts
        )

    async def _repark() -> str | None:
        status = await outbox_db.defer_claimed_job(
            job_id, attempts=attempts, max_attempts=MAX_ATTEMPTS
        )
        logger.info(
            f"[TASK_REPORT_BACK] Re-parked job {job_id} for {subject} on "
            f"thread {thread_id} (status={status})"
        )
        return status

    rb_run_id = payload.get("dispatched_run_id")
    if not rb_run_id:
        # Ledger arbitration: the job was enqueued unconditionally at the
        # run's terminal CAS; whether a notification is still owed is
        # decided HERE, at claim time, against the durable row. TaskOutput
        # deliveries stamp result_delivered_at — a stamped run owes nothing.
        task_run_id = payload.get("task_run_id")
        if task_run_id:
            from src.server.database import subagent_runs as sr_db

            run_row = await sr_db.get_task_run(str(task_run_id))
            if run_row is None:
                logger.info(
                    f"[TASK_REPORT_BACK] Run row gone for {subject} on "
                    f"thread {thread_id}; dropping"
                )
                return
            if run_row.get("result_delivered_at"):
                logger.info(
                    f"[TASK_REPORT_BACK] Result already delivered for "
                    f"{subject} on thread {thread_id}; dropping"
                )
                # Watchers may be riding the pending chip on this job —
                # tell them to reconcile now instead of via the backstop.
                try:
                    await publish_wake(
                        get_cache_client(), thread_id, cleared=True
                    )
                except Exception:
                    pass
                return

        # Live/interrupted-latest guard. A live parent turn may still fetch
        # the result itself (jobs are born at the run's CAS, usually
        # mid-turn); an interrupted one must not receive a POST that would
        # collide with the pending HITL checkpoint. Both park the job until
        # the thread's next completed finalize releases it — the re-read
        # closes the race where that finalize landed between the status
        # read and the park (its release pass saw no deferred row yet).
        latest = await tl_db.get_latest_attempt(thread_id)
        if latest and latest.get("status") in ("interrupted", "in_progress"):
            parked = await _repark()
            if parked == "pending":
                latest = await tl_db.get_latest_attempt(thread_id)
                if not latest or latest.get("status") not in (
                    "interrupted",
                    "in_progress",
                ):
                    from src.server.database.conversation import (
                        get_db_connection,
                    )

                    async with get_db_connection() as conn:
                        await outbox_db.release_deferred_jobs(
                            conn, thread_id, "task_report_back"
                        )
                    try:
                        from src.server.services.hook_outbox import (
                            HookOutboxDrainer,
                        )

                        HookOutboxDrainer.get_instance().nudge()
                    except Exception:
                        pass
            return

        body = {
            "messages": [
                {"role": "user", "content": _build_notification_message(payload)}
            ],
            "agent_mode": "ptc",
            "workspace_id": workspace_id,
            "query_type": "system",
            "request_key": _job_request_key(job_id),
        }
        outcome, rb_run_id = await post_notification_turn(
            thread_id=thread_id,
            body=body,
            user_id=user_id,
            wait_cap=_TASK_RB_BUSY_WAIT_CAP,
            heartbeat=_fence,
            log_prefix="[TASK_REPORT_BACK]",
            subject=subject,
        )
        if outcome == "lost":
            return  # reclaimed: the live owner resumes; no teardown
        if outcome == "cap":
            # Busy thread (long user turn, backlog). Not a failure: park
            # until the next completed finalize re-releases the job.
            await _repark()
            return
        if outcome in ("deleted", "drop"):
            return  # thread gone or terminal rejection: ack, nothing owed
        if rb_run_id is None:
            raise RuntimeError(
                f"task report-back for {subject} dispatched without a run_id; "
                f"nacking to recover it via request_key dedup"
            )
        # Durable resume pointer: after this a reclaim resumes the terminal
        # wait (before it, request_key dedup makes the re-POST safe). Merge
        # failure must NACK — recents are derived from dispatched_run_id on
        # DONE rows, so acking without the pointer erases the notification
        # from wake-miss recovery.
        try:
            await outbox_db.merge_job_payload(
                job_id, {"dispatched_run_id": rb_run_id}
            )
        except Exception:
            logger.warning(
                f"[TASK_REPORT_BACK] Failed persisting dispatched_run_id "
                f"{rb_run_id} on job {job_id}; nacking (request_key dedup "
                f"makes the retry's re-POST safe)",
                exc_info=True,
            )
            raise
        # Nudge watching clients toward the notification run. Best-effort;
        # unfenced — a stale owner's wake still points at the real run.
        try:
            await publish_wake(get_cache_client(), thread_id, run_id=rb_run_id)
        except Exception:
            pass
    else:
        logger.info(
            f"[TASK_REPORT_BACK] Resuming in-flight notification run "
            f"{rb_run_id} for {subject} on thread {thread_id} (no re-dispatch)"
        )

    outcome = await await_run_terminal(
        job_id,
        attempts,
        rb_run_id,
        wait_cap=_TASK_RB_TERMINAL_WAIT_CAP,
        log_prefix="[TASK_REPORT_BACK]",
    )
    if outcome == "timeout":
        # No pair state to tear down (unlike flash): ack and release the
        # chain — the run row itself is the durable record of what happened.
        logger.warning(
            f"[TASK_REPORT_BACK] Terminal wait cap hit for {subject} on "
            f"thread {thread_id}; acking anyway"
        )
    # No recovery-ledger write here: the drainer's ack (status='done' +
    # completed_at) IS the ledger — the slice derives its recents from
    # recently DONE rows, so wake-miss recovery cannot be lost to a Redis
    # failure in this window.
