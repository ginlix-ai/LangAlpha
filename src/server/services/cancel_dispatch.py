"""Public cancel dispatcher — routes a user stop to the right mechanism.

One entry point for POST /cancel: a manual compact/offload stop goes to the
ThreadMutationRunner; a live turn gets durable cancel intent on its run row,
a local executor signal, and a cross-worker nudge when the owner is
elsewhere. Terminal state is only ever written by the finalize CAS.
"""

import logging
from typing import Optional

from fastapi import HTTPException

logger = logging.getLogger(__name__)


def _cancel_outcome(
    *,
    cancelled: bool,
    message: str,
    state: Optional[str] = None,
    thread_id: Optional[str] = None,
    task_id: Optional[str] = None,
) -> dict:
    """The one shape a cancel answers in — one user-facing action, one envelope.

    ``state`` is the machine-readable reason a cancel did nothing, so it is
    absent exactly when one was dispatched. ``thread_id`` and ``task_id`` stay
    separate keys rather than one generic subject id because the two endpoints
    cancel different objects, and a client reading the answer has to know which
    of them it got back.
    """
    outcome: dict = {"cancelled": cancelled}
    if thread_id is not None:
        outcome["thread_id"] = thread_id
    if task_id is not None:
        outcome["task_id"] = task_id
    if state is not None:
        outcome["state"] = state
    outcome["message"] = message
    return outcome


async def cancel_workflow(thread_id: str, run_id: Optional[str] = None) -> dict:
    """
    Explicitly cancel a workflow execution (user stop) — v4 honest cancel.

    Records durable cancel *intent* on the run's in_progress row
    (``cancel_requested_at``), then signals the local task via
    ``manager.signal_cancel`` (which interrupts the current step
    immediately). The terminal ``cancelled`` state is written only by the
    finalize CAS when the teardown actually completes — never eagerly from
    here. A cancel that arrives after the run finalized is an honest
    idempotent "already finished", not a recorded losing cancel.

    The subagent kill + registry wipe is owned by the single-owner teardown
    in ``LocalRunExecutor`` when the ``CancelledError`` lands — this
    handler only runs ``cancel_and_clear`` as a safety net when no active
    task exists (e.g. an orphaned registry left by a crash).

    ``run_id`` targets a specific run so a slow/retried stop can't cancel a
    *newer* turn the user started after the stopped one finished. Omitted =
    the thread's active run.
    """
    try:
        from src.server.services.runs.executor import (
            LocalRunExecutor,
        )

        manager = LocalRunExecutor.get_instance()
        has_active = await manager.has_active_task_for_thread(thread_id)

        # Manual mutation stop. A manual /compact|/offload registers no
        # workflow task (it runs inside its own HTTP request handler), so when
        # there is no active workflow, stopping the in-flight mutation is the
        # entire job — and it must not stamp cancel intent on a run row. The
        # runner cancels a locally-owned op directly, or flags the stop key a
        # foreign worker's heartbeat polls. (An AUTO compaction runs inside
        # the turn's task — there has_active is True, so we fall through and
        # cancel_workflow's inner_task cancel interrupts the summarize.)
        if not has_active:
            from src.server.services.thread_mutation import ThreadMutationRunner

            stopped = await ThreadMutationRunner.get_instance().request_stop(
                thread_id
            )
            if stopped != "none":
                logger.info(
                    f"Manual mutation stop ({stopped}) by user: {thread_id}"
                )
                return _cancel_outcome(
                    cancelled=True,
                    thread_id=thread_id,
                    message=(
                        "Compaction stopped."
                        if stopped == "cancelled"
                        else "Compaction stop signalled to its worker."
                    ),
                )

        # Durable cancel intent on the run row. Only an in_progress row
        # accepts it (the row lock linearizes cancel vs finalize), so this is
        # self-gating: no active run, nothing stamped — the old eager
        # tracker/thread-status "cancelled" writes are gone with it.
        from src.server.database.runs import lifecycle as tl_db

        # `or None`: an empty-string run_id (e.g. `?run_id=`) must resolve
        # like an omitted one, not skip both the active-run lookup and the
        # honest no_active_run response below.
        target_run_id = run_id or None
        if target_run_id is None:
            active = await tl_db.get_active_run(thread_id)
            if active:
                target_run_id = str(active["conversation_response_id"])

        intent_state = None
        if target_run_id:
            intent = await tl_db.request_run_cancel(target_run_id, thread_id=thread_id)
            intent_state = intent["state"]
            logger.info(
                f"[cancel] durable intent for run={target_run_id} "
                f"thread={thread_id}: {intent_state}"
            )

        # Local execution signal. Signal the SAME run the intent was stamped
        # on: if the resolved run finalizes and a newer one starts between
        # the stamp and this call, an untargeted (None) signal would cancel
        # the newer run. None only when no ledger row exists — the pre-START
        # window, where the manager's thread scan is the only handle.
        cancel_success = await manager.signal_cancel(thread_id, target_run_id)

        # F5 nudge: intent stamped but no local executor — the owner is
        # (likely) another worker; nudge it to interrupt now. Best-effort:
        # a lost nudge still converges via the finalize CAS adopting
        # 'cancelled' from the durable intent.
        if not cancel_success and intent_state in ("requested", "already_requested"):
            from src.server.services.runs.cancel import publish_cancel_nudge

            await publish_cancel_nudge(thread_id, target_run_id)

        if not cancel_success and not await manager.has_active_task_for_thread(
            thread_id
        ):
            logger.warning(
                f"Could not cancel background task for {thread_id} "
                "(may be already completed or not found)"
            )
            # Safety net, RUN-scoped: cancel any local subagents the target
            # run left behind (e.g. its main task settled but tail writers
            # survive — an explicit cancel of that run stops its tail too).
            # Never a thread-wide wipe: the target run may live on ANOTHER
            # worker while this registry holds a terminal local run's live
            # tail, whose guard drain must keep seeing its writers.
            if target_run_id:
                from src.server.services.background_registry_store import (
                    BackgroundRegistryStore,
                )

                registry_store = BackgroundRegistryStore.get_instance()
                await registry_store.cancel_run_tasks(
                    thread_id, target_run_id, force=True
                )

        if intent_state == "already_terminal":
            return _cancel_outcome(
                cancelled=False,
                thread_id=thread_id,
                state="already_finished",
                message="Run already finished; nothing to cancel.",
            )
        if not cancel_success and intent_state in (None, "not_found"):
            # This cancel stopped nothing (a `not_found` that DID signal is the
            # pre-START window, where the placeholder cancel took). Which
            # nothing decides whether the caller should care: an idle thread
            # had nothing to stop, while a live run means the stop the user
            # asked for never happened. Reporting that is the whole job —
            # cancelling the live run instead is the cross-turn hazard that
            # run_id targeting exists to prevent.
            if await tl_db.get_active_run(thread_id) is None:
                return _cancel_outcome(
                    cancelled=False,
                    thread_id=thread_id,
                    state="no_active_run",
                    message="No active run to cancel.",
                )
            return _cancel_outcome(
                cancelled=False,
                thread_id=thread_id,
                state="another_run_active",
                message=(
                    "A run is active on this thread but the one to stop is "
                    "gone; nothing was cancelled."
                ),
            )

        # Reaching here means intent was stamped or a task was signalled: every
        # way of stopping nothing is answered above, with the state token that
        # says which. Keep it that way — a `cancelled: False` with no state
        # would tell a caller a stop failed without telling it what to do.
        logger.info(f"Workflow cancel requested: {thread_id}")
        return _cancel_outcome(
            cancelled=True,
            thread_id=thread_id,
            message="Cancellation signal sent. The turn will stop shortly.",
        )

    except Exception as e:
        logger.exception(f"Error cancelling workflow {thread_id}: {e}")
        # The log keeps the exception; the wire gets a fixed string. A ledger
        # or transport failure names hosts, queries and occasionally
        # credential fragments, and this reply crosses to an authenticated
        # caller (src/server/AGENTS.md: sanitize before the wire).
        raise HTTPException(
            status_code=500, detail="Failed to cancel the turn."
        ) from e


async def cancel_subagent_task(thread_id: str, task_id: str) -> dict:
    """Targeted stop for one background task (e.g. a workflow run).

    Local-first: when this worker owns the live writer, the registry cancel
    stamps durable intent and interrupts immediately. Otherwise stamp intent
    on the task's active ledger run and nudge the owning worker over the
    shared cancel channel. A lost nudge degrades to the task finishing on
    its own — never a stuck state.
    """
    try:
        from src.server.services.background_registry_store import (
            BackgroundRegistryStore,
        )

        registry_store = BackgroundRegistryStore.get_instance()
        if await registry_store.cancel_task(thread_id, task_id):
            logger.info(
                f"[cancel-task] local cancel thread={thread_id} task={task_id}"
            )
            return _cancel_outcome(
                cancelled=True,
                thread_id=thread_id,
                task_id=task_id,
                message="Cancellation signal sent. Task will stop shortly.",
            )

        # Foreign or unknown owner: the ledger is the truth for liveness.
        from src.server.database.runs import subagent_runs as sr_db

        active = await sr_db.get_active_task_run(thread_id, task_id)
        if active is None:
            statuses = await sr_db.get_latest_run_statuses(thread_id, [task_id])
            status = statuses.get(task_id)
            if status:
                return _cancel_outcome(
                    cancelled=False,
                    thread_id=thread_id,
                    task_id=task_id,
                    state="already_finished",
                    message=f"Task already {status}; nothing to cancel.",
                )
            return _cancel_outcome(
                cancelled=False,
                thread_id=thread_id,
                task_id=task_id,
                state="task_not_found",
                message="No such task on this thread; nothing to cancel.",
            )

        intent = await sr_db.request_task_run_cancel(
            str(active["task_run_id"]), thread_id=thread_id
        )
        if intent["state"] == "already_terminal":
            return _cancel_outcome(
                cancelled=False,
                thread_id=thread_id,
                task_id=task_id,
                state="already_finished",
                message="Task already finished; nothing to cancel.",
            )
        if intent["state"] == "not_found":
            # The local cancel already failed to reach this task, so nothing
            # was stopped — and the run row moved or vanished between the
            # liveness read and the stamp, so nothing was recorded either.
            # Nudging and answering "cancelled" would claim a stop that never
            # happened. The task itself still exists — only its run is gone,
            # which is why this is not ``task_not_found``.
            return _cancel_outcome(
                cancelled=False,
                thread_id=thread_id,
                task_id=task_id,
                state="run_not_found",
                message="No active run for this task; nothing to cancel.",
            )

        from src.server.services.runs.cancel import publish_cancel_nudge

        await publish_cancel_nudge(thread_id, None, task_id=task_id)
        logger.info(
            f"[cancel-task] intent stamped + nudge published "
            f"thread={thread_id} task={task_id}"
        )
        return _cancel_outcome(
            cancelled=True,
            thread_id=thread_id,
            task_id=task_id,
            message="Cancellation signalled to the task's worker.",
        )
    except Exception as e:
        logger.exception(f"Error cancelling task {task_id} in {thread_id}: {e}")
        # The log keeps the exception; the wire gets a fixed string. A ledger
        # or transport failure names hosts, queries and occasionally
        # credential fragments, and this reply crosses to an authenticated
        # caller (src/server/AGENTS.md: sanitize before the wire).
        raise HTTPException(
            status_code=500, detail="Failed to cancel task."
        ) from e

