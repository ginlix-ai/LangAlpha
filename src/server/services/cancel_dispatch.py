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
                return {
                    "cancelled": True,
                    "thread_id": thread_id,
                    "message": (
                        "Compaction stopped."
                        if stopped == "cancelled"
                        else "Compaction stop signalled to its worker."
                    ),
                }

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
            return {
                "cancelled": False,
                "thread_id": thread_id,
                "state": "already_finished",
                "message": "Run already finished; nothing to cancel.",
            }
        if intent_state == "not_found" and not cancel_success:
            # A caller-supplied run_id that matches neither a ledger row nor
            # a local task (wrong id, or another thread's run). Distinct from
            # the pre-START dispatched window, where the placeholder cancel
            # succeeds (cancel_success=True) despite the row not existing yet.
            return {
                "cancelled": False,
                "thread_id": thread_id,
                "state": "run_not_found",
                "message": "No such run on this thread; nothing to cancel.",
            }
        if target_run_id is None and not cancel_success:
            # No durable run, no local task, no compaction — an honest no-op
            # instead of pretending a signal was sent.
            return {
                "cancelled": False,
                "thread_id": thread_id,
                "state": "no_active_run",
                "message": "No active run to cancel.",
            }

        logger.info(f"Workflow cancel requested: {thread_id}")
        return {
            "cancelled": bool(intent_state in ("requested", "already_requested"))
            or cancel_success,
            "thread_id": thread_id,
            "message": "Cancellation signal sent. Workflow will stop shortly.",
        }

    except Exception as e:
        logger.exception(f"Error cancelling workflow {thread_id}: {e}")
        raise HTTPException(
            status_code=500, detail=f"Failed to cancel workflow: {str(e)}"
        )

