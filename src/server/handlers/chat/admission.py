"""Turn admission for chat workflows: admit a new turn, steer, or 409.

``wait_or_steer`` is the single admission decision for the foreground and
dispatched paths; ``admission_conflict_detail`` is the single wording
source for every in-generator admission 409; ``ADMISSION_CONFLICT_CODES``
is the closed set of codes ``handle_workflow_error`` treats as protocol
responses (never persisted, never mark_failed).
"""

from __future__ import annotations

import json
from typing import TYPE_CHECKING

from fastapi import HTTPException

if TYPE_CHECKING:
    from src.server.services.background_task_manager import BackgroundTaskManager


# Structured codes carried by every in-generator admission-conflict 409
# (``admission_conflict_detail`` and ``wait_or_steer``). ``handle_workflow_error``
# keys its skip-persist/skip-mark_failed path on these so a non-admission 409
# can't be mistaken for one. ``request_cancelled`` (from the cancellation
# wrapper) is included defensively though it does not currently reach this path.
# ``not_running`` (steer_only probe against a fresh thread) is likewise an
# admission outcome, not a workflow failure — nothing was admitted to persist
# or mark failed.
ADMISSION_CONFLICT_CODES = {
    "stopping",
    "compacting",
    "running",
    "not_running",
    "request_cancelled",
    # Retry-validation outcomes from ``resolve_retry_of``: protocol
    # responses raised in-generator pre-START (run_handle is None, nothing
    # was admitted). Without these codes they'd fall into the generic
    # persist + mark_failed path — and a stale retry often coexists with a
    # NEWER running turn whose tracker status mark_failed would clobber.
    "stale_retry",
    "not_retryable",
}


def admission_conflict_detail(state: str) -> dict:
    """Map an admission outcome to its 409 ``{"code", "message"}`` detail.

    The single wording source for every in-generator admission 409 (see
    ``ADMISSION_CONFLICT_CODES``): the ``stopping``/``compacting``/``running``
    conflicts and the ``steer_only`` probe's ``not_running`` refusal. Returns a
    structured dict so ``handle_workflow_error`` can tag the SSE error with the
    code and the client can recognize the state; the thread_id is deliberately
    absent from the user-facing message so it can't leak into the UI on a
    client-state desync.
    """
    if state == "stopping":
        return {
            "code": "stopping",
            "message": "The workflow is stopping. Wait a moment, then retry.",
        }
    if state == "compacting":
        return {
            "code": "compacting",
            "message": "The assistant is compacting its context. Wait a moment, then retry.",
        }
    if state == "not_running":
        return {
            "code": "not_running",
            "message": "No workflow is running to steer. Resubmit as a new message.",
        }
    return {
        "code": "running",
        "message": (
            "The workflow is still running. Wait a moment, then retry — "
            "or use /reconnect to continue streaming, or /cancel to stop it."
        ),
    }


async def wait_or_steer(
    manager: BackgroundTaskManager,
    thread_id: str,
    user_input: str,
    user_id: str,
    *,
    steer_only: bool = False,
    can_steer: bool = True,
) -> tuple[bool, str | None]:
    """Admit a new turn, steer the running one, or 409.

    The single admission decision for both the foreground and dispatched
    paths. Returns ``(ready, steering_event)``: ``ready=True`` → start a new
    workflow; ``ready=False`` with a non-None ``steering_event`` → yield that
    SSE string and return. A non-admissible state raises HTTP 409 whose detail
    carries a structured ``admission_conflict_detail`` code (surfaced as an SSE
    ``error`` event once the stream has started).

    ``steer_only=True`` (gateway steer probes) forbids the fresh-admission
    fallback: the probe's SSE reader only understands ``steering_accepted``/
    ``error``, so admitting a full turn on that connection streams the whole
    response — interrupts included — into a client that ignores it. A steer
    that finds no running turn gets ``not_running`` and the gateway resubmits
    it as a normal message. A steer accepted just before the workflow's exit
    may still go unconsumed — the final drain returns it via
    ``steering_returned`` on the turn stream, not this connection.

    ``can_steer=False`` (dispatched X-Dispatch=background flows) forbids
    steering entirely: any in-flight run is a hard conflict, never a steer.

    Admission states (see ``BackgroundTaskManager.wait_for_admission``):
    - ``"fresh"``     → start a new turn ``(True, None)``; with ``steer_only``,
      409 ``not_running`` instead.
    - ``"stopping"``/``"compacting"`` → 409 (never start a second checkpoint
      writer, never steer mid-summarize).
    - ``"running"``   → steer immediately (no wait); an accept that raced the
      workflow's exit is reclaimed and re-routed as "fresh"; 409 only if
      steering fails. With ``can_steer=False`` a running peer is a 409 too.
    """
    # Deferred: steering imports chat-handler modules at module level.
    from src.server.handlers.chat.steering import steer_thread, unsteer_thread

    state, active_row = await manager.wait_for_admission(thread_id)
    if state == "fresh":
        if steer_only:
            raise HTTPException(
                status_code=409, detail=admission_conflict_detail("not_running")
            )
        return True, None

    # Only a genuinely-running turn on a steerable path can be steered; every
    # other non-fresh state — and every dispatched peer (``can_steer=False``) —
    # is a conflict. Steering mid-stopping would start a second checkpoint
    # writer; mid-compaction would corrupt the context rewrite.
    if state != "running" or not can_steer:
        raise HTTPException(
            status_code=409, detail=admission_conflict_detail(state)
        )

    # state == "running" and steerable → steer the running workflow
    # immediately, stamped with the run it targets (v4 2.4c): the consuming
    # middleware delivers only own-run payloads, so a message steered into a
    # run that then died can never leak into a later turn's context.
    active_run_id = (
        str(active_row["conversation_response_id"]) if active_row else None
    )
    result = await steer_thread(
        thread_id, user_input, user_id, run_id=active_run_id
    )
    if not result:
        # Steering failed (e.g. Redis unavailable) on a running turn.
        raise HTTPException(
            status_code=409, detail=admission_conflict_detail("running")
        )

    # Close the accept-after-exit race: the admission snapshot said "running",
    # but the workflow may have exited — and run its final steering drain —
    # between that snapshot and the Redis push. If the slot has no live run
    # anymore (worker-agnostic: the ledger row, not the local registry),
    # try to reclaim the message: a successful reclaim proves nothing
    # consumed it, so route as if admission had said "fresh". Reclaim failure is
    # the best-effort branch: almost always the exit drain got there first and
    # ``steering_returned`` carried the text back on the turn stream, so report
    # accepted — but a Redis fault on the reclaim also lands here, so "accepted"
    # is the graceful default, not a hard delivery guarantee.
    from src.server.database import turn_lifecycle as tl_db

    if await tl_db.get_active_run(thread_id) is None:
        reclaimed = await unsteer_thread(thread_id, result["payload"])
        if reclaimed:
            if steer_only:
                raise HTTPException(
                    status_code=409,
                    detail=admission_conflict_detail("not_running"),
                )
            return True, None
    event_data = json.dumps(
        {
            "thread_id": thread_id,
            "content": user_input,
            "position": result["position"],
        }
    )
    return False, f"event: steering_accepted\ndata: {event_data}\n\n"
