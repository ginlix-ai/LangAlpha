"""Turn admission for chat workflows: admit a new turn, steer, or 409.

``wait_or_steer`` is the single admission decision for the foreground and
dispatched paths, and ``steer_allowed`` the single answer to whether a
request may steer at all; ``admission_conflict_detail`` is the single wording
source for every in-generator admission 409; ``ADMISSION_CONFLICT_CODES``
is the closed set of codes ``handle_workflow_error`` treats as protocol
responses (never persisted, never mark_failed).
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import TYPE_CHECKING
from uuid import uuid4

from fastapi import HTTPException

from src.server.services.runs.admission import (
    BUSY_STATES,
    dedup_retransmit_or_raise,
    steer_idempotency_key,
    steer_job_run_id,
)

if TYPE_CHECKING:
    from src.server.handlers.chat.request_prep import HandOffSteered
    from src.server.models.chat import ChatRequest
    from src.server.services.runs.executor import LocalRunExecutor


# Structured codes carried by every in-generator admission-conflict 409
# (``admission_conflict_detail`` and ``wait_or_steer``). ``handle_workflow_error``
# keys its skip-persist/skip-mark_failed path on these so a non-admission 409
# can't be mistaken for one. ``request_cancelled`` (from the cancellation
# wrapper) is included defensively though it does not currently reach this path.
# ``not_running`` (steer_only probe against a fresh thread) is likewise an
# admission outcome, not a workflow failure — nothing was admitted to persist
# or mark failed. The busy states are the thread-held conflicts, one code each.
ADMISSION_CONFLICT_CODES = {
    *BUSY_STATES,
    "not_running",
    "request_cancelled",
    # Retry-validation outcomes from ``resolve_retry_of``: protocol
    # responses raised in-generator pre-START (run_handle is None, nothing
    # was admitted). Without these codes they'd fall into the generic
    # persist + mark_failed path — and a stale retry often coexists with a
    # NEWER running turn whose tracker status mark_failed would clobber.
    "stale_retry",
    "not_retryable",
    # A START posted for a parked steer that a running turn read first.
    "steer_taken",
    # START refused because the workspace is moving to another computer.
    "workspace_moving",
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
            "message": "The turn is stopping. Wait a moment, then retry.",
        }
    if state == "compacting":
        return {
            "code": "compacting",
            "message": "The assistant is compacting its context. Wait a moment, then retry.",
        }
    if state == "not_running":
        return {
            "code": "not_running",
            "message": "No turn is running to steer. Resubmit as a new message.",
        }
    if state == "workspace_moving":
        return {
            "code": "workspace_moving",
            "message": (
                "This workspace is moving to another computer. "
                "Try again when the move finishes."
            ),
        }
    return {
        "code": "running",
        "message": (
            "The turn is still running. Wait a moment, then retry — "
            "or use /reconnect to continue streaming, or /cancel to stop it."
        ),
    }


def steer_allowed(
    request: ChatRequest, *, dispatched: bool, steerable: bool
) -> bool:
    """Whether a running turn on the thread may take this request as a steer.

    Foreground turns steer, and so does a Chief of Staff hand-off
    (``agent_steer_of``). Any other dispatched flow, a retry (a /retry that
    finds another live run is a conflict, not an empty steering message into
    it), a fork (an edit or a regenerate replaces a turn, which a message
    added to the running one cannot do) and a non-steerable caller (an
    automation's instruction is a turn of its own) meet a running peer with a
    409 instead.
    """
    if dispatched:
        return steerable and _is_agent_hand_off(request)
    is_fork = request.fork_from_turn is not None and bool(request.checkpoint_id)
    return steerable and request.retry_of_run_id is None and not is_fork


def _is_agent_hand_off(request: ChatRequest) -> bool:
    """A dispatched message from the Chief of Staff, and nothing else: a
    report-back summary, a retry, a fork, an answer to a pause, or a
    turn the server posts for a parked steer is a turn of its own."""
    origin = request.origin
    return bool(
        origin is not None
        and origin.type == "agent"
        and origin.agent == "chief_of_staff"
        and not request.report_back_ptc_thread_id
        and request.report_source is None
        and request.steer_job_id is None
        and request.retry_of_run_id is None
        and request.fork_from_turn is None
        and not request.hitl_response
    )


@dataclass(frozen=True)
class AgentSteer:
    """How a Chief of Staff hand-off enters a running turn."""

    sender: str
    origin: dict
    idempotency_key: str
    # The Home thread and generation the run then reports back to, when the
    # hand-off asked for a report-back.
    origin_stamp: dict | None


def agent_steer_of(
    request: ChatRequest, *, dispatched: bool, run_id: str
) -> AgentSteer | None:
    """The steer a dispatched hand-off becomes if it meets a running turn.

    Keyed by its dispatch generation, so the executor's later turn and the
    report-back read the same identity. A hand-off without a report-back has
    no generation and is keyed by the run id minted for this request.
    """
    if not dispatched or not _is_agent_hand_off(request):
        return None
    home_thread_id = request.origin_home_thread_id
    gen = request.origin_dispatch_gen
    stamp = (
        {"origin_home_thread_id": home_thread_id, "origin_dispatch_gen": gen}
        if home_thread_id and gen
        else None
    )
    origin = {"type": "agent", "agent": request.origin.agent, "id": request.origin.id}
    return AgentSteer(
        sender=request.origin.agent,
        origin={**origin, **(stamp or {})},
        idempotency_key=f"steer:dispatch:{gen or run_id}",
        origin_stamp=stamp,
    )


# How many admission reads a steer may take. A steer meeting a run that
# ended under it reads admission again, which can find the next run live.
_STEER_ADMISSION_LAPS = 3


async def wait_or_steer(
    manager: LocalRunExecutor,
    thread_id: str,
    user_input: str,
    user_id: str,
    *,
    steer_only: bool = False,
    can_steer: bool = True,
    platform: str | None = None,
    request_key: str | None = None,
    workspace_id: str | None = None,
    steer_job_id: str | None = None,
    agent: AgentSteer | None = None,
) -> tuple[bool, str | HandOffSteered | None]:
    """Admit a new turn, steer the running one, or 409.

    The single admission decision for both the foreground and dispatched
    paths. Returns ``(ready, steering_event)``: ``ready=True`` → start a new
    workflow; ``ready=False`` with a non-None ``steering_event`` → yield that
    (an SSE string, or a hand-off's ``HandOffSteered``) and return. A
    non-admissible state raises HTTP 409 whose detail carries a structured
    ``admission_conflict_detail`` code (surfaced as an SSE ``error`` event
    once the stream has started).

    A steer is a ``hook_outbox`` row on the thread, admitted only while the
    run it names is in_progress with no stop requested, in the same
    statement that reads the run. Whatever run reads it next takes it, so
    one that misses this run becomes the next turn instead of being lost.
    The row's unique key is the send's ``request_key``, so two copies of a
    send steer once.

    ``steer_only=True`` (gateway steer probes) forbids the fresh-admission
    fallback: the probe's SSE reader only understands ``steering_accepted``/
    ``error``, so admitting a full turn on that connection streams the whole
    response, interrupts included, into a client that ignores it. A steer
    that finds no running turn gets ``not_running`` and the gateway resubmits
    it as a normal message.

    ``can_steer=False`` (whatever ``steer_allowed`` refuses) forbids
    steering entirely: any in-flight run is a hard conflict, never a steer.
    ``steer_job_id`` is the parked steer a server-posted turn carries, whose
    own row is not a copy of it. ``agent`` is a Chief of Staff hand-off:
    accepted, it stamps the run with its report-back origin and answers with
    ``HandOffSteered``, naming the run and the steer, for the dispatch
    primer, which has no SSE reader, as does a copy of it that finds its
    steer still one; a run that reports to another Home thread is a 409.

    Admission states (see ``LocalRunExecutor.wait_for_admission``):
    - ``"fresh"``     → start a new turn ``(True, None)``; with ``steer_only``,
      409 ``not_running`` instead.
    - ``"stopping"``/``"compacting"`` → 409 (never start a second checkpoint
      writer, never steer mid-summarize).
    - ``"running"``   → steer it; a run that ended or was stopped before the
      steer landed sends admission round again.
    """
    from src.server.database.runs import lifecycle as tl_db
    from src.server.database.runs import outbox as outbox_db
    from src.server.handlers.chat.steering import steer_payload

    for _lap in range(_STEER_ADMISSION_LAPS):
        state, active_row = await manager.wait_for_admission(thread_id)
        # A START that committed before that read, or a steer admitted
        # before it, is visible from here on, whatever worker made it.
        await dedup_retransmit_or_raise(
            thread_id, request_key, except_job_id=steer_job_id
        )
        if state == "fresh":
            if steer_only:
                raise HTTPException(
                    status_code=409, detail=admission_conflict_detail("not_running")
                )
            return True, None

        # Only a genuinely-running turn on a steerable path can be steered;
        # every other non-fresh state, and every request ``steer_allowed``
        # refuses, is a conflict. Steering mid-stopping would start a second
        # checkpoint writer; mid-compaction would corrupt the context rewrite.
        if state != "running" or not can_steer or active_row is None:
            raise HTTPException(
                status_code=409, detail=admission_conflict_detail(state)
            )

        active_run_id = str(active_row["conversation_response_id"])
        payload = steer_payload(
            user_input,
            user_id,
            sender=agent.sender if agent else "user",
            workspace_id=workspace_id,
            platform=platform,
            request_key=request_key,
            origin=agent.origin if agent else None,
        )
        if agent is not None:
            idempotency_key = agent.idempotency_key
        elif request_key:
            idempotency_key = steer_idempotency_key(request_key)
        else:
            idempotency_key = f"steer:{uuid4()}"
        result = await outbox_db.admit_steer(
            thread_id=thread_id,
            run_id=active_run_id,
            payload=payload,
            idempotency_key=idempotency_key,
            origin_stamp=agent.origin_stamp if agent else None,
        )
        if result["state"] == "accepted" and agent is not None:
            from src.server.handlers.chat.request_prep import HandOffSteered

            return False, HandOffSteered(active_run_id, result["job_id"])
        if result["state"] == "accepted":
            event_data = json.dumps(
                {
                    "thread_id": thread_id,
                    "content": payload["content"],
                    "position": result["position"],
                }
            )
            return False, f"event: steering_accepted\ndata: {event_data}\n\n"
        if result["state"] == "duplicate":
            # Another copy of this send steered between the recheck and here.
            job = result["job"]
            run_id, steered = steer_job_run_id(job)
            run = await tl_db.get_run(run_id) if run_id else None
            if run is None:
                raise HTTPException(
                    status_code=409, detail=admission_conflict_detail("running")
                )
            if steered and agent is not None:
                # The primer answers it as it answered the copy that steered.
                from src.server.handlers.chat.request_prep import HandOffSteered

                return False, HandOffSteered(
                    str(run["conversation_response_id"]), str(job["id"])
                )
            raise tl_db.DuplicateRequestError(run, steered=steered)
        if result["state"] != "not_running":
            # ``owned_elsewhere``: the run reports back to another Home
            # thread, so this one's hand-off cannot ride it.
            raise HTTPException(
                status_code=409, detail=admission_conflict_detail("running")
            )
        # The run ended or was stopped between the admission read and the
        # insert: nothing was written, so read admission again.

    raise HTTPException(status_code=409, detail=admission_conflict_detail("running"))
