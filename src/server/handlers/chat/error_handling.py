"""Error classification and the terminal error funnel for chat workflows.

``classify_error`` decides recoverable vs non-recoverable;
``handle_workflow_error`` is the one in-generator error surface for both
agent modes — it routes protocol responses (admission conflicts, START-txn
refusals) past persistence, and finalizes genuinely-failed runs through
the TurnCoordinator CAS.
"""

from __future__ import annotations

import json
import logging
import time
from collections.abc import AsyncGenerator
from typing import TYPE_CHECKING

import psycopg
from fastapi import HTTPException
from ptc_agent.core.sandbox.runtime import SandboxGoneError, SandboxTransientError

from src.config.settings import get_max_workflow_retries
from src.server.database import conversation as qr_db
from src.server.dependencies.usage_limits import release_burst_slot

from .admission import ADMISSION_CONFLICT_CODES, admission_conflict_detail

if TYPE_CHECKING:
    from src.server.models.chat import ChatRequest

# Hard-coded logger name for backward-compat with existing log routing.
logger = logging.getLogger("src.server.handlers.chat_handler")


def _classify_non_recoverable_error_type(e: Exception) -> str:
    """Map a non-recoverable exception to a structured ``error_type`` label.

    Channel gateways switch on this label to surface user-actionable
    messages (e.g. "this thread's workspace is gone — start fresh") instead
    of opaque tracebacks. Defaults to ``"workflow_error"`` for unrecognized
    cases so existing consumers keep working.
    """
    if isinstance(e, (ValueError, RuntimeError)):
        msg = str(e)
        if "Workspace" in msg:
            if "not found" in msg:
                return "workspace_not_found"
            if "has been deleted" in msg:
                return "workspace_deleted"
            if "error state" in msg:
                return "workspace_error_state"
            return "workspace_unavailable"
    return "workflow_error"


def classify_error(e: Exception) -> dict:
    """Classify an exception as recoverable or non-recoverable.

    Returns ``{is_recoverable, is_non_recoverable, error_type}`` where
    ``error_type`` is one of ``"connection_error"``, ``"timeout_error"``,
    ``"api_error"``, ``"transient_error"``, or ``None`` for non-recoverable.
    """
    # Non-recoverable error types (code bugs, config issues)
    non_recoverable_types = (
        AttributeError,
        NameError,
        SyntaxError,
        ImportError,
        TypeError,
        KeyError,
    )

    is_non_recoverable = isinstance(e, non_recoverable_types)

    # Recoverable error patterns (transient issues)
    is_sandbox_error = isinstance(e, (SandboxTransientError, SandboxGoneError))

    is_postgres_connection = isinstance(
        e, psycopg.OperationalError
    ) and "server closed the connection" in str(e)

    is_timeout = (
        isinstance(e, TimeoutError)
        or "timeout" in str(e).lower()
        or "timed out" in str(e).lower()
    )

    is_network_issue = (
        isinstance(e, ConnectionError)
        or "connection" in str(e).lower()
        or "network" in str(e).lower()
        or "unreachable" in str(e).lower()
        or "connection refused" in str(e).lower()
    )

    # API errors (transient server errors, rate limits, etc.)
    error_str = str(e).lower()
    error_type_name = type(e).__name__.lower()

    api_error_indicators = [
        "internal server error",
        "api_error",
        "system error",
        "error code: 500",
        "error code: 502",
        "error code: 503",
        "error code: 429",
        "rate limit",
        "service unavailable",
        "bad gateway",
        "gateway timeout",
    ]

    is_api_error = (
        any(indicator in error_str for indicator in api_error_indicators)
        or "internal" in error_type_name
        or "api" in error_type_name
        or "server" in error_type_name
    )

    is_recoverable = (
        is_sandbox_error
        or is_postgres_connection
        or is_timeout
        or is_network_issue
        or is_api_error
    ) and not is_non_recoverable

    # Determine specific error_type label
    if is_recoverable:
        if is_sandbox_error:
            error_type = "transient_error"
        elif is_postgres_connection or is_network_issue:
            error_type = "connection_error"
        elif is_timeout:
            error_type = "timeout_error"
        elif is_api_error:
            error_type = "api_error"
        else:
            error_type = "transient_error"
    else:
        error_type = None

    return {
        "is_recoverable": is_recoverable,
        "is_non_recoverable": is_non_recoverable,
        "error_type": error_type,
    }


def _emit_sse_error(handler, payload: dict) -> str:
    """Format an ``error`` SSE frame via *handler* when present, else raw.

    Single source for the ``if handler: _format_sse_event else: raw f-string``
    shape repeated across ``handle_workflow_error``'s terminal branches.
    """
    if handler:
        return handler._format_sse_event("error", payload)
    return f"event: error\ndata: {json.dumps(payload)}\n\n"


async def handle_workflow_error(
    e: Exception,
    thread_id: str,
    user_id: str,
    workspace_id: str | None,
    handler,
    token_callback,
    run_handle,
    start_time: float,
    request: ChatRequest,
    is_byok: bool,
    msg_type: str,
    log_prefix: str,
    timezone_str: str | None = None,
) -> AsyncGenerator[str, None]:
    """Handle a workflow exception: classify, retry-or-fail, finalize, yield SSE events.

    This is an async generator that yields SSE event strings (``retry`` or
    ``error``).  Call it with ``async for event in handle_workflow_error(...): yield event``.

    ``run_handle`` is the STARTed run to finalize, or ``None`` when the error
    fired before START — or after handoff to BTM, whose ``_finalize_run``
    owns the terminal write from that point (callers pass
    ``run_handle if slot_owned else None`` to encode exactly that).
    ``workspace_id`` accepts ``None`` to guard against the case where the
    error occurred before the workspace was resolved.
    ``timezone_str`` is the resolved timezone; falls back to ``request.timezone``.
    """
    from src.server.database.subagent_runs import TaskRunSlotBusyError
    from src.server.services.turn_lifecycle import (
        AttemptConflictError,
        DuplicateRequestError,
        RunSlotBusyError,
        TurnCoordinator,
        TurnOutcome,
        protected_finalize,
    )

    # Metadata for persistence calls — built from parameters alone, up here so
    # ``_finalize_error`` never closes over a cell assigned below its def.
    persist_metadata = {
        "msg_type": msg_type,
        "is_byok": is_byok,
    }
    if workspace_id is not None:
        persist_metadata["workspace_id"] = workspace_id
    # Prefer request.workspace_id when available (PTC sets it on the request)
    if hasattr(request, "workspace_id") and request.workspace_id:
        persist_metadata["workspace_id"] = request.workspace_id
    if hasattr(request, "locale") and request.locale:
        persist_metadata["locale"] = request.locale
    # Use the resolved timezone_str (validated/defaulted) when available,
    # falling back to the raw request field.
    _tz = timezone_str or getattr(request, "timezone", None)
    if _tz:
        persist_metadata["timezone"] = _tz

    async def _finalize_error(error_msg: str, extra_metadata: dict) -> bool:
        """Terminal-write the open run as error; CRITICAL on failure (row
        stays in_progress for recovery rather than masking the persist).

        Returns True when the durable cancel intent won inside the CAS (or
        the lost-race survivor is cancelled): the user asked this run to
        stop, so the caller must present no failure surface — no error/retry
        SSE (the error detail stays on the row). False = the run is
        terminally errored (or nothing was finalized) — proceed with the
        failure surface.

        Runs via ``protected_finalize``: this generator lives on the
        client-stream task, so a disconnect-injected CancelledError must not
        abort the terminal transaction mid-flight."""
        if run_handle is None or run_handle.finalized:
            return False
        records = token_callback.per_call_records if token_callback else None
        tools = handler.get_tool_usage() if handler else None
        if not (run_handle.workspace_id and run_handle.user_id):
            records = None
            tools = None
        try:
            result = await protected_finalize(
                TurnCoordinator.get_instance().finalize_turn(
                    run_handle,
                    TurnOutcome(
                        status="error",
                        metadata={**persist_metadata, **extra_metadata},
                        errors=[error_msg],
                        execution_time=time.time() - start_time,
                        sse_events=handler.get_sse_events() if handler else None,
                        per_call_records=records,
                        tool_usage=tools,
                    ),
                ),
                label=run_handle.run_id,
            )
        except Exception:
            logger.critical(
                f"[{log_prefix}] FINALIZE FAILED for run={run_handle.run_id}: "
                f"row remains in_progress for recovery",
                exc_info=True,
            )
            return False
        if (result.run or {}).get("status") != "cancelled":
            return False
        return True

    # An admission-conflict 409 is a deliberate protocol response (raised
    # in-generator by ``wait_or_steer`` for both the foreground and dispatched
    # paths), not a workflow execution failure. Surface it to the client as an
    # SSE error, but never finalize it as a conversation error: this path
    # runs with ``run_handle=None`` — the open run (if any) belongs to a
    # concurrently-running peer turn, and failing it would clobber that
    # peer. Keyed on the structured admission CODE, not the bare 409 status:
    # every admission 409 now carries ``detail={"code", ...}`` (see
    # ``admission_conflict_detail`` / ``wait_or_steer``), so any other
    # HTTPException — a 503, or even a future non-admission 409 from middleware —
    # falls through to the finalize + classify path below.
    if (
        isinstance(e, HTTPException)
        and e.status_code == 409
        and isinstance(e.detail, dict)
        and e.detail.get("code") in ADMISSION_CONFLICT_CODES
    ):
        await release_burst_slot(user_id, getattr(request, "burst_slot_id", None))
        # Normally pre-START (run_handle is None). The flash RuntimeError
        # fallback can 409 after START, though — release the durable slot so
        # it doesn't leak until the stale-run sweep.
        if run_handle is not None:
            await TurnCoordinator.get_instance().fail_open_run(
                run_handle, "admission conflict after START", status="cancelled"
            )
        # The guard above already proved e.detail is a dict whose "code" is in
        # ADMISSION_CONFLICT_CODES (truthy), so no re-checking is needed here.
        detail = e.detail
        error_payload = {
            "thread_id": thread_id,
            "error": detail.get("message"),
            "type": "workflow_error",
            "error_type": "admission_conflict",
            "error_class": type(e).__name__,
            "code": detail["code"],
        }
        yield _emit_sse_error(handler, error_payload)
        return

    # START-txn conflicts (v4): the durable ledger refused this attempt via a
    # DB constraint. Protocol responses, not workflow failures — START rolled
    # back, so nothing was persisted for THIS request (run_handle is None) and
    # there is nothing to finalize.
    if isinstance(e, DuplicateRequestError):
        await release_burst_slot(user_id, getattr(request, "burst_slot_id", None))
        existing = e.existing_run or {}
        error_payload = {
            "thread_id": thread_id,
            "error": (
                "This request was already accepted; reconnect to the "
                "existing run instead of resending."
            ),
            "type": "workflow_error",
            "error_type": "duplicate_request",
            "error_class": type(e).__name__,
            "code": "duplicate_request",
        }
        # Two users can race the same client-controlled key: disclose the
        # winner's identity only to its owner, failing closed on unknown.
        # run_thread_id may differ from the ambient thread_id: an
        # initial-message retransmit that raced past the route probe minted
        # a second thread before START refused it — the existing run lives
        # on the FIRST thread, and reconnects must target that one.
        ex_thread = str(existing.get("conversation_thread_id") or "")
        owner_id = None
        if ex_thread:
            try:
                owner_id = await qr_db.get_thread_owner_id(ex_thread)
            except Exception:
                owner_id = None
        if owner_id is not None and owner_id == user_id:
            error_payload.update(
                run_id=str(existing.get("conversation_response_id") or ""),
                run_status=existing.get("status"),
                run_thread_id=ex_thread,
            )
        yield _emit_sse_error(handler, error_payload)
        return

    # TaskRunSlotBusyError reaches here from the fork guard: a background task
    # run still owns a response row the truncation would delete. Same remedy
    # as a live root run — wait, then retry — so it gets the same 409 detail.
    # QueryConflictError is the loser of an unfenced cross-instance race (two
    # servers sharing app tables with the guard disabled): its START rolled
    # back on the differing-content query collision, so it is a protocol 409
    # like its siblings, never a workflow failure.
    if isinstance(
        e,
        (
            RunSlotBusyError,
            TaskRunSlotBusyError,
            AttemptConflictError,
            qr_db.QueryConflictError,
        ),
    ):
        await release_burst_slot(user_id, getattr(request, "burst_slot_id", None))
        if isinstance(e, (RunSlotBusyError, TaskRunSlotBusyError)):
            detail = admission_conflict_detail("running")
        elif isinstance(e, qr_db.QueryConflictError):
            detail = {
                "code": "turn_conflict",
                "message": (
                    "A concurrent request already wrote a different query "
                    "at this turn; refresh the thread and resend."
                ),
            }
        else:
            detail = {
                "code": "attempt_conflict",
                "message": "A concurrent request already claimed this attempt.",
            }
        error_payload = {
            "thread_id": thread_id,
            "error": detail["message"],
            "type": "workflow_error",
            "error_type": "admission_conflict",
            "error_class": type(e).__name__,
            "code": detail["code"],
        }
        yield _emit_sse_error(handler, error_payload)
        return

    # Pinned-session budget/lock refusal (I2): bounded capacity signal, raised
    # by START before anything durable exists — retryable, nothing to finalize.
    from src.server.services.writer_guard import WriterGuardUnavailable

    if isinstance(e, WriterGuardUnavailable):
        await release_burst_slot(user_id, getattr(request, "burst_slot_id", None))
        error_payload = {
            "thread_id": thread_id,
            "error": (
                "The server is at its concurrent-run capacity; please retry "
                "in a moment."
            ),
            "type": "workflow_error",
            "error_type": "capacity_limit",
            "error_class": type(e).__name__,
            "code": "writer_capacity",
        }
        yield _emit_sse_error(handler, error_payload)
        return

    # A (platform, external_id) create race won by a DIFFERENT user is a
    # deterministic protocol conflict, not a workflow execution failure: thread
    # creation lost the ``idx_conversation_threads_external`` check. Surface it
    # as a clean SSE error carrying the same structured ``error_type`` as the
    # stamp API's HTTP 409, and (like the admission-conflict path above) never
    # finalize it as a turn failure. Thread creation runs inside the
    # already-started stream, so this is the response surface — a synchronous
    # HTTP 409 status is not reachable from here.
    if isinstance(e, qr_db.ExternalIdConflictError):
        await release_burst_slot(user_id, getattr(request, "burst_slot_id", None))
        # Same core fields (error_type discriminator + offending pair + human
        # wording) as the stamp API's HTTP 409, built from the shared helper so
        # the two surfaces never drift; the SSE-envelope fields (thread_id, type,
        # error_class) are layered on top.
        conflict = qr_db.external_id_conflict_payload(e.platform, e.external_id)
        error_payload = {
            "thread_id": thread_id,
            "error": conflict["message"],
            "type": "workflow_error",
            "error_type": conflict["error_type"],
            "error_class": type(e).__name__,
            "platform": conflict["platform"],
            "external_id": conflict["external_id"],
        }
        yield _emit_sse_error(handler, error_payload)
        return

    MAX_RETRIES = get_max_workflow_retries()

    # Release burst slot on error (setup errors before background task starts)
    await release_burst_slot(user_id, getattr(request, "burst_slot_id", None))

    classification = classify_error(e)
    is_recoverable = classification["is_recoverable"]
    error_type = classification["error_type"]

    if is_recoverable:
        # v4: the retry count IS the attempt chain — this run's attempt_no,
        # durable and race-free (the Redis increment_retry_count counter is
        # gone). Post-handoff calls have no handle; those errors surface via
        # BTM's finalize, so 1 is only a display fallback here.
        retry_count = run_handle.attempt_no if run_handle else 1

        if retry_count > MAX_RETRIES:
            logger.error(
                f"[{log_prefix}] Max retries exceeded ({retry_count}/{MAX_RETRIES}) for "
                f"thread_id={thread_id}: {type(e).__name__}: {str(e)[:100]}"
            )

            error_msg = (
                f"Max retries exceeded ({retry_count}/{MAX_RETRIES}): "
                f"{type(e).__name__}: {str(e)}"
            )

            if await _finalize_error(
                error_msg,
                {
                    "error_type": error_type,
                    "error_class": type(e).__name__,
                    "retry_count": retry_count,
                },
            ):
                return  # durable cancel won — no failure surface

            error_data = {
                "message": f"Workflow failed after {MAX_RETRIES} retry attempts",
                "error_type": error_type,
                "error_class": type(e).__name__,
                "retry_count": retry_count,
                "max_retries": MAX_RETRIES,
                "thread_id": thread_id,
            }
            yield f"event: error\ndata: {json.dumps(error_data)}\n\n"
        else:
            logger.warning(
                f"[{log_prefix}] Recoverable error ({error_type}) for thread_id={thread_id} "
                f"(retry {retry_count}/{MAX_RETRIES}): "
                f"{type(e).__name__}: {str(e)[:100]}"
            )

            # v4: the run itself is terminal (error, retryable) — leaving it
            # in_progress would hold the thread's run slot with no executor.
            # The 1.4 /retry redesign starts attempt N+1 from this row.
            # Finalize BEFORE the retry SSE: a client disconnect while this
            # generator is suspended at the yield would otherwise strand the
            # run in_progress with no executor.
            if await _finalize_error(
                f"{type(e).__name__}: {str(e)}",
                {
                    "error_type": error_type,
                    "error_class": type(e).__name__,
                    "retryable": True,
                    "retry_count": retry_count,
                },
            ):
                return  # durable cancel won — cancelled, not retryable: no retry SSE

            retry_data = {
                "message": "Temporary error occurred, you can retry or resume the workflow",
                "thread_id": thread_id,
                "auto_retry": True,
                "error_type": error_type,
                "error_class": type(e).__name__,
                "retry_count": retry_count,
                "max_retries": MAX_RETRIES,
            }
            yield f"event: retry\ndata: {json.dumps(retry_data)}\n\n"

    else:
        # Non-recoverable error
        logger.exception(f"[{log_prefix.replace('CHAT', 'ERROR')}] thread_id={thread_id}: {e}")

        error_type_label = _classify_non_recoverable_error_type(e)
        # error_type/error_class ride the row so replay can reconstruct the
        # terminal error event (the yield below happens after this finalize).
        if await _finalize_error(
            str(e),
            {
                "error_type": error_type_label,
                "error_class": type(e).__name__,
            },
        ):
            return  # durable cancel won — no failure surface

        error_payload = {
            "thread_id": thread_id,
            "error": str(e),
            "type": "workflow_error",
            "error_type": error_type_label,
            "error_class": type(e).__name__,
        }
        yield _emit_sse_error(handler, error_payload)
