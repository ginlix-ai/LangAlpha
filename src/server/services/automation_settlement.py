"""
Settlement: the one place an automation firing ends.

A firing ends once. ``settle`` ends the row in one transaction with what the
firing leaves on its automation (the strike or its reset, and the schedule),
and only the writer whose transaction lands goes on to the webhook, the wait
notice and the metric. The executor, a user's skip, the run's finalize hook
and the scheduler's sweep can race to end the same firing; the loser finds it
settled and leaves it as it is.

Once a firing's turn reached the run ledger, the run's finalize settles it
through the hook outbox, on whichever worker drains the job, as the run's
ledger row says it ended: ``settle_by_run``. The sweep reads the same row for
a job that never ran.
"""

import asyncio
import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import StrEnum
from typing import Any, Dict, Literal, Optional

from src.observability import automation_executions, safe_add
from src.server.contracts.status import INTERRUPT_REASON_CREDIT_PAUSE
from src.server.database import automation as auto_db
from src.server.models.automation import (
    ExecutionStatus,
    FailureReason,
    PriceTriggerConfig,
    RetriggerMode,
    SkipReason,
)
from src.server.services.automation_excerpt import read_run_excerpt
from src.server.services.thread_lifecycle_feed import publish_automation_wait
from src.server.services.webhook_client import WebhookClient
from src.server.utils.error_sanitization import sanitize_error_text

logger = logging.getLogger(__name__)

INTERRUPTED_ERROR = "The server restarted during the run"
STOPPED_ERROR = "Stopped by the user"

_ERROR_MAX_CHARS = 500

# A provider's refusal of the credential that made the model call. Only a
# 401 says the credential itself is bad; a 403 is a working key refused one
# model or region, which the next run may not meet.
_KEY_REFUSED = 401
_AUTH_STATUSES = (401, 403)


class Outcome(StrEnum):
    COMPLETED = "completed"
    # The automation's own failure, which counts toward auto-disable.
    FAILED = "failed"
    # The provider rejected the user's own key. Every run fails the same way
    # until they replace it, so the automation switches off now rather than
    # at its limit.
    KEY_REJECTED = "key_rejected"
    # A usage limit refused the firing or paused its run. What to do about it
    # is the user's call (retry, pause, or change their plan), so it never
    # counts toward auto-disable and a schedule carries on. A one-shot price
    # alert ends instead: re-armed, it would fire into the same limit every
    # few minutes for as long as its condition holds.
    LIMITED = "limited"
    # Our outage, such as the credit gate's 503 when the quota service gives
    # no verdict, or a provider refusing the platform's own key. A restart
    # landing on a schedule window must not quietly switch off a user's
    # automation.
    FAILED_OURS = "failed_ours"
    # A restart cut the firing off: not the automation's failure either.
    INTERRUPTED = "interrupted"
    # The reader pressed Stop on the run. Their choice, so it reads like a
    # skip they made: no strike, and it counts as the firing. The failure
    # event still goes out, since it is the one terminal event a channel
    # clears its "started" notice on.
    STOPPED = "stopped"
    SKIPPED = "skipped"


@dataclass(frozen=True)
class _Policy:
    status: ExecutionStatus
    from_statuses: tuple[ExecutionStatus, ...]
    metric: str
    # "fuse": disable at once, whatever the count.
    strike: Optional[Literal["count", "fuse", "reset"]] = None
    # "close": the firing counts as the schedule's run.
    # "close_alert": it counts only for a price alert; a one-time automation
    #   keeps its schedule, so the reader can still give it a new time.
    # "rearm_price": a price alert goes back to watching; nothing else moves.
    schedule: Optional[Literal["close", "close_alert", "rearm_price"]] = None
    webhook: Optional[str] = None
    error: Optional[str] = None
    skip_reason: Optional[SkipReason] = None
    failure_reason: Optional[FailureReason] = None


_POLICIES: Dict[Outcome, _Policy] = {
    Outcome.COMPLETED: _Policy(
        status="completed",
        from_statuses=("running",),
        metric="success",
        strike="reset",
        schedule="close",
        webhook="automation.completed",
    ),
    Outcome.FAILED: _Policy(
        status="failed",
        from_statuses=("pending", "waiting", "running"),
        metric="failure",
        strike="count",
        schedule="rearm_price",
        webhook="automation.failed",
    ),
    Outcome.KEY_REJECTED: _Policy(
        status="failed",
        from_statuses=("pending", "waiting", "running"),
        metric="failure",
        strike="fuse",
        schedule="rearm_price",
        webhook="automation.failed",
        failure_reason="provider_auth",
    ),
    Outcome.LIMITED: _Policy(
        status="failed",
        from_statuses=("pending", "waiting", "running"),
        metric="limited",
        schedule="close_alert",
        webhook="automation.failed",
        failure_reason="usage_limit",
    ),
    Outcome.FAILED_OURS: _Policy(
        status="failed",
        from_statuses=("pending", "waiting", "running"),
        metric="failure",
        schedule="rearm_price",
        webhook="automation.failed",
        failure_reason="server_error",
    ),
    # A wait the server stopped is skipped by its own wait loop, or by the
    # sweep, never failed.
    Outcome.INTERRUPTED: _Policy(
        status="failed",
        from_statuses=("pending", "running"),
        metric="interrupted",
        schedule="rearm_price",
        webhook="automation.failed",
        error=INTERRUPTED_ERROR,
        failure_reason="interrupted",
    ),
    Outcome.STOPPED: _Policy(
        status="skipped",
        from_statuses=("running",),
        metric="stopped",
        schedule="close",
        webhook="automation.failed",
        error=STOPPED_ERROR,
        skip_reason="user",
    ),
    # Not a failure: no strike, no webhook. It still counts as the firing,
    # so the schedule moves on as it would after a run.
    Outcome.SKIPPED: _Policy(
        status="skipped",
        from_statuses=("waiting",),
        metric="skipped",
        schedule="close",
    ),
}


# The outcomes a run's own ledger row explains, in ``run_failure_message``.
RUN_FAILURES = frozenset(
    {Outcome.FAILED, Outcome.KEY_REJECTED, Outcome.LIMITED, Outcome.FAILED_OURS}
)


def ledger_outcome(run: Optional[Dict[str, Any]]) -> Optional[Outcome]:
    """How a firing ended, by its admitted run's ledger row; None while the
    run is still going.

    A cancel the reader did not ask for is a server stopping, and an error
    written by the ledger's recovery is a worker lost: both are ours. A run
    the credit gate paused hit a usage limit. A model call refused 401/403
    is our outage when the key was the platform's; a row that does not say
    whose it was is treated as ours, so no disable rests on a guess. On the
    user's own key a 401 is the key rejected and a 403 an ordinary failure. A run paused on any other
    question counts as a failure, as it does when the executor reads it.
    """
    if run is None:
        return Outcome.INTERRUPTED
    status = run["status"]
    if status == "in_progress":
        return None
    if status == "completed":
        return Outcome.COMPLETED
    metadata = run.get("metadata") or {}
    if status == "cancelled":
        return Outcome.STOPPED if metadata.get("cancelled_by_user") else Outcome.INTERRUPTED
    if status == "interrupted" and run.get("interrupt_reason") == INTERRUPT_REASON_CREDIT_PAUSE:
        return Outcome.LIMITED
    if status == "error":
        if metadata.get("recovery"):
            return Outcome.INTERRUPTED
        code = metadata.get("error_status_code")
        if code in _AUTH_STATUSES:
            if metadata.get("error_credential_owned") is not True:
                return Outcome.FAILED_OURS
            if code == _KEY_REFUSED:
                return Outcome.KEY_REJECTED
    return Outcome.FAILED


def clean_error_text(text: str) -> str:
    """Error text fit to persist and send: credential-shaped values scrubbed
    first, so a cut can never leave half a secret behind, then cut."""
    return sanitize_error_text(text)[:_ERROR_MAX_CHARS]


def run_failure_message(run: Dict[str, Any]) -> Optional[str]:
    """What the user reads about a run that did not complete: its own last
    error, else a line naming it.

    A credit pause records no error. Its words are the quota service's
    denial, which the pause's interrupt carries on the row, and they are
    relayed verbatim or not at all.
    """
    if run.get("interrupt_reason") == INTERRUPT_REASON_CREDIT_PAUSE:
        return _pause_message(run.get("sse_events"))
    errors = run.get("errors")
    if errors:
        return clean_error_text(str(errors[-1]))
    return (
        f"workflow run finished as '{run['status']}' "
        f"(run_id={run['conversation_response_id']})"
    )


def _pause_message(events: Any) -> Optional[str]:
    for event in reversed(events if isinstance(events, list) else []):
        if not isinstance(event, dict) or event.get("event") != "interrupt":
            continue
        data = event.get("data")
        requests = data.get("action_requests") if isinstance(data, dict) else None
        for request in requests if isinstance(requests, list) else ():
            if (
                isinstance(request, dict)
                and request.get("type") == INTERRUPT_REASON_CREDIT_PAUSE
                and isinstance(request.get("message"), str)
            ):
                return request["message"]
    return None


def _one_shot_price(automation: Dict[str, Any]) -> bool:
    try:
        config = PriceTriggerConfig(**(automation.get("trigger_config") or {}))
    except Exception as e:
        # Re-arming is the move that loses nothing if the config is wrong.
        logger.warning(
            f"[AUTOMATION_SETTLE] Unreadable price trigger on "
            f"automation_id={automation['automation_id']}, re-arming: {e}"
        )
        return False
    return config.retrigger.mode == RetriggerMode.ONE_SHOT


def _schedule_action(
    policy: _Policy, automation: Dict[str, Any]
) -> Optional[auto_db.ScheduleAction]:
    """What the firing leaves on the schedule: a one-time automation is
    done, and a price alert is either done or watching again."""
    trigger = automation.get("trigger_type")
    if trigger == "price" and policy.schedule is not None:
        if policy.schedule != "rearm_price" and _one_shot_price(automation):
            return "close_price"
        return "rearm_price"
    if trigger == "once" and policy.schedule == "close":
        return "close_once"
    return None


async def settle(
    automation: Dict[str, Any],
    execution_id: str,
    outcome: Outcome,
    *,
    thread_id: Optional[str] = None,
    run_id: Optional[str] = None,
    workspace_id: Optional[str] = None,
    error: Optional[str] = None,
    skip_reason: Optional[SkipReason] = None,
    excerpt: Optional[str] = None,
    quiet_for: Optional[int] = None,
) -> bool:
    """End a firing as ``outcome``; False when it was no longer in a state
    that outcome can end, because someone else settled it first.

    The execution must belong to ``automation``, which keeps the skip
    endpoint to its own automation's firings. ``quiet_for`` is the sweep's:
    settle only while the heartbeat is still that many seconds quiet.
    What follows the settled row never raises.
    """
    policy = _POLICIES[outcome]
    error = error or policy.error
    row = await auto_db.settle_execution(
        execution_id,
        automation_id=str(automation["automation_id"]),
        from_statuses=policy.from_statuses,
        to=policy.status,
        strike=policy.strike,
        schedule=_schedule_action(policy, automation),
        quiet_for=quiet_for,
        conversation_thread_id=thread_id,
        conversation_response_id=run_id,
        result_excerpt=excerpt,
        error_message=error,
        skip_reason=skip_reason or policy.skip_reason,
        failure_reason=policy.failure_reason,
        completed_at=datetime.now(timezone.utc),
    )
    if row is None:
        logger.info(
            f"[AUTOMATION_SETTLE] Not settling as {outcome}: "
            f"execution_id={execution_id} was already settled or moved on"
        )
        return False
    logger.info(
        f"[AUTOMATION_SETTLE] Settled as {outcome}: execution_id={execution_id}"
        + (f" reason={skip_reason}" if skip_reason else "")
    )
    recorded_thread = row["conversation_thread_id"]
    recorded_run = row["conversation_response_id"]
    thread_id = thread_id or (str(recorded_thread) if recorded_thread else None)
    run_id = run_id or (str(recorded_run) if recorded_run else None)

    # Shielded: a cancel landing after the commit, such as shutdown stopping
    # the sweep, must not drop the notice the commit made this writer's to
    # send, since no later sweep finds the row to send it again.
    tail = asyncio.create_task(
        _after_settling(outcome, automation, execution_id, row,
                        thread_id, run_id, workspace_id, error),
        name=f"settle_tail_{execution_id}",
    )
    _tails.add(tail)
    tail.add_done_callback(_tails.discard)
    await asyncio.shield(tail)
    return True


# Held so a tail a cancel left running is not collected mid-webhook, and so
# shutdown can wait for it (``drain_tails``).
_tails: set[asyncio.Task] = set()

# One webhook's timeout (``WebhookClient.fire``) plus the writes after it.
TAIL_DRAIN_SECONDS = 20.0


async def drain_tails(timeout: float = TAIL_DRAIN_SECONDS) -> None:
    """Wait, up to ``timeout``, for this process's tails still running.

    A tail cut off by shutdown is lost: its row is already settled, so no
    sweep finds it to send the webhook or the notice again.
    """
    if not _tails:
        return
    _, pending = await asyncio.wait(set(_tails), timeout=timeout)
    if pending:
        logger.warning(
            f"[AUTOMATION_SETTLE] {len(pending)} settlement tail(s) still "
            f"running after {timeout:g}s at shutdown: "
            f"{sorted(t.get_name() for t in pending)}"
        )


async def _after_settling(
    outcome: Outcome,
    automation: Dict[str, Any],
    execution_id: str,
    row: Dict[str, Any],
    thread_id: Optional[str],
    run_id: Optional[str],
    workspace_id: Optional[str],
    error: Optional[str],
) -> None:
    """The webhook, the chat's wait line and the metric a settle sets off."""
    policy = _POLICIES[outcome]
    if policy.webhook and not _repeats_a_refusal(policy.failure_reason, run_id, row):
        delivery_result = await WebhookClient().fire_event(
            policy.webhook, automation, execution_id, thread_id, workspace_id,
            error=error, run_id=run_id, failure_reason=policy.failure_reason,
        )
        if delivery_result is not None:
            try:
                await auto_db.record_delivery(execution_id, delivery_result)
            except Exception as e:
                logger.error(
                    f"[AUTOMATION_SETTLE] Recording delivery failed: "
                    f"execution_id={execution_id} error={e}"
                )
    if row["settled_from"] == "waiting" and thread_id:
        await publish_automation_wait(
            user_id=automation["user_id"],
            thread_id=thread_id,
            automation_execution_id=execution_id,
            waiting=False,
        )
    safe_add(
        automation_executions,
        1,
        {"status": policy.metric, "trigger": automation.get("trigger_type") or "unknown"},
    )


# Reasons that refuse firing after firing until something outside the
# automation changes: a usage limit until it resets, our outage until it ends.
_STREAK_REASONS = frozenset({"usage_limit", "server_error"})


def _repeats_a_refusal(
    reason: Optional[FailureReason], run_id: Optional[str], row: Dict[str, Any]
) -> bool:
    """This firing was refused for the same streak reason as the one before,
    which the channel already heard about, so a frequent schedule cannot
    flood it.

    Only a firing refused before its turn: one that was admitted sent a
    started notice, and only a terminal event clears it. A notice no channel
    took was not heard, so the next refusal sends it again.
    """
    delivered = row.get("previous_delivery_result")
    return (
        reason in _STREAK_REASONS
        and run_id is None
        and row.get("previous_failure_reason") == reason
        and not (delivered and not any(d.get("success") for d in delivered))
    )


async def settle_by_run(
    automation: Dict[str, Any],
    execution_id: str,
    run_id: Optional[str],
    run: Optional[Dict[str, Any]],
    *,
    thread_id: Optional[str],
    workspace_id: Optional[str],
    quiet_for: Optional[int] = None,
) -> Optional[Outcome]:
    """Settle a firing as ``run``, its turn's ledger row, ended; the outcome,
    or None while the run is still going or once someone else settled it.

    The row decides, not the stream the executor drained (v4 2.4), and a run
    that failed says why in its own words. No row at all is a run the server
    lost.
    """
    outcome = ledger_outcome(run)
    if outcome is None:
        logger.debug(
            f"[AUTOMATION_SETTLE] Run still going, not settling: "
            f"execution_id={execution_id} run_id={run_id}"
        )
        return None
    error = excerpt = None
    if outcome in RUN_FAILURES:
        error = run_failure_message(run)
        log = logger.warning if outcome is Outcome.LIMITED else logger.error
        log(
            f"[AUTOMATION_SETTLE] Run ended as {outcome}: "
            f"execution_id={execution_id} run_id={run_id} error={error}"
        )
    elif outcome is Outcome.COMPLETED and thread_id:
        # Read now, while this run is still the thread's newest turn.
        excerpt = await read_run_excerpt(thread_id, run_id)
    settled = await settle(
        automation, execution_id, outcome,
        thread_id=thread_id, run_id=run_id, workspace_id=workspace_id,
        error=error, excerpt=excerpt, quiet_for=quiet_for,
    )
    return outcome if settled else None


async def settle_abandoned(
    automation: Dict[str, Any], row: Dict[str, Any], quiet_for: int
) -> Optional[Outcome]:
    """Settle a firing whose process stopped holding it; the outcome, or
    None when it was left alone or someone else settled it.

    A wait is skipped. Anything else settles by its run, and a firing that
    never had one linked was interrupted. A run still going is left to a
    later pass, since its worker may be draining it and the ledger's own
    recovery ends a run whose worker died.
    """
    execution_id = str(row["automation_execution_id"])
    thread_id = (
        str(row["conversation_thread_id"]) if row["conversation_thread_id"] else None
    )
    run_id = (
        str(row["conversation_response_id"])
        if row["conversation_response_id"]
        else None
    )
    workspace_id = row.get("workspace_id") or automation.get("workspace_id")
    if row["status"] == "waiting":
        settled = await settle(
            automation, execution_id, Outcome.SKIPPED,
            thread_id=thread_id, run_id=run_id, workspace_id=workspace_id,
            skip_reason="interrupted", quiet_for=quiet_for,
        )
        return Outcome.SKIPPED if settled else None
    return await settle_by_run(
        automation, execution_id, run_id,
        await auto_db.get_settling_run(run_id) if run_id else None,
        thread_id=thread_id, workspace_id=workspace_id, quiet_for=quiet_for,
    )


async def _settle_finished_run(job: Dict[str, Any]) -> None:
    """The ``automation_settle`` outbox job: settle the firing whose turn
    the finalized run was, as the run ended.

    Exactly once however often it runs: ``settle`` ends the execution row
    under its lock only from an unsettled status, so a retry, a reclaimed
    lease or the sweep getting there first finds it settled and sends
    nothing. A failure raises, for the drainer to retry.
    """
    payload = job.get("payload") or {}
    automation = await auto_db.get_automation(
        payload["automation_id"], payload.get("user_id")
    )
    if automation is None:
        # Deleted, and its firings with it.
        return
    run_id = str(job["run_id"])
    await settle_by_run(
        automation, payload["execution_id"], run_id,
        await auto_db.get_settling_run(run_id),
        thread_id=str(job["conversation_thread_id"]),
        workspace_id=payload.get("workspace_id"),
    )


def register_outbox_executors() -> None:
    """Bind the automation_settle executor. Must run before the drainer starts."""
    from src.server.services.hook_outbox import register_hook_executor

    register_hook_executor("automation_settle", _settle_finished_run)
