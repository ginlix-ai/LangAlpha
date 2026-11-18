"""How an automation firing's end is classified, with no I/O.

Each ``Outcome`` names one way a firing ends and its ``OutcomePolicy`` row
says what that end leaves: the execution's status, the strike, the schedule,
the webhook and the messaging service's finish. ``ledger_outcome`` reads the
outcome off a run's ledger row; settlement (``automation_settlement``) acts
on it.
"""

import logging
from dataclasses import dataclass
from enum import StrEnum
from typing import Any, Dict, Literal, Optional

from src.server.contracts.status import (
    GATEWAY_OUTAGE_META,
    INTERRUPT_REASON_CREDIT_PAUSE,
    is_restart_interrupted,
    is_suspension_stop,
    is_user_stop,
    restart_resume_expired,
)
from src.server.database import automation_executions as exec_db
from src.server.models.automation import (
    ExecutionStatus,
    FailureReason,
    PriceTriggerConfig,
    RetriggerMode,
    SkipReason,
)
from src.server.services import automation_delivery
from src.server.services.account import ACCOUNT_SUSPENDED
from src.server.utils.error_sanitization import sanitize_error_text

logger = logging.getLogger(__name__)

INTERRUPTED_ERROR = "The server restarted during the run"
STOPPED_ERROR = "Stopped by the user"
UNREACHABLE_ERROR = "The computer could not be reached"

_ERROR_MAX_CHARS = 500

# The run ledger's ``error_kind`` for a turn whose computer did not answer,
# as the turn's failure frame names it.
COMPUTER_UNREACHABLE = "computer_unreachable"

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
    # The Claude plan of the automation's Claude Code model refused the
    # firing or stopped its run, until it resets: a usage limit, but the
    # user's Claude plan rather than their LangAlpha plan, so it is told apart.
    PLAN_LIMITED = "plan_limited"
    # Another subscription's limit (ChatGPT, a Claude sign-in, Grok) refused
    # the run's model call until it resets: the same wait, told apart because
    # it is not the Claude Code plan.
    SUBSCRIPTION_LIMITED = "subscription_limited"
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
    # The computer the turn runs on did not answer: a machine of the user's
    # own asleep, offline or disconnected. Not the automation's doing, so it
    # reads as a skip for that reason, as a wait for the computer that ran
    # out does: no strike, and it counts as the firing.
    UNREACHABLE = "unreachable"
    SKIPPED = "skipped"
    # The account is suspended, at the firing or during its run. Nobody can
    # act on it but the organization, so it is no strike and tells no
    # channel. A schedule moves on as after a run, and a one-shot is put off
    # (``defer``), so the automation is as it was if the account returns.
    SUSPENDED = "suspended"


@dataclass(frozen=True)
class OutcomePolicy:
    status: ExecutionStatus
    from_statuses: tuple[ExecutionStatus, ...]
    metric: str
    # "fuse": disable at once, whatever the count.
    strike: Optional[Literal["count", "fuse", "reset"]] = None
    # "close": the firing counts as the schedule's run.
    # "close_alert": it counts only for a price alert; a one-time automation
    #   keeps its schedule, so the reader can still give it a new time.
    # "rearm_price": a price alert goes back to watching; nothing else moves.
    # "defer": a one-shot is not used up but fires again later
    #   (``exec_db.DEFER_INTERVAL``), a schedule moves on, a price alert that
    #   retriggers goes back to watching.
    schedule: Optional[Literal["close", "close_alert", "rearm_price", "defer"]] = None
    webhook: Optional[str] = None
    # How a run delivering through the messaging service ends there, in
    # place of the webhook: its answer to the chats the agent didn't reach,
    # or a notice to every chat.
    finish: Optional[automation_delivery.FinishStatus] = None
    error: Optional[str] = None
    skip_reason: Optional[SkipReason] = None
    failure_reason: Optional[FailureReason] = None


def _limit_policy(failure_reason: FailureReason) -> OutcomePolicy:
    """A usage limit's: no strike toward auto-disable, and a one-shot price
    alert closes, since the wait outlasts what it watched for."""
    return OutcomePolicy(
        status="failed",
        from_statuses=("pending", "waiting", "running"),
        metric="limited",
        schedule="close_alert",
        webhook="automation.failed",
        finish="failed",
        failure_reason=failure_reason,
    )


POLICIES: Dict[Outcome, OutcomePolicy] = {
    Outcome.COMPLETED: OutcomePolicy(
        status="completed",
        from_statuses=("running",),
        metric="success",
        strike="reset",
        schedule="close",
        webhook="automation.completed",
        finish="completed",
    ),
    Outcome.FAILED: OutcomePolicy(
        status="failed",
        from_statuses=("pending", "waiting", "running"),
        metric="failure",
        strike="count",
        schedule="rearm_price",
        webhook="automation.failed",
        finish="failed",
    ),
    Outcome.KEY_REJECTED: OutcomePolicy(
        status="failed",
        from_statuses=("pending", "waiting", "running"),
        metric="failure",
        strike="fuse",
        schedule="rearm_price",
        webhook="automation.failed",
        finish="failed",
        failure_reason="provider_auth",
    ),
    Outcome.LIMITED: _limit_policy("usage_limit"),
    Outcome.PLAN_LIMITED: _limit_policy("claude_plan"),
    Outcome.SUBSCRIPTION_LIMITED: _limit_policy("plan_limit"),
    Outcome.FAILED_OURS: OutcomePolicy(
        status="failed",
        from_statuses=("pending", "waiting", "running"),
        metric="failure",
        schedule="rearm_price",
        webhook="automation.failed",
        finish="failed",
        failure_reason="server_error",
    ),
    # A wait the server stopped is skipped by its own wait loop, or by the
    # sweep, never failed.
    Outcome.INTERRUPTED: OutcomePolicy(
        status="failed",
        from_statuses=("pending", "running"),
        metric="interrupted",
        schedule="rearm_price",
        webhook="automation.failed",
        finish="failed",
        error=INTERRUPTED_ERROR,
        failure_reason="interrupted",
    ),
    Outcome.STOPPED: OutcomePolicy(
        status="skipped",
        from_statuses=("running",),
        metric="stopped",
        schedule="close",
        webhook="automation.failed",
        finish="stopped",
        error=STOPPED_ERROR,
        skip_reason="user",
    ),
    # As a stop, the failure event still goes out: it is the one terminal
    # event a channel clears a started notice on.
    Outcome.UNREACHABLE: OutcomePolicy(
        status="skipped",
        from_statuses=("pending", "waiting", "running"),
        metric="unreachable",
        schedule="close",
        webhook="automation.failed",
        finish="failed",
        error=UNREACHABLE_ERROR,
        skip_reason="computer_unreachable",
    ),
    # Not a failure: no strike, no webhook. It still counts as the firing,
    # so the schedule moves on as it would after a run.
    Outcome.SKIPPED: OutcomePolicy(
        status="skipped",
        from_statuses=("waiting",),
        metric="skipped",
        schedule="close",
    ),
    Outcome.SUSPENDED: OutcomePolicy(
        status="skipped",
        from_statuses=("pending", "waiting", "running"),
        metric="suspended",
        schedule="defer",
    ),
}


# The outcomes a run's own ledger row explains, in ``run_failure_message``.
RUN_FAILURES = frozenset({
    Outcome.FAILED, Outcome.KEY_REJECTED, Outcome.LIMITED, Outcome.PLAN_LIMITED,
    Outcome.SUBSCRIPTION_LIMITED, Outcome.FAILED_OURS,
})
# The outcomes of a usage limit, which never count toward auto-disable.
LIMITS = frozenset(outcome for outcome, policy in POLICIES.items() if policy.metric == "limited")
# The ends a suspension may make of a run whose row cannot name it: a refusal
# on the way, read by a ledger that cannot tell the cause. The notice's own
# stop names itself (``is_suspension_stop``). A run that completed, or that
# the reader stopped, ended as it says, suspended since or not.
SUSPENSION_ENDS = frozenset({
    Outcome.FAILED, Outcome.FAILED_OURS, Outcome.INTERRUPTED, Outcome.LIMITED,
})


def ledger_outcome(run: Optional[Dict[str, Any]]) -> Optional[Outcome]:
    """How a firing ended, by its admitted run's ledger row; None while the
    run is still going.

    A run that ended because its computer did not answer was skipped for
    that, whichever role ran it. A run a restart stopped goes on in the turn that takes it over, for as
    long as one may (``carried_firing``). Any other cancel the reader did
    not ask for is a server stopping, an error
    written by the ledger's recovery is a worker lost, and one the tunnel
    gateway not answering ended is its outage: all are ours. A run
    the credit gate paused hit a usage limit, and one the Claude plan stopped
    (``claude_plan``, ``plan_stop_metadata``) the plan's, and one another
    subscription refused at its limit (``plan_limit``) that one's. A model call refused 401/403
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
    if status == "error" and metadata.get("error_kind") == COMPUTER_UNREACHABLE:
        return Outcome.UNREACHABLE
    if status == "cancelled":
        if is_restart_interrupted(run) and not restart_resume_expired(
            run["cancel_requested_at"]
        ):
            return None
        if is_suspension_stop(run):
            return Outcome.SUSPENDED
        return Outcome.STOPPED if is_user_stop(run) else Outcome.INTERRUPTED
    if status == "interrupted" and run.get("interrupt_reason") == INTERRUPT_REASON_CREDIT_PAUSE:
        return Outcome.LIMITED
    if status == "error":
        if metadata.get("recovery"):
            return Outcome.INTERRUPTED
        if metadata.get("error_type") == ACCOUNT_SUSPENDED:
            return Outcome.SUSPENDED
        if metadata.get(GATEWAY_OUTAGE_META):
            return Outcome.FAILED_OURS
        if metadata.get("claude_plan"):
            return Outcome.PLAN_LIMITED
        upstream = metadata.get("upstream") or {}
        if upstream.get("reason") == "plan_limit" and upstream.get("payer") == "subscription":
            return Outcome.SUBSCRIPTION_LIMITED
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
    denial, which every finalize stamps into the row's metadata, and they
    are relayed verbatim or not at all.
    """
    if run.get("interrupt_reason") == INTERRUPT_REASON_CREDIT_PAUSE:
        stamped = (run.get("metadata") or {}).get("credit_pause_message")
        return stamped if isinstance(stamped, str) else None
    errors = run.get("errors")
    if errors:
        return clean_error_text(str(errors[-1]))
    return (
        f"workflow run finished as '{run['status']}' "
        f"(run_id={run['conversation_response_id']})"
    )


def runs_once(automation: Dict[str, Any]) -> bool:
    """Whether the automation fires once: a one-time automation, or a price
    alert that does not re-arm. Nothing later stands in for a firing of one
    that never happens."""
    trigger = automation.get("trigger_type")
    if trigger != "price":
        return trigger == "once"
    try:
        config = PriceTriggerConfig(**(automation.get("trigger_config") or {}))
    except Exception as e:
        # Re-arming is the move that loses nothing if the config is wrong.
        logger.warning(
            f"[AUTOMATION_SETTLE] Unreadable price trigger on "
            f"automation_id={automation['automation_id']}, read as re-arming: {e}"
        )
        return False
    return config.retrigger.mode == RetriggerMode.ONE_SHOT


def schedule_action(
    policy: OutcomePolicy, automation: Dict[str, Any]
) -> Optional[exec_db.ScheduleAction]:
    """What the firing leaves on the schedule: a one-time automation is
    done, and a price alert is either done or watching again."""
    trigger = automation.get("trigger_type")
    if policy.schedule == "defer":
        if trigger == "price":
            return "defer_price" if runs_once(automation) else "rearm_price"
        return "defer_once" if trigger == "once" else None
    if trigger == "price" and policy.schedule is not None:
        if policy.schedule != "rearm_price" and runs_once(automation):
            return "close_price"
        return "rearm_price"
    if trigger == "once" and policy.schedule == "close":
        return "close_once"
    return None
