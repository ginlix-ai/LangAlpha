"""Public status vocabulary (v4 1.6) — the one set the API speaks.

Internal stores keep narrower vocabularies: run rows use
``in_progress/completed/interrupted/error/cancelled``, tracker blobs use
``active/completed/interrupted/cancelled/failed/unknown``, and the thread
projection is a legacy free-form string. This module is the boundary — every
status crossing to a client maps through :func:`to_public`, so internal
renames never leak and the frontend switches on exactly one vocabulary.
"""

from __future__ import annotations

from collections.abc import Mapping
from datetime import datetime, timedelta, timezone
from typing import Any, Literal, Optional

# The public vocabulary is a PARTITION, not a list: every status is live
# (still owed to the user), terminal (settled), or one of the two singletons —
# `interrupted` (awaits input) and `idle` (the no-run placeholder).
# PUBLIC_STATUSES derives from the families below, so adding a status forces
# choosing its family. The frontend mirrors these two sets by hand in
# web/src/lib/threadLifecycle/store.ts (LIVE_STATUSES / TERMINAL_FAMILY) —
# change here first, then the mirror.
LIVE_PUBLIC_STATUSES = ("queued", "running", "stopping", "recovering")
TERMINAL_PUBLIC_STATUSES = ("completed", "failed", "cancelled")

PUBLIC_STATUSES = frozenset(
    {"idle", "interrupted", *LIVE_PUBLIC_STATUSES, *TERMINAL_PUBLIC_STATUSES}
)

# The one internal terminal set — both run ledgers (runs/lifecycle,
# subagent_runs) import it, and the migration CHECK constraints are
# test-bound to it, so a new outcome cannot land in one store only.
TERMINAL_STATUSES = ("completed", "interrupted", "error", "cancelled")

# Task-run outcomes that owe the user a report-back. One notification policy,
# imported by every site that decides *whether a run is owed one*: the outbox
# producer (subagent_runs.finalize_task_run), its drainer nudge, and the
# ledger-path TaskOutput reply that settles the obligation. Errors report: a
# failure the user never hears about is a silently lost task. Cancelled
# doesn't: cancellation is intentful and its UX is the cancel act itself.
#
# Not the same set as "may be stamped delivered" — mark_result_delivered
# admits any terminal status, because reporting a stop to the agent is a
# delivery too (it is what keeps the sweep from re-announcing the task).
REPORT_BACK_STATUSES = ("completed", "error")

# Run-row spellings, for SQL that must classify without calling `to_public`.
# ⚠️ Run rows persist `error`; `failed` exists only downstream of `to_public`
# (migration CHECK, 001_initial_schema.py:341), so a NOT-IN filter over the
# public vocabulary would leave every errored run classified live forever.
# `interrupted` is live-LIKE here — it awaits the user, so the feed's live
# branch owns it and the terminal branch must not.
RAW_LIVE_STATUSES = ("in_progress", "interrupted")
RAW_TERMINAL_SNAPSHOT_STATUSES = ("completed", "error", "cancelled")

# Live-run internal spellings, refined by durable intent/liveness below.
_LIVE = ("in_progress", "active")

_LEGACY = {
    "error": "failed",  # run-row / thread-projection spelling
    "unknown": "idle",  # tracker's "no blob" placeholder
}


def to_public(
    raw: Any,
    *,
    cancel_requested_at: Any = None,
    has_executor: Optional[bool] = None,
) -> str:
    """Map any internal status to the public vocabulary.

    A live run refines by durable state: cancel intent → ``stopping``; known
    absence of a local executor (``has_executor=False``, tri-state — None
    means unknown) → ``recovering``. Unrecognized/absent values collapse to
    ``idle`` rather than leaking an internal spelling.
    """
    value = getattr(raw, "value", raw)
    if value is None:
        return "idle"
    value = str(value)
    if value in _LIVE:
        if cancel_requested_at is not None:
            return "stopping"
        if has_executor is False:
            return "recovering"
        return "running"
    mapped = _LEGACY.get(value, value)
    return mapped if mapped in PUBLIC_STATUSES else "idle"


def is_terminal(raw: Any) -> bool:
    """True iff the internal status is a settled run outcome."""
    value = getattr(raw, "value", raw)
    return value is not None and str(value) in TERMINAL_STATUSES


def is_live(raw: Any) -> bool:
    """True iff the internal status names a run still in flight."""
    value = getattr(raw, "value", raw)
    return value is not None and str(value) in _LIVE


# The cancel stamp of a stop the account's suspension asked for. It is the
# account's ending, not the user's choice: a run carrying it is no user stop
# (``is_user_stop``), and every reader that derives an outcome reads it as the
# suspension.
SUSPENSION_STOP_META = "stopped_by_suspension"
#: The cancel stamp of a stop by a member of a shared workspace other than the
#: turn's sender. Still a person's stop; only the sender's queued messages are
#: settled as at an agent's stop, kept for the sender rather than handed to the
#: member who stopped it.
MEMBER_STOP_META = "stopped_by_member"
#: ``run_end``'s ``stopped_by`` for such a run, and its replay's.
SUSPENSION_STOPPER = "suspension"

#: Who asked for a stop: the user (their own turn), an agent (the Chief of
#: Staff, a hosted Claude Code session, a removal from a shared workspace,
#: which no person asked for), the account's suspension, or another member of
#: a shared workspace than the turn's sender.
Stopper = Literal["user", "agent", "suspension", "member"]
_STOP_MARKS: dict[str, Optional[str]] = {
    "user": None,
    "agent": "stopped_by_agent",
    "suspension": SUSPENSION_STOP_META,
    "member": MEMBER_STOP_META,
}


def stops_mark(stopper: Stopper) -> Optional[str]:
    """The metadata key the cancel stamp sets on the run; none for the
    user's own stop."""
    return _STOP_MARKS[stopper]


def gives_back(stopper: Stopper) -> bool:
    """Whether the stop hands the user's queued steers back to its caller:
    only the user's own does. Any other leaves them for the run's ending to
    keep for its sender."""
    return stopper == "user"


def chases_hand_offs(stopper: Stopper) -> bool:
    """Whether a stop that finds the run already ended also stops the turn a
    hand-off steered into it went on to: an agent's or a suspension's, which
    stop the work rather than one turn of it."""
    return stopper in ("agent", "suspension")


def stopper_of(row: Optional[Mapping[str, Any]]) -> Optional[Stopper]:
    """Who asked for a cancelled run's stop, read off its cancel stamp. None
    for any other ending, and for a cancel no one asked for: shutdown and
    recovery cancel runs too. The finalize sets ``cancelled_by_user`` on
    every stamped stop but a suspension's, whichever worker took it, so
    readers check the stamp and never ``cancel_requested_at``, which a
    system cancel sets as well."""
    if not row or row.get("status") != "cancelled":
        return None
    metadata = row.get("metadata")
    if not isinstance(metadata, dict):
        return None
    for stopper in ("suspension", "member", "agent"):
        if metadata.get(_STOP_MARKS[stopper]):
            return stopper
    return "user" if metadata.get("cancelled_by_user") else None


def asked_for(row: Optional[Mapping[str, Any]]) -> bool:
    """True iff someone asked for the run's stop."""
    return stopper_of(row) is not None


def is_user_stop(row: Optional[Mapping[str, Any]]) -> bool:
    """True iff a person, or an agent for them, stopped the run: any stop
    asked for but the account's suspension's, which is its ending rather
    than anyone's choice."""
    return stopper_of(row) not in (None, "suspension")


def is_suspension_stop(row: Optional[Mapping[str, Any]]) -> bool:
    """True iff a run row was cancelled by its account's suspension."""
    return stopper_of(row) == "suspension"


# Endings whose subagents are collected and billed. A stop someone asked for
# (the user's, an agent's, a suspension's) bills them too, killed, for the
# work they did before it; any other ending (an error, a timeout, a shutdown)
# is not the account's doing, so its subagents are killed unbilled.
COLLECTED_ENDINGS = frozenset({"completed", "interrupted"})


def bills_subagents(row: Optional[dict[str, Any]]) -> bool:
    """True iff a settled run row's ending bills the subagents it spawned."""
    return bool(row) and (row.get("status") in COLLECTED_ENDINGS or asked_for(row))


# The credit gate's two wire spellings, declared rather than derived. Both are
# matched outside Python — the resume query filters on them in SQL and the
# stream reducer compares the error type in TypeScript — so neither may follow
# a class name: a rename would silently stop the resume from finding anything
# and the stopped task from ever reading as stopped. The frontend mirrors
# CREDIT_STOP_ERROR_TYPE by hand in web/src/types/sse.ts, like the status
# families above — change here first, then the mirror.
INTERRUPT_REASON_CREDIT_PAUSE = "credit_pause"
CREDIT_STOP_ERROR_TYPE = "credit_stop"

# A graceful restart stops each running turn and each running subagent run so
# the next process can resume them: the turn's row carries the metadata key,
# the subagent run's failure the error type. The resume reads both from the
# ledger, so neither may follow a name in code.
RESTART_INTERRUPTED_META = "interrupted_by_restart"
RESTART_STOP_ERROR_TYPE = "restart"
# Beside the mark on a turn the system stopped for another reason than a
# restart, to be resumed the same way: ``computer_lost`` for a Claude Code
# turn whose CLI's computer stopped answering, which the resume words.
RESTART_REASON_META = "interrupted_by"
#: The reason, beside the mark, on a Claude Code turn stopped because its
#: computer stopped answering, and the ``stopped_by`` of its ``run_end`` and of
#: its replay's close. The turn's stop is no one's: a user's stop that lands
#: first wins and keeps its own.
COMPUTER_LOST_REASON = "computer_lost"
# How long after a restart its work is still resumed: a resume that late would
# surprise more than help.
RESTART_RESUME_TTL = timedelta(hours=1)


def is_restart_interrupted(row: Optional[Mapping[str, Any]]) -> bool:
    """True iff a run row was cancelled by a graceful restart and is owed a
    resume. A user's stop wins wherever it lands (the executor, or the
    finalize adopting a durable cancel), which drops the stamp."""
    if not row or row.get("status") != "cancelled":
        return False
    metadata = row.get("metadata")
    return isinstance(metadata, dict) and bool(metadata.get(RESTART_INTERRUPTED_META))


def is_computer_lost_stop(row: Optional[Mapping[str, Any]]) -> bool:
    """True iff a run row was stopped because its Claude Code computer
    stopped answering, and by nothing anyone asked for. The reason stays on
    the row once a declined resume's settle takes the restart mark off."""
    metadata = (row or {}).get("metadata")
    return (
        bool(row)
        and row.get("status") == "cancelled"
        and isinstance(metadata, dict)
        and metadata.get(RESTART_REASON_META) == COMPUTER_LOST_REASON
        and stopper_of(row) is None
    )


def run_end_stopper(row: Optional[Mapping[str, Any]]) -> Optional[str]:
    """The ``stopped_by`` a cancelled run's ``run_end``, and its replay's close,
    name, so a reader tells the stop from the user's own: the account's
    suspension, or a lost computer. None for anyone's other stop."""
    if is_suspension_stop(row):
        return SUSPENSION_STOPPER
    return COMPUTER_LOST_REASON if is_computer_lost_stop(row) else None


def restart_resume_expired(stopped_at: datetime) -> bool:
    """Whether work a restart stopped at ``stopped_at`` is past resuming."""
    return datetime.now(timezone.utc) - stopped_at > RESTART_RESUME_TTL


# Every interrupt raised here names itself: the ones this codebase raises
# directly carry ``type``, and the tool-approval middleware carries
# ``name``/``args`` instead. Reading that discriminator IS the classification,
# so the only thing spelled out below is which of the three questions the user
# is being asked. Enumerating actions instead would rot on the next one added.
_QUESTION_TYPE = "ask_user_question"

# The ``interrupt_reason`` vocabulary, most specific first — one tuple serving
# as both the closed value set and the precedence order, so a new reason cannot
# be added without deciding what it outranks. ``credit_pause`` leads because it
# is the one value with behaviour attached (the resume query selects on it), so
# it must not lose to a proposal raised alongside it. ``_classify_one`` must
# only ever return a member; ``.index`` below is what enforces that.
INTERRUPT_REASONS = (
    INTERRUPT_REASON_CREDIT_PAUSE,
    "user_question",
    "approval_required",
)

# The pauses that wait on the person who sent the turn answering something.
INTERRUPT_REASONS_AWAITING_ANSWER = ("user_question", "approval_required")


def _action_requests(interrupts: Any):
    """Every action request across every payload.

    The approval middleware batches a turn's whole approval set into one
    ``interrupt()``, so a payload's requests are siblings, not a leader and a
    tail — reading only the first would let precedence depend on tool-call
    order within a payload while claiming to be order-independent.
    """
    for intr in interrupts:
        value = getattr(intr, "value", None)
        if isinstance(value, dict):
            yield from (
                r for r in value.get("action_requests", ()) if isinstance(r, dict)
            )


def _classify_one(request: dict) -> Optional[str]:
    kind = request.get("type")
    if kind == INTERRUPT_REASON_CREDIT_PAUSE:
        return INTERRUPT_REASON_CREDIT_PAUSE
    if kind == _QUESTION_TYPE:
        return "user_question"
    if kind:
        # A typed interrupt this function has not met. An interrupt always
        # waits on the user, and past a question or a top-up the only shape
        # left is "approve this", so the generalization holds where naming a
        # specific action would be a guess.
        return "approval_required"
    return "approval_required" if request.get("name") else None


def classify_interrupts(interrupts: Any) -> tuple[Optional[str], Optional[str]]:
    """Classify HITL interrupt payloads -> (``interrupt_reason``, the credit
    pause's denial).

    One authority for both: the live producer, the finalize fallback and the
    recovery scanner must never drift apart on the spelling or on which
    words a pause stamps. The reason is ``None`` when no payload carries a
    discriminator, which the column stores as NULL. The denial is the quota
    service's own words, relayed verbatim or not at all; the last pause wins.
    """
    reasons: list[str] = []
    message: Optional[str] = None
    for request in _action_requests(interrupts):
        reason = _classify_one(request)
        if reason:
            reasons.append(reason)
        if reason == INTERRUPT_REASON_CREDIT_PAUSE and isinstance(
            request.get("message"), str
        ):
            message = request["message"]
    return min(reasons, key=INTERRUPT_REASONS.index, default=None), message


#: What a ``computer_unreachable`` error frame says of the machine beside its
#: kind, and its run row keeps for a reload: the connect code, and which
#: computer did not answer.
UNREACHABLE_FRAME_KEYS = ("error_code", "computer_id", "computer_name")

# Stamped on a run that failed because the tunnel gateway did not answer:
# the service's outage, which settling an automation's firing never counts
# against the automation.
GATEWAY_OUTAGE_META = "gateway_outage"


def unreachable_facts(source: Mapping[str, Any]) -> dict[str, Any]:
    """The ``UNREACHABLE_FRAME_KEYS`` *source* has a value for."""
    return {key: source[key] for key in UNREACHABLE_FRAME_KEYS if source.get(key)}
