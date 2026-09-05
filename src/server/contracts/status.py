"""Public status vocabulary (v4 1.6) — the one set the API speaks.

Internal stores keep narrower vocabularies: run rows use
``in_progress/completed/interrupted/error/cancelled``, tracker blobs use
``active/completed/interrupted/cancelled/failed/unknown``, and the thread
projection is a legacy free-form string. This module is the boundary — every
status crossing to a client maps through :func:`to_public`, so internal
renames never leak and the frontend switches on exactly one vocabulary.
"""

from __future__ import annotations

from typing import Any, Optional

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


# The credit gate's two wire spellings, declared rather than derived. Both are
# matched outside Python — the resume query filters on them in SQL and the
# stream reducer compares the error type in TypeScript — so neither may follow
# a class name: a rename would silently stop the resume from finding anything
# and the stopped task from ever reading as stopped. The frontend mirrors
# CREDIT_STOP_ERROR_TYPE by hand in web/src/types/sse.ts, like the status
# families above — change here first, then the mirror.
INTERRUPT_REASON_CREDIT_PAUSE = "credit_pause"
CREDIT_STOP_ERROR_TYPE = "credit_stop"

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
    "plan_review_required",
    "approval_required",
)


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
    name = request.get("name")
    if name == "SubmitPlan":
        return "plan_review_required"
    return "approval_required" if name else None


def classify_interrupt_reason(interrupts: Any) -> Optional[str]:
    """Classify HITL interrupt payloads into the ``interrupt_reason`` column.

    One authority for the spelling: the live streaming path and the recovery
    scanner must never drift apart on it. ``None`` when no payload carries a
    discriminator at all, which the column already stores as NULL.
    """
    reasons = [r for r in map(_classify_one, _action_requests(interrupts)) if r]
    return min(reasons, key=INTERRUPT_REASONS.index, default=None)
