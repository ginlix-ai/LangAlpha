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


def classify_interrupt_reason(interrupts: Any) -> str:
    """Classify HITL interrupt payloads: user question vs plan review.

    One authority for the ``interrupt_reason`` column spelling — the live
    streaming path and the recovery scanner must never drift apart on it.
    Accepts Interrupt objects or their dict form.
    """
    for intr in interrupts:
        value = getattr(intr, "value", None)
        if value is None and isinstance(intr, dict):
            value = intr.get("value")
        if isinstance(value, dict):
            requests = value.get("action_requests", [])
            if requests and isinstance(requests[0], dict):
                if requests[0].get("type") == "ask_user_question":
                    return "user_question"
    return "plan_review_required"
