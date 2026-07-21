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

PUBLIC_STATUSES = frozenset(
    {
        "idle",
        "queued",
        "running",
        "stopping",
        "recovering",
        "completed",
        "interrupted",
        "failed",
        "cancelled",
    }
)

# The one internal terminal set — both run ledgers (turn_lifecycle,
# subagent_runs) import it, and the migration CHECK constraints are
# test-bound to it, so a new outcome cannot land in one store only.
TERMINAL_STATUSES = ("completed", "interrupted", "error", "cancelled")

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
