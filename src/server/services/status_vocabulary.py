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
