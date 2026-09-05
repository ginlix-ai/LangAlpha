"""The one thread-lifecycle projection: latest-attempt row → public fields.

Three surfaces answer "what is this thread doing, and has the user seen it" —
the thread list, the feed snapshot, and the feed's event builder. They used to
each re-derive `to_public` + interrupt-reason nulling + the unseen comparison,
which is how the classification drifted apart. This module owns that derivation
once; callers only re-key it into their own wire shape.
"""

from __future__ import annotations

from typing import Any, Optional, TypedDict

from src.server.contracts.status import TERMINAL_PUBLIC_STATUSES, to_public


class LifecycleFields(TypedDict):
    """Canonical projection. ``run_status`` is PUBLIC vocabulary."""

    run_status: str
    interrupt_reason: Optional[str]
    latest_run_id: Optional[str]
    latest_run_seq: int
    last_seen_run_seq: int
    unseen: bool
    run_started_at: Any


def interrupt_reason_for(
    public_status: str, reason: Optional[str]
) -> Optional[str]:
    """``interrupt_reason`` is meaningful only while the run is interrupted.

    A settled run keeps the column populated from the attempt that awaited
    input, so every consumer must null it or the client renders a stale
    "needs your answer" affordance on a completed turn.
    """
    return reason if public_status == "interrupted" else None


def project_lifecycle(row: dict) -> LifecycleFields:
    """Project a row carrying the ``latest_*`` attempt columns."""
    run_status = to_public(
        row.get("latest_run_status"),
        cancel_requested_at=row.get("latest_cancel_requested_at"),
    )
    latest_seq = int(row.get("latest_run_seq") or 0)
    last_seen = int(row.get("last_seen_run_seq") or 0)
    return LifecycleFields(
        run_status=run_status,
        interrupt_reason=interrupt_reason_for(
            run_status, row.get("latest_interrupt_reason")
        ),
        latest_run_id=(
            str(row["latest_run_id"]) if row.get("latest_run_id") else None
        ),
        latest_run_seq=latest_seq,
        last_seen_run_seq=last_seen,
        unseen=(
            run_status in TERMINAL_PUBLIC_STATUSES and latest_seq > last_seen
        ),
        run_started_at=row.get("latest_run_started_at"),
    )
