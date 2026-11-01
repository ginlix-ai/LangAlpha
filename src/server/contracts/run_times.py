"""How the API times a run: epoch ms on the wire.

One spelling for every surface that sends a run's times (the stream's
frames, replay's stamps, the status routes), so a run reads the same length
live and after a reload.
"""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any, Optional

from src.server.contracts.status import TERMINAL_STATUSES


def epoch_ms(ts: Optional[datetime]) -> Optional[float]:
    return ts.timestamp() * 1000 if ts is not None else None


def run_settled_at(response: Optional[dict[str, Any]]) -> Optional[datetime]:
    """When a ``conversation_responses`` run settled, or None while it runs.

    ``usage_settled_at`` is the terminal CAS's own ``NOW()``, the settle
    instant itself. Rows written before that column existed fall back to the
    run's start plus its measured duration, and a row that knows neither
    returns None. Replay, the liveness route and the next turn's cache clock
    all time a run by it, so one copy of the fallback chain keeps them
    agreeing.
    """
    if response is None or response.get("status") not in TERMINAL_STATUSES:
        return None
    settled = response.get("usage_settled_at")
    if settled is not None:
        return settled
    created_at = response.get("created_at")
    execution_time = response.get("execution_time")
    if not isinstance(created_at, datetime) or not isinstance(execution_time, (int, float)):
        return None
    return created_at + timedelta(seconds=float(execution_time))


def run_completed_at(response: Optional[dict[str, Any]]) -> Optional[str]:
    """``run_settled_at`` in ISO 8601, so the client shows no duration
    rather than a wrong one when it is None."""
    at = run_settled_at(response)
    return at.isoformat() if at is not None else None
