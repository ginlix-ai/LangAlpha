"""What this package keeps in agent state, and the one way to read it.

State arrives as a plain dict on some paths and as a state object on others, so
without this every reader grows its own accessor and they drift. Decoding the
epoch itself happens once, at the state boundary in ``epoch.py``, which is also
why the checkpoint readers there share one coercion rather than each guarding
the shape of a key an older build may never have written.
"""

from __future__ import annotations

from typing import Any

#: The per-thread epoch, written by the baseline middleware at the turn boundary.
STATE_BASELINE = "runtime_baseline"


def state_get(state: Any, key: str) -> Any:
    """One value out of agent state, whether it is a dict or an object."""
    if state is None:
        return None
    if isinstance(state, dict):
        return state.get(key)
    getter = getattr(state, "get", None)
    if callable(getter):
        try:
            return getter(key)
        except Exception:  # noqa: BLE001 - a missing key is never a turn failure
            return None
    return getattr(state, key, None)


def as_dict(value: Any) -> dict[str, Any]:
    """A checkpointed value as a dict, or empty when it is anything else."""
    return dict(value) if isinstance(value, dict) else {}
