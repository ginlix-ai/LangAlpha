"""The ``credit_usage`` wire payload, shared by the live stream and replay.

Its own module because replay stores what it builds: the replay projection
digests this file, so a change to the payload's shape re-projects every
stored turn instead of serving the old shape from storage.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, Optional


def build_credit_usage_data(
    thread_id: str,
    token_usage: dict,
    total_credits: float,
    timestamp: Optional[str] = None,
) -> Dict[str, Any]:
    """Aggregate per-model usage into the ``credit_usage`` wire payload.

    Intentionally omits USD costs and model names (hidden from the client).
    Shared by the live handler and table-sourced replay so both wires carry
    the same shape.
    """
    total_input_tokens = 0
    total_output_tokens = 0
    total_tokens = 0
    for usage in (token_usage or {}).get("by_model", {}).values():
        total_input_tokens += usage.get("input_tokens", 0)
        total_output_tokens += usage.get("output_tokens", 0)
        total_tokens += usage.get("total_tokens", 0)

    return {
        "thread_id": thread_id,
        "tokens": {
            "input_tokens": total_input_tokens,
            "output_tokens": total_output_tokens,
            "total_tokens": total_tokens,
        },
        "total_credits": round(total_credits, 2),
        "timestamp": timestamp or datetime.now().isoformat(),
    }
