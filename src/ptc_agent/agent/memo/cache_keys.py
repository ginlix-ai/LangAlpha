"""Redis key builders for memo metadata coordination.

Lives in the agent layer so ``metadata.py`` (agent) and ``memo.py`` (server)
can both import without crossing the agent → server boundary.
"""

from __future__ import annotations


def memo_metadata_inflight_key(user_id: str, key: str) -> str:
    """Redis key advertising that some worker is generating metadata for this memo."""
    return f"memo:metadata:inflight:{user_id}:{key}"


def memo_metadata_cancel_key(user_id: str, key: str) -> str:
    """Redis key carrying a cooperative cross-worker cancel signal for this memo."""
    return f"memo:metadata:cancel:{user_id}:{key}"
