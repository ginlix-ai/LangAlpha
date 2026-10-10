"""Probe the per-task Redis streams for whether a subagent run sealed."""

import asyncio
import json
import logging
from typing import Any

from ptc_agent.agent.middleware.background_subagent.redis_stream import (
    SUBAGENT_STREAM_END_EVENT,
    task_stream_key,
)
from src.utils.cache.redis_cache import get_cache_client

logger = logging.getLogger(__name__)


STREAM_SEALED = "sealed"
STREAM_OPEN = "open"
STREAM_MISSING = "missing"
STREAM_UNKNOWN = "unknown"


async def task_stream_states(thread_id: str, task_ids: set[str]) -> dict[str, str]:
    """Each task's per-task Redis stream: sealed, open, missing or unknown.

    Only an end sentinel proves a stream sealed. A missing stream is either
    one that expired after sealing or one that was never written, which this
    probe cannot tell apart; the caller decides with the launching turn's
    age (``RunLane``). A failed probe is unknown.

    Probe BEFORE reading task-namespace state, and gate caching on this
    pre-read verdict: sealing is monotonic, so "sealed before the read"
    proves the read saw final state, while a post-read probe can pass for a
    stream that sealed after the read returned pre-terminal data, freezing
    it into stored lines.
    """
    if not task_ids:
        return {}
    client = get_cache_client().client
    if client is None:
        return dict.fromkeys(task_ids, STREAM_UNKNOWN)

    async def _state(task_id: str) -> str:
        try:
            entries = await client.xrevrange(
                task_stream_key(thread_id, task_id), count=1
            )
        except Exception as e:
            logger.warning(
                f"[TaskStreams] task stream check failed for "
                f"{thread_id}/{task_id}: {e}"
            )
            return STREAM_UNKNOWN
        if not entries:
            return STREAM_MISSING
        raw = entries[0][1].get(b"event") or entries[0][1].get("event")
        if isinstance(raw, bytes):
            raw = raw.decode("utf-8", errors="replace")
        return STREAM_SEALED if _is_stream_end_sentinel(raw) else STREAM_OPEN

    ordered = list(task_ids)
    states = await asyncio.gather(*(_state(task_id) for task_id in ordered))
    return dict(zip(ordered, states))


def _is_stream_end_sentinel(raw: Any) -> bool:
    """Match forwarder.finalize()'s ``{"event": "subagent_stream_end"}``
    sentinel (a JSON dict without ``seq`` — same test as the reconnect
    consumer's payload classifier)."""
    if not isinstance(raw, str) or not raw.startswith("{"):
        return False
    try:
        record = json.loads(raw)
    except json.JSONDecodeError:
        return False
    return (
        isinstance(record, dict)
        and record.get("event") == SUBAGENT_STREAM_END_EVENT
        and "seq" not in record
    )
