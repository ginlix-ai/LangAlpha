"""Per-thread control lane (STREAM_CONTRACT_V2.md discovery).

Durable, replayable announcements of new runs — root turns and subagent
task runs — so an attached mux admits channels push-style with no
subscribe-after-snapshot race: the stream IS the backlog. Bounded and
best-effort by design; periodic ledger reconciliation backstops anything
a trim or failed append drops.
"""

import logging

logger = logging.getLogger(__name__)

# Nudge surface, not an archive: old entries only matter to consumers that
# were attached (or attach within the reconciliation interval), so a bounded
# trim never loses anything the backstop can't recover.
_CONTROL_MAXLEN = 512


def control_stream_key(thread_id: str) -> str:
    return f"subagent:control:{thread_id}"


async def announce_run_started(thread_id: str, run_id: str) -> None:
    """A root turn's main-lane stream exists (`workflow:stream:{t}:{run}`)."""
    await _append(
        thread_id, {b"type": b"run_started", b"run_id": run_id.encode()}
    )


async def announce_task_run_started(
    thread_id: str,
    *,
    task_run_id: str,
    task_id: str,
    cause: str,
    parent_run_id: str | None = None,
) -> None:
    """An admitted task run's stream exists (`subagent:stream:{t}:{run}`)."""
    await _append(
        thread_id,
        {
            b"type": b"task_run_started",
            b"run_id": task_run_id.encode(),
            b"task_id": task_id.encode(),
            b"cause": cause.encode(),
            b"parent_run_id": (parent_run_id or "").encode(),
        },
    )


async def _append(thread_id: str, fields: dict) -> None:
    try:
        from src.config.settings import get_redis_ttl_workflow_events
        from src.utils.cache.redis_cache import get_cache_client

        cache = get_cache_client()
        if not (getattr(cache, "enabled", False) and cache.client):
            return
        key = control_stream_key(thread_id)
        await cache.client.xadd(key, fields, maxlen=_CONTROL_MAXLEN)
        await cache.client.expire(key, get_redis_ttl_workflow_events())
    except Exception:
        logger.warning(
            f"[control_stream] announce failed for thread={thread_id}",
            exc_info=True,
        )
