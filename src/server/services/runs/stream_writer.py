"""Redis stream-write transport for root workflow runs.

Owns the per-run event stream's write side: buffering SSE frames with the
fatal-on-loss contract (I6), the exactly-once ``run_end`` closing frame, and
the cross-worker consumer counter. Process-local run state stays in
``LocalRunExecutor``; this module is pure transport.
"""

import json
import logging
from typing import Any, Dict, Optional

from src.utils.cache.redis_cache import get_cache_client

logger = logging.getLogger(__name__)


def stream_key(thread_id: str, run_id: str) -> str:
    """Per-run workflow event stream."""
    return f"workflow:stream:{thread_id}:{run_id}"


def stream_meta_key(thread_id: str, run_id: str) -> str:
    """Per-run event-buffer metadata (HSET counter)."""
    return f"workflow:events:meta:{thread_id}:{run_id}"


# Visible end-of-run frame (I6): written only after the finalize CAS commits,
# carrying {thread_id, run_id, outcome}. Replaces the pre-finalize
# ``workflow_stream_end`` sentinel, which survives only as a deploy-compat
# swallow in ``stream_from_log``.
RUN_END_EVENT_TYPE = "run_end"


class TransportLostError(RuntimeError):
    """Mid-run event-buffer failure — fatal to the run (I6).

    Raised by the Redis buffering path; the workflow failure handler turns it
    into a ``failed(transport_lost)`` finalize instead of letting the run
    complete with silently missing events.
    """


async def buffer_event(
    thread_id: str, run_id: str, event: str, *, max_stored_messages: int
) -> None:
    """Append a workflow event to the per-run Redis Stream.

    Buffer failure is FATAL to the run (I6): a dropped event means the
    replay archive and any attached consumer silently diverge from what
    the model actually produced, so the run must finalize
    ``failed(transport_lost)`` instead of completing with holes.
    """
    key = (thread_id, run_id)
    try:
        cache = get_cache_client()
    except Exception as e:
        raise TransportLostError(
            f"transport_lost: cache client unavailable ({e})"
        ) from e
    if not cache.enabled:
        raise TransportLostError(
            "transport_lost: Redis event transport is disabled/unreachable"
        )

    event_id = None
    try:
        first_line, _, _ = event.partition("\n")
        event_id = int(first_line.replace("id: ", "").strip())
    except (ValueError, IndexError):
        pass

    if event_id is None:
        raise TransportLostError(
            "transport_lost: unparsable event ID in SSE frame; replay "
            "archive would silently diverge"
        )

    meta_k = stream_meta_key(thread_id, run_id)
    stream_k = stream_key(thread_id, run_id)

    # Retention contract: active streams carry NO TTL (ttl=None) — a
    # quiet-but-alive run must never lose its stream mid-run. The
    # attach-grace TTL is stamped once, at terminal, by
    # ``append_run_end_event``. MAXLEN is a 2x backstop only: the quota
    # check below finalizes the run before FIFO trim could ever touch
    # the head, so a served replay never has a silent hole.
    success, seq = await cache.pipelined_event_buffer(
        meta_key=meta_k,
        event=event,
        max_size=max_stored_messages * 2,
        ttl=None,
        last_event_id=event_id,
        stream_key=stream_k,
    )

    if not success:
        raise TransportLostError(
            f"transport_lost: Redis pipeline write failed for {key}"
        )

    logger.debug(f"[EventBuffer] Buffered event to Redis: {key} (id={event_id}, seq={seq})")

    if seq > max_stored_messages:
        raise TransportLostError(
            f"transport_lost: stream quota exceeded for {key} "
            f"({seq}/{max_stored_messages} events); finalizing "
            "instead of silently trimming the replay head"
        )

    capacity_threshold = int(max_stored_messages * 0.9)
    if seq >= capacity_threshold and (seq - capacity_threshold) % 1000 == 0:
        logger.warning(
            f"[EventBuffer] Buffer near quota for {key}: "
            f"{seq}/{max_stored_messages} events. "
            "At quota the run finalizes error(transport_lost)."
        )


async def append_run_end_event(
    thread_id: str,
    run_id: str,
    outcome: Optional[str],
    *,
    error_frame: Optional[Dict[str, Any]] = None,
    redis_event_ttl: int,
    max_stored_messages: int,
) -> None:
    """Write the closing frames to the per-run Stream (I6), exactly once.

    Written only AFTER the finalize CAS commits, carrying the ADOPTED
    terminal status — a consumer that sees ``run_end`` may trust the
    durable row exists with that outcome. The SETNX gate picks ONE
    emitter among the owner, the recovery scanner, and the dispatch
    reconcile; the winner writes the whole closing story — the caller's
    ``error_frame`` payload (a failure the dead owner never told), then
    ``run_end``. Raw auto-ID XADD, not ``buffer_event``: it has no seq
    slot, and the auto ID (ms timestamp) always sorts after the
    explicit ``seq-0`` IDs real events use. Best-effort: on failure the
    consumer's two-empty-round terminal handshake still closes the
    stream, just slower.

    ``outcome=None`` is the no-durable-truth path (reconcile could not
    establish a terminal): the error frame is appended only while the
    gate is unclaimed, WITHOUT claiming it — no terminal is asserted,
    and a later real finalize still closes the stream.
    """
    try:
        cache = get_cache_client()
        if not cache.enabled or not cache.client:
            return
        stream_k = stream_key(thread_id, run_id)
        meta_k = stream_meta_key(thread_id, run_id)
        gate_key = f"workflow:run_end_gate:{thread_id}:{run_id}"

        if outcome is None:
            if error_frame is None:
                return
            # No terminal to claim, and no retention stamp: the row may
            # still be in_progress, and active streams carry no TTL.
            if await cache.client.exists(gate_key):
                return
            frame = (
                f"event: error\n"
                f"data: {json.dumps(error_frame, ensure_ascii=False)}\n\n"
            )
            await cache.client.xadd(
                stream_k, {b"event": frame.encode("utf-8")}
            )
            return

        # Terminal retention stamp: active streams carry no TTL, so the
        # attach-grace clock starts HERE. Unconditional and idempotent —
        # every emitter stamps it, regardless of who wins the run_end
        # gate below (EXPIRE on a missing key is a no-op).
        try:
            async with cache.client.pipeline(transaction=False) as pipe:
                pipe.expire(stream_k, redis_event_ttl)
                pipe.expire(meta_k, redis_event_ttl)
                await pipe.execute()
        except Exception as exc:
            logger.debug(
                f"[EventBuffer] terminal TTL stamp failed for "
                f"({thread_id}, {run_id}): {exc}"
            )
        # Atomic exactly-once gate: the owner (possibly alive but
        # fence-lost), a recovery scanner, and the dispatch reconcile
        # can all reach this after the same finalize CAS — SETNX picks
        # one emitter, so the stream never carries two run_end frames.
        acquired = await cache.client.set(
            gate_key, "1", nx=True, ex=redis_event_ttl
        )
        if not acquired:
            return
        data = json.dumps(
            {"thread_id": thread_id, "run_id": run_id, "outcome": outcome},
            ensure_ascii=False,
        )
        payload = f"event: {RUN_END_EVENT_TYPE}\ndata: {data}\n\n".encode(
            "utf-8"
        )
        # A pipeline failure here is AMBIGUOUS (the XADD may have landed
        # with its reply lost) and the gate is deliberately NOT released
        # — even a tail recheck can't rule out an in-flight XADD landing
        # after it. At-most-once beats retryability: run_end is
        # best-effort by contract, and the consumer's two-empty-round
        # terminal handshake covers a missing frame.
        async with cache.client.pipeline(transaction=False) as pipe:
            if error_frame is not None:
                err_wire = (
                    f"event: error\n"
                    f"data: {json.dumps(error_frame, ensure_ascii=False)}\n\n"
                )
                pipe.xadd(stream_k, {b"event": err_wire.encode("utf-8")})
            pipe.xadd(
                stream_k,
                {b"event": payload},
                maxlen=max_stored_messages * 2,
                approximate=True,
            )
            # Stamp TTLs so a run_end landing on an expired/cleared
            # key can't recreate it without an expiry — meta_k included,
            # else a swallowed first stamp leaves it permanent.
            pipe.expire(stream_k, redis_event_ttl)
            pipe.expire(meta_k, redis_event_ttl)
            await pipe.execute()
    except Exception as exc:
        logger.debug(
            f"[EventBuffer] run_end append failed for "
            f"({thread_id}, {run_id}): {exc}"
        )


# -- cross-worker consumer signal ---------------------------------------
# SSE watchers attach on ANY worker, so the abandoned heuristic cannot
# trust process-local counters alone: the run's owner would reap a run
# watched entirely through a sibling worker. Every attach/detach bumps a
# Redis counter whose TTL (the abandonment window) is refreshed on each
# bump — "key absent" therefore means no watcher activity for at least
# that window.


def consumers_key(thread_id: str, run_id: str) -> str:
    return f"workflow:consumers:{thread_id}:{run_id}"


async def bump_remote_consumers(
    thread_id: str, run_id: str, delta: int, *, ttl_seconds: float
) -> None:
    """Best-effort: a failed bump only skews a heuristic, never a run."""
    try:
        cache = get_cache_client()
        if (
            cache is None
            or not getattr(cache, "enabled", False)
            or cache.client is None
        ):
            return
        key = consumers_key(thread_id, run_id)
        pipe = cache.client.pipeline(transaction=False)
        pipe.incrby(key, delta)
        pipe.expire(key, int(ttl_seconds))
        await pipe.execute()
    except Exception:
        logger.warning(
            f"[StreamWriter] consumer-counter bump failed for "
            f"thread_id={thread_id} run_id={run_id}",
            exc_info=True,
        )


async def remote_consumer_signal(
    thread_id: str, run_id: str
) -> Optional[int]:
    """Tri-state read: an int is authoritative (absent key = 0 — no
    watcher for a full window); None means Redis is unreachable, and the
    caller must NOT reap (unknown is not abandoned). Cache-disabled
    deployments read 0 — no cross-worker signal can exist there, so the
    local counters are the whole truth."""
    try:
        cache = get_cache_client()
        if (
            cache is None
            or not getattr(cache, "enabled", False)
            or cache.client is None
        ):
            return 0
        raw = await cache.client.get(consumers_key(thread_id, run_id))
        return max(0, int(raw)) if raw is not None else 0
    except Exception:
        return None
