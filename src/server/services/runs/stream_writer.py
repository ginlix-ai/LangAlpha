"""Redis stream-write transport for root workflow runs.

Owns the per-run event stream's write side: buffering SSE frames with the
fatal-on-loss contract (I6), the exactly-once ``run_end`` closing frame, and
the cross-worker consumer counter. Process-local run state stays in
``LocalRunExecutor``; this module is pure transport.
"""

import json
import logging
from typing import Any, Dict, Optional, Sequence

from src.utils.cache.redis_cache import get_cache_client
from src.utils.cache.stream_append import (
    StreamAppendError,
    stream_append_with_retry,
)
from src.utils.cache.stream_quota import StreamQuota

logger = logging.getLogger(__name__)


def stream_key(thread_id: str, run_id: str) -> str:
    """Per-run workflow event stream."""
    return f"workflow:stream:{thread_id}:{run_id}"


def stream_meta_key(thread_id: str, run_id: str) -> str:
    """Per-run event-buffer metadata, no longer written by this release.

    LEGACY_META_COMPAT — kept only so the terminal path can stamp a TTL on
    hashes a previous release's workers wrote during a rolling deploy. Grep
    that tag for every site that goes with it; the removal checklist lives in
    ``stream_retention_sweep``.
    """
    return f"workflow:events:meta:{thread_id}:{run_id}"


# Visible end-of-run frame (I6): written only after the finalize CAS commits,
# carrying {thread_id, run_id, outcome}, plus the thread's token_totals when
# they were read. Replaces the pre-finalize ``workflow_stream_end`` sentinel,
# which survives only as a deploy-compat swallow in ``stream_from_log``.
RUN_END_EVENT_TYPE = "run_end"


class TransportLostError(RuntimeError):
    """Mid-run event-buffer failure — fatal to the run (I6).

    Raised by the Redis buffering path; the workflow failure handler turns it
    into a ``failed(transport_lost)`` finalize instead of letting the run
    complete with silently missing events.
    """


class StreamQuotaExceededError(TransportLostError):
    """The run produced more events, or more bytes, than its replay archive
    may hold.

    A subtype so dashboards can tell a runaway run from a sick Redis: both
    finalize the run, but only one of them is an infrastructure problem.
    """


async def buffer_event(
    thread_id: str,
    run_id: str,
    event: str,
    *,
    quota: StreamQuota,
) -> None:
    """Append a workflow event to the per-run Redis Stream.

    Buffer failure is FATAL to the run (I6): a dropped event means the
    replay archive and any attached consumer silently diverge from what
    the model actually produced, so the run must finalize
    ``failed(transport_lost)`` instead of completing with holes.

    ``quota`` is the run's budget, charged here; the caller keeps it,
    because this module holds no per-run state.
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

    # Charged before the write, so a frame that would break the budget never
    # reaches Redis and the cap bounds what the stream holds.
    breach = quota.charge(event_id, len(event.encode("utf-8")), label=str(key))
    if breach == "events":
        raise StreamQuotaExceededError(
            f"transport_lost: stream quota exceeded for {key} "
            f"({event_id}/{quota.max_events} events); finalizing "
            "instead of silently trimming the replay head"
        )
    if breach == "bytes":
        raise StreamQuotaExceededError(
            f"transport_lost: stream byte quota exceeded for {key} "
            f"({quota.bytes}/{quota.max_bytes} bytes); finalizing "
            "instead of growing a stream nothing can evict"
        )

    # Retention contract: active streams carry NO TTL — a quiet-but-alive run
    # must never lose its stream mid-run. The attach-grace TTL is stamped once,
    # at terminal, by ``append_run_end_event``. MAXLEN is a 2x backstop only:
    # the quota check above finalizes the run before FIFO trim could ever touch
    # the head, so a served replay never has a silent hole.
    try:
        await stream_append_with_retry(
            cache,
            stream_key(thread_id, run_id),
            event_id=event_id,
            max_size=quota.maxlen,
            stream_event=event,
            label=str(key),
        )
    except StreamAppendError as exc:
        # The append policy is layer-neutral; the fatal-to-the-run verdict is
        # this layer's vocabulary.
        raise TransportLostError(str(exc)) from exc

    logger.debug(f"[EventBuffer] Buffered event to Redis: {key} (id={event_id})")


async def steering_returned_frame(thread_id: str, run_id: str) -> str | None:
    """The ``steering_returned`` SSE event for the chat-app steers ``run_id``
    returned, or None. Read once its finalize has committed, which settled
    them.

    Only chat-app steers come back this way: the gateway resubmits them as
    the next turn and relays the reply, which it does not do for a turn the
    server starts. Every other steer a run missed stays on the thread for
    the next one.
    """
    from src.server.database.runs import outbox as outbox_db

    try:
        returned = await outbox_db.list_returned_platform_steers(run_id)
    except Exception:
        logger.exception(f"Failed to read returned steers for run {run_id}")
        return None
    if not returned:
        return None
    event_data = json.dumps({
        "thread_id": thread_id,
        "messages": [
            {"content": m["content"], "user_id": m.get("user_id")}
            for m in returned
        ],
    })
    return f"event: steering_returned\ndata: {event_data}\n\n"


async def append_run_end_event(
    thread_id: str,
    run_id: str,
    outcome: Optional[str],
    *,
    error_frame: Optional[Dict[str, Any]] = None,
    token_totals: Optional[Dict[str, int]] = None,
    closing_frames: Sequence[str] = (),
    redis_event_ttl: int,
    max_stored_messages: int,
) -> None:
    """Write the closing frames to the per-run Stream (I6), exactly once.

    Written only AFTER the finalize CAS commits, carrying the ADOPTED
    terminal status — a consumer that sees ``run_end`` may trust the
    durable row exists with that outcome. The SETNX gate picks ONE
    emitter among the owner, the recovery scanner, and the dispatch
    reconcile; the winner writes the whole closing story — the caller's
    ``closing_frames`` (SSE text the settled run owes its readers), the
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
        # gate below (EXPIRE on a missing key is a no-op). ``meta_k`` is
        # LEGACY_META_COMPAT: this release stopped writing that hash, but a
        # previous-release worker in the same rotation may have created it,
        # and an unstamped hash would live forever.
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
        frame_data: Dict[str, Any] = {
            "thread_id": thread_id,
            "run_id": run_id,
            "outcome": outcome,
        }
        if token_totals is not None:
            frame_data["token_totals"] = token_totals
        data = json.dumps(frame_data, ensure_ascii=False)
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
            for frame in closing_frames:
                pipe.xadd(stream_k, {b"event": frame.encode("utf-8")})
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
