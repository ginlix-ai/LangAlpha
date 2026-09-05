"""Writer-side Redis transport ownership for background-subagent streams.

Single owner of the subagent stream/meta key shapes and the terminal
retention stamp. Key identity is load-bearing: the v1 pair is keyed by
*task_id* (mutable across resumes — resume hard-deletes it as the epoch
bump), the v2 stream by *task_run_id* (immutable per run, the ledger's
identity). Server-side readers keep their own key builders; every writer
builds keys here.
"""

from __future__ import annotations

import asyncio
import json
import time
from typing import TYPE_CHECKING, Any, Literal

import redis.exceptions as redis_exceptions
import structlog

if TYPE_CHECKING:
    from .registry import BackgroundTask

logger = structlog.get_logger(__name__)


def task_stream_key(thread_id: str, task_id: str) -> str:
    return f"subagent:stream:{thread_id}:{task_id}"


def task_meta_key(thread_id: str, task_id: str) -> str:
    """Write-only counter hash, no longer written by this release.

    LEGACY_META_COMPAT — retained so the terminal stamp and the resume cleanup
    still reach hashes a previous-release worker created during a rolling
    deploy. Grep that tag for every site that goes with it; the removal
    checklist lives in ``stream_retention_sweep``.
    """
    return f"subagent:events:meta:{thread_id}:{task_id}"


def run_stream_key(thread_id: str, task_run_id: str) -> str:
    return f"subagent:stream:{thread_id}:{task_run_id}"


def legacy_task_events_key(thread_id: str, task_id: str) -> str:
    return f"subagent:events:{thread_id}:{task_id}"


async def stamp_task_retention(
    thread_id: str,
    task_id: str,
    task_run_id: str | None = None,
    *,
    timeout: float | None = None,
) -> None:
    """Start the attach-grace expiry clock on a task's event keys at terminal.

    Active streams carry no TTL, so this is the only place their expiry
    clock starts. ``nx=True`` makes each stamp set-if-absent: a late
    attach-grace stamp must never resurrect the shorter post-collection
    retention window the collector may already have applied to the run
    stream. Raises on transport failure — callers own their retry policy.
    """
    from src.config.settings import get_redis_ttl_workflow_events
    from src.utils.cache.redis_cache import get_cache_client

    cache = get_cache_client()
    if not getattr(cache, "enabled", False) or not cache.client:
        return
    ttl = get_redis_ttl_workflow_events()
    async with cache.client.pipeline(transaction=False) as pipe:
        pipe.expire(task_stream_key(thread_id, task_id), ttl, nx=True)
        pipe.expire(task_meta_key(thread_id, task_id), ttl, nx=True)
        if task_run_id:
            pipe.expire(run_stream_key(thread_id, task_run_id), ttl, nx=True)
        if timeout is not None:
            await asyncio.wait_for(pipe.execute(), timeout=timeout)
        else:
            await pipe.execute()


# Per-call cap for the un-fenced writes on the subagent hot path (the v2
# auto-id append, the tail probes, the retention stamp). A healthy pipeline
# acks in <10ms; this cap bounds the worst case so a degraded Redis can't pace
# subagent execution. ``asyncio.wait_for`` measures wall-clock INCLUDING
# event-loop scheduling delay, so the cap must absorb loop stalls (checkpoint
# serialization, large json.dumps) — 0.5s fired on a healthy Redis and killed a
# run whose write had actually landed. It must also clear the cache pool's
# acquire timeout (2s) plus a real command, or contention alone cancels a
# queued write.
_SPILL_TIMEOUT_SECONDS = 5.0

# The fenced v1 append gets a budget rather than one cap — the id fence makes
# it retryable, unlike everything above, which stays single-shot because an
# auto-id write is not replayable. That budget is NOT set here: it is derived
# from the pool's acquire timeout by ``stream_append``, which is the only layer
# that knows how much of an attempt the queue can eat. A hand-picked number
# here silently made the last attempt too short to be classified honestly.

# Event-type marker for the per-task stream-end sentinel. The producer writes
# one of these via ``append_sentinel_to_stream`` when the subagent finishes
# streaming; the per-task SSE consumer treats it as "drain complete" and exits.
# Shared between producer (registry) and consumer (stream_from_log) so the
# string lives in exactly one place.
SUBAGENT_STREAM_END_EVENT = "subagent_stream_end"


# How many tail entries the probe walks back before giving up. The run stream
# is NOT exclusively ours: ``SubagentRunCoordinator._append_v2_frame`` writes
# control frames (``lane_open``, ``run_end``) to a byte-identical key without
# holding ``redis_spill_lock``, and those payloads carry no ``seq``. Reading
# only the newest entry therefore reports "not landed" whenever a control frame
# raced in behind our write — and "not landed" means replay, which on an
# auto-id stream renders a duplicate token to the user. Two control frame types
# exist, so a handful of slots is generous; the cap keeps a degraded probe from
# dragging a whole stream back.
_TAIL_PROBE_SCAN = 8


async def _stream_tail_seq(cache: Any, stream_key: str, field: bytes) -> int | None:
    """Seq of the newest *seq-bearing* entry in a spill stream, or None.

    ``wait_for`` cancels the awaiting coroutine on timeout, but the
    command may already be on the wire — the tail decides landed
    (continue) vs lost (retry, then circuit). Reads the seq out of the
    payload because the v2 stream uses Redis auto-ids, which carry no
    relation to the record's sequence number, and skips past seq-less
    control frames rather than reading one as "our write is missing".
    """
    try:
        entries = await asyncio.wait_for(
            cache.client.xrevrange(stream_key, count=_TAIL_PROBE_SCAN),
            timeout=_SPILL_TIMEOUT_SECONDS,
        )
    except Exception:
        return None
    for entry in entries or ():
        try:
            raw = (entry[1] or {}).get(field)
            if raw is None:
                continue
            if isinstance(raw, (bytes, bytearray)):
                raw = raw.decode("utf-8")
            payload = json.loads(raw)
            if not isinstance(payload, dict) or "seq" not in payload:
                continue
            return int(payload["seq"])
        except Exception:
            continue
    return None


# redis-py spells each stage of a command's journey with a distinct message, so
# how far a failed write got is recoverable from the error text. Line refs are
# redis-py 7.4.0 ``redis/asyncio/connection.py``.
_V2_PRE_SEND_MARKERS = (
    # :629 ``_send_ping`` — ``check_health()`` runs at :657-658, BEFORE the
    # command is written. With ``health_check_interval=30`` any pooled
    # connection idle for half a minute is PINGed first, so a Redis restart,
    # failover or LB idle-reap fails here with nothing sent. This is the
    # dominant real-world trigger.
    "Bad response from PING health check",
    # :387 -> ``redis/utils.py`` :277-286 ``format_error_message`` — the socket
    # was never established.
    " connecting to ",
    # :1496 / :1501 ``ensure_connection`` — the pool refused to hand out the
    # connection; the command was not even packed.
    "Connection has data",
    "Connection not ready",
)

_V2_AMBIGUOUS_MARKERS = (
    # :705 ``can_read_destructive`` / :748 ``read_response`` — the command is on
    # the wire and only the reply was lost.
    "Error while reading from ",
    # :681 ``send_packed_command``. Textually this is the write side, but it is
    # NOT provably pre-send: ``StreamWriter.drain()`` also raises when the peer
    # resets a connection whose bytes it had already flushed AND executed, so a
    # fully-written XADD can surface here. Ambiguous is the conservative read —
    # a probe costs one XREVRANGE, a wrong replay costs a duplicate token.
    " while writing to socket",
)


def _classify_v2_write_failure(
    exc: BaseException,
) -> Literal["pre_send", "ambiguous", "fatal"]:
    """How far the auto-id v2 XADD got — the only question worth asking here.

    The v2 leg carries no id fence and nothing downstream dedupes by payload
    seq (the mux stamps the Redis entry id), so replaying a write that actually
    landed renders a duplicate token to the user. Only ``pre_send`` may be
    replayed blind; ``ambiguous`` is replayed only when the tail does not
    confirm it landed, which is weaker than proving it absent — a probe that
    itself fails counts as not-landed, so that one replay is bounded rather
    than safe. The root lane gets the proof for free: its ``{id}-0`` fence
    makes Redis adjudicate, and the duplicate-id rejection *is* the
    confirmation. This lane cannot have that while the contract makes the
    server-assigned entry id the reader's cursor (``STREAM_CONTRACT_V2.md``);
    fencing it would delete the probe and this caveat with it. Anything
    unrecognized stays fatal.
    """
    from src.utils.cache.redis_cache import is_pool_exhaustion

    if is_pool_exhaustion(exc):
        # The acquire never produced a connection.
        return "pre_send"
    if isinstance(exc, redis_exceptions.TimeoutError):
        # Not ``builtins.TimeoutError`` — a socket timeout can fire either side
        # of the server seeing the command, same as our own ``wait_for`` cap.
        return "ambiguous"
    if isinstance(exc, redis_exceptions.ConnectionError):
        text = str(exc)
        if any(marker in text for marker in _V2_PRE_SEND_MARKERS):
            return "pre_send"
        if any(marker in text for marker in _V2_AMBIGUOUS_MARKERS):
            return "ambiguous"
    return "fatal"


async def spill_task_record(
    thread_id: str, task: "BackgroundTask", record: dict[str, Any]
) -> None:
    """Best-effort spill of one captured record to the per-task Stream.

    Writes a single XADD entry with two fields: ``b"event"`` (pre-rendered SSE
    wire string, consumed live by SSE clients) and ``b"record"`` (JSON record,
    consumed post-turn by ``iter_subagent_events_full`` via XRANGE). The v1
    append runs the shared fenced-retry policy; only an exhausted budget flips
    ``task.redis_write_failed`` (sticky circuit-break), silently logged — never
    raised. Returns silently when the circuit-break is set, no thread_id was
    configured (test fixtures), the spill flag is off, or the cache client is
    unavailable.
    """
    if task.redis_write_failed:
        return

    if not thread_id:
        return

    # Lazy import to avoid circular imports during test collection.
    try:
        from src.config.settings import (
            get_max_stored_messages_per_agent,
            is_subagent_event_redis_spill_enabled,
        )
    except Exception:
        return

    try:
        if not is_subagent_event_redis_spill_enabled():
            return
    except Exception:
        return

    try:
        from src.utils.cache.redis_cache import get_cache_client
        from src.utils.cache.stream_append import (
            StreamAppendError,
            stream_append_with_retry,
        )

        cache = get_cache_client()
    except Exception as exc:
        task.redis_write_failed = True
        logger.warning(
            "subagent_event_spill_failed",
            phase="cache_init",
            tool_call_id=task.tool_call_id,
            task_id=task.task_id,
            error=str(exc),
        )
        return

    if not getattr(cache, "enabled", False):
        return

    # Records are JSON-serialized ``{"seq", "event", "data", "agent_id", "ts"}`` dicts.
    stream_key = task_stream_key(thread_id, task.task_id)

    try:
        payload = json.dumps(record, ensure_ascii=False, default=str)
    except Exception as exc:
        task.redis_write_failed = True
        logger.warning(
            "subagent_event_spill_failed",
            phase="serialize",
            tool_call_id=task.tool_call_id,
            task_id=task.task_id,
            seq=record.get("seq"),
            error=str(exc),
        )
        return

    # Pre-render the SSE wire format for the Stream so the live consumer
    # can yield bytes verbatim — no JSON-decode + re-render branch in the
    # read path. The post-turn collector (``iter_subagent_events_full``)
    # reads the parallel ``b"record"`` field via XRANGE.
    try:
        seq = int(record.get("seq") or 0)
        data = {
            **(record.get("data") or {}),
            "thread_id": thread_id,
            "agent": f"task:{task.task_id}",
        }
        stream_payload = (
            f"id: {seq}\n"
            f"event: {record.get('event') or 'message_chunk'}\n"
            f"data: {json.dumps(data, ensure_ascii=False, default=str)}\n\n"
        )
    except Exception as exc:
        task.redis_write_failed = True
        logger.warning(
            "subagent_event_spill_failed",
            phase="render_sse",
            tool_call_id=task.tool_call_id,
            task_id=task.task_id,
            seq=record.get("seq"),
            error=str(exc),
        )
        return

    # Serialize spills per task. The registry-wide lock is released
    # before this call so multiple tasks can spill in parallel; the
    # per-task lock guarantees that for any two appends to the SAME
    # task, the second's pipeline cannot start until the first's
    # pipeline has acked at Redis. Without this, two appends that
    # acquired distinct seq numbers can race to the server via
    # different pool connections and land out of order.
    try:
        quota = get_max_stored_messages_per_agent()
        async with task.redis_spill_lock:
            # XADD carries both the pre-rendered SSE wire string
            # (``b"event"``, consumed live by ``stream_subagent_from_log``)
            # and the JSON record (``b"record"``, consumed post-turn by
            # ``iter_subagent_events_full`` via XRANGE). Active streams
            # carry no TTL (retention contract — the attach-grace TTL is
            # stamped at terminal by ``stamp_terminal_retention``), and
            # MAXLEN is a 2x backstop: the quota check below opens the
            # circuit before FIFO trim could touch the head.
            # Same fenced-append policy as the root lane, including the
            # retry on pool exhaustion this path used to lack: an exhausted
            # pool tore the subagent run outright, which is the failure this
            # whole subsystem exists to survive.
            success = False
            append_error: str | None = None
            try:
                await stream_append_with_retry(
                    cache,
                    stream_key,
                    event_id=seq,
                    max_size=quota * 2,
                    stream_event=stream_payload,
                    stream_record=payload,
                    label=f"task:{task.task_id}",
                )
                success = True
            except StreamAppendError as exc:
                append_error = str(exc)
            # v2 dual-write (STREAM_CONTRACT_V2.md): the immutable
            # per-run stream, keyed by ledger identity. Same lock hold so
            # per-run frame order matches append order; seq is the XADD
            # id (Redis-side). Contract-grade: a hole in the canonical
            # per-run stream opens the circuit like a v1 failure — the
            # run tears as error(transport_lost) rather than a reader
            # ever being served a stream with a silent gap.
            if success and task.task_run_id:
                v2_key = run_stream_key(thread_id, task.task_run_id)
                # No per-write TTL: the immutable per-run stream is
                # active until terminal, when stamp_terminal_retention
                # applies the attach-grace TTL.
                v2_fields = {
                    b"run_id": task.task_run_id.encode(),
                    b"lane": f"task:{task.task_id}".encode(),
                    b"type": (record.get("event") or "message_chunk").encode(),
                    b"payload": payload.encode("utf-8"),
                }
                for attempt in (1, 2):
                    try:
                        await asyncio.wait_for(
                            # Same 2x MAXLEN backstop as the v1 leg: the quota
                            # circuit below tears the run before FIFO trim
                            # could touch the head (STREAM_CONTRACT_V2.md).
                            cache.client.xadd(
                                v2_key,
                                v2_fields,
                                maxlen=quota * 2,
                                approximate=True,
                            ),
                            timeout=_SPILL_TIMEOUT_SECONDS,
                        )
                        break
                    except asyncio.TimeoutError:
                        if (
                            await _stream_tail_seq(cache, v2_key, b"payload")
                            == seq
                        ):
                            logger.info(
                                "subagent_event_spill_recovered",
                                phase="v2_timeout_landed",
                                tool_call_id=task.tool_call_id,
                                task_id=task.task_id,
                                task_run_id=task.task_run_id,
                                seq=seq,
                            )
                            break
                        if attempt == 1:
                            logger.warning(
                                "subagent_event_spill_retry",
                                phase="v2_timeout",
                                tool_call_id=task.tool_call_id,
                                task_id=task.task_id,
                                task_run_id=task.task_run_id,
                                seq=seq,
                                timeout_seconds=_SPILL_TIMEOUT_SECONDS,
                            )
                            continue
                        task.redis_write_failed = True
                        logger.warning(
                            "subagent_event_spill_failed",
                            phase="v2_pipeline",
                            tool_call_id=task.tool_call_id,
                            task_id=task.task_id,
                            task_run_id=task.task_run_id,
                            seq=record.get("seq"),
                        )
                        break
                    except Exception as exc:
                        # An auto-id write cannot be REPLAYED blind, but that
                        # is no reason not to CLASSIFY it: most failures
                        # reachable here are pre-send and unambiguous, and
                        # tearing the run on one of those throws away a
                        # subagent's finished work. Same policy as the timeout
                        # branch above, split by what the server can have seen.
                        verdict = _classify_v2_write_failure(exc)
                        if verdict != "fatal":
                            if verdict == "ambiguous" and (
                                await _stream_tail_seq(cache, v2_key, b"payload")
                                == seq
                            ):
                                logger.info(
                                    "subagent_event_spill_recovered",
                                    phase="v2_error_landed",
                                    tool_call_id=task.tool_call_id,
                                    task_id=task.task_id,
                                    task_run_id=task.task_run_id,
                                    seq=seq,
                                    error=str(exc),
                                )
                                break
                            if attempt == 1:
                                logger.warning(
                                    "subagent_event_spill_retry",
                                    phase=f"v2_{verdict}",
                                    tool_call_id=task.tool_call_id,
                                    task_id=task.task_id,
                                    task_run_id=task.task_run_id,
                                    seq=seq,
                                    error=str(exc),
                                )
                                continue
                        task.redis_write_failed = True
                        logger.warning(
                            "subagent_event_spill_failed",
                            phase="v2_pipeline",
                            tool_call_id=task.tool_call_id,
                            task_id=task.task_id,
                            task_run_id=task.task_run_id,
                            seq=record.get("seq"),
                            error=str(exc),
                        )
                        break
        if not success:
            task.redis_write_failed = True
            logger.warning(
                "subagent_event_spill_failed",
                phase="pipeline",
                tool_call_id=task.tool_call_id,
                task_id=task.task_id,
                seq=record.get("seq"),
                error=append_error,
            )
        elif seq > quota:
            # Quota breach tears the transport by contract: opening the
            # circuit here (instead of trimming FIFO) makes the abort
            # loop + terminal escalation finalize error(transport_lost),
            # so a replay with a silent hole is never served.
            task.redis_write_failed = True
            logger.warning(
                "subagent_event_spill_failed",
                phase="quota",
                tool_call_id=task.tool_call_id,
                task_id=task.task_id,
                seq=record.get("seq"),
                quota=quota,
            )
    except asyncio.TimeoutError:
        task.redis_write_failed = True
        logger.warning(
            "subagent_event_spill_failed",
            phase="timeout",
            tool_call_id=task.tool_call_id,
            task_id=task.task_id,
            seq=record.get("seq"),
            timeout_seconds=_SPILL_TIMEOUT_SECONDS,
        )
    except Exception as exc:
        task.redis_write_failed = True
        logger.warning(
            "subagent_event_spill_failed",
            phase="exception",
            tool_call_id=task.tool_call_id,
            task_id=task.task_id,
            seq=record.get("seq"),
            error=str(exc),
        )


async def write_task_meta(thread_id: str, task: "BackgroundTask", status: str) -> None:
    """Best-effort mirror of the task's routing identity + writer liveness
    to Redis (``subagent:meta:{thread}:{task}``) so OTHER workers can
    resolve steer/update targets and gate resumes.

    ``status`` tracks the WRITER ("running" while an asyncio writer owns
    the namespace, "completed"/"cancelled"/"error" once it settled), not
    result availability. Advisory only — the N(thread, task:id) advisory
    lock, not this hash, is the write fence.
    """
    if not thread_id:
        return
    try:
        from src.config.settings import get_redis_ttl_workflow_events
        from src.utils.cache.redis_cache import get_cache_client

        cache = get_cache_client()
        if not getattr(cache, "enabled", False) or not cache.client:
            return
        key = f"subagent:meta:{thread_id}:{task.task_id}"
        pipe = cache.client.pipeline()
        pipe.hset(
            key,
            mapping={
                "tool_call_id": task.tool_call_id,
                "status": status,
                "subagent_type": task.subagent_type,
                "description": (task.description or "")[:200],
                "spawned_run_id": task.spawned_run_id or "",
                # Execution-scoped stream epoch: spawned_run_id is
                # parent-turn-scoped and does NOT change on a same-turn
                # resume, so epoch consumers prefer this field.
                "task_run_id": task.task_run_id or "",
                "updated_at": str(time.time()),
            },
        )
        pipe.expire(key, get_redis_ttl_workflow_events())
        await asyncio.wait_for(pipe.execute(), timeout=_SPILL_TIMEOUT_SECONDS)
    except Exception as exc:
        logger.warning(
            "task meta write failed",
            task_id=task.task_id,
            status=status,
            error=str(exc),
        )


async def append_task_sentinel(thread_id: str, task: "BackgroundTask") -> None:
    """Write the stream-end sentinel record to the per-task Stream.

    Transport-level signal, not content — the per-task SSE consumer
    closes on it instead of polling writer liveness. Best-effort.
    """
    if not thread_id:
        return

    if task.redis_write_failed:
        return

    # Defensive guard: settings/cache imports are stable in normal
    # operation, so a raise here means a broken deployment — bail
    # quietly rather than crash the producer's astream loop.
    try:
        from src.config.settings import (
            get_max_stored_messages_per_agent,
            get_redis_ttl_workflow_events,
            is_subagent_event_redis_spill_enabled,
        )
        if not is_subagent_event_redis_spill_enabled():
            return
        from src.utils.cache.redis_cache import get_cache_client

        cache = get_cache_client()
    except Exception:
        return

    if not getattr(cache, "enabled", False) or not getattr(cache, "client", None):
        return

    stream_key = task_stream_key(thread_id, task.task_id)
    payload = json.dumps(
        {"event": SUBAGENT_STREAM_END_EVENT}, ensure_ascii=False
    ).encode("utf-8")

    # Hold redis_spill_lock across pipe.execute() so the sentinel cannot
    # land before an in-flight content spill on the same task. Auto-id
    # XADD ordering is only a server-side guarantee — once two pipelines
    # are both in flight, either can win the race. The per-task lock is
    # the issue-order guarantee: a concurrent content spill must finish
    # its XADD before the sentinel's pipeline opens; otherwise the
    # consumer exits on the sentinel and the late content event is lost.
    # _SPILL_TIMEOUT_SECONDS caps queue depth under load.
    #
    # ``wait_for`` timeout window: if the timeout fires *after*
    # ``pipe.execute()`` has already dispatched the commands but before
    # Redis ACKs, the sentinel XADD has already landed. The lock is then
    # released and a queued content spill will write its XADD *after* the
    # sentinel — at which point the consumer has already exited on the
    # sentinel and that late event is lost. Best-effort by design; the
    # sub-500-ms window makes it astronomically unlikely under normal
    # load, and the fallback (``terminal_check`` closes the stream once
    # the asyncio task finishes) still fires on the next BLOCK timeout.
    try:
        async with task.redis_spill_lock:
            async with cache.client.pipeline(transaction=False) as pipe:
                pipe.xadd(
                    stream_key,
                    {b"event": payload},
                    maxlen=get_max_stored_messages_per_agent() * 2,
                    approximate=True,
                )
                pipe.expire(stream_key, get_redis_ttl_workflow_events())
                await asyncio.wait_for(
                    pipe.execute(),
                    timeout=_SPILL_TIMEOUT_SECONDS,
                )
    except Exception as exc:
        logger.debug(
            "subagent_stream_end_sentinel_failed",
            tool_call_id=task.tool_call_id,
            task_id=task.task_id,
            error=str(exc),
        )


def steering_queue_key(tool_call_id: str, task_run_id: str | None = None) -> str:
    """Redis list a subagent drains for follow-up steering input.

    Run-scoped when the execution's ledger identity is known — a queued
    message can then never be consumed by a later resume of the same task;
    the run-end sweep returns whatever its own run left unconsumed. The
    task-lifetime key is the pre-ledger fallback (legacy semantics: next
    writer drains it).
    """
    if task_run_id:
        return f"subagent:steering:{tool_call_id}:{task_run_id}"
    return f"subagent:steering:{tool_call_id}"


def parse_steering_payload(raw: Any) -> dict[str, Any] | None:
    """Normalize a raw steering-queue entry to ``{content,
    expected_task_run_id, input_id}``. Legacy entries are bare JSON strings
    (no fence, no id); unparseable entries return None."""
    try:
        data = json.loads(raw.decode("utf-8") if isinstance(raw, bytes) else raw)
    except (json.JSONDecodeError, UnicodeDecodeError):
        return None
    if isinstance(data, str):
        return {"content": data, "expected_task_run_id": None, "input_id": None}
    if isinstance(data, dict) and data.get("content"):
        return {
            "content": str(data["content"]),
            "expected_task_run_id": data.get("expected_task_run_id") or None,
            "input_id": data.get("input_id") or None,
        }
    return None


async def read_task_meta(thread_id: str, task_id: str) -> dict[str, str] | None:
    """Read the cross-worker task meta hash written by ``write_task_meta``.

    Returns a decoded str->str dict, or None when the key is absent, Redis is
    unavailable, or the read fails (callers treat None as "no distributed
    knowledge" and fall back to local/checkpoint state).
    """
    if not thread_id or not task_id:
        return None
    try:
        from src.utils.cache.redis_cache import get_cache_client

        cache = get_cache_client()
        if not getattr(cache, "enabled", False) or not cache.client:
            return None
        raw = await cache.client.hgetall(f"subagent:meta:{thread_id}:{task_id}")
        if not raw:
            return None
        return {
            (k.decode() if isinstance(k, bytes) else str(k)): (
                v.decode() if isinstance(v, bytes) else str(v)
            )
            for k, v in raw.items()
        }
    except Exception as exc:
        logger.warning(
            "task meta read failed", thread_id=thread_id, task_id=task_id,
            error=str(exc),
        )
        return None
