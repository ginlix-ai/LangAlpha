"""V1 per-subagent SSE reader — dies with the v1 lane (Phase 7).

Serves GET /threads/{tid}/tasks/{task_id} from ``subagent:stream:*`` keys,
including legacy JSON records that pre-date the wire-format streams. The v2
mux (thread_stream_mux_v2) replaces this surface; new code must not grow it.
"""

from __future__ import annotations

import asyncio
import json
import logging
from typing import Any, AsyncGenerator, Optional

from ptc_agent.agent.middleware.background_subagent.redis_stream import (
    SUBAGENT_STREAM_END_EVENT,
    read_task_meta,
)
from src.server.services.background_registry_store import BackgroundRegistryStore
from src.server.handlers.chat.run_stream_reader import _stream_from_redis_log

# Same hard-coded logger name request_prep uses — existing log routing keys off it.
logger = logging.getLogger("src.server.handlers.chat_handler")

_SUBAGENT_STARTUP_TIMEOUT = 30.0
_SUBAGENT_STARTUP_POLL = 0.5


# ---------------------------------------------------------------------------
# Subagent-task consumer
# ---------------------------------------------------------------------------


async def _wait_for_subagent_task(thread_id: str, task_id: str) -> Any:
    """Block until the per-task BackgroundTask exists locally, the task is
    known cross-worker, or timeout.

    Returns the local task object, or None when another worker owns the
    task (its Redis meta exists — no point polling this process's registry
    for 30s) or nothing ever appears. None-callers stream straight from
    Redis. The window only serves the true spawn race: GET arriving before
    the producer has registered anywhere.
    """
    registry_store = BackgroundRegistryStore.get_instance()
    waited = 0.0
    while waited < _SUBAGENT_STARTUP_TIMEOUT:
        registry = await registry_store.get_registry(thread_id)
        if registry is not None:
            task = await registry.get_task_by_task_id(task_id)
            if task is not None:
                return task
        if await read_task_meta(thread_id, task_id) is not None:
            return None
        await asyncio.sleep(_SUBAGENT_STARTUP_POLL)
        waited += _SUBAGENT_STARTUP_POLL
    return None


async def _subagent_writer_settled(thread_id: str, task_id: str) -> bool:
    """Cross-worker terminal predicate for a per-task stream consumer with
    no local live writer.

    Lock-first: the N(thread, task:id) advisory lock IS the liveness signal
    — held for the writer's whole life (kept even through a double-cancel
    with the inner handler still running), released on settle or worker
    death. Meta breaks the tie only when the probe is unavailable, because
    meta can lag or be lost while the lock is authoritative (a resume that
    re-locked but hasn't rewritten terminal meta yet; a spawn whose Redis
    publication failed). The producer's stream-end sentinel remains the
    canonical close signal; this predicate only ends sentinel-less streams.
    """
    from src.server.services.writer_guard import held_task_namespaces

    held = await held_task_namespaces(thread_id, [task_id])
    if held is not None:
        return task_id not in held
    meta = await read_task_meta(thread_id, task_id)
    return meta is None or meta.get("status") != "running"


# Subagent payload classifications used by ``_classify_subagent_payload``.
# Strings rather than enum members because the consumer hot loop branches
# directly on the return value and string equality stays cheap
# (these constants intern at module load).
_PAYLOAD_WIRE = "wire"          # already SSE-formatted; pass-through
_PAYLOAD_SENTINEL = "sentinel"  # stream-end signal; consumer exits
_PAYLOAD_RECORD = "record"      # legacy JSON record; needs SSE rendering
_PAYLOAD_UNKNOWN = "unknown"    # invalid or unrecognised; pass through raw


def _classify_subagent_payload(raw: str) -> tuple[str, dict | None]:
    """Single-pass classification of a per-task Stream entry.

    The per-task Stream may carry three shapes:

    1. **Pre-rendered SSE wire strings** — steady-state producer output
       (``id: N\\nevent: ...\\ndata: ...\\n\\n``). Yielded verbatim.
    2. **Stream-end sentinels** — ``{"event": "subagent_stream_end"}``
       written by ``forwarder.finalize()``. Consumer exits on sight.
    3. **Legacy JSON records** — ``{"seq", "event", "data", "agent_id"}``
       from older producer versions; age out after their TTL. Rendered by
       ``_record_to_sse``.

    Returns ``(kind, parsed_or_None)``; ``parsed_or_None`` is non-None
    only for ``_PAYLOAD_RECORD``.
    """
    # Fast path: pre-rendered SSE always starts with ``id:`` (the producer
    # in ``registry._spill_record_to_redis`` emits ``id:`` first) or ``:``
    # for keepalive comments. If a future producer widens the wire format
    # so it no longer starts with one of those, this fast path will fall
    # through to the JSON-decode branch and the entry will be classified
    # as ``_PAYLOAD_UNKNOWN`` (still passed through raw, but bypassing the
    # intended fast path). Audit both sites together if the producer
    # changes.
    if not raw or raw[0] in ("i", ":"):
        return _PAYLOAD_WIRE, None
    try:
        record = json.loads(raw)
    except json.JSONDecodeError:
        return _PAYLOAD_UNKNOWN, None
    if not isinstance(record, dict):
        return _PAYLOAD_UNKNOWN, None
    if record.get("event") == SUBAGENT_STREAM_END_EVENT and "seq" not in record:
        # Sentinels carry only ``event`` — no ``seq``. The absence of ``seq``
        # also makes a stray sentinel fall through ``_record_to_sse``'s
        # legacy-record path rather than rendering as a fake event.
        return _PAYLOAD_SENTINEL, None
    if "seq" in record:
        return _PAYLOAD_RECORD, record
    return _PAYLOAD_UNKNOWN, None


def _record_to_sse(record: dict, thread_id: str, task_id: str) -> str:
    """Render a stored subagent record dict as SSE wire format.

    The producer stores records without thread_id/task_id (the registry only
    knows agent_id), so those are injected here.
    """
    seq = int(record.get("seq") or 0)
    # Inner data spreads first so consumer-injected thread_id/agent always
    # win — ``record["agent_id"]`` is the LangGraph namespace UUID and must
    # not surface as the user-facing label.
    data = {
        **(record.get("data") or {}),
        "thread_id": thread_id,
        "agent": f"task:{task_id}",
    }
    return (
        f"id: {seq}\n"
        f"event: {record.get('event') or 'message_chunk'}\n"
        f"data: {json.dumps(data, ensure_ascii=False)}\n\n"
    )


async def stream_subagent_from_log(
    thread_id: str,
    task_id: str,
    last_event_id: Optional[int] = None,
) -> AsyncGenerator[str, None]:
    """SSE consumer for a single subagent's stream.

    Mirrors ``stream_from_log`` but adds:
    - Startup-window wait for the registry/task to exist.
    - ``sse_consumer_count`` increment/decrement so cleanup waits for live
      readers before DELing the stream key.
    - Terminal predicate that checks task completion (``task.completed`` or
      ``task.asyncio_task.done()``).
    - Per-entry payload classification via ``_classify_subagent_payload``
      so pre-rendered wire strings, stream-end sentinels, and legacy JSON
      records are dispatched in a single pass with at most one
      ``json.loads`` per entry.

    Without a local writer (peer worker under ``--workers>1``, evicted
    entry, inert placeholder) the stream is served straight from Redis and
    stays open while the cross-worker signals say the producer is alive —
    never assume terminal just because THIS process doesn't know the task.
    """
    task = await _wait_for_subagent_task(thread_id, task_id)
    stream_key = f"subagent:stream:{thread_id}:{task_id}"

    def _dispatch(raw: str) -> str | None:
        """Return the SSE string to yield, or None to signal stream-end."""
        kind, parsed = _classify_subagent_payload(raw)
        if kind == _PAYLOAD_SENTINEL:
            return None
        if kind == _PAYLOAD_RECORD and parsed is not None:
            return _record_to_sse(parsed, thread_id, task_id)
        # _PAYLOAD_WIRE / _PAYLOAD_UNKNOWN: pass through raw.
        return raw

    # A local entry counts as the writer only while its asyncio task is
    # mid-flight in THIS process. Anything else — no entry (peer worker,
    # evicted after settle), an inert placeholder (asyncio_task=None), or a
    # done outer task (a double-cancel can leave the shielded inner handler
    # still emitting) — must not be trusted for liveness: the namespace
    # lock is the truth.
    ato = task.asyncio_task if task is not None else None
    if ato is None or ato.done():

        async def _term_remote() -> bool:
            return await _subagent_writer_settled(thread_id, task_id)

        inner = _stream_from_redis_log(
            stream_key=stream_key,
            terminal_check=_term_remote,
            last_event_id=last_event_id,
        )
    else:
        async def on_attach() -> None:
            task.sse_consumer_count += 1

        async def on_detach() -> None:
            task.sse_consumer_count -= 1
            if task.sse_consumer_count <= 0:
                try:
                    task.sse_drain_complete.set()
                except Exception:
                    pass

        async def terminal_check() -> bool:
            if task.completed:
                return True
            ato = task.asyncio_task
            return ato is not None and ato.done()

        inner = _stream_from_redis_log(
            stream_key=stream_key,
            terminal_check=terminal_check,
            last_event_id=last_event_id,
            on_attach=on_attach,
            on_detach=on_detach,
        )

    # Manage the inner generator explicitly so its ``finally`` block (which
    # runs ``on_detach`` and decrements ``sse_consumer_count``) fires the
    # moment we ``return`` on a sentinel — not whenever FastAPI gets around
    # to calling ``aclose`` on the outer generator. Tight ``sse_drain_complete``
    # waits race the GC otherwise.
    try:
        async for raw in inner:
            rendered = _dispatch(raw)
            if rendered is None:
                return
            yield rendered
    finally:
        await inner.aclose()