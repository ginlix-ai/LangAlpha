"""Redis-Streams-backed SSE consumer.

Replaces the parallel live-queue / List-replay paths with one XREAD BLOCK
loop that every consumer (first-connect, reconnect, second tab, subagent
SSE, late subscriber) traverses identically. The workflow is a fire-and-
forget producer that writes to ``workflow:stream:{thread_id}`` and
``subagent:stream:{thread_id}:{task_id}``; consumers attach by stream key
and read by cursor — they share no in-process state with the workflow.

Both streams store pre-rendered SSE wire strings in the steady state.
The subagent consumer also handles legacy JSON records
(``{seq, event, data, agent_id}``) written by earlier producer
versions — those age out after their TTL window.

Gated behind ``USE_REDIS_STREAM_SSE`` per request.
"""

from __future__ import annotations

import asyncio
import json
import logging
from typing import Any, AsyncGenerator, Awaitable, Callable, Optional

from src.config.settings import get_redis_socket_timeout
from ptc_agent.agent.middleware.background_subagent.registry import (
    SUBAGENT_STREAM_END_EVENT,
)
from src.server.services.background_registry_store import BackgroundRegistryStore
from src.server.services.background_task_manager import (
    BackgroundTaskManager,
    TaskStatus,
)
from src.utils.cache.redis_cache import get_cache_client

logger = logging.getLogger(__name__)


_XREAD_BLOCK_MARGIN_MS = 1_000
_XREAD_BLOCK_FLOOR_MS = 500


def _xread_block_ms() -> int:
    """Compute XREAD's BLOCK arg given the pool's socket_timeout.

    redis-py applies the connection's ``socket_timeout`` to every command,
    blocking ones included. If BLOCK >= socket_timeout the socket read
    raises ``Timeout reading from redis`` before XREAD ever returns. We
    keep BLOCK strictly below socket_timeout by ``_XREAD_BLOCK_MARGIN_MS``
    (1 s by default — the cost is one extra XREAD round-trip per
    ``socket_timeout - 1`` s on idle streams, negligible vs LLM latency).

    When ``socket_timeout`` is configured very low (1-2 s) the natural
    ``timeout - margin`` would go to zero or negative; we floor at
    ``_XREAD_BLOCK_FLOOR_MS`` (500 ms) so the consumer still polls at a
    sane cadence. The accepted trade-off is that with ``socket_timeout=1
    s`` the safety margin shrinks from 1 s to 500 ms — still positive, but
    redis-py is more likely to win the race and surface a Timeout. Bump
    ``redis.socket_timeout`` (config.yaml) above 2 s in production.
    """
    socket_seconds = get_redis_socket_timeout() or 5
    socket_ms = max(1, socket_seconds) * 1_000
    return max(_XREAD_BLOCK_FLOOR_MS, socket_ms - _XREAD_BLOCK_MARGIN_MS)


# Cap entries per XREAD round. Keeps us responsive to terminal-check
# polling under sustained traffic without per-event round-trips.
_XREAD_COUNT = 100

# Startup window for subagent: how long to wait for the registry/task
# to come into existence before giving up. The registry is created when
# the subagent middleware first runs; for short-lived turns it can take
# a few seconds.
_SUBAGENT_STARTUP_TIMEOUT = 30.0
_SUBAGENT_STARTUP_POLL = 0.5


async def _stream_from_redis_log(
    stream_key: str,
    terminal_check: Callable[[], Awaitable[bool]],
    last_event_id: Optional[int] = None,
    on_attach: Optional[Callable[[], Awaitable[None]]] = None,
    on_detach: Optional[Callable[[], Awaitable[None]]] = None,
) -> AsyncGenerator[str, None]:
    """Generic XREAD BLOCK loop yielding SSE strings stored in a Redis Stream.

    Cursor semantics (``last_event_id`` argument):
    - ``None`` or ``<= 0`` → start at ``0`` (replay everything in the
      stream + then wait for new). This is the right default for a fresh
      attach: chat clients want history first, then live updates. The
      ``$`` ("live tail only") cursor is intentionally NOT exposed because
      no caller currently wants it — by the time per-task SSE consumers
      attach, the subagent has already been writing events for hundreds
      of ms, and live-tail-only would miss them.
    - ``> 0`` → resume after seq N (XREAD's ``after`` is exclusive on the
      explicit ID, so the next emitted entry will be seq=N+1).

    ``terminal_check()`` is invoked after each empty XREAD round — when
    it returns True and the next XREAD round still returns no entries,
    the generator exits (two-empty-round handshake avoids missing a late
    tail event between status flip and stream drain).

    ``on_attach``/``on_detach`` let subagent consumers maintain
    ``sse_consumer_count`` so cleanup waits for live readers to drain
    before DELing the stream.
    """
    cache = get_cache_client()
    if not cache.enabled or not cache.client:
        logger.warning(
            "[stream_from_log] Redis disabled — no events to stream for %s",
            stream_key,
        )
        return

    if last_event_id is None or last_event_id <= 0:
        cursor: bytes = b"0"
    else:
        # XREAD reads strictly AFTER the given ID; use `<seq>-0` to start
        # right after seq N (so the next entry seq=N+1 is returned).
        cursor = f"{last_event_id}-0".encode("utf-8")

    stream_key_bytes = stream_key.encode("utf-8")
    block_ms = _xread_block_ms()

    attached = False
    try:
        if on_attach is not None:
            await on_attach()
            attached = True
        terminal_seen = False
        while True:
            try:
                # asyncio.wait_for guards against the underlying redis-py
                # XREAD hanging past BLOCK if the connection is poisoned.
                # The buffer is small because BLOCK is already < socket_timeout.
                result = await asyncio.wait_for(
                    cache.client.xread(
                        {stream_key_bytes: cursor},
                        block=block_ms,
                        count=_XREAD_COUNT,
                    ),
                    timeout=(block_ms / 1000.0) + 2.0,
                )
            except asyncio.TimeoutError:
                # XREAD wedged — yield keepalive, recheck terminal, retry.
                yield ":keepalive\n\n"
                if await terminal_check():
                    if terminal_seen:
                        return
                    terminal_seen = True
                continue
            except (asyncio.CancelledError, GeneratorExit):
                raise
            except Exception as exc:
                logger.warning(
                    "[stream_from_log] XREAD failed on %s: %s",
                    stream_key,
                    exc,
                )
                # If the workflow has already terminated and reads are now
                # erroring (likely Redis transient + stream already DEL'd),
                # there is nothing more to drain — exit instead of looping.
                if await terminal_check():
                    return
                # Brief backoff to avoid tight error loops, then retry.
                await asyncio.sleep(0.5)
                continue

            if not result:
                # BLOCK timed out — emit keepalive comment so proxies and
                # the browser see liveness, then re-check terminal.
                yield ":keepalive\n\n"
                if await terminal_check():
                    if terminal_seen:
                        return
                    terminal_seen = True
                continue

            # result format: [(stream_key, [(entry_id, {field: value}), ...])]
            for _stream_name, entries in result:
                for entry_id, fields in entries:
                    payload = fields.get(b"event")
                    if payload is None:
                        continue
                    if isinstance(payload, bytes):
                        try:
                            yield payload.decode("utf-8")
                        except UnicodeDecodeError:
                            logger.warning(
                                "[stream_from_log] Non-UTF8 payload in %s entry %s",
                                stream_key,
                                entry_id,
                            )
                            continue
                    else:
                        yield payload
                    cursor = entry_id

            # Reset terminal-seen flag whenever we emit new events; the
            # exit handshake requires two consecutive empty rounds with
            # terminal=True so we don't race past the trailing tail.
            terminal_seen = False
    finally:
        if attached and on_detach is not None:
            try:
                await on_detach()
            except Exception:
                logger.exception("[stream_from_log] on_detach hook raised for %s", stream_key)


# ---------------------------------------------------------------------------
# Main-workflow consumer
# ---------------------------------------------------------------------------


async def stream_from_log(
    thread_id: str,
    last_event_id: Optional[int] = None,
) -> AsyncGenerator[str, None]:
    """SSE consumer for the main workflow stream.

    First-connect callers pass ``last_event_id=None`` (cursor at 0 — replay
    everything in the stream then wait for new). Reconnect callers pass the
    integer seq from ``Last-Event-ID`` / ``?last_event_id=`` so XREAD
    resumes after that seq.

    The ``increment_connection``/``decrement_connection`` calls bump the
    workflow's ``active_connections`` counter (and refresh
    ``last_access_at``). Without them, the abandoned-task cleanup at
    ``BackgroundTaskManager._periodic_cleanup`` would force-cancel any
    RUNNING task whose ``active_connections == 0`` after
    ``abandoned_workflow_timeout`` (6 h default).
    """
    manager = BackgroundTaskManager.get_instance()

    async def terminal_check() -> bool:
        status = await manager.get_task_status(thread_id)
        return status in (TaskStatus.COMPLETED, TaskStatus.FAILED, TaskStatus.CANCELLED)

    async def on_attach() -> None:
        await manager.increment_connection(thread_id)

    async def on_detach() -> None:
        await manager.decrement_connection(thread_id)

    async for event in _stream_from_redis_log(
        stream_key=f"workflow:stream:{thread_id}",
        terminal_check=terminal_check,
        last_event_id=last_event_id,
        on_attach=on_attach,
        on_detach=on_detach,
    ):
        yield event


# ---------------------------------------------------------------------------
# Subagent-task consumer
# ---------------------------------------------------------------------------


async def _wait_for_subagent_task(thread_id: str, task_id: str) -> Any:
    """Block until the per-task BackgroundTask exists, or timeout.

    Returns the task object on success, None on timeout.
    """
    registry_store = BackgroundRegistryStore.get_instance()
    waited = 0.0
    while waited < _SUBAGENT_STARTUP_TIMEOUT:
        registry = await registry_store.get_registry(thread_id)
        if registry is not None:
            task = await registry.get_task_by_task_id(task_id)
            if task is not None:
                return task
        await asyncio.sleep(_SUBAGENT_STARTUP_POLL)
        waited += _SUBAGENT_STARTUP_POLL
    return None


# Subagent payload classifications used by ``_classify_subagent_payload``.
# Strings rather than enum members because the consumer hot loop branches
# directly on the return value and string identity comparison stays cheap
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

    Mirrors the legacy consumer's ``_record_to_sse`` (stream_reconnect.py)
    so frontend parsers see byte-for-byte the same wire format whether the
    flag is on or off. The producer stores records without thread_id /
    task_id (the registry only knows agent_id), so those are injected here.
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

    On startup-timeout (no task ever appears), we still tail the Redis
    Stream for ``last_event_id`` replay — the producer may have written
    events that arrived before the registry call finished registering.
    """
    task = await _wait_for_subagent_task(thread_id, task_id)
    stream_key = f"subagent:stream:{thread_id}:{task_id}"

    def _dispatch(raw: str) -> str | None:
        """Return the SSE string to yield, or None to signal stream-end."""
        kind, parsed = _classify_subagent_payload(raw)
        if kind is _PAYLOAD_SENTINEL:
            return None
        if kind is _PAYLOAD_RECORD and parsed is not None:
            return _record_to_sse(parsed, thread_id, task_id)
        # _PAYLOAD_WIRE / _PAYLOAD_UNKNOWN: pass through raw.
        return raw

    if task is None:
        # No registry — treat as a pure replay-from-Redis case. If the
        # stream key doesn't exist either (TTL expired or never written),
        # XREAD returns empty and we fall out via the terminal check.
        async def _term_no_task() -> bool:
            return True  # Nothing to wait for; stream is already at end.

        async for raw in _stream_from_redis_log(
            stream_key=stream_key,
            terminal_check=_term_no_task,
            last_event_id=last_event_id,
        ):
            rendered = _dispatch(raw)
            if rendered is None:
                return
            yield rendered
        return

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

    async for raw in _stream_from_redis_log(
        stream_key=stream_key,
        terminal_check=terminal_check,
        last_event_id=last_event_id,
        on_attach=on_attach,
        on_detach=on_detach,
    ):
        rendered = _dispatch(raw)
        if rendered is None:
            return
        yield rendered
