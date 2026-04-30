"""Redis-Streams-backed SSE consumer.

Replaces the parallel live-queue / List-replay paths with one XREAD BLOCK
loop that every consumer (first-connect, reconnect, second tab, subagent
SSE, late subscriber) traverses identically. The workflow is a fire-and-
forget producer that writes to ``workflow:stream:{thread_id}`` and
``subagent:stream:{thread_id}:{task_id}``; consumers attach by stream key
and read by cursor — they share no in-process state with the workflow.

Gated behind ``USE_REDIS_STREAM_SSE`` per request.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any, AsyncGenerator, Awaitable, Callable, Optional

from src.server.services.background_registry_store import BackgroundRegistryStore
from src.server.services.background_task_manager import (
    BackgroundTaskManager,
    TaskStatus,
)
from src.utils.cache.redis_cache import get_cache_client

logger = logging.getLogger(__name__)

# XREAD BLOCK timeout. Long enough to avoid CPU-burning polling, short
# enough to keep proxies / load balancers from severing idle SSE
# connections (CDNs typically idle-time at 60–120 s).
_XREAD_BLOCK_MS = 30_000

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

    Cursor semantics:
    - ``last_event_id is None`` → start at ``$`` (live tail; emit only
      events appended after attach).
    - ``last_event_id == 0`` → start at ``0`` (replay from the beginning).
    - ``last_event_id > 0`` → resume after seq N (XREAD's ``after`` is
      exclusive on the explicit ID).

    ``terminal_check()`` is invoked after each empty XREAD round (BLOCK
    timed out with no new entries) — when it returns True and the next
    XREAD round still returns no entries, the generator exits.

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

    if last_event_id is None:
        cursor: bytes = b"$"
    elif last_event_id <= 0:
        cursor = b"0"
    else:
        # XREAD reads strictly AFTER the given ID; use `<seq>-0` to start
        # right after seq N (so the next entry seq=N+1 is returned).
        cursor = f"{last_event_id}-0".encode("utf-8")

    if on_attach is not None:
        await on_attach()

    stream_key_bytes = stream_key.encode("utf-8")

    try:
        terminal_seen = False
        while True:
            try:
                # asyncio.wait_for guards against the underlying redis-py
                # XREAD hanging past BLOCK if the connection is poisoned.
                result = await asyncio.wait_for(
                    cache.client.xread(
                        {stream_key_bytes: cursor},
                        block=_XREAD_BLOCK_MS,
                        count=_XREAD_COUNT,
                    ),
                    timeout=(_XREAD_BLOCK_MS / 1000.0) + 5.0,
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
        if on_detach is not None:
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

    First-connect callers pass ``last_event_id=None`` (start at $); reconnect
    callers pass the integer seq from ``Last-Event-ID`` / ``?last_event_id=``
    so XREAD resumes after that seq.
    """
    manager = BackgroundTaskManager.get_instance()

    async def terminal_check() -> bool:
        status = await manager.get_task_status(thread_id)
        return status in (TaskStatus.COMPLETED, TaskStatus.FAILED, TaskStatus.CANCELLED)

    async for event in _stream_from_redis_log(
        stream_key=f"workflow:stream:{thread_id}",
        terminal_check=terminal_check,
        last_event_id=last_event_id,
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

    On startup-timeout (no task ever appears), we still tail the Redis
    Stream for ``last_event_id`` replay — the producer may have written
    events that arrived before the registry call finished registering.
    """
    task = await _wait_for_subagent_task(thread_id, task_id)

    if task is None:
        # No registry — treat as a pure replay-from-Redis case. If the
        # stream key doesn't exist either (TTL expired or never written),
        # XREAD returns empty and we fall out via the terminal check.
        async def _term_no_task() -> bool:
            return True  # Nothing to wait for; stream is already at end.

        async for event in _stream_from_redis_log(
            stream_key=f"subagent:stream:{thread_id}:{task_id}",
            terminal_check=_term_no_task,
            last_event_id=last_event_id,
        ):
            yield event
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

    async for event in _stream_from_redis_log(
        stream_key=f"subagent:stream:{thread_id}:{task_id}",
        terminal_check=terminal_check,
        last_event_id=last_event_id,
        on_attach=on_attach,
        on_detach=on_detach,
    ):
        yield event
