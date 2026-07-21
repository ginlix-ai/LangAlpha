"""Redis-Streams-backed SSE consumer.

One XREAD BLOCK loop serves every consumer type (first-connect, reconnect,
second tab, subagent SSE, late subscriber). The workflow is a fire-and-forget
producer writing to ``workflow:stream:{thread_id}`` and
``subagent:stream:{thread_id}:{task_id}``; consumers attach by stream key and
read by cursor with no in-process state shared with the workflow.

Both streams store pre-rendered SSE wire strings plus a terminal JSON
sentinel (``{"event": ...}``) that closes attached consumers immediately at
run end. The subagent consumer also handles legacy JSON records
(``{seq, event, data, agent_id}``) that age out after their TTL window.
"""

from __future__ import annotations

import asyncio
import json
from typing import Any, AsyncGenerator, Awaitable, Callable, Optional

from src.config.settings import get_redis_socket_timeout
from ptc_agent.agent.middleware.background_subagent.registry import (
    SUBAGENT_STREAM_END_EVENT,
    read_task_meta,
)
from src.server.services.background_registry_store import BackgroundRegistryStore
from src.server.services.background_task_manager import (
    WORKFLOW_RUN_END_EVENT,
    BackgroundTaskManager,
)
from src.utils.cache.redis_cache import get_cache_client

from ._common import logger

# Pre-finalize stream-end sentinel written by pre-1.5 producers. Nothing on
# this release writes it; the swallow in ``_stream_from_redis_log`` is
# one-release deploy compat for streams the previous release produced
# (bounded by the 24h stream TTL). Remove after the first deploy cycle.
WORKFLOW_STREAM_END_EVENT = "workflow_stream_end"


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


def _is_stream_end_sentinel(raw: str, sentinel_event: str) -> bool:
    """True when ``raw`` is a terminal sentinel record ``{"event": <sentinel>}``.

    Real payloads are SSE wire strings, so anything not starting with ``{``
    bails before ``json.loads``. ``seq`` must be absent, mirroring
    ``_classify_subagent_payload``'s sentinel shape.
    """
    if not raw or raw[0] != "{":
        return False
    try:
        record = json.loads(raw)
    except json.JSONDecodeError:
        return False
    return (
        isinstance(record, dict)
        and record.get("event") == sentinel_event
        and "seq" not in record
    )


def _first_available_seq(entries: list) -> Optional[int]:
    """Parse the seq of the first ``id: N``-prefixed entry, or None.

    Auto-ID entries (run_end / legacy sentinels) have no seq and sort after
    every explicit ``seq-0`` ID, so the first parseable entry IS the oldest
    surviving real event.
    """
    for _entry_id, fields in entries:
        payload = fields.get(b"event")
        if isinstance(payload, bytes):
            try:
                payload = payload.decode("utf-8")
            except UnicodeDecodeError:
                continue
        if not payload or not payload.startswith("id: "):
            return None
        first_line, _, _ = payload.partition("\n")
        try:
            return int(first_line[4:].strip())
        except ValueError:
            return None
    return None


async def _stream_from_redis_log(
    stream_key: str,
    terminal_check: Callable[[], Awaitable[bool]],
    last_event_id: Optional[int] = None,
    on_attach: Optional[Callable[[], Awaitable[None]]] = None,
    on_detach: Optional[Callable[[], Awaitable[None]]] = None,
    sentinel_event: Optional[str] = None,
    close_event: Optional[str] = None,
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

    ``sentinel_event`` (optional) names a producer-written stream-end
    marker (``{"event": <name>}``): on reading it the generator returns
    immediately without yielding it, skipping the handshake above. The
    handshake stays as the fallback for sentinel-less streams (crashed
    producers, pre-deploy buffers).

    ``close_event`` (optional) names an SSE event (wire-string entries
    starting ``event: <name>\\n``) that is YIELDED to the client and then
    ends the stream — the visible ``run_end`` frame, vs the swallowed
    legacy sentinel above.
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

    if last_event_id is not None and last_event_id > 0:
        # Trimmed-head detection (1.5c): if the oldest surviving entry's seq
        # is beyond the client's cursor + 1, events were dropped (FIFO maxlen
        # trim or TTL churn). Say so explicitly instead of replaying with a
        # silent hole.
        try:
            head = await cache.client.xrange(stream_key_bytes, count=1)
            first_seq = _first_available_seq(head) if head else None
            if first_seq is not None and first_seq > last_event_id + 1:
                gap = json.dumps(
                    {
                        "expected_from": last_event_id + 1,
                        "first_available": first_seq,
                    },
                    ensure_ascii=False,
                )
                yield f"event: stream_gap\ndata: {gap}\n\n"
        except Exception as exc:
            logger.warning(
                "[stream_from_log] gap probe failed on %s: %s", stream_key, exc
            )

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
                #
                # Sized so the outer wait_for fires AFTER redis-py's own
                # socket_timeout. Recall ``block_ms = socket_timeout -
                # _XREAD_BLOCK_MARGIN_MS`` (i.e. socket_timeout - 1 s) from
                # ``_xread_block_ms``. Adding 2.0 s here gives an outer
                # timeout of ``socket_timeout + 1 s`` — redis-py gets a full
                # second past socket_timeout to surface its own
                # ``Timeout reading from redis`` before wait_for races it.
                # Using ``+ 1.0`` would equal socket_timeout and produce a
                # racy double-fire.
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
                # Counts as one "empty" round for the terminal handshake:
                # a wedged read followed by one empty BLOCK round still
                # exits, dwelling ``(block_ms + 2 s) + block_ms`` total —
                # long enough to drain a trailing tail.
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
                    # Advance the cursor unconditionally before any skip path.
                    # If the *last* entry in a batch hits a continue (missing
                    # ``event`` field, non-UTF8 payload), leaving the cursor
                    # behind that entry would make the next XREAD return it
                    # again — a skip-loop that never terminates.
                    cursor = entry_id
                    payload = fields.get(b"event")
                    if payload is None:
                        continue
                    if isinstance(payload, bytes):
                        try:
                            payload = payload.decode("utf-8")
                        except UnicodeDecodeError:
                            logger.warning(
                                "[stream_from_log] Non-UTF8 payload in %s entry %s",
                                stream_key,
                                entry_id,
                            )
                            continue
                    if sentinel_event is not None and _is_stream_end_sentinel(
                        payload, sentinel_event
                    ):
                        # Legacy pre-finalize sentinel (pre-1.5 producers) —
                        # close without yielding it to the wire.
                        return
                    if close_event is not None and payload.startswith(
                        f"event: {close_event}\n"
                    ):
                        # Visible end-of-run frame: deliver it, then close.
                        yield payload
                        return
                    yield payload

            # Reset terminal-seen on any non-empty XREAD batch: while
            # entries are still arriving the stream is not yet at end, so
            # the two-empty-round handshake must restart. (Note: this
            # resets even when every entry was skipped — the producer
            # always writes ``b"event"`` so all-skipped batches don't
            # occur in practice; if that invariant changes, revisit.)
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
    run_id: Optional[str] = None,
    last_event_id: Optional[int] = None,
) -> AsyncGenerator[str, None]:
    """SSE consumer for the main workflow stream.

    The stream is keyed by ``(thread_id, run_id)``. Callers without a
    ``run_id`` fall back to the most recent run on the thread (status
    endpoint convenience) — resolved locally first, then from the ledger
    (v4 2.4), so the resolution works on any worker and after a restart.
    With no ledger row at all there is nothing durable to stream: yield
    nothing (the legacy thread-only stream key died with the tracker
    cutover).
    """
    manager = BackgroundTaskManager.get_instance()

    # Resolve run_id if caller didn't provide one. We may be reconnecting
    # to a turn that is still in the cache (latest) — pick the most recent.
    if run_id is None:
        async with manager.task_lock:
            info = manager._find_latest_for_thread(thread_id)
        if info is not None:
            run_id = info.run_id

    if run_id is None:
        # No in-process TaskInfo (restart, or the run lives on another
        # worker): the ledger names the thread's most recent run — its
        # retained stream is the replay source even when terminal.
        from src.server.database import turn_lifecycle as tl_db

        row = await tl_db.get_active_run(thread_id)
        if row is None:
            row = await tl_db.get_latest_attempt(thread_id)
        if row is not None:
            run_id = str(row["conversation_response_id"])

    if run_id is None:
        return

    async def terminal_check() -> bool:
        # Local record answers cheaply; otherwise the ledger row decides —
        # a foreign worker's live run must keep this watcher attached, and
        # its terminal row (owner finalize or recovery scanner) releases it.
        info = manager.tasks.get((thread_id, run_id))
        if info is not None:
            return info.status.terminal
        from src.server.database import turn_lifecycle as tl_db

        row = await tl_db.get_run(run_id)
        if row is None:
            return True
        return row["status"] != "in_progress"

    async def on_attach() -> None:
        await manager.increment_connection(thread_id, run_id)

    async def on_detach() -> None:
        await manager.decrement_connection(thread_id, run_id)

    async for event in _stream_from_redis_log(
        stream_key=f"workflow:stream:{thread_id}:{run_id}",
        terminal_check=terminal_check,
        last_event_id=last_event_id,
        on_attach=on_attach,
        on_detach=on_detach,
        sentinel_event=WORKFLOW_STREAM_END_EVENT,
        close_event=WORKFLOW_RUN_END_EVENT,
    ):
        yield event


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
