"""Redis-Streams-backed SSE consumer for the main run lane.

One XREAD BLOCK loop serves every consumer type (first-connect, reconnect,
second tab, late subscriber). The run is a fire-and-forget producer writing
to ``workflow:stream:{thread_id}:{run_id}``; consumers attach by stream key
and read by cursor with no in-process state shared with the run.

The stream stores pre-rendered SSE wire strings plus a terminal JSON
sentinel (``{"event": ...}``) that closes attached consumers immediately at
run end. The v1 per-subagent reader lives in legacy_task_sse_reader.py.
"""

from __future__ import annotations

import logging
import asyncio
import json
from typing import AsyncGenerator, Awaitable, Callable, Optional

from src.server.handlers.chat.xread_tuning import (
    XREAD_COUNT,
    XREAD_ERROR_BACKOFF_S,
    XREAD_EXHAUSTION_BACKOFF_S,
    xread_block_ms,
    xread_wait_timeout_s,
)
from src.server.services.runs.executor import LocalRunExecutor
from src.server.services.runs.stream_writer import RUN_END_EVENT_TYPE

from src.utils.cache.redis_cache import get_cache_client, is_pool_exhaustion
from src.utils.cache import stream_pool

# Emitted once by the main-workflow consumer where the replayed backlog ends
# and the live tail begins. The client batches everything before it. The
# ``_EVENT_TYPE`` suffix is what the event-ledger scan keys on, so the type
# stays visible even though the frame around it is built from the parameter.
CAUGHT_UP_EVENT_TYPE = "caught_up"

# Same hard-coded logger name request_prep uses — existing log routing keys off it.
logger = logging.getLogger("src.server.handlers.chat_handler")


# Pre-finalize stream-end sentinel written by pre-1.5 producers. Nothing on
# this release writes it; the swallow in ``_stream_from_redis_log`` is
# one-release deploy compat for streams the previous release produced
# (bounded by the 24h stream TTL). Remove after the first deploy cycle.
WORKFLOW_STREAM_END_EVENT = "workflow_stream_end"


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


def _entry_id_key(entry_id: bytes | str) -> tuple[int, int]:
    """Order key for a Redis stream ID (``ms-seq``).

    Explicit ``seq-0`` ids and auto ids (epoch ms) share one stream, and the
    tuple order is the order Redis itself uses, so a cursor compares against
    the head without caring which kind either one is.
    """
    text = entry_id.decode("utf-8") if isinstance(entry_id, bytes) else entry_id
    ms, _, seq = text.partition("-")
    try:
        return (int(ms), int(seq or 0))
    except ValueError:
        return (0, 0)


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
    caught_up_event: Optional[str] = None,
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

    ``caught_up_event`` (optional) names a marker frame yielded exactly once,
    where the cursor first reaches the stream head sampled at attach: what
    precedes it is backlog the client may apply in one pass, what follows is
    live. An empty stream marks the boundary at once (nothing to batch); an
    unreadable head sends no marker at all, and the client's quiet-window and
    cap timers close the backlog instead. An early marker would have the
    client present the whole retained backlog live, which is the retype the
    marker exists to prevent.
    """
    cache = get_cache_client()
    if not cache.enabled or not cache.client:
        logger.warning(
            "[stream_from_log] Redis disabled — no events to stream for %s",
            stream_key,
        )
        return

    # The blocking read lives on its own pool: this loop holds a connection for
    # the life of the stream, and parking that in the cache pool is what
    # starved every short op in the process.
    reader = await stream_pool.get_stream_reader_client(cache)
    if reader is None:
        logger.warning(
            "[stream_from_log] no stream-reader pool — cannot stream %s",
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
    block_ms = xread_block_ms()

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

    # Backlog/live boundary: the head at attach time. Read once, up front, so
    # a producer that keeps writing during the replay cannot push the marker
    # out indefinitely. An empty stream leaves the zero key, which the first
    # boundary check below clears; a failed probe drops the marker instead.
    caught_up_pending = caught_up_event is not None
    head_key: tuple[int, int] = (0, 0)
    if caught_up_event is not None:
        try:
            tail = await asyncio.wait_for(
                cache.client.xrevrange(stream_key_bytes, count=1),
                timeout=xread_wait_timeout_s(),
            )
            if tail:
                head_key = _entry_id_key(tail[0][0])
        except Exception as exc:
            caught_up_pending = False
            logger.warning(
                "[stream_from_log] head probe failed on %s, no boundary marker: %s",
                stream_key,
                exc,
            )
    caught_up_frame = f"event: {caught_up_event}\ndata: {{}}\n\n"

    attached = False
    try:
        if on_attach is not None:
            await on_attach()
            attached = True
        terminal_seen = False
        while True:
            # The boundary, checked once per round before the read: covers
            # the fresh attach and a batch that ends exactly on the head. A
            # batch that runs past it emits the marker mid-batch below.
            if caught_up_pending and _entry_id_key(cursor) >= head_key:
                caught_up_pending = False
                yield caught_up_frame
            try:
                # asyncio.wait_for guards against the underlying redis-py
                # XREAD hanging past BLOCK if the connection is poisoned.
                # It must stay a backstop: it has to fire after redis-py's
                # own socket_timeout AND after a cold connect's full budget,
                # or it cancels mid-handshake and redials. Both terms are
                # derived in ``xread_tuning.xread_wait_timeout_s``.
                result = await asyncio.wait_for(
                    reader.xread(
                        {stream_key_bytes: cursor},
                        block=block_ms,
                        count=XREAD_COUNT,
                    ),
                    timeout=xread_wait_timeout_s(),
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
                await asyncio.sleep(
                    XREAD_EXHAUSTION_BACKOFF_S
                    if is_pool_exhaustion(exc)
                    else XREAD_ERROR_BACKOFF_S
                )
                continue

            if not result:
                # Fallback for a head that raced ahead of what is readable:
                # settle the boundary rather than hold the client's backlog
                # open until the producer catches up to its own head.
                if caught_up_pending:
                    caught_up_pending = False
                    yield caught_up_frame
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
                    # The boundary inside a batch: the first entry past the
                    # recorded head is live, so the marker goes ahead of it
                    # rather than after the whole batch.
                    if caught_up_pending and _entry_id_key(entry_id) > head_key:
                        caught_up_pending = False
                        yield caught_up_frame
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
    manager = LocalRunExecutor.get_instance()

    # Resolve run_id if caller didn't provide one. We may be reconnecting
    # to a turn that is still in the cache (latest) — pick the most recent.
    if run_id is None:
        async with manager.task_lock:
            info = manager._find_latest_for_thread(thread_id)
        if info is not None:
            run_id = info.run_id

    if run_id is None:
        # No in-process LocalRunExecution (restart, or the run lives on another
        # worker): the ledger names the thread's most recent run — its
        # retained stream is the replay source even when terminal.
        from src.server.database.runs import lifecycle as tl_db

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
        info = manager.executions.get((thread_id, run_id))
        if info is not None:
            return info.status.terminal
        from src.server.database.runs import lifecycle as tl_db

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
        close_event=RUN_END_EVENT_TYPE,
        caught_up_event=CAUGHT_UP_EVENT_TYPE,
    ):
        yield event
