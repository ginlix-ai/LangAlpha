"""V2-native per-task SSE reader — serves GET /threads/{tid}/tasks/{task_id}
from the immutable per-run stream (STREAM_CONTRACT_V2.md), byte-identical to
the v1 wire shape.

The task's latest ledger run decides the stream key; the registry/meta polls
close the spawn race (GET arriving before the run row commits). Tasks with no
ledgered run — pre-ledger launches, or a resolution timeout — fall back to the
legacy v1 reader, which dies with the v1 lane in the follow-up PR.
"""

from __future__ import annotations

import asyncio
import json
import logging
from typing import AsyncGenerator, Optional

from ptc_agent.agent.middleware.background_subagent.redis_stream import (
    read_task_meta,
    run_stream_key,
)
from src.server.services.background_registry_store import BackgroundRegistryStore
from src.server.handlers.chat.run_stream_reader import _XREAD_COUNT, _xread_block_ms
from src.utils.cache.redis_cache import get_cache_client

# Same hard-coded logger name request_prep uses — existing log routing keys off it.
logger = logging.getLogger("src.server.handlers.chat_handler")

# Same spawn-race window the v1 reader used: how long a GET waits for the task
# to become resolvable anywhere before the legacy fallback serves it.
_STARTUP_TIMEOUT = 30.0
_STARTUP_POLL = 0.5


async def _resolve_task_run_id(thread_id: str, task_id: str) -> Optional[str]:
    """Latest ``task_run_id`` for the task, or None → legacy v1 semantics.

    Resolution order per poll round: ledger task row (authoritative), local
    registry entry (same-process spawn race, row not yet committed), Redis
    meta (peer-worker spawn race). A source that answers *without* a run id
    is a live pre-ledger writer — legacy immediately, don't burn the window.
    """
    from src.server.database.runs import subagent_runs as sr_db

    registry_store = BackgroundRegistryStore.get_instance()
    waited = 0.0
    while True:
        try:
            task_row = await sr_db.get_task(thread_id, task_id)
        except Exception:
            task_row = None
        if task_row and task_row.get("latest_run_id"):
            return str(task_row["latest_run_id"])

        local = None
        try:
            registry = await registry_store.get_registry(thread_id)
            if registry is not None:
                local = await registry.get_task_by_task_id(task_id)
        except Exception:
            local = None
        run_id = getattr(local, "task_run_id", None)
        if run_id:
            return str(run_id)

        meta = await read_task_meta(thread_id, task_id)
        if meta is not None:
            meta_run = meta.get("task_run_id") or ""
            if meta_run:
                return meta_run
            return None
        if local is not None:
            return None

        if waited >= _STARTUP_TIMEOUT:
            return None
        await asyncio.sleep(_STARTUP_POLL)
        waited += _STARTUP_POLL


def _record_to_v1_sse(record: dict, thread_id: str, task_id: str) -> str:
    """Render a v2 content payload as the v1 wire string, byte-identical to
    the pre-rendered form ``spill_task_record`` wrote to the v1 leg."""
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
        f"data: {json.dumps(data, ensure_ascii=False, default=str)}\n\n"
    )


async def stream_task_run_sse(
    thread_id: str,
    task_id: str,
    last_event_id: Optional[int] = None,
) -> AsyncGenerator[str, None]:
    """SSE consumer for a single subagent, served from its latest run stream.

    v1-compatible on the wire: content frames are ``id: <seq>`` + event +
    data (with thread_id/agent injected), the cursor is the record seq
    (``?last_event_id=N`` resumes after N), ``:keepalive`` comments flow on
    every empty XREAD round, and a trimmed head past the cursor yields the
    explicit ``stream_gap`` frame. Control frames (lane_open, steering) are
    v2-only and never rendered; ``run_end`` closes the stream the way the
    swallowed v1 sentinel did. Sentinel-less closes (crashed writer) come
    from the ledger row via the two-empty-round handshake.
    """
    run_id = await _resolve_task_run_id(thread_id, task_id)
    if run_id is None:
        from src.server.handlers.chat.legacy_task_sse_reader import (
            stream_subagent_from_log,
        )

        async for frame in stream_subagent_from_log(
            thread_id, task_id, last_event_id
        ):
            yield frame
        return

    cache = get_cache_client()
    if not cache.enabled or not cache.client:
        logger.warning(
            "[task_run_sse] Redis disabled — no events to stream for %s/%s",
            thread_id,
            task_id,
        )
        return

    stream_key = run_stream_key(thread_id, run_id).encode("utf-8")
    block_ms = _xread_block_ms()
    # v2 entries carry auto XADD ids; the v1 seq cursor filters on the
    # payload's seq instead of mapping onto an explicit entry id.
    cursor: bytes = b"0"
    resume_after = last_event_id if (last_event_id or 0) > 0 else None
    gap_checked = resume_after is None

    async def _settled() -> bool:
        """Terminal ledger row (or a deleted one) ends a sentinel-less stream."""
        try:
            from src.server.database.runs import subagent_runs as sr_db

            row = await sr_db.get_task_run(run_id)
        except Exception:
            return False
        return row is None or str(row.get("status") or "") != "in_progress"

    terminal_seen = False
    while True:
        try:
            result = await asyncio.wait_for(
                cache.client.xread(
                    {stream_key: cursor},
                    block=block_ms,
                    count=_XREAD_COUNT,
                ),
                timeout=(block_ms / 1000.0) + 2.0,
            )
        except asyncio.TimeoutError:
            yield ":keepalive\n\n"
            if await _settled():
                if terminal_seen:
                    return
                terminal_seen = True
            continue
        except (asyncio.CancelledError, GeneratorExit):
            raise
        except Exception as exc:
            logger.warning(
                "[task_run_sse] XREAD failed on %s: %s", stream_key, exc
            )
            if await _settled():
                return
            await asyncio.sleep(0.5)
            continue

        if not result:
            yield ":keepalive\n\n"
            if await _settled():
                if terminal_seen:
                    return
                terminal_seen = True
            continue

        for _stream_name, entries in result:
            for entry_id, fields in entries:
                cursor = entry_id
                ftype = fields.get(b"type")
                if isinstance(ftype, bytes):
                    ftype = ftype.decode("utf-8", errors="replace")
                if ftype == "run_end":
                    # Positive close from the writer — nothing follows it by
                    # contract. v1 clients see the stream end, exactly like
                    # the swallowed v1 sentinel.
                    return
                payload = fields.get(b"payload")
                if isinstance(payload, bytes):
                    try:
                        payload = payload.decode("utf-8")
                    except UnicodeDecodeError:
                        continue
                if not payload:
                    continue
                try:
                    record = json.loads(payload)
                except json.JSONDecodeError:
                    continue
                if not isinstance(record, dict) or "seq" not in record:
                    # lane_open / steering / other control frames — v2-only.
                    continue
                seq = int(record.get("seq") or 0)
                if resume_after is not None and seq <= resume_after:
                    continue
                if not gap_checked:
                    gap_checked = True
                    if seq > resume_after + 1:
                        gap = json.dumps(
                            {
                                "expected_from": resume_after + 1,
                                "first_available": seq,
                            },
                            ensure_ascii=False,
                        )
                        yield f"event: stream_gap\ndata: {gap}\n\n"
                yield _record_to_v1_sse(record, thread_id, task_id)
