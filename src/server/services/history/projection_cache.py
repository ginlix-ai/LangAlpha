"""Per-turn replay projection cache.

Wire-ready replay items for one settled turn, keyed by the turn's own last
checkpoint id (its *tail*). The tail exists at persist time and survives
forks of later turns (edits/regenerates fork new tails → new keys; orphaned
entries expire by TTL), so entries are immutable — except the interrupted-turn
refresh: a turn's ``ending_interrupts`` ride the resume boundary created
later, so the post-persist refresh rebuilds a two-turn window to overwrite
the previous turn's entry under its unchanged key.

Entries are canonicalized to the wire's JSON form (``default=str``) before
storing so a cache hit is byte-identical to a fresh projection. Purely an
optimization: any miss falls back to a full checkpoint rebuild + backfill.
"""

import asyncio
import json
import logging
from typing import Any

from src.config.settings import get_replay_projection_cache_ttl
from src.utils.cache.redis_cache import get_cache_client

logger = logging.getLogger(__name__)

# v2: per-run task-namespace projection + task-lane user_message events —
# v1 entries carry the whole-namespace-at-spawn-turn shape and must not mix.
_KEY_PREFIX = "replay:turn:v2"
# Entries beyond this are pathological (widget-heavy legacy stored events);
# skip caching rather than bloat Redis — replay just rebuilds those threads.
_MAX_ENTRY_BYTES = 512 * 1024

# One strong-referenced runner per thread (create_task results are otherwise
# GC-eligible). A trigger that arrives while its runner is active marks the
# thread dirty; the same runner performs one coalesced follow-up pass after the
# current pass finishes. This preserves refresh generation order, so an older
# snapshot can never finish after and overwrite a newer one.
_refresh_tasks: dict[str, asyncio.Task[None]] = {}
_refresh_dirty: set[str] = set()


def _key(thread_id: str, tail_checkpoint_id: str) -> str:
    return f"{_KEY_PREFIX}:{thread_id}:{tail_checkpoint_id}"


def cache_active() -> bool:
    client = get_cache_client()
    return bool(
        client.enabled and client.client and get_replay_projection_cache_ttl() > 0
    )


async def get_cached_turns(
    thread_id: str, tail_checkpoint_ids: list[str]
) -> dict[str, list[dict[str, Any]] | None]:
    """Fetch entries for the given tails; None per tail on miss."""
    keys = [_key(thread_id, t) for t in tail_checkpoint_ids]
    values = await get_cache_client().mget(keys)
    return dict(zip(tail_checkpoint_ids, values))


async def store_turn(
    thread_id: str, tail_checkpoint_id: str | None, items: list[dict[str, Any]]
) -> None:
    """Best-effort write of one turn's wire-ready items."""
    if not tail_checkpoint_id or not cache_active():
        return
    try:
        # Canonicalize exactly as the SSE endpoint serializes (default=str),
        # so cached datetimes replay with the same wire text as fresh ones.
        serialized = json.dumps(items, ensure_ascii=False, default=str)
        if len(serialized.encode("utf-8")) > _MAX_ENTRY_BYTES:
            logger.debug(
                f"[ProjectionCache] entry too large for {thread_id} "
                f"tail={tail_checkpoint_id}, skipping"
            )
            return
        await get_cache_client().set(
            _key(thread_id, tail_checkpoint_id),
            json.loads(serialized),
            ttl=get_replay_projection_cache_ttl(),
        )
    except Exception as e:
        logger.warning(f"[ProjectionCache] store failed for {thread_id}: {e}")


async def delete_turns(thread_id: str, tail_checkpoint_ids: list[str]) -> None:
    """Best-effort eviction — keeps salvage-bearing turns out of the cache so
    the fast path can't replay them without their trailing salvage."""
    if not tail_checkpoint_ids or not cache_active():
        return
    try:
        client = get_cache_client()
        for tail_checkpoint_id in tail_checkpoint_ids:
            await client.delete(_key(thread_id, tail_checkpoint_id))
    except Exception as e:
        logger.warning(f"[ProjectionCache] delete failed for {thread_id}: {e}")


async def task_streams_live(thread_id: str, task_ids: set[str]) -> bool:
    """True when any task's per-task Redis stream is still unfinalized.

    A turn whose projected subagent transcript is still growing must not be
    cached: task-ns writes never move the main-branch tail, so a frozen
    partial transcript would never self-heal. Only an explicit end sentinel
    proves terminal here. Missing streams and verification failures count as
    live (conservative — skip caching rather than freeze a partial).
    """
    if not task_ids:
        return False
    client = get_cache_client().client
    if client is None:
        return True

    async def _task_live(task_id: str) -> bool:
        try:
            entries = await client.xrevrange(
                f"subagent:stream:{thread_id}:{task_id}", count=1
            )
        except Exception as e:
            logger.warning(
                f"[ProjectionCache] task stream check failed for "
                f"{thread_id}/{task_id}: {e}"
            )
            return True  # conservative: skip caching rather than freeze a partial
        if not entries:
            return True
        raw = entries[0][1].get(b"event") or entries[0][1].get("event")
        if isinstance(raw, bytes):
            raw = raw.decode("utf-8", errors="replace")
        return not _is_stream_end_sentinel(raw)

    liveness = await asyncio.gather(*(_task_live(task_id) for task_id in task_ids))
    return any(liveness)


def _is_stream_end_sentinel(raw: Any) -> bool:
    """Match forwarder.finalize()'s ``{"event": "subagent_stream_end"}``
    sentinel (a JSON dict without ``seq`` — same test as the reconnect
    consumer's payload classifier)."""
    from ptc_agent.agent.middleware.background_subagent.registry import (
        SUBAGENT_STREAM_END_EVENT,
    )

    if not isinstance(raw, str) or not raw.startswith("{"):
        return False
    try:
        record = json.loads(raw)
    except json.JSONDecodeError:
        return False
    return (
        isinstance(record, dict)
        and record.get("event") == SUBAGENT_STREAM_END_EVENT
        and "seq" not in record
    )


def _refresh_done(thread_id: str, task: asyncio.Task[None]) -> None:
    """Drop a completed runner without clobbering a newer replacement task."""
    if _refresh_tasks.get(thread_id) is task:
        _refresh_tasks.pop(thread_id, None)
        _refresh_dirty.discard(thread_id)


async def _run_projection_refreshes(thread_id: str) -> None:
    """Run refreshes serially, coalescing overlapping triggers per thread."""
    while True:
        # Triggers arriving after this point mark the pass dirty and force one
        # follow-up pass. Multiple triggers during the same pass coalesce.
        _refresh_dirty.discard(thread_id)
        await refresh_thread_projection(thread_id)
        if thread_id not in _refresh_dirty:
            return


def schedule_projection_refresh(thread_id: str) -> None:
    """Fire-and-forget post-persist warm-up of the thread's last two turns.

    At most one runner executes per thread. A trigger overlapping an active
    pass requests a coalesced follow-up pass, preserving snapshot write order.
    """
    if not cache_active():
        return
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:  # no running loop (sync test contexts)
        return

    current = _refresh_tasks.get(thread_id)
    if current is not None and not current.done():
        _refresh_dirty.add(thread_id)
        return

    # A completed task's callback may not have run yet. Install the replacement
    # first; _refresh_done's identity guard prevents the old callback from
    # removing it.
    _refresh_dirty.discard(thread_id)
    task = loop.create_task(_run_projection_refreshes(thread_id))
    _refresh_tasks[thread_id] = task
    task.add_done_callback(
        lambda completed, tid=thread_id: _refresh_done(tid, completed)
    )


async def refresh_thread_projection(thread_id: str) -> None:
    """Rebuild the last two turns so their entries land while checkpoints are
    hot — two turns because the previous turn's ``ending_interrupts`` only
    become known at this turn's (resume) boundary."""
    from src.server.database.conversation import get_replay_thread_data
    from src.server.services.history.replay import (
        CheckpointReplayUnavailable,
        build_checkpoint_replay_items,
    )

    try:
        _owner, thread, queries, responses, usages, provenance = (
            await get_replay_thread_data(thread_id)
        )
        branch_tip = (thread or {}).get("latest_checkpoint_id")
        if not branch_tip:
            return
        responses_by_turn = {
            r.get("turn_index"): r for r in responses if isinstance(r, dict)
        }
        # Backfills the cache as a side effect of the rebuild.
        await build_checkpoint_replay_items(
            thread_id,
            queries,
            responses_by_turn,
            branch_tip_checkpoint_id=branch_tip,
            last_n_turns=2,
            usages=usages,
            provenance=provenance,
        )
    except CheckpointReplayUnavailable as e:
        logger.debug(f"[ProjectionCache] refresh skipped for {thread_id}: {e}")
    except Exception as e:
        logger.warning(f"[ProjectionCache] refresh failed for {thread_id}: {e}")
