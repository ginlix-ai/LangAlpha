"""Per-turn replay projection cache.

Wire-ready replay items for one settled turn, keyed by the turn's own last
checkpoint id (its *tail*) **and** a fingerprint of the turn's table-sourced
replay inputs. The tail exists at persist time and survives forks of later
turns (edits/regenerates fork new tails → new keys; orphaned entries expire
by TTL), but it alone does not identify a complete projection: a settled
turn's stored events, provenance rows and usage row keep being rewritten
after finalize (the subagent collector's archive drain, atomic
``context_window`` appends) without ever moving the tail. Folding those
inputs into the key makes a superseded entry *unreachable* rather than
merely stale, so no invalidation call has to succeed for correctness — and
a reader that built from pre-drain state can only ever write its own
generation's key, never poison the current one.

Entries are therefore immutable, except the interrupted-turn refresh: a
turn's ``ending_interrupts`` ride the resume boundary created later, so the
post-persist refresh rebuilds a two-turn window to overwrite the previous
turn's entry under its unchanged key.

Entries are canonicalized to the wire's JSON form (``default=str``) before
storing so a cache hit is byte-identical to a fresh projection. Purely an
optimization: any miss falls back to a full checkpoint rebuild + backfill.
"""

import asyncio
import hashlib
import json
import logging
from typing import Any

from src.config.settings import get_replay_projection_cache_ttl
from src.utils.cache.redis_cache import get_cache_client

logger = logging.getLogger(__name__)

# v3: fingerprinted keys (v1/v2 entries are tail-only, so a v2 hit could
# serve a projection built before the turn's archive drain landed).
_KEY_PREFIX = "replay:turn:v3"
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


def _key(thread_id: str, tail_checkpoint_id: str, fingerprint: str) -> str:
    return f"{_KEY_PREFIX}:{thread_id}:{tail_checkpoint_id}:{fingerprint}"


def turn_fingerprint(
    response: dict[str, Any] | None,
    provenance_rows: list[dict[str, Any]] | None,
    usage_row: dict[str, Any] | None,
) -> str:
    """Digest of the post-finalize-mutable replay inputs the tail can't identify.

    Hashes each source whole rather than a chosen subset of fields: an
    over-inclusive digest only costs an extra rebuild when something
    irrelevant changes, while an under-inclusive one silently resurrects the
    bug this key exists to prevent. The response row is the one source read
    through a proxy — see ``_response_generation``.
    """
    digest = hashlib.blake2b(digest_size=16)
    for source in (_response_generation(response), provenance_rows, usage_row):
        digest.update(
            json.dumps(source, default=str, sort_keys=True).encode("utf-8")
        )
        digest.update(b"\x1e")
    return digest.hexdigest()


def _response_generation(
    response: dict[str, Any] | None,
) -> dict[str, Any] | None:
    """The response row reduced to its identity plus its Postgres row version.

    Every fingerprinted field of a settled turn lives on that one row, so any
    rewrite — the archive drain replacing ``sse_events``, an atomic
    ``context_window`` append — lands a new tuple with a new ``xmin``. Reading
    the version is strictly more inclusive than hashing the columns and costs
    a tuple-header read, where serializing the megabyte-scale transcript here
    ran per turn, per replay request, on the event loop. Rows fetched without
    ``xmin`` keep the whole-row hash.
    """
    if not response or "xmin" not in response:
        return response
    return {
        "conversation_response_id": response.get("conversation_response_id"),
        "xmin": response["xmin"],
    }


def cache_active() -> bool:
    client = get_cache_client()
    return bool(
        client.enabled and client.client and get_replay_projection_cache_ttl() > 0
    )


async def get_cached_turns(
    thread_id: str, turn_keys: list[tuple[str, str]]
) -> dict[str, list[dict[str, Any]] | None]:
    """Fetch entries for ``(tail, fingerprint)`` pairs; None per tail on miss.

    Keyed by fingerprint, indexed back by tail — callers hold a tail per turn
    and a fingerprint is only ever derived from it.
    """
    keys = [_key(thread_id, tail, fp) for tail, fp in turn_keys]
    values = await get_cache_client().mget(keys)
    return dict(zip([tail for tail, _ in turn_keys], values))


async def store_turn(
    thread_id: str,
    tail_checkpoint_id: str | None,
    fingerprint: str,
    items: list[dict[str, Any]],
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
            _key(thread_id, tail_checkpoint_id, fingerprint),
            json.loads(serialized),
            ttl=get_replay_projection_cache_ttl(),
        )
    except Exception as e:
        logger.warning(f"[ProjectionCache] store failed for {thread_id}: {e}")


async def delete_turns(thread_id: str, turn_keys: list[tuple[str, str]]) -> None:
    """Best-effort eviction — keeps salvage-bearing turns out of the cache so
    the fast path can't replay them without their trailing salvage."""
    if not turn_keys or not cache_active():
        return
    try:
        client = get_cache_client()
        for tail_checkpoint_id, fingerprint in turn_keys:
            await client.delete(_key(thread_id, tail_checkpoint_id, fingerprint))
    except Exception as e:
        logger.warning(f"[ProjectionCache] delete failed for {thread_id}: {e}")


async def live_task_streams(thread_id: str, task_ids: set[str]) -> set[str]:
    """The subset of tasks whose per-task Redis stream is still unfinalized.

    A turn whose projected task state is still growing must not be cached:
    task-ns writes never move the main-branch tail, so a frozen partial
    would never self-heal. Only an explicit end sentinel proves terminal
    here. Missing streams and verification failures count as live
    (conservative — skip caching rather than freeze a partial).

    Probe BEFORE reading task-namespace state, and gate caching on this
    pre-read verdict: sealing is monotonic, so "sealed before the read"
    proves the read saw final state, while a post-read probe can pass for a
    stream that sealed after the read returned pre-terminal data — freezing
    it for the cache TTL.
    """
    if not task_ids:
        return set()
    client = get_cache_client().client
    if client is None:
        return set(task_ids)

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

    ordered = list(task_ids)
    liveness = await asyncio.gather(*(_task_live(task_id) for task_id in ordered))
    return {task_id for task_id, live in zip(ordered, liveness) if live}


def _is_stream_end_sentinel(raw: Any) -> bool:
    """Match forwarder.finalize()'s ``{"event": "subagent_stream_end"}``
    sentinel (a JSON dict without ``seq`` — same test as the reconnect
    consumer's payload classifier)."""
    from ptc_agent.agent.middleware.background_subagent.redis_stream import (
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
