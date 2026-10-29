"""Hand-off report-back composition points: watch stream + outbox executors.

``watch_wakes`` binds the status read-model to the wake wire-protocol; the
outbox executors (registered at startup, so the outbox service never imports
this package) validate applicability and delegate into ``executor``/
``reserve``/``pointer``.
"""

from __future__ import annotations

import logging

from src.server.services.report_back.handoff import executor, pointer, reserve, status, wake
from src.server.services.report_back.handoff.keys import (
    ORIGIN_THREAD_FIELD,
    ptc_origin_key,
)

# Same hard-coded logger name request_prep uses — existing log routing keys off it.
logger = logging.getLogger("src.server.handlers.chat_handler")


# ---------------------------------------------------------------------------
# WAKE — wire-protocol in handoff/wake.py; core binds the snapshot reader
# ---------------------------------------------------------------------------


def watch_wakes(cache, thread_id: str):
    """Watch report-back wakes with the status slice bound as the snapshot.

    The binding lives here, not in ``wake``: the wire-protocol module stays
    free of the read-model, and this composition point supplies it.

    Returns the generator rather than delegating to it with ``async for``:
    an outer wrapper swallows ``aclose()``, leaving the real generator's
    cleanup to the async-gen finalizer at some later tick.
    """
    return wake.watch_wakes(
        cache, thread_id, snapshot_reader=status.read_report_back_slice
    )


# ---------------------------------------------------------------------------
# Outbox executors — registered with the drainer at startup
# (``register_outbox_executors``, called from app setup before the drainer
# starts) so the outbox service never imports this module. Raising = nack
# (retry with backoff, dead at MAX_ATTEMPTS); returning = ack. Each
# validates its own applicability so a job enqueued for an ordinary run
# degrades to a no-op, never an error.
# ---------------------------------------------------------------------------


def _require_ptc_thread_id(job: dict) -> str:
    ptc_thread_id = (job.get("payload") or {}).get("ptc_thread_id")
    if not ptc_thread_id:
        raise ValueError(
            f"{job.get('hook_type')} job {job.get('hook_outbox_id')} missing "
            f"payload.ptc_thread_id"
        )
    return ptc_thread_id


async def _exec_handoff_report_back(job: dict) -> None:
    _require_ptc_thread_id(job)
    await executor.execute_report_back(job)


async def _exec_needs_input_wake(job: dict) -> None:
    from src.utils.cache.redis_cache import get_cache_client

    cache = get_cache_client()
    ptc_thread_id = _require_ptc_thread_id(job)
    # Strict read: raises on ANY unavailable state — transport blip, failed
    # startup connect (enabled flipped off at runtime), or config-off — so
    # the drainer nacks instead of acking a dropped wake. No config-off
    # carve-out: without Redis the chat preflight 503s every turn, so no
    # legitimate deployment produces these jobs Redis-less.
    origin = await cache.get_strict(ptc_origin_key(ptc_thread_id))
    if not isinstance(origin, dict) or not origin.get(ORIGIN_THREAD_FIELD):
        return  # ordinary run — nobody is watching
    await wake.publish_wake(
        cache, origin[ORIGIN_THREAD_FIELD], needs_input=ptc_thread_id
    )


async def _exec_watch_clear(job: dict) -> None:
    from src.server.database.runs import outbox as outbox_db
    from src.server.services.hook_outbox import MAX_ATTEMPTS
    from src.utils.cache.redis_cache import get_cache_client

    cache = get_cache_client()
    payload = job.get("payload") or {}
    ptc_thread_id = _require_ptc_thread_id(job)
    # Strict read: raises on ANY unavailable state (see _exec_needs_input_wake)
    # so the drainer nacks instead of acking a dropped clear.
    origin = await cache.get_strict(ptc_origin_key(ptc_thread_id))
    if not origin:
        return  # ordinary run, or already cleared — idempotent
    dispatcher_tid = origin.get(ORIGIN_THREAD_FIELD)
    # Claimed under a stale ordering key (pre-deploy row that finalized
    # unstamped, keyed on its own PTC thread): requeue onto the dispatching
    # thread's chain instead of executing here. Off-chain, this teardown — and the
    # orphan resolver it can escalate to — runs CONCURRENTLY with the
    # chain's report_back admission; a resolver settling the pair between
    # the admission's atomic pointer claim and the route consummating it
    # schedules an orphan summary behind a settled pair. On the chain, the
    # open report_back lease serializes the clear past the whole admission
    # window. The fenced ack no-ops after the requeue.
    if dispatcher_tid and job.get("ordering_key") != dispatcher_tid:
        requeued = await outbox_db.requeue_job_with_key(
            str(job["hook_outbox_id"]),
            attempts=job["attempts"],
            ordering_key=dispatcher_tid,
            max_attempts=MAX_ATTEMPTS,
        )
        logger.info(
            f"[HookOutbox] requeued watch_clear {job['hook_outbox_id']} for "
            f"{ptc_thread_id} onto the chain of {dispatcher_tid} "
            f"(stale key {job.get('ordering_key')!r}, status={requeued})"
        )
        return
    # Key-lock + row-lock fence across the Redis teardown, and a dispatch-
    # generation CAS inside the clear itself: a stale owner (or a clear
    # enqueued by an OLD incarnation's terminal) must not destroy state a
    # re-dispatched pair has since re-established.
    async with outbox_db.fenced_job_guard(
        str(job["hook_outbox_id"]), job["attempts"]
    ) as owned:
        if not owned:
            return  # reclaimed: the live owner performs the clear
        result = await pointer.clear_handoff_report_back(
            cache,
            ptc_thread_id,
            dispatcher_tid,
            user_id=payload.get("user_id") or origin.get("user_id"),
            expected_gen=payload.get("dispatch_gen"),
            # Dead-report_back compensations set this: their source may have
            # died mid-admission with the pointer claimed, and the dead row
            # no longer holds the chain lease that serializes ordinary
            # teardowns past the admission window.
            refuse_if_pointer=bool(payload.get("refuse_if_pointer")),
        )
        if not dispatcher_tid:
            return
        settled = result.cleared
        if not settled and result.fencer_gen:
            # Fenced by another generation. One that never got admitted (a
            # lost-409 continuation) has no run to drain the pair, so either
            # kind of clear would leave the watcher pending until the origin
            # expires. The resolver judges the EXACT fencing generation and
            # suppresses when it was admitted or a run is live on the thread;
            # otherwise it resolves the pending state durably BEFORE the wake
            # (origin spared) and receipts the phantom so its late admission
            # is refused. A dropped nudge then degrades to a status poll.
            settled, _ = await reserve.resolve_orphaned_watch(
                cache,
                ptc_thread_id,
                dispatcher_tid,
                payload.get("user_id") or origin.get("user_id"),
                fencer_gen=result.fencer_gen,
                job_gen=payload.get("dispatch_gen"),
            )
        if not settled:
            # Still owned, or nothing sound to resolve against: no wake.
            return
        if payload.get("error_wake"):
            # A cancelled or failed dispatch: wake watching clients so its
            # card reconciles instead of spinning until TTL.
            await wake.publish_wake(
                cache, dispatcher_tid, error="background_workflow_failed"
            )
        else:
            # Consumption clear (summary consumed, pair drained): push the
            # pending to idle transition so watchers re-read now. Without it
            # a watcher holds its last read until its watch recycles.
            await wake.publish_wake(cache, dispatcher_tid, cleared=True)


def register_outbox_executors() -> None:
    """Register this module's executors with the outbox drainer.

    Called from app setup BEFORE the drainer starts — an unregistered type's
    jobs nack toward dead.
    """
    from src.server.services.hook_outbox import register_hook_executor

    register_hook_executor("report_back", _exec_handoff_report_back)
    register_hook_executor("needs_input_wake", _exec_needs_input_wake)
    register_hook_executor("watch_clear", _exec_watch_clear)
