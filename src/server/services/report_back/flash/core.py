"""Flash report-back composition points: watch stream + outbox executors.

``watch_wakes`` binds the status read-model to the wake wire-protocol; the
outbox executors (registered at startup, so the outbox service never imports
this package) validate applicability and delegate into ``executor``/
``reserve``/``pointer``. The legacy FIFO migration sweeps pre-outbox queues
once at registration.
"""

from __future__ import annotations

import logging

from src.server.services.report_back.flash import executor, pointer, reserve, status, wake
from src.server.services.report_back.flash.keys import (
    ptc_origin_key,
)

# Same hard-coded logger name request_prep uses — existing log routing keys off it.
logger = logging.getLogger("src.server.handlers.chat_handler")


# ---------------------------------------------------------------------------
# WAKE — wire-protocol in flash/wake.py; core binds the snapshot reader
# ---------------------------------------------------------------------------


async def watch_wakes(cache, flash_thread_id: str):
    """Watch report-back wakes with the status slice bound as the snapshot.

    The binding lives here, not in ``wake``: the wire-protocol module stays
    free of the read-model, and this composition point supplies it.
    """
    async for frame in wake.watch_wakes(
        cache, flash_thread_id, snapshot_reader=status.read_report_back_slice
    ):
        yield frame


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


async def _exec_flash_report_back(job: dict) -> None:
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
    if not isinstance(origin, dict) or not origin.get("flash_thread_id"):
        return  # ordinary run — nobody is watching
    await wake.publish_wake(
        cache, origin["flash_thread_id"], needs_input=ptc_thread_id
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
    flash_tid = origin.get("flash_thread_id")
    # Claimed under a stale ordering key (pre-deploy row that finalized
    # unstamped, keyed on its own PTC thread): requeue onto the real flash
    # chain instead of executing here. Off-chain, this teardown — and the
    # orphan resolver it can escalate to — runs CONCURRENTLY with the
    # chain's report_back admission; a resolver settling the pair between
    # the admission's atomic pointer claim and the route consummating it
    # schedules an orphan summary behind a settled pair. On the chain, the
    # open report_back lease serializes the clear past the whole admission
    # window. The fenced ack no-ops after the requeue.
    if flash_tid and job.get("ordering_key") != flash_tid:
        requeued = await outbox_db.requeue_job_with_key(
            str(job["hook_outbox_id"]),
            attempts=job["attempts"],
            ordering_key=flash_tid,
            max_attempts=MAX_ATTEMPTS,
        )
        logger.info(
            f"[HookOutbox] requeued watch_clear {job['hook_outbox_id']} for "
            f"{ptc_thread_id} onto flash chain {flash_tid} "
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
        result = await pointer.clear_flash_report_back(
            cache,
            ptc_thread_id,
            flash_tid,
            user_id=payload.get("user_id") or origin.get("user_id"),
            expected_gen=payload.get("dispatch_gen"),
            # Dead-report_back compensations set this: their source may have
            # died mid-admission with the pointer claimed, and the dead row
            # no longer holds the chain lease that serializes ordinary
            # teardowns past the admission window.
            refuse_if_pointer=bool(payload.get("refuse_if_pointer")),
        )
        if flash_tid and payload.get("error_wake"):
            # Wake watching clients so a cancelled/failed dispatch's card
            # reconciles instead of spinning until TTL. A fenced clear
            # suppresses the wake when the fencing generation legitimately
            # owns the pair (admitted, or a live run's lineage); a fence
            # held by a never-admitted reservation (lost-409 continuation)
            # must not swallow the predecessor's only failure signal — the
            # resolver decides atomically against the EXACT generation that
            # fenced this clear, resolves the pending state durably BEFORE
            # the wake (origin spared), and receipts the phantom so its
            # late admission is refused. A dropped nudge then degrades to a
            # status poll that reads the resolved state.
            if result.cleared:
                await wake.publish_wake(
                    cache, flash_tid, error="background_workflow_failed"
                )
            elif result.fencer_gen:
                resolved, _ = await reserve.resolve_orphaned_watch(
                    cache,
                    ptc_thread_id,
                    flash_tid,
                    payload.get("user_id") or origin.get("user_id"),
                    fencer_gen=result.fencer_gen,
                    job_gen=payload.get("dispatch_gen"),
                )
                if resolved:
                    await wake.publish_wake(
                        cache, flash_tid, error="background_workflow_failed"
                    )
        elif flash_tid and result.cleared:
            # Consumption clear (summary consumed, pair drained): push the
            # pending→idle transition so watchers drop the chip now. Without
            # this the error path is the only clear that wakes, and the
            # frontend's 60s status backstop becomes the de-facto clear
            # signal. A fenced (not cleared) consumption clear means a newer
            # incarnation owns the pair — still pending, no wake.
            await wake.publish_wake(cache, flash_tid, cleared=True)


async def _migrate_legacy_fifo_queues() -> None:
    """One-shot upgrade sweep folding pre-drainer report-back state into the outbox.

    Two legacy shapes can exist at upgrade time: (a) Redis FIFO queues
    (``flash_rb_queue:*``) holding completions the old in-process consumer
    never delivered — re-enqueued as outbox report_back rows keyed on their
    flash thread (queue keys deleted only after every entry lands); (b)
    outbox report_back / watch_clear rows enqueued before origin stamping,
    keyed on their own PTC thread — rekeyed onto the flash chain their
    execution actually serializes with. Idempotent throughout (stable
    idempotency keys, pending-only rekey), so a failed sweep simply reruns.
    """
    from src.server.database.runs import outbox as outbox_db
    from src.server.database.runs import lifecycle as tl_db
    from src.utils.cache.redis_cache import get_cache_client

    def _s(value) -> str:
        return value.decode() if isinstance(value, (bytes, bytearray)) else value

    cache = get_cache_client()
    if not cache.enabled:
        return  # Redis-less deployment: no legacy queues can exist
    client = cache.client
    if client is None:
        raise RuntimeError("Redis enabled but not connected — sweep retries")

    # Backdating puts migrated entries AHEAD of any younger pending outbox
    # row on the same chain (queue entries are completions that predate
    # every still-pending row), with intra-queue order preserved via a
    # per-index millisecond stagger.
    LEGACY_BACKDATE_SECONDS = 30 * 86400.0

    async for raw_key in client.scan_iter(match="flash_rb_queue:*"):
        queue_key = _s(raw_key)
        flash_tid = queue_key.split(":", 1)[1]
        entries = [_s(e) for e in await client.lrange(queue_key, 0, -1) or []]
        for i, ptc_tid in enumerate(entries):
            latest = await tl_db.get_latest_attempt(ptc_tid)
            if latest is None:
                # No run row to FK onto — nothing can ever render this turn.
                # Legacy-scoped clear ONLY (expected_gen=None): the current
                # origin's gen would authorize destroying whatever is there
                # NOW — including a rival worker's just-reserved generation
                # whose START hasn't committed (round-19 P1). A queue entry
                # proves only a legacy delivery was owed; a generated origin
                # belongs to a newer incarnation and is left intact (a
                # fenced-out legacy clear tombstones, so that reservation's
                # rollback completes it). And like every off-chain caller,
                # the teardown refuses while a run pointer is live — only
                # that admission's serialized lifecycle may drain the pair.
                origin = await cache.get_strict(ptc_origin_key(ptc_tid))
                await pointer.clear_flash_report_back(
                    cache,
                    ptc_tid,
                    flash_tid,
                    user_id=(origin or {}).get("user_id")
                    if isinstance(origin, dict)
                    else None,
                    expected_gen=None,
                    refuse_if_pointer=True,
                )
            else:
                await outbox_db.enqueue_compensation_job(
                    run_id=str(latest["conversation_response_id"]),
                    thread_id=ptc_tid,
                    hook_type="report_back",
                    payload={"ptc_thread_id": ptc_tid},
                    ordering_key=flash_tid,
                    idempotency_key=f"legacy_fifo:{flash_tid}:{ptc_tid}",
                    backdate_seconds=LEGACY_BACKDATE_SECONDS - i * 0.001,
                )
            # Remove only THIS entry (not the whole key): an entry appended
            # after the LRANGE snapshot survives for the next process
            # start's sweep instead of being deleted unread.
            await client.lrem(queue_key, 1, ptc_tid)
            await client.srem(f"flash_rb_queued:{flash_tid}", ptc_tid)
        if not await client.llen(queue_key):
            await client.delete(queue_key, f"flash_rb_queued:{flash_tid}")
        if entries:
            logger.info(
                f"[HookOutbox] migrated {len(entries)} legacy FIFO report-backs "
                f"for flash thread {flash_tid}"
            )

    for hook_type in ("report_back", "watch_clear"):
        for row in await outbox_db.list_pending_jobs(hook_type):
            ptc_tid = (row.get("payload") or {}).get("ptc_thread_id")
            if not ptc_tid:
                continue
            origin = await cache.get_strict(ptc_origin_key(ptc_tid))
            flash_tid = (
                origin.get("flash_thread_id") if isinstance(origin, dict) else None
            )
            if flash_tid and row.get("ordering_key") != flash_tid:
                if await outbox_db.set_job_ordering_key(
                    str(row["hook_outbox_id"]), flash_tid
                ):
                    logger.info(
                        f"[HookOutbox] rekeyed legacy {hook_type} job="
                        f"{row['hook_outbox_id']} onto flash chain {flash_tid}"
                    )


def register_outbox_executors() -> None:
    """Register this module's executors + legacy sweep with the outbox drainer.

    Called from app setup BEFORE the drainer starts — an unregistered type's
    jobs nack toward dead.
    """
    from src.server.services.hook_outbox import (
        register_hook_executor,
        register_startup_sweep,
    )

    register_hook_executor("report_back", _exec_flash_report_back)
    register_hook_executor("needs_input_wake", _exec_needs_input_wake)
    register_hook_executor("watch_clear", _exec_watch_clear)
    register_startup_sweep(_migrate_legacy_fifo_queues)
