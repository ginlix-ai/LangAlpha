"""Uniform terminal hooks via the durable outbox (v4 1.7, invariant I5).

Every required post-commit effect of a run's terminal transition is a
unique idempotent ``hook_outbox`` row written INSIDE the finalize
transaction, and executed afterwards by the ``HookOutboxDrainer``. The
decision table (``build_finalize_jobs``) lives in ``database.hook_outbox``;
``finalize_run`` applies it as the DEFAULT from the row's START-stamped
metadata — no finalize path can skip required effects. The job protocol is
multi-worker safe: committed claims, lease-expiry reclaim, stable
idempotency keys, per-ordering-key FIFO, effect-before-ack retry safety —
competing drainers (one per worker) share the same table.

Effects validate their own applicability at execution time (an ordinary
run's report_back / watch_clear no-ops on a confirmed-absent origin), and
RAISE on transport failure so the drainer's nack/backoff — never a
swallowed read — is the retry path.
"""

import asyncio
import logging
from typing import Any, Awaitable, Callable, Dict, Optional

from src.server.database import hook_outbox as outbox_db

logger = logging.getLogger(__name__)

CLAIM_BATCH = 20
LEASE_SECONDS = 60
POLL_INTERVAL = 5.0
MAX_ATTEMPTS = 5
# Bound on concurrently executing jobs per drainer. Same-ordering-key jobs are
# never co-claimable (the claim query hides a job while an earlier open job
# shares its key), so concurrency here never reorders a chain.
MAX_CONCURRENT_JOBS = 10
# task_report_back jobs hold their slot through an entire PTC notification
# turn (sandbox bringup + a full agent run — tens of minutes); cap their
# share so a burst of tail completions can't starve flash report-backs and
# cheap wakes. Excluded types stay unclaimed but still head their chains.
TASK_RB_INFLIGHT_QUOTA = 3


# ---------------------------------------------------------------------------
# Executors — one per hook_type, taking the full job row (report_back needs
# its id for lease heartbeats + payload merges). Raising = nack (retry with
# backoff, dead at MAX_ATTEMPTS); returning = ack. Each validates its own
# applicability so a job enqueued for an ordinary run degrades to a no-op,
# never an error.
# ---------------------------------------------------------------------------


async def _exec_burst_release(job: Dict[str, Any]) -> None:
    from src.server.dependencies.usage_limits import release_burst_slot

    payload = job.get("payload") or {}
    user_id = payload.get("user_id")
    if user_id:
        await release_burst_slot(user_id, payload.get("slot_id"), strict=True)


async def _exec_report_back(job: Dict[str, Any]) -> None:
    from src.server.handlers.chat.report_back import execute_report_back

    await execute_report_back(job)


async def _exec_needs_input_wake(job: Dict[str, Any]) -> None:
    from src.server.handlers.chat.report_back import publish_wake
    from src.server.handlers.chat.report_back_keys import ptc_origin_key
    from src.utils.cache.redis_cache import get_cache_client

    cache = get_cache_client()
    ptc_thread_id = (job.get("payload") or {})["ptc_thread_id"]
    # Strict read: raises on ANY unavailable state — transport blip, failed
    # startup connect (enabled flipped off at runtime), or config-off — so
    # the drainer nacks instead of acking a dropped wake. No config-off
    # carve-out: without Redis the chat preflight 503s every turn, so no
    # legitimate deployment produces these jobs Redis-less.
    origin = await cache.get_strict(ptc_origin_key(ptc_thread_id))
    if not isinstance(origin, dict) or not origin.get("flash_thread_id"):
        return  # ordinary run — nobody is watching
    await publish_wake(
        cache, origin["flash_thread_id"], needs_input=ptc_thread_id
    )


async def _exec_watch_clear(job: Dict[str, Any]) -> None:
    from src.server.handlers.chat.report_back import (
        clear_flash_report_back,
        publish_wake,
        resolve_orphaned_watch,
    )
    from src.server.handlers.chat.report_back_keys import ptc_origin_key
    from src.utils.cache.redis_cache import get_cache_client

    cache = get_cache_client()
    payload = job.get("payload") or {}
    ptc_thread_id = payload["ptc_thread_id"]
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
        result = await clear_flash_report_back(
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
                await publish_wake(
                    cache, flash_tid, error="background_workflow_failed"
                )
            elif result.fencer_gen:
                resolved, _ = await resolve_orphaned_watch(
                    cache,
                    ptc_thread_id,
                    flash_tid,
                    payload.get("user_id") or origin.get("user_id"),
                    fencer_gen=result.fencer_gen,
                    job_gen=payload.get("dispatch_gen"),
                )
                if resolved:
                    await publish_wake(
                        cache, flash_tid, error="background_workflow_failed"
                    )
        elif flash_tid and result.cleared:
            # Consumption clear (summary consumed, pair drained): push the
            # pending→idle transition so watchers drop the chip now. Without
            # this the error path is the only clear that wakes, and the
            # frontend's 60s status backstop becomes the de-facto clear
            # signal. A fenced (not cleared) consumption clear means a newer
            # incarnation owns the pair — still pending, no wake.
            await publish_wake(cache, flash_tid, cleared=True)


async def _exec_task_report_back(job: Dict[str, Any]) -> None:
    from src.server.handlers.chat.task_report_back import execute_task_report_back

    await execute_task_report_back(job)


_EXECUTORS: Dict[str, Callable[[Dict[str, Any]], Awaitable[None]]] = {
    "burst_release": _exec_burst_release,
    "report_back": _exec_report_back,
    "needs_input_wake": _exec_needs_input_wake,
    "watch_clear": _exec_watch_clear,
    "task_report_back": _exec_task_report_back,
}


# Dead-letter compensation (a dead report_back's watch_clear) is inserted
# ATOMICALLY with the dead transition inside nack_outbox_job /
# park_exhausted_jobs — a separate post-commit insert could be lost to a
# crash and the dead row is never swept again.


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
    from src.server.database import turn_lifecycle as tl_db
    from src.server.handlers.chat.report_back import clear_flash_report_back
    from src.server.handlers.chat.report_back_keys import ptc_origin_key
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
                await clear_flash_report_back(
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


# ---------------------------------------------------------------------------
# Drainer
# ---------------------------------------------------------------------------


class HookOutboxDrainer:
    """Per-process drainer: claim → execute concurrently → ack/nack, forever.

    Effects run OUTSIDE the claim row lock (the claim transaction commits
    the lease first), so a crash mid-effect leaves a claimed row whose
    lease expiry re-offers it — the reclaim path doubles as startup
    recovery, hence executors must be idempotent. Claimed jobs execute as
    concurrent tasks (bounded by ``MAX_CONCURRENT_JOBS``) so a long-held
    job — a report_back awaiting its summary run's terminal — never stalls
    unrelated chains; same-key ordering is enforced by the claim query,
    not by execution order here.
    """

    _instance: Optional["HookOutboxDrainer"] = None

    @classmethod
    def get_instance(cls) -> "HookOutboxDrainer":
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance

    def __init__(self) -> None:
        self._task: Optional[asyncio.Task] = None
        self._nudge = asyncio.Event()
        # task -> hook_type, so per-type quotas can count in-flight work.
        self._inflight: Dict[asyncio.Task, str] = {}

    def start(self) -> None:
        if self._task is None or self._task.done():
            self._task = asyncio.create_task(self._loop(), name="hook-outbox-drainer")

    async def stop(self) -> None:
        if self._task is not None:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            except Exception:
                logger.warning(
                    "[HookOutbox] drainer raised during stop", exc_info=True
                )
            self._task = None
        # Cancel-safety: an interrupted effect's claimed row is re-offered at
        # lease expiry and every executor is idempotent/resumable.
        inflight = [t for t in self._inflight if not t.done()]
        for task in inflight:
            task.cancel()
        if inflight:
            await asyncio.gather(*inflight, return_exceptions=True)
        self._inflight.clear()

    def nudge(self) -> None:
        """Post-commit hint that new jobs exist; the 5s poll is the backstop."""
        self._nudge.set()

    async def _loop(self) -> None:
        legacy_swept = False
        while True:
            if not legacy_swept:
                try:
                    await _migrate_legacy_fifo_queues()
                    legacy_swept = True
                except asyncio.CancelledError:
                    raise
                except Exception:
                    # The sweep is a BARRIER: claiming before unmigrated rows
                    # are rekeyed would run them outside their real chain, and
                    # a claimed row can no longer be rekeyed. Idempotent, so
                    # back off and retry until it lands.
                    logger.error(
                        "[HookOutbox] legacy FIFO migration failed; deferring "
                        "claims until it succeeds",
                        exc_info=True,
                    )
                    await self._wait_for_nudge()
                    continue

            # Dead-park expired claims that already burned max_attempts leases
            # (crashed owners never nack); their chains unwedge here. The
            # watch_clear compensation rides the same statement.
            try:
                for parked in await outbox_db.park_exhausted_jobs(
                    max_attempts=MAX_ATTEMPTS
                ):
                    logger.error(
                        f"[HookOutbox] parked exhausted job="
                        f"{parked.get('hook_outbox_id')} "
                        f"type={parked.get('hook_type')} as dead"
                    )
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.error("[HookOutbox] park sweep failed", exc_info=True)

            # Dead terminal-cleanup jobs get a durable successor: a dead
            # report_back is compensated with a watch_clear, but a dead
            # watch_clear otherwise had none — its pair state (memberships,
            # cap slot, error wake) would sit until the 24h origin TTL.
            try:
                for revived in await outbox_db.revive_dead_cleanup_jobs():
                    logger.warning(
                        f"[HookOutbox] revived dead cleanup job="
                        f"{revived.get('hook_outbox_id')} "
                        f"key={revived.get('ordering_key')}"
                    )
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.error("[HookOutbox] revive sweep failed", exc_info=True)

            # Claim only what free slots can run: an over-claimed job would sit
            # here burning its lease while it waits for a task slot.
            free = MAX_CONCURRENT_JOBS - len(self._inflight)
            jobs = []
            if free > 0:
                task_rb_inflight = sum(
                    1 for ht in self._inflight.values() if ht == "task_report_back"
                )
                excluded = (
                    ["task_report_back"]
                    if task_rb_inflight >= TASK_RB_INFLIGHT_QUOTA
                    else None
                )
                try:
                    jobs = await outbox_db.claim_outbox_jobs(
                        limit=min(CLAIM_BATCH, free),
                        lease_seconds=LEASE_SECONDS,
                        max_attempts=MAX_ATTEMPTS,
                        excluded_hook_types=excluded,
                    )
                except asyncio.CancelledError:
                    raise
                except Exception:
                    logger.error("[HookOutbox] claim query failed", exc_info=True)

                for job in jobs:
                    task = asyncio.create_task(
                        self._execute(job),
                        name=f"hook-outbox-{job.get('hook_type')}-{job.get('hook_outbox_id')}",
                    )
                    self._inflight[task] = job.get("hook_type") or ""
                    task.add_done_callback(
                        lambda t: self._inflight.pop(t, None)
                    )

                if len(jobs) >= free:
                    continue  # claimed to capacity; more are likely due right now

            await self._wait_for_nudge()

    async def _wait_for_nudge(self) -> None:
        try:
            await asyncio.wait_for(self._nudge.wait(), timeout=POLL_INTERVAL)
        except asyncio.TimeoutError:
            pass
        self._nudge.clear()

    async def _execute(self, job: Dict[str, Any]) -> None:
        job_id = str(job["hook_outbox_id"])
        hook_type = job["hook_type"]
        attempts = job["attempts"]  # fence token for this lease generation
        executor = _EXECUTORS.get(hook_type)
        if executor is None:
            logger.error(
                f"[HookOutbox] unknown hook_type={hook_type} job={job_id}; "
                f"nacking toward dead"
            )
            try:
                await outbox_db.nack_outbox_job(
                    job_id, attempts=attempts, max_attempts=MAX_ATTEMPTS
                )
            except Exception:
                logger.error(f"[HookOutbox] nack failed for {job_id}", exc_info=True)
            return
        try:
            await executor(job)
        except asyncio.CancelledError:
            raise  # shutdown: lease expiry re-offers the claimed row
        except Exception:
            logger.warning(
                f"[HookOutbox] {hook_type} failed for job={job_id} "
                f"run={job.get('run_id')} (attempt {attempts})",
                exc_info=True,
            )
            try:
                new_status = await outbox_db.nack_outbox_job(
                    job_id, attempts=attempts, max_attempts=MAX_ATTEMPTS
                )
                if new_status == "dead":
                    # A dead report_back's watch_clear compensation was
                    # inserted atomically by the nack statement itself.
                    logger.error(
                        f"[HookOutbox] job={job_id} type={hook_type} dead "
                        f"after {attempts} attempts"
                    )
            except Exception:
                logger.error(f"[HookOutbox] nack failed for {job_id}", exc_info=True)
            return
        try:
            await outbox_db.ack_outbox_job(job_id, attempts=attempts)
        except Exception:
            # The effect ran; a lost ack re-runs it after lease expiry —
            # acceptable because every executor is idempotent.
            logger.warning(f"[HookOutbox] ack failed for {job_id}", exc_info=True)
