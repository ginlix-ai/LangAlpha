"""Uniform terminal hooks via the durable outbox (v4 1.7, invariant I5).

Every required post-commit effect of a run's terminal transition is a
unique idempotent ``hook_outbox`` row written INSIDE the finalize
transaction, and executed afterwards by the in-process
``HookOutboxDrainer``. The decision table (``build_finalize_jobs``) lives
in ``database.hook_outbox``; ``finalize_run`` applies it as the DEFAULT
from the row's START-stamped metadata — no finalize path can skip
required effects. Phase 1 runs one drainer; Phase 2 adds competing
drainers over the same job protocol — committed claims, lease-expiry
reclaim, stable idempotency keys, per-ordering-key FIFO, effect-before-ack
retry safety.

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


# ---------------------------------------------------------------------------
# Executors — one per hook_type. Raising = nack (retry with backoff, dead at
# MAX_ATTEMPTS); returning = ack. Each validates its own applicability so a
# job enqueued for an ordinary run degrades to a no-op, never an error.
# ---------------------------------------------------------------------------


async def _exec_burst_release(payload: Dict[str, Any]) -> None:
    from src.server.dependencies.usage_limits import release_burst_slot

    user_id = payload.get("user_id")
    if user_id:
        await release_burst_slot(user_id, payload.get("slot_id"), strict=True)


async def _exec_report_back(payload: Dict[str, Any]) -> None:
    from src.server.handlers.chat.report_back import _flash_report_back

    await _flash_report_back(payload["ptc_thread_id"])


async def _exec_needs_input_wake(payload: Dict[str, Any]) -> None:
    from src.server.handlers.chat.report_back import publish_wake
    from src.server.handlers.chat.report_back_keys import ptc_origin_key
    from src.utils.cache.redis_cache import get_cache_client

    cache = get_cache_client()
    ptc_thread_id = payload["ptc_thread_id"]
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


async def _exec_watch_clear(payload: Dict[str, Any]) -> None:
    from src.server.handlers.chat.report_back import (
        clear_flash_report_back,
        publish_wake,
    )
    from src.server.handlers.chat.report_back_keys import ptc_origin_key
    from src.utils.cache.redis_cache import get_cache_client

    cache = get_cache_client()
    ptc_thread_id = payload["ptc_thread_id"]
    # Strict read: raises on ANY unavailable state (see _exec_needs_input_wake)
    # so the drainer nacks instead of acking a dropped clear.
    origin = await cache.get_strict(ptc_origin_key(ptc_thread_id))
    if not origin:
        return  # ordinary run, or already cleared — idempotent
    flash_tid = origin.get("flash_thread_id")
    await clear_flash_report_back(
        cache,
        ptc_thread_id,
        flash_tid,
        user_id=payload.get("user_id") or origin.get("user_id"),
    )
    if flash_tid and payload.get("error_wake"):
        # Wake watching clients so a cancelled/failed dispatch's card
        # reconciles instead of spinning until TTL.
        await publish_wake(cache, flash_tid, error="background_workflow_failed")


_EXECUTORS: Dict[str, Callable[[Dict[str, Any]], Awaitable[None]]] = {
    "burst_release": _exec_burst_release,
    "report_back": _exec_report_back,
    "needs_input_wake": _exec_needs_input_wake,
    "watch_clear": _exec_watch_clear,
}


# ---------------------------------------------------------------------------
# Drainer
# ---------------------------------------------------------------------------


class HookOutboxDrainer:
    """Single in-process drainer: claim → execute → ack/nack, forever.

    Effects run OUTSIDE the claim row lock (the claim transaction commits
    the lease first), so a crash mid-effect leaves a claimed row whose
    lease expiry re-offers it — the reclaim path doubles as startup
    recovery, hence executors must be idempotent.
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

    def nudge(self) -> None:
        """Post-commit hint that new jobs exist; the 5s poll is the backstop."""
        self._nudge.set()

    async def _loop(self) -> None:
        while True:
            try:
                jobs = await outbox_db.claim_outbox_jobs(
                    limit=CLAIM_BATCH, lease_seconds=LEASE_SECONDS
                )
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.error("[HookOutbox] claim query failed", exc_info=True)
                jobs = []

            for job in jobs:
                await self._execute(job)

            if len(jobs) >= CLAIM_BATCH:
                continue  # a full batch means more are likely due right now
            try:
                await asyncio.wait_for(self._nudge.wait(), timeout=POLL_INTERVAL)
            except asyncio.TimeoutError:
                pass
            self._nudge.clear()

    async def _execute(self, job: Dict[str, Any]) -> None:
        job_id = str(job["hook_outbox_id"])
        hook_type = job["hook_type"]
        executor = _EXECUTORS.get(hook_type)
        if executor is None:
            logger.error(
                f"[HookOutbox] unknown hook_type={hook_type} job={job_id}; "
                f"nacking toward dead"
            )
            try:
                await outbox_db.nack_outbox_job(job_id, max_attempts=MAX_ATTEMPTS)
            except Exception:
                logger.error(f"[HookOutbox] nack failed for {job_id}", exc_info=True)
            return
        try:
            await executor(job.get("payload") or {})
        except asyncio.CancelledError:
            raise  # shutdown: lease expiry re-offers the claimed row
        except Exception:
            logger.warning(
                f"[HookOutbox] {hook_type} failed for job={job_id} "
                f"run={job.get('run_id')} (attempt {job.get('attempts')})",
                exc_info=True,
            )
            try:
                new_status = await outbox_db.nack_outbox_job(
                    job_id, max_attempts=MAX_ATTEMPTS
                )
                if new_status == "dead":
                    logger.error(
                        f"[HookOutbox] job={job_id} type={hook_type} dead "
                        f"after {job.get('attempts')} attempts"
                    )
            except Exception:
                logger.error(f"[HookOutbox] nack failed for {job_id}", exc_info=True)
            return
        try:
            await outbox_db.ack_outbox_job(job_id)
        except Exception:
            # The effect ran; a lost ack re-runs it after lease expiry —
            # acceptable because every executor is idempotent.
            logger.warning(f"[HookOutbox] ack failed for {job_id}", exc_info=True)
