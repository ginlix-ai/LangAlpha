"""Uniform terminal hooks via the durable outbox (v4 1.7, invariant I5).

Every required post-commit effect of a run's terminal transition is a
unique idempotent ``hook_outbox`` row written INSIDE the finalize
transaction, and executed afterwards by the ``HookOutboxDrainer``. The
decision table (``build_finalize_jobs``) lives in ``database.runs.outbox``;
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
import time
from typing import Any, Awaitable, Callable, Dict, Optional

from src.server.database.runs import outbox as outbox_db

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
# share so a burst of tail completions can't starve hand-off report-backs and
# cheap wakes. Excluded types stay unclaimed but still head their chains.
TASK_RB_INFLIGHT_QUOTA = 3
# command_report_back jobs hold their slot through a whole notification turn
# too, and one deploy restarting many computers can end many commands at
# once, so they get the same share.
COMMAND_RB_INFLIGHT_QUOTA = 3
# market_target jobs ride out admission refusals holding their slot, and each
# thread's notices are their own chain, so one market-wide move (or a session
# close, which every target on the venue warns ahead of) makes a job due on
# every watching thread at once. Cap their share too, or a burst of wakes
# refused for credit or rate could pin every slot and stall steers and
# report-backs for everyone. A claim never asks for more jobs than the room
# left under any quota, so a burst cannot overshoot one in a single batch.
MARKET_TARGET_INFLIGHT_QUOTA = 3
# A wakeup's notice rides out refusals the same way, and a market-wide move
# fires every price wakeup on the venue together, so it gets the same share.
WAKEUP_INFLIGHT_QUOTA = 3
_INFLIGHT_QUOTAS = {
    "task_report_back": TASK_RB_INFLIGHT_QUOTA,
    "command_report_back": COMMAND_RB_INFLIGHT_QUOTA,
    "market_target": MARKET_TARGET_INFLIGHT_QUOTA,
    "wakeup": WAKEUP_INFLIGHT_QUOTA,
}
# How long shutdown waits for the loop, and then for its in-flight jobs.
STOP_GRACE = 10.0
# Terminal-row retention: far beyond every read-back window (done-recents
# recovery, dead-revive ceiling ≈ a day, idempotency re-observation).
RETENTION_SECONDS = 7 * 24 * 3600.0
PURGE_INTERVAL = 3600.0
PURGE_BATCH = 5000
# How often the drainer closes alerts (a price target's or a wakeup's notice)
# no turn will take in, and how often it revives dead ones. Neither is urgent; a revive waits out a
# cool-down anyway (``outbox.revive_dead_alerts``), and reads terminal rows.
ALERT_CLOSE_INTERVAL = 60.0
ALERT_REVIVE_INTERVAL = 600.0


# ---------------------------------------------------------------------------
# Executors — one per hook_type, taking the full job row (report_back needs
# its id for lease heartbeats + payload merges). Raising = nack (retry with
# backoff, dead at MAX_ATTEMPTS); returning = ack. Each validates its own
# applicability so a job enqueued for an ordinary run degrades to a no-op,
# never an error.
#
# Only the infra-owned burst_release lives here. Domain executors register
# from their owning handlers modules at startup (register_hook_executor,
# called by each module's register_outbox_executors() before the drainer
# starts) — the outbox never imports handlers.
# ---------------------------------------------------------------------------


async def _exec_burst_release(job: Dict[str, Any]) -> None:
    from src.server.dependencies.usage_limits import release_burst_slot

    payload = job.get("payload") or {}
    user_id = payload.get("user_id")
    if user_id:
        await release_burst_slot(user_id, payload.get("slot_id"), strict=True)


_EXECUTORS: Dict[str, Callable[[Dict[str, Any]], Awaitable[None]]] = {
    "burst_release": _exec_burst_release,
}
# Optional per-type followups once a row stops being open: run best-effort
# AFTER a successful fenced ack, or once the row is parked dead (e.g. a
# "reconcile now" wake whose whole point is that the row is no longer open).
# A failure never un-acks the job.
_CLOSE_FOLLOWUPS: Dict[str, Callable[[Dict[str, Any]], Awaitable[None]]] = {}
# Best-effort followups for a row its type settles itself at the attempts
# ceiling rather than parking it dead (a steer, ``outbox._exhaust_steers``;
# an alert, ``outbox._exhaust_alerts``).
_EXHAUST_FOLLOWUPS: Dict[str, Callable[[Dict[str, Any]], Awaitable[None]]] = {}


def register_hook_executor(
    hook_type: str,
    executor: Callable[[Dict[str, Any]], Awaitable[None]],
    *,
    on_closed: Optional[Callable[[Dict[str, Any]], Awaitable[None]]] = None,
    on_exhausted: Optional[Callable[[Dict[str, Any]], Awaitable[None]]] = None,
) -> None:
    """Bind a hook_type to its executor. Idempotent for the same function;
    a different executor for a registered type is a wiring bug — raise."""
    existing = _EXECUTORS.get(hook_type)
    if existing is not None and existing is not executor:
        raise ValueError(
            f"hook_type {hook_type!r} already registered to "
            f"{existing.__qualname__}; refusing {executor.__qualname__}"
        )
    _EXECUTORS[hook_type] = executor
    if on_closed is not None:
        _CLOSE_FOLLOWUPS[hook_type] = on_closed
    if on_exhausted is not None:
        _EXHAUST_FOLLOWUPS[hook_type] = on_exhausted


# Dead-letter compensation (a dead report_back's watch_clear) is inserted
# ATOMICALLY with the dead transition inside nack_outbox_job /
# park_exhausted_jobs — a separate post-commit insert could be lost to a
# crash and the dead row is never swept again.


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
        self._stopping = asyncio.Event()
        # task -> hook_type, so per-type quotas can count in-flight work.
        self._inflight: Dict[asyncio.Task, str] = {}
        self._next_purge = 0.0
        self._next_alert_revive = 0.0
        self._next_alert_close = 0.0

    def start(self) -> None:
        if self._task is None or self._task.done():
            self._stopping.clear()
            self._task = asyncio.create_task(self._loop(), name="hook-outbox-drainer")

    async def stop(self) -> None:
        """Wind the loop down by flag, escalating to cancellation only on grace.

        Cancellation alone is not a reliable stop signal here. One await that
        absorbs a ``CancelledError`` and returns normally — psycopg's pool wait
        does exactly that on the claim query — leaves the task permanently
        ``cancelling()``, and ``asyncio.wait_for`` then reads every *later*
        cancel as its own timeout expiring and converts it to ``TimeoutError``.
        The loop becomes uncancellable, and an unbounded ``await self._task``
        hangs shutdown until the deploy's grace period SIGKILLs the worker
        mid-run. The flag can't be swallowed, and the waits below are bounded
        so no single wedged await can hold the process.
        """
        self._stopping.set()
        self._nudge.set()  # wake the poll immediately rather than after 5s
        if self._task is not None:
            try:
                await asyncio.wait_for(self._task, timeout=STOP_GRACE)
            except (TimeoutError, asyncio.TimeoutError):
                # wait_for already cancelled it on the way out.
                logger.warning("[HookOutbox] drainer exceeded stop grace")
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
            try:
                await asyncio.wait(inflight, timeout=STOP_GRACE)
            except Exception:
                pass
        self._inflight.clear()

    def nudge(self) -> None:
        """Post-commit hint that new jobs exist; the 5s poll is the backstop."""
        self._nudge.set()

    async def _loop(self) -> None:
        while not self._stopping.is_set():
            # Dead-park expired claims that already burned max_attempts leases
            # (crashed owners never nack); their chains unwedge here. The
            # watch_clear compensation rides the same statement.
            try:
                for parked in await outbox_db.park_exhausted_jobs(
                    max_attempts=MAX_ATTEMPTS
                ):
                    if parked.get("status") != "dead":
                        await self._exhausted(parked)
                        continue
                    logger.error(
                        f"[HookOutbox] parked exhausted job="
                        f"{parked.get('hook_outbox_id')} "
                        f"type={parked.get('hook_type')} as dead"
                    )
                    await self._close_followup(parked)
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

            # An alert out of attempts waits for its thread's next turn
            # instead of going dead. One an older build dead-lettered is
            # made due again, at most twice, while its notice is owed, as is
            # a command's report, a type an older build cannot run; one no
            # turn will take in (not owed, or its thread archived or its
            # workspace deleted) is closed rather than left pending for good.
            if time.monotonic() >= self._next_alert_revive:
                self._next_alert_revive = time.monotonic() + ALERT_REVIVE_INTERVAL
                try:
                    for revived in await outbox_db.revive_dead_alerts():
                        logger.warning(
                            f"[HookOutbox] revived dead alert job="
                            f"{revived.get('hook_outbox_id')} "
                            f"key={revived.get('ordering_key')} for another "
                            f"wake"
                        )
                except asyncio.CancelledError:
                    raise
                except Exception:
                    logger.error(
                        "[HookOutbox] alert revive sweep failed",
                        exc_info=True,
                    )
            if time.monotonic() >= self._next_alert_close:
                self._next_alert_close = time.monotonic() + ALERT_CLOSE_INTERVAL
                try:
                    for closed in await outbox_db.close_unowed_waiting_alerts():
                        logger.info(
                            f"[HookOutbox] closed notice job="
                            f"{closed.get('hook_outbox_id')} "
                            f"key={closed.get('ordering_key')}: no turn will "
                            f"take it in"
                        )
                except asyncio.CancelledError:
                    raise
                except Exception:
                    logger.error(
                        "[HookOutbox] alert close sweep failed",
                        exc_info=True,
                    )

            # Retention: purge terminal rows past every read-back window so
            # the table (and the sweeps above) stays flat on a long-lived
            # deployment.
            if time.monotonic() >= self._next_purge:
                self._next_purge = time.monotonic() + PURGE_INTERVAL
                try:
                    purged = 0
                    for _ in range(10):
                        n = await outbox_db.purge_terminal_jobs(
                            retention_seconds=RETENTION_SECONDS,
                            batch_size=PURGE_BATCH,
                        )
                        purged += n
                        if n < PURGE_BATCH:
                            break
                    if purged:
                        logger.info(
                            f"[HookOutbox] purged {purged} terminal jobs past "
                            f"retention"
                        )
                except asyncio.CancelledError:
                    raise
                except Exception:
                    logger.error(
                        "[HookOutbox] retention purge failed", exc_info=True
                    )

            # Claim only what free slots can run: an over-claimed job would sit
            # here burning its lease while it waits for a task slot.
            free = MAX_CONCURRENT_JOBS - len(self._inflight)
            jobs = []
            if free > 0:
                running = list(self._inflight.values())
                room = {
                    hook_type: quota - running.count(hook_type)
                    for hook_type, quota in _INFLIGHT_QUOTAS.items()
                }
                excluded = [t for t, left in room.items() if left <= 0] or None
                batch = min(
                    CLAIM_BATCH, free, *(left for left in room.values() if left > 0)
                )
                try:
                    jobs = await outbox_db.claim_outbox_jobs(
                        limit=batch,
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

                if len(jobs) >= batch:
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
            except Exception:
                logger.error(f"[HookOutbox] nack failed for {job_id}", exc_info=True)
                return
            if new_status == "exhausted":
                await self._exhausted(job)
            elif new_status == "dead":
                # A dead report_back's watch_clear compensation was
                # inserted atomically by the nack statement itself.
                logger.error(
                    f"[HookOutbox] job={job_id} type={hook_type} dead "
                    f"after {attempts} attempts"
                )
                await self._close_followup(job)
            return
        try:
            await outbox_db.ack_outbox_job(job_id, attempts=attempts)
        except Exception:
            # The effect ran; a lost ack re-runs it after lease expiry —
            # acceptable because every executor is idempotent. No wake either:
            # the row is still open, so the thread legitimately reads pending.
            logger.warning(f"[HookOutbox] ack failed for {job_id}", exc_info=True)
            return
        await self._close_followup(job)

    async def _exhausted(self, job: Dict[str, Any]) -> None:
        """A row out of attempts that its type settled rather than letting it
        go dead: worth a warning, not a dead letter, and no close followup."""
        logger.warning(
            f"[HookOutbox] job={job.get('hook_outbox_id')} "
            f"type={job.get('hook_type')} out of attempts after "
            f"{job.get('attempts')}; settled by its type"
        )
        followup = _EXHAUST_FOLLOWUPS.get(job.get("hook_type") or "")
        if followup is None:
            return
        try:
            await followup(job)
        except Exception:
            logger.debug(
                f"[HookOutbox] exhaust followup failed for "
                f"{job.get('hook_outbox_id')} type={job.get('hook_type')}",
                exc_info=True,
            )

    async def _close_followup(self, job: Dict[str, Any]) -> None:
        followup = _CLOSE_FOLLOWUPS.get(job.get("hook_type") or "")
        if followup is None:
            return
        # Best-effort: the row is already closed; a dropped followup degrades
        # to the type's own backstop (e.g. a watch recycle).
        try:
            await followup(job)
        except Exception:
            logger.debug(
                f"[HookOutbox] close followup failed for "
                f"{job.get('hook_outbox_id')} type={job.get('hook_type')}",
                exc_info=True,
            )
