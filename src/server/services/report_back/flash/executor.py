"""Flash report-back execution: the outbox job that delivers one summary turn.

``execute_report_back`` is the sole entry — run under the hook-outbox
drainer's lease, it POSTs the synthetic summary turn (job-deterministic
``request_key``), persists the dispatched run id for crash-resume, and holds
the job open until the summary run's row reaches terminal so the per-flash
ordering chain can't advance early.
"""

from __future__ import annotations

import logging
import uuid

from src.server.services.report_back.flash import leases, pointer, wake
from src.server.services.report_back.flash.keys import (
    decode,
    flash_watch_key,
    ptc_origin_key,
)

# Same hard-coded logger name request_prep uses — existing log routing keys off it.
logger = logging.getLogger("src.server.handlers.chat_handler")

# Namespace for the report-back POST's request_key: uuid5(NS, outbox job id).
# Deterministic per job, so a crash-and-reclaim re-POST dedups to the original
# summary run instead of starting a second one.
RB_REQUEST_NS = uuid.UUID("6f7a2b1c-9d3e-4f50-8a61-c2d4e5f60718")


async def execute_report_back(job: dict) -> None:
    """Execute one ``report_back`` outbox job: POST the summary turn, await terminal.

    Sole caller is the hook-outbox drainer — raising nacks the job (retry with
    backoff), returning acks it. Per-flash ordering is the outbox's ordering-key
    FIFO: this job stays open (lease-heartbeated, including through the POST's
    defer loop) until the summary run reaches terminal, so the next report-back
    on the same flash thread can't POST early. Crash-safe resume: the dispatched
    run id is merged into the job payload right after the POST, and the POST
    itself carries a job-deterministic ``request_key`` so a re-POST before that
    merge lands adopts the original run via 409 duplicate_request. Every
    destructive exit re-verifies the lease fence first — a stale owner must
    not tear down state the reclaiming owner is executing against.
    """
    from src.server.database.runs import outbox as outbox_db
    from src.server.services.hook_outbox import LEASE_SECONDS
    from src.utils.cache.redis_cache import get_cache_client

    payload = job.get("payload") or {}
    ptc_thread_id = payload["ptc_thread_id"]
    dispatch_gen = payload.get("dispatch_gen")
    job_id = str(job["hook_outbox_id"])
    attempts = job["attempts"]
    # One deterministic request identity per job: dedups the POST at the DB
    # layer AND scopes the run-pointer claim/re-assert to this job.
    job_request_key = str(uuid.uuid5(RB_REQUEST_NS, job_id))

    async def _fence() -> bool:
        """Heartbeat + ownership check in one: extends our lease iff we still
        hold this claim generation."""
        return await outbox_db.extend_job_lease(
            job_id, LEASE_SECONDS, attempts=attempts
        )

    cache = get_cache_client()
    # Strict read: raises on ANY unavailable state — blip, failed startup
    # connect, config-off — so the drainer nacks instead of acking a dropped
    # dispatch as "not report-back".
    origin = await cache.get_strict(ptc_origin_key(ptc_thread_id))
    if not origin or origin.get("origin") != "flash" or not origin.get("report_back"):
        return
    flash_thread_id = origin.get("flash_thread_id")
    user_id = origin.get("user_id")
    if not flash_thread_id or not user_id:
        return
    # Already terminal-cleared (duplicate completion event, or a reclaimed job
    # whose summary turn already finished and watch-cleared): nothing to do.
    if not await cache.client.sismember(flash_watch_key(flash_thread_id), ptc_thread_id):
        return
    # Claimed under a stale ordering key (pre-deploy row that finalized
    # unstamped, keyed on its own PTC thread): requeue onto the real flash
    # chain instead of executing here — N such jobs would busy-wait their
    # individual caps CONCURRENTLY against one flash thread's admission gate
    # and can drop summaries permanently, where correctly-keyed rows just
    # wait their turn at the chain head. The fenced ack no-ops afterwards.
    if job.get("ordering_key") != flash_thread_id:
        from src.server.services.hook_outbox import MAX_ATTEMPTS

        requeued = await outbox_db.requeue_job_with_key(
            job_id,
            attempts=attempts,
            ordering_key=flash_thread_id,
            max_attempts=MAX_ATTEMPTS,
        )
        logger.info(
            f"[FLASH_REPORT_BACK] Requeued job {job_id} for {ptc_thread_id} "
            f"onto flash chain {flash_thread_id} "
            f"(stale key {job.get('ordering_key')!r}, status={requeued})"
        )
        return

    rb_run_id = payload.get("dispatched_run_id")
    if rb_run_id:
        logger.info(
            f"[FLASH_REPORT_BACK] Resuming in-flight report-back run {rb_run_id} "
            f"for {ptc_thread_id} on flash thread {flash_thread_id} (no re-dispatch)"
        )
    else:
        outcome, rb_run_id = await _post_report_back(
            cache,
            flash_thread_id,
            ptc_thread_id,
            origin,
            request_key=job_request_key,
            heartbeat=_fence,
            dispatch_gen=dispatch_gen,
        )

        if outcome == "lost":
            # Lease lost inside the POST defer loop: the reclaiming owner is
            # (or will be) executing this job; do nothing further. The
            # drainer's fenced ack no-ops.
            return
        if outcome in ("deleted", "drop", "cap"):
            # Key-lock + row-lock fence held ACROSS the teardown (not just
            # checked before it): while held, no sibling claim on this
            # ordering key can be gated and this row can't be reclaimed — a
            # paused stale owner can't clear state a newer incarnation has
            # re-established.
            async with outbox_db.fenced_job_guard(job_id, attempts) as owned:
                if not owned:
                    return  # reclaimed: the live owner decides the teardown
                if outcome == "deleted":
                    # Flash thread is gone (404). Nothing will consume these
                    # report-backs; clear every watch member.
                    await _discard_flash_thread(cache, flash_thread_id)
                else:
                    # Terminal rejection or exhausted defer-wait. Clear this
                    # member so the chain advances; otherwise the next
                    # report-back on this flash thread would wait behind a
                    # summary turn that never starts. Pointer-gated: a POST's
                    # server route can outlive the client's socket timeout and
                    # still sit pre-START inside a lawful admission hold — the
                    # chain lease serializes THIS executor, not that in-flight
                    # route (round-5 F1). Its live claim fences the teardown
                    # atomically (and a route that has not claimed yet is
                    # refused by the claim script's membership gate once the
                    # clear lands); on refusal we nack, and the retry adopts
                    # the claim's run row, takes over its corpse, or finds
                    # the claim released.
                    cleared = await pointer.clear_flash_report_back(
                        cache, ptc_thread_id, flash_thread_id, user_id=user_id,
                        expected_gen=dispatch_gen, refuse_if_pointer=True,
                    )
                    if not cleared and cleared.fencer_gen is None:
                        raise RuntimeError(
                            f"report-back drop for {ptc_thread_id} refused: a "
                            f"live run pointer on flash thread {flash_thread_id} "
                            f"may still be mid-admission; nacking to retry"
                        )
            return

        # outcome == "dispatched"
        if rb_run_id is None:
            # POSTed but the response body didn't yield a run id — we can't
            # await its terminal, and acking would release the chain before
            # this summary finishes. Nack: the retry re-POSTs the SAME
            # request_key and recovers the id via 409 duplicate_request (or
            # the admission run-pointer claim).
            raise RuntimeError(
                f"report-back for {ptc_thread_id} dispatched without a run_id; "
                f"nacking to recover it via request_key dedup"
            )

        # Durable resume pointer FIRST: after this merge lands, a crash-and-
        # reclaim resumes the terminal wait instead of re-POSTing. (Before it
        # lands, the request_key dedup makes the re-POST safe.) A merge failure
        # is therefore tolerable — log, don't nack, the effect already ran.
        try:
            await outbox_db.merge_job_payload(job_id, {"dispatched_run_id": rb_run_id})
        except Exception:
            logger.warning(
                f"[FLASH_REPORT_BACK] Failed persisting dispatched_run_id "
                f"{rb_run_id} on job {job_id}; request_key dedup covers a re-POST",
                exc_info=True,
            )

    # Both paths (fresh dispatch AND crash-resume) confirm the run pointer and
    # publish INSIDE one fence window. The pointer re-assert is belt-and-
    # suspenders for a degraded-cache admission and lets a reloading client
    # reattach via /status; atomically gated on membership + absent-or-same-run
    # (Lua) so a fast-terminal clear is never resurrected and a paused stale
    # owner can't repoint a re-dispatched pair at its dead run. The wake is
    # published only while the fence is held AND the pointer is confirmed
    # current — a stale resume must not wake clients toward a dead run.
    async with outbox_db.fenced_job_guard(job_id, attempts) as owned:
        if not owned:
            return  # reclaimed: the live owner publishes and awaits
        pointer_ok = True
        try:
            pointer_ok = await pointer.reassert_run_pointer(
                cache,
                flash_thread_id,
                ptc_thread_id,
                rb_run_id,
                dispatch_gen=dispatch_gen,
                request_key=job_request_key,
            )
        except Exception:
            pass  # transient Redis failure: the wake is best-effort anyway
        if pointer_ok:
            await wake.publish_wake(cache, flash_thread_id, run_id=rb_run_id)
        else:
            logger.warning(
                f"[FLASH_REPORT_BACK] Pointer for {ptc_thread_id} on flash "
                f"thread {flash_thread_id} refused run {rb_run_id}; not waking"
            )

    await _await_run_terminal(
        job_id, attempts, rb_run_id, flash_thread_id, ptc_thread_id, user_id,
        dispatch_gen,
    )


async def _await_run_terminal(
    job_id: str,
    attempts: int,
    rb_run_id: str,
    flash_thread_id: str,
    ptc_thread_id: str,
    user_id: str | None,
    dispatch_gen: str | None = None,
) -> None:
    """Hold the report-back job open until its summary run reaches terminal.

    Polls the durable run row (NOT watch membership — the member is removed by
    the summary run's own watch_clear job, which queues BEHIND this one on the
    same ordering key; waiting on it would deadlock until the cap). On timeout
    (never terminal, or the row vanished for good — thread deleted) it
    force-clears the pair so the chain and dispatch caps can't stay wedged;
    on a lost lease it stands down with NO teardown — the reclaiming owner
    resumes via ``dispatched_run_id``.
    """
    from src.server.database.runs import outbox as outbox_db
    from src.server.services.report_back.notify_turn import await_run_terminal
    from src.utils.cache.redis_cache import get_cache_client

    outcome = await await_run_terminal(
        job_id,
        attempts,
        rb_run_id,
        wait_cap=leases.RB_TERMINAL_WAIT_CAP,
        log_prefix="[FLASH_REPORT_BACK]",
    )
    if outcome != "timeout":
        return
    # Row-lock fence held across the clear (see fenced_job_guard) — a
    # heartbeat alone can't cover a pause between check and mutation.
    logger.warning(
        f"[FLASH_REPORT_BACK] Terminal wait cap hit for {ptc_thread_id} "
        f"on flash thread {flash_thread_id}; clearing"
    )
    cache = get_cache_client()
    async with outbox_db.fenced_job_guard(job_id, attempts) as owned:
        if owned:
            await pointer.clear_flash_report_back(
                cache, ptc_thread_id, flash_thread_id, user_id=user_id,
                expected_gen=dispatch_gen,
            )


async def _post_report_back(
    cache,
    flash_thread_id: str,
    ptc_thread_id: str,
    origin: dict,
    *,
    request_key: str | None = None,
    heartbeat=None,
    dispatch_gen: str | None = None,
) -> tuple[str, str | None]:
    """POST the synthetic report-back message to the flash thread.

    Builds the flash-specific body (summary prompt, watch-member identity,
    dispatch generation) and delegates the admission-aware defer loop to the
    shared ``post_notification_turn``. Returns its ``(outcome, run_id)``:
    ``"dispatched"`` / ``"drop"``/``"cap"`` (caller clears the member) /
    ``"deleted"`` (caller discards the watch) / ``"lost"`` (caller stops,
    no teardown).
    """
    from src.server.services.report_back.notify_turn import post_notification_turn

    ws_label = origin.get("ptc_workspace_id") or "an auto-created workspace"
    message = (
        "<system>\n"
        f"The analysis you dispatched (thread {ptc_thread_id} in workspace "
        f"{ws_label}) has completed. Use agent_output to retrieve and "
        f"summarize the results for the user.\n"
        "</system>"
    )
    body = {
        "messages": [{"role": "user", "content": message}],
        "agent_mode": "flash",
        "workspace_id": origin.get("flash_workspace_id"),
        "query_type": "system",
        # Lets the report-back flash run identify which watch member to clear
        # on its own completion.
        "report_back_ptc_thread_id": ptc_thread_id,
        # The pair's dispatch generation rides into the summary run's START
        # metadata so its consumption watch_clear is fenced to THIS incarnation.
        "origin_dispatch_gen": dispatch_gen,
    }
    if request_key:
        body["request_key"] = request_key
    return await post_notification_turn(
        thread_id=flash_thread_id,
        body=body,
        user_id=origin.get("user_id"),
        wait_cap=leases.RB_BUSY_WAIT_CAP,
        heartbeat=heartbeat,
        log_prefix="[FLASH_REPORT_BACK]",
        subject=f"PTC thread {ptc_thread_id}",
    )


async def _discard_flash_thread(cache, flash_thread_id: str) -> None:
    """Flash thread deleted (404): disposition every snapshotted watch member.

    Raises on any failure so the drainer nacks and retries — a swallowed
    member clear here would strand that member's origin/pointer/cap state
    forever; that is also why the origin reads are STRICT — a transient
    read degrading to a gen-less clear would refuse a generated origin.
    Each member is cleared against the generation its origin holds RIGHT
    NOW (observed-gen CAS); a member whose origin moved to a different
    flash thread loses only OUR stale watch reference — its live dispatch
    owns the rest. There is deliberately NO final DEL of the watch set: a
    member SADDed by a concurrent reserve after our snapshot must survive
    (its completion would otherwise ack as a non-member and drop the
    summary). Every snapshotted member is removed individually — the
    clear's teardown Lua SREMs it, the cross-flash branches SREM it, and
    an origin that vanished mid-walk means a rival teardown already did —
    and Redis drops an empty set automatically; any residue is an orphan
    the reapers collect.
    """
    watch_key = flash_watch_key(flash_thread_id)
    members = await cache.client.smembers(watch_key)
    for member in members or []:
        ptc_tid = decode(member)
        origin = await cache.get_strict(ptc_origin_key(ptc_tid))
        if isinstance(origin, dict) and origin.get("flash_thread_id") not in (
            None,
            flash_thread_id,
        ):
            await cache.client.srem(watch_key, ptc_tid)
            continue
        observed_gen = (
            origin.get("dispatch_gen") if isinstance(origin, dict) else None
        )
        # No drained-run record: the flash thread is gone, so nothing can
        # ever render these turns.
        cleared = await pointer.clear_flash_report_back(
            cache,
            ptc_tid,
            flash_thread_id,
            record_drained=False,
            expected_gen=observed_gen,
        )
        if not cleared:
            # The origin's generation moved between our read and the CAS.
            # Re-read: moved cross-flash -> drop only our reference; gone ->
            # already torn down; still ours -> retry the whole job rather
            # than delete the watch set out from under live state.
            fresh = await cache.get_strict(ptc_origin_key(ptc_tid))
            if isinstance(fresh, dict) and fresh.get("flash_thread_id") not in (
                None,
                flash_thread_id,
            ):
                await cache.client.srem(watch_key, ptc_tid)
                continue
            if fresh is not None:
                raise RuntimeError(
                    f"discard of flash thread {flash_thread_id}: clear of "
                    f"member {ptc_tid} refused (generation moved); nacking"
                )
