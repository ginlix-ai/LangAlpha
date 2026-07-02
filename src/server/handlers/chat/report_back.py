"""Concurrent PTC report-back subsystem — sole owner of the machinery.

A flash thread can dispatch N background PTC analyses; each completion "reports
back" as its own ordered flash turn. This module owns the whole lifecycle so no
other layer hand-rolls Redis against the same key namespace: ``reserve()``
(dispatch slot + origin), ``claim()`` (idempotent run-pointer claim at
admission), ``read_report_back_status`` (the ``/status`` slice), and the
durable per-flash FIFO + single in-process consumer + report-back POST +
``clear_flash_report_back``. Single-uvicorn-worker assumption throughout; the
authoritative rationale lives in ``server.py``.
"""

from __future__ import annotations

import asyncio
import contextlib
import json

from src.config.settings import get_workflow_timeout
from src.server.handlers.chat._common import logger
from src.server.handlers.chat.report_back_keys import (
    flash_rb_done_key,
    flash_rb_queue_key,
    flash_rb_queued_key,
    flash_rb_run_key,
    flash_user_pending_key,
    flash_watch_key,
    ptc_origin_key,
    thread_wake_key,
)


# TTL for report-back Redis state (run pointers, watch SET, queue); 24h.
_FLASH_RB_RUN_TTL = 86400

# TTL for the recently-drained run-id list (flash_rb_done); 15 min.
_FLASH_RB_DONE_TTL = 900

# Max recently-drained run ids kept per flash thread (LTRIM bound).
_FLASH_RB_DONE_MAX = 10

# TTL for ptc_origin and flash_watch / flash_user_pending Redis keys (24 hours).
PTC_ORIGIN_TTL = 86400

# Caps on concurrent report-back dispatches (cost/DoS guardrail), enforced as an
# atomic reserve-before-dispatch. Known accepted gap: report_back=False
# dispatches skip reserve() entirely and count against neither cap (bounded only
# by per-dispatch HITL approval).
MAX_DISPATCH_PER_FLASH = 5
MAX_DISPATCH_PER_USER = 10

# Serializes the cap check + reservation so two concurrent ptc_agent dispatches
# can't both read an under-cap count and overshoot. In-process only.
_dispatch_reserve_lock = asyncio.Lock()

# Per-flash-thread serialization state, process-global (single-worker rationale
# is canonical in server.py). ``_rb_consumers``: one consumer task per flash
# thread draining the durable FIFO — POSTing each report-back as its own turn,
# awaiting its terminal before the next. ``_rb_terminal_events``: per-(flash,
# ptc) Event registered by the consumer before POSTing; set by
# ``clear_flash_report_back`` on terminal.
_rb_consumers: dict[str, asyncio.Task] = {}
_rb_terminal_events: dict[tuple[str, str], asyncio.Event] = {}

# Cap (seconds) on retrying a 409 (flash thread busy with the user's own turn)
# for one item; derived from the workflow timeout so a long user turn is waited out.
_RB_BUSY_WAIT_CAP = float(get_workflow_timeout())

# Cap (seconds) on waiting for a POSTed report-back to reach terminal before
# force-clearing it, so a crashed run can't wedge the whole flash queue.
_RB_TERMINAL_WAIT_CAP = _RB_BUSY_WAIT_CAP

# Statuses the report-back POST DEFERS on (retry with backoff) beyond the
# always-retried 409/>=500: 402 payment, 403 access gate, 429 rate-limit — the
# flash thread momentarily can't admit but the analysis must still report back.
# Any other 4xx is a request we built wrong, so it drops; 404 means "deleted".
_RB_DEFER_STATUSES = frozenset({402, 403, 429})


# Atomic "enqueue-if-eligible" as one EVAL: membership gate -> dedup SADD ->
# RPUSH -> 2x EXPIRE. Must be indivisible: a crash between the SADD marker and
# the RPUSH would strand the report-back (marker blocks re-enqueue); the
# conditional RPUSH-only-when-SADD-created-the-marker can't be expressed with
# MULTI/EXEC. Never touches flash_watch's TTL (owned by the dispatch path).
# Returns 1 when newly enqueued, 0 when not a live member / already queued.
# KEYS: 1=flash_watch 2=flash_rb_queued 3=flash_rb_queue  ARGV: 1=ptc_id 2=ttl_seconds
_ENQUEUE_REPORT_BACK_LUA = """
if redis.call('sismember', KEYS[1], ARGV[1]) == 0 then return 0 end
if redis.call('sadd', KEYS[2], ARGV[1]) == 0 then return 0 end
redis.call('rpush', KEYS[3], ARGV[1])
redis.call('expire', KEYS[3], ARGV[2])
redis.call('expire', KEYS[2], ARGV[2])
return 1
"""


def _decode(value) -> str:
    return value.decode() if isinstance(value, (bytes, bytearray)) else value


# ---------------------------------------------------------------------------
# ACQUIRE — reserve a dispatch slot under the caps + record the PTC origin
# ---------------------------------------------------------------------------


def _cap_error_flash() -> str:
    return (
        f"too many concurrent analyses on this thread "
        f"(max {MAX_DISPATCH_PER_FLASH}); wait for one to finish"
    )


def _cap_error_user() -> str:
    return (
        f"too many concurrent analyses running "
        f"(max {MAX_DISPATCH_PER_USER}); wait for one to finish"
    )


async def _reserve_slot_membership(
    flash_thread_id: str, ptc_thread_id: str, user_id: str
) -> tuple[str | None, dict, bool]:
    """Atomically reserve a report-back slot under the per-flash + per-user caps.

    Returns ``(cap_error_or_None, added, watch_member)``. ``added`` reports which
    memberships THIS call created so the caller rolls back only those.
    ``watch_member`` is True only when flash_watch membership is durably in place
    (the gate the completion-time enqueue checks); False on Redis-disabled /
    exception / cap rejection so the caller never promises an undeliverable
    report-back.
    """
    from src.utils.cache.redis_cache import get_cache_client

    no_add = {"watch": False, "user": False}
    try:
        cache = get_cache_client()
        if not (cache.enabled and cache.client):
            return None, dict(no_add), False
        watch_key = flash_watch_key(flash_thread_id)
        user_key = flash_user_pending_key(user_id)
        async with _dispatch_reserve_lock:
            in_watch_before = await cache.client.sismember(watch_key, ptc_thread_id)
            in_user_before = await cache.client.sismember(user_key, ptc_thread_id)
            added = {"watch": not in_watch_before, "user": not in_user_before}
            # An existing member (idempotent re-dispatch) doesn't add load, so it
            # never counts against the cap.
            if not in_watch_before:
                if await cache.client.scard(watch_key) >= MAX_DISPATCH_PER_FLASH:
                    return _cap_error_flash(), dict(no_add), False
            if not in_user_before:
                if await cache.client.scard(user_key) >= MAX_DISPATCH_PER_USER:
                    return _cap_error_user(), dict(no_add), False
            # Add only the memberships not already present (so ``added`` stays
            # truthful); refresh both TTLs either way.
            pipe = cache.client.pipeline(transaction=True)
            if added["watch"]:
                pipe.sadd(watch_key, ptc_thread_id)
            pipe.expire(watch_key, PTC_ORIGIN_TTL)
            if added["user"]:
                pipe.sadd(user_key, ptc_thread_id)
            pipe.expire(user_key, PTC_ORIGIN_TTL)
            await pipe.execute()
        return None, added, True
    except Exception as e:
        logger.warning(f"Failed to reserve PTC dispatch slot: {e}")
        return None, dict(no_add), False


async def _release_slot_membership(
    flash_thread_id: str, ptc_thread_id: str, user_id: str, added: dict
) -> None:
    """Roll back a reservation, removing only the memberships this call added."""
    try:
        from src.utils.cache.redis_cache import get_cache_client

        cache = get_cache_client()
        if cache.client:
            if added.get("watch"):
                await cache.client.srem(flash_watch_key(flash_thread_id), ptc_thread_id)
            if added.get("user"):
                await cache.client.srem(
                    flash_user_pending_key(user_id), ptc_thread_id
                )
    except Exception:
        pass


async def check_dispatch_capacity(flash_thread_id: str | None, user_id: str) -> str | None:
    """Advisory cap read for callers about to do expensive pre-dispatch work.

    Returns the cap error ``reserve()`` would raise for a NEW dispatch right
    now, else None. Takes no reservation — ``reserve()`` stays the atomic
    authority — and fails open like it (report_back off / Redis off / error
    -> None).
    """
    if not flash_thread_id:
        return None
    try:
        from src.utils.cache.redis_cache import get_cache_client

        cache = get_cache_client()
        if not (cache.enabled and cache.client):
            return None
        if await cache.client.scard(flash_watch_key(flash_thread_id)) >= MAX_DISPATCH_PER_FLASH:
            return _cap_error_flash()
        if await cache.client.scard(flash_user_pending_key(user_id)) >= MAX_DISPATCH_PER_USER:
            return _cap_error_user()
        return None
    except Exception as e:
        logger.warning(f"Dispatch capacity pre-check failed: {e}")
        return None


class _DispatchSlot:
    """Typed outcome of ``reserve()``.

    ``error`` (an over-cap message or ``"dispatch_failed"``) tells the caller to
    abort; ``wired`` is True only when flash_watch membership is durably in place
    (the completion-time gate can then deliver a report-back). ``commit()`` keeps
    the reservation on the success path; any non-commit exit rolls it back.
    """

    def __init__(self) -> None:
        self.error: str | None = None
        self.wired: bool = False
        self._committed = False
        self._reserved = False
        self._origin_owned = False
        self._added: dict = {"watch": False, "user": False}
        self._cache = None
        self._flash_thread_id: str | None = None
        self._ptc_thread_id: str | None = None
        self._user_id: str | None = None

    def commit(self) -> None:
        """Mark the dispatch as durably started so the reservation is kept."""
        self._committed = True

    async def _rollback(self) -> None:
        if self._reserved:
            await _release_slot_membership(
                self._flash_thread_id, self._ptc_thread_id, self._user_id, self._added
            )
        # Only the owning dispatch deletes the origin it wrote; never strand a
        # concurrent owning dispatch's record.
        if self._origin_owned and self._cache is not None:
            try:
                await self._cache.delete(ptc_origin_key(self._ptc_thread_id))
            except Exception:
                pass


@contextlib.asynccontextmanager
async def reserve(
    flash_thread_id: str | None,
    ptc_thread_id: str,
    ptc_workspace_id: str | None,
    flash_workspace_id: str | None,
    user_id: str,
):
    """Reserve a report-back dispatch slot + record the PTC origin, as a CM.

    The symmetric partner of ``clear_flash_report_back``: yields a typed
    ``_DispatchSlot`` and rolls the reservation back on any non-committed exit.
    ``flash_thread_id`` is None for a non-report-back dispatch — a no-op slot
    (nothing reserved/wired) so the dispatch still POSTs. Fail-closed on an owning
    origin-write failure (``slot.error = "dispatch_failed"``); cross-flash reuse
    releases its own slot and proceeds unwired.
    """
    from src.utils.cache.redis_cache import get_cache_client

    slot = _DispatchSlot()
    slot._flash_thread_id = flash_thread_id
    slot._ptc_thread_id = ptc_thread_id
    slot._user_id = user_id
    try:
        # Non-report-back dispatch: nothing to reserve or wire.
        if not flash_thread_id:
            yield slot
            return

        # Reserve a concurrency slot BEFORE the caller dispatches so two
        # concurrent ptc_agent calls can't both pass the cap check then overshoot.
        cap_error, added, watch_member = await _reserve_slot_membership(
            flash_thread_id, ptc_thread_id, user_id
        )
        slot._added = added
        if cap_error is not None:
            slot.error = cap_error
            yield slot
            return
        slot._reserved = True
        slot.wired = watch_member
        # The dispatch that newly added the watch membership OWNS the ptc_origin
        # record: it writes it fail-closed and deletes it on rollback. An
        # idempotent re-dispatch or a Redis-down fail-open path never owns it.
        slot._origin_owned = added.get("watch", False)

        cache = get_cache_client()
        slot._cache = cache

        # Record origin BEFORE the caller's POST so a watch member's origin
        # exists by the time its PTC completion can enqueue a report-back.
        origin_payload = {
            "origin": "flash",
            "flash_thread_id": flash_thread_id,
            "flash_workspace_id": flash_workspace_id,
            "ptc_thread_id": ptc_thread_id,
            "ptc_workspace_id": ptc_workspace_id,
            "report_back": True,
            "user_id": user_id,
        }
        # Serialize origin read -> cross-flash decision -> write under the global
        # dispatch lock: two concurrent dispatches of the SAME ptc thread from
        # DIFFERENT flash threads would otherwise both write, and the loser's
        # rollback would delete the winner's origin — stranding its report-back.
        # Held only for the origin phase, never across the yield.
        async with _dispatch_reserve_lock:
            existing = (
                await cache.get(ptc_origin_key(ptc_thread_id))
                if slot._origin_owned
                else None
            )
            cross_flash = (
                isinstance(existing, dict)
                and existing.get("flash_thread_id") not in (None, flash_thread_id)
            )
            if cross_flash:
                # A different flash thread already owns this PTC's origin: we
                # can't wire a second one, so release exactly what we reserved
                # (never the other flash's origin) and proceed unwired.
                await _release_slot_membership(
                    flash_thread_id, ptc_thread_id, user_id, added
                )
                slot._reserved = False
                slot._origin_owned = False
                slot.wired = False
            elif slot._origin_owned:
                # Fail-closed: a missing origin would strand the report-back, so a
                # write failure is a dispatch failure (rollback deletes it).
                try:
                    origin_written = await cache.set(
                        ptc_origin_key(ptc_thread_id), origin_payload, ttl=PTC_ORIGIN_TTL
                    )
                except Exception as e:
                    logger.warning(f"Failed to store PTC origin metadata: {e}")
                    origin_written = False
                if not origin_written:
                    slot.error = "dispatch_failed"
            else:
                # Re-dispatch / fail-open: another live dispatch owns the origin —
                # best-effort refresh, never rolled back.
                try:
                    await cache.set(
                        ptc_origin_key(ptc_thread_id), origin_payload, ttl=PTC_ORIGIN_TTL
                    )
                except Exception as e:
                    logger.warning(f"Failed to refresh PTC origin metadata: {e}")

        yield slot
    finally:
        # Single rollback path: cap-clear / origin-write failure / POST failure /
        # cancellation all exit without commit() and release the reservation.
        if not slot._committed:
            await slot._rollback()


async def claim_report_back_run(
    cache, flash_thread_id: str, ptc_thread_id: str, run_id: str
) -> tuple[str, bool]:
    """Atomically claim the report-back run pointer for one (flash, ptc) pair.

    SET NX the pointer to ``run_id``. Returns ``(winning_run_id, claimed)``:
    ``(run_id, True)`` if we won, or ``(incumbent_run_id, False)`` if a prior
    admission already owns it — making a lost-response retry (or a crash
    re-drain) idempotent. Degrades to ``(run_id, True)`` when the cache is
    unavailable or the incumbent can't be read, so the dispatch still proceeds.
    """
    if not (cache.enabled and cache.client):
        return run_id, True
    key = flash_rb_run_key(flash_thread_id, ptc_thread_id)
    try:
        won = await cache.client.set(
            key, json.dumps({"run_id": run_id}), nx=True, ex=_FLASH_RB_RUN_TTL
        )
        if won:
            return run_id, True
        existing = await cache.get(key)
    except Exception:
        # A Redis hiccup here must not 500 the admission (the POST retry loop
        # would eventually drop a completed analysis); degrade to claimed.
        logger.warning(
            f"[FLASH_REPORT_BACK] Run-claim failed for {flash_thread_id}/"
            f"{ptc_thread_id}; degrading to claimed",
            exc_info=True,
        )
        return run_id, True
    incumbent = existing.get("run_id") if isinstance(existing, dict) else None
    if incumbent:
        return incumbent, False
    return run_id, True


async def release_report_back_run(
    cache, flash_thread_id: str, ptc_thread_id: str, run_id: str
) -> None:
    """Delete a just-claimed run pointer iff it still points at ``run_id``.

    Compensates a claim whose admission then failed (e.g. a 409 from the gate) so
    a later retry isn't short-circuited to a run that never started.
    """
    if not (cache.enabled and cache.client):
        return
    key = flash_rb_run_key(flash_thread_id, ptc_thread_id)
    try:
        existing = await cache.get(key)
        if isinstance(existing, dict) and existing.get("run_id") == run_id:
            await cache.delete(key)
    except Exception:
        pass


class _ReportBackClaim:
    """Handle for the dispatched-flash report-back run claim (see ``claim``).

    ``incumbent`` is a prior admission's run_id (caller short-circuits to it,
    starting no new run) or None (caller proceeds). ``consummate()`` keeps the
    just-made claim once the run actually starts.
    """

    def __init__(self) -> None:
        self.incumbent: str | None = None
        self._consummated = False

    def consummate(self) -> None:
        """Mark the claim as backing a started run so it isn't released on exit."""
        self._consummated = True


@contextlib.asynccontextmanager
async def claim(cache, flash_thread_id: str, ptc_thread_id: str | None, run_id: str):
    """Claim the per-(flash, ptc) report-back run pointer at dispatched admission.

    Closes the report-back double-deliver: a lost-response retry (or a crash
    re-drain) must NOT start a second summary run. On enter, SET-NX the pointer; a
    prior admission's pointer surfaces as ``handle.incumbent`` (caller returns
    that run, no new one). Releases the just-made claim on any non-consummated
    exit so a later retry isn't short-circuited to a run that never started.
    No-op when ``ptc_thread_id`` is falsy (ordinary flash dispatch — zero Redis).
    """
    handle = _ReportBackClaim()
    if not ptc_thread_id or cache is None:
        yield handle
        return
    winning_run_id, claimed = await claim_report_back_run(
        cache, flash_thread_id, ptc_thread_id, run_id
    )
    if not claimed:
        handle.incumbent = winning_run_id
        yield handle
        return
    try:
        yield handle
    finally:
        if not handle._consummated:
            await release_report_back_run(cache, flash_thread_id, ptc_thread_id, run_id)


async def clear_flash_report_back(
    cache,
    ptc_thread_id: str,
    flash_thread_id: str | None,
    user_id: str | None = None,
    *,
    record_drained: bool = True,
) -> None:
    """Tear down all report-back state for one PTC thread and wake its consumer.

    Idempotent; all mutations run in one transaction so a partial failure can't
    leak the per-user cap. ``user_id`` (else read from ``ptc_origin``) releases
    the cap slot — if unresolvable we WARN rather than silently leak it. Does not
    swallow Redis errors — callers wrap it. The drained run id is recorded on
    ``flash_rb_done`` so a client that missed the wake can still find the
    finished turn; ``record_drained=False`` skips that (deleted flash thread —
    nothing can render those turns).
    """
    origin = await cache.get(ptc_origin_key(ptc_thread_id))
    if user_id is None and isinstance(origin, dict):
        user_id = origin.get("user_id")

    # Read the run pointer BEFORE the transaction deletes it; best-effort.
    drained_run_id = None
    if record_drained and flash_thread_id:
        try:
            ptr = await cache.get(flash_rb_run_key(flash_thread_id, ptc_thread_id))
            if isinstance(ptr, dict):
                drained_run_id = ptr.get("run_id")
        except Exception:
            pass

    if cache.client:
        pipe = cache.client.pipeline(transaction=True)
        pipe.delete(ptc_origin_key(ptc_thread_id))
        if flash_thread_id:
            pipe.delete(flash_rb_run_key(flash_thread_id, ptc_thread_id))
        if user_id:
            pipe.srem(flash_user_pending_key(user_id), ptc_thread_id)
        elif flash_thread_id:
            # The only path that leaks a per-user cap slot: it won't self-heal
            # (later dispatches refresh flash_user_pending's TTL) and can lock
            # the user out at MAX_DISPATCH_PER_USER.
            logger.warning(
                f"[FLASH_REPORT_BACK] Cannot release per-user cap slot for "
                f"{ptc_thread_id} (flash thread {flash_thread_id}): user id "
                f"unresolved (ptc_origin expired/missing); slot may leak"
            )
        if flash_thread_id:
            pipe.srem(flash_watch_key(flash_thread_id), ptc_thread_id)
            pipe.lrem(flash_rb_queue_key(flash_thread_id), 0, ptc_thread_id)
            pipe.srem(flash_rb_queued_key(flash_thread_id), ptc_thread_id)
        await pipe.execute()

        # LREM-first dedups a retried clear; newest first, bounded, TTL'd.
        # Best-effort — never breaks the clear.
        if drained_run_id:
            try:
                done_key = flash_rb_done_key(flash_thread_id)
                pipe = cache.client.pipeline(transaction=True)
                pipe.lrem(done_key, 0, drained_run_id)
                pipe.lpush(done_key, drained_run_id)
                pipe.ltrim(done_key, 0, _FLASH_RB_DONE_MAX - 1)
                pipe.expire(done_key, _FLASH_RB_DONE_TTL)
                await pipe.execute()
            except Exception:
                logger.warning(
                    f"[FLASH_REPORT_BACK] Failed recording drained run "
                    f"{drained_run_id} for flash thread {flash_thread_id}",
                    exc_info=True,
                )

    if flash_thread_id and cache.client:
        # Wake the consumer waiting on this exact pair. Per-(flash, ptc) so an
        # unrelated PTC's terminal never wakes (or skips) the wrong wait.
        event = _rb_terminal_events.get((flash_thread_id, ptc_thread_id))
        if event is not None:
            event.set()


# ---------------------------------------------------------------------------
# WAKE — the report-back wake wire-protocol (publish + subscribe), one home
# ---------------------------------------------------------------------------

# SSE event name every report-back wake is delivered under on ``/watch``. The
# frontend watch parser keys on this exact string (web api.ts
# ``REPORT_BACK_WAKE_EVENT``) — keep the two in lockstep.
WAKE_EVENT = "workflow_started"

# ``/watch`` subscriber defaults.
WAKE_KEEPALIVE_INTERVAL = 45  # seconds between keepalive comment frames
WAKE_MAX_WATCH_DURATION = 30 * 60  # auto-close an abandoned watch after 30 min


async def publish_wake(
    cache, flash_thread_id: str, run_id: str | None = None, *, error: str | None = None
) -> None:
    """Publish a report-back wake on a flash thread's channel. Best-effort.

    Single home for the wire payload shape: a normal wake carries
    ``{thread_id, run_id}``; an error wake carries ``{error}``. Swallows publish
    failures — a dropped nudge degrades to the client's ``/status`` poll.
    """
    if not (cache and getattr(cache, "client", None)):
        return
    payload = (
        {"error": error}
        if error
        else {"thread_id": flash_thread_id, "run_id": run_id}
    )
    try:
        await cache.client.publish(thread_wake_key(flash_thread_id), json.dumps(payload))
    except Exception:
        pass


async def watch_wakes(cache, flash_thread_id: str):
    """Yield SSE frames for a flash thread's report-back wake subscription.

    Owns the pub/sub lifecycle, ``WAKE_EVENT`` frame format, keepalives, and the
    max-duration auto-close so the ``/watch`` route stays a thin auth wrapper.
    Forwards EVERY wake, not just the first: N concurrent PTCs' report-backs
    arrive as separate runs and must all be delivered on the one connection.
    """
    import time

    if not (
        cache
        and getattr(cache, "enabled", False)
        and getattr(cache, "client", None)
    ):
        yield 'event: error\ndata: {"error": "watch unavailable"}\n\n'
        return

    channel = thread_wake_key(flash_thread_id)
    pubsub = cache.client.pubsub()
    started_at = time.monotonic()
    try:
        await pubsub.subscribe(channel)
        while True:
            if time.monotonic() - started_at > WAKE_MAX_WATCH_DURATION:
                yield 'event: timeout\ndata: {}\n\n'
                break
            msg = await pubsub.get_message(
                ignore_subscribe_messages=True, timeout=WAKE_KEEPALIVE_INTERVAL
            )
            if msg and msg["type"] == "message":
                data = msg["data"]
                if isinstance(data, bytes):
                    data = data.decode("utf-8")
                yield f'event: {WAKE_EVENT}\ndata: {data}\n\n'
            else:
                yield ': ping\n\n'
    finally:
        await pubsub.unsubscribe(channel)
        await pubsub.aclose()


async def clear_on_crash(
    thread_id: str,
    report_back_ptc_thread_id: str | None = None,
    user_id: str | None = None,
) -> None:
    """Tear down the report-back watch for a crashed background run. Best-effort.

    Called from ``threads._consume_background_gen``'s except branch. The origin
    is keyed by the *PTC* thread id, so: a PTC_DISPATCH crash hits directly
    (thread_id IS the ptc thread); a report-back run crash resolves via
    ``report_back_ptc_thread_id``; an ordinary flash-dispatch crash misses the
    origin and no-ops — a still-running dispatched PTC's keys survive for
    reload recovery.
    """
    try:
        from src.utils.cache.redis_cache import get_cache_client

        cache = get_cache_client()
        if not (cache.enabled and cache.client):
            return
        ptc_thread_id = report_back_ptc_thread_id or thread_id
        origin = await cache.get(ptc_origin_key(ptc_thread_id))
        if not origin:
            return
        flash_tid = origin.get("flash_thread_id")
        # Pass the known owner so the per-user cap slot is released even if
        # ptc_origin TTL-expired before this crash.
        await clear_flash_report_back(
            cache, ptc_thread_id, flash_tid,
            user_id=user_id or origin.get("user_id"),
        )
        if flash_tid:
            await publish_wake(cache, flash_tid, error="background_workflow_failed")
    except Exception:
        logger.warning(
            f"[FLASH_REPORT_BACK] Crash teardown after failure also failed for "
            f"thread_id={thread_id}",
            exc_info=True,
        )


# ---------------------------------------------------------------------------
# READ-MODEL — the ``/status?fields=report_back`` slice + consumer restart-nudge
# ---------------------------------------------------------------------------


async def read_report_back_status(thread_id: str) -> dict:
    """Report-back-only status slice for a flash thread.

    The JSON shape is a frontend contract; the recent list is NEWEST FIRST
    (LPUSH order). On its own Redis-read failure ``pending_report_back`` is
    ``None`` (unknown — the frontend keeps watching), distinct from an explicit
    ``False`` (drained).
    """
    pending_report_back: bool | None = False
    report_back_run_id = None
    recent_report_back_run_ids: list[str] = []
    try:
        from src.utils.cache.redis_cache import get_cache_client

        cache = get_cache_client()
        if cache.enabled and cache.client:
            # Membership is the source of truth for "pending"; the queue head
            # drives candidate priority and the restart-nudge.
            pipe = cache.client.pipeline(transaction=False)
            pipe.smembers(flash_watch_key(thread_id))
            pipe.lindex(flash_rb_queue_key(thread_id), 0)
            pipe.lrange(flash_rb_done_key(thread_id), 0, _FLASH_RB_DONE_MAX - 1)
            members_raw, head, recent_raw = await pipe.execute()

            recent_report_back_run_ids = [_decode(r) for r in (recent_raw or [])]
            members = [_decode(m) for m in (members_raw or [])]
            if members:
                pending_report_back = True
                # Resolve the run to attach to from a live per-(flash, ptc)
                # pointer — prefer the queue head (currently draining), else any
                # pending member with a pointer. Never a finished run's id.
                candidates: list[str] = []
                if head is not None:
                    candidates.append(_decode(head))
                for ptc in members:
                    if ptc not in candidates:
                        candidates.append(ptc)
                # One MGET vs N serial GETs; values are raw serialized JSON.
                ptr_keys = [flash_rb_run_key(thread_id, ptc) for ptc in candidates]
                if ptr_keys:
                    for raw in await cache.client.mget(ptr_keys):
                        if raw is None:
                            continue
                        try:
                            ptr = json.loads(raw)
                        except (TypeError, ValueError):
                            continue
                        if isinstance(ptr, dict) and ptr.get("run_id"):
                            report_back_run_id = ptr["run_id"]
                            break
                # Restart-nudge: durable queued work but (after a process
                # restart) no live consumer — (re)start it.
                if head is not None:
                    ensure_rb_consumer(thread_id)
    except Exception:
        logger.warning(
            f"Report-back status read failed for {thread_id}; reporting unknown",
            exc_info=True,
        )
        pending_report_back = None
        report_back_run_id = None
        recent_report_back_run_ids = []

    return {
        "thread_id": thread_id,
        "pending_report_back": pending_report_back,
        "report_back_run_id": report_back_run_id,
        "recent_report_back_run_ids": recent_report_back_run_ids,
    }


async def _flash_report_back(ptc_thread_id: str) -> None:
    """Enqueue a completed PTC's report-back and ensure its flash consumer runs.

    Called once per PTC completion. Enqueue-only: validates the dispatch is a
    live report-back, appends the PTC id to the flash thread's durable FIFO
    (deduped against a duplicate completion event), and lazily starts the
    consumer. The POST + terminal wait happen in ``_drain_one_report_back``.
    """
    from src.utils.cache.redis_cache import get_cache_client

    cache = get_cache_client()
    if not cache.client:
        return
    origin = await cache.get(ptc_origin_key(ptc_thread_id))
    if not origin or origin.get("origin") != "flash" or not origin.get("report_back"):
        return
    flash_thread_id = origin.get("flash_thread_id")
    if not flash_thread_id or not origin.get("user_id"):
        return

    # Atomic enqueue — see _ENQUEUE_REPORT_BACK_LUA.
    try:
        enqueued = await cache.client.eval(
            _ENQUEUE_REPORT_BACK_LUA,
            3,
            flash_watch_key(flash_thread_id),
            flash_rb_queued_key(flash_thread_id),
            flash_rb_queue_key(flash_thread_id),
            ptc_thread_id,
            _FLASH_RB_RUN_TTL,
        )
    except Exception:
        logger.warning(
            f"[FLASH_REPORT_BACK] Enqueue EVAL failed for {ptc_thread_id} "
            f"on flash thread {flash_thread_id}",
            exc_info=True,
        )
        return

    # 1 = newly enqueued -> start the consumer; 0 = not a member / already queued.
    if enqueued:
        ensure_rb_consumer(flash_thread_id)


def ensure_rb_consumer(flash_thread_id: str) -> None:
    """Start the per-flash report-back consumer if one isn't already running.

    Safe to call repeatedly (each enqueue, and the ``/status`` restart-nudge
    after a process restart). Single-process, so the dict + ``done()`` check is
    sufficient mutual exclusion — no Redis lock needed.
    """
    task = _rb_consumers.get(flash_thread_id)
    if task is not None and not task.done():
        return
    _rb_consumers[flash_thread_id] = asyncio.create_task(
        _rb_consumer_loop(flash_thread_id), name=f"rb-consumer-{flash_thread_id}"
    )


async def _rb_consumer_loop(flash_thread_id: str) -> None:
    """Drain the flash thread's report-back FIFO one turn at a time, in order."""
    from src.utils.cache.redis_cache import get_cache_client

    cache = get_cache_client()
    queue_key = flash_rb_queue_key(flash_thread_id)
    try:
        while cache.client:
            head = await cache.client.lindex(queue_key, 0)  # keep-until-terminal
            if head is None:
                return
            ptc_thread_id = _decode(head)
            # A head already terminal-cleared (membership gone) is stale; drop it.
            if not await cache.client.sismember(flash_watch_key(flash_thread_id), ptc_thread_id):
                await cache.client.lrem(queue_key, 1, head)
                continue
            await _drain_one_report_back(cache, flash_thread_id, ptc_thread_id)
    except asyncio.CancelledError:
        raise
    except Exception:
        logger.warning(
            f"[FLASH_REPORT_BACK] Consumer for flash thread {flash_thread_id} crashed",
            exc_info=True,
        )
    finally:
        _rb_consumers.pop(flash_thread_id, None)
        # An item enqueued during our teardown window would otherwise wait for
        # the next completion; restart if the durable queue is non-empty.
        try:
            if cache.client and await cache.client.llen(queue_key):
                ensure_rb_consumer(flash_thread_id)
        except Exception:
            pass


async def _drain_one_report_back(cache, flash_thread_id: str, ptc_thread_id: str) -> None:
    """POST one report-back and await its terminal before the consumer advances.

    Idempotent across a consumer crash/restart: a pre-existing run pointer means a
    prior drain already dispatched this report-back, so we resume its terminal
    wait instead of POSTing — and paying for — a duplicate summary turn.
    """
    origin = await cache.get(ptc_origin_key(ptc_thread_id))
    if not isinstance(origin, dict):
        # Terminal-cleared between the loop's check and here; drop the entry.
        await cache.client.lrem(flash_rb_queue_key(flash_thread_id), 1, ptc_thread_id)
        return

    user_id = origin.get("user_id")
    run_key = flash_rb_run_key(flash_thread_id, ptc_thread_id)

    # Register the terminal Event BEFORE posting: a fast report-back can reach
    # terminal during/just-after the POST, and clear_flash_report_back must find
    # an Event to set. The wait below is membership-first, so an already-cleared
    # member skips waiting entirely (no missed-wakeup).
    event = asyncio.Event()
    _rb_terminal_events[(flash_thread_id, ptc_thread_id)] = event
    try:
        # Clear may have fired between the loop's check and registration above
        # (its .set() then hit no Event); re-check membership and bail if gone.
        if not await cache.client.sismember(flash_watch_key(flash_thread_id), ptc_thread_id):
            await cache.client.lrem(flash_rb_queue_key(flash_thread_id), 1, ptc_thread_id)
            return

        # Idempotency gate: an existing run pointer means a prior drain already
        # dispatched this report-back before a consumer crash/restart. Resume its
        # terminal wait — re-POSTing would create a second summary turn.
        existing = await cache.get(run_key)
        rb_run_id = existing.get("run_id") if isinstance(existing, dict) else None

        if rb_run_id:
            logger.info(
                f"[FLASH_REPORT_BACK] Resuming in-flight report-back run {rb_run_id} "
                f"for {ptc_thread_id} on flash thread {flash_thread_id} (no re-dispatch)"
            )
        else:
            outcome, rb_run_id = await _post_report_back(
                cache, flash_thread_id, ptc_thread_id, origin
            )

            if outcome == "deleted":
                # Flash thread is gone (404). Nothing will consume these
                # report-backs; discard the whole queue + clear every watch member.
                await _discard_flash_thread(cache, flash_thread_id)
                return
            if outcome == "drop":
                # Terminal rejection or exhausted defer-wait. Clear this member so
                # the consumer advances; otherwise it would await a terminal that
                # never comes (no flash run was created).
                await clear_flash_report_back(
                    cache, ptc_thread_id, flash_thread_id, user_id=user_id
                )
                return

            # outcome == "dispatched": admission already claimed this pointer
            # (claim_report_back_run); re-assert it as belt-and-suspenders for a
            # degraded-cache admission and so a reloading client can reattach via
            # /status. MUST be gated on membership still being present: a
            # fast-terminal report-back may have already cleared the pointer, and
            # an unconditional set would resurrect a dead pointer — a later
            # continuation would resume a dead run and drop its summary.
            if rb_run_id and await cache.client.sismember(
                flash_watch_key(flash_thread_id), ptc_thread_id
            ):
                try:
                    await cache.set(run_key, {"run_id": rb_run_id}, ttl=_FLASH_RB_RUN_TTL)
                except Exception:
                    pass

        # Publish the wake (in-session client attaches directly). Idempotent, so
        # it's safe on the resume path too.
        await publish_wake(cache, flash_thread_id, run_id=rb_run_id)

        # Membership-first sticky wait: a terminal handler removes the member and
        # sets our Event. Bounded re-checks guard against a missed in-process
        # signal; the hard deadline force-clears a report-back that POSTs but
        # never reaches terminal so it can't wedge the whole flash queue.
        deadline = asyncio.get_running_loop().time() + _RB_TERMINAL_WAIT_CAP
        while await cache.client.sismember(flash_watch_key(flash_thread_id), ptc_thread_id):
            if asyncio.get_running_loop().time() >= deadline:
                logger.warning(
                    f"[FLASH_REPORT_BACK] Terminal wait cap hit for {ptc_thread_id} "
                    f"on flash thread {flash_thread_id}; clearing stuck member"
                )
                await clear_flash_report_back(
                    cache, ptc_thread_id, flash_thread_id, user_id=user_id
                )
                break
            try:
                await asyncio.wait_for(event.wait(), timeout=30.0)
            except asyncio.TimeoutError:
                pass
            if event.is_set():
                break
        # The terminal clear lrem'd the entry; ensure it's gone if we exited via
        # the timeout re-check path instead.
        await cache.client.lrem(flash_rb_queue_key(flash_thread_id), 0, ptc_thread_id)
    finally:
        _rb_terminal_events.pop((flash_thread_id, ptc_thread_id), None)


async def _post_report_back(
    cache, flash_thread_id: str, ptc_thread_id: str, origin: dict
) -> tuple[str, str | None]:
    """POST the synthetic report-back message to the flash thread.

    Returns ``(outcome, run_id)`` where outcome is ``"dispatched"`` (run_id set),
    ``"drop"`` (terminal 4xx or exhausted defer-wait — caller clears the member),
    or ``"deleted"`` (flash thread 404 — caller discards the queue). Defers
    (retries with backoff) on 409, >=500, and the capacity/credit/rate-limit
    gates in ``_RB_DEFER_STATUSES``, bounded by ``_RB_BUSY_WAIT_CAP``.
    """
    import os

    import aiohttp

    self_base_url = os.environ.get("GINLIXFLOW_BASE_URL", "http://localhost:8000")
    service_token = os.environ.get("INTERNAL_SERVICE_TOKEN", "")
    ws_label = origin.get("ptc_workspace_id") or "an auto-created workspace"
    message = (
        "<system>\n"
        f"The analysis you dispatched (thread {ptc_thread_id} in workspace "
        f"{ws_label}) has completed. Use agent_output to retrieve and "
        f"summarize the results for the user.\n"
        "</system>"
    )
    payload = {
        "messages": [{"role": "user", "content": message}],
        "agent_mode": "flash",
        "workspace_id": origin.get("flash_workspace_id"),
        "query_type": "system",
        # Lets the report-back flash run identify which watch member to clear
        # on its own completion.
        "report_back_ptc_thread_id": ptc_thread_id,
    }
    headers = {
        "X-Service-Token": service_token,
        "X-User-Id": origin.get("user_id"),
        "X-Dispatch": "background",
    }
    url = f"{self_base_url}/api/v1/threads/{flash_thread_id}/messages"

    deadline = asyncio.get_running_loop().time() + _RB_BUSY_WAIT_CAP
    backoff = 1.0
    async with aiohttp.ClientSession() as session:
        while True:
            try:
                async with session.post(
                    url,
                    json=payload,
                    headers=headers,
                    timeout=aiohttp.ClientTimeout(connect=10, sock_read=30),
                ) as resp:
                    if resp.status < 400:
                        try:
                            run_id = (await resp.json()).get("run_id")
                        except Exception:
                            run_id = None
                        logger.info(
                            f"[FLASH_REPORT_BACK] Dispatched report-back to flash "
                            f"thread {flash_thread_id} for PTC thread {ptc_thread_id}"
                        )
                        return "dispatched", run_id
                    if resp.status == 404:
                        logger.warning(
                            f"[FLASH_REPORT_BACK] Flash thread {flash_thread_id} gone "
                            f"(404); discarding its report-back queue"
                        )
                        return "deleted", None
                    if (
                        resp.status == 409
                        or resp.status >= 500
                        or resp.status in _RB_DEFER_STATUSES
                    ):
                        # Don't log the body here: a 429 (credit/burst) response can
                        # carry the user's balance/limit figures. Status is enough.
                        logger.info(
                            f"[FLASH_REPORT_BACK] Flash thread {flash_thread_id} cannot "
                            f"admit yet ({resp.status}); deferring report-back for "
                            f"{ptc_thread_id}"
                        )
                    else:
                        body = await resp.text()
                        logger.warning(
                            f"[FLASH_REPORT_BACK] Terminal {resp.status} POSTing to "
                            f"flash thread {flash_thread_id}: {body[:200]}; dropping"
                        )
                        return "drop", None
            except Exception as e:
                logger.warning(
                    f"[FLASH_REPORT_BACK] HTTP error POSTing to flash thread "
                    f"{flash_thread_id}: {e}"
                )

            if asyncio.get_running_loop().time() >= deadline:
                logger.warning(
                    f"[FLASH_REPORT_BACK] Busy-wait cap hit for {ptc_thread_id} on flash "
                    f"thread {flash_thread_id}; dropping"
                )
                return "drop", None
            await asyncio.sleep(min(backoff, 5.0))
            backoff = min(backoff * 2, 5.0)


async def _discard_flash_thread(cache, flash_thread_id: str) -> None:
    """Flash thread deleted (404): clear every watch member + drop all its queue state."""
    try:
        members = await cache.client.smembers(flash_watch_key(flash_thread_id))
        for member in members or []:
            # No drained-run record: the flash thread is gone, so nothing can
            # ever render these turns.
            await clear_flash_report_back(
                cache, _decode(member), flash_thread_id, record_drained=False
            )
    except Exception:
        logger.warning(
            f"[FLASH_REPORT_BACK] Failed clearing members for deleted flash thread "
            f"{flash_thread_id}",
            exc_info=True,
        )
    try:
        await cache.client.delete(flash_rb_queue_key(flash_thread_id))
        await cache.client.delete(flash_rb_queued_key(flash_thread_id))
        await cache.client.delete(flash_watch_key(flash_thread_id))
    except Exception:
        pass

