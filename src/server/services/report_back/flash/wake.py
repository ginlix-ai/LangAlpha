"""Report-back wake wire-protocol (publish + subscribe), one home.

The subscribe side takes its state-on-attach slice as an injected
``snapshot_reader`` so the wire-protocol never depends on the read-model —
the composition point (core) binds it.
"""

from __future__ import annotations

import asyncio
import json
import logging
import random
import time

from src.server.services.report_back.flash.keys import thread_wake_key
from src.server.services.report_back.flash.wake_listener import (
    ThreadWakeListener,
    WakeSubscription,
)

# Same hard-coded logger name request_prep uses — existing log routing keys off it.
logger = logging.getLogger("src.server.handlers.chat_handler")


# SSE event name every report-back wake is delivered under on ``/watch``. The
# frontend watch parser keys on this exact string (web api.ts
# ``REPORT_BACK_WAKE_EVENT``) — keep the two in lockstep.
WAKE_EVENT = "workflow_started"

# State-on-attach frame emitted once per ``/watch`` subscription, carrying the
# report-back slice (same JSON as ``/status?fields=report_back``). Pub/sub has
# no replay, so this is what makes a (re)subscribe gapless: anything published
# while the client was disconnected is reflected here. Contract with web
# api.ts ``WATCH_SNAPSHOT_EVENT``.
SNAPSHOT_EVENT = "watch_snapshot"

# ``/watch`` subscriber defaults.
WAKE_KEEPALIVE_INTERVAL = 45  # seconds between keepalive comment frames
WAKE_MAX_WATCH_DURATION = 30 * 60  # auto-close an abandoned watch after 30 min

# How long a viewer waits for the per-worker listener to prove its
# registration before closing and letting the client resubscribe.
_LIVE_TIMEOUT_S = 5.0

# Upper bound on the pre-resync stagger.
_RESYNC_JITTER_S = 2.0

# A listener that stays down this long is not a blip. Viewers close so the
# client resubscribes; each close drives a /status reconcile, and the paced
# resubscribe caps at ~30s — that is the backstop, not a polling loop.
_DEAD_LISTENER_S = 30.0


async def publish_wake(
    cache,
    thread_id: str,
    run_id: str | None = None,
    *,
    error: str | None = None,
    needs_input: str | None = None,
    cleared: bool = False,
) -> None:
    """Publish a report-back wake on a watching thread's channel. Best-effort.

    Single home for the wire payload shape: a normal wake carries
    ``{thread_id, run_id}``; an error wake carries ``{error}``; a HITL pause
    on a dispatched PTC carries ``{needs_input: <ptc thread id>}`` (run_id-less,
    so the client treats it as a /status-refresh nudge); a consumption clear
    carries ``{thread_id, cleared: true}`` (the watcher reconciles and drops
    its pending chip without waiting for the status backstop). Swallows
    publish failures — a dropped nudge degrades to the client's close-driven
    ``/status`` reconcile.
    """
    if not (cache and getattr(cache, "client", None)):
        logger.warning(
            f"[RB_WAKE] No cache client; wake for thread {thread_id} not published"
        )
        return
    if error:
        payload = {"error": error}
    elif needs_input:
        payload = {"needs_input": needs_input}
    elif cleared:
        payload = {"thread_id": thread_id, "cleared": True}
    else:
        payload = {"thread_id": thread_id, "run_id": run_id}
    try:
        await cache.client.publish(thread_wake_key(thread_id), json.dumps(payload))
    except Exception:
        logger.warning(
            f"[RB_WAKE] Wake publish failed for thread {thread_id}", exc_info=True
        )


async def watch_wakes(cache, flash_thread_id: str, *, snapshot_reader):
    """Yield SSE frames for a flash thread's report-back wake subscription.

    Owns the ``WAKE_EVENT`` frame format, keepalives, and the max-duration
    auto-close so the ``/watch`` route stays a thin auth wrapper. Forwards
    EVERY wake, not just the first: N concurrent PTCs' report-backs arrive as
    separate runs and must all be delivered on the one connection.

    The Redis subscription itself belongs to the per-worker
    ``ThreadWakeListener``; this generator holds nothing but a queue.
    """
    if not (
        cache
        and getattr(cache, "enabled", False)
        and getattr(cache, "client", None)
    ):
        yield 'event: error\ndata: {"error": "watch unavailable"}\n\n'
        return

    listener = ThreadWakeListener.get_instance()
    sub = listener.attach(flash_thread_id)
    if sub is None:
        yield 'event: error\ndata: {"error": "watch unavailable"}\n\n'
        return
    started_at = time.monotonic()
    try:
        # Registration is proven once per process, not once per viewer — but
        # it is still proven. Snapshotting against an unregistered pattern
        # recreates the very window the snapshot exists to close (a wake
        # published during registration missing both the queue and the
        # slice), so an unproven subscription CLOSES the stream instead and
        # the client's paced resubscribe retries.
        if not await listener.wait_live(_LIVE_TIMEOUT_S):
            logger.warning(
                f"[RB_WAKE] Wake listener not live; closing watch for thread "
                f"{flash_thread_id} (client will resubscribe)"
            )
            return
        # Attach happened before this point without awaiting, so a wake
        # published during the slice read is already queued and is delivered
        # right behind the snapshot: state-then-deltas with no gap in either
        # order.
        resync = False
        while True:
            if time.monotonic() - started_at > WAKE_MAX_WATCH_DURATION:
                yield 'event: timeout\ndata: {}\n\n'
                return

            if resync:
                # One listener reconnect re-arms every viewer in the process
                # at once; jitter keeps them from re-reading in lockstep.
                await asyncio.sleep(random.uniform(0.0, _RESYNC_JITTER_S))
                if not await listener.wait_live(_LIVE_TIMEOUT_S):
                    return
            # Cleared BEFORE the read, so a reconnect *during* it re-arms and
            # we snapshot again — a wasted read, never a missed one.
            sub.needs_resync = False
            try:
                snapshot = await snapshot_reader(flash_thread_id)
            except Exception:
                logger.warning(
                    f"[RB_WAKE] Snapshot read failed for thread {flash_thread_id}",
                    exc_info=True,
                )
                if resync:
                    # Deltas were lost and the state that would have replaced
                    # them didn't arrive. Resuming here leaves the client
                    # silently stale; close and let it resubscribe.
                    return
            else:
                yield f'event: {SNAPSHOT_EVENT}\ndata: {json.dumps(snapshot)}\n\n'

            while not sub.needs_resync:
                if time.monotonic() - started_at > WAKE_MAX_WATCH_DURATION:
                    yield 'event: timeout\ndata: {}\n\n'
                    return
                try:
                    item = await asyncio.wait_for(
                        sub.queue.get(), timeout=WAKE_KEEPALIVE_INTERVAL
                    )
                except asyncio.TimeoutError:
                    dark = listener.dark_for()
                    if dark > _DEAD_LISTENER_S:
                        logger.error(
                            f"[RB_WAKE] Listener dark for {dark:.0f}s; closing "
                            f"watch for thread {flash_thread_id}"
                        )
                        return
                    yield ': ping\n\n'
                    continue
                if item is WakeSubscription.NUDGE:
                    continue
                yield f'event: {WAKE_EVENT}\ndata: {item}\n\n'
            resync = True
    finally:
        # Synchronous, so cancellation cannot interrupt it half-done — and
        # there is no Redis resource behind it to leak either way.
        listener.detach(sub)
