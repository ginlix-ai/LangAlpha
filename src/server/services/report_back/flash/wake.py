"""Report-back wake wire-protocol (publish + subscribe), one home.

The subscribe side takes its state-on-attach slice as an injected
``snapshot_reader`` so the wire-protocol never depends on the read-model —
the composition point (core) binds it.
"""

from __future__ import annotations

import json
import logging
import time

from src.server.services.report_back.flash.keys import thread_wake_key

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
    publish failures — a dropped nudge degrades to the client's ``/status``
    poll.
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

    Owns the pub/sub lifecycle, ``WAKE_EVENT`` frame format, keepalives, and the
    max-duration auto-close so the ``/watch`` route stays a thin auth wrapper.
    Forwards EVERY wake, not just the first: N concurrent PTCs' report-backs
    arrive as separate runs and must all be delivered on the one connection.
    """
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
        # subscribe() only WRITES the command; the registration is proven only
        # by a frame coming back (the confirmation, or a message — which only
        # a registered subscriber receives). Snapshotting without that proof
        # recreates the very window the snapshot exists to close (a wake
        # published during registration missing both the buffer and the
        # slice), so an unproven subscribe CLOSES the stream instead — the
        # client's paced resubscribe retries. A raced-in wake is held and
        # delivered right behind the snapshot.
        early_wake = None
        registered = False
        try:
            ack = await pubsub.get_message(
                ignore_subscribe_messages=False, timeout=1.0
            )
            if ack is not None:
                registered = True
                if ack.get("type") == "message":
                    early_wake = ack
        except Exception:
            logger.warning(
                f"[RB_WAKE] Subscribe-ack wait failed for thread "
                f"{flash_thread_id}",
                exc_info=True,
            )
        if not registered:
            logger.warning(
                f"[RB_WAKE] Subscribe unconfirmed for thread "
                f"{flash_thread_id}; closing watch (client will resubscribe)"
            )
            return
        # Snapshot AFTER subscribing: a wake published during the slice read
        # waits in the pub/sub buffer and is delivered right behind it, so the
        # subscriber sees state-then-deltas with no gap in either order.
        try:
            snapshot = await snapshot_reader(flash_thread_id)
            yield f'event: {SNAPSHOT_EVENT}\ndata: {json.dumps(snapshot)}\n\n'
        except Exception:
            logger.warning(
                f"[RB_WAKE] Snapshot read failed for thread {flash_thread_id}",
                exc_info=True,
            )
        while True:
            if time.monotonic() - started_at > WAKE_MAX_WATCH_DURATION:
                yield 'event: timeout\ndata: {}\n\n'
                break
            if early_wake is not None:
                msg, early_wake = early_wake, None
            else:
                msg = await pubsub.get_message(
                    ignore_subscribe_messages=True,
                    timeout=WAKE_KEEPALIVE_INTERVAL,
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
