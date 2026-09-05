"""Per-worker demux for report-back wake nudges.

One ``PSUBSCRIBE thread:wake:*`` per process feeds every open ``/watch``,
replacing one pinned Redis connection per viewer — the shape that exhausted
the shared pool. Two properties follow from that and are the reason for the
design:

* Attach and detach are plain dict operations. There is no per-viewer Redis
  resource, so no teardown path can leak one; the whole class of "cancelled
  during cleanup, connection never released" bugs disappears rather than being
  guarded against.
* Pub/sub has no replay, so any gap in this process's registration loses
  wakes for *every* viewer at once. The listener therefore tracks liveness
  explicitly and re-arms each subscriber for a state re-read whenever it
  reconnects, instead of letting them resume deltas across a hole.
"""

from __future__ import annotations

import asyncio
import logging
import time
import uuid
from typing import Optional

from src.utils.concurrency import cancel_and_join
from src.server.services.report_back.flash.keys import (
    parse_thread_wake_key,
    thread_wake_pattern,
)

# Same hard-coded logger name request_prep uses — existing log routing keys off it.
logger = logging.getLogger("src.server.handlers.chat_handler")

# Per-viewer buffer. Wakes are nudges, not data: a viewer this far behind
# gains nothing from the backlog, so overflow degrades to a state re-read.
_QUEUE_MAX = 64

# get_message poll window; also bounds how quickly stop() is observed.
_POLL_TIMEOUT_S = 5.0
_RETRY_BACKOFF_S = 2.0

# Registration must be proven, not assumed: psubscribe() only writes.
_ACK_TIMEOUT_S = 5.0

# Silence is not evidence of health — see ``_pump``.
_PING_IDLE_S = 30.0
_PING_TIMEOUT_S = 10.0


class WakeSubscription:
    """One viewer's slot in the demux.

    ``needs_resync`` is the single source of truth for "you may have missed
    something"; the queue sentinel only wakes a viewer parked on ``get()``, so
    duplicate or dropped sentinels are harmless.
    """

    __slots__ = ("thread_id", "queue", "needs_resync")

    #: Queue item meaning "wake up and re-check ``needs_resync``".
    NUDGE = object()

    def __init__(self, thread_id: str) -> None:
        self.thread_id = thread_id
        self.queue: asyncio.Queue = asyncio.Queue(maxsize=_QUEUE_MAX)
        self.needs_resync = False

    def offer(self, payload: str) -> None:
        try:
            self.queue.put_nowait(payload)
        except asyncio.QueueFull:
            self.request_resync()

    def request_resync(self) -> None:
        """Drop the backlog and ask for a state re-read instead.

        The snapshot supersedes every delta being discarded, so this is
        gapless as well as cheaper than a growing buffer.
        """
        self.needs_resync = True
        while True:
            try:
                self.queue.get_nowait()
            except asyncio.QueueEmpty:
                break
        try:
            self.queue.put_nowait(self.NUDGE)
        except asyncio.QueueFull:  # pragma: no cover - just emptied it
            pass


class ThreadWakeListener:
    """Per-worker singleton owning the process's single wake subscription."""

    _instance: Optional["ThreadWakeListener"] = None

    def __init__(self) -> None:
        self._subs: dict[str, set[WakeSubscription]] = {}
        self._task: Optional[asyncio.Task] = None
        self._stopping = asyncio.Event()
        self._live = asyncio.Event()
        self._dark_since: Optional[float] = None

    @classmethod
    def get_instance(cls) -> "ThreadWakeListener":
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance

    # -- lifecycle ---------------------------------------------------------

    def start(self) -> None:
        if self._task is not None and not self._task.done():
            return
        self._stopping.clear()
        self._dark_since = time.monotonic()
        self._task = asyncio.create_task(self._run(), name="thread-wake-listener")

    async def stop(self) -> None:
        self._stopping.set()
        task, self._task = self._task, None
        if task is not None:
            await cancel_and_join(task)
        self._go_dark()

    # -- viewer API --------------------------------------------------------

    def attach(self, thread_id: str) -> Optional[WakeSubscription]:
        """Register a viewer. Synchronous, and deliberately so.

        Nothing is awaited between the caller deciding to watch and being
        routable, so there is no window in which a wake can be published to a
        viewer that exists but is not yet reachable. Returns None once
        stopping, so a watch opened during shutdown closes instead of
        subscribing to a listener that will never run.
        """
        if self._stopping.is_set():
            return None
        sub = WakeSubscription(thread_id)
        self._subs.setdefault(thread_id, set()).add(sub)
        return sub

    def detach(self, sub: WakeSubscription) -> None:
        """Synchronous on purpose: a viewer's teardown runs under cancellation,
        where any await can be interrupted before it completes."""
        peers = self._subs.get(sub.thread_id)
        if peers is None:
            return
        peers.discard(sub)
        if not peers:
            self._subs.pop(sub.thread_id, None)

    def dark_for(self) -> float:
        """Seconds since the pattern registration was last known good."""
        if self._live.is_set() or self._dark_since is None:
            return 0.0
        return time.monotonic() - self._dark_since

    async def wait_live(self, timeout: float) -> bool:
        """Never starts the listener: app setup owns its lifecycle, and a
        viewer silently reviving it would mask a failed startup. A listener
        that isn't running simply never goes live, and viewers degrade to the
        client's close-driven reconcile (the watch is push-only — there is no
        polling loop), which paces out to ~30s."""
        try:
            await asyncio.wait_for(self._live.wait(), timeout=timeout)
            return True
        except asyncio.TimeoutError:
            return False

    # -- internals ---------------------------------------------------------

    def _go_live(self) -> None:
        if self._live.is_set():
            return
        self._live.set()
        self._dark_since = None
        # Whatever was published while this process had no registration is
        # simply gone. Every attached viewer re-reads state rather than
        # resuming deltas across the hole.
        for peers in tuple(self._subs.values()):
            for sub in tuple(peers):
                sub.request_resync()

    def _go_dark(self) -> None:
        if not self._live.is_set():
            return
        self._live.clear()
        self._dark_since = time.monotonic()

    def _dispatch(self, msg: dict) -> None:
        channel = msg.get("channel")
        if isinstance(channel, (bytes, bytearray)):
            try:
                channel = channel.decode("utf-8")
            except UnicodeDecodeError:
                return
        thread_id = parse_thread_wake_key(channel or "")
        if thread_id is None:
            logger.warning(f"[RB_WAKE] Dropped unroutable wake channel {channel!r}")
            return
        peers = self._subs.get(thread_id)
        if not peers:
            return
        data = msg.get("data")
        if isinstance(data, (bytes, bytearray)):
            try:
                data = data.decode("utf-8")
            except UnicodeDecodeError:
                # The nudge is real even though its payload is unreadable —
                # dropping it silently would park these viewers until the next
                # wake, which may never come. Re-read state instead.
                logger.warning(
                    f"[RB_WAKE] Undecodable wake payload for {thread_id}; "
                    "forcing resync"
                )
                for sub in tuple(peers):
                    sub.request_resync()
                return
        for sub in tuple(peers):
            sub.offer(data)

    async def _await_psubscribe_ack(self, pubsub, pattern: str) -> bool:
        """psubscribe() only writes the command; only a confirmation frame
        proves the server registered it."""
        deadline = time.monotonic() + _ACK_TIMEOUT_S
        while time.monotonic() < deadline:
            msg = await pubsub.get_message(
                ignore_subscribe_messages=False,
                timeout=max(0.1, deadline - time.monotonic()),
            )
            if msg is None:
                continue
            if msg.get("type") == "pmessage":
                # Registered already, and this frame proves it — but it is
                # also a real wake, so route it rather than drop it.
                self._dispatch(msg)
                return True
            if msg.get("type") != "psubscribe":
                continue
            acked = msg.get("channel")
            if isinstance(acked, (bytes, bytearray)):
                acked = acked.decode("utf-8", errors="replace")
            if acked == pattern:
                return True
            logger.warning(f"[RB_WAKE] Unexpected psubscribe ack for {acked!r}")
        return False

    async def _pump(self, pubsub) -> None:
        last_rx = time.monotonic()
        pending_nonce: Optional[str] = None
        ping_deadline = 0.0
        while not self._stopping.is_set():
            msg = await pubsub.get_message(
                ignore_subscribe_messages=False, timeout=_POLL_TIMEOUT_S
            )
            now = time.monotonic()
            if msg is not None:
                mtype = msg.get("type")
                if mtype == "pmessage":
                    last_rx = now
                    pending_nonce = None
                    self._dispatch(msg)
                elif mtype == "pong":
                    data = msg.get("data")
                    if isinstance(data, (bytes, bytearray)):
                        data = data.decode("utf-8", errors="replace")
                    if pending_nonce is not None and data == pending_nonce:
                        last_rx = now
                        pending_nonce = None
                elif mtype == "punsubscribe":
                    # No registration left to pump. Rebuild from _run.
                    raise ConnectionError("wake listener registration dropped")
                elif mtype == "psubscribe":
                    # An ack here means redis-py silently reconnected and
                    # re-registered under us — so this process had no
                    # subscription for a stretch, and whatever was published in
                    # it is gone. Read that evidence directly instead of
                    # relying on the client's retry policy to re-raise: that
                    # policy is a library default nobody in this repo pins, and
                    # a single retry would turn every missed wake into a silent
                    # permanent hole for every viewer on this worker.
                    last_rx = now
                    pending_nonce = None
                    logger.warning(
                        "[RB_WAKE] Subscription rebuilt underneath the pump; "
                        "re-arming attached viewers for a resync"
                    )
                    self._go_dark()
                    self._go_live()
                continue
            # A blackholed socket returns None from get_message forever
            # without ever disconnecting, so an idle stretch proves nothing
            # on its own. Probe it, and treat an unanswered probe as dead.
            if pending_nonce is not None:
                if now > ping_deadline:
                    raise ConnectionError("wake listener PING unanswered")
                continue
            if now - last_rx >= _PING_IDLE_S:
                pending_nonce = uuid.uuid4().hex
                await pubsub.ping(pending_nonce)
                ping_deadline = now + _PING_TIMEOUT_S

    async def _run(self) -> None:
        from src.server.services.workspace_status_pubsub import (
            get_shared_pubsub_client,
        )

        pattern = thread_wake_pattern()
        while not self._stopping.is_set():
            pubsub = None
            try:
                client = await get_shared_pubsub_client()
                if client is None:
                    # No pub/sub pool right now. Retry rather than retire: the
                    # accessor also returns None while its rebuild cooldown is
                    # latched, and returning would turn that transient window
                    # into a process-lifetime outage for every viewer here —
                    # attach() keeps succeeding into a task that has exited.
                    raise ConnectionError("pub/sub pool unavailable")
                pubsub = client.pubsub()
                await pubsub.psubscribe(pattern)
                if not await self._await_psubscribe_ack(pubsub, pattern):
                    raise ConnectionError(f"psubscribe {pattern} unconfirmed")
                logger.info(f"[RB_WAKE] Wake listener subscribed to {pattern}")
                self._go_live()
                await self._pump(pubsub)
            except asyncio.CancelledError:
                return
            except Exception as exc:
                logger.warning(f"[RB_WAKE] Wake listener error, resubscribing: {exc}")
            finally:
                # Go dark and drop the socket BEFORE the backoff sleep, so no
                # viewer spends the gap believing it is being served.
                self._go_dark()
                if pubsub is not None:
                    try:
                        await pubsub.aclose()
                    except Exception:
                        pass
            if not self._stopping.is_set():
                await asyncio.sleep(_RETRY_BACKOFF_S)
