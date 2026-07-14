"""Cross-worker cancel nudge (v4 Phase 2.4e, plan item F5).

/cancel stamps durable intent on the run's ledger row; when the owning
executor lives in another worker process, this channel nudges it to
interrupt now. Purely a latency optimization: a lost or undelivered nudge
still converges, because the finalize CAS adopts 'cancelled' from the
durable intent whenever the run reaches any terminal.
"""

from __future__ import annotations

import asyncio
import json
import logging
from typing import Optional

logger = logging.getLogger(__name__)

CANCEL_CHANNEL = "turn:cancel"

# get_message poll window; also bounds how quickly stop() is observed.
_POLL_TIMEOUT_S = 5.0
_RETRY_BACKOFF_S = 2.0


async def publish_cancel_nudge(thread_id: str, run_id: Optional[str]) -> None:
    """Best-effort: never raises — the durable intent row is the truth."""
    from src.utils.cache.redis_cache import get_cache_client

    cache = get_cache_client()
    if not cache.enabled or not cache.client:
        return
    try:
        await cache.client.publish(
            CANCEL_CHANNEL, json.dumps({"thread_id": thread_id, "run_id": run_id})
        )
        logger.info(
            f"[cancel-nudge] published for thread={thread_id} run={run_id}"
        )
    except Exception as exc:
        logger.warning(f"[cancel-nudge] publish failed for {thread_id}: {exc}")


class TurnCancelListener:
    """Per-worker subscriber: signals the local executor when a /cancel that
    landed on another worker targets a run this process owns.

    Delivery is idempotent — ``BackgroundTaskManager.cancel_workflow`` on a
    run this worker doesn't own returns False and does nothing, so every
    worker (including the publisher's) may safely receive every nudge.
    """

    _instance: Optional["TurnCancelListener"] = None

    def __init__(self) -> None:
        self._task: Optional[asyncio.Task] = None
        self._stopping = asyncio.Event()

    @classmethod
    def get_instance(cls) -> "TurnCancelListener":
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance

    def start(self) -> None:
        if self._task is not None and not self._task.done():
            return
        self._stopping.clear()
        self._task = asyncio.create_task(self._run(), name="turn-cancel-listener")

    async def stop(self) -> None:
        self._stopping.set()
        if self._task is not None:
            self._task.cancel()
            try:
                await self._task
            except (asyncio.CancelledError, Exception):
                pass
            self._task = None

    async def _run(self) -> None:
        from src.server.services.workspace_status_pubsub import (
            get_shared_pubsub_client,
        )

        while not self._stopping.is_set():
            pubsub = None
            try:
                client = await get_shared_pubsub_client()
                if client is None:
                    # Redis disabled: nothing to listen on (single-worker /
                    # OSS deployments don't need the nudge either).
                    return
                pubsub = client.pubsub()
                await pubsub.subscribe(CANCEL_CHANNEL)
                logger.info("[cancel-nudge] listener subscribed")
                while not self._stopping.is_set():
                    msg = await pubsub.get_message(
                        ignore_subscribe_messages=True, timeout=_POLL_TIMEOUT_S
                    )
                    if not msg or msg.get("type") != "message":
                        continue
                    await self._handle(msg.get("data"))
            except asyncio.CancelledError:
                return
            except Exception as exc:
                logger.warning(
                    f"[cancel-nudge] listener error, resubscribing: {exc}"
                )
                await asyncio.sleep(_RETRY_BACKOFF_S)
            finally:
                if pubsub is not None:
                    try:
                        await pubsub.unsubscribe(CANCEL_CHANNEL)
                    except Exception:
                        pass
                    try:
                        await pubsub.aclose()
                    except Exception:
                        pass

    async def _handle(self, data) -> None:
        try:
            if isinstance(data, bytes):
                data = data.decode("utf-8")
            payload = json.loads(data)
            thread_id = payload.get("thread_id")
            run_id = payload.get("run_id")
            # run_id is required: an untargeted local cancel could hit a
            # NEWER run than the one the intent was stamped on.
            if not thread_id or not run_id:
                return
        except Exception:
            logger.warning("[cancel-nudge] undecodable payload dropped")
            return
        try:
            from src.server.services.background_task_manager import (
                BackgroundTaskManager,
            )

            handled = await BackgroundTaskManager.get_instance().cancel_workflow(
                thread_id, run_id
            )
            if handled:
                logger.info(
                    f"[cancel-nudge] local owner signalled for "
                    f"thread={thread_id} run={run_id}"
                )
        except Exception:
            logger.error(
                f"[cancel-nudge] local cancel dispatch failed for "
                f"thread={thread_id}",
                exc_info=True,
            )
