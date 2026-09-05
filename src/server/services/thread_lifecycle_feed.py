"""User-scoped thread lifecycle events: publish, subscribe, outbox executor.

One Redis channel per user (``user:events:{user_id}``) carries every thread's
lifecycle transitions to that user's open tabs. The channel is TRANSPORT, not
truth: ``run_settled`` rides the hook outbox (at-least-once, enqueued inside
the finalize transaction), and the feed endpoint's 30s DB reconcile backs it
up — every settle status is representable in the snapshot. The thread events
(title, pinned, deleted, archived, unarchived) are NOT: they have no snapshot
representation, so a dropped publish for those is repaired only by the
client's stream-cap reconnect, never by the reconcile.
"""

from __future__ import annotations

import json
import logging
from contextlib import asynccontextmanager
from datetime import datetime
from typing import Any, AsyncIterator, Dict, Optional

from src.server.services.thread_lifecycle import interrupt_reason_for
from src.server.services.workspace_status_pubsub import (
    ChannelWaitFn,
    subscribe_to_channel,
)
from src.utils.cache.redis_cache import get_cache_client

logger = logging.getLogger(__name__)

EVENT_VERSION = 1

# SSE event name the feed endpoint emits for every forwarded payload.
LIFECYCLE_EVENT = "thread_lifecycle"


def user_events_channel(user_id: str) -> str:
    return f"user:events:{user_id}"


def build_lifecycle_event(
    *,
    type: str,
    thread_id: str,
    workspace_id: Optional[str] = None,
    run_id: Optional[str] = None,
    run_seq: Optional[int] = None,
    status: Optional[str] = None,
    interrupt_reason: Optional[str] = None,
    title: Optional[str] = None,
    pinned: Optional[bool] = None,
    updated_at: Optional[str] = None,
) -> Dict[str, Any]:
    """The one wire shape (plan v6 §2.2). ``status`` is PUBLIC vocabulary."""
    event: Dict[str, Any] = {
        "v": EVENT_VERSION,
        "type": type,
        "thread_id": thread_id,
        "workspace_id": workspace_id,
        "run_id": run_id,
        "run_seq": run_seq,
        "status": status,
        "interrupt_reason": interrupt_reason_for(status or "", interrupt_reason),
    }
    if title is not None:
        event["title"] = title
    if pinned is not None:
        event["pinned"] = pinned
    if updated_at is not None:
        event["updated_at"] = updated_at
    return event


async def publish_user_event_strict(user_id: str, event: Dict[str, Any]) -> None:
    """Raising publisher — the outbox executor's transport.

    A Redis failure must SURFACE so the drainer's nack/backoff owns the retry;
    a swallowing publisher would ack-and-discard the settle event. Redis
    disabled by config is a clean no-op (nothing to retry into).
    """
    cache = get_cache_client()
    if not cache.enabled or not cache.client:
        return
    await cache.client.publish(user_events_channel(user_id), json.dumps(event))


async def publish_user_event(
    user_id: Optional[str], event: Dict[str, Any]
) -> None:
    """Best-effort publisher for advisory sites (start, title, delete)."""
    if not user_id:
        return
    try:
        await publish_user_event_strict(user_id, event)
    except Exception as exc:
        logger.debug(
            "user-feed publish failed (type=%s thread=%s): %s",
            event.get("type"),
            event.get("thread_id"),
            exc,
        )


async def publish_run_started(
    *,
    user_id: Optional[str],
    thread_id: str,
    workspace_id: Optional[str],
    run_id: str,
    run_seq: Optional[int],
) -> None:
    await publish_user_event(
        user_id,
        build_lifecycle_event(
            type="run_started",
            thread_id=thread_id,
            workspace_id=workspace_id,
            run_id=run_id,
            run_seq=run_seq,
            status="running",
        ),
    )


async def publish_thread_title(
    *,
    user_id: Optional[str],
    thread_id: str,
    workspace_id: Optional[str],
    title: str,
    updated_at: Optional[datetime | str] = None,
) -> None:
    """``updated_at`` is the title write's row timestamp — the event's version.

    Clients drop a title event not newer than their cached row, so a delayed
    generated-title event can't overwrite a manual rename that beat it.
    """
    await publish_user_event(
        user_id,
        build_lifecycle_event(
            type="thread_title",
            thread_id=thread_id,
            workspace_id=workspace_id,
            title=title,
            updated_at=(
                updated_at.isoformat()
                if isinstance(updated_at, datetime)
                else updated_at
            ),
        ),
    )


async def publish_thread_pinned(
    *,
    user_id: Optional[str],
    thread_id: str,
    workspace_id: Optional[str],
    pinned: bool,
) -> None:
    """Pin hint: pin state lives only in list rows (the snapshot frame carries
    no ``is_pinned``, and unchanged frames are suppressed), so without an event
    other tabs never learn of a pin until an unrelated refetch."""
    await publish_user_event(
        user_id,
        build_lifecycle_event(
            type="thread_pinned",
            thread_id=thread_id,
            workspace_id=workspace_id,
            pinned=pinned,
        ),
    )


async def publish_thread_deleted(
    *,
    user_id: Optional[str],
    thread_id: str,
    workspace_id: Optional[str],
) -> None:
    await publish_user_event(
        user_id,
        build_lifecycle_event(
            type="thread_deleted",
            thread_id=thread_id,
            workspace_id=workspace_id,
        ),
    )


async def publish_thread_archived(
    *,
    user_id: Optional[str],
    thread_id: str,
    workspace_id: Optional[str],
) -> None:
    """Prune hint: an archived thread leaves the snapshot's live/unseen sets.

    Advisory like every non-settle event — the archive's durable seen-stamp is
    what makes the row's later absence honest.
    """
    await publish_user_event(
        user_id,
        build_lifecycle_event(
            type="thread_archived",
            thread_id=thread_id,
            workspace_id=workspace_id,
        ),
    )


async def publish_thread_unarchived(
    *,
    user_id: Optional[str],
    thread_id: str,
    workspace_id: Optional[str],
) -> None:
    """Reappearance hint: the row must leave other tabs' archived views and
    rejoin their live lists (via the refetch the event triggers)."""
    await publish_user_event(
        user_id,
        build_lifecycle_event(
            type="thread_unarchived",
            thread_id=thread_id,
            workspace_id=workspace_id,
        ),
    )


# ---------------------------------------------------------------------------
# Outbox executor — the strict run_settled path
# ---------------------------------------------------------------------------


async def _exec_user_feed(job: Dict[str, Any]) -> None:
    """Publish a settled run's lifecycle event from its outbox payload.

    Raises on transport failure (nack → backoff → dead at 5) — acceptable
    because the durable list fields remain the correctness backstop; the
    event is latency, not truth.
    """
    payload = job.get("payload") or {}
    user_id = payload.get("user_id")
    thread_id = payload.get("thread_id") or job.get("conversation_thread_id")
    if not user_id or not thread_id:
        # Malformed payload: ack (retrying can't repair it) — but leave a
        # trace, or enqueue-site bugs surface only as silently missing events.
        logger.warning(
            "user_feed outbox job missing user_id/thread_id — dropping "
            "(job=%s run_id=%s)",
            job.get("hook_outbox_id"),
            payload.get("run_id") or job.get("run_id"),
        )
        return
    await publish_user_event_strict(
        user_id,
        build_lifecycle_event(
            type="run_settled",
            thread_id=str(thread_id),
            workspace_id=payload.get("workspace_id"),
            run_id=str(payload.get("run_id") or job.get("run_id")),
            run_seq=payload.get("run_seq"),
            status=payload.get("status"),
            interrupt_reason=payload.get("interrupt_reason"),
        ),
    )


def register_outbox_executors() -> None:
    """Bind the user_feed executor. Must run before the drainer starts."""
    from src.server.services.hook_outbox import register_hook_executor

    register_hook_executor("user_feed", _exec_user_feed)


# ---------------------------------------------------------------------------
# Subscription — the feed endpoint's transport
# ---------------------------------------------------------------------------

UserEventWaitFn = ChannelWaitFn


@asynccontextmanager
async def subscribe_to_user_events(
    user_id: str,
) -> AsyncIterator[Optional[UserEventWaitFn]]:
    """Subscribe to a user's event channel (see subscribe_to_channel)."""
    async with subscribe_to_channel(user_events_channel(user_id)) as wait:
        yield wait
