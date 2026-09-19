"""Shared workspace and computer SSE status streams.

One loop keeps cancellation, fallback, and busy-spin protections consistent
between both routes.
"""

from __future__ import annotations

import asyncio
import json
import time
from contextlib import AbstractAsyncContextManager
from typing import (
    Any,
    AsyncIterator,
    Awaitable,
    Callable,
    Dict,
    Optional,
)

SSE_HEADERS = {
    "Cache-Control": "no-cache",
    "X-Accel-Buffering": "no",
    "Connection": "keep-alive",
}
EVENTS_KEEPALIVE_S = 30.0
EVENTS_MAX_DURATION_S = 600.0
EVENTS_TERMINAL = {"running", "error", "deleted"}

# None means the subject row is gone.
ReadStatusFn = Callable[[], Awaitable[Optional[str]]]
# Frame callback accepts status, sandbox_state=None, and error=None.
FrameFn = Callable[..., str]
SubscribeFn = Callable[[], AbstractAsyncContextManager[Any]]


def sse_status_event(
    subject: Dict[str, Optional[str]],
    status: str,
    sandbox_state: Optional[str] = None,
    error: Optional[str] = None,
) -> str:
    """subject carries ids only, never status."""
    data: Dict[str, Optional[str]] = {**subject, "status": status}
    if sandbox_state:
        data["sandbox_state"] = sandbox_state
    if error:
        data["error"] = error
    return f"event: status\ndata: {json.dumps(data)}\n\n"


async def status_event_stream(
    *,
    initial_status: str,
    read_status: ReadStatusFn,
    subscribe: SubscribeFn,
    frame: FrameFn,
) -> AsyncIterator[str]:
    """Fall back to keepalive-paced DB polling when Redis fails so no sidecar poller is needed."""
    started = time.monotonic()
    last_status = initial_status
    last_sandbox_state: str | None = None

    async def reconcile() -> tuple[str | None, bool]:
        nonlocal last_status
        status = await read_status()
        if status is None:
            return None, True
        if status == last_status:
            return None, False
        last_status = status
        return frame(last_status), last_status in EVENTS_TERMINAL

    yield frame(last_status)
    if last_status in EVENTS_TERMINAL:
        return

    while time.monotonic() - started < EVENTS_MAX_DURATION_S:
        subscription_broke = False
        async with subscribe() as wait_for_notify:
            if wait_for_notify is not None:
                # Re-read after SUBSCRIBE to catch publishes between the initial read and subscription.
                event, close = await reconcile()
                if event:
                    yield event
                if close:
                    return

                while time.monotonic() - started < EVENTS_MAX_DURATION_S:
                    kind, payload = await wait_for_notify(EVENTS_KEEPALIVE_S)
                    if kind == "error":
                        # Broken waits return immediately; abandon them to avoid busy-spinning DB reads.
                        subscription_broke = True
                        break
                    if payload is None:
                        # Reconcile on keepalive to recover missed publishes.
                        event, close = await reconcile()
                        if event:
                            yield event
                        if close:
                            return
                        yield ": ping\n\n"
                        continue
                    # Sub-state hints are not persisted; forward them for the slow-restore spinner
                    # even when a background warm owns the start.
                    sandbox_state = payload.get("sandbox_state")
                    hinted = payload.get("status")
                    # Use the payload status: starting may publish before commit while DB says stopped.
                    # Pairing archived with stopped loses the spinner, and reconciliation has no hint.
                    event_status = hinted if isinstance(hinted, str) else last_status
                    if (
                        sandbox_state
                        and sandbox_state != last_sandbox_state
                        and event_status not in EVENTS_TERMINAL
                    ):
                        # Only reconcile owns terminal transitions; guard the status actually emitted.
                        last_sandbox_state = sandbox_state
                        yield frame(event_status, sandbox_state=sandbox_state)
                    # A failed background start reverts to stopped, so DB confirmation hides failure.
                    # Only its error-bearing publish carries the reason and can end the wait.
                    failure = payload.get("error")
                    if hinted == "error" and isinstance(failure, str) and failure:
                        yield frame("error", error=failure)
                        return
                    # Confirm hints in DB: they may precede commit, be spurious, or name a sibling;
                    # trusting a terminal hint would close the stream incorrectly.
                    if not hinted or hinted == last_status:
                        continue
                    event, close = await reconcile()
                    if event:
                        yield event
                    if close:
                        return
                if not subscription_broke:
                    break

        # Pace DB fallback and resubscribe attempts at keepalive intervals to avoid a tight loop.
        remaining = EVENTS_MAX_DURATION_S - (time.monotonic() - started)
        if remaining <= 0:
            break
        await asyncio.sleep(min(EVENTS_KEEPALIVE_S, remaining))
        event, close = await reconcile()
        if event:
            yield event
        if close:
            return
        yield ": ping\n\n"

    yield "event: timeout\ndata: {}\n\n"
