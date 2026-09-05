"""User thread-lifecycle feed: ``GET /api/v1/users/me/thread-events`` (SSE).

One connection per tab (mounted at the authenticated shell) carrying every
thread's lifecycle for the user. Frame contract (thread-lifecycle v6 §2.2):

- ``snapshot`` on attach and whenever a 30s-paced DB reconcile finds change:
  the UNCAPPED live+interrupted set (absence there is proof), the unseen set
  capped at the newest 256 by run_seq (``unseen_truncated`` +
  ``oldest_included_unseen_seq`` tell the client where absence stops proving
  anything), and ``as_of_seq`` (max user-visible run_seq at read time — the
  client's guard against stale frames). Both sets are classified in SQL by
  ``get_thread_lifecycle_rows``; archived threads are absent by construction.
- ``thread_lifecycle`` for each forwarded Redis event (advisory hints — the
  client's seq-aware merge owns correctness).

Redis is transport only: a runtime pub/sub failure abandons the subscription
and degrades to the 30s DB cadence with ≥30s-paced resubscribe attempts —
never a 1 Hz error loop.

"Snapshot on attach" is best-effort, not guaranteed: a DB read that raises
degrades to a keepalive rather than killing the stream. The attach read has no
"next reconcile tick" of its own, but a failure leaves ``last_snapshot`` unset,
so the loop's post-subscribe read re-emits and the contract self-heals.
"""

import asyncio
import json
import logging
import time

from fastapi import APIRouter
from fastapi.responses import StreamingResponse

from src.server.database.conversation import get_thread_lifecycle_rows
from src.server.services.thread_lifecycle import project_lifecycle
from src.server.services.thread_lifecycle_feed import (
    LIFECYCLE_EVENT,
    subscribe_to_user_events,
)
from src.server.utils.api import CurrentUserId

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1/users/me", tags=["Thread lifecycle"])

_SSE_HEADERS = {
    "Cache-Control": "no-cache",
    "X-Accel-Buffering": "no",
    "Connection": "keep-alive",
}
_KEEPALIVE_S = 30.0
_RECONCILE_S = 30.0
_MAX_DURATION_S = 600.0
_UNSEEN_CAP = 256
_PING = ": ping\n\n"


async def _read_snapshot(user_id: str) -> dict:
    rows, as_of_seq = await get_thread_lifecycle_rows(
        user_id, unseen_cap=_UNSEEN_CAP
    )
    live: list[dict] = []
    unseen: list[dict] = []
    for r in rows:
        fields = project_lifecycle(r)
        entry = {
            "thread_id": str(r["conversation_thread_id"]),
            "workspace_id": str(r["workspace_id"]),
            "run_id": fields["latest_run_id"],
            "run_seq": fields["latest_run_seq"],
            "status": fields["run_status"],
            "interrupt_reason": fields["interrupt_reason"],
            "last_seen_run_seq": fields["last_seen_run_seq"],
        }
        (live if r["branch"] == "live" else unseen).append(entry)

    # SQL already capped the unseen branch at _UNSEEN_CAP + 1, newest first;
    # re-sort because UNION ALL guarantees no ordering across the branches.
    unseen.sort(key=lambda e: e["run_seq"], reverse=True)
    truncated = len(unseen) > _UNSEEN_CAP
    if truncated:
        unseen = unseen[:_UNSEEN_CAP]
    return {
        "v": 1,
        "as_of_seq": as_of_seq,
        "unseen_truncated": truncated,
        # Absence of a terminal entry proves seen only at/above this cutoff;
        # 0 = the unseen set is complete, every absence proves.
        "oldest_included_unseen_seq": (
            unseen[-1]["run_seq"] if truncated else 0
        ),
        "live": live,
        "unseen": unseen,
    }


@router.get("/thread-events")
async def user_thread_events(x_user_id: CurrentUserId):
    """SSE feed of the user's thread lifecycle (see module docstring)."""
    user_id = x_user_id

    async def event_generator():
        started = time.monotonic()
        last_snapshot: str | None = None

        async def snapshot_frame(force: bool = False) -> str | None:
            """The next frame to emit: a snapshot, None when unchanged, or a
            keepalive when the read failed.

            A failed read must never kill the stream — it leaves
            ``last_snapshot`` untouched, so the next reconcile tick re-emits.
            """
            nonlocal last_snapshot
            try:
                frame = await _read_snapshot(user_id)
            except Exception as exc:
                logger.warning(
                    "thread-feed snapshot read failed (user=%s): %s",
                    user_id,
                    exc,
                )
                return _PING
            encoded = json.dumps(frame, sort_keys=True, default=str)
            if not force and encoded == last_snapshot:
                return None
            last_snapshot = encoded
            return f"event: snapshot\ndata: {json.dumps(frame, default=str)}\n\n"

        yield await snapshot_frame(force=True)

        while time.monotonic() - started < _MAX_DURATION_S:
            subscription_broke = False
            async with subscribe_to_user_events(user_id) as wait:
                if wait is not None:
                    # Gapless attach: a publish between the snapshot read and
                    # the SUBSCRIBE would otherwise be lost — re-read.
                    event = await snapshot_frame()
                    if event:
                        yield event
                    next_reconcile = time.monotonic() + _RECONCILE_S
                    while time.monotonic() - started < _MAX_DURATION_S:
                        timeout = min(
                            _KEEPALIVE_S,
                            max(0.5, next_reconcile - time.monotonic()),
                        )
                        kind, payload = await wait(timeout)
                        # Reconcile BEFORE the message fast-path: sustained
                        # sub-timeout arrivals (parallel subagent settles)
                        # would otherwise starve the 30s backstop — the very
                        # guarantee the outbox's no-dead-letter stance leans on.
                        if kind != "error" and time.monotonic() >= next_reconcile:
                            next_reconcile = time.monotonic() + _RECONCILE_S
                            event = await snapshot_frame()
                            if event:
                                yield event
                        if kind == "message":
                            yield (
                                f"event: {LIFECYCLE_EVENT}\n"
                                f"data: {json.dumps(payload)}\n\n"
                            )
                            continue
                        if kind == "error":
                            # Broken pub/sub connection: abandon it — the DB
                            # cadence below owns liveness until resubscribe.
                            subscription_broke = True
                            break
                        # Quiet interval: keepalive.
                        yield _PING
                    if not subscription_broke:
                        break  # duration cap reached while subscribed

            # No subscription (Redis disabled / subscribe failed / broke):
            # one DB-paced cycle, then retry the subscribe — attempts are
            # thereby paced at the reconcile interval, never a tight loop.
            remaining = _MAX_DURATION_S - (time.monotonic() - started)
            if remaining <= 0:
                break
            await asyncio.sleep(min(_RECONCILE_S, remaining))
            event = await snapshot_frame()
            if event:
                yield event
            yield _PING

        yield "event: timeout\ndata: {}\n\n"

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers=_SSE_HEADERS,
    )
