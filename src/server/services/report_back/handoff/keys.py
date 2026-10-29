"""Redis key builders for the concurrent PTC report-back system.

One home for every report-back key so a typo in a raw f-string can't silently
break the cross-coroutine coordination. Pure functions, zero imports — safe to
import from any layer without cycles.
"""

from __future__ import annotations

# The ORIGIN_* names are the ``ptc_origin`` record's kind and the fields naming
# the thread that handed the work off and its workspace; ORIGIN_THREAD_META is
# the run metadata key (and chat request field) naming the thread a dispatched
# run reports back to. State an earlier build left under the retired Flash
# agent's names is renamed to these at startup (``retired_names``).
WATCH_PREFIX = "handoff_watch:"
RB_RUN_PREFIX = "handoff_rb_run:"
RB_DONE_PREFIX = "handoff_rb_done:"
USER_PENDING_PREFIX = "handoff_user_pending:"
ORIGIN_KIND = "home"
ORIGIN_THREAD_FIELD = "home_thread_id"
ORIGIN_WORKSPACE_FIELD = "home_workspace_id"
ORIGIN_THREAD_META = "origin_home_thread_id"


def decode(value) -> str:
    """Normalize a Redis reply that may arrive as bytes."""
    return value.decode() if isinstance(value, (bytes, bytearray)) else value


def watch_key(dispatcher_thread_id: str) -> str:
    """Redis SET of PTC thread ids dispatched from this thread, pending report-back."""
    return f"{WATCH_PREFIX}{dispatcher_thread_id}"


def rb_run_key(dispatcher_thread_id: str, ptc_thread_id: str) -> str:
    """Redis key holding the report-back run_id for one (dispatcher, ptc) pair."""
    return f"{RB_RUN_PREFIX}{dispatcher_thread_id}:{ptc_thread_id}"


def rb_done_key(dispatcher_thread_id: str) -> str:
    """Redis LIST of recently drained report-back run ids, newest first (bounded + TTL'd)."""
    return f"{RB_DONE_PREFIX}{dispatcher_thread_id}"


def user_pending_key(user_id: str) -> str:
    """Per-user SET of pending dispatched PTC thread ids, gating the per-user cap."""
    return f"{USER_PENDING_PREFIX}{user_id}"


def ptc_origin_key(ptc_thread_id: str) -> str:
    """Dispatch origin metadata (dispatching thread, user, workspaces, report_back flag) for a PTC thread."""
    return f"ptc_origin:{ptc_thread_id}"


def ptc_teardown_tombstone_key(ptc_thread_id: str) -> str:
    """Records a teardown fenced out by a provisional (uncommitted) dispatch
    generation, so that generation's rollback honors it instead of resurrecting
    the already-torn-down predecessor."""
    return f"ptc_rb_tombstone:{ptc_thread_id}"


def ptc_rb_resolved_key(ptc_thread_id: str) -> str:
    """Redis SET of dispatch generations resolved as phantom (never admitted,
    fence honored no further): the admission marker write refuses these, so
    resolution vs late admission is a race exactly one side can win."""
    return f"ptc_rb_resolved:{ptc_thread_id}"


THREAD_WAKE_PREFIX = "thread:wake:"


def thread_wake_key(thread_id: str) -> str:
    """Pub/sub channel an in-session client subscribes to for report-back wake nudges."""
    return f"{THREAD_WAKE_PREFIX}{thread_id}"


def thread_wake_pattern() -> str:
    """Glob for the one per-worker PSUBSCRIBE that feeds every open watch."""
    return f"{THREAD_WAKE_PREFIX}*"


def parse_thread_wake_key(channel: str) -> str | None:
    """Thread id a wake channel addresses, or None when it is not one.

    Pattern-subscribing puts routing in this function's hands: under
    per-channel SUBSCRIBE, delivering one thread's wake to another viewer was
    structurally impossible, and now it is one parser bug away. So the match
    is exact and the remainder is returned verbatim — callers route by string
    equality against the id the subscriber attached with, which is byte-for-byte
    what the publisher built the channel from.
    """
    if not channel.startswith(THREAD_WAKE_PREFIX):
        return None
    return channel[len(THREAD_WAKE_PREFIX) :] or None


# TTL for report-back Redis state (run pointers, watch SET, queue); 24h.
RB_RUN_TTL = 86400

# TTL for the recently-drained run-id list (``rb_done_key``); 15 min.
RB_DONE_TTL = 900

# Max recently-drained run ids kept per dispatching thread (LTRIM bound).
RB_DONE_MAX = 10

# TTL for ptc_origin and the watch / user-pending Redis keys (24 hours).
PTC_ORIGIN_TTL = 86400

# TTL for a teardown tombstone (a clear fenced out by a provisional dispatch
# generation). It only needs to outlive that provisional reserve's
# commit-or-rollback window (seconds); an hour is comfortably beyond it.
TEARDOWN_TOMBSTONE_TTL = 3600
