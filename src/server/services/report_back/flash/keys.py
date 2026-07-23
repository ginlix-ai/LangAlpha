"""Redis key builders for the concurrent PTC report-back system.

One home for every report-back key so a typo in a raw f-string can't silently
break the cross-coroutine coordination. Pure functions, zero imports — safe to
import from any layer without cycles.
"""

from __future__ import annotations


def decode(value) -> str:
    """Normalize a Redis reply that may arrive as bytes."""
    return value.decode() if isinstance(value, (bytes, bytearray)) else value


def flash_watch_key(flash_thread_id: str) -> str:
    """Redis SET of PTC thread ids dispatched from this flash thread, pending report-back."""
    return f"flash_watch:{flash_thread_id}"


def flash_rb_run_key(flash_thread_id: str, ptc_thread_id: str) -> str:
    """Redis key holding the report-back flash run_id for one (flash, ptc) pair."""
    return f"flash_rb_run:{flash_thread_id}:{ptc_thread_id}"


def flash_rb_done_key(flash_thread_id: str) -> str:
    """Redis LIST of recently drained report-back run ids, newest first (bounded + TTL'd)."""
    return f"flash_rb_done:{flash_thread_id}"


def flash_user_pending_key(user_id: str) -> str:
    """Per-user SET of pending dispatched PTC thread ids, gating the per-user cap."""
    return f"flash_user_pending:{user_id}"


def ptc_origin_key(ptc_thread_id: str) -> str:
    """Dispatch origin metadata (flash thread, user, workspaces, report_back flag) for a PTC thread."""
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


def thread_wake_key(flash_thread_id: str) -> str:
    """Pub/sub channel an in-session client subscribes to for report-back wake nudges."""
    return f"thread:wake:{flash_thread_id}"


# TTL for report-back Redis state (run pointers, watch SET, queue); 24h.
FLASH_RB_RUN_TTL = 86400

# TTL for the recently-drained run-id list (flash_rb_done); 15 min.
FLASH_RB_DONE_TTL = 900

# Max recently-drained run ids kept per flash thread (LTRIM bound).
FLASH_RB_DONE_MAX = 10

# TTL for ptc_origin and flash_watch / flash_user_pending Redis keys (24 hours).
PTC_ORIGIN_TTL = 86400

# TTL for a teardown tombstone (a clear fenced out by a provisional dispatch
# generation). It only needs to outlive that provisional reserve's
# commit-or-rollback window (seconds); an hour is comfortably beyond it.
TEARDOWN_TOMBSTONE_TTL = 3600
