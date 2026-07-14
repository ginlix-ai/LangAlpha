"""
Workflow Background Tracking

Manages workflow execution state in Redis to support background execution
and reconnection after client disconnect.

Key Features:
- Track workflow status (active/completed/cancelled/failed)
- TTL-based cleanup of completed workflows
- Graceful degradation if Redis unavailable
- Retry count tracking for transient error handling (max 3 retries)
"""

import json
import logging
from datetime import datetime
from typing import Optional, Dict, Any
from enum import Enum

from src.utils.cache.redis_cache import get_cache_client
from src.config.settings import get_redis_ttl_workflow_status

logger = logging.getLogger(__name__)


# Atomic read-gate-merge-write for status transitions. The pre-v4 Python
# read-modify-write let a stale terminal writer (run G1) pass the run_id
# check before a newer run G2's mark_active and then clobber G2's blob —
# erasing the admission stamp the dispatch oracle relies on. The gate and
# the write must be one Redis-side step.
#
# Blob constraint: because the blob round-trips through Lua cjson here,
# metadata values (from mark_active onward) must stay flat scalars —
# cjson does not preserve the empty-array/object distinction and floats
# transit through Lua-number precision. Deep/collection metadata needs a
# schema decision first (or the CAS identity moved off the JSON blob).
# KEYS: 1=status key
# ARGV: 1=expected run_id ('' = ungated) 2=patch JSON (metadata merged,
#       other fields replaced) 3=ttl seconds 4=thread_id 5=started_at
#       6=expected stored status ('' = ungated) — a healer-style writer
#         gates on the exact state it observed, so a same-run transition
#         (ACTIVE→INTERRUPTED) between its read and this write refuses the
#         CAS instead of being overwritten. An absent blob has no status
#         and refuses any status-gated write.
_CAS_STATUS_LUA = """
local t
local v = redis.call('get', KEYS[1])
if v then
  local ok, dec = pcall(cjson.decode, v)
  if ok and type(dec) == 'table' then t = dec end
end
if t == nil then
  t = {}
  t['thread_id'] = ARGV[4]
  t['started_at'] = ARGV[5]
end
local rid = t['run_id']
if rid == cjson.null then rid = nil end
if ARGV[1] ~= '' and rid ~= nil and rid ~= ARGV[1] then
  return 0
end
if ARGV[6] ~= nil and ARGV[6] ~= '' and t['status'] ~= ARGV[6] then
  return 0
end
local patch = cjson.decode(ARGV[2])
for k, pv in pairs(patch) do
  if k == 'metadata' then
    local m = t['metadata']
    if type(m) ~= 'table' then m = {} end
    for mk, mv in pairs(pv) do m[mk] = mv end
    t['metadata'] = m
  else
    t[k] = pv
  end
end
redis.call('set', KEYS[1], cjson.encode(t), 'EX', tonumber(ARGV[3]))
return 1
"""


# Admission marker write, refused for phantom-receipted generations: the
# orphan resolver receipts a generation it resolved as never-admitted
# (memberships dropped, pending state cleared client-side) — if that
# generation's own HTTP admission then landed, it would run with its watch
# state already erased and its report-back silently dropped. Receipt check
# and marker write are ONE Redis-side step, making resolution vs late
# admission a race exactly one side can win.
# The admission is ALSO stamped as ``admitted_gen`` on the exact-gen origin
# blob (KEEPTTL — the origin's lifetime is the identity's lifetime): the
# marker's terminal TTL (1h) is 23h shorter than the origin's, so the
# marker alone would let an admitted-and-finished generation read as
# phantom to the resolver once its marker expired. A moved/absent origin
# skips the stamp — that lifecycle isn't ours to write.
# KEYS: 1=status key 2=resolved-receipt set 3=origin key
# ARGV: 1=status blob JSON 2=this dispatch generation 3=ttl seconds
_ADMISSION_MARK_LUA = """
if redis.call('sismember', KEYS[2], ARGV[2]) == 1 then return 0 end
redis.call('set', KEYS[1], ARGV[1], 'EX', tonumber(ARGV[3]))
local o = redis.call('get', KEYS[3])
if o then
  local ok, origin = pcall(cjson.decode, o)
  if ok and type(origin) == 'table' and origin['dispatch_gen'] == ARGV[2] then
    origin['admitted_gen'] = ARGV[2]
    redis.call('set', KEYS[3], cjson.encode(origin), 'KEEPTTL')
  end
end
return 1
"""


class WorkflowStatus(str, Enum):
    """Workflow execution status."""
    ACTIVE = "active"
    COMPLETED = "completed"
    INTERRUPTED = "interrupted"
    CANCELLED = "cancelled"
    FAILED = "failed"
    UNKNOWN = "unknown"


# Terminal states — no further transitions, ``can_reconnect`` returns False.
# Adding a new terminal state requires wiring it into
# ``BackgroundTaskManager``'s corresponding ``_mark_*`` method so Postgres +
# Redis stay in sync. ``test_terminal_disjoint_from_reconnectable`` pins the
# invariant against ``RECONNECTABLE_STATUSES``.
TERMINAL_STATUSES: frozenset[WorkflowStatus] = frozenset({
    WorkflowStatus.COMPLETED,
    WorkflowStatus.CANCELLED,
    WorkflowStatus.FAILED,
})


# Statuses for which a client may reconnect to a live SSE stream. Source of
# truth for ``workflow_handler.get_workflow_status``'s ``can_reconnect``
# decision; must stay disjoint with ``TERMINAL_STATUSES``.
RECONNECTABLE_STATUSES: frozenset[WorkflowStatus] = frozenset({
    WorkflowStatus.ACTIVE,
})


class WorkflowTracker:
    """
    Tracks workflow execution state in Redis.

    Uses Redis for lightweight tracking with TTL-based cleanup.
    Gracefully degrades if Redis is unavailable.

    Redis Key Structure:
    - workflow:status:{thread_id} -> JSON status object (TTL: redis.ttl.workflow_status)

    v4: the Redis cancel flag and retry-count tracking are gone — cancel
    intent is durable on the run row (cancel_requested_at) and retry counts
    come from the attempt chain (attempt_no).
    """

    # Singleton instance
    _instance: Optional['WorkflowTracker'] = None

    # Redis key prefixes
    STATUS_PREFIX = "workflow:status:"

    def __init__(self):
        """Initialize workflow tracker with Redis client."""
        self.cache = get_cache_client()
        self.enabled = self.cache.enabled

        if not self.enabled:
            logger.warning(
                "WorkflowTracker: Redis unavailable, running in degraded mode. "
                "Background tracking disabled."
            )

    @classmethod
    def get_instance(cls) -> 'WorkflowTracker':
        """
        Get singleton instance of WorkflowTracker.

        Returns:
            WorkflowTracker instance
        """
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance

    async def _update_status_with_metadata(
        self,
        thread_id: str,
        new_status: WorkflowStatus,
        timestamp_field: str,
        metadata: Optional[Dict[str, Any]] = None,
        ttl: Optional[int] = None,
        run_id: Optional[str] = None,
        expected_status: Optional[WorkflowStatus] = None,
    ) -> bool:
        """
        Helper to update workflow status with metadata preservation.

        When ``run_id`` is provided, the update is skipped if the stored
        blob's ``run_id`` doesn't match; ``expected_status`` additionally
        gates on the exact stored status the caller observed (healer-style
        writers — a same-run ACTIVE→terminal transition between read and
        write must refuse, run_id alone can't see it). Gate + merge + write
        run as ONE Lua eval — a separate read-then-write let a stale
        terminal writer pass the gate before a newer run's mark_active and
        then clobber its blob.
        """
        try:
            key = f"{self.STATUS_PREFIX}{thread_id}"
            now = datetime.now().isoformat()
            patch: Dict[str, Any] = {
                "status": new_status,
                timestamp_field: now,
                "last_update": now,
            }
            if metadata:
                patch["metadata"] = metadata

            from src.utils.cache.redis_cache import SAFETY_TTL

            res = await self.cache.client.eval(
                _CAS_STATUS_LUA,
                1,
                key,
                run_id or "",
                json.dumps(patch, ensure_ascii=False),
                ttl if ttl and ttl > 0 else SAFETY_TTL,
                thread_id,
                now,
                expected_status.value if expected_status else "",
            )
            if not int(res):
                logger.debug(
                    f"[WorkflowTracker] Skipping {new_status} update for "
                    f"thread_id={thread_id}: stored run_id != {run_id}"
                )
                return False
            return True

        except Exception as e:
            logger.error(
                f"[WorkflowTracker] Error updating status for {thread_id}: {e}"
            )
            return False

    async def mark_active(
        self,
        thread_id: str,
        workspace_id: str,
        user_id: str,
        metadata: Optional[Dict[str, Any]] = None,
        run_id: Optional[str] = None,
        refuse_receipt_key: Optional[str] = None,
        receipt_member: Optional[str] = None,
    ) -> bool:
        """
        Mark workflow as active (currently executing with connection).

        Args:
            thread_id: Thread/workflow identifier
            workspace_id: Workspace identifier
            user_id: User identifier
            metadata: Optional additional metadata
            run_id: Current turn's LangGraph run_id (== conversation_response_id)
            refuse_receipt_key: Redis SET of phantom-resolved dispatch
                generations; when set (with receipt_member), the marker
                write atomically refuses if the member was receipted, and
                stamps ``admitted_gen`` on the exact-gen origin blob
            receipt_member: this run's dispatch generation to check

        Returns:
            True if successfully marked, False otherwise
        """
        if not self.enabled:
            return False

        try:
            key = f"{self.STATUS_PREFIX}{thread_id}"
            status_obj = {
                "status": WorkflowStatus.ACTIVE,
                "thread_id": thread_id,
                "workspace_id": workspace_id,
                "user_id": user_id,
                "run_id": run_id,
                "started_at": datetime.now().isoformat(),
                "last_update": datetime.now().isoformat(),
                "metadata": metadata or {}
            }

            # Cleaned up on completion; the cache client's safety TTL (7d)
            # backstops a crash that skips the cleanup.
            if refuse_receipt_key and receipt_member:
                from src.server.handlers.chat.report_back_keys import (
                    ptc_origin_key,
                )
                from src.utils.cache.redis_cache import SAFETY_TTL

                admitted = await self.cache.client.eval(
                    _ADMISSION_MARK_LUA,
                    3,
                    key,
                    refuse_receipt_key,
                    ptc_origin_key(thread_id),
                    json.dumps(status_obj, ensure_ascii=False),
                    receipt_member,
                    SAFETY_TTL,
                )
                if not int(admitted):
                    logger.warning(
                        f"[WorkflowTracker] Refusing admission for "
                        f"{thread_id}: generation {receipt_member} was "
                        f"already resolved as phantom"
                    )
                    return False
                success = True
            else:
                success = await self.cache.set(key, status_obj)

            if success:
                logger.debug(f"[WorkflowTracker] Marked workflow as active: {thread_id}")

            return success

        except Exception as e:
            logger.error(f"[WorkflowTracker] Error marking active: {e}")
            return False

    async def mark_completed(
        self,
        thread_id: str,
        metadata: Optional[Dict[str, Any]] = None,
        run_id: Optional[str] = None,
        expected_status: Optional[WorkflowStatus] = None,
    ) -> bool:
        """
        Mark workflow as completed (finished executing).

        Sets TTL per redis.ttl.workflow_status config (keeps brief history).
        Pass ``run_id`` to no-op the write when the active run has
        already advanced to a different turn; ``expected_status`` to
        additionally no-op when the stored status left the state the
        caller observed (healer-style CAS).
        """
        if not self.enabled:
            return False

        ttl = get_redis_ttl_workflow_status()
        success = await self._update_status_with_metadata(
            thread_id=thread_id,
            new_status=WorkflowStatus.COMPLETED,
            timestamp_field="completed_at",
            metadata=metadata,
            ttl=ttl,
            run_id=run_id,
            expected_status=expected_status,
        )

        if success:
            logger.debug(
                f"[WorkflowTracker] Marked workflow as completed: {thread_id} "
                f"(TTL: {ttl}s)"
            )

        return success

    async def mark_interrupted(
        self,
        thread_id: str,
        metadata: Optional[Dict[str, Any]] = None,
        run_id: Optional[str] = None,
    ) -> bool:
        """
        Mark workflow as interrupted (paused for human-in-the-loop review).

        The workflow is waiting for user input (e.g., plan approval) and is
        NOT actively streaming. Uses the same TTL as completed workflows.

        When `run_id` is supplied, the write is skipped if the stored status
        belongs to a different run — prevents a stale HITL interrupt from
        clobbering a newer turn's ACTIVE status.
        """
        if not self.enabled:
            return False

        success = await self._update_status_with_metadata(
            thread_id=thread_id,
            new_status=WorkflowStatus.INTERRUPTED,
            timestamp_field="interrupted_at",
            metadata=metadata,
            ttl=None,  # No TTL - workflow can be resumed at any time
            run_id=run_id,
        )

        if success:
            logger.info(
                f"[WorkflowTracker] Marked workflow as interrupted: {thread_id}"
            )

        return success

    async def mark_cancelled(
        self,
        thread_id: str,
        run_id: Optional[str] = None,
    ) -> bool:
        """Mark workflow as cancelled (explicitly stopped by user)."""
        if not self.enabled:
            return False

        success = await self._update_status_with_metadata(
            thread_id=thread_id,
            new_status=WorkflowStatus.CANCELLED,
            timestamp_field="cancelled_at",
            metadata=None,
            ttl=get_redis_ttl_workflow_status(),
            run_id=run_id,
        )

        if success:
            logger.info(
                f"[WorkflowTracker] Marked workflow as cancelled: {thread_id}"
            )

        return success

    async def mark_failed(
        self,
        thread_id: str,
        error: Optional[str] = None,
        run_id: Optional[str] = None,
    ) -> bool:
        """Mark workflow as failed (uncaught exception or unrecoverable error)."""
        if not self.enabled:
            return False

        success = await self._update_status_with_metadata(
            thread_id=thread_id,
            new_status=WorkflowStatus.FAILED,
            timestamp_field="failed_at",
            metadata={"error": error} if error else None,
            ttl=get_redis_ttl_workflow_status(),
            run_id=run_id,
        )

        if success:
            logger.info(
                f"[WorkflowTracker] Marked workflow as failed: {thread_id}"
            )

        return success

    async def get_status(self, thread_id: str) -> Optional[Dict[str, Any]]:
        """
        Get current workflow status.

        Args:
            thread_id: Thread/workflow identifier

        Returns:
            Status object or None if not found
        """
        if not self.enabled:
            return None

        try:
            key = f"{self.STATUS_PREFIX}{thread_id}"
            status = await self.cache.get(key)

            if status:
                logger.debug(
                    f"[WorkflowTracker] Retrieved status for {thread_id}: "
                    f"{status.get('status')}"
                )

            return status

        except Exception as e:
            logger.error(f"[WorkflowTracker] Error getting status: {e}")
            return None

    async def get_statuses(
        self, thread_ids: list[str]
    ) -> Dict[str, Dict[str, Any]]:
        """Batch status read for many threads in one MGET.

        Returns ``{thread_id: status_blob}`` for the keys that exist; missing or
        undecodable keys are omitted. Powers the batched liveness endpoint so N
        dispatch cards cost one round-trip, not N.
        """
        if not self.enabled or not self.cache.client or not thread_ids:
            return {}
        try:
            keys = [f"{self.STATUS_PREFIX}{tid}" for tid in thread_ids]
            raws = await self.cache.client.mget(keys)
            out: Dict[str, Dict[str, Any]] = {}
            for tid, raw in zip(thread_ids, raws):
                if raw is None:
                    continue
                try:
                    blob = json.loads(raw)
                except (TypeError, ValueError):
                    continue
                if isinstance(blob, dict):
                    out[tid] = blob
            return out
        except Exception as e:
            logger.error(f"[WorkflowTracker] Error getting statuses: {e}")
            return {}

    async def delete_status(self, thread_id: str) -> bool:
        """
        Delete workflow status (manual cleanup).

        Args:
            thread_id: Thread/workflow identifier

        Returns:
            True if deleted, False otherwise
        """
        if not self.enabled:
            return False

        try:
            status_key = f"{self.STATUS_PREFIX}{thread_id}"
            status_deleted = await self.cache.delete(status_key)

            if status_deleted:
                logger.info(f"[WorkflowTracker] Deleted status: {thread_id}")

            return status_deleted

        except Exception as e:
            logger.error(f"[WorkflowTracker] Error deleting status: {e}")
            return False
