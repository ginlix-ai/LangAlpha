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
    ) -> bool:
        """
        Helper to update workflow status with metadata preservation.

        When ``run_id`` is provided, the update is skipped if the
        stored blob's ``run_id`` doesn't match — prevents a late terminal
        from run A overwriting active status set by run B.
        """
        try:
            key = f"{self.STATUS_PREFIX}{thread_id}"

            # Get existing status to preserve metadata
            existing = await self.cache.get(key)
            if not existing:
                existing = {
                    "thread_id": thread_id,
                    "started_at": datetime.now().isoformat()
                }

            if run_id is not None:
                stored_run_id = existing.get("run_id")
                if stored_run_id is not None and stored_run_id != run_id:
                    logger.debug(
                        f"[WorkflowTracker] Skipping {new_status} update for "
                        f"thread_id={thread_id}: stored run_id={stored_run_id} "
                        f"!= expected={run_id}"
                    )
                    return False

            # Update status and timestamp
            existing["status"] = new_status
            existing[timestamp_field] = datetime.now().isoformat()
            existing["last_update"] = datetime.now().isoformat()

            # Merge metadata if provided
            if metadata:
                existing_meta = existing.get("metadata", {})
                existing_meta.update(metadata)
                existing["metadata"] = existing_meta

            # Save with optional TTL
            success = await self.cache.set(key, existing, ttl=ttl)
            return success

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
    ) -> bool:
        """
        Mark workflow as active (currently executing with connection).

        Args:
            thread_id: Thread/workflow identifier
            workspace_id: Workspace identifier
            user_id: User identifier
            metadata: Optional additional metadata
            run_id: Current turn's LangGraph run_id (== conversation_response_id)

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
    ) -> bool:
        """
        Mark workflow as completed (finished executing).

        Sets TTL per redis.ttl.workflow_status config (keeps brief history).
        Pass ``run_id`` to no-op the write when the active run has
        already advanced to a different turn.
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
