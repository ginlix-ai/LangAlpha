"""
Workflow Background Tracking

Manages workflow execution state in Redis to support background execution
and reconnection after client disconnect.

Key Features:
- Track workflow status (active/disconnected/completed/cancelled)
- Detect explicit user cancellation vs accidental disconnect
- TTL-based cleanup of completed workflows
- Graceful degradation if Redis unavailable
- Retry count tracking for transient error handling (max 3 retries)
"""

import logging
from datetime import datetime
from typing import Optional, Dict, Any
from enum import Enum

from src.utils.cache.redis_cache import get_cache_client
from src.config.settings import get_redis_ttl_workflow_status, get_redis_ttl_cancel_flag

logger = logging.getLogger(__name__)


class WorkflowStatus(str, Enum):
    """Workflow execution status."""
    ACTIVE = "active"
    DISCONNECTED = "disconnected"
    COMPLETED = "completed"
    INTERRUPTED = "interrupted"
    CANCELLED = "cancelled"
    UNKNOWN = "unknown"


class WorkflowTracker:
    """
    Tracks workflow execution state in Redis.

    Uses Redis for lightweight tracking with TTL-based cleanup.
    Gracefully degrades if Redis is unavailable.

    Redis Key Structure:
    - workflow:status:{thread_id} -> JSON status object (TTL: redis.ttl.workflow_status)
      - Includes retry_count and last_retry_at for error handling
    - workflow:cancel:{thread_id} -> "true" (TTL: redis.ttl.cancel_flag)

    Retry Tracking:
    - Increments retry_count on each transient error
    - Resets retry_count on successful completion
    - Maximum retries: 3 (enforced by app.py exception handler)
    """

    # Singleton instance
    _instance: Optional['WorkflowTracker'] = None

    # Redis key prefixes
    STATUS_PREFIX = "workflow:status:"
    CANCEL_PREFIX = "workflow:cancel:"

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
    ) -> bool:
        """
        Helper to update workflow status with metadata preservation.

        Consolidates the common pattern of:
        1. Get existing status from Redis
        2. Create default if not exists
        3. Update status and timestamp fields
        4. Merge metadata
        5. Save with optional TTL

        Args:
            thread_id: Thread/workflow identifier
            new_status: New status to set
            timestamp_field: Field name for timestamp (e.g., "completed_at")
            metadata: Optional metadata to merge
            ttl: Optional TTL in seconds

        Returns:
            True if successful, False otherwise
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
        metadata: Optional[Dict[str, Any]] = None
    ) -> bool:
        """
        Mark workflow as active (currently executing with connection).

        Args:
            thread_id: Thread/workflow identifier
            workspace_id: Workspace identifier
            user_id: User identifier
            metadata: Optional additional metadata

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
                "started_at": datetime.now().isoformat(),
                "last_update": datetime.now().isoformat(),
                "metadata": metadata or {}
            }

            # No TTL for active workflows - will be cleaned up on completion
            success = await self.cache.set(key, status_obj)

            if success:
                logger.debug(f"[WorkflowTracker] Marked workflow as active: {thread_id}")

            return success

        except Exception as e:
            logger.error(f"[WorkflowTracker] Error marking active: {e}")
            return False

    async def mark_disconnected(
        self,
        thread_id: str,
        metadata: Optional[Dict[str, Any]] = None
    ) -> bool:
        """
        Mark workflow as disconnected (client lost connection but workflow continues).

        Args:
            thread_id: Thread/workflow identifier
            metadata: Optional metadata about disconnection

        Returns:
            True if successfully marked, False otherwise
        """
        if not self.enabled:
            return False

        success = await self._update_status_with_metadata(
            thread_id=thread_id,
            new_status=WorkflowStatus.DISCONNECTED,
            timestamp_field="disconnected_at",
            metadata=metadata,
            ttl=None  # No TTL - workflow still running
        )

        if success:
            logger.info(
                f"[WorkflowTracker] Marked workflow as disconnected: {thread_id}"
            )

        return success

    async def mark_completed(
        self,
        thread_id: str,
        metadata: Optional[Dict[str, Any]] = None
    ) -> bool:
        """
        Mark workflow as completed (finished executing).

        Sets TTL per redis.ttl.workflow_status config (keeps brief history).

        Args:
            thread_id: Thread/workflow identifier
            metadata: Optional completion metadata

        Returns:
            True if successfully marked, False otherwise
        """
        if not self.enabled:
            return False

        ttl = get_redis_ttl_workflow_status()
        success = await self._update_status_with_metadata(
            thread_id=thread_id,
            new_status=WorkflowStatus.COMPLETED,
            timestamp_field="completed_at",
            metadata=metadata,
            ttl=ttl
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
        metadata: Optional[Dict[str, Any]] = None
    ) -> bool:
        """
        Mark workflow as interrupted (paused for human-in-the-loop review).

        The workflow is waiting for user input (e.g., plan approval) and is
        NOT actively streaming. Uses the same TTL as completed workflows.

        Args:
            thread_id: Thread/workflow identifier
            metadata: Optional metadata about the interrupt

        Returns:
            True if successfully marked, False otherwise
        """
        if not self.enabled:
            return False

        success = await self._update_status_with_metadata(
            thread_id=thread_id,
            new_status=WorkflowStatus.INTERRUPTED,
            timestamp_field="interrupted_at",
            metadata=metadata,
            ttl=None  # No TTL - workflow can be resumed at any time
        )

        if success:
            logger.info(
                f"[WorkflowTracker] Marked workflow as interrupted: {thread_id}"
            )

        return success

    async def mark_cancelled(self, thread_id: str) -> bool:
        """
        Mark workflow as cancelled (explicitly stopped by user).

        Args:
            thread_id: Thread/workflow identifier

        Returns:
            True if successfully marked, False otherwise
        """
        if not self.enabled:
            return False

        success = await self._update_status_with_metadata(
            thread_id=thread_id,
            new_status=WorkflowStatus.CANCELLED,
            timestamp_field="cancelled_at",
            metadata=None,
            ttl=get_redis_ttl_workflow_status()
        )

        if success:
            logger.info(
                f"[WorkflowTracker] Marked workflow as cancelled: {thread_id}"
            )

        return success

    async def set_cancel_flag(self, thread_id: str) -> bool:
        """
        Set explicit cancellation flag (separate from status).

        This flag is checked by the streaming generator to distinguish
        explicit user cancellation from accidental disconnect.

        Args:
            thread_id: Thread/workflow identifier

        Returns:
            True if successfully set, False otherwise
        """
        if not self.enabled:
            return False

        try:
            key = f"{self.CANCEL_PREFIX}{thread_id}"

            success = await self.cache.set(key, "true", ttl=get_redis_ttl_cancel_flag())

            if success:
                logger.debug(f"[WorkflowTracker] Set cancel flag: {thread_id}")

            return success

        except Exception as e:
            logger.error(f"[WorkflowTracker] Error setting cancel flag: {e}")
            return False

    async def is_cancelled(self, thread_id: str) -> bool:
        """
        Check if explicit cancellation flag is set.

        Args:
            thread_id: Thread/workflow identifier

        Returns:
            True if cancel flag exists, False otherwise
        """
        if not self.enabled:
            return False

        try:
            key = f"{self.CANCEL_PREFIX}{thread_id}"
            exists = await self.cache.exists(key)

            if exists:
                logger.debug(f"[WorkflowTracker] Cancel flag exists: {thread_id}")

            return exists

        except Exception as e:
            logger.error(f"[WorkflowTracker] Error checking cancel flag: {e}")
            return False

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
            cancel_key = f"{self.CANCEL_PREFIX}{thread_id}"

            # Delete both status and cancel flag
            status_deleted = await self.cache.delete(status_key)
            await self.cache.delete(cancel_key)  # Best effort

            if status_deleted:
                logger.info(f"[WorkflowTracker] Deleted status: {thread_id}")

            return status_deleted

        except Exception as e:
            logger.error(f"[WorkflowTracker] Error deleting status: {e}")
            return False

    # ==================== Retry Count Tracking ====================

    async def increment_retry_count(self, thread_id: str) -> int:
        """
        Increment retry count for a workflow.

        Args:
            thread_id: Thread/workflow identifier

        Returns:
            Current retry count after increment, or 0 if tracking disabled
        """
        if not self.enabled:
            return 0

        try:
            key = f"{self.STATUS_PREFIX}{thread_id}"
            status = await self.cache.get(key)

            if not status:
                logger.warning(
                    f"[WorkflowTracker] No status found for {thread_id}, "
                    "cannot increment retry count"
                )
                return 0

            # Increment retry count
            retry_count = status.get("retry_count", 0) + 1
            status["retry_count"] = retry_count
            status["last_retry_at"] = datetime.now().isoformat()
            status["last_update"] = datetime.now().isoformat()

            await self.cache.set(key, status)

            logger.info(
                f"[WorkflowTracker] Incremented retry count for {thread_id}: {retry_count}"
            )

            return retry_count

        except Exception as e:
            logger.error(f"[WorkflowTracker] Error incrementing retry count: {e}")
            return 0

    async def get_retry_count(self, thread_id: str) -> int:
        """
        Get current retry count for a workflow.

        Args:
            thread_id: Thread/workflow identifier

        Returns:
            Current retry count, or 0 if not found
        """
        if not self.enabled:
            return 0

        try:
            key = f"{self.STATUS_PREFIX}{thread_id}"
            status = await self.cache.get(key)

            if not status:
                return 0

            return status.get("retry_count", 0)

        except Exception as e:
            logger.error(f"[WorkflowTracker] Error getting retry count: {e}")
            return 0

    async def reset_retry_count(self, thread_id: str) -> bool:
        """
        Reset retry count for a workflow (e.g., after successful execution).

        Args:
            thread_id: Thread/workflow identifier

        Returns:
            True if reset successfully, False otherwise
        """
        if not self.enabled:
            return False

        try:
            key = f"{self.STATUS_PREFIX}{thread_id}"
            status = await self.cache.get(key)

            if not status:
                return False

            status["retry_count"] = 0
            status["last_update"] = datetime.now().isoformat()

            await self.cache.set(key, status)

            logger.info(f"[WorkflowTracker] Reset retry count for {thread_id}")

            return True

        except Exception as e:
            logger.error(f"[WorkflowTracker] Error resetting retry count: {e}")
            return False
