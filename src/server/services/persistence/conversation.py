"""
Conversation Persistence Service - Workflow-driven DB persistence

Decouples database persistence from SSE connection lifecycle.
DB operations follow LangGraph workflow stages, not HTTP request/response cycles.

Architecture:
- Stage-level transactions (atomic operations per workflow stage)
- Simple logging: [conversation] prefix for all operations
- Per-run service instances, keyed by (thread_id, run_id) where
  ``run_id == conversation_response_id`` 1:1.
"""

import logging
from typing import Optional, Dict, Any, List
from datetime import datetime
from uuid import uuid4

from src.server.database import conversation as qr_db
from src.observability.tracing import safe_aspan

logger = logging.getLogger(__name__)

# Module-level instance cache: (thread_id, run_id) -> service instance.
# Keyed by run_id so a concurrent new POST gets its own slot — no cross-turn
# state inheritance is possible by construction.
_service_instances: Dict[tuple[str, str], "ConversationPersistenceService"] = {}


class ConversationPersistenceService:
    """
    Manages database persistence for a single workflow execution turn.

    Lifecycle:
    1. get_instance(thread_id, run_id) — get or create service for this turn
    2. persist_query_start() — create query at workflow start
    3. persist_interrupt() / persist_completion() / persist_error() / persist_cancelled()
       — accept response_id (= run_id) so the DB row's primary key matches
       the on-the-wire identity
    4. cleanup() — remove this turn's service instance from the cache

    ``run_id == conversation_response_id`` is a 1:1 contract: each POST gets
    a fresh ``run_id`` at the handler entry, and that value is the response
    row's ``conversation_response_id``. HITL resumes, retries, and steering
    all produce new ``run_id``s (and therefore new response rows).
    """

    def __init__(
        self,
        thread_id: str,
        run_id: str,
        workspace_id: Optional[str] = None,
        user_id: Optional[str] = None,
    ):
        self.thread_id = thread_id
        self.run_id = run_id
        self.workspace_id = workspace_id
        self.user_id = user_id

        # Post-persist callback (BackgroundTaskManager sets this to clear the
        # Redis event buffer once persistence is durable).
        self._on_pair_persisted: Optional[callable] = None

        # Track persistence state per turn_index (Set-based for multi-iteration support)
        self._persisted_interrupts: set[int] = set()
        self._persisted_completions: set[int] = set()

        # Cache turn_index to avoid repeated DB queries
        self._turn_index_cache: Optional[int] = None
        self._current_query_id: Optional[str] = None
        # ``_current_response_id`` is just ``run_id`` for the canonical
        # path; kept as a field for legacy callers that read it.
        self._current_response_id: Optional[str] = run_id

        # Set once cleanup() runs so any post-cleanup terminal persist call
        # short-circuits instead of re-INSERTing the response row at a stale
        # turn_index (which would PK-collide on conversation_response_id).
        self._finalized: bool = False

        logger.debug(
            f"[ConversationPersistence] Initialized service "
            f"thread_id={thread_id} run_id={run_id} "
            f"workspace_id={workspace_id} user_id={user_id}"
        )

    @classmethod
    def get_instance(
        cls,
        thread_id: str,
        run_id: str,
        workspace_id: Optional[str] = None,
        user_id: Optional[str] = None,
    ) -> "ConversationPersistenceService":
        """Get or create the service instance for ``(thread_id, run_id)``.

        Per-run keying eliminates the cross-turn singleton aliasing that
        the old thread-keyed cache was prone to.
        """
        key = (thread_id, run_id)
        if key not in _service_instances:
            _service_instances[key] = cls(
                thread_id, run_id, workspace_id, user_id
            )
            logger.debug(
                f"[ConversationPersistence] Created new service instance for "
                f"thread_id={thread_id} run_id={run_id}"
            )

        instance = _service_instances[key]
        if workspace_id and not instance.workspace_id:
            instance.workspace_id = workspace_id
        if user_id and not instance.user_id:
            instance.user_id = user_id

        return instance

    def _clear_tracking_state(self):
        """Clear all per-turn tracking sets and cached IDs."""
        self._persisted_interrupts.clear()
        self._persisted_completions.clear()
        self._current_query_id = None

    async def cleanup(self):
        """Drop this turn's service instance from the module cache.

        Per-run keying makes this trivially identity-safe: no other turn can
        share our cache slot, so removing it can never affect another turn.
        """
        logger.info(
            f"[ConversationPersistence] Cleaning up service for "
            f"thread_id={self.thread_id} run_id={self.run_id}"
        )

        self._clear_tracking_state()
        self._turn_index_cache = None
        self._finalized = True

        _service_instances.pop((self.thread_id, self.run_id), None)

    def reset_for_fork(self, fork_turn_index: int):
        """Reset persistence state for a fork/branch operation."""
        self._clear_tracking_state()
        self._turn_index_cache = fork_turn_index
        logger.debug(
            f"[ConversationPersistence] Reset for fork at turn_index={fork_turn_index} "
            f"thread_id={self.thread_id} run_id={self.run_id}"
        )

    async def get_or_calculate_turn_index(self, conn=None) -> int:
        """Get cached turn_index or calculate from database."""
        if self._turn_index_cache is None:
            self._turn_index_cache = await qr_db.get_next_turn_index(
                self.thread_id, conn=conn
            )
            logger.debug(
                f"[ConversationPersistence] Calculated turn_index={self._turn_index_cache} "
                f"for thread_id={self.thread_id} run_id={self.run_id}"
            )
        return self._turn_index_cache

    def increment_turn_index(self):
        """Increment cached turn_index after creating a query-response pair."""
        if self._turn_index_cache is not None:
            self._turn_index_cache += 1
            logger.debug(
                f"[ConversationPersistence] Incremented turn_index to {self._turn_index_cache} "
                f"for thread_id={self.thread_id} run_id={self.run_id}"
            )

    async def _finalize_pair(self):
        """Increment turn index, run post-persist hook, release the per-run instance."""
        self.increment_turn_index()
        if self._on_pair_persisted:
            try:
                await self._on_pair_persisted()
            except Exception as e:
                logger.warning(
                    f"[ConversationPersistence] _on_pair_persisted callback failed "
                    f"for thread_id={self.thread_id} run_id={self.run_id}: {e}"
                )
        await self.cleanup()

    async def _get_latest_checkpoint_id(self) -> str | None:
        """Best-effort: get latest checkpoint_id from the checkpointer."""
        try:
            from src.server.app import setup

            if not setup.checkpointer:
                return None

            cp_tuple = await setup.checkpointer.aget_tuple(
                {"configurable": {"thread_id": self.thread_id}}
            )
            if not cp_tuple:
                return None

            return cp_tuple.config["configurable"]["checkpoint_id"]
        except Exception as e:
            logger.warning(
                f"[ConversationPersistence] Failed to get checkpoint_id "
                f"for thread_id={self.thread_id}: {e}"
            )
            return None

    async def persist_query_start(
        self,
        content: str,
        query_type: str,
        feedback_action: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
        timestamp: Optional[datetime] = None,
    ) -> str:
        """Persist query at workflow start. Returns ``query_id``."""
        turn_index = await self.get_or_calculate_turn_index()

        try:
            query_id = str(uuid4())

            row = await qr_db.create_query(
                conversation_query_id=query_id,
                conversation_thread_id=self.thread_id,
                turn_index=turn_index,
                content=content,
                query_type=query_type,
                feedback_action=feedback_action,
                metadata=metadata,
                created_at=timestamp,
            )

            # ON CONFLICT DO UPDATE keeps the existing primary key, so trust
            # the RETURNING value over the freshly-generated UUID.
            stored_query_id = (
                row.get("conversation_query_id", query_id)
                if isinstance(row, dict)
                else query_id
            )

            self._current_query_id = stored_query_id

            logger.debug(
                f"[ConversationPersistence] Persisted query for thread_id={self.thread_id} "
                f"run_id={self.run_id} turn_index={turn_index} query_id={stored_query_id}"
            )

            return stored_query_id

        except qr_db.QueryConflictError as e:
            logger.warning(
                f"[ConversationPersistence] Query conflict on persist_query_start "
                f"thread_id={self.thread_id} run_id={self.run_id} "
                f"turn_index={e.turn_index}: existing row content differs from new write"
            )
            raise
        except Exception as e:
            logger.error(
                f"[ConversationPersistence] Failed to persist query start "
                f"thread_id={self.thread_id} run_id={self.run_id}: {e}",
                exc_info=True,
            )
            raise

    async def persist_interrupt(
        self,
        interrupt_reason: str,
        execution_time: Optional[float] = None,
        metadata: Optional[Dict[str, Any]] = None,
        timestamp: Optional[datetime] = None,
        per_call_records: Optional[list] = None,
        tool_usage: Optional[Dict[str, int]] = None,
        sse_events: Optional[List[Dict[str, Any]]] = None,
    ) -> str:
        """Persist interrupt state (atomic). ``conversation_response_id`` = ``self.run_id``."""
        if self._finalized:
            logger.warning(
                f"[ConversationPersistence] persist_interrupt called after finalization "
                f"for thread_id={self.thread_id} run_id={self.run_id}; returning cached response_id"
            )
            return self._current_response_id

        turn_index = await self.get_or_calculate_turn_index()

        if turn_index in self._persisted_interrupts:
            logger.warning(
                f"[ConversationPersistence] Interrupt already persisted for "
                f"thread_id={self.thread_id} run_id={self.run_id} "
                f"turn_index={turn_index}, skipping"
            )
            return self._current_response_id

        try:
            response_id = self.run_id
            _checkpoint_id = await self._get_latest_checkpoint_id()

            async with qr_db.get_db_connection() as conn:
                async with conn.transaction():
                    await qr_db.update_thread_status(
                        self.thread_id, "interrupted",
                        checkpoint_id=_checkpoint_id, conn=conn,
                    )

                    await qr_db.create_response(
                        conversation_response_id=response_id,
                        conversation_thread_id=self.thread_id,
                        turn_index=turn_index,
                        status="interrupted",
                        interrupt_reason=interrupt_reason,
                        metadata=metadata,
                        execution_time=execution_time,
                        created_at=timestamp,
                        sse_events=sse_events,
                        conn=conn,
                    )

                    if per_call_records or tool_usage:
                        from src.server.services.persistence.usage import UsagePersistenceService

                        usage_service = UsagePersistenceService(
                            thread_id=self.thread_id,
                            workspace_id=self.workspace_id,
                            user_id=self.user_id,
                        )

                        if per_call_records:
                            await usage_service.track_llm_usage(per_call_records)

                        if tool_usage:
                            usage_service.record_tool_usage_batch(tool_usage)

                        deepthinking = metadata.get("deepthinking", False) if metadata else False
                        is_byok = metadata.get("is_byok", False) if metadata else False

                        usage_persisted = await usage_service.persist_usage(
                            response_id=response_id,
                            timestamp=timestamp,
                            msg_type="interrupted",
                            deepthinking=deepthinking,
                            status="interrupted",
                            conn=conn,
                            is_byok=is_byok,
                        )

                        if usage_persisted:
                            logger.info(
                                f"Persisted interrupted workflow: thread_id={self.thread_id} "
                                f"response_id={response_id}"
                            )
                        else:
                            logger.warning(
                                f"[ConversationPersistence] Failed to persist usage for interrupted workflow "
                                f"thread_id={self.thread_id} response_id={response_id}"
                            )
                    else:
                        logger.debug(
                            f"[ConversationPersistence] No usage data to persist for interrupted workflow "
                            f"thread_id={self.thread_id} response_id={response_id}"
                        )

            self._persisted_interrupts.add(turn_index)
            self._current_response_id = response_id

            logger.info(
                f"[ConversationPersistence] Persisted interrupt for thread_id={self.thread_id} "
                f"turn_index={turn_index} response_id={response_id}"
            )

            await self._finalize_pair()

            return response_id

        except Exception as e:
            logger.error(
                f"[ConversationPersistence] Failed to persist interrupt "
                f"thread_id={self.thread_id} run_id={self.run_id}: {e}",
                exc_info=True,
            )
            _service_instances.pop((self.thread_id, self.run_id), None)
            raise

    async def persist_resume_feedback(
        self,
        feedback_action: str,
        content: str = "",
        metadata: Optional[Dict[str, Any]] = None,
        timestamp: Optional[datetime] = None,
    ) -> str:
        """Persist resume feedback query."""
        try:
            query_id = str(uuid4())
            turn_index = await self.get_or_calculate_turn_index()

            await qr_db.create_query(
                conversation_query_id=query_id,
                conversation_thread_id=self.thread_id,
                turn_index=turn_index,
                content=content,
                query_type="resume_feedback",
                feedback_action=feedback_action,
                metadata=metadata,
                created_at=timestamp,
            )

            self._current_query_id = query_id

            return query_id

        except Exception as e:
            logger.error(
                f"[ConversationPersistence] Failed to persist resume feedback "
                f"thread_id={self.thread_id}: {e}",
                exc_info=True,
            )
            raise

    async def persist_completion(
        self,
        metadata: Optional[Dict[str, Any]] = None,
        warnings: Optional[list] = None,
        errors: Optional[list] = None,
        execution_time: Optional[float] = None,
        timestamp: Optional[datetime] = None,
        per_call_records: Optional[list] = None,
        tool_usage: Optional[Dict[str, int]] = None,
        sse_events: Optional[List[Dict[str, Any]]] = None,
        skip_finalize: bool = False,
    ) -> str:
        """Persist workflow completion (atomic). ``conversation_response_id`` = ``self.run_id``."""
        if self._finalized:
            logger.warning(
                f"[ConversationPersistence] persist_completion called after finalization "
                f"for thread_id={self.thread_id} run_id={self.run_id}; returning cached response_id"
            )
            return self._current_response_id

        async with safe_aspan(
            "chat.turn.persist",
            {"status": "completed", "thread_id_hash": (self.thread_id or "")[:16]},
        ):
            turn_index = await self.get_or_calculate_turn_index()

            if turn_index in self._persisted_completions:
                logger.warning(
                    f"[ConversationPersistence] Completion already persisted for "
                    f"thread_id={self.thread_id} run_id={self.run_id} "
                    f"turn_index={turn_index}, skipping"
                )
                if not skip_finalize:
                    await self._finalize_pair()
                return self._current_response_id

            try:
                response_id = self.run_id
                _checkpoint_id = await self._get_latest_checkpoint_id()

                async with qr_db.get_db_connection() as conn:
                    async with conn.transaction():
                        await qr_db.update_thread_status(
                            self.thread_id, "completed",
                            checkpoint_id=_checkpoint_id, conn=conn,
                        )

                        await qr_db.create_response(
                            conversation_response_id=response_id,
                            conversation_thread_id=self.thread_id,
                            turn_index=turn_index,
                            status="completed",
                            metadata=metadata,
                            warnings=warnings,
                            errors=errors,
                            execution_time=execution_time,
                            created_at=timestamp,
                            sse_events=sse_events,
                            conn=conn,
                        )

                        if per_call_records or tool_usage:
                            from src.server.services.persistence.usage import UsagePersistenceService

                            usage_service = UsagePersistenceService(
                                thread_id=self.thread_id,
                                workspace_id=self.workspace_id,
                                user_id=self.user_id,
                            )

                            if per_call_records:
                                await usage_service.track_llm_usage(per_call_records)

                            if tool_usage:
                                usage_service.record_tool_usage_batch(tool_usage)

                            msg_type = metadata.get("msg_type") if metadata else None
                            deepthinking = metadata.get("deepthinking", False) if metadata else False
                            is_byok = metadata.get("is_byok", False) if metadata else False

                            await usage_service.persist_usage(
                                response_id=response_id,
                                timestamp=timestamp,
                                msg_type=msg_type,
                                deepthinking=deepthinking,
                                status="completed",
                                conn=conn,
                                is_byok=is_byok,
                            )

                self._persisted_completions.add(turn_index)
                self._current_response_id = response_id

                logger.debug(
                    f"[ConversationPersistence] Persisted completion for thread_id={self.thread_id} "
                    f"turn_index={turn_index} response_id={response_id}"
                )

                if not skip_finalize:
                    await self._finalize_pair()

                return response_id

            except Exception as e:
                logger.error(
                    f"[ConversationPersistence] Failed to persist completion "
                    f"thread_id={self.thread_id} run_id={self.run_id}: {e}",
                    exc_info=True,
                )
                _service_instances.pop((self.thread_id, self.run_id), None)
                raise

    async def persist_error(
        self,
        error_message: str,
        errors: Optional[list] = None,
        execution_time: Optional[float] = None,
        timestamp: Optional[datetime] = None,
        per_call_records: Optional[list] = None,
        tool_usage: Optional[Dict[str, int]] = None,
        sse_events: Optional[List[Dict[str, Any]]] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> str:
        """Persist error state (atomic). ``conversation_response_id`` = ``self.run_id``."""
        if self._finalized:
            logger.warning(
                f"[ConversationPersistence] persist_error called after finalization "
                f"for thread_id={self.thread_id} run_id={self.run_id}; returning cached response_id"
            )
            return self._current_response_id

        try:
            response_id = self.run_id
            turn_index = await self.get_or_calculate_turn_index()
            _checkpoint_id = await self._get_latest_checkpoint_id()

            if errors is None:
                errors = [error_message]

            async with qr_db.get_db_connection() as conn:
                async with conn.transaction():
                    await qr_db.update_thread_status(
                        self.thread_id, "error",
                        checkpoint_id=_checkpoint_id, conn=conn,
                    )

                    await qr_db.create_response(
                        conversation_response_id=response_id,
                        conversation_thread_id=self.thread_id,
                        turn_index=turn_index,
                        status="error",
                        interrupt_reason=None,
                        metadata=metadata,
                        warnings=None,
                        errors=None,
                        execution_time=execution_time,
                        created_at=timestamp,
                        sse_events=sse_events,
                        conn=conn,
                    )

                    if per_call_records or tool_usage:
                        from src.server.services.persistence.usage import UsagePersistenceService

                        usage_service = UsagePersistenceService(
                            thread_id=self.thread_id,
                            workspace_id=self.workspace_id,
                            user_id=self.user_id,
                        )

                        if per_call_records:
                            await usage_service.track_llm_usage(per_call_records)

                        if tool_usage:
                            usage_service.record_tool_usage_batch(tool_usage)

                        msg_type = metadata.get("msg_type") if metadata else None
                        deepthinking = metadata.get("deepthinking", False) if metadata else False
                        is_byok = metadata.get("is_byok", False) if metadata else False

                        usage_persisted = await usage_service.persist_usage(
                            response_id=response_id,
                            timestamp=timestamp,
                            msg_type=msg_type,
                            deepthinking=deepthinking,
                            status="error",
                            conn=conn,
                            is_byok=is_byok,
                        )

                        if usage_persisted:
                            logger.info(
                                f"Persisted failed workflow: thread_id={self.thread_id} "
                                f"response_id={response_id}"
                            )
                        else:
                            logger.warning(
                                f"[ConversationPersistence] Failed to persist usage for failed workflow "
                                f"thread_id={self.thread_id} response_id={response_id}"
                            )
                    else:
                        logger.debug(
                            f"[ConversationPersistence] No usage data to persist for failed workflow "
                            f"thread_id={self.thread_id} response_id={response_id}"
                        )

            self._current_response_id = response_id

            logger.info(
                f"[ConversationPersistence] Persisted error for thread_id={self.thread_id} "
                f"turn_index={turn_index} response_id={response_id}"
            )

            await self._finalize_pair()

            return response_id

        except Exception as e:
            logger.error(
                f"[ConversationPersistence] Failed to persist error "
                f"thread_id={self.thread_id} run_id={self.run_id}: {e}",
                exc_info=True,
            )
            _service_instances.pop((self.thread_id, self.run_id), None)
            raise

    async def persist_cancelled(
        self,
        execution_time: Optional[float] = None,
        metadata: Optional[Dict[str, Any]] = None,
        timestamp: Optional[datetime] = None,
        per_call_records: Optional[list] = None,
        tool_usage: Optional[Dict[str, int]] = None,
        sse_events: Optional[List[Dict[str, Any]]] = None,
    ) -> str:
        """Persist cancelled state (atomic). ``conversation_response_id`` = ``self.run_id``."""
        if self._finalized:
            logger.warning(
                f"[ConversationPersistence] persist_cancelled called after finalization "
                f"for thread_id={self.thread_id} run_id={self.run_id}; returning cached response_id"
            )
            return self._current_response_id

        try:
            response_id = self.run_id
            turn_index = await self.get_or_calculate_turn_index()
            _checkpoint_id = await self._get_latest_checkpoint_id()

            async with qr_db.get_db_connection() as conn:
                async with conn.transaction():
                    await qr_db.update_thread_status(
                        self.thread_id, "cancelled",
                        checkpoint_id=_checkpoint_id, conn=conn,
                    )

                    await qr_db.create_response(
                        conversation_response_id=response_id,
                        conversation_thread_id=self.thread_id,
                        turn_index=turn_index,
                        status="cancelled",
                        interrupt_reason=None,
                        metadata=metadata,
                        warnings=None,
                        errors=None,
                        execution_time=execution_time,
                        created_at=timestamp,
                        sse_events=sse_events,
                        conn=conn,
                    )

                    if per_call_records or tool_usage:
                        from src.server.services.persistence.usage import UsagePersistenceService

                        usage_service = UsagePersistenceService(
                            thread_id=self.thread_id,
                            workspace_id=self.workspace_id,
                            user_id=self.user_id,
                        )

                        if per_call_records:
                            await usage_service.track_llm_usage(per_call_records)

                        if tool_usage:
                            usage_service.record_tool_usage_batch(tool_usage)

                        msg_type = metadata.get("msg_type") if metadata else None
                        deepthinking = metadata.get("deepthinking", False) if metadata else False
                        is_byok = metadata.get("is_byok", False) if metadata else False

                        usage_persisted = await usage_service.persist_usage(
                            response_id=response_id,
                            timestamp=timestamp,
                            msg_type=msg_type,
                            deepthinking=deepthinking,
                            status="cancelled",
                            conn=conn,
                            is_byok=is_byok,
                        )

                        if usage_persisted:
                            logger.info(
                                f"Persisted cancelled workflow: thread_id={self.thread_id} "
                                f"response_id={response_id}"
                            )
                        else:
                            logger.warning(
                                f"[ConversationPersistence] Failed to persist usage for cancelled workflow "
                                f"thread_id={self.thread_id} response_id={response_id}"
                            )
                    else:
                        logger.debug(
                            f"[ConversationPersistence] No usage data to persist for cancelled workflow "
                            f"thread_id={self.thread_id} response_id={response_id}"
                        )

            self._current_response_id = response_id

            logger.info(
                f"[ConversationPersistence] Persisted cancellation for thread_id={self.thread_id} "
                f"turn_index={turn_index} response_id={response_id}"
            )

            await self._finalize_pair()

            return response_id

        except Exception as e:
            logger.error(
                f"[ConversationPersistence] Failed to persist cancellation "
                f"thread_id={self.thread_id} run_id={self.run_id}: {e}",
                exc_info=True,
            )
            _service_instances.pop((self.thread_id, self.run_id), None)
            raise

    async def update_sse_events(
        self,
        response_id: str,
        sse_events: List[Dict[str, Any]],
    ) -> bool:
        """Update sse_events for an already-persisted response."""
        try:
            result = await qr_db.update_sse_events(
                conversation_response_id=response_id,
                sse_events=sse_events,
            )
            if result:
                logger.info(
                    f"[ConversationPersistence] Updated sse_events for "
                    f"response_id={response_id} ({len(sse_events)} events)"
                )
            return result
        except Exception as e:
            logger.error(
                f"[ConversationPersistence] Failed to update sse_events "
                f"response_id={response_id}: {e}",
                exc_info=True,
            )
            return False
