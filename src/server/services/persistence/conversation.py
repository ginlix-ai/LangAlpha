"""
Conversation Persistence Service - Workflow-driven DB persistence

Decouples database persistence from SSE connection lifecycle.
DB operations follow LangGraph workflow stages, not HTTP request/response cycles.

Architecture:
- Stage-level transactions (atomic operations per workflow stage)
- Simple logging: [conversation] prefix for all operations
- Thread-scoped service instances (one per workflow execution)
- Works independently of SSE streaming
"""

import logging
from typing import Optional, Dict, Any, List
from datetime import datetime
from uuid import uuid4

from src.server.database import conversation as qr_db

logger = logging.getLogger(__name__)

# Module-level instance cache: thread_id -> service instance
_service_instances: Dict[str, "ConversationPersistenceService"] = {}


class ConversationPersistenceService:
    """
    Manages database persistence for a single workflow execution thread.

    Lifecycle:
    1. get_instance(thread_id) - Get or create service for thread
    2. persist_query_start() - Create query at workflow start
    3. persist_interrupt() - Update thread + create response (atomic)
    4. persist_resume_feedback() - Create feedback query
    5. persist_completion() - Update thread + create response (atomic)
    6. cleanup() - Remove service instance from cache

    Usage:
        service = ConversationPersistenceService.get_instance(thread_id)
        await service.persist_query_start(content="Analyze Tesla", query_type="initial")
        # ... workflow executes ...
        await service.persist_completion(metadata={...})
        await service.cleanup()
    """

    def __init__(
        self,
        thread_id: str,
        workspace_id: Optional[str] = None,
        user_id: Optional[str] = None
    ):
        """
        Initialize persistence service for a workflow thread.

        Args:
            thread_id: LangGraph thread ID
            workspace_id: Workspace ID
            user_id: User ID
        """
        self.thread_id = thread_id
        self.workspace_id = workspace_id
        self.user_id = user_id

        # Post-persist callback (set by BackgroundTaskManager to clear event buffer, etc.)
        self._on_pair_persisted: Optional[callable] = None

        # Track persistence state per turn_index (Set-based for multi-iteration support)
        self._persisted_queries: set[int] = set()        # Track which turn_index queries created
        self._persisted_interrupts: set[int] = set()     # Track which turn_index interrupts saved
        self._persisted_completions: set[int] = set()    # Track which turn_index completions saved

        # Cache turn_index to avoid repeated DB queries
        self._turn_index_cache: Optional[int] = None
        self._current_query_id: Optional[str] = None
        self._current_response_id: Optional[str] = None

        logger.debug(
            f"[ConversationPersistence] Initialized service "
            f"thread_id={thread_id} workspace_id={workspace_id} user_id={user_id}"
        )

    @classmethod
    def get_instance(
        cls,
        thread_id: str,
        workspace_id: Optional[str] = None,
        user_id: Optional[str] = None
    ) -> "ConversationPersistenceService":
        """
        Get or create service instance for a thread.

        Uses module-level cache to ensure single instance per thread.
        """
        if thread_id not in _service_instances:
            _service_instances[thread_id] = cls(thread_id, workspace_id, user_id)
            logger.debug(f"[ConversationPersistence] Created new service instance for thread_id={thread_id}")

        # Update workspace_id and user_id if provided (may not be available at creation)
        instance = _service_instances[thread_id]
        if workspace_id and not instance.workspace_id:
            instance.workspace_id = workspace_id
        if user_id and not instance.user_id:
            instance.user_id = user_id

        return instance

    def _clear_tracking_state(self):
        """Clear all per-turn tracking sets and cached IDs."""
        self._persisted_queries.clear()
        self._persisted_interrupts.clear()
        self._persisted_completions.clear()
        self._current_query_id = None
        self._current_response_id = None

    async def cleanup(self):
        """Clean up service state and remove from instance cache."""
        logger.info(f"[ConversationPersistence] Cleaning up service for thread_id={self.thread_id}")

        self._clear_tracking_state()
        self._turn_index_cache = None

        # Remove from instance cache
        if self.thread_id in _service_instances:
            del _service_instances[self.thread_id]
            logger.debug(f"[ConversationPersistence] Removed service from cache for thread_id={self.thread_id}")

    def mark_query_persisted(self, turn_index: int):
        """Mark a turn's query as already persisted (e.g., preserved during fork)."""
        self._persisted_queries.add(turn_index)

    def reset_for_fork(self, fork_turn_index: int):
        """Reset persistence state for a fork/branch operation.

        Sets turn_index to the fork point and clears tracking so the normal
        flow persists fresh records for the new branch.
        """
        self._clear_tracking_state()
        self._turn_index_cache = fork_turn_index
        logger.debug(
            f"[ConversationPersistence] Reset for fork at turn_index={fork_turn_index} "
            f"thread_id={self.thread_id}"
        )

    async def get_or_calculate_turn_index(self, conn=None) -> int:
        """
        Get cached turn_index or calculate from database.

        Caches result to avoid repeated COUNT queries within same workflow.
        """
        if self._turn_index_cache is None:
            self._turn_index_cache = await qr_db.get_next_turn_index(self.thread_id, conn=conn)
            logger.debug(
                f"[ConversationPersistence] Calculated turn_index={self._turn_index_cache} "
                f"for thread_id={self.thread_id}"
            )
        return self._turn_index_cache

    def increment_turn_index(self):
        """Increment cached turn_index after creating a query-response pair."""
        if self._turn_index_cache is not None:
            self._turn_index_cache += 1
            logger.debug(
                f"[ConversationPersistence] Incremented turn_index to {self._turn_index_cache} "
                f"for thread_id={self.thread_id}"
            )

    async def _finalize_pair(self):
        """Increment turn index and run post-persist hook (clear event buffer, etc.)."""
        self.increment_turn_index()
        if self._on_pair_persisted:
            try:
                await self._on_pair_persisted()
            except Exception as e:
                logger.warning(
                    f"[ConversationPersistence] _on_pair_persisted callback failed "
                    f"for thread_id={self.thread_id}: {e}"
                )

    async def _get_latest_checkpoint_id(self) -> str | None:
        """Best-effort: get latest checkpoint_id from the checkpointer.

        Called before terminal persist transactions so the ID can be passed
        to update_thread_status in the same UPDATE.
        Returns None silently if checkpointer is unavailable.
        """
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
        timestamp: Optional[datetime] = None
    ) -> str:
        """
        Persist query at workflow start.

        Should be called when workflow begins processing a user query.

        Args:
            content: User query content
            query_type: Query type (initial, follow_up, resume_feedback)
            feedback_action: Feedback action for HITL (ACCEPTED, EDIT_PLAN, etc.)
            metadata: Additional metadata
            timestamp: Query timestamp (defaults to now)

        Returns:
            query_id: Created query ID
        """
        turn_index = await self.get_or_calculate_turn_index()

        if turn_index in self._persisted_queries:
            logger.warning(
                f"[ConversationPersistence] Query already created for thread_id={self.thread_id} "
                f"turn_index={turn_index}, skipping"
            )
            return self._current_query_id

        try:
            query_id = str(uuid4())

            await qr_db.create_query(
                conversation_query_id=query_id,
                conversation_thread_id=self.thread_id,
                turn_index=turn_index,
                content=content,
                query_type=query_type,
                feedback_action=feedback_action,
                metadata=metadata,
                created_at=timestamp
            )

            self._persisted_queries.add(turn_index)
            self._current_query_id = query_id

            logger.debug(
                f"[ConversationPersistence] Created query for thread_id={self.thread_id} "
                f"turn_index={turn_index} query_id={query_id}"
            )

            return query_id

        except Exception as e:
            logger.error(
                f"[ConversationPersistence] Failed to persist query start "
                f"thread_id={self.thread_id}: {e}",
                exc_info=True
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
        sse_events: Optional[List[Dict[str, Any]]] = None
    ) -> str:
        """
        Persist interrupt state (atomic transaction).

        Groups operations:
        1. Update thread status to "interrupted"
        2. Create response with status="interrupted"
        3. Create usage record (token + infrastructure credits)

        Args:
            interrupt_reason: Reason for interrupt (e.g., "plan_review_required")
            execution_time: Execution time up to interrupt
            metadata: Additional metadata (msg_type, stock_code, etc.)
            timestamp: Response timestamp (defaults to now)
            per_call_records: Per-call token records for accurate cost calculation
            tool_usage: Tool usage counts for infrastructure cost tracking

        Returns:
            response_id: Created response ID
        """
        turn_index = await self.get_or_calculate_turn_index()

        if turn_index in self._persisted_interrupts:
            logger.warning(
                f"[ConversationPersistence] Interrupt already persisted for thread_id={self.thread_id} "
                f"turn_index={turn_index}, skipping"
            )
            return self._current_response_id

        try:
            response_id = str(uuid4())
            _checkpoint_id = await self._get_latest_checkpoint_id()

            # Stage-level transaction: group update + create + usage tracking
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
                        conn=conn
                    )

                    # NEW: Create usage record (token + infrastructure credits)
                    # Track credits even for interrupted workflows to enable proper billing
                    if per_call_records or tool_usage:
                        from src.server.services.persistence.usage import UsagePersistenceService

                        usage_service = UsagePersistenceService(
                            thread_id=self.thread_id,
                            workspace_id=self.workspace_id,
                            user_id=self.user_id
                        )

                        # Track token usage if available
                        if per_call_records:
                            await usage_service.track_llm_usage(per_call_records)

                        # Track tool usage if available
                        if tool_usage:
                            usage_service.record_tool_usage_batch(tool_usage)

                        # Extract deepthinking from metadata
                        # Note: msg_type is overridden to 'interrupted' for interrupted workflows
                        # to enable clear separation in analytics/billing
                        deepthinking = metadata.get("deepthinking", False) if metadata else False

                        # Extract BYOK flag from metadata
                        is_byok = metadata.get("is_byok", False) if metadata else False

                        # Persist to conversation_usage table (status='interrupted')
                        # Override msg_type to 'interrupted' for interrupted workflows
                        usage_persisted = await usage_service.persist_usage(
                            response_id=response_id,
                            timestamp=timestamp,
                            msg_type="interrupted",  # Always use 'interrupted' for interrupted workflows
                            deepthinking=deepthinking,
                            status="interrupted",
                            conn=conn,
                            is_byok=is_byok
                        )

                        if usage_persisted:
                            logger.info(
                                f"Persisted interrupted workflow: thread_id={self.thread_id} response_id={response_id}"
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

            # Increment turn_index and run post-persist hook (e.g. clear event buffer)
            await self._finalize_pair()

            return response_id

        except Exception as e:
            logger.error(
                f"[ConversationPersistence] Failed to persist interrupt "
                f"thread_id={self.thread_id}: {e}",
                exc_info=True
            )
            raise

    async def persist_resume_feedback(
        self,
        feedback_action: str,
        content: str = "",
        metadata: Optional[Dict[str, Any]] = None,
        timestamp: Optional[datetime] = None
    ) -> str:
        """
        Persist resume feedback query.

        Called when user provides feedback after interrupt (e.g., accepts plan).

        Args:
            feedback_action: Feedback action (ACCEPTED, EDIT_PLAN, etc.)
            content: User's additional input (if any)
            metadata: Additional metadata
            timestamp: Query timestamp (defaults to now)

        Returns:
            query_id: Created query ID
        """
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
                created_at=timestamp
            )

            self.query_created = True
            self._current_query_id = query_id

            return query_id

        except Exception as e:
            logger.error(
                f"[ConversationPersistence] Failed to persist resume feedback "
                f"thread_id={self.thread_id}: {e}",
                exc_info=True
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
        skip_finalize: bool = False
    ) -> str:
        """
        Persist workflow completion (atomic transaction).

        Groups operations:
        1. Update thread status to "completed"
        2. Create response with status="completed"
        3. Create usage record (token + infrastructure credits)

        Args:
            metadata: Additional metadata
            warnings: Warning messages
            errors: Error messages
            execution_time: Total execution time
            timestamp: Response timestamp (defaults to now)
            per_call_records: Per-call token records for accurate cost calculation
            tool_usage: Tool usage counts for infrastructure cost tracking

        Returns:
            response_id: Created response ID
        """
        turn_index = await self.get_or_calculate_turn_index()

        if turn_index in self._persisted_completions:
            logger.warning(
                f"[ConversationPersistence] Completion already persisted for thread_id={self.thread_id} "
                f"turn_index={turn_index}, skipping"
            )
            # Advance turn if caller expects it (e.g. reinvoke dedup after pre-tail persist)
            if not skip_finalize:
                await self._finalize_pair()
            return self._current_response_id

        try:
            response_id = str(uuid4())
            _checkpoint_id = await self._get_latest_checkpoint_id()

            # Stage-level transaction: group update + create + usage tracking
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
                        conn=conn
                    )

                    # NEW: Create usage record (token + infrastructure credits)
                    if per_call_records or tool_usage:
                        from src.server.services.persistence.usage import UsagePersistenceService

                        usage_service = UsagePersistenceService(
                            thread_id=self.thread_id,
                            workspace_id=self.workspace_id,
                            user_id=self.user_id
                        )

                        # Track token usage if available
                        if per_call_records:
                            await usage_service.track_llm_usage(per_call_records)

                        # Track tool usage if available
                        if tool_usage:
                            usage_service.record_tool_usage_batch(tool_usage)

                        # Extract msg_type and deepthinking from metadata
                        msg_type = metadata.get("msg_type") if metadata else None
                        deepthinking = metadata.get("deepthinking", False) if metadata else False

                        # Extract BYOK flag from metadata
                        is_byok = metadata.get("is_byok", False) if metadata else False

                        # Persist to conversation_usage table (status='completed')
                        await usage_service.persist_usage(
                            response_id=response_id,
                            timestamp=timestamp,
                            msg_type=msg_type,
                            deepthinking=deepthinking,
                            status="completed",
                            conn=conn,
                            is_byok=is_byok
                        )

            self._persisted_completions.add(turn_index)
            self._current_response_id = response_id

            logger.debug(
                f"[ConversationPersistence] Persisted completion for thread_id={self.thread_id} "
                f"turn_index={turn_index} response_id={response_id}"
            )

            if not skip_finalize:
                # Increment turn_index and run post-persist hook (e.g. clear event buffer)
                await self._finalize_pair()

            return response_id


        except Exception as e:
            logger.error(
                f"[ConversationPersistence] Failed to persist completion "
                f"thread_id={self.thread_id}: {e}",
                exc_info=True
            )
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
        metadata: Optional[Dict[str, Any]] = None
    ) -> str:
        """
        Persist error state (atomic transaction).

        Groups operations:
        1. Update thread status to "error"
        2. Create response with status="error"
        3. Create usage record (token + infrastructure credits)

        Args:
            error_message: Error message
            errors: Error list
            execution_time: Execution time until error
            timestamp: Response timestamp (defaults to now)
            per_call_records: Per-call token records for accurate cost calculation
            tool_usage: Tool usage counts for infrastructure cost tracking
            metadata: Additional metadata (msg_type, deepthinking, etc.)

        Returns:
            response_id: Created response ID
        """
        try:
            response_id = str(uuid4())
            turn_index = await self.get_or_calculate_turn_index()
            _checkpoint_id = await self._get_latest_checkpoint_id()

            if errors is None:
                errors = [error_message]

            # Stage-level transaction: group update + create + usage tracking
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
                        conn=conn
                    )


                    # NEW: Create usage record (token + infrastructure credits)
                    # Track credits even for failed workflows for accurate billing
                    if per_call_records or tool_usage:
                        from src.server.services.persistence.usage import UsagePersistenceService

                        usage_service = UsagePersistenceService(
                            thread_id=self.thread_id,
                            workspace_id=self.workspace_id,
                            user_id=self.user_id
                        )

                        # Track token usage if available
                        if per_call_records:
                            await usage_service.track_llm_usage(per_call_records)

                        # Track tool usage if available
                        if tool_usage:
                            usage_service.record_tool_usage_batch(tool_usage)

                        # Extract msg_type and deepthinking from metadata
                        msg_type = metadata.get("msg_type") if metadata else None
                        deepthinking = metadata.get("deepthinking", False) if metadata else False

                        # Extract BYOK flag from metadata
                        is_byok = metadata.get("is_byok", False) if metadata else False

                        # Persist to conversation_usage table (status='error')
                        usage_persisted = await usage_service.persist_usage(
                            response_id=response_id,
                            timestamp=timestamp,
                            msg_type=msg_type,
                            deepthinking=deepthinking,
                            status="error",
                            conn=conn,
                            is_byok=is_byok
                        )

                        if usage_persisted:
                            logger.info(
                                f"Persisted failed workflow: thread_id={self.thread_id} response_id={response_id}"
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

            # Increment turn_index and run post-persist hook (e.g. clear event buffer)
            await self._finalize_pair()

            return response_id

        except Exception as e:
            logger.error(
                f"[ConversationPersistence] Failed to persist error "
                f"thread_id={self.thread_id}: {e}",
                exc_info=True
            )
            raise

    async def persist_cancelled(
        self,
        execution_time: Optional[float] = None,
        metadata: Optional[Dict[str, Any]] = None,
        timestamp: Optional[datetime] = None,
        per_call_records: Optional[list] = None,
        tool_usage: Optional[Dict[str, int]] = None,
        sse_events: Optional[List[Dict[str, Any]]] = None
    ) -> str:
        """
        Persist cancelled state (atomic transaction).

        Groups operations:
        1. Update thread status to "cancelled"
        2. Create response with status="cancelled"
        3. Create usage record (token + infrastructure credits)

        Args:
            execution_time: Execution time until cancellation
            metadata: Additional metadata
            timestamp: Response timestamp (defaults to now)
            per_call_records: Per-call token records for accurate cost calculation
            tool_usage: Tool usage counts for infrastructure cost tracking

        Returns:
            response_id: Created response ID
        """
        try:
            response_id = str(uuid4())
            turn_index = await self.get_or_calculate_turn_index()
            _checkpoint_id = await self._get_latest_checkpoint_id()

            # Stage-level transaction: group update + create + usage tracking
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
                        conn=conn
                    )


                    # NEW: Create usage record (token + infrastructure credits)
                    # Track credits even for cancelled workflows for accurate billing
                    if per_call_records or tool_usage:
                        from src.server.services.persistence.usage import UsagePersistenceService

                        usage_service = UsagePersistenceService(
                            thread_id=self.thread_id,
                            workspace_id=self.workspace_id,
                            user_id=self.user_id
                        )

                        # Track token usage if available
                        if per_call_records:
                            await usage_service.track_llm_usage(per_call_records)

                        # Track tool usage if available
                        if tool_usage:
                            usage_service.record_tool_usage_batch(tool_usage)

                        # Extract msg_type and deepthinking from metadata
                        msg_type = metadata.get("msg_type") if metadata else None
                        deepthinking = metadata.get("deepthinking", False) if metadata else False

                        # Extract BYOK flag from metadata
                        is_byok = metadata.get("is_byok", False) if metadata else False

                        # Persist to conversation_usage table (status='cancelled')
                        usage_persisted = await usage_service.persist_usage(
                            response_id=response_id,
                            timestamp=timestamp,
                            msg_type=msg_type,
                            deepthinking=deepthinking,
                            status="cancelled",
                            conn=conn,
                            is_byok=is_byok
                        )

                        if usage_persisted:
                            logger.info(
                                f"Persisted cancelled workflow: thread_id={self.thread_id} response_id={response_id}"
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

            # Increment turn_index and run post-persist hook (e.g. clear event buffer)
            await self._finalize_pair()

            return response_id

        except Exception as e:
            logger.error(
                f"[ConversationPersistence] Failed to persist cancellation "
                f"thread_id={self.thread_id}: {e}",
                exc_info=True
            )
            raise

    async def update_sse_events(
        self,
        response_id: str,
        sse_events: List[Dict[str, Any]],
    ) -> bool:
        """Update sse_events for an already-persisted response.

        Used by the post-interrupt subagent result collector to replace
        incomplete subagent events with the full set captured by middleware.

        Args:
            response_id: The response ID to update
            sse_events: Updated SSE events list

        Returns:
            True if the row was updated, False if not found
        """
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
