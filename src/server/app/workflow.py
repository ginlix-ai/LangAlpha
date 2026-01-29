"""
Workflow state management and control endpoints.

This module handles:
- Workflow state retrieval (via checkpointer)
- Checkpoint history
- Workflow cancellation
- Workflow status checking
"""

import logging

from fastapi import APIRouter, HTTPException, Query

from src.server.models.workflow import (
    WorkflowStateResponse,
    CheckpointHistoryResponse,
    CheckpointResponse,
    CheckpointMetadata,
    serialize_message,
)
from src.server.utils.checkpoint_helpers import (
    build_checkpoint_config,
    get_checkpointer,
)

# Import setup module to access initialized globals
from src.server.app import setup

logger = logging.getLogger(__name__)
INTERNAL_SERVER_ERROR_DETAIL = "Internal Server Error"

# Create router
router = APIRouter(prefix="/api/v1/workflow", tags=["Workflow"])


# ============================================================================
# Helper Functions for Checkpointer Access
# ============================================================================

async def _get_checkpoint_tuple(thread_id: str, checkpoint_id: str = None):
    """
    Get checkpoint tuple from checkpointer.

    Args:
        thread_id: Thread identifier
        checkpoint_id: Optional specific checkpoint ID

    Returns:
        CheckpointTuple or None if not found
    """
    checkpointer = get_checkpointer()
    config = build_checkpoint_config(thread_id, checkpoint_id)
    return await checkpointer.aget_tuple(config)


async def _list_checkpoints(thread_id: str, limit: int = 10):
    """
    List checkpoints for a thread from checkpointer.

    Args:
        thread_id: Thread identifier
        limit: Maximum number of checkpoints to return

    Yields:
        CheckpointTuple objects
    """
    checkpointer = get_checkpointer()
    config = build_checkpoint_config(thread_id)
    count = 0

    async for checkpoint_tuple in checkpointer.alist(config):
        if count >= limit:
            break
        yield checkpoint_tuple
        count += 1


def _extract_state_values(checkpoint_tuple) -> dict:
    """
    Extract state values from checkpoint tuple.

    The checkpoint contains serialized channel values that we can extract.
    """
    if not checkpoint_tuple or not checkpoint_tuple.checkpoint:
        return {}

    checkpoint = checkpoint_tuple.checkpoint
    channel_values = checkpoint.get("channel_values", {})

    # Return the channel values as state
    return channel_values


@router.get("/state/{thread_id}", response_model=WorkflowStateResponse)
async def get_workflow_state(thread_id: str):
    """
    Get the complete workflow state for a given thread_id.

    Retrieves the latest checkpoint state from the checkpointer, including:
    - All conversation messages
    - Execution metadata

    Note: PTC agent does not use plan/observations structure from deep_research.
    Those fields will be null in responses.

    Args:
        thread_id: The thread/conversation ID to retrieve state for

    Returns:
        WorkflowStateResponse with workflow state

    Raises:
        404: Thread ID not found or no checkpoints exist
        500: Internal server error
    """
    try:
        # Get checkpoint tuple using helper
        logger.info(f"Retrieving state for thread_id={thread_id}")
        checkpoint_tuple = await _get_checkpoint_tuple(thread_id)

        # Check if state exists
        if not checkpoint_tuple:
            logger.warning(f"No state found for thread_id={thread_id}")
            raise HTTPException(
                status_code=404,
                detail=f"No workflow state found for thread_id: {thread_id}"
            )

        # Extract state values from checkpoint
        state_values = _extract_state_values(checkpoint_tuple)
        logger.debug(f"State values keys: {list(state_values.keys())}")

        # Serialize messages
        raw_messages = state_values.get("messages", [])
        if not isinstance(raw_messages, list):
            raw_messages = [raw_messages] if raw_messages else []
        messages = [serialize_message(msg) for msg in raw_messages]

        # PTC agent doesn't use plan/observations - set to None
        plan = None
        observations = []

        # Extract other state fields (may not exist in PTC state)
        final_report = state_values.get("final_report")
        research_topic = state_values.get("research_topic")
        market_type = state_values.get("market_type")
        locale = state_values.get("locale")
        deepthinking = state_values.get("deepthinking", False)
        auto_accepted_plan = state_values.get("auto_accepted_plan", True)
        plan_iterations = state_values.get("plan_iterations", 0)

        # Get pending sends to determine if workflow is complete
        checkpoint = checkpoint_tuple.checkpoint or {}
        pending_sends = checkpoint.get("pending_sends", [])
        completed = len(pending_sends) == 0
        next_nodes = []  # Not available from raw checkpoint

        # Get checkpoint ID
        checkpoint_id = None
        if checkpoint_tuple.config:
            checkpoint_id = checkpoint_tuple.config.get("configurable", {}).get("checkpoint_id")

        # Get timestamps from metadata
        created_at = None
        updated_at = None
        if checkpoint_tuple.metadata:
            # Metadata may contain timestamp info
            created_at = checkpoint_tuple.metadata.get("created_at")
            updated_at = created_at

        # Build response
        response = WorkflowStateResponse(
            thread_id=thread_id,
            checkpoint_id=checkpoint_id,
            messages=messages,
            plan=plan,
            observations=observations,
            final_report=final_report,
            research_topic=research_topic,
            market_type=market_type,
            locale=locale,
            deepthinking=deepthinking,
            auto_accepted_plan=auto_accepted_plan,
            plan_iterations=plan_iterations,
            completed=completed,
            next_nodes=next_nodes,
            created_at=created_at,
            updated_at=updated_at
        )

        logger.info(f"Successfully retrieved state for thread_id={thread_id}, "
                   f"messages={len(messages)}, completed={completed}")

        return response

    except HTTPException:
        raise
    except Exception as e:
        logger.exception(f"Error retrieving workflow state for thread_id={thread_id}: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to retrieve workflow state: {str(e)}"
        )


@router.get(
    "/{thread_id}/checkpoints",
    response_model=CheckpointHistoryResponse,
    summary="Get checkpoint history for a workflow thread"
)
async def get_workflow_checkpoints(
    thread_id: str,
    limit: int = Query(10, ge=1, le=50, description="Maximum checkpoints to return")
):
    """
    Get checkpoint history for a workflow thread.

    Returns chronologically ordered list of checkpoints (newest first).
    Each checkpoint represents a snapshot of the workflow state at that point in time.

    Use cases:
    - Time-travel debugging: Inspect state at different execution points
    - State inspection: Understand workflow progression
    - Debugging: Identify where interrupts or errors occurred

    Args:
        thread_id: The thread/conversation ID to retrieve checkpoints for
        limit: Maximum number of checkpoints to return (1-50, default 10)

    Returns:
        CheckpointHistoryResponse with list of checkpoint snapshots ordered
        chronologically (newest first)

    Raises:
        404: Thread ID not found or no checkpoints exist
        500: Internal server error

    Example:
        GET /api/v1/workflow/abc123/checkpoints?limit=5
    """
    try:
        logger.info(f"Retrieving checkpoint history for thread_id={thread_id}, limit={limit}")

        checkpoints = []

        # Use helper to list checkpoints from checkpointer
        async for checkpoint_tuple in _list_checkpoints(thread_id, limit=limit):
            # Extract checkpoint metadata
            metadata_dict = checkpoint_tuple.metadata or {}
            metadata = CheckpointMetadata(
                source=metadata_dict.get("source", "unknown"),
                step=metadata_dict.get("step", -1),
                writes=metadata_dict.get("writes")
            )

            # Extract parent checkpoint ID for lineage tracking
            parent_checkpoint_id = None
            if checkpoint_tuple.parent_config:
                parent_checkpoint_id = checkpoint_tuple.parent_config.get(
                    "configurable", {}
                ).get("checkpoint_id")

            # Build state preview with key fields only
            state_values = _extract_state_values(checkpoint_tuple)
            state_preview = {
                "research_topic": state_values.get("research_topic"),
                "plan_iterations": state_values.get("plan_iterations", 0),
                "has_final_report": bool(state_values.get("final_report")),
                "message_count": len(state_values.get("messages", [])),
                "market_type": state_values.get("market_type"),
                "deepthinking": state_values.get("deepthinking", False),
                "auto_accepted_plan": state_values.get("auto_accepted_plan", True)
            }

            # Get checkpoint data for completion status
            checkpoint_data = checkpoint_tuple.checkpoint or {}
            pending_sends = checkpoint_data.get("pending_sends", [])

            # Get checkpoint ID from config
            checkpoint_id = checkpoint_tuple.config.get("configurable", {}).get("checkpoint_id")

            # Create checkpoint response (simplified - no task info from raw checkpoint)
            checkpoint = CheckpointResponse(
                checkpoint_id=checkpoint_id,
                parent_checkpoint_id=parent_checkpoint_id,
                created_at=metadata_dict.get("created_at"),
                metadata=metadata,
                next_nodes=[],  # Not available from raw checkpoint
                pending_tasks=len(pending_sends),
                tasks=[],  # Task info not available from raw checkpoint
                completed=len(pending_sends) == 0,
                state_preview=state_preview
            )

            checkpoints.append(checkpoint)

        # Check if any checkpoints were found
        if not checkpoints:
            logger.warning(f"No checkpoints found for thread_id={thread_id}")
            raise HTTPException(
                status_code=404,
                detail=f"No checkpoints found for thread_id: {thread_id}"
            )

        logger.info(f"Retrieved {len(checkpoints)} checkpoints for thread_id={thread_id}")

        return CheckpointHistoryResponse(
            thread_id=thread_id,
            total_checkpoints=len(checkpoints),
            checkpoints=checkpoints
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.exception(f"Error retrieving checkpoint history for thread_id={thread_id}: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to retrieve checkpoint history: {str(e)}"
        )


@router.post("/{thread_id}/cancel", status_code=200)
async def cancel_workflow(thread_id: str):
    """
    Explicitly cancel a workflow execution.

    Sets cancellation flag that the streaming generator will check.
    This distinguishes intentional user cancellation from accidental disconnect.

    The cancellation flag has a short TTL (5 minutes) and is checked when
    the streaming generator encounters a CancelledError exception.

    Args:
        thread_id: Thread ID to cancel

    Returns:
        Confirmation of cancellation with thread_id

    Example:
        POST /api/v1/workflow/abc123/cancel
        Response: {"cancelled": true, "thread_id": "abc123"}
    """
    try:
        from src.server.services.workflow_tracker import WorkflowTracker

        tracker = WorkflowTracker.get_instance()

        # Set cancellation flag (checked by exception handler)
        success = await tracker.set_cancel_flag(thread_id)

        # Mark workflow as cancelled immediately (don't wait for exception handler)
        # This provides immediate feedback to frontend
        await tracker.mark_cancelled(thread_id)

        # Update thread status in database for consistency
        from src.server.database import conversation as qr_db
        await qr_db.update_thread_status(thread_id, "cancelled")

        from src.config.settings import is_background_execution_enabled
        if is_background_execution_enabled():
            from src.server.services.background_task_manager import BackgroundTaskManager
            manager = BackgroundTaskManager.get_instance()
            cancel_success = await manager.cancel_workflow(thread_id)

            if not cancel_success:
                logger.warning(
                    f"Could not cancel background task for {thread_id} "
                    "(may be already completed or not found)"
                )

        if not success:
            logger.warning(
                f"Failed to set cancel flag for {thread_id} (Redis may be unavailable)"
            )

        from src.server.services.background_registry_store import BackgroundRegistryStore
        registry_store = BackgroundRegistryStore.get_instance()
        await registry_store.cancel_and_clear(thread_id, force=True)

        logger.info(f"Workflow cancelled: {thread_id}")

        return {
            "cancelled": True,
            "thread_id": thread_id,
            "message": "Cancellation signal sent. Workflow will stop shortly."
        }

    except Exception as e:
        logger.exception(f"Error cancelling workflow {thread_id}: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to cancel workflow: {str(e)}"
        )


@router.post("/{thread_id}/soft-interrupt", status_code=200)
async def soft_interrupt_workflow(thread_id: str):
    """
    Soft interrupt a workflow - pause main agent, keep subagents running.

    Unlike /cancel which stops everything, soft interrupt:
    - Signals the main agent to pause at the next safe point
    - Background subagents continue execution
    - Workflow can be resumed with new input

    This is designed for the CLI ESC key behavior where the user wants to
    interrupt the current response but keep background work running.

    Args:
        thread_id: Thread ID to soft interrupt

    Returns:
        Status including whether workflow can be resumed and active subagents

    Example:
        POST /api/v1/workflow/abc123/soft-interrupt
        Response: {
            "status": "soft_interrupted",
            "thread_id": "abc123",
            "can_resume": true,
            "background_tasks": ["researcher", "analyst"]
        }
    """
    try:
        from src.config.settings import is_background_execution_enabled

        if not is_background_execution_enabled():
            # Without background execution, soft interrupt is same as cancel
            return {
                "status": "not_supported",
                "thread_id": thread_id,
                "can_resume": False,
                "background_tasks": [],
                "message": "Soft interrupt requires background execution mode"
            }

        from src.server.services.background_task_manager import BackgroundTaskManager
        manager = BackgroundTaskManager.get_instance()

        result = await manager.soft_interrupt_workflow(thread_id)

        logger.info(
            f"Workflow soft interrupted: {thread_id}, "
            f"background_tasks={result.get('background_tasks', [])}"
        )

        return result

    except Exception as e:
        logger.exception(f"Error soft interrupting workflow {thread_id}: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to soft interrupt workflow: {str(e)}"
        )


@router.get("/{thread_id}/status")
async def get_workflow_status(thread_id: str):
    """
    Get current workflow execution status.

    Checks Redis for active/disconnected/completed status and combines
    with checkpoint data for detailed progress information.

    Returns workflow status, reconnection capability, and execution progress.

    Args:
        thread_id: Thread ID to check status for

    Returns:
        WorkflowStatusResponse with current status and progress info

    Status Values:
        - "active": Workflow is running with active connection
        - "disconnected": Workflow is running but client disconnected
        - "completed": Workflow finished successfully
        - "cancelled": Workflow was explicitly cancelled by user
        - "unknown": No tracking info found (may be old workflow or never started)

    Example:
        GET /api/v1/workflow/abc123/status
        Response: {
            "thread_id": "abc123",
            "status": "disconnected",
            "can_reconnect": true,
            "last_update": "2025-01-15T10:35:00Z",
            "progress": {
                "has_plan": false,
                "has_final_report": false,
                "message_count": 15
            }
        }
    """
    try:
        from src.server.services.workflow_tracker import WorkflowTracker, WorkflowStatus

        tracker = WorkflowTracker.get_instance()

        # Get status from Redis
        redis_status = await tracker.get_status(thread_id)

        # Check checkpoint for additional info
        checkpoint_info = None
        try:
            checkpoint_tuple = await _get_checkpoint_tuple(thread_id)
            if checkpoint_tuple:
                state_values = _extract_state_values(checkpoint_tuple)
                checkpoint_data = checkpoint_tuple.checkpoint or {}
                pending_sends = checkpoint_data.get("pending_sends", [])

                checkpoint_info = {
                    "has_plan": False,  # PTC doesn't use plans
                    "has_final_report": bool(state_values.get("final_report")),
                    "message_count": len(state_values.get("messages", [])),
                    "completed": len(pending_sends) == 0,
                    "checkpoint_id": checkpoint_tuple.config.get("configurable", {}).get("checkpoint_id")
                }
        except Exception as e:
            logger.debug(f"Could not fetch checkpoint info for {thread_id}: {e}")

        # Determine overall status
        if redis_status:
            status = redis_status.get("status", WorkflowStatus.UNKNOWN)
            last_update = redis_status.get("last_update")
            workspace_id = redis_status.get("workspace_id")
            user_id = redis_status.get("user_id")
        elif checkpoint_info and checkpoint_info.get("completed"):
            # Found in checkpoint but not in Redis = old completed workflow
            status = WorkflowStatus.COMPLETED
            last_update = None
            workspace_id = None
            user_id = None
        else:
            # Not in Redis, not in checkpoint = unknown
            status = WorkflowStatus.UNKNOWN
            last_update = None
            workspace_id = None
            user_id = None

        # Determine if reconnection is possible
        can_reconnect = status in [WorkflowStatus.ACTIVE, WorkflowStatus.DISCONNECTED]

        # Get subagent info from background task manager
        active_subagents = []
        completed_subagents = []
        soft_interrupted = False

        from src.config.settings import is_background_execution_enabled
        if is_background_execution_enabled():
            try:
                from src.server.services.background_task_manager import BackgroundTaskManager
                manager = BackgroundTaskManager.get_instance()
                bg_status = await manager.get_workflow_status(thread_id)
                if bg_status.get("status") != "not_found":
                    active_subagents = bg_status.get("active_subagents", [])
                    completed_subagents = bg_status.get("completed_subagents", [])
                    soft_interrupted = bg_status.get("soft_interrupted", False)
            except Exception as e:
                logger.debug(f"Could not get background task status for {thread_id}: {e}")

        response = {
            "thread_id": thread_id,
            "status": status,
            "can_reconnect": can_reconnect,
            "last_update": last_update,
            "workspace_id": workspace_id,
            "user_id": user_id,
            "progress": checkpoint_info,
            "active_subagents": active_subagents,
            "completed_subagents": completed_subagents,
            "soft_interrupted": soft_interrupted,
        }

        logger.debug(f"Status check for {thread_id}: {status}")

        return response

    except Exception as e:
        logger.exception(f"Error checking workflow status for {thread_id}: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to check workflow status: {str(e)}"
        )


@router.post("/{thread_id}/summarize", status_code=200)
async def trigger_summarization(
    thread_id: str,
    keep_messages: int = Query(default=5, ge=1, le=20, description="Number of recent messages to preserve")
):
    """
    Manually trigger conversation summarization for a thread.

    This summarizes the conversation history and updates the thread state,
    preserving the last `keep_messages` messages. Unlike automatic summarization
    which triggers based on token count, this endpoint allows explicit summarization
    at any time.

    The summarization replaces all existing messages with:
    1. A summary message containing the conversation context
    2. The last N preserved messages (specified by keep_messages)

    Args:
        thread_id: The thread/conversation ID to summarize
        keep_messages: Number of recent messages to preserve (1-20, default 5)

    Returns:
        JSON response with:
        - success: Whether summarization completed
        - thread_id: The thread ID
        - original_message_count: Number of messages before summarization
        - new_message_count: Number of messages after (1 summary + preserved)
        - summary_length: Character length of the generated summary

    Raises:
        404: Thread not found
        400: Not enough messages to summarize
        500: Checkpointer not initialized or other internal error

    Example:
        POST /api/v1/workflow/abc123/summarize?keep_messages=5
        Response: {
            "success": true,
            "thread_id": "abc123",
            "original_message_count": 45,
            "new_message_count": 6,
            "summary_length": 1234
        }
    """
    try:
        # Import dependencies
        from src.server.database import conversation as qr_db
        from src.server.services.workspace_manager import WorkspaceManager
        from ptc_agent.agent.graph import build_ptc_graph_with_session
        from ptc_agent.agent.middleware.summarization import summarize_messages
        from src.config.settings import get_summarization_config

        # 1. Validate thread exists and get workspace_id
        thread_info = await qr_db.get_thread_with_summary(thread_id)
        if not thread_info:
            raise HTTPException(
                status_code=404,
                detail=f"Thread not found: {thread_id}"
            )

        workspace_id = thread_info.get("workspace_id")
        if not workspace_id:
            raise HTTPException(
                status_code=400,
                detail=f"Thread {thread_id} has no associated workspace"
            )

        # 2. Get session for the workspace
        workspace_manager = WorkspaceManager.get_instance()
        try:
            session = await workspace_manager.get_session_for_workspace(workspace_id)
        except ValueError as e:
            raise HTTPException(status_code=404, detail=str(e))
        except RuntimeError as e:
            raise HTTPException(status_code=400, detail=str(e))

        # 3. Verify checkpointer is available
        checkpointer = get_checkpointer()

        # 4. Build graph to access state (use global config from setup)
        if not setup.agent_config:
            raise HTTPException(
                status_code=500,
                detail="Agent configuration not initialized"
            )

        graph = await build_ptc_graph_with_session(
            session=session,
            config=setup.agent_config,
            checkpointer=checkpointer,
        )

        # 5. Get current state
        config = build_checkpoint_config(thread_id)
        state = await graph.aget_state(config)

        if not state or not state.values:
            raise HTTPException(
                status_code=404,
                detail=f"No state found for thread: {thread_id}"
            )

        messages = state.values.get("messages", [])
        if not messages:
            raise HTTPException(
                status_code=400,
                detail="No messages to summarize"
            )

        original_count = len(messages)

        # 6. Call summarize_messages
        summarization_config = get_summarization_config()
        model_name = summarization_config.get("llm", "gpt-5-nano")

        try:
            result = await summarize_messages(
                messages=messages,
                keep_messages=keep_messages,
                model_name=model_name,
            )
        except ValueError as e:
            raise HTTPException(status_code=400, detail=str(e))

        # 7. Update state with summarized messages
        await graph.aupdate_state(
            config,
            {"messages": result["messages"]},
        )

        logger.info(
            f"Manual summarization completed for thread {thread_id}: "
            f"{original_count} -> {result['preserved_count']} messages"
        )

        return {
            "success": True,
            "thread_id": thread_id,
            "original_message_count": original_count,
            "new_message_count": result["preserved_count"],
            "summary_length": len(result.get("summary_text", "")),
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.exception(f"Error triggering summarization for thread {thread_id}: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to trigger summarization: {str(e)}"
        )
