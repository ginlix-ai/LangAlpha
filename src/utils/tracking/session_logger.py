"""
Session logging for workflow executions.

Encapsulates all session logging logic for saving query-response pairs,
state snapshots, token usage, and execution metrics to database and file logs.
"""

import logging
import time
from datetime import datetime
from typing import Any, Dict, Optional
from uuid import uuid4

from langchain_core.messages import AIMessage

from .core import ExecutionTracker, add_cost_to_token_usage, calculate_cost_from_per_call_records, serialize_agent_message

logger = logging.getLogger(__name__)


class SessionLogger:
    """
    Handles session logging for workflow executions.

    Consolidates logic for:
    - Token usage extraction and cost calculation
    - Final report extraction from agent messages
    - Response status determination
    - State snapshot serialization
    - Session data building
    - Result logging to database/file
    """

    def __init__(self):
        """Initialize session logger."""
        pass

    async def save_session_log_async(
        self,
        *,
        thread_id: str,
        conversation_id: str,
        user_id: str,
        query_metadata: Dict[str, Any],
        start_time: float,
        success: bool,
        interrupt_detected: bool,
        token_callback: Optional[Any],
        graph: Any,
        msg_type: str,
        stock_code: str,
    ) -> None:
        """
        Save complete session log with query-response pair.

        Args:
            thread_id: Thread identifier
            conversation_id: Conversation identifier
            user_id: User identifier
            query_metadata: Query metadata dict
            start_time: Execution start time
            success: Whether execution succeeded
            interrupt_detected: Whether interrupt was detected during streaming
            token_callback: UsageMetadataCallbackHandler instance
            graph: LangGraph workflow graph
            msg_type: Message type (chat, technical_analysis, etc.)
            stock_code: Stock code for analysis
        """
        try:
            execution_time = time.time() - start_time

            # Import required modules
            from src.llms.result_logger import get_result_logger
            from src.llms import format_llm_content
            from src.server.models.workflow import serialize_state_snapshot
            # Generate IDs for query-response pair
            result_logger = get_result_logger()
            query_id = str(uuid4())
            response_id = str(uuid4())

            # Extract token usage if tracking was enabled
            token_usage_with_cost = None
            if token_callback:
                # Calculate costs from per-call records for accurate tiered pricing
                per_call_records = token_callback.per_call_records
                if per_call_records:
                    token_usage_with_cost = calculate_cost_from_per_call_records(per_call_records)
                    logger.debug(f"Token usage tracked for thread_id={thread_id}: {len(per_call_records)} calls")

            # Get execution tracking context
            tracking_context = ExecutionTracker.get_context()

            # Extract final report with reasoning from tracked agent messages
            # Try different agent types based on workflow
            final_content = None

            # Priority 1: Try reporter (deep_research workflow)
            if tracking_context and tracking_context.agent_messages.get("reporter"):
                reporter_messages = tracking_context.agent_messages["reporter"]
                reporter_ai_messages = [msg for msg in reporter_messages if isinstance(msg, AIMessage)]
                if reporter_ai_messages:
                    last_reporter_msg = reporter_ai_messages[-1]
                    additional_kwargs = last_reporter_msg.additional_kwargs if hasattr(last_reporter_msg, 'additional_kwargs') else {}
                    final_content = format_llm_content(
                        last_reporter_msg.content if hasattr(last_reporter_msg, 'content') else "",
                        additional_kwargs=additional_kwargs
                    )
                    logger.debug(f"Final report extracted from reporter with reasoning={bool(final_content.get('reasoning'))}")

            # Priority 2: Try technical_analysis (technical_analysis subgraph)
            elif tracking_context and tracking_context.agent_messages.get("technical_analysis"):
                ta_messages = tracking_context.agent_messages["technical_analysis"]
                ta_ai_messages = [msg for msg in ta_messages if isinstance(msg, AIMessage)]
                if ta_ai_messages:
                    last_ta_msg = ta_ai_messages[-1]
                    additional_kwargs = last_ta_msg.additional_kwargs if hasattr(last_ta_msg, 'additional_kwargs') else {}
                    final_content = format_llm_content(
                        last_ta_msg.content if hasattr(last_ta_msg, 'content') else "",
                        additional_kwargs=additional_kwargs
                    )
                    logger.debug(f"Final report extracted from technical_analysis with reasoning={bool(final_content.get('reasoning'))}")

            # Priority 3: Try fundamental_analysis (fundamental_analysis subgraph)
            elif tracking_context and tracking_context.agent_messages.get("fundamental_analysis"):
                fa_messages = tracking_context.agent_messages["fundamental_analysis"]
                fa_ai_messages = [msg for msg in fa_messages if isinstance(msg, AIMessage)]
                if fa_ai_messages:
                    last_fa_msg = fa_ai_messages[-1]
                    additional_kwargs = last_fa_msg.additional_kwargs if hasattr(last_fa_msg, 'additional_kwargs') else {}
                    final_content = format_llm_content(
                        last_fa_msg.content if hasattr(last_fa_msg, 'content') else "",
                        additional_kwargs=additional_kwargs
                    )
                    logger.debug(f"Final report extracted from fundamental_analysis with reasoning={bool(final_content.get('reasoning'))}")

            # Priority 4: Try direct_response (fast-path direct response from deep_research)
            elif tracking_context and tracking_context.agent_messages.get("direct_response"):
                dr_messages = tracking_context.agent_messages["direct_response"]
                dr_ai_messages = [msg for msg in dr_messages if isinstance(msg, AIMessage)]
                if dr_ai_messages:
                    last_dr_msg = dr_ai_messages[-1]
                    additional_kwargs = last_dr_msg.additional_kwargs if hasattr(last_dr_msg, 'additional_kwargs') else {}
                    final_content = format_llm_content(
                        last_dr_msg.content if hasattr(last_dr_msg, 'content') else "",
                        additional_kwargs=additional_kwargs
                    )
                    logger.debug(f"Final report extracted from direct_response with reasoning={bool(final_content.get('reasoning'))}")

            # Determine response status and interrupt reason
            response_status = "completed" if success else "error"
            interrupt_reason = None
            final_state = None  # Initialize for state snapshot extraction

            # Check final state for interrupt
            try:
                final_config = {"configurable": {"thread_id": thread_id}}
                final_state = await graph.aget_state(final_config)
                if final_state and final_state.next:
                    response_status = "interrupted"
                    # Use streaming-based detection instead of state check
                    if interrupt_detected:
                        interrupt_reason = "plan_review_required"
            except Exception as e:
                logger.debug(f"Could not retrieve final state for interrupt detection: {e}")

            # Extract state snapshot for database logging
            raw_state = final_state.values if (final_state and hasattr(final_state, 'values')) else None

            # Serialize state snapshot to make it JSON-safe before storing in database
            state_snapshot = serialize_state_snapshot(raw_state) if raw_state else None

            if state_snapshot:
                logger.debug(f"State snapshot serialized for thread_id={thread_id}, keys={list(state_snapshot.keys())}")
            else:
                logger.debug(f"No state snapshot available for thread_id={thread_id}")

            # Serialize agent messages if available
            organized_messages = {}
            if tracking_context and tracking_context.agent_messages:
                for agent_name, msgs in tracking_context.agent_messages.items():
                    organized_messages[agent_name] = [
                        {**serialize_agent_message(msg), "message_index": idx}
                        for idx, msg in enumerate(msgs)
                    ]

            # Build session data with query-response structure
            session_data = {
                "thread_id": thread_id,
                "conversation_id": conversation_id,
                "user_id": user_id,
                "query_id": query_id,
                "query": query_metadata,
                "response_id": response_id,
                "response": {
                    "final_output": {
                        "text": final_content["text"] if final_content else None,
                        "reasoning": final_content["reasoning"] if final_content else None
                    },
                    "status": response_status,
                    "interrupt_reason": interrupt_reason,
                    "token_usage": token_usage_with_cost,
                    "agent_messages": organized_messages,
                    "execution_metrics": tracking_context.metrics if tracking_context else {},
                    "warnings": tracking_context.warnings if tracking_context else [],
                    "errors": tracking_context.errors if tracking_context else [],
                    "execution_time": execution_time,
                    "timestamp": datetime.now().isoformat(),
                    "metadata": {
                        "msg_type": msg_type,
                        "stock_code": stock_code,
                    }
                }
            }

            # Save result log
            await result_logger.save_result_async(session_data, state_snapshot)
            logger.info(f"Execution log saved for thread_id={thread_id} (state_snapshot={'captured' if state_snapshot else 'not available'})")

        except Exception as log_error:
            logger.error(f"Failed to save execution log for thread_id={thread_id}: {log_error}")
        finally:
            # Always stop execution tracking
            ExecutionTracker.stop_tracking()
            logger.debug(f"Execution tracking stopped for thread_id={thread_id}")


# Public API
__all__ = [
    'SessionLogger',
]
