"""
Utility functions for workflow persistence operations.

This module provides helper functions to extract token usage, tool usage,
and calculate execution time from workflow metadata. These are used by
the LocalRunExecutor for workflow persistence at interrupt, error,
and cancellation points.
"""

import logging
import time
from typing import Any, Dict, Optional, List

logger = logging.getLogger(__name__)


def get_token_usage_from_callback(
    metadata: Dict[str, Any],
    context: str,
    thread_id: str
) -> tuple[Optional[Dict[str, Any]], Optional[list]]:
    """
    Extract token usage from callback if available.

    Args:
        metadata: Task metadata containing token_callback
        context: Context string for logging ("interrupt", "error", or "cancellation")
        thread_id: Thread ID for logging

    Returns:
        Tuple of (token_usage dict, per_call_records list) or (None, None)
    """
    token_usage = None
    per_call_records = None
    token_callback = metadata.get("token_callback")

    if token_callback and hasattr(token_callback, "per_call_records"):
        try:
            from src.utils.tracking import calculate_cost_from_per_call_records
            per_call_records = token_callback.per_call_records
            token_usage = calculate_cost_from_per_call_records(per_call_records)
            logger.debug(
                f"[WorkflowPersistence] Captured token usage at {context}: "
                f"thread_id={thread_id} calls={len(per_call_records)}"
            )
        except Exception as e:
            logger.warning(
                f"[WorkflowPersistence] Failed to get token usage at {context} "
                f"thread_id={thread_id}: {e}"
            )

    return token_usage, per_call_records


def get_tool_usage_from_handler(
    metadata: Dict[str, Any],
    context: str,
    thread_id: str
) -> Optional[Dict[str, int]]:
    """
    Extract tool usage from handler if available.

    Args:
        metadata: Task metadata containing handler
        context: Context string for logging ("interrupt", "error", or "cancellation")
        thread_id: Thread ID for logging

    Returns:
        Tool usage dict or None
    """
    tool_usage = None
    try:
        handler = metadata.get("handler")
        if handler:
            tool_usage = handler.get_tool_usage()
            if tool_usage:
                logger.debug(
                    f"[WorkflowPersistence] Captured tool usage at {context}: "
                    f"thread_id={thread_id} tools={tool_usage}"
                )
            else:
                logger.debug(
                    f"[WorkflowPersistence] No tool usage at {context} "
                    f"thread_id={thread_id}"
                )
        else:
            logger.debug(
                f"[WorkflowPersistence] No handler available for tool usage at {context}"
            )
    except Exception as e:
        logger.warning(
            f"[WorkflowPersistence] Failed to get tool usage at {context} "
            f"thread_id={thread_id}: {e}"
        )

    return tool_usage


def get_sse_events_from_handler(
    metadata: Dict[str, Any],
    context: str,
    thread_id: str
) -> Optional[List[Dict[str, Any]]]:
    """Extract merged SSE events from handler if available."""
    try:
        handler = metadata.get("handler")
        if handler and hasattr(handler, "get_sse_events"):
            events = handler.get_sse_events()
            if events:
                logger.debug(
                    f"[WorkflowPersistence] Captured SSE events at {context}: "
                    f"thread_id={thread_id} events={len(events)}"
                )
            return events
    except Exception as e:
        logger.warning(
            f"[WorkflowPersistence] Failed to get SSE events at {context} "
            f"thread_id={thread_id}: {e}"
        )

    return None


def calculate_execution_time(metadata: Dict[str, Any]) -> Optional[float]:
    """
    Calculate execution time from start_time in metadata.

    Args:
        metadata: Task metadata containing start_time

    Returns:
        Execution time in seconds or None
    """
    start_time = metadata.get("start_time")
    if start_time:
        return time.time() - start_time
    return None
