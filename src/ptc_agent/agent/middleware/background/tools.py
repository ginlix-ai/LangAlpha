"""Background subagent management tools.

This module provides tools for the main agent to interact with background
subagents: waiting for results and checking progress.
"""

from __future__ import annotations

import time
from typing import TYPE_CHECKING, Any

import structlog
from langchain_core.tools import StructuredTool

if TYPE_CHECKING:
    from ptc_agent.agent.middleware.background.middleware import BackgroundSubagentMiddleware
    from ptc_agent.agent.middleware.background.registry import BackgroundTask, BackgroundTaskRegistry

logger = structlog.get_logger(__name__)


def _sync_task_completion(task: BackgroundTask) -> None:
    """Sync task completion status from asyncio task.

    If the asyncio task is done but task.completed is False,
    update task.completed and task.result.
    """
    if task.completed:
        return
    if task.asyncio_task is None:
        return
    if not task.asyncio_task.done():
        return

    # Task finished but not yet synced
    task.completed = True
    try:
        task.result = task.asyncio_task.result()
    except Exception as e:
        task.error = str(e)
        task.result = {"success": False, "error": str(e)}


def create_wait_tool(middleware: BackgroundSubagentMiddleware) -> StructuredTool:
    """Create the wait tool for entering the waiting room.

    This tool allows the main agent to explicitly wait for background
    subagent(s) to complete and retrieve their results.

    Args:
        middleware: The BackgroundSubagentMiddleware instance

    Returns:
        A StructuredTool for waiting
    """

    async def wait_for_subagents(
        task_number: int | None = None,
        timeout: float = 60.0,
    ) -> str:
        """Wait for background task(s) to complete.

        Args:
            task_number: Task number (1, 2, ...) or None for all
            timeout: Max seconds (default: 60)

        Returns:
            Task result(s)
        """
        registry = middleware.registry

        if task_number is not None:
            # Wait for specific task
            logger.info(
                "Waiting for specific task",
                task_number=task_number,
                timeout=timeout,
            )
            result = await registry.wait_for_specific(task_number, timeout)
            task = await registry.get_by_number(task_number)

            if task:
                # Check if still running (wait timed out but task continues)
                if isinstance(result, dict) and result.get("status") == "timeout":
                    # Don't mark as seen - task is still running
                    return (
                        f"**{task.display_id}** ({task.subagent_type}) still running "
                        f"(waited {timeout}s, task continues in background)"
                    )
                task.result_seen = True  # Mark as seen only if completed
                return (
                    f"**{task.display_id}** ({task.subagent_type}) completed:\n\n"
                    f"{_format_result(result)}"
                )
            return f"Task-{task_number} not found"
        # Wait for all tasks
        logger.info("Waiting for all background tasks", timeout=timeout)
        results = await registry.wait_for_all(timeout=timeout)
        # Don't store in _pending_results - results are returned directly
        # to the agent via the tool response. Storing them would cause
        # the orchestrator to inject a duplicate HumanMessage later.

        if not results:
            return "No background tasks were pending."

        # Count completed vs still running
        completed_count = sum(
            1 for r in results.values()
            if not (isinstance(r, dict) and r.get("status") == "timeout")
        )
        running_count = len(results) - completed_count

        if running_count == 0:
            output = f"All {len(results)} background task(s) completed:\n\n"
        elif completed_count == 0:
            output = f"All {len(results)} background task(s) still running (waited {timeout}s):\n\n"
        else:
            output = f"Background tasks: {completed_count} completed, {running_count} still running:\n\n"

        for task_id, result in results.items():
            task = registry.get_by_id(task_id)
            if task:
                is_running = isinstance(result, dict) and result.get("status") == "timeout"
                if not is_running:
                    task.result_seen = True  # Mark as seen only if completed
                status = "still running" if is_running else "completed"
                output += f"### {task.display_id} ({task.subagent_type}) - {status}\n"
                if not is_running:
                    output += _format_result(result) + "\n\n"
                else:
                    output += "\n"
        return output

    return StructuredTool.from_function(
        name="Wait",
        description=(
            "Wait for background subagent(s) to complete and retrieve their results. "
            "Use Wait(task_number=1) for a specific task or Wait() for all pending tasks. "
            "You can also specify a custom timeout in seconds."
        ),
        coroutine=wait_for_subagents,
    )


def create_task_output_tool(registry: BackgroundTaskRegistry) -> StructuredTool:
    """Create tool to get background task output.

    This tool allows the main agent to get the output of background subagents.
    If the task is still running, it shows progress. If completed, it returns
    the cached result.

    Args:
        registry: The BackgroundTaskRegistry instance

    Returns:
        A StructuredTool for getting task output
    """

    async def task_output(task_number: int | None = None) -> str:
        """Get background task output.

        Args:
            task_number: Task number or None for all

        Returns:
            Result if completed, progress if still running
        """
        if task_number is not None:
            task = await registry.get_by_number(task_number)
            if not task:
                return f"Task-{task_number} not found"
            # Sync completion status from asyncio task
            _sync_task_completion(task)
            # If completed, return the result
            if task.completed:
                task.result_seen = True  # Mark as seen
                return (
                    f"**{task.display_id}** ({task.subagent_type}) completed:\n\n"
                    f"{_format_result(task.result)}"
                )
            # If still running, show progress
            return _format_task_progress(task)

        # Show all tasks
        all_tasks = await registry.get_all_tasks()
        if not all_tasks:
            return "No background tasks have been assigned yet."

        # Sync completion status for all tasks
        for task in all_tasks:
            _sync_task_completion(task)

        pending_count = sum(1 for t in all_tasks if not t.completed)
        completed_count = len(all_tasks) - pending_count

        output = (
            f"**Background Tasks** ({len(all_tasks)} total: "
            f"{completed_count} completed, {pending_count} running)\n\n"
        )

        for task in sorted(all_tasks, key=lambda t: t.task_number):
            if task.completed:
                task.result_seen = True  # Mark as seen
                output += (
                    f"### {task.display_id} ({task.subagent_type})\n"
                    f"{_format_result(task.result)}\n\n"
                )
            else:
                output += _format_task_progress(task) + "\n"

        return output

    return StructuredTool.from_function(
        name="TaskOutput",
        description=(
            "Get the output of background subagent tasks. Returns the result "
            "if the task is completed, or shows progress if still running. "
            "Use TaskOutput(task_number=1) for a specific task or "
            "TaskOutput() to see all tasks."
        ),
        coroutine=task_output,
    )


def extract_result_content(result: dict[str, Any] | Any) -> tuple[bool, str]:
    """Extract content from a task result.

    Handles various result types including raw values, dicts with success/error,
    objects with .content attribute, and Command types with .update.messages.

    Args:
        result: The task result (dict, Command, or raw value)

    Returns:
        Tuple of (success: bool, content: str)
    """
    if not isinstance(result, dict):
        return (True, str(result))

    if result.get("success"):
        inner = result.get("result")
        if inner is None:
            return (True, "Task completed successfully (no output)")
        if hasattr(inner, "content"):
            return (True, str(inner.content))
        # Handle Command type
        if hasattr(inner, "update"):
            update = inner.update
            if isinstance(update, dict) and "messages" in update:
                messages = update["messages"]
                if messages:
                    last_msg = messages[-1]
                    if hasattr(last_msg, "content"):
                        return (True, str(last_msg.content))
        return (True, str(inner))

    error = result.get("error", "Unknown error")
    status = result.get("status", "error")
    return (False, f"{status.upper()}: {error}")


def _format_result(result: dict[str, Any] | Any) -> str:
    """Format a single task result for display.

    Args:
        result: The task result dict

    Returns:
        Formatted string
    """
    success, content = extract_result_content(result)
    if success:
        return content
    return f"**{content}**"


def _format_task_progress(task: BackgroundTask) -> str:
    """Format progress info for a single task.

    Args:
        task: The BackgroundTask to format

    Returns:
        Formatted progress string
    """
    elapsed = time.time() - task.created_at

    # Status indicator
    status = ("[ERROR]" if task.error else "[DONE]") if task.completed else "[RUNNING]"

    # Tool call summary (always show, even if 0)
    tool_summary = f" | {task.total_tool_calls} tool calls"
    if task.tool_call_counts:
        # Show top 3 tools
        top_tools = sorted(
            task.tool_call_counts.items(),
            key=lambda x: -x[1]
        )[:3]
        tool_details = ", ".join(f"{t}: {c}" for t, c in top_tools)
        tool_summary += f" ({tool_details})"

    # Current activity (only for running tasks)
    activity = ""
    if not task.completed and task.current_tool:
        activity = f"\n  Currently executing: `{task.current_tool}`"

    return (
        f"### {task.display_id}: {task.subagent_type}\n"
        f"  Status: {status} | Elapsed: {elapsed:.1f}s{tool_summary}{activity}\n"
        f"  Task: {task.description[:100]}{'...' if len(task.description) > 100 else ''}"
    )
