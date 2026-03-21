"""Background subagent management tools.

This module provides tools for the main agent to interact with background
subagents: waiting for results and checking progress.
"""

from __future__ import annotations

import time
from typing import TYPE_CHECKING, Any

import structlog
from langchain_core.tools import StructuredTool
from langgraph.config import get_config

from ptc_agent.agent.middleware.background_subagent.utils import build_message_checker

if TYPE_CHECKING:
    from ptc_agent.agent.middleware.background_subagent.middleware import (
        BackgroundSubagentMiddleware,
    )
    from ptc_agent.agent.middleware.background_subagent.registry import (
        BackgroundTask,
    )

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


def create_task_output_tool(middleware: BackgroundSubagentMiddleware) -> StructuredTool:
    """Create tool to get background task output.

    This tool allows the main agent to get the output of background subagents.
    If the task is still running, it shows progress. If completed, it returns
    the cached result. When timeout > 0, blocks until task(s) complete with
    user-message-interruption support.

    Args:
        middleware: The BackgroundSubagentMiddleware instance

    Returns:
        A StructuredTool for getting task output
    """

    async def task_output(
        task_id: str | None = None,
        timeout: float = 0,
    ) -> str:
        """Get background task output.

        Args:
            task_id: Task ID (e.g., 'k7Xm2p') or None for all
            timeout: Max seconds to wait (0 = non-blocking, default)

        Returns:
            Result if completed, progress if still running
        """
        registry = middleware.registry
        blocking = timeout > 0

        if task_id is not None:
            task = await registry.get_by_task_id(task_id)
            if not task:
                return f"Task-{task_id} not found"

            # Sync completion status from asyncio task
            _sync_task_completion(task)

            # If already completed, return immediately regardless of timeout
            if task.completed:
                task.result_seen = True
                return (
                    f"**{task.display_id}** ({task.subagent_type}) completed:\n\n"
                    f"{_format_result(task.result)}"
                )

            if not blocking:
                return _format_task_progress(task)

            # Blocking: wait for this specific task
            logger.info(
                "Waiting for specific task",
                task_id=task_id,
                timeout=timeout,
            )
            thread_id = get_config().get("configurable", {}).get("thread_id")
            checker = await build_message_checker(thread_id)
            result = await registry.wait_for_specific(
                task_id, timeout, message_checker=checker
            )
            task = await registry.get_by_task_id(task_id)

            if task:
                if isinstance(result, dict) and result.get("status") == "interrupted":
                    return (
                        f"Wait interrupted: new user steering received. "
                        f"**{task.display_id}** ({task.subagent_type}) still running in background."
                    )
                if isinstance(result, dict) and result.get("status") == "timeout":
                    return (
                        f"**{task.display_id}** ({task.subagent_type}) still running "
                        f"(waited {timeout}s, task continues in background)"
                    )
                task.result_seen = True
                return (
                    f"**{task.display_id}** ({task.subagent_type}) completed:\n\n"
                    f"{_format_result(result)}"
                )
            return f"Task-{task_id} not found"

        # --- All tasks ---

        if not blocking:
            # Non-blocking: show current state of all tasks
            all_tasks = await registry.get_all_tasks()
            if not all_tasks:
                return "No background tasks have been assigned yet."

            for task in all_tasks:
                _sync_task_completion(task)

            pending_count = sum(1 for t in all_tasks if not t.completed)
            completed_count = len(all_tasks) - pending_count

            output = (
                f"**Background Tasks** ({len(all_tasks)} total: "
                f"{completed_count} completed, {pending_count} running)\n\n"
            )

            for task in sorted(all_tasks, key=lambda t: t.task_id):
                if task.completed:
                    task.result_seen = True
                    output += (
                        f"### {task.display_id} ({task.subagent_type})\n"
                        f"{_format_result(task.result)}\n\n"
                    )
                else:
                    output += _format_task_progress(task) + "\n"

            return output

        # Blocking: wait for all tasks
        logger.info("Waiting for all background tasks", timeout=timeout)
        thread_id = get_config().get("configurable", {}).get("thread_id")
        checker = await build_message_checker(thread_id)
        results = await registry.wait_for_all(timeout=timeout, message_checker=checker)

        if not results:
            return "No background tasks were pending."

        # Check for interruption
        any_interrupted = any(
            isinstance(r, dict) and r.get("status") == "interrupted"
            for r in results.values()
        )
        if any_interrupted:
            still_running = [
                registry.get_by_tool_call_id(tcid)
                for tcid, r in results.items()
                if isinstance(r, dict) and r.get("status") == "interrupted"
            ]
            running_names = ", ".join(f"**{t.display_id}**" for t in still_running if t)
            completed_parts = []
            for tcid, r in results.items():
                t = registry.get_by_tool_call_id(tcid)
                if t and not (isinstance(r, dict) and r.get("status") == "interrupted"):
                    t.result_seen = True
                    completed_parts.append(
                        f"### {t.display_id} ({t.subagent_type}) - completed\n"
                        f"{_format_result(r)}\n"
                    )
            output = (
                f"Wait interrupted: new user steering received. "
                f"Still running in background: {running_names}.\n\n"
            )
            if completed_parts:
                output += "\n".join(completed_parts)
            return output

        # Count completed vs still running
        completed_count = sum(
            1
            for r in results.values()
            if not (isinstance(r, dict) and r.get("status") == "timeout")
        )
        running_count = len(results) - completed_count

        if running_count == 0:
            output = f"All {len(results)} background task(s) completed:\n\n"
        elif completed_count == 0:
            output = f"All {len(results)} background task(s) still running (waited {timeout}s):\n\n"
        else:
            output = f"Background tasks: {completed_count} completed, {running_count} still running:\n\n"

        for tcid, result in results.items():
            task = registry.get_by_tool_call_id(tcid)
            if task:
                is_running = (
                    isinstance(result, dict) and result.get("status") == "timeout"
                )
                if not is_running:
                    task.result_seen = True
                status = "still running" if is_running else "completed"
                output += f"### {task.display_id} ({task.subagent_type}) - {status}\n"
                if not is_running:
                    output += _format_result(result) + "\n\n"
                else:
                    output += "\n"
        return output

    return StructuredTool.from_function(
        name="TaskOutput",
        description=(
            "Get the output of background subagent tasks. Returns the result "
            "if the task is completed, or shows progress if still running. "
            "Use TaskOutput(task_id=\"k7Xm2p\") for a specific task or "
            "TaskOutput() to see all tasks. "
            "Set timeout (seconds) to block until completion: "
            "TaskOutput(task_id=\"k7Xm2p\", timeout=60)."
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
        top_tools = sorted(task.tool_call_counts.items(), key=lambda x: -x[1])[:3]
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
