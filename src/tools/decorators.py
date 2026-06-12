
import functools
import logging
from typing import Any, Callable, TypeVar, Optional
from contextvars import ContextVar
from collections import defaultdict

logger = logging.getLogger(__name__)

T = TypeVar("T")


# ========== Tool Usage Tracking ==========

class ToolUsageTracker:
    """
    Track tool usage counts for infrastructure cost calculation.

    This class is used to record how many times each tool is called
    during workflow execution, which is then used to calculate
    infrastructure credits (e.g., Tavily searches, analysis tools).
    """

    def __init__(self, thread_id: Optional[str] = None):
        """
        Initialize usage tracker with empty counts.

        Args:
            thread_id: Optional workflow thread identifier for tracker lookup
        """
        self.usage: dict[str, int] = defaultdict(int)
        self.thread_id = thread_id

    def record_usage(self, tool_name: str, count: int = 1) -> None:
        """
        Record tool usage.

        Args:
            tool_name: Tool class name (e.g., "TavilySearchTool")
            count: Number of uses (default: 1)
        """
        if count > 0:
            self.usage[tool_name] += count
            logger.debug(f"[ToolUsageTracker] Recorded {tool_name} x{count}")

    def get_summary(self) -> dict[str, int]:
        """
        Get usage summary as a regular dict.

        Returns:
            Dict mapping tool names to usage counts
        """
        return dict(self.usage)

    def reset(self) -> None:
        """Reset all usage counts."""
        self.usage.clear()

    def __repr__(self) -> str:
        total_calls = sum(self.usage.values())
        return f"ToolUsageTracker(tools={len(self.usage)}, total_calls={total_calls})"


# ContextVar storage for tool usage tracker
# This follows the same pattern as ExecutionTracker (agent message tracking)
_tool_usage_context: ContextVar[Optional[ToolUsageTracker]] = ContextVar(
    'tool_usage_context',
    default=None
)


def start_tool_tracking() -> ToolUsageTracker:
    """
    Start tracking tool usage for the current context.

    Returns:
        ToolUsageTracker instance

    Usage:
        tracker = start_tool_tracking()
        # ... tools are called ...
        usage_summary = tracker.get_summary()
    """
    tracker = ToolUsageTracker()
    _tool_usage_context.set(tracker)
    logger.debug("[ToolUsageTracker] Started tracking")
    return tracker


def set_tool_tracker(tracker: Optional[ToolUsageTracker]) -> None:
    """Install a specific tracker for the current context (e.g. a per-task
    tracker inside a background subagent's asyncio.Task)."""
    _tool_usage_context.set(tracker)


def get_tool_tracker() -> Optional[ToolUsageTracker]:
    """
    Get the current tool usage tracker from ContextVar.

    Returns:
        ToolUsageTracker instance or None if not tracking
    """
    tracker = _tool_usage_context.get()
    if tracker:
        logger.debug("[ToolUsageTracker] Found tracker via ContextVar")
    return tracker


def stop_tool_tracking() -> Optional[dict[str, int]]:
    """
    Stop tracking and return usage summary.

    Clears the ContextVar tracker.

    Returns:
        Usage summary dict or None if not tracking
    """
    tracker = _tool_usage_context.get()

    if tracker:
        summary = tracker.get_summary()
        # Clear ContextVar
        _tool_usage_context.set(None)

        return summary

    logger.warning("[ToolUsageTracker] stop_tool_tracking() called but no tracker found")
    return None


def log_io(func: Callable) -> Callable:
    """Decorator that logs input parameters and output of a function."""

    @functools.wraps(func)
    def wrapper(*args: Any, **kwargs: Any) -> Any:
        func_name = func.__name__
        params = ", ".join(
            [*(str(arg) for arg in args), *(f"{k}={v}" for k, v in kwargs.items())]
        )
        logger.debug(f"Tool call start {func_name} params: {params}")
        result = func(*args, **kwargs)
        logger.debug(f"Tool call end {func_name}")
        return result

    return wrapper


def create_logged_tool(
    tool_instance: T,
    name: Optional[str] = None,
    tracking_name: Optional[str] = None,
) -> T:
    """
    Wrap a StructuredTool instance with usage tracking.

    Args:
        tool_instance: A StructuredTool instance (from @tool decorator)
        name: Optional name override for the tool (LLM-facing)
        tracking_name: Optional separate name for usage tracking (e.g., "SerperSearchTool").
            Defaults to ``name`` when not provided. Use this to map provider-specific
            billing keys while keeping a generic LLM-facing tool name.

    Returns:
        A new tool instance with usage tracking
    """
    from langchain_core.tools import StructuredTool

    if not isinstance(tool_instance, StructuredTool):
        raise TypeError(f"Expected StructuredTool instance, got {type(tool_instance)}")

    original_coroutine = tool_instance.coroutine
    original_func = tool_instance.func
    tool_name = name or tool_instance.name
    usage_name = tracking_name or tool_name

    async def tracked_coroutine(*args: Any, **kwargs: Any) -> Any:
        tracker = get_tool_tracker()
        if tracker:
            tracker.record_usage(usage_name, count=1)
        return await original_coroutine(*args, **kwargs)

    def tracked_func(*args: Any, **kwargs: Any) -> Any:
        tracker = get_tool_tracker()
        if tracker:
            tracker.record_usage(usage_name, count=1)
        return original_func(*args, **kwargs)

    tracked_tool = tool_instance.copy()
    tracked_tool.name = tool_name
    if original_coroutine:
        tracked_tool.coroutine = tracked_coroutine
    if original_func:
        tracked_tool.func = tracked_func

    return tracked_tool
