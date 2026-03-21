"""Background subagent execution middleware.

This module provides async/background execution for subagent tasks,
allowing the main agent to continue working while subagents run.
"""

from ptc_agent.agent.middleware.background_subagent.counter import ToolCallCounterMiddleware
from ptc_agent.agent.middleware.background_subagent.middleware import (
    BackgroundSubagentMiddleware,
    current_background_agent_id,
    current_background_tool_call_id,
)
from ptc_agent.agent.middleware.background_subagent.orchestrator import (
    BackgroundSubagentOrchestrator,
)
from ptc_agent.agent.middleware.background_subagent.registry import (
    BackgroundTask,
    BackgroundTaskRegistry,
)
from ptc_agent.agent.middleware.background_subagent.steering import (
    SubagentSteeringMiddleware,
)
from ptc_agent.agent.middleware.background_subagent.tools import (
    create_task_output_tool,
)

__all__ = [
    "BackgroundSubagentMiddleware",
    "BackgroundSubagentOrchestrator",
    "BackgroundTask",
    "BackgroundTaskRegistry",
    "SubagentSteeringMiddleware",
    "ToolCallCounterMiddleware",
    "create_task_output_tool",
    "current_background_agent_id",
    "current_background_tool_call_id",
]
