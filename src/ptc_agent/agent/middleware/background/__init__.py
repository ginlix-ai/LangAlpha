"""Background subagent execution middleware.

This module provides async/background execution for subagent tasks,
allowing the main agent to continue working while subagents run.
"""

from ptc_agent.agent.middleware.background.counter import ToolCallCounterMiddleware
from ptc_agent.agent.middleware.background.middleware import (
    BackgroundSubagentMiddleware,
    current_background_agent_id,
    current_background_task_id,
)
from ptc_agent.agent.middleware.background.orchestrator import BackgroundSubagentOrchestrator
from ptc_agent.agent.middleware.background.registry import BackgroundTask, BackgroundTaskRegistry
from ptc_agent.agent.middleware.background.tools import (
    create_task_output_tool,
    create_wait_tool,
)

__all__ = [
    "BackgroundSubagentMiddleware",
    "BackgroundSubagentOrchestrator",
    "BackgroundTask",
    "BackgroundTaskRegistry",
    "ToolCallCounterMiddleware",
    "create_task_output_tool",
    "create_wait_tool",
    "current_background_agent_id",
    "current_background_task_id",
]
