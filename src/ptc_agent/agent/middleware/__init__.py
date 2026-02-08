"""Agent middleware components.

This module provides middleware for LangChain/LangGraph agents:

- background/: Background subagent orchestration
- plan_mode: Human-in-the-loop plan review
- tool/: Tool argument parsing, error handling, result normalization
- caching/: Tool result caching with SSE events
- file_operations/: File operation SSE event emission and vision middleware
- summarization/: SSE-enabled summarization
"""

# Background subagent middleware
from ptc_agent.agent.middleware.background import (
    BackgroundSubagentMiddleware,
    BackgroundSubagentOrchestrator,
    ToolCallCounterMiddleware,
)

# Plan mode middleware
from ptc_agent.agent.middleware.plan_mode import (
    PlanModeMiddleware,
    create_plan_mode_interrupt_config,
)

# Tool middleware (argument parsing, error handling, result normalization)
from ptc_agent.agent.middleware.tool import (
    ToolArgumentParsingMiddleware,
    ToolErrorHandlingMiddleware,
    ToolResultNormalizationMiddleware,
    simplify_tool_error,
)

# Caching middleware
from ptc_agent.agent.middleware.caching import (
    ToolResultCacheMiddleware,
    ToolResultCacheState,
)

# File operations middleware (includes MultimodalMiddleware for images/PDFs)
from ptc_agent.agent.middleware.file_operations import (
    FileOperationMiddleware,
    FileOperationState,
    MultimodalMiddleware,
)

# Todo operations middleware
from ptc_agent.agent.middleware.todo_operations import (
    TodoWriteMiddleware,
)

# Summarization middleware
from ptc_agent.agent.middleware.summarization import (
    CustomSummarizationMiddleware,
    SummarizationMiddleware,
    DEFAULT_SUMMARY_PROMPT,
    count_tokens_tiktoken,
)

# Dynamic skill loader middleware
from ptc_agent.agent.middleware.dynamic_skill_loader import (
    DynamicSkillLoaderMiddleware,
)

# Large result eviction middleware
from ptc_agent.agent.middleware.large_result_eviction import (
    LargeResultEvictionMiddleware,
)

# Subagent middleware
from ptc_agent.agent.middleware.subagents import (
    CompiledSubAgent,
    SubAgent,
    SubAgentMiddleware,
)

__all__ = [
    # Background subagent
    "BackgroundSubagentMiddleware",
    "BackgroundSubagentOrchestrator",
    "ToolCallCounterMiddleware",
    # Plan mode
    "PlanModeMiddleware",
    "create_plan_mode_interrupt_config",
    # Multimodal middleware (for read_file image/PDF support)
    "MultimodalMiddleware",
    # Tool middleware
    "ToolArgumentParsingMiddleware",
    "ToolErrorHandlingMiddleware",
    "ToolResultNormalizationMiddleware",
    "simplify_tool_error",
    # Caching
    "ToolResultCacheMiddleware",
    "ToolResultCacheState",
    # File operations
    "FileOperationMiddleware",
    "FileOperationState",
    # Todo operations
    "TodoWriteMiddleware",
    # Summarization
    "CustomSummarizationMiddleware",
    "SummarizationMiddleware",
    "DEFAULT_SUMMARY_PROMPT",
    "count_tokens_tiktoken",
    # Dynamic skill loader
    "DynamicSkillLoaderMiddleware",
    # Large result eviction
    "LargeResultEvictionMiddleware",
    # Subagent middleware
    "CompiledSubAgent",
    "SubAgent",
    "SubAgentMiddleware",
]
