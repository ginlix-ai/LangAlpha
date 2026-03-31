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
from .background_subagent import (
    BackgroundSubagentMiddleware,
    BackgroundSubagentOrchestrator,
    ToolCallCounterMiddleware,
)

# Plan mode middleware
from .plan_mode import (
    PlanModeMiddleware,
    create_plan_mode_interrupt_config,
)

# Ask user middleware
from .ask_user import AskUserMiddleware

# Tool middleware (argument parsing, error handling, result normalization, leak detection, code validation, empty call retry)
from .tool import (
    CodeValidationMiddleware,
    EmptyToolCallRetryMiddleware,
    LeakDetectionMiddleware,
    ProtectedPathMiddleware,
    ToolArgumentParsingMiddleware,
    ToolErrorHandlingMiddleware,
    ToolResultNormalizationMiddleware,
    simplify_tool_error,
)

# Caching middleware
from .caching import (
    ToolResultCacheMiddleware,
    ToolResultCacheState,
)

# File operations middleware (includes MultimodalMiddleware for images/PDFs)
from .file_operations import (
    FileOperationMiddleware,
    FileOperationState,
    MultimodalMiddleware,
)

# Todo operations middleware
from .todo_operations import (
    TodoWriteMiddleware,
)

# Summarization middleware
from .summarization import (
    SummarizationMiddleware,
    DEFAULT_SUMMARY_PROMPT,
    count_tokens_tiktoken,
)

# Skills middleware (registry + dynamic loader)
from .skills import (
    SkillsMiddleware,
)

# Large result eviction middleware
from .large_result_eviction import (
    LargeResultEvictionMiddleware,
)

# Steering middleware
from .steering import (
    SteeringMiddleware,
)

# Workspace context middleware (agent.md injection)
from .workspace_context import (
    WorkspaceContextMiddleware,
)

# Subagent steering middleware
from .background_subagent.steering import (
    SubagentSteeringMiddleware,
)

# Subagent middleware
from .background_subagent.subagent import (
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
    # Ask user
    "AskUserMiddleware",
    # Multimodal middleware (for read_file image/PDF support)
    "MultimodalMiddleware",
    # Tool middleware
    "CodeValidationMiddleware",
    "EmptyToolCallRetryMiddleware",
    "LeakDetectionMiddleware",
    "ProtectedPathMiddleware",
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
    "SummarizationMiddleware",
    "DEFAULT_SUMMARY_PROMPT",
    "count_tokens_tiktoken",
    # Skills
    "SkillsMiddleware",
    # Large result eviction
    "LargeResultEvictionMiddleware",
    # Steering
    "SteeringMiddleware",
    # Subagent steering
    "SubagentSteeringMiddleware",
    # Workspace context
    "WorkspaceContextMiddleware",
    # Subagent middleware
    "CompiledSubAgent",
    "SubAgent",
    "SubAgentMiddleware",
]
