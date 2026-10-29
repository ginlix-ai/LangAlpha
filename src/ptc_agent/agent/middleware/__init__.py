"""Agent middleware components.

This module provides middleware for LangChain/LangGraph agents:

- background/: Background subagent orchestration
- tool/: Tool argument parsing, error handling, result normalization
- caching/: Tool result caching with SSE events
- file_operations/: File operation SSE event emission and vision middleware
- compaction/: SSE-enabled context window compaction
"""

# Background subagent middleware
from .background_subagent import (
    BackgroundSubagentMiddleware,
    BackgroundSubagentOrchestrator,
    SubagentEventCaptureMiddleware,
)

# Ask user middleware
from .ask_user import AskUserMiddleware

# Runtime credit gate (model-boundary spend enforcement). Only the middleware
# is re-exported: the lease, the lane state and the ContextVar are the gate's
# own wiring and their callers import them from the module directly.
from .credit_gate import CreditGateMiddleware
from .repeat_guard import RepeatCallGuardMiddleware

# Tool middleware (argument parsing, error handling, result normalization, leak detection, code validation, empty call retry)
from .tool import (
    CodeValidationMiddleware,
    ComputerArgumentGuardMiddleware,
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

# File operations middleware (includes the multimodal write/read halves)
from .file_operations import (
    FileOperationMiddleware,
    FileOperationState,
    MultimodalMiddleware,
    MultimodalStripMiddleware,
)

# Todo operations middleware
from .todo_operations import (
    TodoWriteMiddleware,
)

# Provenance middleware (data-access tracing → provenance SSE events)
from .provenance import (
    ProvenanceMiddleware,
)

# Compaction middleware
from .compaction import (
    CompactionMiddleware,
    DEFAULT_SUMMARY_PROMPT,
    count_tokens_tiktoken,
    resolve_compaction_client,
)

# Skills middleware (registry + dynamic loader)
from .skills import (
    SkillsMiddleware,
)

# Large result eviction middleware
from .large_result_eviction import (
    LargeResultEvictionMiddleware,
)

# Runtime context: the per-thread baseline block plus the tail envelope carried
# on every model call. Imported ahead of market_watch, which contributes a
# per-call row to the envelope.
from .runtime_context import (
    BaselineContextMiddleware,
    RowCarrierMiddleware,
)

# Market watch middleware (live price injection for watched tickers)
from .market_watch import (
    MarketWatchMiddleware,
)

# Steering middleware
from .steering import (
    SteeringMiddleware,
)

# System-prompt cache breakpoint middleware (OpenAI GPT-5.6+, Claude over OpenRouter)
from .system_prompt_caching import (
    SystemPromptCacheMiddleware,
)

# Cross-provider reasoning sanitizer (origin + shape gates, origin stamping)
from .reasoning_compat import (
    ReasoningCompatibilityMiddleware,
)

# Stream heartbeat (wakes the stream's delta lanes while a model call runs)
from .stream_heartbeat import (
    StreamHeartbeatMiddleware,
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
    "SubagentEventCaptureMiddleware",
    # Ask user
    "AskUserMiddleware",
    "CreditGateMiddleware",
    "RepeatCallGuardMiddleware",
    # Multimodal middleware (for read_file image/PDF support)
    "MultimodalMiddleware",
    "MultimodalStripMiddleware",
    # Tool middleware
    "CodeValidationMiddleware",
    "ComputerArgumentGuardMiddleware",
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
    # Provenance
    "ProvenanceMiddleware",
    # Compaction
    "CompactionMiddleware",
    "DEFAULT_SUMMARY_PROMPT",
    "count_tokens_tiktoken",
    "resolve_compaction_client",
    # Skills
    "SkillsMiddleware",
    # Large result eviction
    "LargeResultEvictionMiddleware",
    # Market watch
    "MarketWatchMiddleware",
    # Steering
    "SteeringMiddleware",
    # Subagent steering
    "SubagentSteeringMiddleware",
    # Runtime context
    "BaselineContextMiddleware",
    "RowCarrierMiddleware",
    # OpenAI prompt caching
    "SystemPromptCacheMiddleware",
    # Cross-provider reasoning sanitizer
    "ReasoningCompatibilityMiddleware",
    # Stream heartbeat
    "StreamHeartbeatMiddleware",
    # Subagent middleware
    "CompiledSubAgent",
    "SubAgent",
    "SubAgentMiddleware",
]
