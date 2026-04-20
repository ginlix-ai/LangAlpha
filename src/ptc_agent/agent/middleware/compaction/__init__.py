"""Compaction middleware for LangChain agents.

This module provides SSE-enabled context compaction middleware that emits custom
events for frontend visibility. Compaction covers the full context window lifecycle:
token counting, tool-argument truncation, base64 offloading, sandbox persistence of
evicted messages, and LLM-based summarization.
"""

from ptc_agent.agent.middleware.compaction.middleware import (
    CompactionMiddleware,
)
from ptc_agent.agent.middleware.compaction.types import (
    CompactionEvent,
    CompactionState,
)
from ptc_agent.agent.middleware.compaction.utils import (
    DEFAULT_SUMMARY_PROMPT,
    build_summary_message,
    compute_absolute_cutoff,
    count_tokens_tiktoken,
    get_effective_messages,
    strip_base64_from_content,
    strip_base64_from_messages,
)
from ptc_agent.agent.middleware.compaction.offloading import (
    aoffload_base64_content,
)
from ptc_agent.agent.middleware.compaction.compact import (
    compact_messages,
    offload_tool_args,
)

__all__ = [
    "CompactionMiddleware",
    "CompactionEvent",
    "CompactionState",
    "DEFAULT_SUMMARY_PROMPT",
    "aoffload_base64_content",
    "build_summary_message",
    "compact_messages",
    "compute_absolute_cutoff",
    "count_tokens_tiktoken",
    "get_effective_messages",
    "offload_tool_args",
    "strip_base64_from_content",
    "strip_base64_from_messages",
]
