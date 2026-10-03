"""Compaction middleware for LangChain agents.

This module provides SSE-enabled context compaction middleware that emits custom
events for frontend visibility. Compaction covers the full context window lifecycle:
token counting, tool-argument truncation, base64 offloading, and LLM-based
summarization that points the model at the thread's transcript in the sandbox.
"""

from ptc_agent.agent.middleware.compaction.middleware import (
    CompactionMiddleware,
)
from ptc_agent.agent.middleware.compaction.types import (
    CompactionEvent,
    CompactionState,
    OffloadSettings,
)
from ptc_agent.agent.middleware.compaction.summary_request import (
    DEFAULT_SUMMARY_PROMPT,
)
from ptc_agent.agent.middleware.compaction.utils import (
    build_compaction_event,
    build_summary_message,
    count_tokens_tiktoken,
    find_group_safe_cutoff,
    get_effective_messages,
    parse_summary_message,
    resolve_cutoff_index,
    strip_base64_from_content,
    strip_base64_from_messages,
    partition_at_cutoff,
    strip_orphan_tool_messages,
)
from ptc_agent.agent.middleware.compaction.model import resolve_compaction_client
from ptc_agent.agent.middleware.compaction.offloading import (
    aoffload_base64_content,
    record_offloads,
)
from ptc_agent.agent.middleware.compaction.compact import (
    Compaction,
    Summarizer,
    compact_messages,
    offload_tool_args,
)

__all__ = [
    "Compaction",
    "CompactionMiddleware",
    "CompactionEvent",
    "CompactionState",
    "OffloadSettings",
    "Summarizer",
    "DEFAULT_SUMMARY_PROMPT",
    "aoffload_base64_content",
    "build_compaction_event",
    "build_summary_message",
    "parse_summary_message",
    "compact_messages",
    "count_tokens_tiktoken",
    "find_group_safe_cutoff",
    "get_effective_messages",
    "resolve_cutoff_index",
    "offload_tool_args",
    "resolve_compaction_client",
    "strip_base64_from_content",
    "strip_base64_from_messages",
    "partition_at_cutoff",
    "record_offloads",
    "strip_orphan_tool_messages",
]
