"""Types, constants, and defaults for the compaction middleware."""

from collections.abc import Callable, Iterable
from typing import Annotated, Literal, NotRequired

from langchain_core.messages import MessageLikeRepresentation
from langchain_core.messages.human import HumanMessage
from typing_extensions import TypedDict

from langchain.agents.middleware.types import AgentState, PrivateStateAttr


# Constant for context summary prefix - used in both standalone function and middleware
CONTEXT_SUMMARY_PREFIX = (
    "[Context Summary]\n"
    "This session is being continued from a previous conversation "
    "that ran out of context. The conversation is summarized below:\n\n"
)


# =============================================================================
# Types for wrap_model_call compaction tracking
# =============================================================================


class CompactionEvent(TypedDict):
    """Represents a compaction event for chained tracking.

    Stored in private state so the middleware can reconstruct the effective
    message list on subsequent model calls without modifying the checkpoint.
    """

    cutoff_index: int
    summary_message: HumanMessage
    file_path: str | None


class TruncateArgsSettings(TypedDict, total=False):
    """Settings for truncating large tool arguments in old messages.

    Attributes:
        trigger: Threshold to trigger argument truncation. If None, truncation is disabled.
        keep: Context retention policy for message truncation (defaults to last 20 messages).
        max_length: Maximum character length for tool arguments before truncation.
        truncation_text: Text to replace truncated arguments with.
    """

    trigger: "ContextSize | None"
    keep: "ContextSize"
    max_length: int
    truncation_text: str


class CompactionState(AgentState):
    """State for the compaction middleware.

    Extends AgentState with private fields for tracking compaction events,
    offloaded tool call IDs, and batch truncation state.
    The PrivateStateAttr annotation hides them from input/output schemas.

    Note: The ``_summarization_event`` field name is preserved because values are
    stored under that key in the LangGraph checkpointer — renaming it would
    orphan existing persisted state.
    """

    _summarization_event: Annotated[
        NotRequired[CompactionEvent | None], PrivateStateAttr
    ]
    _truncation_batch_count: Annotated[NotRequired[int], PrivateStateAttr]
    _offloaded_tool_call_ids: Annotated[NotRequired[set[str]], PrivateStateAttr]
    _offloaded_read_result_ids: Annotated[NotRequired[set[str]], PrivateStateAttr]
    _cached_input_tokens: Annotated[NotRequired[int], PrivateStateAttr]
    _cached_output_tokens: Annotated[NotRequired[int], PrivateStateAttr]


# Tool names whose arguments carry large payloads (file contents, code strings)
# that bloat context in older messages.
TRUNCATABLE_TOOLS = frozenset({"Write", "Edit", "ExecuteCode"})

# Path prefixes for Read results considered non-critical — these files contain
# previously offloaded content that the agent has already processed.
NON_CRITICAL_READ_PREFIXES: tuple[str, ...] = (
    ".agents/threads/",  # Previously offloaded content (truncated args, evicted messages)
    ".agents/tmp/",  # Temporary agent scratch files
)

TokenCounter = Callable[[Iterable[MessageLikeRepresentation]], int]

_DEFAULT_MESSAGES_TO_KEEP = 20
_DEFAULT_TRIM_TOKEN_LIMIT = 4000
_DEFAULT_FALLBACK_MESSAGE_COUNT = 15

ContextFraction = tuple[Literal["fraction"], float]
ContextTokens = tuple[Literal["tokens"], int]
ContextMessages = tuple[Literal["messages"], int]
ContextSize = ContextFraction | ContextTokens | ContextMessages
