"""Types, constants, and defaults for the compaction middleware."""

from collections.abc import Callable, Iterable
from typing import Annotated, Literal, NotRequired

from langchain_core.messages import MessageLikeRepresentation
from langchain_core.messages.human import HumanMessage
from typing_extensions import TypedDict

from langchain.agents.middleware.types import AgentState, PrivateStateAttr

from ptc_agent.core.paths import AGENT_HISTORY_DIRS, SandboxLayout


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

    ``cutoff_index`` is the positional boundary; ``anchor_message_id`` is the id
    of the first preserved message, used to re-find the boundary by id when the
    underlying list drifts (e.g. DeltaChannel reconstruction). Legacy persisted
    events omit ``anchor_message_id`` and fall back to the positional index.
    """

    cutoff_index: int
    summary_message: HumanMessage
    file_path: str | None
    anchor_message_id: NotRequired[str | None]


class TruncateArgsSettings(TypedDict, total=False):
    """Settings for Tier 1: trimming large tool args and stale Read results.

    Attributes:
        idle_minutes: How long since the last model response a turn must
            start for Tier 1 to run; None turns Tier 1 off.
        keep_messages: The newest messages Tier 1 never touches.
        max_length: Maximum character length for tool arguments before truncation.
        truncation_text: What a cut argument ends in where no transcript
            file can be named.
    """

    idle_minutes: float | None
    keep_messages: int
    max_length: int
    truncation_text: str


class CompactionState(AgentState):
    """State for the compaction middleware.

    Extends AgentState with private fields for tracking compaction events,
    offloaded tool call IDs, and when the model last answered (epoch seconds),
    which gates Tier 1. The PrivateStateAttr annotation hides them from
    input/output schemas.

    Note: The ``_summarization_event`` field name is preserved because values are
    stored under that key in the LangGraph checkpointer — renaming it would
    orphan existing persisted state.
    """

    _summarization_event: Annotated[
        NotRequired[CompactionEvent | None], PrivateStateAttr
    ]
    _offloaded_tool_call_ids: Annotated[NotRequired[set[str]], PrivateStateAttr]
    _offloaded_read_result_ids: Annotated[NotRequired[set[str]], PrivateStateAttr]
    _cached_input_tokens: Annotated[NotRequired[int], PrivateStateAttr]
    _cached_output_tokens: Annotated[NotRequired[int], PrivateStateAttr]
    _last_model_response_at: Annotated[NotRequired[float], PrivateStateAttr]


# Tool names whose arguments carry large payloads (file contents, code strings)
# that bloat context in older messages.
TRUNCATABLE_TOOLS = frozenset({"Write", "Edit", "ExecuteCode"})

# Path prefixes for Read results considered non-critical — these files contain
# previously offloaded content that the agent has already processed.
NON_CRITICAL_READ_PREFIXES: tuple[str, ...] = tuple(
    f"{d}/" for d in (*AGENT_HISTORY_DIRS, SandboxLayout.TMP_DIR)
)

TokenCounter = Callable[[Iterable[MessageLikeRepresentation]], int]

_DEFAULT_MESSAGES_TO_KEEP = 20
_DEFAULT_TRIM_TOKEN_LIMIT = 4000
_DEFAULT_FALLBACK_MESSAGE_COUNT = 15

ContextFraction = tuple[Literal["fraction"], float]
ContextTokens = tuple[Literal["tokens"], int]
ContextMessages = tuple[Literal["messages"], int]
ContextSize = ContextFraction | ContextTokens | ContextMessages
