"""Types, constants, and defaults for the compaction middleware."""

from __future__ import annotations

from collections.abc import Callable, Iterable
from dataclasses import dataclass
from typing import Annotated, Any, NotRequired

from langchain_core.messages import MessageLikeRepresentation
from langchain_core.messages.human import HumanMessage
from typing_extensions import TypedDict

from langchain.agents.middleware.types import AgentState, PrivateStateAttr

from ptc_agent.agent.transcript.identity import WindowState
from ptc_agent.config.agent import CompactionConfig
from ptc_agent.core.paths import AGENT_HISTORY_DIRS, SandboxLayout


# How a summary message opens. The summary is written from the history before
# the cut while the newest messages stay after it word for word, so when work
# finished or the user steered inside those, the two disagree; this says which
# one stands. Summaries written before open with transcript.classify's
# LEGACY_SUMMARY_PREFIX.
CONTEXT_SUMMARY_PREFIX = (
    "[Context Summary]\n"
    "This session is being continued from a previous conversation that ran out "
    "of context. The summary below covers only the earlier messages, which are "
    "no longer in your context. The messages after it are kept word for word and "
    "happened later, so where they disagree with the summary, go by them.\n\n"
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


@dataclass(frozen=True)
class OffloadSettings:
    """Tier 1: which old tool arguments and Read results are hidden.

    ``max_length`` holds with Tier 1 off too: the view still re-applies the
    cuts a manual /offload recorded, and has to cut them as they were chosen.
    """

    #: The newest messages Tier 1 never touches.
    keep_messages: int = 20
    #: The longest string argument left whole.
    max_length: int = 2000
    #: How long since the model last answered a turn must start for Tier 1
    #: to run there; None turns automatic Tier 1 off.
    idle_seconds: float | None = 90 * 60

    @classmethod
    def from_config(cls, config: CompactionConfig) -> OffloadSettings:
        idle = config.truncate_args_idle_minutes
        return cls(
            keep_messages=config.truncate_args_keep_messages,
            max_length=config.truncate_args_max_length,
            idle_seconds=None if idle is None else float(idle) * 60,
        )


class CompactionState(AgentState):
    """State for the compaction middleware.

    Extends AgentState with private fields for tracking compaction events,
    offloaded tool call IDs, and when the model last answered (epoch seconds),
    which gates Tier 1, and what the window trimmed (``Window``). The
    PrivateStateAttr annotation hides them from input/output schemas.

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
    _window: Annotated[NotRequired[WindowState], PrivateStateAttr]
    #: The summary anchor and cut the server last refused a trim at, so the
    #: next turns do not ask again until a summary or a trim moves them.
    _window_refused: Annotated[NotRequired[dict[str, Any]], PrivateStateAttr]


# Tool names whose arguments carry large payloads (file contents, code strings)
# that bloat context in older messages.
TRUNCATABLE_TOOLS = frozenset({"Write", "Edit", "ExecuteCode"})

# The dirs whose Read results are non-critical: content offloaded earlier that
# the agent has already processed, and its own working files, which it reads
# again by path when it needs them.
NON_CRITICAL_READ_PREFIXES: tuple[str, ...] = tuple(
    f"{d}/" for d in (*AGENT_HISTORY_DIRS, SandboxLayout.TMP_DIR)
)

TokenCounter = Callable[[Iterable[MessageLikeRepresentation]], int]

_DEFAULT_FALLBACK_MESSAGE_COUNT = 15
