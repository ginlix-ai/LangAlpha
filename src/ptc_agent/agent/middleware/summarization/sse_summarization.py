"""
Custom Summarization Middleware.

Based on LangChain's SummarizationMiddleware but modified to:
- Emit custom SSE events (summarization_signal) instead of regular message_chunk
- Use get_stream_writer() for lifecycle signaling

This allows the frontend to distinguish summarization events from regular agent output.
"""

import uuid
import warnings
import logging
from collections.abc import Callable, Iterable, Mapping
from functools import partial
from typing import Any, Literal, cast

import tiktoken

from langchain_core.messages import (
    AnyMessage,
    MessageLikeRepresentation,
    RemoveMessage,
    ToolMessage,
)
from langchain_core.messages.human import HumanMessage
from langchain_core.messages.utils import convert_to_messages, trim_messages
from langgraph.graph.message import REMOVE_ALL_MESSAGES
from langgraph.runtime import Runtime
from langgraph.config import get_stream_writer
from typing_extensions import override

from langchain.agents.middleware.types import AgentMiddleware, AgentState
from langchain.chat_models import BaseChatModel, init_chat_model

from src.llms.content_utils import format_llm_content
from src.llms.token_counter import extract_token_usage
from src.config.settings import get_summarization_config
from src.llms import get_llm_by_type

logger = logging.getLogger(__name__)

# Constant for context summary prefix - used in both standalone function and middleware
CONTEXT_SUMMARY_PREFIX = (
    "[Context Summary]\n"
    "This session is being continued from a previous conversation "
    "that ran out of context. The conversation is summarized below:\n\n"
)


async def summarize_messages(
    messages: list[AnyMessage],
    keep_messages: int = 5,
    model_name: str = "gpt-5-nano",
) -> dict[str, Any]:
    """
    Summarize conversation messages (standalone function for manual triggering).

    This function extracts the summarization logic from CustomSummarizationMiddleware
    to allow manual invocation outside of the middleware context.

    Args:
        messages: List of conversation messages to summarize
        keep_messages: Number of recent messages to preserve (default: 5)
        model_name: LLM model name for generating summaries (default: gpt-5-nano)

    Returns:
        Dict with "messages" key containing [summary_message, *preserved_messages]
        formatted for use with graph.aupdate_state() and the add_messages reducer.

    Example:
        result = await summarize_messages(current_messages, keep_messages=5)
        await graph.aupdate_state(config, result)
    """
    if not messages:
        raise ValueError("No messages to summarize")

    # Ensure all messages have IDs
    for msg in messages:
        if msg.id is None:
            msg.id = str(uuid.uuid4())

    # Determine cutoff point (preserve last N messages, respecting tool message pairs)
    if len(messages) <= keep_messages:
        raise ValueError(
            f"Not enough messages to summarize. Have {len(messages)}, "
            f"need more than {keep_messages} to preserve."
        )

    target_cutoff = len(messages) - keep_messages
    # Adjust cutoff to not split AI/Tool message pairs
    cutoff_index = target_cutoff
    while cutoff_index < len(messages) and isinstance(messages[cutoff_index], ToolMessage):
        cutoff_index += 1

    if cutoff_index <= 0:
        raise ValueError("Cannot determine valid cutoff point for summarization")

    messages_to_summarize = messages[:cutoff_index]
    preserved_messages = messages[cutoff_index:]

    # Initialize summarization model
    summarization_model: BaseChatModel = get_llm_by_type(model_name)
    if hasattr(summarization_model, 'streaming'):
        summarization_model.streaming = False

    # Trim messages if needed for summarization call
    config = get_summarization_config()
    token_threshold = config.get("token_threshold", 120000)
    trim_limit = token_threshold + 50000

    token_count = count_tokens_tiktoken(messages_to_summarize)
    if token_count > trim_limit:
        # Use trim_messages to fit within limit
        trimmed = cast(
            "list[AnyMessage]",
            trim_messages(
                messages_to_summarize,
                max_tokens=trim_limit,
                token_counter=count_tokens_tiktoken,
                start_on="human",
                strategy="last",
                allow_partial=True,
                include_system=True,
            ),
        )
        if trimmed:
            messages_to_summarize = trimmed
        else:
            # Fallback to last N messages
            messages_to_summarize = messages_to_summarize[-_DEFAULT_FALLBACK_MESSAGE_COUNT:]

    # Generate summary using the model
    try:
        response = await summarization_model.ainvoke(
            DEFAULT_SUMMARY_PROMPT.format(messages=messages_to_summarize)
        )

        # Extract text content only (discard reasoning/thinking)
        content = response.content if hasattr(response, 'content') else response
        additional_kwargs = getattr(response, 'additional_kwargs', None)
        formatted = format_llm_content(content, additional_kwargs)
        summary_text = formatted.get("text", "").strip()

        if not summary_text:
            summary_text = "Previous conversation context (summary unavailable)."

    except Exception as e:
        logger.error(f"Error generating summary: {e}")
        summary_text = f"Previous conversation context (error: {e})"

    # Build summary message with context prefix
    summary_message = HumanMessage(
        content=f"{CONTEXT_SUMMARY_PREFIX}{summary_text}",
        id=str(uuid.uuid4()),
    )

    # Return in the same format as middleware (for add_messages reducer)
    return {
        "messages": [
            RemoveMessage(id=REMOVE_ALL_MESSAGES),
            summary_message,
            *preserved_messages,
        ],
        "summary_text": summary_text,
        "original_count": len(messages),
        "preserved_count": len(preserved_messages) + 1,  # +1 for summary message
    }

TokenCounter = Callable[[Iterable[MessageLikeRepresentation]], int]

_DEFAULT_MESSAGES_TO_KEEP = 20
_DEFAULT_TRIM_TOKEN_LIMIT = 4000
_DEFAULT_FALLBACK_MESSAGE_COUNT = 15

ContextFraction = tuple[Literal["fraction"], float]
ContextTokens = tuple[Literal["tokens"], int]
ContextMessages = tuple[Literal["messages"], int]
ContextSize = ContextFraction | ContextTokens | ContextMessages


# Lazy-loaded tiktoken encoder
_tiktoken_encoder: tiktoken.Encoding | None = None


def _get_tiktoken_encoder() -> tiktoken.Encoding:
    """Get or create tiktoken encoder (lazy initialization)."""
    global _tiktoken_encoder
    if _tiktoken_encoder is None:
        _tiktoken_encoder = tiktoken.get_encoding("cl100k_base")
    return _tiktoken_encoder


def _extract_text_from_content(content: str | list) -> str:
    """Extract text from message content, handling all provider formats."""
    if isinstance(content, str):
        return content
    if not isinstance(content, list):
        return ""

    texts = []
    for block in content:
        if isinstance(block, str):
            texts.append(block)
        elif isinstance(block, dict):
            block_type = block.get("type", "")

            # Text block
            if block_type == "text":
                texts.append(block.get("text", ""))

            # Anthropic thinking block
            elif block_type == "thinking":
                texts.append(block.get("thinking", ""))

            # OpenAI reasoning block (content_blocks format)
            elif block_type == "reasoning":
                # Direct reasoning field
                if "reasoning" in block:
                    texts.append(block.get("reasoning", ""))
                # Response API summary format
                elif "summary" in block:
                    for item in block.get("summary", []):
                        if isinstance(item, dict) and "text" in item:
                            texts.append(item.get("text", ""))

            # Tool use block - count the input
            elif block_type == "tool_use":
                texts.append(str(block.get("input", "")))

    return " ".join(texts)


def count_tokens_tiktoken(messages: Iterable[MessageLikeRepresentation]) -> int:
    """Count tokens using tiktoken (accurate for all languages including CJK)."""
    enc = _get_tiktoken_encoder()
    total = 0
    for msg in convert_to_messages(messages):
        # Extract from main content
        text = _extract_text_from_content(msg.content)

        # Also check additional_kwargs for OpenAI reasoning (o1/o3 models)
        additional_kwargs = getattr(msg, "additional_kwargs", {}) or {}
        reasoning = additional_kwargs.get("reasoning_content") or additional_kwargs.get("reasoning")
        if reasoning:
            reasoning_text = _extract_text_from_content(reasoning) if isinstance(reasoning, list) else str(reasoning)
            text = f"{text} {reasoning_text}" if text else reasoning_text

        total += len(enc.encode(text)) + 3  # +3 for role/message overhead
    return total


class CustomSummarizationMiddleware(AgentMiddleware):
    """
    Custom summarization middleware that emits SSE events for frontend visibility.

    Key differences from LangChain's SummarizationMiddleware:
    - Emits 'summarization_signal' custom events via get_stream_writer()
    - Signals: "start", "complete", "error"
    - Does NOT stream intermediate chunks (to avoid duplicate events)
    """

    def __init__(
        self,
        model: str | BaseChatModel,
        *,
        trigger: ContextSize | list[ContextSize] | None = None,
        keep: ContextSize = ("messages", _DEFAULT_MESSAGES_TO_KEEP),
        token_counter: TokenCounter = count_tokens_tiktoken,
        summary_prompt: str,
        trim_tokens_to_summarize: int | None = _DEFAULT_TRIM_TOKEN_LIMIT,
        **deprecated_kwargs: Any,
    ) -> None:
        """
        Initialize custom summarization middleware.

        Args:
            model: The language model to use for generating summaries.
            trigger: Threshold(s) that trigger summarization.
            keep: How much context to retain after summarization.
            token_counter: Function to count tokens in messages.
            summary_prompt: Prompt template for generating summaries.
            trim_tokens_to_summarize: Max tokens to keep for summarization call.
        """
        # Handle deprecated parameters
        if "max_tokens_before_summary" in deprecated_kwargs:
            value = deprecated_kwargs["max_tokens_before_summary"]
            warnings.warn(
                "max_tokens_before_summary is deprecated. Use trigger=('tokens', value) instead.",
                DeprecationWarning,
                stacklevel=2,
            )
            if trigger is None and value is not None:
                trigger = ("tokens", value)

        if "messages_to_keep" in deprecated_kwargs:
            value = deprecated_kwargs["messages_to_keep"]
            warnings.warn(
                "messages_to_keep is deprecated. Use keep=('messages', value) instead.",
                DeprecationWarning,
                stacklevel=2,
            )
            if keep == ("messages", _DEFAULT_MESSAGES_TO_KEEP):
                keep = ("messages", value)

        super().__init__()

        if isinstance(model, str):
            model = init_chat_model(model)

        self.model = model
        if trigger is None:
            self.trigger: ContextSize | list[ContextSize] | None = None
            trigger_conditions: list[ContextSize] = []
        elif isinstance(trigger, list):
            validated_list = [self._validate_context_size(item, "trigger") for item in trigger]
            self.trigger = validated_list
            trigger_conditions = validated_list
        else:
            validated = self._validate_context_size(trigger, "trigger")
            self.trigger = validated
            trigger_conditions = [validated]
        self._trigger_conditions = trigger_conditions

        self.keep = self._validate_context_size(keep, "keep")
        self.token_counter = token_counter
        self.summary_prompt = summary_prompt
        self.trim_tokens_to_summarize = trim_tokens_to_summarize

        # Cached token usage from last model call (updated in after_model)
        self._cached_input_tokens: int = 0
        self._cached_output_tokens: int = 0

        requires_profile = any(condition[0] == "fraction" for condition in self._trigger_conditions)
        if self.keep[0] == "fraction":
            requires_profile = True
        if requires_profile and self._get_profile_limits() is None:
            msg = (
                "Model profile information is required to use fractional token limits, "
                "and is unavailable for the specified model. Please use absolute token "
                "counts instead, or pass "
                '`\n\nChatModel(..., profile={"max_input_tokens": ...})`.\n\n'
                "with a desired integer value of the model's maximum input tokens."
            )
            raise ValueError(msg)

    @override
    def before_model(self, state: AgentState, runtime: Runtime) -> dict[str, Any] | None:
        """Process messages before model invocation, potentially triggering summarization."""
        messages = state["messages"]
        self._ensure_message_ids(messages)

        # Use cached token count from last model call (more accurate than tiktoken)
        # Falls back to tiktoken on first call when cache is empty
        if self._cached_input_tokens > 0:
            total_tokens = self._cached_input_tokens + self._cached_output_tokens
        else:
            total_tokens = self.token_counter(messages)

        if not self._should_summarize(messages, total_tokens):
            return None

        cutoff_index = self._determine_cutoff_index(messages)

        if cutoff_index <= 0:
            return None

        messages_to_summarize, preserved_messages = self._partition_messages(messages, cutoff_index)

        summary = self._create_summary(messages_to_summarize)
        new_messages = self._build_new_messages(summary)

        # Reset cached tokens since context is changing
        self._cached_input_tokens = 0
        self._cached_output_tokens = 0

        return {
            "messages": [
                RemoveMessage(id=REMOVE_ALL_MESSAGES),
                *new_messages,
                *preserved_messages,
            ]
        }

    @override
    async def abefore_model(self, state: AgentState, runtime: Runtime) -> dict[str, Any] | None:
        """Process messages before model invocation, potentially triggering summarization."""
        messages = state["messages"]
        self._ensure_message_ids(messages)

        # Use cached token count from last model call (more accurate than tiktoken)
        # Falls back to tiktoken on first call when cache is empty
        if self._cached_input_tokens > 0:
            total_tokens = self._cached_input_tokens + self._cached_output_tokens
        else:
            total_tokens = self.token_counter(messages)

        if not self._should_summarize(messages, total_tokens):
            return None

        cutoff_index = self._determine_cutoff_index(messages)

        if cutoff_index <= 0:
            return None

        messages_to_summarize, preserved_messages = self._partition_messages(messages, cutoff_index)

        summary = await self._acreate_summary(messages_to_summarize)
        new_messages = self._build_new_messages(summary)

        # Reset cached tokens since context is changing
        self._cached_input_tokens = 0
        self._cached_output_tokens = 0

        return {
            "messages": [
                RemoveMessage(id=REMOVE_ALL_MESSAGES),
                *new_messages,
                *preserved_messages,
            ]
        }

    @override
    def after_model(self, state: AgentState, runtime: Runtime) -> dict[str, Any] | None:
        """Capture token usage from model response and emit to frontend."""
        self._update_token_cache(state.get("messages", []))
        return None

    @override
    async def aafter_model(self, state: AgentState, runtime: Runtime) -> dict[str, Any] | None:
        """Capture token usage from model response and emit to frontend."""
        self._update_token_cache(state.get("messages", []))
        return None

    def _update_token_cache(self, messages: list[AnyMessage]) -> None:
        """Extract and cache token usage from last AI message, emit to frontend."""
        if not messages:
            return

        # Find the last AI message
        for msg in reversed(messages):
            if msg.type != "ai":
                continue

            # Use shared extract_token_usage which handles all provider formats
            usage = extract_token_usage(msg)
            input_tokens = usage.get("input_tokens", 0)
            output_tokens = usage.get("output_tokens", 0)

            if input_tokens > 0:
                self._cached_input_tokens = input_tokens
                self._cached_output_tokens = output_tokens

                logger.debug(
                    f"[Summarization] Token usage: "
                    f"input={input_tokens}, output={output_tokens}"
                )

                # Emit token usage to frontend for context window display
                try:
                    stream_writer = get_stream_writer()
                    stream_writer({
                        "type": "token_usage",
                        "input_tokens": input_tokens,
                        "output_tokens": output_tokens,
                        "total_tokens": input_tokens + output_tokens,
                    })
                except Exception as e:
                    logger.warning(f"[Summarization] Could not emit token_usage signal: {e}")

                return  # Found usage, done

    def _should_summarize(self, messages: list[AnyMessage], total_tokens: int) -> bool:
        """Determine whether summarization should run for the current token usage."""
        if not self._trigger_conditions:
            return False

        for kind, value in self._trigger_conditions:
            if kind == "messages" and len(messages) >= value:
                return True
            if kind == "tokens" and total_tokens >= value:
                logger.info(f"[Summarization] Triggered: {total_tokens} >= {value} tokens")
                return True
            if kind == "fraction":
                max_input_tokens = self._get_profile_limits()
                if max_input_tokens is None:
                    continue
                threshold = int(max_input_tokens * value)
                if threshold <= 0:
                    threshold = 1
                if total_tokens >= threshold:
                    return True
        return False

    def _determine_cutoff_index(self, messages: list[AnyMessage]) -> int:
        """Choose cutoff index respecting retention configuration."""
        kind, value = self.keep
        if kind in {"tokens", "fraction"}:
            token_based_cutoff = self._find_token_based_cutoff(messages)
            if token_based_cutoff is not None:
                return token_based_cutoff
            return self._find_safe_cutoff(messages, _DEFAULT_MESSAGES_TO_KEEP)
        return self._find_safe_cutoff(messages, cast("int", value))

    def _find_token_based_cutoff(self, messages: list[AnyMessage]) -> int | None:
        """Find cutoff index based on target token retention."""
        if not messages:
            return 0

        kind, value = self.keep
        if kind == "fraction":
            max_input_tokens = self._get_profile_limits()
            if max_input_tokens is None:
                return None
            target_token_count = int(max_input_tokens * value)
        elif kind == "tokens":
            target_token_count = int(value)
        else:
            return None

        if target_token_count <= 0:
            target_token_count = 1

        if self.token_counter(messages) <= target_token_count:
            return 0

        left, right = 0, len(messages)
        cutoff_candidate = len(messages)
        max_iterations = len(messages).bit_length() + 1
        for _ in range(max_iterations):
            if left >= right:
                break

            mid = (left + right) // 2
            if self.token_counter(messages[mid:]) <= target_token_count:
                cutoff_candidate = mid
                right = mid
            else:
                left = mid + 1

        if cutoff_candidate == len(messages):
            cutoff_candidate = left

        if cutoff_candidate >= len(messages):
            if len(messages) == 1:
                return 0
            cutoff_candidate = len(messages) - 1

        return self._find_safe_cutoff_point(messages, cutoff_candidate)

    def _get_profile_limits(self) -> int | None:
        """Retrieve max input token limit from the model profile."""
        try:
            profile = self.model.profile
        except AttributeError:
            return None

        if not isinstance(profile, Mapping):
            return None

        max_input_tokens = profile.get("max_input_tokens")

        if not isinstance(max_input_tokens, int):
            return None

        return max_input_tokens

    def _validate_context_size(self, context: ContextSize, parameter_name: str) -> ContextSize:
        """Validate context configuration tuples."""
        kind, value = context
        if kind == "fraction":
            if not 0 < value <= 1:
                msg = f"Fractional {parameter_name} values must be between 0 and 1, got {value}."
                raise ValueError(msg)
        elif kind in {"tokens", "messages"}:
            if value <= 0:
                msg = f"{parameter_name} thresholds must be greater than 0, got {value}."
                raise ValueError(msg)
        else:
            msg = f"Unsupported context size type {kind} for {parameter_name}."
            raise ValueError(msg)
        return context

    def _build_new_messages(self, summary: str) -> list[HumanMessage]:
        return [
            HumanMessage(content=f"{CONTEXT_SUMMARY_PREFIX}{summary}")
        ]

    def _extract_summary_text(self, response: Any) -> str:
        """Extract text content from LLM response, discarding reasoning/thinking.

        Args:
            response: The LLM response object

        Returns:
            Extracted text content, stripped
        """
        content = response.content if hasattr(response, 'content') else response
        additional_kwargs = getattr(response, 'additional_kwargs', None)
        formatted = format_llm_content(content, additional_kwargs)
        summary = formatted.get("text", "")

        # Log if reasoning was discarded
        if formatted.get("reasoning"):
            logger.debug(
                f"[Summarization] Discarded reasoning content "
                f"(length={len(formatted.get('reasoning', ''))})"
            )

        return summary.strip()

    def _emit_summarization_signal(
        self,
        signal: str,
        *,
        summary_length: int | None = None,
        error: str | None = None,
    ) -> None:
        """Emit a summarization signal event via stream writer.

        Args:
            signal: Signal type ("start", "complete", or "error")
            summary_length: Length of summary (for "complete" signal)
            error: Error message (for "error" signal)
        """
        try:
            stream_writer = get_stream_writer()
            payload: dict[str, Any] = {
                "type": "summarization_signal",
                "signal": signal,
            }
            if summary_length is not None:
                payload["summary_length"] = summary_length
            if error is not None:
                payload["error"] = error
            stream_writer(payload)
            if signal == "start":
                logger.info("[Summarization] Emitted start signal")
            elif signal == "complete":
                logger.info(f"[Summarization] Emitted complete signal (length={summary_length})")
            elif signal == "error":
                logger.warning(f"[Summarization] Emitted error signal: {error}")
        except Exception as e:
            logger.debug(f"Could not emit summarization {signal} signal: {e}")

    def _ensure_message_ids(self, messages: list[AnyMessage]) -> None:
        """Ensure all messages have unique IDs for the add_messages reducer."""
        for msg in messages:
            if msg.id is None:
                msg.id = str(uuid.uuid4())

    def _partition_messages(
        self,
        conversation_messages: list[AnyMessage],
        cutoff_index: int,
    ) -> tuple[list[AnyMessage], list[AnyMessage]]:
        """Partition messages into those to summarize and those to preserve."""
        messages_to_summarize = conversation_messages[:cutoff_index]
        preserved_messages = conversation_messages[cutoff_index:]

        return messages_to_summarize, preserved_messages

    def _find_safe_cutoff(self, messages: list[AnyMessage], messages_to_keep: int) -> int:
        """Find safe cutoff point that preserves AI/Tool message pairs."""
        if len(messages) <= messages_to_keep:
            return 0

        target_cutoff = len(messages) - messages_to_keep
        return self._find_safe_cutoff_point(messages, target_cutoff)

    def _find_safe_cutoff_point(self, messages: list[AnyMessage], cutoff_index: int) -> int:
        """Find a safe cutoff point that doesn't split AI/Tool message pairs."""
        while cutoff_index < len(messages) and isinstance(messages[cutoff_index], ToolMessage):
            cutoff_index += 1
        return cutoff_index

    def _create_summary(self, messages_to_summarize: list[AnyMessage]) -> str:
        """Generate summary for the given messages (sync version)."""
        if not messages_to_summarize:
            return "No previous conversation history."

        trimmed_messages = self._trim_messages_for_summary(messages_to_summarize)
        if not trimmed_messages:
            return "Previous conversation was too long to summarize."

        try:
            self._emit_summarization_signal("start")
            response = self.model.invoke(self.summary_prompt.format(messages=trimmed_messages))
            summary = self._extract_summary_text(response)
            self._emit_summarization_signal("complete", summary_length=len(summary))
            return summary
        except Exception as e:
            self._emit_summarization_signal("error", error=str(e))
            return f"Error generating summary: {e!s}"

    async def _acreate_summary(self, messages_to_summarize: list[AnyMessage]) -> str:
        """Generate summary for the given messages (async version with custom events)."""
        if not messages_to_summarize:
            return "No previous conversation history."

        trimmed_messages = self._trim_messages_for_summary(messages_to_summarize)
        if not trimmed_messages:
            return "Previous conversation was too long to summarize."

        try:
            self._emit_summarization_signal("start")

            # Use ainvoke (non-streaming) to avoid duplicate events
            # The model should have streaming=False set in factory
            response = await self.model.ainvoke(
                self.summary_prompt.format(messages=trimmed_messages)
            )

            summary = self._extract_summary_text(response)
            self._emit_summarization_signal("complete", summary_length=len(summary))
            return summary
        except Exception as e:
            self._emit_summarization_signal("error", error=str(e))
            return f"Error generating summary: {e!s}"

    def _trim_messages_for_summary(self, messages: list[AnyMessage]) -> list[AnyMessage]:
        """Trim messages to fit within summary generation limits."""
        if not messages:
            return messages

        # If no trim limit set, return all messages
        if self.trim_tokens_to_summarize is None:
            return messages

        try:
            trimmed = cast(
                "list[AnyMessage]",
                trim_messages(
                    messages,
                    max_tokens=self.trim_tokens_to_summarize,
                    token_counter=self.token_counter,
                    start_on="human",
                    strategy="last",
                    allow_partial=True,
                    include_system=True,
                ),
            )

            # If trim_tokens_to_summarize is too restrictive and returns empty,
            # fall back to keeping the last N messages instead of failing
            if not trimmed:
                logger.warning(
                    f"[Summarization] trim_tokens_to_summarize={self.trim_tokens_to_summarize} "
                    f"is too restrictive, falling back to last {_DEFAULT_FALLBACK_MESSAGE_COUNT} messages"
                )
                return messages[-_DEFAULT_FALLBACK_MESSAGE_COUNT:]

            return trimmed
        except Exception as e:
            logger.warning(f"[Summarization] trim_messages failed: {e}, using fallback")
            return messages[-_DEFAULT_FALLBACK_MESSAGE_COUNT:]


# Financial research summarization prompt with recency weighting and locale awareness
DEFAULT_SUMMARY_PROMPT = """<role>
Financial Research Context Summarizer
</role>

<context>
You're nearing your input token limit. The conversation history below will be
replaced with the context you extract. This is critical - ensure you capture
all important information so you can continue the research without losing progress.
</context>

<objective>
Extract the most important context to preserve research continuity and prevent
repeating completed work. Think deeply about what information is essential to
achieving the user's overall goal.
</objective>

<instructions>
Create a natural, readable summary that captures everything needed to continue the work.
Write in the SAME LANGUAGE as the user's queries.
Use your judgment on structure - the categories below are guidelines, not rigid templates.

Key information to capture:

1. **Current Query**: What is the user asking? Include the verbatim question, relevant tickers/entities, and scope.

2. **Progress**: What has been done and what remains? List completed steps with outcomes, current work, and pending tasks.

3. **Key Findings**: All critical discoveries with their sources:
   - Data points with exact values: prices, ratios, growth rates (always include source)
   - Observations and patterns identified
   - Conclusions reached from analysis
   - URLs crawled, APIs used, files created

4. **Decisions**: Any methodology choices or user preferences that affect ongoing work.

5. **Query History** (for multi-turn sessions only): Previous queries in chronological order with their outcomes.

Guidelines:
- Preserve ALL numerical data exactly as discovered
- Include source/citation for each data point
- Omit categories that have no content
- Be concise but comprehensive
- Use natural prose or bullet points as appropriate
</instructions>

<output_format>
Respond ONLY with the extracted context. Do not include preamble or commentary.

Begin with a Brief 1-2 sentence overview of the research session and current goal.
Make sure you maintain the user original query and goal.

Then organize naturally using markdown headers.
Write as if briefing a colleague who needs to continue your work without repeating what's done.
</output_format>

<messages>
{messages}
</messages>"""

# NOTE: LangChain's SummarizationMiddleware hardcodes the prefix in _build_new_messages()
# so we include our custom prefix in the summary_prompt output_format instead


def SummarizationMiddleware(
    config: dict | None = None,
) -> CustomSummarizationMiddleware | None:
    """Factory function that creates a configured summarization middleware.

    Named in PascalCase for consistency with other middleware classes.

    Uses custom middleware that emits 'summarization_signal' events
    instead of regular message_chunk events.

    Args:
        config: Optional config override (defaults to get_summarization_config())

    Returns:
        Configured CustomSummarizationMiddleware or None if disabled
    """
    if config is None:
        config = get_summarization_config()

    if not config.get("enabled", False):
        return None

    # Get summarization model from config
    model_name = config.get("llm", "gpt-5-nano")
    summarization_model: BaseChatModel = get_llm_by_type(model_name)

    # Disable streaming to prevent normal message_chunk events
    # This ensures only our custom summarization_signal events are emitted
    if hasattr(summarization_model, 'streaming'):
        summarization_model.streaming = False

    # Get configuration values
    token_threshold = config.get("token_threshold", 120000)
    keep_messages = config.get("keep_messages", 5)

    return CustomSummarizationMiddleware(
        model=summarization_model,
        trigger=("tokens", token_threshold),
        keep=("messages", keep_messages),
        # Ensure summarizer can see all messages + buffer
        trim_tokens_to_summarize=token_threshold + 50000,
        # Custom prompt for research-focused summarization
        summary_prompt=DEFAULT_SUMMARY_PROMPT,
    )
