"""SummarizationMiddleware — two-tier context management for LangGraph agents.

Based on deepagent's SummarizationMiddleware but modified to:
- Emit unified 'context_window' SSE events (discriminated by action field)
- Use get_stream_writer() for lifecycle signaling
- Use wrap_model_call for non-destructive context management (preserves checkpoint)
- Two-tier context management: tool arg truncation + summarization

Actions emitted via context_window events:
- token_usage: after each model call (input/output/total tokens)
- summarize: start/complete/error signals during summarization
- offload: complete signal after Tier 1 tool arg truncation
"""

import uuid
import warnings
import logging
from collections.abc import Awaitable, Callable, Mapping
from typing import Any, cast

from langchain_core.messages import AIMessage, AnyMessage, ToolMessage
from langchain_core.messages.human import HumanMessage
from langchain_core.messages.utils import trim_messages
from langchain_core.exceptions import ContextOverflowError
from langgraph.config import get_config, get_stream_writer
from langgraph.types import Command
from typing_extensions import override

from langchain.agents.middleware.types import (
    AgentMiddleware,
    ExtendedModelResponse,
    ModelRequest,
    ModelResponse,
)
from langchain.chat_models import BaseChatModel, init_chat_model

from src.llms.content_utils import format_llm_content
from src.llms.token_counter import extract_token_usage
from ptc_agent.config.agent import SummarizationConfig
from src.llms import get_llm_by_type

from ptc_agent.agent.middleware.summarization.types import (
    ContextSize,
    SummarizationEvent,
    SummarizationState,
    TruncateArgsSettings,
    TokenCounter,
    _DEFAULT_FALLBACK_MESSAGE_COUNT,
    _DEFAULT_MESSAGES_TO_KEEP,
    _DEFAULT_TRIM_TOKEN_LIMIT,
)
from ptc_agent.agent.middleware.summarization.utils import (
    DEFAULT_SUMMARY_PROMPT,
    build_summary_message,
    compute_absolute_cutoff,
    count_tokens_tiktoken,
    get_effective_messages,
    strip_base64_from_messages,
    truncate_message_args,
    truncate_read_results,
)
from ptc_agent.agent.middleware.summarization.offloading import (
    aoffload_base64_content,
    aoffload_to_backend,
    aoffload_truncated_args,
    get_thread_id,
)

logger = logging.getLogger(__name__)


class SummarizationMiddleware(AgentMiddleware):
    """
    Custom summarization middleware that emits SSE events for frontend visibility.

    Uses wrap_model_call to reconstruct the message list on-the-fly without
    modifying the LangGraph checkpoint — preserving full history, enabling
    recovery, and supporting chained summarization.

    Two-tier context management:
    - Tier 1: Tool arg truncation (cheap, fires early at message count threshold)
    - Tier 2: Full summarization (expensive, fires at token count threshold)

    Key differences from LangChain's SummarizationMiddleware:
    - Emits unified 'context_window' events via get_stream_writer()
    - Actions: "summarize" (start/complete/error), "offload" (complete), "token_usage"
    - Does NOT stream intermediate chunks (to avoid duplicate events)
    """

    state_schema = SummarizationState

    def __init__(
        self,
        model: str | BaseChatModel,
        *,
        trigger: ContextSize | list[ContextSize] | None = None,
        keep: ContextSize = ("messages", _DEFAULT_MESSAGES_TO_KEEP),
        token_counter: TokenCounter = count_tokens_tiktoken,
        summary_prompt: str,
        trim_tokens_to_summarize: int | None = _DEFAULT_TRIM_TOKEN_LIMIT,
        backend: Any | None = None,
        truncate_args_settings: TruncateArgsSettings | None = None,
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
            backend: Backend for offloading conversation history (SandboxBackend for PTC,
                None for flash). When None, no filesystem ops are attempted.
            truncate_args_settings: Settings for truncating large tool arguments
                in old messages. When None, argument truncation is disabled.
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
            validated_list = [
                self._validate_context_size(item, "trigger") for item in trigger
            ]
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

        # Backend for offloading conversation history to sandbox (immutable config)
        self._backend = backend

        # Parse truncate_args_settings
        if truncate_args_settings is None:
            self._truncate_args_trigger: ContextSize | None = None
            self._truncate_args_keep: ContextSize = ("messages", 20)
            self._max_arg_length = 2000
            self._truncation_text = "...(argument truncated)"
        else:
            self._truncate_args_trigger = truncate_args_settings.get("trigger")
            self._truncate_args_keep = truncate_args_settings.get(
                "keep", ("messages", 20)
            )
            self._max_arg_length = truncate_args_settings.get("max_length", 2000)
            self._truncation_text = truncate_args_settings.get(
                "truncation_text", "...(argument truncated)"
            )

        requires_profile = any(
            condition[0] == "fraction" for condition in self._trigger_conditions
        )
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

    # =========================================================================
    # wrap_model_call — primary async path
    # =========================================================================

    @override
    async def awrap_model_call(
        self,
        request: ModelRequest,
        handler: Callable[[ModelRequest], Awaitable[ModelResponse]],
    ) -> ModelResponse | ExtendedModelResponse:
        """Process messages before model invocation with two-tier context management.

        Flow:
        1. Reconstruct effective messages from previous summarization event
        2. TIER 1: Truncate large tool args in old messages (cheap, fires early)
        3. TIER 2: Check if full summarization is needed (expensive, fires later)
           - NO: call handler with truncated messages, cache tokens, return
             (catch ContextOverflowError -> fall through to summarize)
           - YES: offload -> summarize -> call handler -> cache tokens -> return
                  ExtendedModelResponse with state update
        """
        # 1. Read all per-invocation state from graph state into locals.
        #    No self._ mutations — keeps the middleware instance stateless and
        #    safe to share across concurrent invocations.
        previous_event: SummarizationEvent | None = request.state.get(
            "_summarization_event"
        )
        offloaded_tool_call_ids = set(
            request.state.get("_offloaded_tool_call_ids") or ()
        )
        offloaded_read_result_ids = set(
            request.state.get("_offloaded_read_result_ids") or ()
        )
        last_truncation_msg_count: int = request.state.get("_truncation_batch_count", 0)
        cached_input_tokens: int = request.state.get("_cached_input_tokens", 0)
        cached_output_tokens: int = request.state.get("_cached_output_tokens", 0)

        # 2. Reconstruct effective messages
        self._ensure_message_ids(request.messages)
        effective_messages = self._get_effective_messages(
            request.messages, previous_event
        )

        # 3. Count tokens once (prefer cached from last model call, fall back to tiktoken).
        #    Pass through to _truncate_args / _truncate_read_results to avoid recomputing.
        if cached_input_tokens > 0:
            total_tokens = cached_input_tokens + cached_output_tokens
        else:
            counted_msgs = (
                [request.system_message, *effective_messages]
                if request.system_message is not None
                else effective_messages
            )
            total_tokens = self.token_counter(counted_msgs)

        # 4. TIER 1: Truncate tool args in old messages (batch gated)
        truncated_messages, truncated, originals = self._truncate_args(
            effective_messages,
            request.system_message,
            request.tools,
            total_tokens=total_tokens,
            last_truncation_msg_count=last_truncation_msg_count,
        )

        # Track whether state needs persisting via ExtendedModelResponse
        state_changed = False

        # Offload original args to backend before they're lost (skip already-offloaded)
        if truncated and originals:
            new_originals = {
                k: v for k, v in originals.items() if k not in offloaded_tool_call_ids
            }
            skipped_count = len(originals) - len(new_originals)
            if new_originals:
                await aoffload_truncated_args(self._backend, new_originals)
                offloaded_tool_call_ids.update(new_originals)
                state_changed = True
                self._emit_context_signal(
                    "offload",
                    "complete",
                    kind="args",
                    offloaded_args=len(new_originals),
                )
                if skipped_count:
                    logger.info(
                        "[Summarization] Offloaded %d new tool args, skipped %d already-offloaded",
                        len(new_originals),
                        skipped_count,
                    )
            elif skipped_count:
                logger.debug(
                    "[Summarization] Tier 1 args: %d truncated in-memory, all already offloaded",
                    skipped_count,
                )

        # 4b. TIER 1 (cont.): Offload duplicate/non-critical Read results
        truncated_messages, read_truncated, read_offloaded_ids = (
            self._truncate_read_results(
                truncated_messages,
                request.system_message,
                request.tools,
                total_tokens=total_tokens,
                last_truncation_msg_count=last_truncation_msg_count,
            )
        )
        if read_truncated and read_offloaded_ids:
            new_ids = read_offloaded_ids - offloaded_read_result_ids
            if new_ids:
                offloaded_read_result_ids.update(new_ids)
                state_changed = True
                self._emit_context_signal(
                    "offload",
                    "complete",
                    kind="reads",
                    offloaded_reads=len(new_ids),
                )
            else:
                logger.debug(
                    "[Summarization] Tier 1 reads: %d truncated in-memory, all already offloaded",
                    len(read_offloaded_ids),
                )

        # Advance batch counter when Tier 1 produced new offloads
        if state_changed:
            last_truncation_msg_count = len(effective_messages)

        # 5. TIER 2: Check if summarization is needed
        if not self._should_summarize(truncated_messages, total_tokens):
            try:
                response = await handler(request.override(messages=truncated_messages))
                cached_input_tokens, cached_output_tokens = self._extract_token_usage(
                    response
                )
                return ExtendedModelResponse(
                    model_response=response,
                    command=Command(
                        update=self._build_state_update(
                            offloaded_tool_call_ids=offloaded_tool_call_ids,
                            offloaded_read_result_ids=offloaded_read_result_ids,
                            last_truncation_msg_count=last_truncation_msg_count,
                            cached_input_tokens=cached_input_tokens,
                            cached_output_tokens=cached_output_tokens,
                        )
                    ),
                )
            except ContextOverflowError:
                # Fall through to summarization as emergency fallback
                logger.warning(
                    "[Summarization] ContextOverflowError caught, triggering emergency summarization"
                )

        # 6. Summarization needed
        cutoff_index = self._determine_cutoff_index(truncated_messages)
        if cutoff_index <= 0:
            # Can't summarize — too few messages
            response = await handler(request.override(messages=truncated_messages))
            cached_input_tokens, cached_output_tokens = self._extract_token_usage(
                response
            )
            return ExtendedModelResponse(
                model_response=response,
                command=Command(
                    update=self._build_state_update(
                        offloaded_tool_call_ids=offloaded_tool_call_ids,
                        offloaded_read_result_ids=offloaded_read_result_ids,
                        last_truncation_msg_count=last_truncation_msg_count,
                        cached_input_tokens=cached_input_tokens,
                        cached_output_tokens=cached_output_tokens,
                    )
                ),
            )

        messages_to_summarize, preserved_messages = self._partition_messages(
            truncated_messages, cutoff_index
        )

        # Reset token cache (context is about to change dramatically)
        cached_input_tokens = 0
        cached_output_tokens = 0

        # Offload evicted messages to backend (non-fatal)
        file_path = await aoffload_to_backend(self._backend, messages_to_summarize)

        # Generate summary (emits SSE start/complete/error signals)
        summary = await self._acreate_summary(
            messages_to_summarize, original_count=len(truncated_messages)
        )

        # Build summary message
        summary_message = self._build_summary_message(summary, file_path)

        # Compute absolute cutoff for chained summarization tracking
        state_cutoff_index = self._compute_absolute_cutoff(cutoff_index, previous_event)

        # Create summarization event for state
        new_event: SummarizationEvent = {
            "cutoff_index": state_cutoff_index,
            "summary_message": summary_message,
            "file_path": file_path,
        }

        # Call handler with summarized messages
        modified_messages = [summary_message, *preserved_messages]
        response = await handler(request.override(messages=modified_messages))

        # Cache tokens from the new (reduced) context
        cached_input_tokens, cached_output_tokens = self._extract_token_usage(response)

        # Reset batch counter after summarization (message count drops dramatically)
        last_truncation_msg_count = 0

        # Return with state update to persist summarization event + offloaded IDs
        return ExtendedModelResponse(
            model_response=response,
            command=Command(
                update={
                    "_summarization_event": new_event,
                    **self._build_state_update(
                        offloaded_tool_call_ids=offloaded_tool_call_ids,
                        offloaded_read_result_ids=offloaded_read_result_ids,
                        last_truncation_msg_count=last_truncation_msg_count,
                        cached_input_tokens=cached_input_tokens,
                        cached_output_tokens=cached_output_tokens,
                    ),
                }
            ),
        )

    # =========================================================================
    # wrap_model_call — sync fallback (skips backend persistence)
    # =========================================================================

    @override
    def wrap_model_call(
        self,
        request: ModelRequest,
        handler: Callable[[ModelRequest], ModelResponse],
    ) -> ModelResponse | ExtendedModelResponse:
        """Sync fallback — same flow as awrap_model_call but skips backend offloading."""
        # 1. Load all per-invocation state from graph state into locals
        previous_event: SummarizationEvent | None = request.state.get(
            "_summarization_event"
        )
        offloaded_tool_call_ids = set(
            request.state.get("_offloaded_tool_call_ids") or ()
        )
        offloaded_read_result_ids = set(
            request.state.get("_offloaded_read_result_ids") or ()
        )
        last_truncation_msg_count: int = request.state.get("_truncation_batch_count", 0)
        cached_input_tokens: int = request.state.get("_cached_input_tokens", 0)
        cached_output_tokens: int = request.state.get("_cached_output_tokens", 0)

        self._ensure_message_ids(request.messages)
        effective_messages = self._get_effective_messages(
            request.messages, previous_event
        )

        truncated_messages, truncated, originals = self._truncate_args(
            effective_messages,
            request.system_message,
            request.tools,
            last_truncation_msg_count=last_truncation_msg_count,
        )
        # Note: sync path skips backend offloading for truncated args

        state_changed = False

        # Track newly truncated args (no backend offload in sync path)
        if truncated and originals:
            new_originals = {
                k: v for k, v in originals.items() if k not in offloaded_tool_call_ids
            }
            if new_originals:
                offloaded_tool_call_ids.update(new_originals)
                state_changed = True

        # Tier 1 (cont.): Truncate duplicate/non-critical Read results
        truncated_messages, read_truncated, read_offloaded_ids = (
            self._truncate_read_results(
                truncated_messages,
                request.system_message,
                request.tools,
                last_truncation_msg_count=last_truncation_msg_count,
            )
        )
        if read_truncated and read_offloaded_ids:
            new_ids = read_offloaded_ids - offloaded_read_result_ids
            if new_ids:
                offloaded_read_result_ids.update(new_ids)
                state_changed = True

        # Advance batch counter when Tier 1 produced new offloads
        if state_changed:
            last_truncation_msg_count = len(effective_messages)

        if cached_input_tokens > 0:
            total_tokens = cached_input_tokens + cached_output_tokens
        else:
            total_tokens = self.token_counter(truncated_messages)

        if not self._should_summarize(truncated_messages, total_tokens):
            try:
                response = handler(request.override(messages=truncated_messages))
                cached_input_tokens, cached_output_tokens = self._extract_token_usage(
                    response
                )
                return ExtendedModelResponse(
                    model_response=response,
                    command=Command(
                        update=self._build_state_update(
                            offloaded_tool_call_ids=offloaded_tool_call_ids,
                            offloaded_read_result_ids=offloaded_read_result_ids,
                            last_truncation_msg_count=last_truncation_msg_count,
                            cached_input_tokens=cached_input_tokens,
                            cached_output_tokens=cached_output_tokens,
                        )
                    ),
                )
            except ContextOverflowError:
                logger.warning(
                    "[Summarization] ContextOverflowError caught, triggering emergency summarization"
                )

        cutoff_index = self._determine_cutoff_index(truncated_messages)
        if cutoff_index <= 0:
            response = handler(request.override(messages=truncated_messages))
            cached_input_tokens, cached_output_tokens = self._extract_token_usage(
                response
            )
            return ExtendedModelResponse(
                model_response=response,
                command=Command(
                    update=self._build_state_update(
                        offloaded_tool_call_ids=offloaded_tool_call_ids,
                        offloaded_read_result_ids=offloaded_read_result_ids,
                        last_truncation_msg_count=last_truncation_msg_count,
                        cached_input_tokens=cached_input_tokens,
                        cached_output_tokens=cached_output_tokens,
                    )
                ),
            )

        messages_to_summarize, preserved_messages = self._partition_messages(
            truncated_messages, cutoff_index
        )

        cached_input_tokens = 0
        cached_output_tokens = 0

        # Sync path skips backend persistence (SandboxBackend is async-only)
        file_path = None

        summary = self._create_summary(
            messages_to_summarize, original_count=len(truncated_messages)
        )
        summary_message = self._build_summary_message(summary, file_path)
        state_cutoff_index = self._compute_absolute_cutoff(cutoff_index, previous_event)

        new_event: SummarizationEvent = {
            "cutoff_index": state_cutoff_index,
            "summary_message": summary_message,
            "file_path": file_path,
        }

        modified_messages = [summary_message, *preserved_messages]
        response = handler(request.override(messages=modified_messages))
        cached_input_tokens, cached_output_tokens = self._extract_token_usage(response)

        # Reset batch counter after summarization
        last_truncation_msg_count = 0

        return ExtendedModelResponse(
            model_response=response,
            command=Command(
                update={
                    "_summarization_event": new_event,
                    **self._build_state_update(
                        offloaded_tool_call_ids=offloaded_tool_call_ids,
                        offloaded_read_result_ids=offloaded_read_result_ids,
                        last_truncation_msg_count=last_truncation_msg_count,
                        cached_input_tokens=cached_input_tokens,
                        cached_output_tokens=cached_output_tokens,
                    ),
                }
            ),
        )

    # =========================================================================
    # Effective message reconstruction
    # =========================================================================

    @staticmethod
    def _get_effective_messages(
        messages: list[AnyMessage],
        event: SummarizationEvent | None,
    ) -> list[AnyMessage]:
        """Delegate to shared utility."""
        return get_effective_messages(messages, event)

    @staticmethod
    def _compute_absolute_cutoff(
        effective_cutoff: int,
        previous_event: SummarizationEvent | None,
    ) -> int:
        """Delegate to shared utility."""
        return compute_absolute_cutoff(effective_cutoff, previous_event)

    # =========================================================================
    # Token cache management
    # =========================================================================

    def _extract_token_usage(self, response: ModelResponse) -> tuple[int, int]:
        """Extract token usage from model response and emit to frontend.

        Args:
            response: The ModelResponse from handler().

        Returns:
            (input_tokens, output_tokens) tuple. Returns (0, 0) if no usage found.
        """
        if not response.result:
            return (0, 0)

        for msg in reversed(response.result):
            if not isinstance(msg, AIMessage):
                continue

            # Use shared extract_token_usage which handles all provider formats
            usage = extract_token_usage(msg)
            input_tokens = usage.get("input_tokens", 0)
            output_tokens = usage.get("output_tokens", 0)

            if input_tokens > 0:
                logger.debug(
                    f"[Summarization] Token usage: "
                    f"input={input_tokens}, output={output_tokens}"
                )

                # Emit token usage to frontend via _emit_context_signal
                # (ensures checkpoint_ns is included for proper agent identification)
                self._emit_context_signal(
                    "token_usage",
                    "complete",
                    input_tokens=input_tokens,
                    output_tokens=output_tokens,
                    total_tokens=input_tokens + output_tokens,
                )

                return (input_tokens, output_tokens)

        return (0, 0)

    # =========================================================================
    # Tier 1: Tool argument truncation
    # =========================================================================

    def _should_truncate_args(
        self,
        messages: list[AnyMessage],
        total_tokens: int,
        last_truncation_msg_count: int = 0,
    ) -> bool:
        """Check if argument truncation should be triggered (batch gated).

        Uses batch gating: after truncation fires at N messages, the next
        trigger waits until N + trigger_value messages. This avoids cache
        invalidation and SSE notification spam on every turn.

        Args:
            messages: Current effective message history.
            total_tokens: Total token count of messages.
            last_truncation_msg_count: Message count at last Tier 1 trigger.

        Returns:
            True if truncation should occur.
        """
        if self._truncate_args_trigger is None:
            return False

        trigger_type, trigger_value = self._truncate_args_trigger

        if trigger_type == "messages":
            next_trigger = last_truncation_msg_count + trigger_value
            return len(messages) >= next_trigger
        if trigger_type == "tokens":
            return total_tokens >= trigger_value
        if trigger_type == "fraction":
            max_input_tokens = self._get_profile_limits()
            if max_input_tokens is None:
                return False
            threshold = int(max_input_tokens * trigger_value)
            if threshold <= 0:
                threshold = 1
            return total_tokens >= threshold

        return False

    def _determine_truncate_cutoff_index(self, messages: list[AnyMessage]) -> int:
        """Determine the cutoff index for argument truncation based on keep policy.

        Messages at index >= cutoff are protected from truncation.
        Messages at index < cutoff can have their tool args truncated.

        Args:
            messages: Current effective message history.

        Returns:
            Index where truncation cutoff occurs.
        """
        keep_type, keep_value = self._truncate_args_keep

        if keep_type == "messages":
            if len(messages) <= keep_value:
                return len(messages)  # All messages are recent
            return int(len(messages) - keep_value)

        if keep_type in {"tokens", "fraction"}:
            if keep_type == "fraction":
                max_input_tokens = self._get_profile_limits()
                if max_input_tokens is None:
                    messages_to_keep = 20
                    if len(messages) <= messages_to_keep:
                        return len(messages)
                    return len(messages) - messages_to_keep
                target_token_count = int(max_input_tokens * keep_value)
            else:
                target_token_count = int(keep_value)

            if target_token_count <= 0:
                target_token_count = 1

            tokens_kept = 0
            for i in range(len(messages) - 1, -1, -1):
                msg_tokens = self.token_counter([messages[i]])
                if tokens_kept + msg_tokens > target_token_count:
                    return i + 1
                tokens_kept += msg_tokens
            return 0

        return len(messages)

    def _truncate_args(
        self,
        messages: list[AnyMessage],
        system_message: Any | None,
        tools: list[Any] | None,
        *,
        total_tokens: int | None = None,
        last_truncation_msg_count: int = 0,
    ) -> tuple[list[AnyMessage], bool, dict[str, dict[str, Any]]]:
        """Truncate large tool call arguments in old messages.

        Only processes messages before the keep cutoff. Only modifies AIMessages
        with tool calls to truncatable tools (Write, Edit, ExecuteCode).

        Args:
            messages: Effective messages to potentially truncate.
            system_message: Optional system message for token counting.
            tools: Optional tools for token counting.
            total_tokens: Pre-computed token count (avoids redundant counting).
            last_truncation_msg_count: Message count at last Tier 1 trigger.

        Returns:
            Tuple of (messages, modified, originals). If modified is False,
            messages is the same list object as input. originals maps
            tool_call_id -> {"name": str, "args": dict} for calls that were
            truncated, so callers can offload the original content.
        """
        # Count tokens for truncation threshold check
        if total_tokens is None:
            counted_messages = (
                [system_message, *messages] if system_message is not None else messages
            )
            total_tokens = self.token_counter(counted_messages)

        if not self._should_truncate_args(
            messages, total_tokens, last_truncation_msg_count
        ):
            return messages, False, {}

        cutoff_index = self._determine_truncate_cutoff_index(messages)
        if cutoff_index >= len(messages):
            return messages, False, {}

        # Compute thread_dir so truncation markers can reference the offload path
        thread_dir = None
        if self._backend is not None:
            thread_dir = f".agents/threads/{get_thread_id()}"

        return truncate_message_args(
            messages,
            cutoff_index,
            self._max_arg_length,
            self._truncation_text,
            thread_dir,
        )

    def _truncate_read_results(
        self,
        messages: list[AnyMessage],
        system_message: Any | None,
        tools: list[Any] | None,
        *,
        total_tokens: int | None = None,
        last_truncation_msg_count: int = 0,
    ) -> tuple[list[AnyMessage], bool, set[str]]:
        """Truncate duplicate and non-critical Read tool results in old messages.

        Reuses the same threshold/cutoff logic as _truncate_args to decide whether
        and where to apply Read result truncation.

        Args:
            messages: Effective messages to potentially truncate.
            system_message: Optional system message for token counting.
            tools: Optional tools for token counting.
            total_tokens: Pre-computed token count (avoids redundant counting).
            last_truncation_msg_count: Message count at last Tier 1 trigger.

        Returns:
            Tuple of (messages, modified, offloaded_tool_call_ids).
        """
        if total_tokens is None:
            counted_messages = (
                [system_message, *messages] if system_message is not None else messages
            )
            total_tokens = self.token_counter(counted_messages)

        if not self._should_truncate_args(
            messages, total_tokens, last_truncation_msg_count
        ):
            return messages, False, set()

        cutoff_index = self._determine_truncate_cutoff_index(messages)
        if cutoff_index >= len(messages):
            return messages, False, set()

        return truncate_read_results(messages, cutoff_index)

    # =========================================================================
    # Summary message construction
    # =========================================================================

    def _build_summary_message(
        self, summary: str, file_path: str | None = None
    ) -> HumanMessage:
        """Delegate to shared utility."""
        return build_summary_message(summary, file_path)

    # =========================================================================
    # Summarization trigger and cutoff logic
    # =========================================================================

    def _should_summarize(self, messages: list[AnyMessage], total_tokens: int) -> bool:
        """Determine whether summarization should run for the current token usage."""
        if not self._trigger_conditions:
            return False

        for kind, value in self._trigger_conditions:
            if kind == "messages" and len(messages) >= value:
                return True
            if kind == "tokens" and total_tokens >= value:
                logger.info(
                    f"[Summarization] Triggered: {total_tokens} >= {value} tokens"
                )
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

    def _validate_context_size(
        self, context: ContextSize, parameter_name: str
    ) -> ContextSize:
        """Validate context configuration tuples."""
        kind, value = context
        if kind == "fraction":
            if not 0 < value <= 1:
                msg = f"Fractional {parameter_name} values must be between 0 and 1, got {value}."
                raise ValueError(msg)
        elif kind in {"tokens", "messages"}:
            if value <= 0:
                msg = (
                    f"{parameter_name} thresholds must be greater than 0, got {value}."
                )
                raise ValueError(msg)
        else:
            msg = f"Unsupported context size type {kind} for {parameter_name}."
            raise ValueError(msg)
        return context

    # =========================================================================
    # Summary generation
    # =========================================================================

    def _extract_summary_text(self, response: Any) -> str:
        """Extract text content from LLM response, discarding reasoning/thinking.

        Args:
            response: The LLM response object

        Returns:
            Extracted text content, stripped
        """
        content = response.content if hasattr(response, "content") else response
        additional_kwargs = getattr(response, "additional_kwargs", None)
        formatted = format_llm_content(content, additional_kwargs)
        summary = formatted.get("text", "")

        # Log if reasoning was discarded
        if formatted.get("reasoning"):
            logger.debug(
                f"[Summarization] Discarded reasoning content "
                f"(length={len(formatted.get('reasoning', ''))})"
            )

        return summary.strip()

    def _emit_context_signal(self, action: str, signal: str, **kwargs: Any) -> None:
        """Emit a context_window event via stream writer.

        Args:
            action: Action discriminator ("summarize", "offload", "token_usage")
            signal: Signal type ("start", "complete", or "error")
            **kwargs: Additional payload fields (summary_length, error, truncated_count, etc.)
        """
        try:
            stream_writer = get_stream_writer()
            payload: dict[str, Any] = {
                "type": "context_window",
                "action": action,
                "signal": signal,
            }
            # Include checkpoint_ns for agent identification by streaming handler
            try:
                config = get_config()
                checkpoint_ns = config.get("configurable", {}).get("checkpoint_ns", "")
                if checkpoint_ns:
                    payload["checkpoint_ns"] = checkpoint_ns
            except RuntimeError:
                pass
            payload.update(kwargs)
            stream_writer(payload)
            if signal == "start":
                logger.debug(f"[Summarization] Emitted {action} start signal")
            elif signal == "complete":
                logger.debug(f"[Summarization] Emitted {action} complete signal")
            elif signal == "error":
                logger.warning(
                    f"[Summarization] Emitted {action} error signal: {kwargs.get('error')}"
                )
        except Exception as e:
            logger.debug(f"Could not emit context_window {action}/{signal} signal: {e}")

    def _get_thread_id(self) -> str:
        """Get the current thread ID from LangGraph config."""
        try:
            config = get_config()
            return config.get("configurable", {}).get("thread_id", "")
        except RuntimeError:
            return ""

    def _build_state_update(
        self,
        offloaded_tool_call_ids: set[str],
        offloaded_read_result_ids: set[str],
        last_truncation_msg_count: int,
        cached_input_tokens: int,
        cached_output_tokens: int,
    ) -> dict[str, Any]:
        """Build a state update dict for persisting per-invocation state."""
        return {
            "_offloaded_tool_call_ids": offloaded_tool_call_ids,
            "_offloaded_read_result_ids": offloaded_read_result_ids,
            "_truncation_batch_count": last_truncation_msg_count,
            "_cached_input_tokens": cached_input_tokens,
            "_cached_output_tokens": cached_output_tokens,
        }

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

    def _find_safe_cutoff(
        self, messages: list[AnyMessage], messages_to_keep: int
    ) -> int:
        """Find safe cutoff point that preserves AI/Tool message pairs."""
        if len(messages) <= messages_to_keep:
            return 0

        target_cutoff = len(messages) - messages_to_keep
        return self._find_safe_cutoff_point(messages, target_cutoff)

    def _find_safe_cutoff_point(
        self, messages: list[AnyMessage], cutoff_index: int
    ) -> int:
        """Find a safe cutoff point that doesn't split AI/Tool message pairs."""
        while cutoff_index < len(messages) and isinstance(
            messages[cutoff_index], ToolMessage
        ):
            cutoff_index += 1
        return cutoff_index

    def _create_summary(
        self, messages_to_summarize: list[AnyMessage], *, original_count: int = 0
    ) -> str:
        """Generate summary for the given messages (sync version)."""
        if not messages_to_summarize:
            return "No previous conversation history."

        trimmed_messages = self._trim_messages_for_summary(messages_to_summarize)
        if not trimmed_messages:
            return "Previous conversation was too long to summarize."

        # Strip base64 blobs so the summarization LLM doesn't receive them
        trimmed_messages = strip_base64_from_messages(trimmed_messages)

        try:
            self._emit_context_signal("summarize", "start")
            response = self.model.invoke(
                self.summary_prompt.format(messages=trimmed_messages)
            )
            summary = self._extract_summary_text(response)
            self._emit_context_signal(
                "summarize",
                "complete",
                summary_length=len(summary),
                original_message_count=original_count,
            )
            return summary
        except Exception as e:
            self._emit_context_signal("summarize", "error", error=str(e))
            return f"Error generating summary: {e!s}"

    async def _acreate_summary(
        self, messages_to_summarize: list[AnyMessage], *, original_count: int = 0
    ) -> str:
        """Generate summary for the given messages (async version with custom events)."""
        if not messages_to_summarize:
            return "No previous conversation history."

        trimmed_messages = self._trim_messages_for_summary(messages_to_summarize)
        if not trimmed_messages:
            return "Previous conversation was too long to summarize."

        # Offload base64 blobs to sandbox (or strip if no backend)
        trimmed_messages = await aoffload_base64_content(
            self._backend, trimmed_messages
        )

        try:
            self._emit_context_signal("summarize", "start")

            # Use ainvoke (non-streaming) to avoid duplicate events
            # The model should have streaming=False set in factory
            response = await self.model.ainvoke(
                self.summary_prompt.format(messages=trimmed_messages)
            )

            summary = self._extract_summary_text(response)
            self._emit_context_signal(
                "summarize",
                "complete",
                summary_length=len(summary),
                original_message_count=original_count,
            )
            return summary
        except Exception as e:
            self._emit_context_signal("summarize", "error", error=str(e))
            return f"Error generating summary: {e!s}"

    def _trim_messages_for_summary(
        self, messages: list[AnyMessage]
    ) -> list[AnyMessage]:
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

    # =========================================================================
    # Factory
    # =========================================================================

    @classmethod
    def from_config(
        cls,
        config: dict | None = None,
        backend: Any | None = None,
    ) -> "SummarizationMiddleware | None":
        """Create a configured instance from agent_config.yaml settings.

        Args:
            config: Optional config override (defaults to SummarizationConfig defaults).
            backend: Backend for offloading conversation history (SandboxBackend
                for PTC, None for flash). When None, no filesystem ops are attempted.

        Returns:
            Configured SummarizationMiddleware or None if disabled.
        """
        if config is None:
            config = SummarizationConfig().model_dump()

        if not config.get("enabled", False):
            return None

        # Get summarization model from config (prefer pre-built OAuth/BYOK client)
        llm_client = config.get("_llm_client")
        if llm_client is not None:
            summarization_model: BaseChatModel = llm_client
        else:
            model_name = config.get("llm", "")
            summarization_model: BaseChatModel = get_llm_by_type(model_name)

        # Disable streaming to prevent normal message_chunk events
        # This ensures only our custom context_window events are emitted
        if hasattr(summarization_model, "streaming"):
            summarization_model.streaming = False

        # Get configuration values
        token_threshold = config.get("token_threshold", 120000)
        keep_messages = config.get("keep_messages", 5)

        # Build truncate_args_settings from config (None disables truncation)
        truncate_args_settings: TruncateArgsSettings | None = None
        truncate_trigger_messages = config.get("truncate_args_trigger_messages")
        if truncate_trigger_messages is not None:
            truncate_keep_messages = config.get("truncate_args_keep_messages", 20)
            truncate_max_length = config.get("truncate_args_max_length", 2000)
            truncate_args_settings = TruncateArgsSettings(
                trigger=("messages", int(truncate_trigger_messages)),
                keep=("messages", int(truncate_keep_messages)),
                max_length=int(truncate_max_length),
            )

        return cls(
            model=summarization_model,
            trigger=("tokens", token_threshold),
            keep=("messages", keep_messages),
            trim_tokens_to_summarize=token_threshold + 50000,
            summary_prompt=DEFAULT_SUMMARY_PROMPT,
            backend=backend,
            truncate_args_settings=truncate_args_settings,
        )
