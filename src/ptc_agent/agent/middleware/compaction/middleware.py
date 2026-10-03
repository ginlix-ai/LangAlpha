"""CompactionMiddleware — two-tier context management for LangGraph agents.

Based on deepagent's SummarizationMiddleware but modified to:
- Emit unified 'context_window' SSE events (discriminated by action field)
- Use get_stream_writer() for lifecycle signaling
- Use wrap_model_call for non-destructive context management (preserves checkpoint)
- Two-tier context management: tool arg truncation + LLM summarization

Actions emitted via context_window events (values preserved as wire protocol):
- token_usage: after each model call (input/output/total tokens)
- summarize: start/complete/error signals during LLM summarization
- offload: complete signal after Tier 1 tool arg truncation
"""

import asyncio
import time
import warnings
import logging
from collections.abc import Awaitable, Callable
from typing import Any, cast

from langchain_core.messages import AIMessage, AnyMessage
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

from src.config.settings import get_compaction_timeout
from src.llms.content_utils import format_llm_content
from src.llms.token_counter import extract_token_usage
from ptc_agent.config.agent import CompactionConfig
from src.llms import get_llm_by_type, maybe_disable_streaming

from ptc_agent.agent.state import ensure_message_ids
from ptc_agent.agent.middleware.compaction.types import (
    CompactionEvent,
    CompactionState,
    ContextSize,
    TruncateArgsSettings,
    TokenCounter,
    _DEFAULT_MESSAGES_TO_KEEP,
    _DEFAULT_TRIM_TOKEN_LIMIT,
)
from ptc_agent.agent.middleware.compaction.summary_request import (
    DEFAULT_SUMMARY_PROMPT,
    build_summary_request,
)
from ptc_agent.agent.middleware.compaction.utils import (
    build_summary_event,
    count_tokens_tiktoken,
    find_group_safe_cutoff,
    get_effective_messages,
    strip_base64_from_messages,
    partition_at_cutoff,
)
from ptc_agent.agent.middleware.compaction.model import (
    max_input_tokens,
    summary_trim_budget,
    trim_for_summary,
)
from ptc_agent.agent.middleware.compaction.offloading import (
    aoffload_base64_content,
    apply_recorded_offloads,
    idle_offloads,
    offload_turns,
    tool_call_ids,
)
from ptc_agent.agent.transcript import TranscriptTarget
from ptc_agent.agent.transcript.pointer import (
    TranscriptTurns,
    aexport_transcript,
    transcript_target,
)

logger = logging.getLogger(__name__)


class CompactionMiddleware(AgentMiddleware):
    """
    Custom compaction middleware that emits SSE events for frontend visibility.

    Manages the full context window lifecycle: token counting, tool-arg
    truncation, Read-result deduplication, base64 offloading, sandbox
    persistence of evicted messages, and LLM-based summarization.

    Uses wrap_model_call to reconstruct the message list on-the-fly without
    modifying the LangGraph checkpoint — preserving full history, enabling
    recovery, and supporting chained compactions.

    Two-tier context management:
    - Tier 1: trims large tool args and stale Read results, in before_agent,
      only when a turn starts after a long pause (the prompt cache is cold)
    - Tier 2: Full LLM summarization (expensive, fires at token count threshold)

    Key differences from LangChain's SummarizationMiddleware:
    - Emits unified 'context_window' events via get_stream_writer()
    - Actions: "summarize" (start/complete/error), "offload" (complete), "token_usage"
    - Does NOT stream intermediate chunks (to avoid duplicate events)
    """

    state_schema = CompactionState

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
        Initialize custom compaction middleware.

        Args:
            model: The language model to use for generating summaries.
            trigger: Threshold(s) that trigger full compaction (summarization).
            keep: How much context to retain after compaction.
            token_counter: Function to count tokens in messages.
            summary_prompt: Prompt template for generating summaries.
            trim_tokens_to_summarize: Max tokens to keep for summarization call.
            backend: Backend for offloading conversation history (SandboxBackend for PTC,
                None for flash). When None, no filesystem ops are attempted.
            truncate_args_settings: Settings for Tier 1. When None, Tier 1 is off.
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
        # One failed summary stops summarizing for the life of this instance,
        # which is one turn: the agent is built per turn and its subagents
        # share it. A hung summary model would otherwise hold every later call
        # over the threshold for the whole timeout.
        self._summary_failed = False

        # Backend for offloading conversation history to sandbox (immutable config)
        self._backend = backend

        tier1 = truncate_args_settings or {}
        idle_minutes = tier1.get("idle_minutes", 90)
        self._idle_seconds: float | None = (
            float(idle_minutes) * 60
            if truncate_args_settings is not None and idle_minutes is not None
            else None
        )
        self._truncate_keep = int(tier1.get("keep_messages", 20))
        self._max_arg_length = int(tier1.get("max_length", 2000))
        self._truncation_text = tier1.get("truncation_text", "...(argument truncated)")

        requires_profile = any(
            condition[0] == "fraction" for condition in self._trigger_conditions
        )
        if self.keep[0] == "fraction":
            requires_profile = True
        if requires_profile and max_input_tokens(self.model) is None:
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
        2. Re-apply the recorded Tier 1 offloads (chosen in abefore_agent)
        3. TIER 2: Check if full summarization is needed (expensive, fires later)
           - NO: call handler with truncated messages, cache tokens, return
             (catch ContextOverflowError -> fall through to summarize)
           - YES: offload -> summarize -> call handler -> cache tokens -> return
                  ExtendedModelResponse with state update
        """
        # 1. Read all per-invocation state from graph state into locals.
        #    No self._ mutations — keeps the middleware instance stateless and
        #    safe to share across concurrent invocations.
        previous_event: CompactionEvent | None = request.state.get(
            "_summarization_event"
        )
        offloaded_tool_call_ids = set(
            request.state.get("_offloaded_tool_call_ids") or ()
        )
        offloaded_read_result_ids = set(
            request.state.get("_offloaded_read_result_ids") or ()
        )
        cached_input_tokens: int = request.state.get("_cached_input_tokens", 0)
        cached_output_tokens: int = request.state.get("_cached_output_tokens", 0)

        # 2. Reconstruct effective messages, with every recorded Tier 1
        #    offload applied (Tier 1 itself runs in abefore_agent).
        ensure_message_ids(request.messages)
        effective = self._get_effective_messages(request.messages, previous_event)
        truncated_messages = self._apply_recorded_offloads(
            effective,
            offloaded_tool_call_ids,
            offloaded_read_result_ids,
            request.messages,
        )

        # 3. Count tokens once (prefer cached from last model call, fall back to tiktoken).
        if cached_input_tokens > 0:
            total_tokens = cached_input_tokens + cached_output_tokens
        else:
            counted_msgs = (
                [request.system_message, *truncated_messages]
                if request.system_message is not None
                else truncated_messages
            )
            total_tokens = self.token_counter(counted_msgs)

        # 4. TIER 2: Check if summarization is needed
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
                            cached_input_tokens, cached_output_tokens
                        )
                    ),
                )
            except ContextOverflowError:
                if self._summary_failed:
                    # The fallback is the summary that already failed this turn.
                    raise
                # Fall through to summarization as emergency fallback
                logger.warning(
                    "[Compaction] ContextOverflowError caught, triggering emergency summarization"
                )

        # 5. Summarization needed
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
                        cached_input_tokens, cached_output_tokens
                    )
                ),
            )

        messages_to_summarize, preserved_messages = partition_at_cutoff(
            truncated_messages, cutoff_index
        )

        # Reset token cache (context is about to change dramatically)
        cached_input_tokens = 0
        cached_output_tokens = 0

        # Summarize (emits SSE start/complete/error signals) while the
        # transcript catches up with this turn; the summary points at it only
        # if that save lands.
        target = self._transcript_target()
        summarized = self._trim_messages_for_summary(messages_to_summarize)
        summary, transcript = await asyncio.gather(
            self._acreate_summary(
                messages_to_summarize,
                original_count=len(truncated_messages),
                trimmed=summarized,
                turns=TranscriptTurns.of(target, request.messages) if target else None,
            ),
            aexport_transcript(self._backend, target, request.messages),
        )
        if summary is None:
            # A failed summary must not stand in for the history it was to
            # replace, so this compaction is dropped and the next turn retries
            # it on the same history.
            self._summary_failed = True
            logger.warning(
                "[Compaction] Summary failed, no further compaction this turn"
            )
            response = await handler(request.override(messages=truncated_messages))
            cached_input_tokens, cached_output_tokens = self._extract_token_usage(
                response
            )
            return ExtendedModelResponse(
                model_response=response,
                command=Command(
                    update=self._build_state_update(
                        cached_input_tokens, cached_output_tokens
                    )
                ),
            )

        # Create summarization event with an id anchor (cutoff grounded in raw list)
        new_event = build_summary_event(
            summary,
            transcript,
            raw_messages=request.messages,
            to_summarize=messages_to_summarize,
            summarized=summarized,
            preserved_messages=preserved_messages,
            original_message_count=len(truncated_messages),
            skill_files=self._backend is not None,
        )
        summary_message = new_event["summary_message"]

        # Call handler with summarized messages
        modified_messages = [summary_message, *preserved_messages]
        response = await handler(request.override(messages=modified_messages))

        # Cache tokens from the new (reduced) context
        cached_input_tokens, cached_output_tokens = self._extract_token_usage(response)

        # Summarized calls never reach the model again, so their ids are dead.
        live_ids = tool_call_ids(preserved_messages)
        offloaded_tool_call_ids &= live_ids
        offloaded_read_result_ids &= live_ids

        # Return with state update to persist summarization event + offloaded IDs
        return ExtendedModelResponse(
            model_response=response,
            command=Command(
                update={
                    "_summarization_event": new_event,
                    **self._build_state_update(
                        cached_input_tokens,
                        cached_output_tokens,
                        offloads=(offloaded_tool_call_ids, offloaded_read_result_ids),
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
        """Sync fallback: same flow as awrap_model_call, without the transcript."""
        # 1. Load all per-invocation state from graph state into locals
        previous_event: CompactionEvent | None = request.state.get(
            "_summarization_event"
        )
        offloaded_tool_call_ids = set(
            request.state.get("_offloaded_tool_call_ids") or ()
        )
        offloaded_read_result_ids = set(
            request.state.get("_offloaded_read_result_ids") or ()
        )
        cached_input_tokens: int = request.state.get("_cached_input_tokens", 0)
        cached_output_tokens: int = request.state.get("_cached_output_tokens", 0)

        ensure_message_ids(request.messages)
        effective = self._get_effective_messages(request.messages, previous_event)
        truncated_messages = self._apply_recorded_offloads(
            effective,
            offloaded_tool_call_ids,
            offloaded_read_result_ids,
            request.messages,
        )
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
                            cached_input_tokens, cached_output_tokens
                        )
                    ),
                )
            except ContextOverflowError:
                if self._summary_failed:
                    raise
                logger.warning(
                    "[Compaction] ContextOverflowError caught, triggering emergency summarization"
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
                        cached_input_tokens, cached_output_tokens
                    )
                ),
            )

        messages_to_summarize, preserved_messages = partition_at_cutoff(
            truncated_messages, cutoff_index
        )

        cached_input_tokens = 0
        cached_output_tokens = 0

        # Sync path skips the transcript export and its pointer (SandboxBackend
        # is async-only). It is not the runtime path (the agent runs async via
        # _acreate_summary, which carries the compaction_timeout); a blocking
        # invoke() can't be bounded by asyncio.wait_for, so no timeout here.
        summary = self._create_summary(
            messages_to_summarize, original_count=len(truncated_messages)
        )
        if summary is None:
            # Dropped like the async path's: the next turn retries it.
            self._summary_failed = True
            logger.warning(
                "[Compaction] Summary failed, no further compaction this turn"
            )
            response = handler(request.override(messages=truncated_messages))
            cached_input_tokens, cached_output_tokens = self._extract_token_usage(
                response
            )
            return ExtendedModelResponse(
                model_response=response,
                command=Command(
                    update=self._build_state_update(
                        cached_input_tokens, cached_output_tokens
                    )
                ),
            )
        new_event = build_summary_event(
            summary,
            None,
            raw_messages=request.messages,
            to_summarize=messages_to_summarize,
            preserved_messages=preserved_messages,
            original_message_count=len(truncated_messages),
            skill_files=self._backend is not None,
        )
        summary_message = new_event["summary_message"]

        modified_messages = [summary_message, *preserved_messages]
        response = handler(request.override(messages=modified_messages))
        cached_input_tokens, cached_output_tokens = self._extract_token_usage(response)

        live_ids = tool_call_ids(preserved_messages)
        offloaded_tool_call_ids &= live_ids
        offloaded_read_result_ids &= live_ids

        return ExtendedModelResponse(
            model_response=response,
            command=Command(
                update={
                    "_summarization_event": new_event,
                    **self._build_state_update(
                        cached_input_tokens,
                        cached_output_tokens,
                        offloads=(offloaded_tool_call_ids, offloaded_read_result_ids),
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
        event: CompactionEvent | None,
    ) -> list[AnyMessage]:
        """Delegate to shared utility."""
        return get_effective_messages(messages, event)

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
                    f"[Compaction] Token usage: "
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
    # Tier 1: trimming old tool args and Read results after a long pause
    # =========================================================================

    @override
    async def abefore_agent(self, state: Any, runtime: Any) -> dict[str, Any] | None:
        """Tier 1, once per turn and only when the model last answered more
        than the idle threshold ago: by then the provider's prompt cache has
        expired, so hiding part of the prefix costs no cache hit, while doing
        it mid-turn would throw a warm one away. A resume after an interrupt
        does not pass through here, since the graph picks up where it stopped.
        An argument is hidden only once the transcript holding it is saved,
        which is then its one copy the agent can read."""
        args, reads = self._idle_offloads(state)
        if args and not await self._save_transcript(state["messages"]):
            args = set()
        return self._record_offloads(state, args, reads)

    @override
    def before_agent(self, state: Any, runtime: Any) -> dict[str, Any] | None:
        # Reads only: the transcript an argument would point at saves async-only.
        _, reads = self._idle_offloads(state)
        return self._record_offloads(state, set(), reads)

    def _idle_offloads(self, state: Any) -> tuple[set[str], set[str]]:
        return idle_offloads(
            state,
            idle_seconds=self._idle_seconds,
            keep_messages=self._truncate_keep,
            max_length=self._max_arg_length,
            now=time.time(),
        )

    def _record_offloads(
        self, state: Any, args: set[str], reads: set[str]
    ) -> dict[str, Any] | None:
        update: dict[str, Any] = {}
        if args:
            known = set(state.get("_offloaded_tool_call_ids") or ())
            update["_offloaded_tool_call_ids"] = known | args
            self._emit_context_signal(
                "offload", "complete", kind="args", offloaded_args=len(args)
            )
        if reads:
            known = set(state.get("_offloaded_read_result_ids") or ())
            update["_offloaded_read_result_ids"] = known | reads
            self._emit_context_signal(
                "offload", "complete", kind="reads", offloaded_reads=len(reads)
            )
        return update or None

    def _transcript_target(self) -> TranscriptTarget | None:
        """This agent's transcript, or None where no mount serves one."""
        try:
            configurable = get_config().get("configurable", {})
        except RuntimeError:
            return None
        return transcript_target(
            self._backend,
            configurable.get("thread_id"),
            str(configurable.get("checkpoint_ns") or ""),
        )

    async def _save_transcript(self, messages: list[AnyMessage]) -> bool:
        target = self._transcript_target()
        return (
            target is not None
            and await aexport_transcript(self._backend, target, messages) is not None
        )

    def _apply_recorded_offloads(
        self,
        messages: list[AnyMessage],
        arg_ids: set[str],
        read_ids: set[str],
        raw_messages: list[AnyMessage],
    ) -> list[AnyMessage]:
        turns = None
        if arg_ids and self._backend is not None:
            try:
                configurable = get_config().get("configurable", {})
            except RuntimeError:
                configurable = {}
            turns = offload_turns(
                raw_messages,
                configurable.get("thread_id"),
                str(configurable.get("checkpoint_ns") or ""),
            )
        return apply_recorded_offloads(
            messages,
            arg_ids,
            read_ids,
            self._max_arg_length,
            self._truncation_text,
            turns,
        )

    # =========================================================================
    # Summarization trigger and cutoff logic
    # =========================================================================

    def _should_summarize(self, messages: list[AnyMessage], total_tokens: int) -> bool:
        """Determine whether summarization should run for the current token usage."""
        if not self._trigger_conditions or self._summary_failed:
            return False

        for kind, value in self._trigger_conditions:
            if kind == "messages" and len(messages) >= value:
                return True
            if kind == "tokens" and total_tokens >= value:
                logger.info(
                    f"[Compaction] Triggered: {total_tokens} >= {value} tokens"
                )
                return True
            if kind == "fraction":
                window = max_input_tokens(self.model)
                if window is None:
                    continue
                threshold = int(window * value)
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
            window = max_input_tokens(self.model)
            if window is None:
                return None
            target_token_count = int(window * value)
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

        return find_group_safe_cutoff(messages, cutoff_candidate)

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
                f"[Compaction] Discarded reasoning content "
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
                logger.debug(f"[Compaction] Emitted {action} start signal")
            elif signal == "complete":
                logger.debug(f"[Compaction] Emitted {action} complete signal")
            elif signal == "error":
                logger.warning(
                    f"[Compaction] Emitted {action} error signal: {kwargs.get('error')}"
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
        cached_input_tokens: int,
        cached_output_tokens: int,
        *,
        offloads: tuple[set[str], set[str]] | None = None,
    ) -> dict[str, Any]:
        """Build a state update dict for persisting per-invocation state.

        The offload id sets ride along only when they changed: each write is
        a new checkpoint blob, and they live as long as the thread. The
        response time is what the next turn's Tier 1 gate measures from.
        """
        update: dict[str, Any] = {
            "_cached_input_tokens": cached_input_tokens,
            "_cached_output_tokens": cached_output_tokens,
            "_last_model_response_at": time.time(),
        }
        if offloads is not None:
            update["_offloaded_tool_call_ids"], update["_offloaded_read_result_ids"] = offloads
        return update

    def _find_safe_cutoff(
        self, messages: list[AnyMessage], messages_to_keep: int
    ) -> int:
        """Find safe cutoff point that preserves AI/Tool message pairs."""
        if len(messages) <= messages_to_keep:
            return 0

        target_cutoff = len(messages) - messages_to_keep
        return find_group_safe_cutoff(messages, target_cutoff)

    def _create_summary(
        self, messages_to_summarize: list[AnyMessage], *, original_count: int = 0
    ) -> str | None:
        """Generate summary for the given messages (sync version).

        None means the call failed or came back empty, as in the async version.
        """
        if not messages_to_summarize:
            return "No previous conversation history."

        trimmed_messages = self._trim_messages_for_summary(messages_to_summarize)
        if not trimmed_messages:
            # Nothing new fits the summary budget: a failed summary.
            return None

        # Strip base64 blobs so the summarization LLM doesn't receive them
        trimmed_messages = strip_base64_from_messages(trimmed_messages)

        # Start is outside the try: if it fails, the window was never opened
        # so nothing needs closing. Inside the try we catch BaseException (not
        # just Exception) so CancelledError also closes the window before
        # propagating — otherwise a cancelled stream would leave an orphan
        # start event with no terminator.
        self._emit_context_signal("summarize", "start")
        try:
            response = self.model.invoke(
                build_summary_request(self.summary_prompt, trimmed_messages)
            )
            summary = self._extract_summary_text(response)
            if not summary:
                raise RuntimeError("Compaction LLM returned empty summary")
        except BaseException as e:
            self._emit_context_signal("summarize", "error", error=str(e))
            if isinstance(e, Exception):
                return None
            raise

        self._emit_context_signal(
            "summarize",
            "complete",
            summary_length=len(summary),
            original_message_count=original_count,
            summary_text=summary,
        )
        return summary

    async def _acreate_summary(
        self,
        messages_to_summarize: list[AnyMessage],
        *,
        original_count: int = 0,
        trimmed: list[AnyMessage] | None = None,
        turns: TranscriptTurns | None = None,
    ) -> str | None:
        """Generate summary for the given messages (async version with custom events).

        ``trimmed`` is the already-trimmed list, for a caller that also needs
        to know what trimming dropped. ``turns`` heads each turn of the history
        with its transcript file, for the summary to cite. None means the call
        failed or came back empty, as manual compaction treats it, and its
        error signal is out.
        """
        if not messages_to_summarize:
            return "No previous conversation history."

        trimmed_messages = (
            trimmed
            if trimmed is not None
            else self._trim_messages_for_summary(messages_to_summarize)
        )
        if not trimmed_messages:
            # Nothing new fits the summary budget: a failed summary.
            return None

        # Offload base64 blobs to sandbox (or strip if no backend)
        trimmed_messages = await aoffload_base64_content(
            self._backend, trimmed_messages
        )

        # Start is outside the try: if it fails, the window was never opened
        # so nothing needs closing. Inside the try we catch BaseException (not
        # just Exception) so CancelledError also closes the window before
        # propagating — otherwise a cancelled stream would leave an orphan
        # start event with no terminator.
        self._emit_context_signal("summarize", "start")
        try:
            # Use ainvoke (non-streaming) to avoid duplicate events.
            # The model should have streaming=False set in factory.
            # The bracketing context_window summarize start/complete/error
            # events tell the SSE handler to re-route chunks emitted between
            # them to the compaction_chunk channel.
            #
            # Wall-clock budget: a hung summarize raises TimeoutError, which the
            # except below treats like any LLM failure — emits the error signal
            # (closing the window so the admission guard releases) and returns
            # None. The timeout lives on the call so it fails naturally instead
            # of blocking the in-flight turn forever.
            response = await asyncio.wait_for(
                self.model.ainvoke(
                    build_summary_request(self.summary_prompt, trimmed_messages, turns)
                ),
                timeout=get_compaction_timeout(),
            )
            summary = self._extract_summary_text(response)
            if not summary:
                raise RuntimeError("Compaction LLM returned empty summary")
        except BaseException as e:
            self._emit_context_signal("summarize", "error", error=str(e))
            if isinstance(e, Exception):
                return None
            raise

        self._emit_context_signal(
            "summarize",
            "complete",
            summary_length=len(summary),
            original_message_count=original_count,
            summary_text=summary,
        )
        return summary

    def _trim_messages_for_summary(
        self, messages: list[AnyMessage]
    ) -> list[AnyMessage]:
        """Trim messages to fit within summary generation limits."""
        if self.trim_tokens_to_summarize is None:
            return messages
        return trim_for_summary(
            messages, self.trim_tokens_to_summarize, self.token_counter
        )

    # =========================================================================
    # Factory
    # =========================================================================

    @classmethod
    def from_config(
        cls,
        config: dict | None = None,
        backend: Any | None = None,
    ) -> "CompactionMiddleware | None":
        """Create a configured instance from agent_config.yaml settings.

        Args:
            config: Optional config override (defaults to CompactionConfig defaults).
            backend: Backend for offloading conversation history (SandboxBackend
                for PTC, None for flash). When None, no filesystem ops are attempted.

        Returns:
            Configured CompactionMiddleware or None if disabled.
        """
        if config is None:
            config = CompactionConfig().model_dump()

        if not config.get("enabled", False):
            return None

        # Get compaction model from config (prefer pre-built OAuth/BYOK client)
        llm_client = config.get("_llm_client")
        if llm_client is not None:
            compaction_model: BaseChatModel = llm_client
        else:
            model_name = config.get("llm", "")
            compaction_model: BaseChatModel = get_llm_by_type(model_name)

        # Suppress normal message_chunk emission where the provider permits it.
        maybe_disable_streaming(compaction_model)

        # Get configuration values
        token_threshold = config.get("token_threshold", 120000)
        keep_messages = config.get("keep_messages", 5)

        # Tier 1 settings (an idle threshold of None turns Tier 1 off)
        truncate_args_settings: TruncateArgsSettings | None = None
        idle_minutes = config.get("truncate_args_idle_minutes", 90)
        if idle_minutes is not None:
            truncate_args_settings = TruncateArgsSettings(
                idle_minutes=float(idle_minutes),
                keep_messages=int(config.get("truncate_args_keep_messages", 20)),
                max_length=int(config.get("truncate_args_max_length", 2000)),
            )

        return cls(
            model=compaction_model,
            trigger=("tokens", token_threshold),
            keep=("messages", keep_messages),
            trim_tokens_to_summarize=summary_trim_budget(compaction_model, token_threshold),
            summary_prompt=DEFAULT_SUMMARY_PROMPT,
            backend=backend,
            truncate_args_settings=truncate_args_settings,
        )
