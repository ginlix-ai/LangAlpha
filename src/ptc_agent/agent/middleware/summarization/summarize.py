"""Standalone functions for manual summarization and offloading triggers."""

import logging
import uuid
from typing import Any, cast

from langchain_core.messages import AnyMessage, RemoveMessage, ToolMessage
from langchain_core.messages.utils import trim_messages
from langgraph.graph.message import REMOVE_ALL_MESSAGES

from langchain.chat_models import BaseChatModel

from src.llms.content_utils import format_llm_content
from ptc_agent.config.agent import SummarizationConfig
from src.llms import get_llm_by_type

from ptc_agent.agent.middleware.summarization.types import (
    SummarizationEvent,
    _DEFAULT_FALLBACK_MESSAGE_COUNT,
)
from ptc_agent.agent.middleware.summarization.utils import (
    DEFAULT_SUMMARY_PROMPT,
    build_summary_message,
    compute_absolute_cutoff,
    count_tokens_tiktoken,
    get_effective_messages,
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


async def summarize_messages(
    messages: list[AnyMessage],
    keep_messages: int = 5,
    model_name: str = "",
    backend: Any | None = None,
    previous_event: SummarizationEvent | None = None,
    summarization_config: SummarizationConfig | None = None,
) -> dict[str, Any]:
    """
    Summarize conversation messages with two-tier context management.

    Produces a ``SummarizationEvent`` that the middleware can use to
    reconstruct the effective message list on subsequent model calls,
    without destructively replacing checkpoint messages.

    Two-tier offloading (when backend is provided):
    - Tier 1: Truncate large tool args in old messages, offload originals to sandbox
    - Tier 2: Summarize evicted messages, offload conversation history to sandbox

    When backend is None, offloading is skipped but truncation and summarization
    still occur.

    Args:
        messages: List of conversation messages to summarize (full state).
        keep_messages: Number of recent messages to preserve (default: 5).
        model_name: LLM model name for generating summaries (default: gpt-5-nano).
        backend: Optional DaytonaBackend for offloading to sandbox filesystem.
        previous_event: Previous SummarizationEvent for chained summarization.

    Returns:
        Dict with:
        - "event": SummarizationEvent to write to state
        - "summary_text": The generated summary text
        - "original_count": Number of effective messages before summarization
        - "preserved_count": Number of preserved messages + summary message
        - "offloaded_arg_ids": Set of tool call IDs whose args were truncated/offloaded
        - "offloaded_read_ids": Set of tool call IDs whose Read results were truncated

    Example:
        result = await summarize_messages(messages, previous_event=prev_event)
        await graph.aupdate_state(config, {"_summarization_event": result["event"]})
    """
    if not messages:
        raise ValueError("No messages to summarize")

    # Ensure all messages have IDs
    for msg in messages:
        if msg.id is None:
            msg.id = str(uuid.uuid4())

    # Reconstruct effective messages from previous event
    effective = get_effective_messages(messages, previous_event)

    # ---- Tier 1: Truncate large tool args + stale Read results in old messages ----
    config = (summarization_config or SummarizationConfig()).model_dump()
    truncate_trigger_messages = config.get("truncate_args_trigger_messages")
    offloaded_arg_ids: set[str] = set()
    offloaded_read_ids: set[str] = set()
    if truncate_trigger_messages is not None and len(effective) >= int(
        truncate_trigger_messages
    ):
        truncate_keep = int(config.get("truncate_args_keep_messages", 20))
        truncate_max_length = int(config.get("truncate_args_max_length", 2000))
        truncation_text = "...(argument truncated)"

        cutoff = max(0, len(effective) - truncate_keep)
        thread_dir = None
        if backend is not None:
            thread_dir = f".agent/threads/{get_thread_id()}"

        effective, truncated, originals = truncate_message_args(
            effective,
            cutoff,
            truncate_max_length,
            truncation_text,
            thread_dir,
        )

        # Offload original args before they're lost
        if truncated and originals and backend is not None:
            await aoffload_truncated_args(backend, originals)
            offloaded_arg_ids = set(originals.keys())

        # Truncate duplicate/non-critical Read results (same cutoff)
        effective, _read_truncated, offloaded_read_ids = truncate_read_results(
            effective, cutoff
        )

    # ---- Determine cutoff for summarization ----
    if len(effective) <= keep_messages:
        raise ValueError(
            f"Not enough messages to summarize. Have {len(effective)}, "
            f"need more than {keep_messages} to preserve."
        )

    target_cutoff = len(effective) - keep_messages
    # Adjust cutoff to not split AI/Tool message pairs
    cutoff_index = target_cutoff
    while cutoff_index < len(effective) and isinstance(
        effective[cutoff_index], ToolMessage
    ):
        cutoff_index += 1

    if cutoff_index <= 0:
        raise ValueError("Cannot determine valid cutoff point for summarization")

    messages_to_summarize = effective[:cutoff_index]
    preserved = effective[cutoff_index:]

    # ---- Tier 2: Offload evicted messages to backend ----
    file_path = await aoffload_to_backend(backend, messages_to_summarize)

    # ---- Generate summary ----
    summarization_model: BaseChatModel = get_llm_by_type(model_name)
    if hasattr(summarization_model, "streaming"):
        summarization_model.streaming = False

    token_threshold = config.get("token_threshold", 120000)
    trim_limit = token_threshold + 50000

    token_count = count_tokens_tiktoken(messages_to_summarize)
    if token_count > trim_limit:
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
            messages_to_summarize = messages_to_summarize[
                -_DEFAULT_FALLBACK_MESSAGE_COUNT:
            ]

    # Strip base64 blobs before sending to LLM
    messages_to_summarize = await aoffload_base64_content(
        backend, messages_to_summarize
    )

    try:
        response = await summarization_model.ainvoke(
            DEFAULT_SUMMARY_PROMPT.format(messages=messages_to_summarize)
        )

        content = response.content if hasattr(response, "content") else response
        additional_kwargs = getattr(response, "additional_kwargs", None)
        formatted = format_llm_content(content, additional_kwargs)
        summary_text = formatted.get("text", "").strip()

        if not summary_text:
            summary_text = "Previous conversation context (summary unavailable)."

    except Exception as e:
        logger.error(f"Error generating summary: {e}")
        summary_text = f"Previous conversation context (error: {e})"

    # Build summary message using shared utility
    summary_message = build_summary_message(summary_text, file_path)

    # Compute absolute cutoff for chaining
    absolute_cutoff = compute_absolute_cutoff(cutoff_index, previous_event)

    return {
        "event": SummarizationEvent(
            cutoff_index=absolute_cutoff,
            summary_message=summary_message,
            file_path=file_path,
        ),
        "summary_text": summary_text,
        "original_count": len(effective),
        "preserved_count": len(preserved) + 1,  # +1 for summary message
        "offloaded_arg_ids": offloaded_arg_ids,
        "offloaded_read_ids": offloaded_read_ids,
    }


async def offload_tool_args(
    messages: list[AnyMessage],
    backend: Any | None = None,
    already_offloaded: set[str] | None = None,
    summarization_config: SummarizationConfig | None = None,
) -> dict[str, Any]:
    """Offload large tool args and stale read results (Tier 1 only).

    Performs only the lightweight offload pass — no LLM summarization,
    no message eviction. Use this when the conversation is long enough for tool
    args to bloat the context but not yet large enough to warrant a full summary.

    Two sub-passes:
    - **Arg offload**: Truncate large Write/Edit/ExecuteCode arguments, persist
      originals to sandbox filesystem when backend is provided.
    - **Read offload**: Replace duplicate and non-critical Read results with
      short markers (files already exist in sandbox, agent can re-read).

    Args:
        messages: Current conversation messages.
        backend: Optional DaytonaBackend for offloading originals to sandbox.
        already_offloaded: IDs of tool calls already offloaded by the middleware,
            to skip re-offloading.

    Returns:
        Dict with:
        - "messages": The full message list with offloaded content (for aupdate_state)
        - "offloaded_args": Number of tool call args offloaded (Write/Edit/ExecuteCode)
        - "offloaded_reads": Number of Read results offloaded (duplicates + non-critical)
        - "original_count": Total message count (unchanged)
        - "new_offloaded_ids": Set of newly offloaded tool call IDs (args + reads)

    Raises:
        ValueError: If no messages are provided or nothing can be offloaded.
    """
    if not messages:
        raise ValueError("No messages to offload")

    # Ensure all messages have IDs
    for msg in messages:
        if msg.id is None:
            msg.id = str(uuid.uuid4())

    config = (summarization_config or SummarizationConfig()).model_dump()

    # Use truncation settings, applying reasonable defaults for manual trigger
    truncate_keep = int(config.get("truncate_args_keep_messages", 20))
    truncate_max_length = int(config.get("truncate_args_max_length", 2000))
    truncation_text = "...(argument truncated)"

    cutoff = max(0, len(messages) - truncate_keep)
    if cutoff == 0:
        raise ValueError(
            f"Not enough messages to offload. Have {len(messages)}, "
            f"need more than {truncate_keep} to have any candidates."
        )

    thread_dir = None
    if backend is not None:
        thread_dir = f".agent/threads/{get_thread_id()}"

    messages, truncated, originals = truncate_message_args(
        messages,
        cutoff,
        truncate_max_length,
        truncation_text,
        thread_dir,
    )

    # Also offload duplicate/non-critical Read results
    messages, read_truncated, read_ids = truncate_read_results(messages, cutoff)

    if not truncated and not read_truncated:
        raise ValueError("Nothing to offload at the current threshold")

    # Dedup: skip tool calls already offloaded by middleware
    if already_offloaded and originals:
        new_originals = {
            k: v for k, v in originals.items() if k not in already_offloaded
        }
        skipped = len(originals) - len(new_originals)
        if skipped:
            logger.info("[Offload] Skipped %d already-offloaded tool calls", skipped)
        originals = new_originals

    # Persist original args to backend before they're lost
    if originals and backend is not None:
        await aoffload_truncated_args(backend, originals)

    return {
        "messages": [RemoveMessage(id=REMOVE_ALL_MESSAGES), *messages],
        "offloaded_args": len(originals),
        "offloaded_reads": len(read_ids),
        "original_count": len(messages),
        "new_offloaded_ids": set(originals.keys()) | read_ids,
    }
