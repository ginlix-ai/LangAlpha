"""Standalone functions for manual compaction and offloading triggers."""

import asyncio
import logging
from typing import Any

from langchain_core.messages import AnyMessage

from langchain.chat_models import BaseChatModel

from src.config.settings import get_compaction_timeout
from src.llms.content_utils import format_llm_content
from ptc_agent.config.agent import CompactionConfig
from ptc_agent.core.paths import WorkspaceLayout
from src.llms import get_llm_by_type

from ptc_agent.agent.state import ensure_message_ids
from ptc_agent.agent.middleware.compaction.types import CompactionEvent
from ptc_agent.agent.middleware.compaction.summary_request import (
    DEFAULT_SUMMARY_PROMPT,
    build_summary_request,
)
from ptc_agent.agent.middleware.compaction.utils import (
    build_summary_event,
    find_group_safe_cutoff,
    get_effective_messages,
    partition_at_cutoff,
    truncate_message_args,
    truncate_read_results,
)
from ptc_agent.agent.middleware.compaction.model import (
    summary_trim_budget,
    trim_for_summary,
)
from src.llms import maybe_disable_streaming
from ptc_agent.agent.middleware.compaction.offloading import (
    aoffload_base64_content,
    aoffload_truncated_args,
    apply_recorded_offloads,
    get_thread_id,
)
from ptc_agent.agent.transcript.pointer import (
    TranscriptTurns,
    aexport_transcript,
    transcript_target,
)

logger = logging.getLogger(__name__)


async def compact_messages(
    messages: list[AnyMessage],
    keep_messages: int = 5,
    model_name: str = "",
    backend: Any | None = None,
    previous_event: CompactionEvent | None = None,
    compaction_config: CompactionConfig | None = None,
    llm_client: BaseChatModel | None = None,
    thread_id: str | None = None,
) -> dict[str, Any]:
    """
    Compact conversation messages with two-tier context management.

    Produces a ``CompactionEvent`` that the middleware can use to
    reconstruct the effective message list on subsequent model calls,
    without destructively replacing checkpoint messages.

    Two-tier offloading (when backend is provided):
    - Tier 1: Truncate large tool args in old messages, offload originals to sandbox
    - Tier 2: Summarize evicted messages; the summary points at the thread's
      transcript, which is brought up to date alongside

    When backend is None, offloading is skipped but truncation and summarization
    still occur.

    Args:
        messages: List of conversation messages to compact (full state).
        keep_messages: Number of recent messages to preserve (default: 5).
        model_name: LLM model name for generating summaries (default: gpt-5-nano).
        backend: Optional SandboxBackend for offloading to sandbox filesystem.
        previous_event: Previous CompactionEvent for chained compactions.
        compaction_config: Optional CompactionConfig override.
        llm_client: Pre-built OAuth/BYOK client. When None, built from model_name.
        thread_id: The thread being compacted. This runs outside the graph, so
            without it offloads and the transcript pointer have no thread dir.

    Returns:
        Dict with:
        - "event": CompactionEvent to write to state (under preserved
          ``_summarization_event`` key)
        - "summary_text": The generated summary text
        - "original_count": Number of effective messages before compaction
        - "preserved_count": Number of preserved messages + summary message
        - "offloaded_arg_ids": Set of tool call IDs whose args were truncated/offloaded
        - "offloaded_read_ids": Set of tool call IDs whose Read results were truncated

    Example:
        result = await compact_messages(messages, previous_event=prev_event)
        await graph.aupdate_state(config, {"_summarization_event": result["event"]})
    """
    if not messages:
        raise ValueError("No messages to compact")

    # Ensure all messages have IDs
    ensure_message_ids(messages)

    # Reconstruct effective messages from previous event
    effective = get_effective_messages(messages, previous_event)

    # ---- Tier 1: Truncate large tool args + stale Read results in old messages ----
    config = (compaction_config or CompactionConfig()).model_dump()
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
            thread_dir = WorkspaceLayout.thread_subdir(get_thread_id(thread_id))

        effective, truncated, originals = truncate_message_args(
            effective,
            cutoff,
            truncate_max_length,
            truncation_text,
            thread_dir,
        )

        # Offload original args before they're lost
        if truncated and originals and backend is not None:
            offloaded_arg_ids = await aoffload_truncated_args(
                backend, originals, thread_id=thread_id
            )

        # Truncate duplicate/non-critical Read results (same cutoff)
        effective, _read_truncated, offloaded_read_ids = truncate_read_results(
            effective, cutoff
        )

    # ---- Determine cutoff for summarization ----
    if len(effective) <= keep_messages:
        raise ValueError(
            f"Not enough messages to compact. Have {len(effective)}, "
            f"need more than {keep_messages} to preserve."
        )

    cutoff_index = find_group_safe_cutoff(effective, len(effective) - keep_messages)

    if cutoff_index <= 0:
        raise ValueError("Cannot determine valid cutoff point for compaction")

    messages_to_summarize, preserved = partition_at_cutoff(effective, cutoff_index)

    # ---- Tier 2: Summarize, pointing at the transcript once its save lands ----
    transcript = transcript_target(backend, thread_id)
    to_summarize = messages_to_summarize

    if llm_client is not None:
        compaction_model: BaseChatModel = llm_client
    else:
        compaction_model = get_llm_by_type(model_name)
    maybe_disable_streaming(compaction_model)

    token_threshold = config.get("token_threshold", 120000)
    messages_to_summarize = trim_for_summary(
        messages_to_summarize, summary_trim_budget(compaction_model, token_threshold)
    )
    if not messages_to_summarize:
        raise RuntimeError(
            "Nothing since the last summary fits the compaction model's budget"
        )

    # Strip base64 blobs before sending to LLM
    request_messages = await aoffload_base64_content(
        backend, messages_to_summarize, thread_id=thread_id
    )

    # Manual /compact MUST fail loudly on LLM error. Swallowing the exception
    # and fabricating a fake summary would corrupt state (a "compacted" cutoff
    # with garbage summary text) while reporting HTTP 200 to the client.
    # trigger_compaction's outer except converts the raise into HTTP 500.
    #
    # The call carries its own wall-clock budget: a hung summarize raises
    # TimeoutError here (rather than blocking the thread forever), which the
    # except below re-raises -> HTTP 500. The timeout lives on the call, not on
    # a flat admission-side 409 clock.
    try:
        response, transcript = await asyncio.gather(
            asyncio.wait_for(
                compaction_model.ainvoke(
                    build_summary_request(
                        DEFAULT_SUMMARY_PROMPT,
                        request_messages,
                        TranscriptTurns.of(transcript, messages) if transcript else None,
                    )
                ),
                timeout=get_compaction_timeout(),
            ),
            aexport_transcript(backend, transcript, messages),
        )
    except Exception as e:
        logger.error(f"[Compaction] manual compact LLM call failed: {e}")
        raise

    content = response.content if hasattr(response, "content") else response
    additional_kwargs = getattr(response, "additional_kwargs", None)
    formatted = format_llm_content(content, additional_kwargs)
    summary_text = formatted.get("text", "").strip()

    if not summary_text:
        raise RuntimeError("Compaction LLM returned empty summary")

    # Build the event with an id anchor (cutoff grounded in the raw list)
    event = build_summary_event(
        summary_text,
        transcript,
        raw_messages=messages,
        to_summarize=to_summarize,
        summarized=messages_to_summarize,
        preserved_messages=preserved,
        original_message_count=len(effective),
        skill_files=backend is not None,
    )

    return {
        "event": event,
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
    compaction_config: CompactionConfig | None = None,
    already_offloaded_reads: set[str] | None = None,
    thread_id: str | None = None,
) -> dict[str, Any]:
    """Offload large tool args and stale read results (Tier 1 only).

    Records which calls to offload and never rewrites checkpoint messages: the
    middleware re-applies every recorded id on each model call, so the
    checkpoint keeps full args and a write here cannot race a live turn's
    appends. ``messages`` is the effective list (after any summarization), so
    the cutoff and batch count match what the middleware sees.

    Returns:
        Dict with "offloaded_arg_ids" and "offloaded_read_ids" (new ids only,
        without args whose write failed), and their counts as
        "offloaded_args" / "offloaded_reads".

    Raises:
        ValueError: If no messages are provided or nothing new can be offloaded.
        RuntimeError: If every arg write failed and no Read result was left.
    """
    if not messages:
        raise ValueError("No messages to offload")

    ensure_message_ids(messages)

    config = (compaction_config or CompactionConfig()).model_dump()
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
        thread_dir = WorkspaceLayout.thread_subdir(get_thread_id(thread_id))

    # Start from the view the model already has, so only new offloads count.
    messages = apply_recorded_offloads(
        messages,
        already_offloaded or set(),
        already_offloaded_reads or set(),
        truncate_max_length,
        truncation_text,
        thread_dir,
    )
    messages, _, originals = truncate_message_args(
        messages, cutoff, truncate_max_length, truncation_text, thread_dir
    )
    _, _, read_ids = truncate_read_results(messages, cutoff)

    if not originals and not read_ids:
        raise ValueError("Nothing to offload at the current threshold")

    # Persist original args before the view hides them
    arg_ids = await aoffload_truncated_args(backend, originals, thread_id=thread_id)
    if originals and not arg_ids and not read_ids:
        # A failure to retry, not "nothing to offload": the caller turns this
        # into a 500 and records nothing.
        raise RuntimeError("Could not save any tool arguments to the sandbox")

    return {
        "offloaded_arg_ids": arg_ids,
        "offloaded_read_ids": read_ids,
        "offloaded_args": len(arg_ids),
        "offloaded_reads": len(read_ids),
    }
