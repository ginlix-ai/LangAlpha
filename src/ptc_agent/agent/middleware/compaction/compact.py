"""Standalone functions for manual compaction and offloading triggers."""

import asyncio
import logging
from typing import Any

from langchain_core.messages import AnyMessage

from langchain.chat_models import BaseChatModel

from src.config.settings import get_compaction_timeout
from src.llms.content_utils import format_llm_content
from ptc_agent.config.agent import CompactionConfig
from src.llms import get_llm_by_type

from ptc_agent.agent.state import ensure_message_ids
from ptc_agent.agent.middleware.compaction.types import CompactionEvent
from ptc_agent.agent.middleware.compaction.summary_request import (
    DEFAULT_SUMMARY_PROMPT,
    build_summary_request,
    trim_for_summary,
)
from ptc_agent.agent.middleware.compaction.utils import (
    build_summary_event,
    count_tokens_tiktoken,
    find_group_safe_cutoff,
    get_effective_messages,
    partition_at_cutoff,
)
from ptc_agent.agent.middleware.compaction.model import (
    summary_trim_budget,
)
from src.llms import maybe_disable_streaming
from ptc_agent.agent.middleware.compaction.offloading import (
    aoffload_base64_content,
    select_offloads,
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
    workspace_id: str | None = None,
) -> dict[str, Any]:
    """Summarize all but the last ``keep_messages`` effective messages.

    Produces a ``CompactionEvent`` that the middleware can use to
    reconstruct the effective message list on subsequent model calls,
    without destructively replacing checkpoint messages. Tier 1 has no part
    here: what it would trim is in the stretch the summary replaces.

    Args:
        messages: List of conversation messages to compact (full state).
        keep_messages: Number of recent messages to preserve (default: 5).
        model_name: LLM model name for generating summaries (default: gpt-5-nano).
        backend: Optional SandboxBackend, for attachments and the transcript.
        previous_event: Previous CompactionEvent for chained compactions.
        compaction_config: Optional CompactionConfig override.
        llm_client: Pre-built OAuth/BYOK client. When None, built from model_name.
        thread_id: The thread being compacted. This runs outside the graph, so
            without it the transcript pointer has no thread.
        workspace_id: The thread's workspace, whose folder must reach the
            transcript before the summary points at it.

    Returns:
        Dict with:
        - "event": CompactionEvent to write to state (under preserved
          ``_summarization_event`` key)
        - "summary_text": The generated summary text
        - "original_count": Number of effective messages before compaction
        - "preserved_count": Number of preserved messages + summary message

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
    config = (compaction_config or CompactionConfig()).model_dump()

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

    # ---- Tier 2: Summarize, citing the transcript only once it is readable ----
    transcript = await aexport_transcript(
        backend,
        transcript_target(backend, thread_id),
        messages,
        workspace_id=workspace_id,
    )
    to_summarize = messages_to_summarize

    if llm_client is not None:
        compaction_model: BaseChatModel = llm_client
    else:
        compaction_model = get_llm_by_type(model_name)
    maybe_disable_streaming(compaction_model)

    token_threshold = config.get("token_threshold", 120000)
    messages_to_summarize = trim_for_summary(
        messages_to_summarize,
        summary_trim_budget(compaction_model, token_threshold),
        count_tokens_tiktoken,
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
        response = await asyncio.wait_for(
            compaction_model.ainvoke(
                build_summary_request(
                    DEFAULT_SUMMARY_PROMPT,
                    request_messages,
                    TranscriptTurns.of(transcript, messages) if transcript else None,
                )
            ),
            timeout=get_compaction_timeout(),
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
    }


async def offload_tool_args(
    messages: list[AnyMessage],
    backend: Any | None = None,
    already_offloaded: set[str] | None = None,
    compaction_config: CompactionConfig | None = None,
    already_offloaded_reads: set[str] | None = None,
    thread_id: str | None = None,
    previous_event: CompactionEvent | None = None,
    workspace_id: str | None = None,
) -> dict[str, Any]:
    """Choose the large tool args and stale Read results to hide (Tier 1 only).

    Records which calls to hide and never rewrites checkpoint messages: the
    middleware re-applies every recorded id on each model call. ``messages``
    is the full checkpoint list, which the transcript is saved from; the
    cutoff is taken over the view after ``previous_event``, as the middleware
    takes it. An argument is hidden only once the transcript holding it is
    saved and ``workspace_id``'s folder can read it, since that is then its
    only copy the agent can read.

    Returns:
        Dict with "offloaded_arg_ids" and "offloaded_read_ids" (new ids
        only), and their counts as "offloaded_args" / "offloaded_reads".

    Raises:
        ValueError: If no messages are provided or nothing new can be offloaded.
        RuntimeError: If only args were due and their transcript did not save.
    """
    if not messages:
        raise ValueError("No messages to offload")

    ensure_message_ids(messages)
    effective = get_effective_messages(messages, previous_event)

    config = (compaction_config or CompactionConfig()).model_dump()
    truncate_keep = int(config.get("truncate_args_keep_messages", 20))
    truncate_max_length = int(config.get("truncate_args_max_length", 2000))

    cutoff = max(0, len(effective) - truncate_keep)
    if cutoff == 0:
        raise ValueError(
            f"Not enough messages to offload. Have {len(effective)}, "
            f"need more than {truncate_keep} to have any candidates."
        )

    arg_ids, read_ids = select_offloads(
        effective,
        cutoff,
        truncate_max_length,
        set(already_offloaded or ()),
        set(already_offloaded_reads or ()),
    )

    if not arg_ids and not read_ids:
        raise ValueError("Nothing to offload at the current threshold")

    if arg_ids:
        transcript = transcript_target(backend, thread_id)
        if transcript is None:
            arg_ids = set()
        elif (
            await aexport_transcript(
                backend, transcript, messages, workspace_id=workspace_id
            )
            is None
        ):
            if not read_ids:
                # A failure to retry, not "nothing to offload": the caller
                # turns this into a 500 and records nothing.
                raise RuntimeError(
                    "Could not save the transcript the arguments are kept in"
                )
            arg_ids = set()
        if not arg_ids and not read_ids:
            raise ValueError("Nothing to offload at the current threshold")

    return {
        "offloaded_arg_ids": arg_ids,
        "offloaded_read_ids": read_ids,
        "offloaded_args": len(arg_ids),
        "offloaded_reads": len(read_ids),
    }
