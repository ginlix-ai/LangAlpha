"""Token counting, prompt template, and truncation utilities for compaction."""

import logging
import re
import uuid
from collections.abc import Iterable
from typing import Any

import tiktoken

from langchain_core.messages import (
    AIMessage,
    AnyMessage,
    MessageLikeRepresentation,
    ToolMessage,
)
from langchain_core.messages.human import HumanMessage
from langchain_core.messages.utils import convert_to_messages

from ptc_agent.agent.middleware.compaction.types import (
    CONTEXT_SUMMARY_PREFIX,
    NON_CRITICAL_READ_PREFIXES,
    CompactionEvent,
    TRUNCATABLE_TOOLS,
)

logger = logging.getLogger(__name__)


# =============================================================================
# Token counting
# =============================================================================

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

            # Image blocks (various formats) — short placeholder for counting
            elif block_type == "image_url":
                texts.append("[image]")
            elif block_type == "image":
                texts.append("[image]")

            # File block (PDF uploads etc.)
            elif block_type == "file":
                fname = block.get("filename", "file")
                texts.append(f"[file: {fname}]")

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
        reasoning = additional_kwargs.get("reasoning_content") or additional_kwargs.get(
            "reasoning"
        )
        if reasoning:
            reasoning_text = (
                _extract_text_from_content(reasoning)
                if isinstance(reasoning, list)
                else str(reasoning)
            )
            text = f"{text} {reasoning_text}" if text else reasoning_text

        total += len(enc.encode(text)) + 3  # +3 for role/message overhead
    return total


# =============================================================================
# Base64 stripping (sync — no backend needed)
# =============================================================================

# Regex for data URIs embedded in plain text / strings
_DATA_URI_RE = re.compile(
    r"data:[a-zA-Z0-9_.+-]+/[a-zA-Z0-9_.+-]+;base64,[A-Za-z0-9+/=]{100,}"
)


def strip_base64_from_content(content: str | list) -> str | list:
    """Replace base64 content blocks with lightweight text placeholders.

    Handles all three provider-specific block formats:
    - ``image_url`` with ``data:...;base64,...`` URL (OpenAI style)
    - ``file`` with ``base64`` key (PDF uploads)
    - ``image`` with base64 source (Anthropic native)

    Also cleans data URIs embedded in plain text strings.

    Returns the *original* object when nothing changed (identity check).
    """
    if isinstance(content, str):
        if _DATA_URI_RE.search(content):
            return _DATA_URI_RE.sub("[base64 data removed]", content)
        return content

    if not isinstance(content, list):
        return content

    new_blocks: list = []
    changed = False

    for block in content:
        if isinstance(block, str):
            if _DATA_URI_RE.search(block):
                new_blocks.append(_DATA_URI_RE.sub("[base64 data removed]", block))
                changed = True
            else:
                new_blocks.append(block)
            continue

        if not isinstance(block, dict):
            new_blocks.append(block)
            continue

        block_type = block.get("type", "")

        # OpenAI-style image_url with data URI
        if block_type == "image_url":
            url = (block.get("image_url") or {}).get("url", "")
            if url.startswith("data:") and ";base64," in url:
                new_blocks.append({"type": "text", "text": "[Image]"})
                changed = True
                continue

        # PDF / file upload with inline base64
        elif block_type == "file" and "base64" in block:
            fname = block.get("filename", "file")
            new_blocks.append({"type": "text", "text": f"[PDF: {fname}]"})
            changed = True
            continue

        # Anthropic native image block
        elif block_type == "image":
            source = block.get("source") or {}
            if source.get("type") == "base64":
                new_blocks.append({"type": "text", "text": "[Image]"})
                changed = True
                continue

        # Text block with embedded data URIs
        elif block_type == "text":
            text = block.get("text", "")
            if isinstance(text, str) and _DATA_URI_RE.search(text):
                new_blocks.append(
                    {
                        "type": "text",
                        "text": _DATA_URI_RE.sub("[base64 data removed]", text),
                    }
                )
                changed = True
                continue

        new_blocks.append(block)

    return new_blocks if changed else content


def strip_base64_from_messages(messages: list[AnyMessage]) -> list[AnyMessage]:
    """Strip base64 content from messages, only copying those that changed."""
    result: list[AnyMessage] = []
    changed = False

    for msg in messages:
        new_content = strip_base64_from_content(msg.content)
        if new_content is not msg.content:
            copy = msg.model_copy()
            copy.content = new_content
            result.append(copy)
            changed = True
        else:
            result.append(msg)

    return result if changed else messages


# =============================================================================
# Truncation utilities
# =============================================================================


def truncate_tool_call(
    tool_call: dict[str, Any],
    max_length: int,
    truncation_text: str,
    thread_dir: str | None = None,
) -> dict[str, Any]:
    """Truncate large arguments in a single tool call.

    Only clips individual string args exceeding max_length, preserving arg structure.

    Args:
        tool_call: The tool call dictionary to truncate.
        max_length: Maximum character length for tool arguments before truncation.
        truncation_text: Fallback text when no thread_dir is available.
        thread_dir: If provided, the truncation marker includes the path where
            the original content is saved, so the agent can retrieve it.

    Returns:
        A copy of the tool call with large arguments truncated, or the
        original if no modifications were needed.
    """
    args = tool_call.get("args", {})

    # Build the marker text — include file path when backend offloading is active
    if thread_dir is not None:
        tool_call_id = tool_call.get("id", "unknown")
        path = f"{thread_dir}/truncated_args_{tool_call_id}.md"
        marker = f"... [this tool call's arguments were offloaded to {path} — use Read to access when needed]"
    else:
        marker = truncation_text

    truncated_args = {}
    modified = False

    for key, value in args.items():
        if isinstance(value, str) and len(value) > max_length:
            truncated_args[key] = value[:20] + marker
            modified = True
        else:
            truncated_args[key] = value

    if modified:
        return {**tool_call, "args": truncated_args}
    return tool_call


def truncate_message_args(
    messages: list[AnyMessage],
    cutoff_index: int,
    max_length: int,
    truncation_text: str,
    thread_dir: str | None = None,
) -> tuple[list[AnyMessage], bool, dict[str, dict[str, Any]]]:
    """Truncate large tool call arguments in old messages.

    Only processes messages before the cutoff index. Only modifies AIMessages
    with tool calls to truncatable tools (Write, Edit, ExecuteCode).

    Args:
        messages: Effective messages to potentially truncate.
        cutoff_index: Messages at index >= cutoff are protected from truncation.
        max_length: Maximum character length for tool arguments before truncation.
        truncation_text: Fallback text when no thread_dir is available.
        thread_dir: If provided, truncation markers include the path where
            the original content is saved.

    Returns:
        Tuple of (messages, modified, originals). If modified is False,
        messages is the same list object as input. originals maps
        tool_call_id -> {"name": str, "args": dict} for calls that were
        truncated, so callers can offload the original content.
    """
    if cutoff_index >= len(messages):
        return messages, False, {}

    logger.debug(
        "Truncating tool args in messages before index %d (of %d total)",
        cutoff_index,
        len(messages),
    )

    truncated_messages: list[AnyMessage] = []
    modified = False
    originals: dict[str, dict[str, Any]] = {}

    for i, msg in enumerate(messages):
        if i < cutoff_index and isinstance(msg, AIMessage) and msg.tool_calls:
            truncated_tool_calls = []
            msg_modified = False

            for tool_call in msg.tool_calls:
                if tool_call["name"] in TRUNCATABLE_TOOLS:
                    truncated_call = truncate_tool_call(
                        tool_call, max_length, truncation_text, thread_dir
                    )
                    if truncated_call is not tool_call:
                        msg_modified = True
                        originals[tool_call["id"]] = {
                            "name": tool_call["name"],
                            "args": tool_call["args"],
                        }
                    truncated_tool_calls.append(truncated_call)
                else:
                    truncated_tool_calls.append(tool_call)

            if msg_modified:
                truncated_msg = msg.model_copy()
                truncated_msg.tool_calls = truncated_tool_calls
                truncated_messages.append(truncated_msg)
                modified = True
            else:
                truncated_messages.append(msg)
        else:
            truncated_messages.append(msg)

    if modified:
        logger.debug(
            "Tool arg truncation applied to messages before index %d (%d tool calls)",
            cutoff_index,
            len(originals),
        )

    return truncated_messages, modified, originals


# =============================================================================
# Read result truncation
# =============================================================================


def truncate_read_results(
    messages: list[AnyMessage],
    cutoff_index: int,
) -> tuple[list[AnyMessage], bool, set[str]]:
    """Truncate duplicate and non-critical Read tool results in old messages.

    Complements truncate_message_args (which handles AIMessage args) by targeting
    ToolMessage content for Read tool calls. Two patterns are handled:

    1. **Duplicate reads**: Same file read multiple times with identical
       (file_path, offset, limit) — earlier results are superseded.
    2. **Non-critical reads**: Reads of paths matching NON_CRITICAL_READ_PREFIXES
       (e.g. .agents/threads/) — content already processed by the agent.

    Only messages before cutoff_index are eligible for truncation.

    Args:
        messages: Effective messages to potentially truncate.
        cutoff_index: Messages at index >= cutoff are protected from truncation.

    Returns:
        Tuple of (messages, modified, offloaded_tool_call_ids).
        If modified is False, messages is the same list object as input.
        offloaded_tool_call_ids contains the tool_call_id of every truncated ToolMessage.
    """
    if cutoff_index >= len(messages):
        return messages, False, set()

    # --- Pass 1: Build tool_call_id → Read args index from AIMessages ---
    read_args_by_id: dict[str, dict[str, Any]] = {}
    for msg in messages:
        if isinstance(msg, AIMessage) and msg.tool_calls:
            for tc in msg.tool_calls:
                if tc["name"] == "Read":
                    read_args_by_id[tc["id"]] = tc.get("args", {})

    if not read_args_by_id:
        return messages, False, set()

    # --- Pass 2: Group ToolMessages by read signature, track latest index ---
    # signature key → list of (msg_index, tool_call_id)
    sig_groups: dict[tuple, list[tuple[int, str]]] = {}
    for i, msg in enumerate(messages):
        if not isinstance(msg, ToolMessage):
            continue
        tc_id = msg.tool_call_id
        if tc_id not in read_args_by_id:
            continue
        args = read_args_by_id[tc_id]
        sig = (
            args.get("file_path", ""),
            args.get("offset"),
            args.get("limit"),
        )
        sig_groups.setdefault(sig, []).append((i, tc_id))

    if not sig_groups:
        return messages, False, set()

    # Find the latest msg_index per signature
    latest_per_sig: dict[tuple, int] = {}
    for sig, entries in sig_groups.items():
        latest_per_sig[sig] = max(idx for idx, _ in entries)

    # --- Pass 3: Determine which ToolMessages to truncate ---
    ids_to_truncate: dict[str, str] = {}  # tool_call_id → replacement content

    for sig, entries in sig_groups.items():
        file_path = sig[0]
        latest_idx = latest_per_sig[sig]
        is_non_critical = any(
            file_path.startswith(prefix) for prefix in NON_CRITICAL_READ_PREFIXES
        )

        for msg_idx, tc_id in entries:
            if msg_idx >= cutoff_index:
                continue  # Protected — don't touch

            is_duplicate = len(entries) > 1 and msg_idx != latest_idx

            # Compute the marker we'd insert
            marker: str | None = None
            if is_duplicate or is_non_critical:
                marker = f"... [this tool call's read result was offloaded from {file_path} — use Read to access when needed]"

            # Skip if content already equals the marker (idempotent)
            if marker is not None and messages[msg_idx].content != marker:
                ids_to_truncate[tc_id] = marker

    if not ids_to_truncate:
        return messages, False, set()

    # --- Pass 4: Build new message list with replacements ---
    new_messages: list[AnyMessage] = []
    for msg in messages:
        if isinstance(msg, ToolMessage) and msg.tool_call_id in ids_to_truncate:
            replaced = msg.model_copy()
            replaced.content = ids_to_truncate[msg.tool_call_id]
            new_messages.append(replaced)
        else:
            new_messages.append(msg)

    offloaded_ids = set(ids_to_truncate.keys())
    logger.debug(
        "Read result truncation applied before index %d (%d results truncated)",
        cutoff_index,
        len(offloaded_ids),
    )

    return new_messages, True, offloaded_ids


# =============================================================================
# Shared compaction helpers (used by both middleware and manual triggers)
# =============================================================================


def get_effective_messages(
    messages: list[AnyMessage],
    event: CompactionEvent | None,
) -> list[AnyMessage]:
    """Reconstruct the effective message list from a previous compaction event.

    After compaction, the checkpoint still contains ALL messages. This function
    reconstructs what the model should see: the summary message plus messages
    after the cutoff index.

    Args:
        messages: Full message list from state.
        event: Previous compaction event, or None if no compaction occurred.

    Returns:
        Effective message list for the model.
    """
    if event is None:
        return messages

    result: list[AnyMessage] = [event["summary_message"]]
    result.extend(messages[event["cutoff_index"]:])
    return result


def compute_absolute_cutoff(
    effective_cutoff: int,
    previous_event: CompactionEvent | None,
) -> int:
    """Convert effective message cutoff to absolute state index for chaining.

    When chained compaction occurs, the effective message list starts with
    the previous summary message at index 0. The -1 accounts for this.

    Args:
        effective_cutoff: Cutoff index in the effective message list.
        previous_event: Previous compaction event, or None.

    Returns:
        Absolute cutoff index in the state message list.
    """
    if previous_event is not None:
        return previous_event["cutoff_index"] + effective_cutoff - 1
    return effective_cutoff


def build_summary_message(
    summary: str, file_path: str | None = None
) -> HumanMessage:
    """Build the summary HumanMessage with optional file path reference.

    Tags with lc_source='summarization' for chain filtering.

    Args:
        summary: The generated summary text.
        file_path: Path where conversation history was stored, or None.

    Returns:
        HumanMessage containing the summary.
    """
    if file_path is not None:
        content = (
            f"{CONTEXT_SUMMARY_PREFIX}{summary}\n\n"
            f"Full conversation history saved to `{file_path}`."
        )
    else:
        content = f"{CONTEXT_SUMMARY_PREFIX}{summary}"

    return HumanMessage(
        content=content,
        id=str(uuid.uuid4()),
        additional_kwargs={"lc_source": "summarization"},
    )


# =============================================================================
# Prompt template
# =============================================================================

# Financial research summarization prompt. Instructions only — the conversation
# history is delivered in a separate HumanMessage so the system channel stays
# bounded and cacheable, and so BaseChatModel.format() doesn't try to interpret
# message content as further format placeholders.
DEFAULT_SUMMARY_PROMPT = """<role>
Financial Research Context Summarizer
</role>

<context>
You're nearing your input token limit. The conversation history in the user
message will be replaced with the context you extract. This is critical -
ensure you capture all important information so you can continue the research
without losing progress.
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
</output_format>"""
