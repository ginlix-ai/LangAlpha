"""Token counting, prompt template, and truncation utilities for compaction."""

from __future__ import annotations

import json
import re
import uuid
from collections.abc import Iterable, Mapping, Sequence
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

from ptc_agent.agent.middleware._message_utils import (
    is_tool_message,
    message_field,
    message_id,
)
from ptc_agent.agent.middleware.skills.content import skill_bodies
from ptc_agent.agent.transcript import TranscriptTarget
from ptc_agent.agent.transcript.classify import is_summary_message
from ptc_agent.agent.transcript.pointer import SummarySpan, transcript_note
from src.llms.attachment_payload import FILE_BLOCK_TYPES, IMAGE_BLOCK_TYPES
from ptc_agent.agent.middleware.compaction.types import (
    CONTEXT_SUMMARY_PREFIX,
    NON_CRITICAL_READ_PREFIXES,
    CompactionEvent,
    TRUNCATABLE_TOOLS,
)


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

            # Attachments — a short placeholder is what they cost to count.
            elif block_type in IMAGE_BLOCK_TYPES:
                texts.append("[image]")

            elif block_type in FILE_BLOCK_TYPES:
                fname = block.get("filename", "file")
                texts.append(f"[file: {fname}]")

    return " ".join(texts)


def count_tokens_tiktoken(messages: Iterable[MessageLikeRepresentation]) -> int:
    """Count tokens using tiktoken (accurate for all languages including CJK)."""
    enc = _get_tiktoken_encoder()
    total = 0
    for msg in convert_to_messages(messages):
        tool_calls = getattr(msg, "tool_calls", None)
        content = msg.content
        if tool_calls and isinstance(content, list):
            # Anthropic keeps each call as a tool_use block too; the provider
            # is sent tool_calls, which a Tier 1 cut rewrites, so count those.
            ids = {tc.get("id") for tc in tool_calls}
            content = [
                b
                for b in content
                if not (isinstance(b, dict) and b.get("type") == "tool_use" and b.get("id") in ids)
            ]
        text = _extract_text_from_content(content)

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

        # Sent with the message, and on a code-heavy turn most of its size.
        if tool_calls:
            calls = json.dumps(
                [{"name": tc.get("name"), "args": tc.get("args")} for tc in tool_calls],
                ensure_ascii=False,
                default=str,
            )
            text = f"{text} {calls}" if text else calls

        total += len(enc.encode(text)) + 3  # +3 for role/message overhead
    return total


# =============================================================================
# Base64 stripping (sync — no backend needed)
# =============================================================================

# Regex for data URIs embedded in plain text / strings
_DATA_URI_RE = re.compile(
    r"data:[a-zA-Z0-9_.+-]+/[a-zA-Z0-9_.+-]+;base64,[A-Za-z0-9+/=]{100,}"
)


def _carries_base64(block: dict) -> bool:
    """True if an attachment block holds its bytes inline.

    Two shapes, one question: the langchain v1 ``base64`` key, and the
    Anthropic-native ``source`` object. Asked in one place because the answer
    used to be spelled differently for images and for files.
    """
    if "base64" in block:
        return True
    source = block.get("source") or {}
    return isinstance(source, dict) and source.get("type") == "base64"


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

        # PDF upload with inline base64, under either name for the block.
        elif block_type in FILE_BLOCK_TYPES and _carries_base64(block):
            fname = block.get("filename", "file")
            new_blocks.append({"type": "text", "text": f"[PDF: {fname}]"})
            changed = True
            continue

        # Anthropic native image block
        elif block_type == "image":
            if _carries_base64(block):
                new_blocks.append({"type": "text", "text": "[Image]"})
                changed = True
                continue

        # Text block with embedded data URIs
        elif block_type == "text":
            text = block.get("text", "")
            if isinstance(text, str) and _DATA_URI_RE.search(text):
                # Rebuild from the block so sibling keys (annotations, phase,
                # index) survive the redaction.
                new_blocks.append(
                    {**block, "text": _DATA_URI_RE.sub("[base64 data removed]", text)}
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
    tool_call: dict[str, Any], max_length: int, marker: str
) -> dict[str, Any]:
    """A copy of ``tool_call`` with each string arg over ``max_length`` cut
    to its head plus ``marker``, or the call itself when none is that long."""
    args = tool_call.get("args", {})
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


def oversized_arg_calls(
    messages: list[AnyMessage], cutoff_index: int, max_length: int
) -> set[str]:
    """Ids of the Write, Edit and ExecuteCode calls before ``cutoff_index``
    that pass a string longer than ``max_length``."""
    return {
        call["id"]
        for message in messages[:cutoff_index]
        if isinstance(message, AIMessage)
        for call in message.tool_calls or ()
        if call["name"] in TRUNCATABLE_TOOLS
        and call.get("id")
        and any(
            isinstance(value, str) and len(value) > max_length
            for value in (call.get("args") or {}).values()
        )
    }


# =============================================================================
# Stale Read results
# =============================================================================


def stale_read_ids(messages: list[AnyMessage], cutoff_index: int) -> set[str]:
    """Ids of the Read results before ``cutoff_index`` the agent no longer
    needs in full: one a later Read of the same file, offset and limit
    supersedes, and one of a file under ``NON_CRITICAL_READ_PREFIXES``, whose
    content the agent has already processed."""
    read_args = {
        call["id"]: call.get("args", {})
        for message in messages
        if isinstance(message, AIMessage)
        for call in message.tool_calls or ()
        if call["name"] == "Read"
    }
    reads: dict[tuple, list[tuple[int, str]]] = {}
    for i, message in enumerate(messages):
        if isinstance(message, ToolMessage) and message.tool_call_id in read_args:
            args = read_args[message.tool_call_id]
            # Args as the model wrote them, not as Read accepted them: a null
            # path or a list offset must not fail the turn start this runs in.
            key = (
                str(args.get("file_path") or ""),
                repr(args.get("offset")),
                repr(args.get("limit")),
            )
            reads.setdefault(key, []).append((i, message.tool_call_id))

    stale: set[str] = set()
    for (file_path, _, _), entries in reads.items():
        non_critical = file_path.startswith(NON_CRITICAL_READ_PREFIXES)
        latest = entries[-1][0]
        for i, call_id in entries:
            if (
                i < cutoff_index
                and (non_critical or i != latest)
                and messages[i].content != read_offload_marker(file_path)
            ):
                stale.add(call_id)
    return stale


def read_offload_marker(file_path: str) -> str:
    return f"... [this tool call's read result was offloaded from {file_path} — use Read to access when needed]"


# =============================================================================
# Shared compaction helpers (used by both middleware and manual triggers)
# =============================================================================



def _tool_result_call_id(message: Any) -> str | None:
    """The call a tool result answers, in either typed or dict shape.

    Only ``tool_call_id``. A dict's ``id`` is the *message* id (see
    ``_message_utils.message_id``), so reading it here matched a result against
    the wrong namespace and dropped an id-less result the caller promises to
    keep.
    """
    if isinstance(message, dict):
        call_id = message.get("tool_call_id")
        return call_id if isinstance(call_id, str) else None
    call_id = getattr(message, "tool_call_id", None)
    return call_id if isinstance(call_id, str) else None



def declared_tool_call_ids(message: Any) -> set[str]:
    """Every tool call an assistant turn is on the hook for an answer to.

    Three shapes have to be read, not one. ``tool_calls`` is the parsed list;
    ``invalid_tool_calls`` holds calls with unparseable arguments, which
    ``PatchToolCallsMiddleware`` still answers with a ToolMessage, so a
    predicate that skips them deletes legitimate failure records; and the
    provider-native blocks (Responses ``function_call``, Anthropic
    ``tool_use``) are what survives on a message whose parsed lists were never
    populated.
    """
    ids: set[str] = set()

    for attr in ("tool_calls", "invalid_tool_calls"):
        for call in message_field(message, attr) or []:
            call_id = (
                call.get("id") if isinstance(call, dict) else getattr(call, "id", None)
            )
            if isinstance(call_id, str) and call_id:
                ids.add(call_id)

    content = message_field(message, "content")
    if isinstance(content, list):
        for block in content:
            if not isinstance(block, dict):
                continue
            block_type = block.get("type")
            if block_type == "function_call":
                call_id = block.get("call_id") or block.get("id")
            elif block_type == "tool_use":
                call_id = block.get("id")
            else:
                continue
            if isinstance(call_id, str) and call_id:
                ids.add(call_id)

    return ids


def _result_owners(messages: list[AnyMessage]) -> dict[int, int | None]:
    """Which turn owns each tool result, keyed by the result's position.

    The one ownership rule, read by both the cut and the repair. They used to
    carry a rule each — nearest-assistant-turn for the cut, accumulate-as-you-go
    for the repair — and disagreed on an assistant message sitting between two
    parallel results: the cut called it clean and the repair then dropped a
    result, which landed in neither half and was lost.

    Ownership must *precede* the result rather than merely appear somewhere in
    the list, or a later turn that reuses an id would vouch for an orphan the
    cut had already stranded. A result with no id at all is absent from the map:
    it cannot be matched, and dropping it would lose content on a shape we do not
    recognise. ``None`` marks a result whose owner is nowhere in the list, which
    is what an orphan is.
    """
    last_declared: dict[str, int] = {}
    owners: dict[int, int | None] = {}

    for i, msg in enumerate(messages):
        if is_tool_message(msg):
            call_id = _tool_result_call_id(msg)
            if call_id is not None:
                owners[i] = last_declared.get(call_id)
        else:
            for call_id in declared_tool_call_ids(msg):
                last_declared[call_id] = i

    return owners


def _orphan_indexes(tail: list[AnyMessage]) -> list[int]:
    """Positions in ``tail`` holding a tool result nothing before it asked for."""
    return [i for i, owner in _result_owners(tail).items() if owner is None]


def find_group_safe_cutoff(messages: list[AnyMessage], cutoff_index: int) -> int:
    """Move a cutoff off the inside of a tool-call group.

    A cut is clean exactly when the tail it leaves holds no orphan, so the search
    runs over ``_orphan_indexes`` rather than over a second idea of which
    assistant turn owns what.

    Advancing while the next message is a ToolMessage is not enough: a visual
    Read used to answer with a HumanMessage sitting among the tool results, so
    the walk stopped on the carrier and left the rest of the group orphaned
    behind a deleted parent. Those histories are still in checkpoints, so the
    boundary is found by ownership rather than by type.

    Snapping forward drops the whole partial group, which is also what relieves
    the context. The forward search stops short of the end of the list: an empty
    tail is trivially clean, and preserving nothing would cost the turn its most
    recent work, so the boundary moves back instead.

    A history can arrive already holding a result whose parent is nowhere in the
    list. No cut repairs that one, so a cut is judged clean when every orphan it
    leaves behind was already an orphan, not when the tail is orphan-free
    outright. Otherwise one inherited orphan makes every cutoff look unsafe and
    compaction gives up on a thread it could still relieve.

    That test is on identity, never on a count. A count lets a cut that strands
    a healthy result pass whenever the history carried an inherited orphan of
    its own: one orphan in, one orphan out, and the healthy result deleted by
    the strip that follows.
    """
    if cutoff_index <= 0 or cutoff_index >= len(messages):
        return cutoff_index

    # A cut strands a result exactly when the result survives it and its owner
    # does not, so the whole search reduces to one number per position: the
    # earliest owner among the results from there on. A cut is clean when that
    # owner is not behind it. An inherited orphan has no owner to leave behind
    # and so never counts against a candidate, which is the forgiveness the
    # docstring describes, and identity rather than a count falls out of it.
    # Re-deriving the orphans per candidate answered the same question, but
    # made the search quadratic in the length of the history.
    unowned = len(messages)
    owners = _result_owners(messages)
    earliest_owner = [unowned] * (len(messages) + 1)
    for i in range(len(messages) - 1, -1, -1):
        owner = owners.get(i)
        earliest_owner[i] = min(
            earliest_owner[i + 1], unowned if owner is None else owner
        )

    def adds_no_orphan(start: int) -> bool:
        return earliest_owner[start] >= start

    for i in range(cutoff_index, len(messages)):
        if adds_no_orphan(i):
            return i
    for i in range(cutoff_index - 1, -1, -1):
        if adds_no_orphan(i):
            return i
    return 0


def strip_orphan_tool_messages(tail: list[AnyMessage]) -> list[AnyMessage]:
    """Drop tool results in ``tail`` that no preceding message asked for.

    The crash backstop, and the one thing that heals a history already cut
    inside a group: a tool result whose parent was summarized away is rejected
    by every provider (Anthropic reads it as a ``tool_result`` opening a user
    turn, OpenAI as a ``function_call_output`` with no ``function_call``).

    A cutoff from ``find_group_safe_cutoff`` already leaves nothing to drop; the
    work here is for the checkpoints written before it existed.
    """
    orphans = set(_orphan_indexes(tail))
    if not orphans:
        return tail
    return [msg for i, msg in enumerate(tail) if i not in orphans]


def partition_at_cutoff(
    messages: list[AnyMessage], cutoff_index: int
) -> tuple[list[AnyMessage], list[AnyMessage]]:
    """Split a history at the cutoff, leaving the preserved side provider-legal.

    One place rather than three: every caller that mints a boundary needs the
    same slice and the same repair, and when they each wrote their own the strip
    drifted out of step with the cut that fed it.

    ``find_group_safe_cutoff`` already returns a cut with no orphan behind it, so
    the strip is the backstop for the paths that never asked it — a cutoff of 0,
    and checkpoints written before the group-aware cut existed.
    """
    return messages[:cutoff_index], strip_orphan_tool_messages(messages[cutoff_index:])


def _resolve_anchor_index(
    messages: list[AnyMessage], anchor_id: str | None
) -> int | None:
    """Index of the message whose id equals ``anchor_id``, or ``None``.

    Returns ``None`` immediately when ``anchor_id`` is ``None`` (legacy event)
    so it can never match an id-less message.
    """
    if anchor_id is None:
        return None
    for i, msg in enumerate(messages):
        if message_id(msg) == anchor_id:
            return i
    return None


def build_compaction_event(
    raw_messages: list[AnyMessage],
    preserved_messages: list[AnyMessage],
    summary_message: HumanMessage,
    file_path: str | None,
) -> CompactionEvent:
    """Construct a ``CompactionEvent`` carrying both a positional cutoff and an id anchor.

    The positional ``cutoff_index`` is grounded in ``raw_messages`` by locating
    the first preserved message's id. ``anchor_message_id`` lets reconstruction
    re-find the boundary by id if the raw list later drifts.

    An empty preserved tail has an exact answer — the end of the raw list — and
    it is used instead of arithmetic over the effective list. Chained arithmetic
    assumed the effective tail was a 1:1 suffix of the raw one, which orphan
    stripping is free to violate; when it did, reconstruction resurrected a
    message that had already been summarized.
    """
    anchor_message_id = (
        message_id(preserved_messages[0]) if preserved_messages else None
    )

    if not preserved_messages:
        cutoff_index = len(raw_messages)
    else:
        resolved = _resolve_anchor_index(raw_messages, anchor_message_id)
        # An unresolvable anchor means the tail is not the suffix of raw it is
        # supposed to be. Counting back from the end still lands on a boundary
        # that preserves the right number of messages, where the old chained
        # arithmetic drifted by however many the projection had removed.
        cutoff_index = (
            resolved
            if resolved is not None
            else max(0, len(raw_messages) - len(preserved_messages))
        )

    return CompactionEvent(
        cutoff_index=cutoff_index,
        summary_message=summary_message,
        file_path=file_path,
        anchor_message_id=anchor_message_id,
    )


def get_effective_messages(
    messages: list[AnyMessage],
    event: CompactionEvent | None,
) -> list[AnyMessage]:
    """Reconstruct the effective message list from a previous compaction event.

    After compaction, the checkpoint still contains ALL messages. This function
    reconstructs what the model should see: the summary message plus messages
    after the cutoff boundary.

    The boundary is tracked by ``anchor_message_id`` (the first preserved
    message's id) and re-resolved against the current list only when the stored
    positional ``cutoff_index`` no longer points at that anchor — so list
    perturbation (DeltaChannel reconstruction, injected messages) can't silently
    drift the boundary. Any orphaned tool result in the tail is stripped as a
    crash backstop, which is what heals a history cut inside a tool group by an
    earlier build.

    Args:
        messages: Full message list from state.
        event: Previous compaction event, or None if no compaction occurred.

    Returns:
        Effective message list for the model.
    """
    if event is None:
        return messages

    cutoff = resolve_cutoff_index(messages, event)
    tail = strip_orphan_tool_messages(messages[cutoff:])
    return [event["summary_message"], *tail]


def resolve_cutoff_index(messages: Sequence[AnyMessage], event: Mapping[str, Any]) -> int:
    """Where ``event``'s boundary falls in ``messages`` as they are now.

    The one reader of ``cutoff_index`` outside this module is the turn row,
    which scans the tail the model can still read. It has to agree with the
    slice the model gets, so both resolve the boundary here: the stored index
    is trusted only while it still points at the anchor, and re-found by id
    otherwise. Legacy events (no anchor) keep the positional index as-is.
    """
    raw = event.get("cutoff_index")
    cutoff = raw if isinstance(raw, int) and raw >= 0 else 0
    anchor_id = event.get("anchor_message_id")
    # O(1) happy path: only re-scan when the positional cutoff has drifted off
    # the anchor.
    if anchor_id is not None and not (
        0 <= cutoff < len(messages) and message_id(messages[cutoff]) == anchor_id
    ):
        resolved = _resolve_anchor_index(list(messages), anchor_id)
        if resolved is not None:
            cutoff = resolved
    return cutoff


# The note older checkpoints carry, which parse_summary_message still splits on
# when a message predates the summary_length stamp.
_LEGACY_FILE_NOTE = "\n\nFull conversation history saved to `"
_SUMMARY_SOURCE = "summarization"


#: What a summary message stands in for, read back when the next compaction
#: summarizes it: the span of turns, and the skills its note listed.
SUMMARIZED_KEY = "summarized"


def build_summary_message(
    summary: str,
    transcript: TranscriptTarget | None = None,
    original_message_count: int = 0,
    *,
    span: SummarySpan | None = None,
    index: Sequence[str] = (),
    skills: Sequence[str] = (),
    skill_files: bool = False,
    source: str = "model",
) -> HumanMessage:
    """Build the summary HumanMessage, pointing at the transcript when there is one.

    Tags with lc_source='summarization' for chain filtering, and stamps the
    emit-time ``context_window`` summarize fields into ``additional_kwargs``
    so checkpoint-sourced replay re-emits the event without the stored SSE
    stream. ``skills`` are listed for the agent to reload (see
    ``skill_reload_note``). ``source`` says what wrote the summary (see
    ``summarize``).
    """
    content = f"{CONTEXT_SUMMARY_PREFIX}{summary}"
    if transcript is not None:
        content += transcript_note(transcript, span, index)
    content += skill_reload_note(list(skills), files=skill_files)

    return HumanMessage(
        content=content,
        id=str(uuid.uuid4()),
        additional_kwargs={
            "lc_source": _SUMMARY_SOURCE,
            "summarize_complete": {
                "summary_length": len(summary),
                "original_message_count": original_message_count,
                "source": source,
            },
            SUMMARIZED_KEY: {
                "span": [span.first, span.last] if span is not None else None,
                "gap": list(span.gap) if span is not None and span.gap is not None else None,
                "skills": list(skills),
            },
        },
    )


def _summarized(message: Any) -> Mapping[str, Any]:
    stamp = (getattr(message, "additional_kwargs", None) or {}).get(SUMMARIZED_KEY)
    return stamp if isinstance(stamp, dict) else {}


def summarized_span(message: Any) -> SummarySpan | None:
    """The turns an earlier summary stood in for, and those it left out,
    when it recorded them."""
    stamp = _summarized(message)
    span, gap = stamp.get("span"), stamp.get("gap")
    if not _turn_pair(span):
        return None
    return SummarySpan(span[0], span[1], (gap[0], gap[1]) if _turn_pair(gap) else None)


def _turn_pair(value: Any) -> bool:
    return isinstance(value, list) and len(value) == 2 and all(isinstance(n, int) for n in value)


def listed_skills(messages: Iterable[Any]) -> list[str]:
    """The skills earlier summaries among ``messages`` listed for reloading."""
    return [
        name
        for message in messages
        if is_summary_message(message)
        for name in _summarized(message).get("skills") or ()
        if isinstance(name, str)
    ]


def compacted_skills(summarized: Any, kept: Any) -> list[str]:
    """Skills whose body a summary replaces and no kept message still carries
    whole.

    Part of a body counts on the summarized side, since a skill read in pages
    was still being followed. An earlier summary in ``summarized`` carries no
    body, only the names its note listed, and those skills are still out of
    context unless a kept message has read one again; dropping them would lose
    them for good.
    """
    still = {name for name, whole in skill_bodies(kept).items() if whole}
    names = dict.fromkeys([*listed_skills(summarized), *skill_bodies(summarized)])
    return [name for name in names if name not in still]


def skill_reload_note(names: list[str], *, files: bool) -> str:
    """What a summary tells the agent about skills it may still be following.

    ``files`` is whether the agent reaches skills as files (PTC, where a Read
    of SKILL.md loads one) rather than through LoadSkill (Flash).
    """
    if not names:
        return ""
    listed = ", ".join(f"`{name}`" for name in names)
    how = (
        "Read its `.agents/skills/<name>/SKILL.md` again"
        if files
        else "call LoadSkill with its name"
    )
    return (
        f"\n\nThe instructions of these skills were in the summarized part and "
        f"are no longer in your context: {listed}. Before you continue with one "
        f"you are still following, {how}."
    )


def summary_source(message: Any) -> str | None:
    """What wrote a summary message (see ``summarize``), as it was stamped."""
    stamp = (getattr(message, "additional_kwargs", None) or {}).get("summarize_complete")
    return stamp.get("source") if isinstance(stamp, dict) else None


def parse_summary_message(message: HumanMessage) -> str:
    """Recover the raw summary text from a ``build_summary_message`` message."""
    content = message.content if isinstance(message.content, str) else ""
    text = content.removeprefix(CONTEXT_SUMMARY_PREFIX)
    # The exact length is stamped at build time; slice by it rather than
    # string-splitting on the note, which would mis-truncate a summary that
    # itself contains the note text.
    stamped = message.additional_kwargs.get("summarize_complete") or {}
    length = stamped.get("summary_length")
    if isinstance(length, int) and 0 <= length <= len(text):
        return text[:length]
    # Legacy checkpoints without the stamp: fall back to note-prefix splitting.
    return text.rsplit(_LEGACY_FILE_NOTE, 1)[0]
