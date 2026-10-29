"""Token counting, prompt template, and truncation utilities for compaction."""

from __future__ import annotations

import asyncio
import base64
import io
import json
import math
import re
import uuid
from collections import OrderedDict
from collections.abc import Callable, Iterable, Iterator, Mapping, Sequence
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
from ptc_agent.agent.middleware.skills.registry import (
    RETIRED_SKILLS,
    skills_outside_sandbox,
)
from ptc_agent.agent.transcript import TranscriptTarget
from ptc_agent.agent.transcript.classify import LEGACY_SUMMARY_PREFIX, is_summary_message
from ptc_agent.agent.transcript.pointer import SummarySpan, transcript_note
from src.llms.attachment_payload import FILE_BLOCK_TYPES, IMAGE_BLOCK_TYPES, block_type
from src.llms.provider_compaction import SIDECAR_KEY, sidecar
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
    return sum(len(enc.encode(text)) + 3 for text in _sent_texts(messages))


#: Claude's count from tiktoken's, fitted to 175 samples Claude's
#: ``count_tokens`` counted: code, English, Chinese and Japanese prose,
#: market-data JSON, price tables, printed frames and 13 languages. Words count
#: about 1.6 times tiktoken, text outside ASCII about as tiktoken counts it,
#: and numbers as tiktoken counts them, which the digit term takes back. The
#: term is per digit and tiktoken packs three to a token, so a long number or
#: an id dump would come to almost nothing; each text is held to 0.95 times
#: tiktoken, which errs high only on the few Claude counts lower (Hindi,
#: printed frames).
_CLAUDE_PER_ASCII = 1.59
_CLAUDE_PER_OTHER = 0.99
_CLAUDE_PER_DIGIT = -0.52
_CLAUDE_FLOOR = 0.95
_NON_ASCII = re.compile(r"[^\x00-\x7f]+")


def count_tokens_claude(messages: Iterable[MessageLikeRepresentation]) -> int:
    """Count tokens as Claude does, estimated: its tokenizer is not public,
    and runs from about tiktoken's count on Chinese to 1.6 times it on
    English and code, so no single factor fits.

    Cross-validated, the median miss is 5% and nine in ten are within 15%;
    number-heavy output, the likeliest to miss, by up to about 15% either
    way. Only what no call has measured is counted this way
    (``message_sizes``).
    """
    enc = _get_tiktoken_encoder()
    total = 0
    for text in _sent_texts(messages):
        tokens = len(enc.encode(text))
        other = sum(len(enc.encode(run)) for run in _NON_ASCII.findall(text))
        digits = sum(map(text.count, "0123456789"))
        estimate = (
            _CLAUDE_PER_ASCII * (tokens - other)
            + _CLAUDE_PER_OTHER * other
            + _CLAUDE_PER_DIGIT * digits
        )
        total += round(max(estimate, _CLAUDE_FLOOR * tokens)) + 3
    return total


def _sent_texts(messages: Iterable[MessageLikeRepresentation]) -> Iterator[str]:
    """The text each message sends a model, as the counters count it."""
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

        yield text


# =============================================================================
# Attachment estimates
# =============================================================================

#: What one PDF page is counted at where the target's own charge is not known
#: (``get_pdf_page_tokens``). Anthropic documents 1,500 to 3,000 text tokens a
#: page, and sends each page as an image besides (about 1,500 tokens for a
#: letter page scaled to its 1.15 megapixel bound); OpenAI also reads a page as
#: text plus image, and measured 3,230 a page on an image-heavy paper. The top
#: of the text range covers a typical page with its image. A denser one runs
#: past it, and the provider's count of that send corrects the estimate (see
#: ``fitting.Fit._measured``), or its refusal sends the call to
#: compaction anyway. A vendor that charges far less, as Gemini does, is
#: counted at its own charge, since this would take out pages it can carry.
PDF_PAGE_TOKENS = 3_000

#: Anthropic's per-image bound (1,568 px on the long edge, 1.15 megapixels,
#: a token per 750 px), the most any route charges for one image.
_IMAGE_MAX_EDGE = 1568
_IMAGE_MAX_PIXELS = 1_150_000
_IMAGE_PIXELS_PER_TOKEN = 750
#: OpenAI's base charge, the least an image costs anywhere.
_IMAGE_MIN_TOKENS = 85
#: An image whose size can't be read is counted at the bound.
_IMAGE_UNKNOWN_TOKENS = 1_600

#: Base64 enough for an image header: PNG's is in its first 24 bytes, a
#: JPEG's after whatever EXIF precedes it.
_IMAGE_HEADER_CHARS = 64 * 1024


def attachment_tokens(
    messages: Iterable[Any], cost: Callable[[Any], int] | None = None
) -> int:
    """What the image and PDF blocks in ``messages`` cost a provider.

    Apart from ``count_tokens_tiktoken``, which counts an attachment by its
    short label: that is what the summary model is sent once the attachments
    are saved to files, while a model reading the history is sent the bytes.
    ``cost`` is what the target is charged for a block (``target_cost``);
    None counts every attachment at the default rates.
    """
    cost = cost or block_tokens
    total = 0
    for msg in messages:
        content = message_field(msg, "content")
        if isinstance(content, list):
            total += sum(cost(block) for block in content)
    return total


def target_cost(
    sent: Callable[[Any], bool] | None, page_tokens: int | None
) -> Callable[[Any], int]:
    """What a target is charged for a block: nothing for one it is not sent
    as it is, which goes out as a placeholder of a few tokens, and a PDF page
    at ``page_tokens``, or ``PDF_PAGE_TOKENS`` where that is not known."""
    per_page = page_tokens or PDF_PAGE_TOKENS
    return lambda block: block_tokens(block, per_page) if sent is None or sent(block) else 0


def block_tokens(block: Any, page_tokens: int = PDF_PAGE_TOKENS) -> int:
    """One block's cost as an attachment, 0 for anything else."""
    kind = block_type(block)
    if kind in FILE_BLOCK_TYPES:
        # One that can't be read counts as a page.
        return max(pdf_pages(block) or 1, 1) * page_tokens
    if kind in IMAGE_BLOCK_TYPES:
        size = _image_size(block)
        if size is None:
            return _IMAGE_UNKNOWN_TOKENS
        width, height = size
        scale = min(
            1.0,
            _IMAGE_MAX_EDGE / max(width, height, 1),
            (_IMAGE_MAX_PIXELS / max(width * height, 1)) ** 0.5,
        )
        pixels = (width * scale) * (height * scale)
        return max(_IMAGE_MIN_TOKENS, math.ceil(pixels / _IMAGE_PIXELS_PER_TOKEN))
    return 0


def _inline_base64(block: dict) -> str | None:
    """The base64 an attachment block carries, in any of its shapes."""
    if block.get("type") == "image_url":
        url = (block.get("image_url") or {}).get("url", "")
        return url.split(";base64,", 1)[1] if ";base64," in url else None
    data = block.get("base64")
    source = block.get("source")
    if data is None and isinstance(source, dict) and source.get("type") == "base64":
        data = source.get("data")
    return data if isinstance(data, str) else None


#: What was read from an inline attachment, a PDF's page count or an image's
#: size, by its kind and its base64's hash and length. Each is counted on every
#: model call, on the event loop, so each is read once rather than per call. A
#: pure function of the bytes, so a cache per process agrees with every other.
_PARSED: OrderedDict[tuple[str, int, int], Any] = OrderedDict()
_PARSED_MAX = 256


def _parsed(kind: str, data: str, read: Callable[[str], Any]) -> Any:
    """``read(data)``, kept in ``_PARSED``; a failed read is kept as None too."""
    # A string caches its own hash, so a block counted again costs nothing.
    key = (kind, hash(data), len(data))
    if key in _PARSED:
        _PARSED.move_to_end(key)
        return _PARSED[key]
    return _keep(key, _read_safely(read, data))


def _read_safely(read: Callable[[str], Any], data: str) -> Any:
    try:
        return read(data)
    except Exception:
        return None


def _keep(key: tuple[str, int, int], value: Any) -> Any:
    _PARSED[key] = value
    if len(_PARSED) > _PARSED_MAX:
        _PARSED.popitem(last=False)
    return value


async def read_ahead(messages: Iterable[Any]) -> None:
    """Read off the event loop what counting ``messages`` would parse on it.

    The counts are synchronous and run on every model call. An attachment
    with no stamp, such as a PDF checkpointed by an older build, would be
    decoded and parsed there on its first count, so each one not yet in
    ``_PARSED`` is read in a worker thread first, and kept on the loop.
    """
    misses: dict[tuple[str, int, int], tuple[str, Callable[[str], Any]]] = {}
    for msg in messages:
        content = message_field(msg, "content")
        if not isinstance(content, list):
            continue
        for block in content:
            if not isinstance(block, dict):
                continue
            kind = block_type(block)
            if kind in FILE_BLOCK_TYPES and not isinstance(block.get("pages"), int):
                read, name = _read_pages, "pdf"
            elif kind in IMAGE_BLOCK_TYPES:
                read, name = _read_size, "image"
            else:
                continue
            data = _inline_base64(block)
            if data and (key := (name, hash(data), len(data))) not in _PARSED:
                misses[key] = (data, read)
    if not misses:
        return
    values = await asyncio.to_thread(
        lambda: [_read_safely(read, data) for data, read in misses.values()]
    )
    for key, value in zip(misses, values):
        _keep(key, value)


def pdf_pages(block: dict) -> int | None:
    """A PDF block's page count, None when it can't be read.

    The stamp, else parsed: a block checkpointed before the stamp, or one the
    composer attached inline, carries none. Never guessed from its size, since
    bytes per page run from a few KB of text to a megabyte of scan.
    """
    pages = block.get("pages")
    if isinstance(pages, int):
        return pages
    data = _inline_base64(block)
    if not data:
        return None
    return _parsed("pdf", data, _read_pages)


def _read_pages(data: str) -> int:
    import pypdf

    return len(pypdf.PdfReader(io.BytesIO(base64.b64decode(data))).pages)


def _image_size(block: dict) -> tuple[int, int] | None:
    """An inline image's width and height, read from its header."""
    data = _inline_base64(block)
    if not data:
        return None
    return _parsed("image", data, _read_size)


def _read_size(data: str) -> tuple[int, int] | None:
    from PIL import Image

    # The header first, the whole image only when it lies past that.
    for chars in (_IMAGE_HEADER_CHARS, len(data)):
        head = data[: chars - chars % 4]
        try:
            with Image.open(io.BytesIO(base64.b64decode(head))) as img:
                return img.size
        except Exception:
            if chars >= len(data):
                return None
    return None


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
        non_critical = _is_non_critical_read(file_path)
        latest = entries[-1][0]
        for i, call_id in entries:
            if (
                i < cutoff_index
                and (non_critical or i != latest)
                and messages[i].content != read_offload_marker(file_path)
            ):
                stale.add(call_id)
    return stale


def _is_non_critical_read(file_path: str) -> bool:
    """Whether ``file_path`` lies under a ``NON_CRITICAL_READ_PREFIXES`` dir,
    matched wherever the dir sits in the path: the model names these files
    workspace-relative or absolute, and the scratchpad's are always absolute."""
    return any(
        file_path.startswith(prefix) or f"/{prefix}" in file_path
        for prefix in NON_CRITICAL_READ_PREFIXES
    )


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


def provider_cutoff(messages: list[AnyMessage], cutoff_index: int) -> int:
    """The latest cut at or before ``cutoff_index`` that a provider's own
    compaction takes, or 0 when there is none.

    The kept part opens on an assistant turn right after a user-side one, so
    the API cannot merge it into the messages summarized, and at least two
    messages are summarized, so there is more than an earlier summary to
    condense.
    """
    for i in range(min(cutoff_index, len(messages) - 1), 1, -1):
        if (
            isinstance(messages[i], AIMessage)
            and isinstance(messages[i - 1], (HumanMessage, ToolMessage))
            and find_group_safe_cutoff(messages, i) == i
        ):
            return i
    return 0


def turn_start_cut(messages: list[AnyMessage]) -> int:
    """Where a summary that keeps no turn ends on a turn's first call: right
    after the last answer, which called no tool, with the user's new message
    after it; 0 anywhere else. Mid-loop there is none, since a kept tool
    result needs the call it answers, and that call its thinking.
    """
    for i in range(len(messages) - 1, 0, -1):
        message = messages[i]
        if isinstance(message, ToolMessage):
            return 0
        if isinstance(message, AIMessage):
            following = messages[i + 1] if i + 1 < len(messages) else None
            return i + 1 if not message.tool_calls and isinstance(following, HumanMessage) else 0
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

#: The ``response_metadata`` key holding the summary trigger a model call ran
#: under. The trigger is per turn (the user's or the model's compaction
#: preset), and the checkpoint is all replay has of the turn, so it rides on
#: the response whose usage the ring shows.
CONTEXT_THRESHOLD_KEY = "context_threshold"


def stamp_context_threshold(message: AIMessage, threshold: int) -> None:
    message.response_metadata = {
        **(message.response_metadata or {}),
        CONTEXT_THRESHOLD_KEY: threshold,
    }


def context_threshold(message: Any) -> int | None:
    """The summary trigger stamped on ``message``, or None for a message
    saved before the stamp."""
    value = (getattr(message, "response_metadata", None) or {}).get(CONTEXT_THRESHOLD_KEY)
    return value if isinstance(value, int) and value > 0 else None


#: The ``response_metadata`` key holding the summary a model call compacted
#: to before it ran. The summary message lives only in
#: ``_summarization_event``, which the next compaction overwrites, so each
#: summary, and where in the turn it fired, rides on the response that
#: answered from it.
COMPACTION_SUMMARY_KEY = "context_summary"


def stamp_compaction_summary(
    message: AIMessage, summary_message_id: str, fields: Mapping[str, Any]
) -> None:
    """Stamp ``fields``, the summarize complete signal's payload, on the
    response to the call that compacted to ``summary_message_id``."""
    message.response_metadata = {
        **(message.response_metadata or {}),
        COMPACTION_SUMMARY_KEY: {"summary_message_id": summary_message_id, **fields},
    }


def compaction_summary(message: Any) -> dict[str, Any] | None:
    """The summary stamped on ``message``, or None for a response whose call
    did not compact or that was saved before the stamp."""
    value = (getattr(message, "response_metadata", None) or {}).get(COMPACTION_SUMMARY_KEY)
    if isinstance(value, dict) and isinstance(value.get("summary_message_id"), str):
        return value
    return None


def build_summary_message(
    summary: str,
    transcript: TranscriptTarget | None = None,
    original_message_count: int = 0,
    *,
    span: SummarySpan | None = None,
    index: Sequence[str] = (),
    skills: Sequence[str] = (),
    source: str = "model",
    notes: str = "",
    native: Mapping[str, Any] | None = None,
    origin: str | None = None,
) -> HumanMessage:
    """Build the summary HumanMessage, pointing at the transcript when there is one.

    Tags with lc_source='summarization' for chain filtering, and stamps the
    emit-time ``context_window`` summarize fields into ``additional_kwargs``
    so checkpoint-sourced replay re-emits the event without the stored SSE
    stream. ``skills`` are listed for the agent to reload (see
    ``skill_reload_note``). ``source`` says what wrote the summary (see
    ``summarize``). ``notes`` (the scratchpad pointer, already rendered) goes
    last; the stamped length is what keeps it out of the summary a reader
    parses back. ``native`` is what a provider's own compaction answered
    with, sent in place of ``summary`` with the notes after it where the
    provider can read it (see ``src.llms.provider_compaction``).
    """
    note = transcript_note(transcript, span, index) if transcript is not None else ""
    note += skill_reload_note(list(skills)) + notes
    content = f"{CONTEXT_SUMMARY_PREFIX}{summary}{note}"
    carried = {SIDECAR_KEY: sidecar(native, origin, note.lstrip())} if native else {}

    return HumanMessage(
        content=content,
        id=str(uuid.uuid4()),
        additional_kwargs={
            **carried,
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


def compacted_skills(
    summarized: Any, kept: Any, user_skills: Iterable[str] = ()
) -> list[str]:
    """Skills whose body a summary replaces and no kept message still carries
    whole.

    Part of a body counts on the summarized side, since a skill read in pages
    was still being followed. An earlier summary in ``summarized`` carries no
    body, only the names its note listed, and those skills are still out of
    context unless a kept message has read one again; dropping them would lose
    them for good.

    A built-in the sandbox does not carry (a hidden one, which only the host
    injects, one switched off, or a retired one) has no SKILL.md to read back,
    so it is left out rather than sending the agent to a file that is not there.
    A retired name is free for the user to take, and one of ``user_skills``
    by that name has a file like any other.
    """
    still = {name for name, whole in skill_bodies(kept).items() if whole}
    no_file = skills_outside_sandbox() | (RETIRED_SKILLS - set(user_skills))
    names = dict.fromkeys([*listed_skills(summarized), *skill_bodies(summarized)])
    return [name for name in names if name not in still and name not in no_file]


def skill_reload_note(names: list[str]) -> str:
    """What a summary tells the agent about skills it may still be following."""
    if not names:
        return ""
    listed = ", ".join(f"`{name}`" for name in names)
    return (
        f"\n\nThe instructions of these skills were in the summarized part and "
        f"are no longer in your context: {listed}. Before you continue with one "
        f"you are still following, Read its `.agents/skills/<name>/SKILL.md` again."
    )


def summary_source(message: Any) -> str | None:
    """What wrote a summary message (see ``summarize``), as it was stamped."""
    stamp = (getattr(message, "additional_kwargs", None) or {}).get("summarize_complete")
    return stamp.get("source") if isinstance(stamp, dict) else None


def handed_off(message: Any) -> bool:
    """Whether a summary only its provider can read has been written out as
    text by its model, so its text is a summary rather than a digest."""
    stamp = (getattr(message, "additional_kwargs", None) or {}).get("summarize_complete")
    return isinstance(stamp, dict) and stamp.get("handed_off") is True


def with_summary_text(message: HumanMessage, text: str) -> HumanMessage | None:
    """``message``, a summary message, with ``text`` in place of its summary
    and everything after it (the transcript pointer, the notes) kept; None for
    a message not built by ``build_summary_message``.

    The id is kept, so the marker that names the summary still finds it. What
    the provider is sent in its place is the sidecar, which stays as it was.
    """
    content = message.content if isinstance(message.content, str) else ""
    if not content.startswith(CONTEXT_SUMMARY_PREFIX):
        return None
    after = content[len(CONTEXT_SUMMARY_PREFIX) + len(parse_summary_message(message)) :]
    stamp = message.additional_kwargs.get("summarize_complete") or {}
    return message.model_copy(
        update={
            "content": f"{CONTEXT_SUMMARY_PREFIX}{text}{after}",
            "additional_kwargs": {
                **message.additional_kwargs,
                "summarize_complete": {
                    **stamp,
                    "summary_length": len(text),
                    "handed_off": True,
                },
            },
        }
    )


def parse_summary_message(message: HumanMessage) -> str:
    """Recover the raw summary text from a ``build_summary_message`` message."""
    content = message.content if isinstance(message.content, str) else ""
    text = next(
        (
            content[len(prefix) :]
            for prefix in (CONTEXT_SUMMARY_PREFIX, LEGACY_SUMMARY_PREFIX)
            if content.startswith(prefix)
        ),
        content,
    )
    # The exact length is stamped at build time; slice by it rather than
    # string-splitting on the note, which would mis-truncate a summary that
    # itself contains the note text.
    stamped = message.additional_kwargs.get("summarize_complete") or {}
    length = stamped.get("summary_length")
    if isinstance(length, int) and 0 <= length <= len(text):
        return text[:length]
    # Legacy checkpoints without the stamp: fall back to note-prefix splitting.
    return text.rsplit(_LEGACY_FILE_NOTE, 1)[0]
