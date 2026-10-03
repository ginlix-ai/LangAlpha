"""Offloading: the view that re-applies recorded offloads to each model call,
and the backend files that keep what inline attachments cut."""

import base64
import logging
import uuid
from collections.abc import Mapping
from typing import Any

from langchain_core.messages import AIMessage, AnyMessage, ToolMessage
from langgraph.config import get_config

from ptc_agent.core.paths import WorkspaceLayout
from src.llms.attachment_payload import FILE_BLOCK_TYPES
from ptc_agent.agent.middleware.compaction.types import TRUNCATABLE_TOOLS
from ptc_agent.agent.middleware.compaction.utils import (
    get_effective_messages,
    oversized_arg_calls,
    read_offload_marker,
    strip_base64_from_messages,
    truncate_read_results,
    truncate_tool_call,
)
from ptc_agent.agent.transcript import TranscriptTarget
from ptc_agent.agent.transcript.pointer import TranscriptTurns

logger = logging.getLogger(__name__)


def tool_call_ids(messages: list[AnyMessage]) -> set[str]:
    return {
        tc["id"]
        for msg in messages
        if isinstance(msg, AIMessage)
        for tc in msg.tool_calls or ()
        if tc.get("id")
    }


def arg_offload_marker(path: str, call_id: str) -> str:
    return (
        f"... [argument cut here. Its full call is in {path}; read it with "
        f"jq -c 'select(.call_id==\"{call_id}\" and .type==\"tool_call\") "
        f"| .args' {path}]"
    )


def offload_turns(
    messages: list[AnyMessage], thread_id: str | None, checkpoint_ns: str = ""
) -> TranscriptTurns | None:
    """The transcript files a cut argument names, numbered over the agent's
    whole checkpoint list ``messages``."""
    if not thread_id:
        return None
    return TranscriptTurns.of(
        TranscriptTarget.for_agent(str(thread_id), checkpoint_ns), messages
    )


def select_offloads(
    effective: list[AnyMessage],
    cutoff_index: int,
    max_length: int,
    arg_ids: set[str],
    read_ids: set[str],
) -> tuple[set[str], set[str]]:
    """New (arg ids, read ids) to hide among the effective messages before
    ``cutoff_index``, leaving out those already recorded."""
    if cutoff_index <= 0:
        return set(), set()
    new_args = oversized_arg_calls(effective, cutoff_index, max_length) - arg_ids
    _, _, stale_reads = truncate_read_results(effective, cutoff_index)
    return new_args, stale_reads - read_ids


def idle_offloads(
    state: Mapping[str, Any],
    *,
    idle_seconds: float | None,
    keep_messages: int,
    max_length: int,
    now: float,
) -> tuple[set[str], set[str]]:
    """New (arg ids, read ids) to hide as a turn starts: none unless the
    model last answered at least ``idle_seconds`` before ``now``, and none
    among the newest ``keep_messages`` of the view. A thread with no recorded
    answer time (its first turn, or one from before the time was kept) waits
    for one."""
    last = state.get("_last_model_response_at")
    if idle_seconds is None or not last or now - last < idle_seconds:
        return set(), set()
    effective = get_effective_messages(
        list(state.get("messages") or ()), state.get("_summarization_event")
    )
    return select_offloads(
        effective,
        len(effective) - keep_messages,
        max_length,
        set(state.get("_offloaded_tool_call_ids") or ()),
        set(state.get("_offloaded_read_result_ids") or ()),
    )


def apply_recorded_offloads(
    messages: list[AnyMessage],
    arg_ids: set[str],
    read_ids: set[str],
    max_length: int,
    truncation_text: str,
    turns: TranscriptTurns | None = None,
) -> list[AnyMessage]:
    """Re-apply every recorded Tier 1 offload to one model call's messages.

    An offload is a view over the checkpoint, not a rewrite of it: the id sets
    are the record, and every call re-truncates them, so a call cut once stays
    cut and the prompt cache holds. Each id is checked against its tool, so an
    arg id never blanks a result and a read id only replaces a Read result.

    A cut argument points at its call in the transcript file of its turn
    (``turns``), which the caller passes only while a mount serves that file;
    where none does it does not cut arguments at all. Without ``turns`` it
    reads ``truncation_text``, as ids recorded where no transcript was kept
    did.
    """
    if not arg_ids and not read_ids:
        return messages

    read_paths: dict[str, str] = {}
    out: list[AnyMessage] = []
    changed = False
    for msg in messages:
        if isinstance(msg, AIMessage) and msg.tool_calls:
            calls = []
            msg_changed = False
            path = turns.path(msg.id) if turns is not None else None
            for tc in msg.tool_calls:
                if tc["name"] == "Read" and tc["id"] in read_ids:
                    read_paths[tc["id"]] = tc.get("args", {}).get("file_path", "")
                if tc["id"] in arg_ids and tc["name"] in TRUNCATABLE_TOOLS:
                    marker = (
                        arg_offload_marker(path, tc["id"]) if path else truncation_text
                    )
                    new_tc = truncate_tool_call(tc, max_length, marker)
                    msg_changed = msg_changed or new_tc is not tc
                    calls.append(new_tc)
                else:
                    calls.append(tc)
            if msg_changed:
                msg = msg.model_copy()
                msg.tool_calls = calls
                changed = True
        elif isinstance(msg, ToolMessage) and msg.tool_call_id in read_paths:
            marker = read_offload_marker(read_paths[msg.tool_call_id])
            if msg.content != marker:
                msg = msg.model_copy()
                msg.content = marker
                changed = True
        out.append(msg)

    return out if changed else messages


def get_thread_id(thread_id: str | None = None) -> str:
    """Short thread id: the one passed, else graph config's, else a session id.

    Manual /compact and /offload run outside the graph, so they pass the id;
    the session fallback is fresh on every call and names no real thread.
    """
    if thread_id:
        return str(thread_id)[:8]
    try:
        config = get_config()
        thread_id = config.get("configurable", {}).get("thread_id")
        if thread_id is not None:
            return str(thread_id)[:8]
    except RuntimeError:
        pass

    return f"session_{uuid.uuid4().hex[:8]}"


# =============================================================================
# Base64 content offloading
# =============================================================================

# Mime type → file extension mapping
_MIME_TO_EXT: dict[str, str] = {
    "image/png": "png",
    "image/jpeg": "jpg",
    "image/gif": "gif",
    "image/webp": "webp",
    "application/pdf": "pdf",
}


def _extract_base64_info(block: dict) -> tuple[str, str, str] | None:
    """Extract (base64_data, mime_type, label) from a content block.

    Handles every provider format an attachment arrives in: an ``image_url``
    data URI (OpenAI), a ``base64`` key (langchain v1), and a ``source`` object
    (Anthropic native), under either name the block type goes by.

    Returns None if the block doesn't contain base64 data.
    """
    block_type = block.get("type", "")

    # OpenAI-style image_url with data URI
    if block_type == "image_url":
        url = (block.get("image_url") or {}).get("url", "")
        if url.startswith("data:") and ";base64," in url:
            # Parse "data:image/png;base64,<DATA>"
            header, data = url.split(";base64,", 1)
            mime = header.replace("data:", "")
            return data, mime, "image"
        return None

    source = block.get("source") or {}
    if not isinstance(source, dict):
        source = {}
    inline = source.get("data") if source.get("type") == "base64" else None

    # PDF / file upload with inline base64
    if block_type in FILE_BLOCK_TYPES:
        data = block.get("base64") or inline
        if data is None:
            return None
        mime = block.get("mime_type") or source.get("media_type") or "application/pdf"
        fname = block.get("filename", "file")
        return data, mime, f"pdf_{fname}"

    # Anthropic native image block
    if block_type == "image":
        data = block.get("base64") or inline
        if data is not None:
            mime = block.get("mime_type") or source.get("media_type") or "image/png"
            return data, mime, "image"

    return None


async def aoffload_base64_content(
    backend: Any,
    messages: list[AnyMessage],
    *,
    thread_id: str | None = None,
) -> list[AnyMessage]:
    """Offload base64 content blocks to sandbox files, replacing with path references.

    For each message containing base64 content blocks:
    1. Decode the base64 data
    2. Upload to ``.agents/threads/{thread_id}/`` via ``backend.aupload_files``
    3. Replace the block with a text reference to the saved file

    When ``backend`` is None (e.g. flash agent with no sandbox), falls back to
    :func:`strip_base64_from_messages` which replaces base64 with simple
    ``[Image]`` / ``[PDF: name]`` placeholders.

    Args:
        backend: Daytona backend for file uploads, or None.
        messages: Messages potentially containing base64 content blocks.

    Returns:
        New message list with base64 content replaced. Returns the original
        list if no base64 content was found.
    """
    if backend is None:
        return strip_base64_from_messages(messages)

    thread_dir = WorkspaceLayout.thread_subdir(get_thread_id(thread_id))

    result: list[AnyMessage] = []
    changed = False

    for msg in messages:
        content = msg.content
        if not isinstance(content, list):
            result.append(msg)
            continue

        new_blocks: list = []
        msg_changed = False
        msg_id = (msg.id or uuid.uuid4().hex)[:8]

        for idx, block in enumerate(content):
            if not isinstance(block, dict):
                new_blocks.append(block)
                continue

            info = _extract_base64_info(block)
            if info is None:
                new_blocks.append(block)
                continue

            b64_data, mime_type, label = info
            ext = _MIME_TO_EXT.get(mime_type, "bin")
            filename = f"{label}_{msg_id}_{idx}.{ext}"
            path = f"{thread_dir}/{filename}"

            try:
                raw_bytes = base64.b64decode(b64_data)
                upload_result = await backend.aupload_files([(path, raw_bytes)])

                if upload_result is None or (
                    hasattr(upload_result, "error") and upload_result.error
                ):
                    error_msg = (
                        upload_result.error
                        if upload_result and hasattr(upload_result, "error")
                        else "backend returned None"
                    )
                    logger.warning(
                        "Failed to offload base64 block %d of message %s: %s",
                        idx,
                        msg_id,
                        error_msg,
                    )
                    # Fall back to simple placeholder
                    if "pdf" in label:
                        new_blocks.append({"type": "text", "text": f"[PDF: {label}]"})
                    else:
                        new_blocks.append({"type": "text", "text": "[Image]"})
                    msg_changed = True
                    continue

                # Success — replace with file path reference
                if ext == "pdf":
                    new_blocks.append(
                        {
                            "type": "text",
                            "text": f"[PDF saved to {path} — use Read to view]",
                        }
                    )
                else:
                    new_blocks.append(
                        {
                            "type": "text",
                            "text": f"[Image saved to {path} — use Read to view]",
                        }
                    )
                msg_changed = True
                logger.debug(
                    "Offloaded base64 block %d of message %s to %s", idx, msg_id, path
                )

            except Exception as e:
                logger.warning(
                    "Exception offloading base64 block %d of message %s: %s",
                    idx,
                    msg_id,
                    e,
                )
                # Fall back to simple placeholder
                if "pdf" in label:
                    new_blocks.append({"type": "text", "text": f"[PDF: {label}]"})
                else:
                    new_blocks.append({"type": "text", "text": "[Image]"})
                msg_changed = True

        if msg_changed:
            copy = msg.model_copy()
            copy.content = new_blocks
            result.append(copy)
            changed = True
        else:
            result.append(msg)

    return result if changed else messages
