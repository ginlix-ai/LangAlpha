"""Backend filesystem offloading for evicted messages and truncated args."""

import base64
import logging
import uuid
from typing import Any

from langchain_core.messages import AIMessage, AnyMessage
from langchain_core.messages.human import HumanMessage
from langgraph.config import get_config

from ptc_agent.agent.middleware.compaction.utils import (
    _extract_text_from_content,
    strip_base64_from_messages,
)

logger = logging.getLogger(__name__)


def is_summary_message(msg: AnyMessage) -> bool:
    """Check if a message is a previous summarization message.

    Summary messages are tagged with lc_source='summarization' in additional_kwargs.
    These should be filtered from offloads to avoid summary-of-summary noise.
    """
    if not isinstance(msg, HumanMessage):
        return False
    return msg.additional_kwargs.get("lc_source") == "summarization"


def filter_summary_messages(messages: list[AnyMessage]) -> list[AnyMessage]:
    """Filter out previous summary messages from a message list."""
    return [msg for msg in messages if not is_summary_message(msg)]


def get_thread_id() -> str:
    """Extract short thread_id from langgraph config.

    Returns:
        First 8 characters of thread_id, or a generated session ID.
    """
    try:
        config = get_config()
        thread_id = config.get("configurable", {}).get("thread_id")
        if thread_id is not None:
            return str(thread_id)[:8]
    except RuntimeError:
        pass

    return f"session_{uuid.uuid4().hex[:8]}"


async def aoffload_to_backend(backend: Any, messages: list[AnyMessage]) -> str | None:
    """Persist evicted messages to sandbox before summarization (async).

    Each message is written to its own file keyed by message ID:
    `.agents/threads/{tid}/evicted_{message_id}.md`

    Previous summary messages are filtered out to avoid summary-of-summary noise.
    Individual messages are truncated at 5000 chars for storage.

    Args:
        backend: The Daytona backend for filesystem operations.
        messages: Messages being summarized (evicted from context).

    Returns:
        The thread directory path where files were stored, or None if
        offload failed or backend is not available.
    """
    if backend is None:
        return None

    # Filter out previous summary messages
    filtered_messages = filter_summary_messages(messages)
    if not filtered_messages:
        return None

    thread_id = get_thread_id()
    thread_dir = f".agents/threads/{thread_id}"
    written = 0

    for msg in filtered_messages:
        msg_id = msg.id or uuid.uuid4().hex[:8]
        path = f"{thread_dir}/evicted_{msg_id}.md"

        content = _extract_text_from_content(msg.content)
        if len(content) > 5000:
            content = content[:5000] + "\n...(truncated)"

        # Include tool call info for AI messages
        tool_info = ""
        if isinstance(msg, AIMessage) and msg.tool_calls:
            tool_names = [tc["name"] for tc in msg.tool_calls]
            tool_info = f" [tools: {', '.join(tool_names)}]"

        file_content = f"# {msg.type}{tool_info}\n\n{content}\n"

        try:
            result = await backend.awrite(path, file_content)
            if result is None or result.error:
                error_msg = result.error if result else "backend returned None"
                logger.warning(
                    "Failed to offload evicted message %s to %s: %s",
                    msg_id,
                    path,
                    error_msg,
                )
            else:
                written += 1
        except Exception as e:
            logger.warning(
                "Exception offloading evicted message %s to %s: %s",
                msg_id,
                path,
                e,
            )

    if written > 0:
        logger.debug(
            "Offloaded %d/%d evicted messages to %s",
            written,
            len(filtered_messages),
            thread_dir,
        )
        return thread_dir

    return None


async def aoffload_truncated_args(
    backend: Any, originals: dict[str, dict[str, Any]]
) -> None:
    """Persist original tool call args to sandbox before truncation discards them.

    Each truncated tool call gets its own file at
    `.agents/threads/{tid}/truncated_args_{toolcall_id}.md`.

    Non-fatal -- logs warnings on failure but never raises.

    Args:
        backend: The Daytona backend for filesystem operations.
        originals: Mapping of tool_call_id -> {"name": str, "args": dict}
                   as returned by truncate_message_args.
    """
    if backend is None or not originals:
        return

    thread_id = get_thread_id()

    for tool_call_id, original in originals.items():
        path = f".agents/threads/{thread_id}/truncated_args_{tool_call_id}.md"
        tool_name = original["name"]
        args = original["args"]

        # Format each arg as a section
        parts = [f"# {tool_name} (call {tool_call_id})\n"]
        for key, value in args.items():
            str_value = str(value) if not isinstance(value, str) else value
            parts.append(f"## {key}\n\n```\n{str_value}\n```\n")

        content = "\n".join(parts)

        try:
            result = await backend.awrite(path, content)
            if result is None or result.error:
                error_msg = result.error if result else "backend returned None"
                logger.warning(
                    "Failed to offload truncated args for %s (%s): %s",
                    tool_call_id,
                    tool_name,
                    error_msg,
                )
            else:
                logger.debug(
                    "Offloaded truncated args for %s (%s) to %s",
                    tool_call_id,
                    tool_name,
                    path,
                )
        except Exception as e:
            logger.warning(
                "Exception offloading truncated args for %s (%s): %s",
                tool_call_id,
                tool_name,
                e,
            )


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

    Handles three provider-specific formats:
    - ``image_url`` with ``data:...;base64,...`` URL (OpenAI style)
    - ``file`` with ``base64`` key (PDF uploads)
    - ``image`` with base64 source (Anthropic native)

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

    # PDF / file upload with inline base64
    if block_type == "file" and "base64" in block:
        data = block["base64"]
        mime = block.get("mime_type", "application/pdf")
        fname = block.get("filename", "file")
        return data, mime, f"pdf_{fname}"

    # Anthropic native image block
    if block_type == "image":
        source = block.get("source") or {}
        if source.get("type") == "base64" and "data" in source:
            data = source["data"]
            mime = source.get("media_type", "image/png")
            return data, mime, "image"

    return None


async def aoffload_base64_content(
    backend: Any,
    messages: list[AnyMessage],
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

    thread_id = get_thread_id()
    thread_dir = f".agents/threads/{thread_id}"

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
                            "text": f"[PDF saved to {path} — use read_file to view]",
                        }
                    )
                else:
                    new_blocks.append(
                        {
                            "type": "text",
                            "text": f"[Image saved to {path} — use read_file to view]",
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
