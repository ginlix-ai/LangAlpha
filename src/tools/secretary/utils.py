"""Utility functions for secretary tools."""

import json
import logging
import re
from typing import Any

logger = logging.getLogger(__name__)

MAX_OUTPUT_CHARS = 8000

# File extensions recognized as workspace file references (mirrors frontend KNOWN_EXTS)
_FILE_EXTS = (
    r"md|txt|pdf|doc|docx|rtf|"
    r"py|js|jsx|ts|tsx|html|css|sh|bash|sql|r|ipynb|"
    r"csv|json|yaml|yml|xml|toml|ini|cfg|log|env|xlsx|xls|"
    r"png|jpg|jpeg|gif|svg|webp|bmp|"
    r"zip|tar|gz"
)

# Workspace-qualified path prefix: __wsref__/{workspace_id}/relative/path
# Uses a path-based encoding instead of ws:// protocol to survive HTML sanitizers.
_WSREF_PREFIX = "__wsref__"

# Matches markdown links: [text](path) and ![text](path)
# Captures: group(1)=prefix "![text](" or "[text](", group(2)=path, group(3)=")"
# Path must be relative (no http/https/mailto/#), contain at least one "/",
# and end with a known extension.
_MD_LINK_RE = re.compile(
    r"(!?\[[^\]]*\]\()"  # prefix: ![...]( or [...](
    r"((?!https?://|mailto:|#|__wsref__/)"  # not external URL or already qualified
    r"(?:/home/(?:workspace|daytona)/)?[a-zA-Z_][^\s)]*/"  # at least one dir segment
    r"[^\s)]*\.(?:" + _FILE_EXTS + r"))"  # filename.ext
    r"(\))",  # closing paren
    re.IGNORECASE,
)


def _qualify_file_paths(text: str, workspace_id: str) -> str:
    """Rewrite relative file paths in markdown links to __wsref__/{workspace_id}/path.

    Transforms:
        [report.md](results/report.md) → [report.md](__wsref__/{wid}/results/report.md)
        ![chart](work/t/charts/r.png)  → ![chart](__wsref__/{wid}/work/t/charts/r.png)

    Uses a path-based prefix instead of a protocol (ws://) because HTML sanitizers
    strip non-standard URL protocols. The __wsref__ prefix looks like a relative path
    to the sanitizer and passes through untouched.

    Leaves external URLs and already-qualified __wsref__ paths untouched.
    """
    if not workspace_id or not text:
        return text

    def _rewrite(m: re.Match) -> str:
        prefix, path, suffix = m.group(1), m.group(2), m.group(3)
        # Strip sandbox absolute prefix if present
        path = re.sub(r"^/home/(?:workspace|daytona)/", "", path)
        return f"{prefix}{_WSREF_PREFIX}/{workspace_id}/{path}{suffix}"

    return _MD_LINK_RE.sub(_rewrite, text)


def _parse_sse_string(raw: str) -> tuple[str, dict] | None:
    """Parse a raw SSE string into (event_type, data_dict).

    Raw SSE format: "id: 42\\nevent: message_chunk\\ndata: {...}\\n\\n"

    Args:
        raw: Raw SSE string from Redis

    Returns:
        Tuple of (event_type, data_dict) or None if parsing fails
    """
    try:
        event_type = ""
        data_str = ""

        for line in raw.split("\n"):
            line = line.strip()
            if line.startswith("event:"):
                event_type = line[len("event:"):].strip()
            elif line.startswith("data:"):
                data_str = line[len("data:"):].strip()

        if not event_type or not data_str:
            return None

        data = json.loads(data_str)
        return (event_type, data)
    except (json.JSONDecodeError, ValueError, AttributeError):
        return None


async def extract_text_from_thread(thread_id: str) -> dict[str, Any]:
    """Extract text content from a thread's SSE events.

    Reads from Redis if the thread is actively running, otherwise reads
    from the database. Filters for message_chunk events with text content.

    Args:
        thread_id: The conversation thread ID

    Returns:
        Dict with keys: text, status, thread_id, workspace_id
    """
    from src.server.database.conversation import (
        get_thread_by_id,
    )
    from src.server.services.workflow_tracker import WorkflowTracker

    # Look up thread
    thread = await get_thread_by_id(thread_id)
    if not thread:
        return {
            "text": "",
            "status": "not_found",
            "thread_id": thread_id,
            "workspace_id": "",
        }

    workspace_id = str(thread.get("workspace_id", ""))

    # Check workflow status
    tracker = WorkflowTracker.get_instance()
    status_info = await tracker.get_status(thread_id)

    if status_info:
        status = status_info.get("status", "unknown")
    else:
        status = thread.get("current_status", "unknown")

    # Determine if running (read from Redis) or completed (read from DB)
    active_statuses = {"running", "active", "streaming", "pending"}
    if status in active_statuses:
        text = await _extract_from_redis(thread_id)
    else:
        text = await _extract_from_db(thread_id)

    # Qualify relative file paths with workspace context so the flash
    # agent (and its frontend) can resolve them across workspaces.
    text = _qualify_file_paths(text, workspace_id)

    # Truncate if needed
    if len(text) > MAX_OUTPUT_CHARS:
        text = text[:MAX_OUTPUT_CHARS] + (
            "\n\n[truncated — full output available in workspace]"
        )

    return {
        "text": text,
        "status": status,
        "thread_id": thread_id,
        "workspace_id": workspace_id,
    }


async def _extract_from_redis(thread_id: str) -> str:
    """Extract text content from Redis SSE event buffer.

    Args:
        thread_id: The conversation thread ID

    Returns:
        Concatenated text content from message_chunk events
    """
    from src.utils.cache.redis_cache import get_cache_client

    try:
        cache = get_cache_client()
        raw_events = await cache.list_range(
            f"workflow:events:{thread_id}", start=-500, end=-1
        )
    except Exception as e:
        logger.error(f"Failed to read Redis events for thread {thread_id}: {e}")
        return ""

    chunks: list[str] = []
    for raw in raw_events:
        parsed = _parse_sse_string(raw)
        if parsed is None:
            continue
        event_type, data = parsed
        if (
            event_type == "message_chunk"
            and isinstance(data, dict)
            and data.get("content_type") == "text"
        ):
            content = data.get("content", "")
            if content:
                chunks.append(content)

    return "".join(chunks)


async def _extract_from_db(thread_id: str) -> str:
    """Extract text content from DB-persisted SSE events.

    Args:
        thread_id: The conversation thread ID

    Returns:
        Concatenated text content from message_chunk events
    """
    from src.server.database.conversation import get_responses_for_thread

    try:
        responses, _ = await get_responses_for_thread(thread_id, limit=10)
    except Exception as e:
        logger.error(f"Failed to read DB responses for thread {thread_id}: {e}")
        return ""

    chunks: list[str] = []
    for response in responses:
        sse_events = response.get("sse_events")
        if not sse_events:
            continue
        for event in sse_events:
            if not isinstance(event, dict):
                continue
            if event.get("event") != "message_chunk":
                continue
            data = event.get("data", {})
            if not isinstance(data, dict):
                continue
            if data.get("content_type") == "text":
                content = data.get("content", "")
                if content:
                    chunks.append(content)

    return "".join(chunks)
