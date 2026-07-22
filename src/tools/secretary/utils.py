"""Utility functions for secretary tools."""

import json
import logging
import re
from typing import Any

logger = logging.getLogger(__name__)

MAX_OUTPUT_CHARS = 8000

# Ceiling on how many turns a single read pulls from the DB (output is
# truncated to MAX_OUTPUT_CHARS regardless).
_MAX_HISTORY_TURNS = 50

# When the default single-turn read lands on a text-less newest turn (tool-only
# / chart-only), look back this many turns for the most-recent turn with text.
_EMPTY_LATEST_FALLBACK_TURNS = 5

# Inserted between turns when more than one turn is returned.
_TURN_SEPARATOR = "\n\n---\n\n"

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
    r"((?!https?://|mailto:|#|__wsref__/|[a-zA-Z][a-zA-Z0-9+.-]*:)"  # not URL scheme or already qualified
    r"(?:/home/(?:workspace|daytona)/)?[a-zA-Z_][^\s)]*/"  # at least one dir segment
    r"[^\s)]*\.(?:" + _FILE_EXTS + r"))"  # filename.ext
    r"(\))",  # closing paren
    re.IGNORECASE,
)

# Strip file:// protocol from sandbox paths in markdown links before qualification.
_FILE_PROTO_RE = re.compile(
    r"(!?\[[^\]]*\]\()"  # prefix: ![...]( or [...](
    r"file:///home/(?:workspace|daytona)/",  # file:///home/workspace/ or /daytona/
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

    # Normalize file:///home/workspace/... → relative path in markdown links
    text = _FILE_PROTO_RE.sub(r"\1", text)

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


def _truncate_single(text: str) -> str:
    """Head-truncate one turn's text to ``MAX_OUTPUT_CHARS`` (keeps the start)."""
    if len(text) <= MAX_OUTPUT_CHARS:
        return text
    return text[:MAX_OUTPUT_CHARS] + (
        "\n\n[truncated — full output available in workspace]"
    )


def _join_recent_turns(turn_texts: list[str]) -> str:
    """Join turn texts oldest -> newest, capped at ``MAX_OUTPUT_CHARS``.

    Turn boundaries are known from the list (not rediscovered by scanning the
    joined text), so a turn whose own markdown contains ``---`` is never
    mistaken for a separator. When the cap is exceeded, whole older turns are
    dropped from the front so the most-recent turns survive; the newest turn is
    head-truncated if it alone overflows. A banner notes any dropped turns.
    """
    turn_texts = [t for t in turn_texts if t]
    if not turn_texts:
        return ""

    joined = _TURN_SEPARATOR.join(turn_texts)
    if len(joined) <= MAX_OUTPUT_CHARS:
        return joined

    # Keep the newest turns that fit, always retaining at least the newest one.
    kept: list[str] = []
    total = 0
    for text in reversed(turn_texts):
        extra = len(text) + (len(_TURN_SEPARATOR) if kept else 0)
        if kept and total + extra > MAX_OUTPUT_CHARS:
            break
        kept.append(text)
        total += extra
    kept.reverse()

    body = _truncate_single(_TURN_SEPARATOR.join(kept))
    if len(kept) < len(turn_texts):
        body = (
            "[earlier turns truncated — full output available in workspace]\n\n"
            + body
        )
    return body


async def extract_text_from_thread(
    thread_id: str, turns: int = 1
) -> dict[str, Any]:
    """Extract text content from a thread's SSE events.

    Reads from Redis if the thread is actively running, otherwise reads
    from the database. Filters for message_chunk events with text content.

    Args:
        thread_id: The conversation thread ID
        turns: How many of the most-recent turns to include. 1 (default) =
            only the latest turn; N > 1 = the last N turns; <= 0 = the full
            thread history. The window applies to the persisted record; while
            a turn is actively streaming, only that live turn is returned.

    Returns:
        Dict with keys: text, status, thread_id, workspace_id
    """
    from src.server.database.conversation.threads_read import (
        get_thread_by_id,
    )

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

    # The ledger routes live-vs-settled (v4 2.4): an in_progress row means
    # the turn is still streaming into Redis on SOME worker; anything else
    # reads the finalized turns from the DB.
    from src.server.database.runs import lifecycle as tl_db

    active_run = await tl_db.get_active_run(thread_id)
    if active_run is not None:
        status = "running"
    else:
        status = thread.get("current_status", "unknown")

    # Qualify relative file paths with workspace context so the flash agent
    # (and its frontend) can resolve them across workspaces, then cap length.
    if active_run is not None:
        # The active stream is always a single live turn — read the ledger
        # row's run stream directly. Resolving through local BTM state can
        # pick a retained terminal LocalRunExecution from a prior run on this worker
        # while the live run executes elsewhere (v4 2.4c review F6).
        text = await _extract_from_redis(
            thread_id, str(active_run["conversation_response_id"])
        )
        text = _truncate_single(_qualify_file_paths(text, workspace_id))
    else:
        turn_texts = await _extract_from_db(thread_id, turns)
        turn_texts = [_qualify_file_paths(t, workspace_id) for t in turn_texts]
        text = _join_recent_turns(turn_texts)

    return {
        "text": text,
        "status": status,
        "thread_id": thread_id,
        "workspace_id": workspace_id,
    }


async def _extract_from_redis(thread_id: str, run_id: str) -> str:
    """Extract text content from Redis SSE event buffer.

    Reads the tail of the per-run Redis Stream
    (``workflow:stream:{tid}:{run_id}``) for the caller-resolved *run_id*
    (the ledger-active row — authoritative on every worker). XREVRANGE
    with COUNT yields the most-recent 500 entries cheaply, then we
    reverse to chronological order to mirror the old RPUSH semantics.
    """
    # Local import to avoid load-order coupling with the server package
    # at agent import time.
    from src.server.services.runs.stream_writer import stream_key
    from src.utils.cache.redis_cache import get_cache_client

    key = stream_key(thread_id, run_id)

    try:
        cache = get_cache_client()
        if not getattr(cache, "enabled", False) or cache.client is None:
            return ""
        entries = await cache.client.xrevrange(key, count=500)
    except Exception as e:
        logger.error(f"Failed to read Redis events for thread {thread_id}: {e}")
        return ""

    chunks: list[str] = []
    # XREVRANGE returns newest first; reverse so chunks concatenate in order.
    for _entry_id, fields in reversed(entries or []):
        raw = fields.get(b"event")
        if raw is None:
            continue
        try:
            raw_str = raw.decode("utf-8") if isinstance(raw, bytes) else raw
        except UnicodeDecodeError:
            continue
        parsed = _parse_sse_string(raw_str)
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


def _text_from_response(response: dict[str, Any]) -> str:
    """Concatenate the text of one turn's ``message_chunk`` SSE events."""
    chunks: list[str] = []
    for event in response.get("sse_events") or []:
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


async def _latest_turn_text(thread_id: str) -> list[str]:
    """Newest turn's text, as ``[]`` or ``[text]``.

    Hot path is ``limit=1``; a text-less newest turn (tool-only / chart-only)
    pays one wider ``_EMPTY_LATEST_FALLBACK_TURNS`` read so an empty result
    isn't mistaken for "the agent produced nothing".
    """
    from src.server.database.conversation.responses import get_recent_responses_for_thread

    responses = await get_recent_responses_for_thread(thread_id, limit=1)
    if responses and (text := _text_from_response(responses[0])):
        return [text]

    # No turns at all: nothing to widen to.
    if not responses:
        return []

    # Newest turn is text-less: re-read the fallback window once and surface the
    # most-recent turn that carries text.
    responses = await get_recent_responses_for_thread(
        thread_id, limit=_EMPTY_LATEST_FALLBACK_TURNS
    )
    for text in map(_text_from_response, reversed(responses)):
        if text:
            return [text]
    return []


async def _extract_from_db(thread_id: str, turns: int = 1) -> list[str]:
    """Return per-turn text (oldest -> newest) for the most-recent ``turns`` turns.

    ``turns == 1`` delegates to ``_latest_turn_text``; otherwise reads the last
    ``turns`` turns (``<= 0`` = recent history), clamped to ``_MAX_HISTORY_TURNS``.
    Read failures propagate so the caller surfaces an error instead of an empty,
    success-looking result.
    """
    if turns == 1:
        return await _latest_turn_text(thread_id)

    from src.server.database.conversation.responses import get_recent_responses_for_thread

    limit = _MAX_HISTORY_TURNS if turns <= 0 else min(turns, _MAX_HISTORY_TURNS)
    responses = await get_recent_responses_for_thread(thread_id, limit=limit)
    return [text for text in map(_text_from_response, responses) if text]
