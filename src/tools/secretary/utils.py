"""Utility functions for secretary tools."""

import json
import logging
import re
from typing import Any

from ptc_agent.core.paths import SANDBOX_ROOTS

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

# Destinations worth rewriting into a workspace reference. Deliberately
# narrower than the web client, which treats any relative link as a file
# (`filePaths.isFilePath`) and leaves the panel to resolve the name: the relay
# is rewriting text it will persist, so it only claims destinations that look
# like files. Every extension the client's own tables name as one belongs here
# -- `filePaths.KIND_BY_EXT` (what a deliverable card can show) and
# `filePanel/fileMeta` (what the panel lists and offers to download) -- because
# a deliverable that relays unqualified resolves against the Flash workspace,
# which holds none of these files.
_FILE_EXTS = (
    r"md|markdown|txt|pdf|doc|docx|odt|rtf|"
    r"py|js|jsx|ts|tsx|html|htm|css|sh|bash|sql|r|rb|go|rs|java|ipynb|pptx|ppt|key|"
    r"csv|tsv|json|jsonl|yaml|yml|xml|toml|ini|cfg|log|env|xlsx|xlsm|xls|"
    r"png|jpg|jpeg|gif|svg|webp|bmp|"
    r"numbers|pages|"
    r"parquet|feather|pkl|pickle|npy|npz|h5|hdf5|db|sqlite|"
    r"mp3|wav|mp4|mov|webm|"
    r"zip|tar|tgz|gz|bz2|xz|7z|rar"
)

# Workspace-qualified path prefix: __wsref__/{workspace_id}/relative/path
# Uses a path-based encoding instead of ws:// protocol to survive HTML sanitizers.
_WSREF_PREFIX = "__wsref__"

# A markdown link or image and its destination, bare or in angle brackets.
# A bare destination carries one level of balanced parens, as CommonMark says and
# the client's own `normalizeFileRefs.LINK_DEST_RE` already reads -- `report(1).pdf`
# is a real deliverable name. A title or a URL fails the file test below.
# A bare destination holds no angle bracket either, so an unclosed `<a.jsonl`
# fails to match rather than matching bare and losing its last character to the
# closing-bracket strip -- `.jsonl` shortened to `.json` is another live
# extension, so that link would have resolved to a different real file.
# The label is bounded because it is unanchored: on a run of `[` with no `]`,
# an unbounded `[^\]\n]*` rescans to the end of the line from every one of
# them, and this runs synchronously on a whole turn of agent text inside an
# async handler. 512 is past any real link label.
# A destination may carry a CommonMark title, which is not part of the path. The
# title cannot be found by cutting at the first space, because a bare
# `results/Q3 deck.pptx` is a name the agent really writes and the bracketed form
# above exists to keep it: the quote or paren is the only signal. So the
# destination stops as early as it can and the title rides in the closing group,
# which `_rewrite` already emits untouched. Left unqualified, a titled link is
# not safely ignored the way a URL is -- the parser hands the client the bare
# path, which then resolves against the Flash workspace and opens nothing.
_DEST = r"<[^<>\n]+>|(?:[^()<>\n]|\([^()<>\n]*\))+?"
_TITLE = r"""(?:[ \t]+(?:"[^"\n]*"|'[^'\n]*'|\([^()\n]*\)))?"""
_MD_LINK_RE = re.compile(r"(!?\[[^\]\n]{0,512}\]\()(" + _DEST + r")(" + _TITLE + r"\))")

# Fenced blocks and inline code spans: shown, not read. A link inside one is
# syntax the reply is displaying, so splicing a workspace id into it corrupts
# what the reader copies out, and this text is persisted, so the corruption
# outlives the turn. The web client splits the same way before its own rewrites
# (`markdownSegments.mapOutsideCode`).
# A closing fence only has to be *at least* as long as the opener, so the
# closer is the opener plus any further run of its own character. Repeating
# the whole opener instead would accept only multiples of its length, and a
# three-backtick block closed by four would swallow the rest of the turn.
# An inline span is delimited by a whole run of backticks and closes on the
# next run of the same length, which is why both delimiters are fenced off by
# lookarounds: a run is only a delimiter if nothing longer contains it. Reading
# one backtick at a time instead paired the inner run of `` `[r](a.md)` `` with
# itself and handed the link in between to the rewrite, so an agent showing the
# citation syntax got a workspace id spliced into the example it was showing.
_CODE_RE = re.compile(
    r"^[ \t]*(?P<fence>(?P<fchar>[`~])(?P=fchar){2,})[^\n]*\n"
    r".*?(?:^[ \t]*(?P=fence)(?P=fchar)*[ \t]*$|\Z)"
    r"|(?<!`)(?P<tick>`+)(?!`)[^\n]*?(?<!`)(?P=tick)(?!`)",
    re.MULTILINE | re.DOTALL,
)


def _map_outside_code(text: str, rewrite) -> str:
    """Apply `rewrite` to the prose of `text`, leaving code spans untouched."""
    out: list[str] = []
    last = 0
    for m in _CODE_RE.finditer(text):
        out.append(rewrite(text[last : m.start()]))
        out.append(m.group(0))
        last = m.end()
    out.append(rewrite(text[last:]))
    return "".join(out)

# Path separators, encoded or not: `..%2f..%2f` climbs exactly as `../../` does.
_SEP_RE = re.compile(r"/|%2[fF]")

# The place inside a file a reference points at, in its `:line` forms:
# :line, :start-end, :line:col. The `#fragment` and `?query` forms are read by
# `_split_location`.
_LINE_LOCATION_RE = re.compile(r":\d+(?:-\d+|:\d+)?$")
# The computer root in every spelling, current and legacy, with or without a
# `file://` scheme.
_SANDBOX_ROOT_RE = re.compile(
    "^(?:file://)?(?:" + "|".join(re.escape(r) for r in SANDBOX_ROOTS) + ")/",
    re.IGNORECASE,
)
_BARE_DOMAIN_RE = re.compile(r"^www\.", re.IGNORECASE)
_FILE_NAME_RE = re.compile(r"\.(?:" + _FILE_EXTS + r")$", re.IGNORECASE)
# A colon introduces a scheme only before the first slash, which is the rule
# every URL reader on the client uses too (react-markdown's `defaultUrlTransform`
# and `hast-util-sanitize`). Rejecting every colon also rejected
# `results/Q3:final.pdf`, a name the sandbox allows and the resolver accepts, so
# the link relayed unqualified and opened nothing in the Flash workspace.
_SCHEME_RE = re.compile(r"^[^/]*:")


def _split_location(path: str) -> tuple[str, str]:
    """A reference split into the file and everything trailing it.

    A destination is a URL, so `#` opens the fragment and `?` the query, each
    at its first occurrence. That is the reading `agentPaths.normalizeAgentHref`
    does on the other side of the wire, and a name carrying either character
    travels percent-encoded, so `results/issue%231.md` arrives here whole and
    the client decodes it after the split. Reading a literal `#` as part of a
    name instead would make this the only reader in the chain that disagrees
    with the markdown its own client renders.

    Whitespace marks nothing, which is the half worth spelling out: the panel's
    `findHeadingIndex` matches a heading as written and not only as a slug, so
    `report.md#Valuation Assumptions` is a reference it opens. Cutting at the
    space left a head ending in `Assumptions`, which is no file, and the link
    relayed unqualified into a workspace that does not hold it.
    """
    tail = ""
    # `#` first: everything after it is fragment, a `?` inside it included, so
    # the query pass then reads only what really precedes the fragment.
    for mark in ("#", "?"):
        cut = path.find(mark)
        if cut != -1:
            path, tail = path[:cut], path[cut:] + tail
    line = _LINE_LOCATION_RE.search(path)
    return (path[: line.start()], line.group(0) + tail) if line else (path, tail)


def _qualify_file_paths(text: str, workspace_id: str) -> str:
    """Rewrite workspace file links to __wsref__/{workspace_id}/path.

    Transforms:
        [report.md](t/report.md) → [report.md](__wsref__/{wid}/t/report.md)
        [model](./model.py:42)        → [model](__wsref__/{wid}/model.py:42)
        [deck](<t/Q3 deck.pptx>) → [deck](<__wsref__/{wid}/t/Q3 deck.pptx>)

    The relayed text renders in the Flash thread, whose own workspace holds none
    of these files, so every relative file link has to carry where it lives,
    bare names included. A path-based prefix survives HTML sanitizers that strip
    unknown protocols. URLs, in-page anchors, paths outside the workspace and
    already-qualified links are left as written.
    """
    if not workspace_id or not text:
        return text

    def _rewrite(m: re.Match) -> str:
        prefix, dest, suffix = m.group(1), m.group(2), m.group(3)
        bracketed = dest.startswith("<")
        # `file_refs.clean_path` folds separators before it judges a path, so
        # the guards below read the same string the resolver will, or the relay
        # mints a reference that resolver then refuses.
        path = (dest[1:-1] if bracketed else dest).strip().replace("\\", "/")
        path, tail = _split_location(path)
        path = _SANDBOX_ROOT_RE.sub("", path)
        while path.startswith("./"):
            path = path[2:]
        if (
            not path
            or path.startswith(("/", "#", f"{_WSREF_PREFIX}/"))
            or _SCHEME_RE.match(path)
            # A destination that climbs out of the working directory names no
            # workspace file, and `file_refs.clean_path` refuses one, so
            # qualifying it would mint a reference the resolver rejects.
            or ".." in _SEP_RE.split(path)
            # A scheme-less bare domain is a link out, not a file. The client
            # says so itself (`filePaths.isFilePath`), but reads the `__wsref__`
            # prefix first, so a qualified one arrives already claimed.
            or _BARE_DOMAIN_RE.match(path)
            or not _FILE_NAME_RE.search(path)
        ):
            return m.group(0)
        qualified = f"{_WSREF_PREFIX}/{workspace_id}/{path}{tail}"
        if bracketed or any(c.isspace() for c in qualified):
            qualified = f"<{qualified}>"
        return f"{prefix}{qualified}{suffix}"

    return _map_outside_code(text, lambda prose: _MD_LINK_RE.sub(_rewrite, prose))


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
