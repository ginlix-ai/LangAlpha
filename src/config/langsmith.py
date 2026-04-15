"""LangSmith client with trace-size filtering.

Strips large base64 payloads (images, PDFs) from trace inputs/outputs
before upload, replacing them with ``[mime: size]`` placeholders.

The ``Client`` uses the ``anonymizer`` hook (not ``hide_inputs`` /
``hide_outputs``) because it JSON-roundtrips data to plain dicts first,
which is necessary since LangGraph passes LangChain message objects in
run inputs/outputs.  The ``Client`` is a process-wide singleton.

Env vars:
- ``LANGSMITH_TRACE_FILTER`` — toggle (default ``true``).
- ``LANGSMITH_TRACE_FILTER_MIN_BYTES`` — byte threshold (default 1000).
"""

import logging
import os
import re
import threading
from typing import Any, Optional

# Matches "data:<mime>;base64," prefix — group 1 captures the MIME type.
_DATA_URL_RE = re.compile(
    r"data:([A-Za-z0-9][A-Za-z0-9!#$&\-^_.+]*/[A-Za-z0-9][A-Za-z0-9!#$&\-^_.+]*)"
    r";base64,"
)
_BASE64_RE = re.compile(r"[A-Za-z0-9+/=]+\Z")
_DEFAULT_MIN_BYTES = 1_000
_client_lock = threading.Lock()
_cached_client: Optional[Any] = None
_cached_project: Optional[str] = None
_min_bytes_cache: Optional[int] = None

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Config helpers
# ---------------------------------------------------------------------------

def _is_trace_filter_enabled() -> bool:
    return os.environ.get("LANGSMITH_TRACE_FILTER", "true").lower() != "false"


def _get_min_bytes() -> int:
    global _min_bytes_cache
    if _min_bytes_cache is not None:
        return _min_bytes_cache
    raw = os.environ.get("LANGSMITH_TRACE_FILTER_MIN_BYTES", "")
    try:
        _min_bytes_cache = int(raw)
    except (ValueError, TypeError):
        _min_bytes_cache = _DEFAULT_MIN_BYTES
    return _min_bytes_cache


def _format_size(n: int) -> str:
    if n >= 1_000_000:
        return f"{n / 1_000_000:.1f}MB"
    if n >= 1_000:
        return f"{n / 1_000:.0f}KB"
    return f"{n}B"


def _looks_like_base64(s: str) -> bool:
    """True when *s* is ≥ 200 chars drawn exclusively from the base64 alphabet."""
    return len(s) >= 200 and _BASE64_RE.fullmatch(s) is not None


# ---------------------------------------------------------------------------
# Core filter
# ---------------------------------------------------------------------------

def strip_base64_from_trace(data: Any) -> Any:
    """Replace large base64 strings with ``[mime: size]`` placeholders.

    Works on plain dicts (the ``anonymizer`` hook JSON-roundtrips before
    calling us).  Two detection paths cover every known content-block format:

    * **Data-URL strings** — ``data:<mime>;base64,<payload>``.
    * **Raw base64 strings** — long string of ``[A-Za-z0-9+/=]`` only.

    A MIME hint is derived from the parent dict's ``mime_type`` or
    ``media_type`` key when available; otherwise falls back to ``binary``.
    """
    min_bytes = _get_min_bytes()

    def _strip(node: Any, mime_hint: str = "binary") -> Any:
        if isinstance(node, str):
            # Path 1 — data URL: data:<mime>;base64,<payload>
            m = _DATA_URL_RE.match(node)
            if m:
                decoded_size = (len(node) - m.end()) * 3 // 4
                if decoded_size >= min_bytes:
                    return f"[{m.group(1)}: {_format_size(decoded_size)}]"
                return node
            # Path 2 — raw base64 (e.g. PDF "base64" field, Anthropic "data" field)
            if _looks_like_base64(node):
                decoded_size = len(node) * 3 // 4
                if decoded_size >= min_bytes:
                    return f"[{mime_hint}: {_format_size(decoded_size)}]"
            return node

        if isinstance(node, dict):
            hint = node.get("mime_type") or node.get("media_type") or mime_hint
            return {k: _strip(v, hint) for k, v in node.items()}

        if isinstance(node, list):
            return [_strip(item, mime_hint) for item in node]

        return node

    try:
        return _strip(data)
    except Exception:
        logger.warning("Failed to filter trace data, sending unfiltered", exc_info=True)
        return data


# ---------------------------------------------------------------------------
# Client / tracer
# ---------------------------------------------------------------------------

def _get_or_create_client() -> Any:
    """Return the singleton ``langsmith.Client`` with the base64 anonymizer."""
    global _cached_client
    if _cached_client is not None:
        return _cached_client
    with _client_lock:
        if _cached_client is not None:
            return _cached_client
        from langsmith import Client

        _cached_client = Client(anonymizer=strip_base64_from_trace)
        return _cached_client


def get_filtered_langsmith_tracer() -> Optional[Any]:
    """Return a ``LangChainTracer`` that strips base64 from traces.

    Returns ``None`` when ``LANGSMITH_TRACING`` env var is not ``true``
    or when filtering is disabled via ``LANGSMITH_TRACE_FILTER=false``.
    When filtering is off but tracing is on, the SDK auto-tracer still
    runs (unfiltered), so returning ``None`` is correct.

    The client and project name are cached at module level. A fresh tracer
    is returned each call because ``LangChainTracer`` holds mutable per-run
    state (``run_map``, ``order_map``, ``latest_run``) that must not be
    shared across concurrent workflows.
    """
    global _cached_project
    if os.environ.get("LANGSMITH_TRACING", "").lower() != "true":
        return None

    if not _is_trace_filter_enabled():
        return None

    from langchain_core.tracers import LangChainTracer

    client = _get_or_create_client()
    if _cached_project is None:
        _cached_project = os.environ.get("LANGSMITH_PROJECT", "langalpha")
    return LangChainTracer(client=client, project_name=_cached_project)
