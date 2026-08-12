"""Show interactive HTML/SVG widgets inline in the chat."""

import asyncio
import base64
import hashlib
import json
import mimetypes
import os
import re
from typing import Any
from uuid import uuid4

import structlog
from langchain_core.tools import BaseTool, tool

from src.utils.storage import is_storage_enabled, upload_bytes

from ..backends.sandbox import SandboxBackend

logger = structlog.get_logger(__name__)

# Max total size for inline-embedded data when cloud storage is unavailable.
_INLINE_DATA_CAP = 500 * 1024  # 500 KB

# Resolved data up to this size is inlined into the checkpointed artifact;
# larger payloads go to object storage as a content-addressed ref so graph
# state carries pointers, never bulk payloads.
_INLINE_ARTIFACT_CAP = 32 * 1024  # 32 KB

# Extensions treated as text (everything else is binary).
_TEXT_EXTENSIONS = frozenset({
    ".json", ".csv", ".txt", ".html", ".xml", ".svg",
    ".md", ".yaml", ".yml", ".tsv", ".geojson", ".topojson",
})


# ---------------------------------------------------------------------------
# Validation rules — each returns (violation_name, detail) or None
# ---------------------------------------------------------------------------

_RULES: list[tuple[str, re.Pattern[str], str]] = [
    (
        "ResizeObserver",
        re.compile(r"new\s+ResizeObserver\b", re.IGNORECASE),
        "Do not create ResizeObserver — the host handles iframe sizing automatically.",
    ),
    (
        "position:fixed",
        re.compile(r"position\s*:\s*fixed", re.IGNORECASE),
        "Do not use position:fixed — the iframe auto-sizes to content; fixed elements collapse to 0 height.",
    ),
    (
        "parent.postMessage",
        re.compile(r"parent\.postMessage\b"),
        "Do not call parent.postMessage directly — use the provided sendPrompt('text') global instead.",
    ),
    (
        "frame escape",
        re.compile(r"window\.(top|parent)\s*\."),
        "Do not access window.top or window.parent — the widget runs in a sandboxed iframe.",
    ),
]


def _detect_outer_wrapper_issues(html: str) -> list[str]:
    """Check if the outermost element has background, border, or border-radius.

    We parse the first opening tag's style attribute. This is intentionally
    simple — it only looks at the very first HTML element.
    """
    issues: list[str] = []
    # Find the first HTML tag with a style attribute
    m = re.search(r"<\w+[^>]*\sstyle\s*=\s*[\"']([^\"']*)[\"']", html, re.DOTALL)
    if not m:
        return issues
    style = m.group(1).lower()
    # Only flag if this is the first substantial tag (skip whitespace)
    prefix = html[: m.start()].strip()
    if prefix:
        return issues  # not the outermost element

    if re.search(r"(?<!-)background\s*:", style):
        bg_val = re.search(r"background\s*:\s*([^;]+)", style)
        if bg_val and "transparent" not in bg_val.group(1):
            issues.append(
                "Outermost element must NOT have a background — it must be transparent so the widget sits seamlessly on the chat surface."
            )
    if re.search(r"(?<![a-z-])border\s*:", style):
        border_val = re.search(r"(?<![a-z-])border\s*:\s*([^;]+)", style)
        if border_val and "none" not in border_val.group(1):
            issues.append(
                "Outermost element must NOT have a border — only inner cards/sections should have borders."
            )
    if re.search(r"border-radius\s*:", style):
        issues.append(
            "Outermost element must NOT have border-radius — only inner cards/sections should be rounded."
        )
    return issues


def _validate_html(html: str) -> list[str]:
    """Return a list of violation descriptions, empty if HTML is clean."""
    violations: list[str] = []
    for name, pattern, detail in _RULES:
        if pattern.search(html):
            violations.append(f"[{name}] {detail}")
    violations.extend(_detect_outer_wrapper_issues(html))
    return violations


# ---------------------------------------------------------------------------
# Guidance text sent back on validation failure
# ---------------------------------------------------------------------------

_SKILL_CONTENT_CACHE: str | None = None


def _load_widget_guidelines() -> str:
    """Load the inline-widget SKILL.md as the canonical guideline source."""
    global _SKILL_CONTENT_CACHE  # noqa: PLW0603
    if _SKILL_CONTENT_CACHE is not None:
        return _SKILL_CONTENT_CACHE

    try:
        from ptc_agent.agent.middleware.skills import load_skill_content

        content = load_skill_content("inline-widget")
        if content:
            _SKILL_CONTENT_CACHE = content
            return content
    except Exception:
        pass

    # Fallback: minimal inline rules if skill file can't be loaded
    # Don't cache the fallback — retry loading on next call
    return (
        "Outermost element: NO background/border/border-radius (transparent shell). "
        "Inner cards use var(--color-bg-card), 0.5px border. "
        "No ResizeObserver, no parent.postMessage, no position:fixed. "
        "Charts: wrap canvas in div with explicit height, use responsive:true, maintainAspectRatio:false."
    )


def _is_text_file(path: str) -> bool:
    """Return True if *path* should be read as text based on its extension."""
    _, ext = os.path.splitext(path)
    return ext.lower() in _TEXT_EXTENSIONS


async def _read_one_file(
    backend: SandboxBackend,
    path: str,
) -> tuple[str, str | bytes | None]:
    """Read a single file from the sandbox, returning (path, content_or_None)."""
    is_text = _is_text_file(path)
    try:
        if is_text:
            content = await backend.aread_text(path)
        else:
            content = await backend.adownload_file_bytes(path)
        if content is None:
            logger.warning("ShowWidget data_files: file not found", path=path)
        return path, content
    except Exception:
        logger.warning("ShowWidget data_files: failed to read", path=path, exc_info=True)
        return path, None


async def _resolve_data_files(
    backend: SandboxBackend,
    data_files: list[str],
) -> dict[str, str]:
    """Read *data_files* via *backend* and return inline data dict.

    Returns a mapping of filename → content string.  Text files are
    returned as raw strings; binary files as ``data:{mime};base64,...``
    data-URLs.  A cumulative size cap of ``_INLINE_DATA_CAP`` is enforced.

    All sandbox reads are dispatched concurrently via ``asyncio.gather``
    to avoid sequential-await latency.
    """
    import asyncio

    # Filter out empty basenames before issuing I/O
    valid_paths = [p for p in data_files if os.path.basename(p)]
    if not valid_paths:
        return {}

    # Read all files concurrently
    results = await asyncio.gather(
        *(_read_one_file(backend, p) for p in valid_paths)
    )

    # Post-process: encode and apply size cap (order-preserving)
    inline_data: dict[str, str] = {}
    inline_total = 0
    seen_basenames: set[str] = set()

    for path, content in results:
        if content is None:
            continue
        filename = os.path.basename(path)
        is_text = _is_text_file(path)

        if filename in seen_basenames:
            logger.warning(
                "ShowWidget data_files: duplicate basename, skipping",
                path=path,
                basename=filename,
            )
            continue
        seen_basenames.add(filename)

        mime = mimetypes.guess_type(filename)[0] or (
            "text/plain" if is_text else "application/octet-stream"
        )

        # Build inline value
        if is_text:
            value = content  # type: ignore[assignment]
            # Sanitize non-standard JSON tokens (NaN/Infinity) that Python's
            # json.dumps emits by default — these break browser JSON.parse.
            _, ext = os.path.splitext(path)
            if ext.lower() in ('.json', '.geojson', '.topojson'):
                value = re.sub(r'\bNaN\b', 'null', value)
                value = re.sub(r'(?<![A-Za-z_])-?Infinity\b', 'null', value)
        else:
            b64 = base64.b64encode(content).decode()  # type: ignore[arg-type]
            value = f"data:{mime};base64,{b64}"

        entry_size = len(value.encode())
        if inline_total + entry_size > _INLINE_DATA_CAP:
            logger.warning(
                "ShowWidget data_files: inline cap exceeded, skipping",
                path=path,
                cap=_INLINE_DATA_CAP,
                current=inline_total,
            )
            continue
        inline_total += entry_size
        inline_data[filename] = value

    return inline_data


def widget_data_storage_key(thread_id: str, sha_hex: str) -> str:
    """Content-addressed storage key for resolved widget data."""
    prefix = f"widgets/{thread_id}" if thread_id else "widgets"
    return f"{prefix}/{sha_hex}.json"


async def _attach_widget_data(
    artifact: dict[str, Any], resolved_data: dict[str, str]
) -> None:
    """Make resolved data recoverable from the checkpointed artifact.

    Small payloads (and everything when storage is disabled) inline as
    ``data``; larger ones upload content-addressed and attach
    ``data_ref {key, sha256, size}``. On upload failure the artifact carries
    neither — replay then falls back to the stored-event merge.
    """
    payload = json.dumps(resolved_data, ensure_ascii=False, sort_keys=True).encode()
    if len(payload) <= _INLINE_ARTIFACT_CAP or not is_storage_enabled():
        artifact["data"] = resolved_data
        return

    sha = hashlib.sha256(payload).hexdigest()
    try:
        from langgraph.config import get_config

        thread_id = (get_config().get("configurable") or {}).get("thread_id") or ""
    except Exception:
        thread_id = ""
    key = widget_data_storage_key(thread_id, sha)
    if await asyncio.to_thread(upload_bytes, key, payload, "application/json"):
        artifact["data_ref"] = {"key": key, "sha256": sha, "size": len(payload)}
        logger.debug("ShowWidget data uploaded", key=key, size=len(payload))
    else:
        logger.warning("ShowWidget data upload failed", key=key, size=len(payload))


def create_show_widget_tool(backend: SandboxBackend) -> BaseTool:
    """Factory function to create ShowWidget tool."""

    @tool(response_format="content_and_artifact")
    async def ShowWidget(
        html: str,
        title: str | None = None,
        data_files: list[str] | None = None,
    ) -> tuple[str, dict[str, Any]]:
        """Render an interactive HTML/SVG widget inline in the chat.

        Use this to display charts, dashboards, data tables, or any interactive
        visualization directly in the conversation. The HTML is rendered in a
        sandboxed iframe with access to CDN libraries (Chart.js, D3, etc.).

        Available in the widget:
        - CDN libraries: cdnjs.cloudflare.com, cdn.jsdelivr.net, unpkg.com, esm.sh
        - CSS variables: var(--color-bg-page), var(--color-text-primary), etc. for theme matching
        - sendPrompt('text'): trigger a follow-up chat message from a button click
        - window.__WIDGET_DATA__: dict of filename→content for files passed via data_files

        Args:
            html: Raw HTML string to render. No DOCTYPE/html/head/body tags needed.
            title: Optional display title shown above the widget.
            data_files: Optional list of sandbox file paths whose contents will be
                made available in the widget as ``window.__WIDGET_DATA__["filename"]``.
                Text files (json/csv/txt/…) are strings; binary files (png/jpg/…)
                become data-URL strings.

        Returns:
            Confirmation message and artifact dict for inline rendering.
        """
        # Validate HTML before rendering
        violations = _validate_html(html)
        if violations:
            error_lines = "\n".join(f"  - {v}" for v in violations)
            msg = (
                f"Widget HTML rejected — fix the following issues and call ShowWidget again:\n"
                f"{error_lines}\n\n{_load_widget_guidelines()}"
            )
            logger.warning(
                "ShowWidget HTML rejected",
                violations=[v.split("]")[0].strip("[") for v in violations],
            )
            return msg, {}

        try:
            from langgraph.config import get_stream_writer

            writer = get_stream_writer()
        except Exception:
            writer = None

        widget_id = f"widget_{uuid4().hex[:8]}"
        display_title = title or ""

        artifact: dict[str, Any] = {
            "type": "html_widget",
            "html": html,
            "title": display_title,
        }

        # Resolve data files. Small payloads inline into the checkpointed
        # artifact; large ones persist to object storage as a content-addressed
        # data_ref — state carries pointers, not bulk payloads, and replay can
        # recover the data without the stored SSE stream.
        resolved_data: dict[str, str] | None = None
        if data_files and backend is not None:
            resolved_data = await _resolve_data_files(backend, data_files) or None
        if resolved_data:
            await _attach_widget_data(artifact, resolved_data)

        if writer:
            stream_payload = artifact
            if resolved_data:
                stream_payload = {**artifact, "data": resolved_data}
            writer({
                "artifact_type": "html_widget",
                "artifact_id": widget_id,
                "payload": stream_payload,
            })

        logger.debug("Rendered inline widget", widget_id=widget_id, title=display_title)

        content = f"Widget rendered: {display_title or widget_id}"
        return content, artifact

    return ShowWidget
