"""Show interactive HTML/SVG widgets inline in the chat."""

import asyncio
import base64
import hashlib
import json
import mimetypes
import os
import re
from typing import TYPE_CHECKING, Annotated, Any

import structlog
from langchain_core.tools import BaseTool, InjectedToolCallId

from src.tools.turn_tool import turn_tool
from src.utils.storage import is_storage_enabled, upload_bytes

from ..backends.sandbox import SandboxBackend
from ..middleware._utils import configurable

if TYPE_CHECKING:
    from src.server.services.widget_render import WidgetRenderReport

logger = structlog.get_logger(__name__)

# Max total size of a widget's resolved data. It is also the most a
# checkpointed artifact may carry inline, which it does whenever object
# storage cannot take the payload (disabled, or the upload failed).
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


def _kb(size: int) -> str:
    return f"{size / 1024:,.0f} KB" if size >= 1024 else f"{size} bytes"


class DataFilesRefused(Exception):
    """Some data_files cannot be delivered; ``problems`` names each, with sizes."""

    def __init__(self, problems: list[str]) -> None:
        super().__init__("; ".join(problems))
        self.problems = problems


async def _resolve_data_files(
    backend: SandboxBackend,
    data_files: list[str],
) -> dict[str, str]:
    """The inline data for *data_files*, keyed by file name.

    A widget missing a file it reads renders broken with nothing to say why,
    so a file that cannot be delivered refuses the call (``DataFilesRefused``)
    rather than being left out: every problem is named at once, with sizes, so
    one retry fixes them all. Text files stay strings; binary files become
    data URLs, counted at their base64 size against ``_INLINE_DATA_CAP``.
    """
    paths = [p for p in data_files if p and os.path.basename(p)]
    results = await asyncio.gather(*(_read_one_file(backend, p) for p in paths))

    #: file name -> (path, value, size in bytes)
    taken: dict[str, tuple[str, str, int]] = {}
    total = 0
    problems: list[str] = []

    for path, content in results:
        if content is None:
            problems.append(f"{path}: not found")
            continue
        filename = os.path.basename(path)
        if filename in taken:
            problems.append(
                f"{path}: same file name as {taken[filename][0]}, and the data is "
                "keyed by file name; copy one under another name"
            )
            continue
        is_text = _is_text_file(path)
        mime = mimetypes.guess_type(filename)[0] or (
            "text/plain" if is_text else "application/octet-stream"
        )

        if is_text:
            value = content  # type: ignore[assignment]
            # Python's json.dumps writes NaN/Infinity, which browser JSON.parse
            # rejects. Sanitizing shrinks text by at most 5/9 (`-Infinity` ->
            # `null`), so text past 9/4 of the remaining budget is over the cap
            # either way and skips the scan, whose per-token cost is ~20 bytes
            # of memory per input character on dense short-string JSON.
            if (
                os.path.splitext(path)[1].lower() in (".json", ".geojson", ".topojson")
                and len(value) * 4 <= max(_INLINE_DATA_CAP - total, 0) * 9
                and ("NaN" in value or "Infinity" in value)
            ):
                # Quoted strings match first so names and labels stay intact,
                # and no parse/dump round trip can alter numeric precision. The
                # closing quote is optional and a backslash escapes any
                # character, so the string branch never backtracks once it
                # starts: truncated JSON stays linear and byte-for-byte intact.
                # The possessive `*+` keeps the engine from holding
                # per-iteration backtrack state (~120 bytes per character of
                # the longest string with a plain `*`).
                value = re.sub(
                    r'"(?:\\[\s\S]|[^"\\])*+"?|(?P<constant>(?<!\w)(?:NaN|-?Infinity)\b)',
                    lambda token: "null" if token.group("constant") else token.group(0),
                    value,
                )
        else:
            value = f"data:{mime};base64,{base64.b64encode(content).decode()}"  # type: ignore[arg-type]

        size = len(value.encode())
        taken[filename] = (path, value, size)
        total += size

    if total > _INLINE_DATA_CAP:
        listed = ", ".join(f"{path} {_kb(size)}" for path, _, size in taken.values())
        problems.append(
            f"data_files come to {_kb(total)} ({listed}); they carry "
            f"{_kb(_INLINE_DATA_CAP)} in all, binary files counted as base64. "
            "Load large files by relative URL instead"
        )
    if problems:
        raise DataFilesRefused(problems)
    return {filename: value for filename, (_, value, _) in taken.items()}


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
    ``data_ref {key, sha256, size}``. The artifact is the only copy replay
    reads, so a failed upload inlines the data too, as a deployment without
    storage does: a checkpoint heavier by at most ``_INLINE_DATA_CAP`` beats a
    widget that reloads empty.
    """
    payload = json.dumps(resolved_data, ensure_ascii=False, sort_keys=True).encode()
    if len(payload) <= _INLINE_ARTIFACT_CAP or not is_storage_enabled():
        artifact["data"] = resolved_data
        return

    sha = hashlib.sha256(payload).hexdigest()
    key = widget_data_storage_key(configurable().get("thread_id") or "", sha)
    if await asyncio.to_thread(upload_bytes, key, payload, "application/json"):
        artifact["data_ref"] = {"key": key, "sha256": sha, "size": len(payload)}
        logger.debug("ShowWidget data uploaded", key=key, size=len(payload))
    else:
        logger.warning(
            "ShowWidget data upload failed; inlining it", key=key, size=len(payload)
        )
        artifact["data"] = resolved_data


async def _render(html: str, data: dict[str, str] | None) -> "WidgetRenderReport | None":
    """What the widget did in a headless browser, or None where none runs.

    The user already has the widget by now; this only tells the agent whether
    it works, so any failure here costs the report, never the widget.
    """
    # Imported here: the tool module stays importable without the server's
    # render stack loaded.
    from src.server.services.widget_render import WidgetRenderError, render_widget

    run = configurable()
    try:
        return await render_widget(
            html=html,
            data=data,
            workspace_id=run.get("workspace_id"),
            user_id=run.get("user_id"),
        )
    except WidgetRenderError as e:
        logger.info("ShowWidget render check unavailable", reason=type(e).__name__)
        return None
    except Exception:
        logger.warning("ShowWidget render check failed", exc_info=True)
        return None


def _listed(label: str, items: list[str], limit: int = 8) -> list[str]:
    if not items:
        return []
    lines = [f"{label}:"] + [f"  - {item}" for item in items[:limit]]
    if len(items) > limit:
        lines.append(f"  - and {len(items) - limit} more")
    return lines


def _describe(name: str, report: "WidgetRenderReport") -> str:
    """The render report as the agent reads it: problems first, then what
    rendered, so a broken widget is the first thing it sees."""
    seen = report.seen
    problems: list[str] = []
    problems += _listed("Errors", report.errors)
    problems += _listed("Failed to load", report.load_failed)
    problems += _listed("Blocked", report.blocked)
    if seen.images.broken:
        problems.append(f"{seen.images.broken} of {seen.images.total} images did not load.")
    if report.blank:
        problems.append("It looks blank: no text, image, canvas or SVG rendered.")
    if seen.fixed_elements:
        problems.append(
            f"{seen.fixed_elements} "
            + ("element uses" if seen.fixed_elements == 1 else "elements use")
            + " position: fixed, which sits against the widget's own frame, and that "
            "frame is only as tall as its in-flow content, so a fixed overlay is cut "
            "off. To enlarge an image, put data-zoom on the <img> instead."
        )
    if overflow := report.overflow:
        problems.append(
            f"Content is {overflow.content} px wide in a {overflow.frame} px frame, "
            "so it scrolls sideways."
        )
    if narrow := report.narrow_overflow:
        problems.append(
            f"On a phone ({narrow.frame} px) its content is {narrow.content} px wide, so it "
            "scrolls sideways there."
        )
    if report.hung:
        problems.append(
            "It never finished loading: a script may be stuck in a loop or "
            "blocking the page."
        )
    elif report.unsettled:
        problems.append(
            "It was still changing when the check stopped waiting, so the "
            "screenshot may show it part-drawn."
        )

    lines = [f"Shown to the user: {name}."]
    size = " x ".join("?" if n is None else str(n) for n in (report.width, report.height))
    size += " px"
    made = [
        f"{n} {one if n == 1 else many}"
        for n, one, many in (
            (seen.canvases, "canvas", "canvases"),
            (seen.svgs, "SVG", "SVGs"),
            (seen.images.total, "image", "images"),
        )
        if n
    ]
    lines.append(f"Rendered {size}" + (f" with {', '.join(made)}" if made else "") + ".")
    if seen.data_keys:
        lines.append(f"__WIDGET_DATA__ keys: {', '.join(seen.data_keys)}.")
    # Not a problem: the check caps what it loads and the user's browser does
    # not, so this is no reason for a new call.
    if report.too_large:
        lines.append("")
        lines += _listed(
            "Too large for this check to load, though they load for the user",
            report.too_large,
        )
    if problems:
        lines.append("")
        lines += problems
    if report.screenshot:
        lines.append("")
        lines.append("Below is how it looks in the light theme.")
    return "\n".join(lines)


def create_show_widget_tool(backend: SandboxBackend) -> BaseTool:
    """Factory function to create ShowWidget tool."""

    @turn_tool(response_format="content_and_artifact")
    async def ShowWidget(
        html: str,
        tool_call_id: Annotated[str, InjectedToolCallId],
        title: str | None = None,
        data_files: list[str] | None = None,
    ) -> tuple[str | list[dict[str, Any]], dict[str, Any]]:
        """Show an interactive HTML/SVG widget inline in the chat: a chart, table, dashboard, calculator or gallery the user can explore, or a choice the user answers by clicking.

        Read `.agents/skills/inline-widget/SKILL.md` before your first widget.

        A widget runs like a web page: it loads libraries from any CDN, public APIs, fonts and workers, and opens a file in this workspace at any size by its path relative to the workspace folder, as `<img src="charts/q3.png">` or `fetch("data/bars.json")`.

        Args:
            html: An HTML fragment, without <html>, <head> or <body>. Put <style> first, the content next and <script> last: the user sees it build as you write it, and scripts run once the call completes.
            title: Names the widget for its download and full-screen view, and labels the data it sends you. It is not shown above the widget.
            data_files: Workspace files to embed as `window.__WIDGET_DATA__["<file name>"]`: a text file as a string, any other file as a data: URL. Up to 500 KB in all, a binary file counted at its base64 size; past that the call is refused.

        Returns:
            A render report at 768 px wide, with a check at phone width (errors, failed loads, size, the __WIDGET_DATA__ keys received), and a screenshot in the light theme.

        The user sees the widget as soon as the call runs, so a fix is a new call, which shows a second widget.
        """
        if not html or not html.strip():
            return "Widget not shown: html is empty.", {}
        # Imported here: the runtime_context package imports the tools package.
        from ..middleware.runtime_context.surface import chat_app_name

        chat_app = chat_app_name(configurable().get("platform"))
        if chat_app:
            return (
                f"Widget not shown: this turn came from {chat_app}, which cannot "
                "show widgets. Send a chart as an image file with send_message instead."
            ), {}

        resolved: dict[str, str] = {}
        if data_files and backend is not None:
            try:
                resolved = await _resolve_data_files(backend, data_files)
            except DataFilesRefused as refused:
                logger.info("ShowWidget data_files refused", count=len(refused.problems))
                return (
                    "Widget not shown. Some data_files cannot be delivered:\n"
                    + "\n".join(f"  - {line}" for line in refused.problems)
                ), {}

        name = title or "widget"
        artifact: dict[str, Any] = {"type": "html_widget", "html": html, "title": title or ""}
        # Replay reads only the checkpointed artifact: large payloads go to
        # object storage as a content-addressed data_ref so state carries
        # pointers, not bulk data.
        if resolved:
            await _attach_widget_data(artifact, resolved)

        try:
            from langgraph.config import get_stream_writer

            writer = get_stream_writer()
        except Exception:
            writer = None
        if writer:
            # The call's own id, as replay uses, so the live widget replaces
            # the preview streamed from this call's arguments in place.
            writer({
                "artifact_type": "html_widget",
                "artifact_id": tool_call_id,
                "payload": {**artifact, "data": resolved} if resolved else artifact,
            })

        report = await _render(html, resolved or None)
        if report is None:
            return f"Shown to the user: {name}. Its rendering could not be checked, so there is no report or screenshot.", artifact

        text = _describe(name, report)
        if not report.screenshot:
            return text, artifact
        image = base64.b64encode(report.screenshot).decode()
        mime = report.media_type
        return [
            {"type": "text", "text": text},
            {"type": "image_url", "image_url": {"url": f"data:{mime};base64,{image}"}},
        ], artifact

    return ShowWidget
