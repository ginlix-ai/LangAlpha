"""Show interactive HTML/SVG widgets inline in the chat."""

import re
from typing import Any
from uuid import uuid4

import structlog
from langchain_core.tools import BaseTool, tool

logger = structlog.get_logger(__name__)

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


def create_show_widget_tool() -> BaseTool:
    """Factory function to create ShowWidget tool."""

    @tool(response_format="content_and_artifact")
    async def ShowWidget(
        html: str,
        title: str | None = None,
    ) -> tuple[str, dict[str, Any]]:
        """Render an interactive HTML/SVG widget inline in the chat.

        Use this to display charts, dashboards, data tables, or any interactive
        visualization directly in the conversation. The HTML is rendered in a
        sandboxed iframe with access to CDN libraries (Chart.js, D3, etc.).

        Available in the widget:
        - CDN libraries: cdnjs.cloudflare.com, cdn.jsdelivr.net, unpkg.com, esm.sh
        - CSS variables: var(--color-bg-page), var(--color-text-primary), etc. for theme matching
        - sendPrompt('text'): trigger a follow-up chat message from a button click

        Args:
            html: Raw HTML string to render. No DOCTYPE/html/head/body tags needed.
            title: Optional display title shown above the widget.

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

        artifact = {
            "type": "html_widget",
            "html": html,
            "title": display_title,
        }

        if writer:
            writer({
                "artifact_type": "html_widget",
                "artifact_id": widget_id,
                "payload": artifact,
            })

        logger.info("Rendered inline widget", widget_id=widget_id, title=display_title)

        content = f"Widget rendered: {display_title or widget_id}"
        return content, artifact

    return ShowWidget
