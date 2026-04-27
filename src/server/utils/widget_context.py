"""
Widget context utilities for chat endpoint.

Parses WidgetContext items from additional_context and builds a single
``<system-reminder>`` block that concatenates each widget's pre-rendered
``<widget-context>...</widget-context>`` text. Mirrors the directive context
middleware shape so widgets and directives share the same injection path.
"""

import logging
from datetime import datetime
from typing import Any, List, Optional

from src.server.models.additional_context import WidgetContext

logger = logging.getLogger(__name__)


def parse_widget_contexts(
    additional_context: Optional[List[Any]],
) -> List[WidgetContext]:
    """Extract WidgetContext items from additional_context list."""
    if not additional_context:
        return []

    contexts: List[WidgetContext] = []

    for ctx in additional_context:
        if isinstance(ctx, dict):
            if ctx.get("type") == "widget":
                contexts.append(_from_dict(ctx))
        elif isinstance(ctx, WidgetContext):
            contexts.append(ctx)
        elif hasattr(ctx, "type") and ctx.type == "widget":
            contexts.append(
                WidgetContext(
                    type="widget",
                    widget_type=getattr(ctx, "widget_type", ""),
                    widget_id=getattr(ctx, "widget_id", ""),
                    label=getattr(ctx, "label", ""),
                    text=getattr(ctx, "text", ""),
                    data=getattr(ctx, "data", {}) or {},
                    captured_at=getattr(ctx, "captured_at", None),
                    description=getattr(ctx, "description", None),
                )
            )

    return contexts


def _from_dict(ctx: dict) -> WidgetContext:
    captured_raw = ctx.get("captured_at")
    captured: Optional[datetime] = None
    if isinstance(captured_raw, datetime):
        captured = captured_raw
    elif isinstance(captured_raw, str) and captured_raw:
        try:
            captured = datetime.fromisoformat(captured_raw.replace("Z", "+00:00"))
        except ValueError:
            captured = None

    return WidgetContext(
        type="widget",
        widget_type=ctx.get("widget_type", ""),
        widget_id=ctx.get("widget_id", ""),
        label=ctx.get("label", ""),
        text=ctx.get("text", ""),
        data=ctx.get("data") or {},
        captured_at=captured,
        description=ctx.get("description"),
    )


_WIDGET_CONTEXT_PREAMBLE = (
    "The user attached the following dashboard widget snapshot(s) to this turn "
    "via the \"+ to context\" button. Each <widget-context> block below is a "
    "point-in-time view of what the user was looking at when they sent this "
    "message. Evaluate whether each is relevant or helpful for the user's task "
    "before relying on it — some may be load-bearing context, others incidental. "
    "Don't force relevance where none exists."
)


def build_widget_context_reminder(widgets: List[WidgetContext]) -> Optional[str]:
    """Build a system-reminder block from widget contexts.

    Concatenates each widget's pre-rendered ``<widget-context>`` text into one
    ``<system-reminder>`` envelope, prefixed by an explainer so the agent
    knows the blocks are user-attached dashboard snapshots and should be
    evaluated for relevance rather than blindly trusted. Returns ``None`` when
    there is nothing to inject so the caller can skip the append step entirely.
    """
    if not widgets:
        return None

    parts = [w.text.strip() for w in widgets if w.text and w.text.strip()]
    if not parts:
        return None

    body = "\n\n".join(parts)
    return (
        "\n\n<system-reminder>\n"
        f"{_WIDGET_CONTEXT_PREAMBLE}\n\n"
        f"{body}\n"
        "</system-reminder>"
    )


def serialize_widget_contexts_for_metadata(
    widgets: List[WidgetContext],
) -> List[dict]:
    """Serialize widgets for persistence in ``query_metadata['widget_contexts']``.

    Keeps ``text`` so the chip preview UI can show exactly what the agent
    saw on history replay. ``data`` is kept for replay UIs that want to
    render rich chips.
    """
    out: List[dict] = []
    for w in widgets:
        out.append(
            {
                "widget_type": w.widget_type,
                "widget_id": w.widget_id,
                "label": w.label,
                "text": w.text,
                "data": w.data,
                "captured_at": w.captured_at.isoformat() if w.captured_at else None,
                "description": w.description,
            }
        )
    return out
