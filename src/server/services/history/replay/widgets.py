"""Widget output stage: inline content-addressed data_ref payloads from storage."""

from __future__ import annotations

import asyncio
import json
import logging
from typing import Any

from src.utils.storage import get_bytes

logger = logging.getLogger(__name__)


def _is_widget(event_type: str, data: dict[str, Any]) -> bool:
    return event_type == "artifact" and data.get("artifact_type") == "html_widget"


async def _resolve_widget_data_refs(turn_items: list[dict[str, Any]]) -> None:
    """Inline widget data referenced by a content-addressed ``data_ref``.

    ShowWidget offloads large resolved data to object storage and checkpoints
    only ``data_ref {key, sha256, size}``. Runs after the stored-payload merge,
    so a widget already carrying ``data`` (stored event, or small inlined
    payload) skips the storage read. Unresolvable refs are left in place — the
    frontend renders the widget without its data files.
    """
    pending: list[tuple[dict[str, Any], dict[str, Any]]] = []  # (payload, data_ref)
    for item in turn_items:
        if not _is_widget(item["event"], item["data"]):
            continue
        payload = item["data"].get("payload")
        if not isinstance(payload, dict) or "data" in payload:
            continue
        ref = payload.get("data_ref")
        if not isinstance(ref, dict) or not ref.get("key"):
            continue
        pending.append((payload, ref))
    if not pending:
        return

    raws = await asyncio.gather(
        *(asyncio.to_thread(get_bytes, ref["key"]) for _payload, ref in pending)
    )
    for (payload, ref), raw in zip(pending, raws):
        if raw is None:
            logger.warning(f"[REPLAY] widget data_ref unreadable: {ref['key']}")
            continue
        try:
            payload["data"] = json.loads(raw)
        except (ValueError, UnicodeDecodeError):
            logger.warning(f"[REPLAY] widget data_ref not valid JSON: {ref['key']}")
