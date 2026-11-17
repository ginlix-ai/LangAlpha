"""Claude Code's own events on a graph's custom stream."""

from __future__ import annotations

import logging
from typing import Any

from langgraph.config import get_stream_writer

logger = logging.getLogger(__name__)


def emit(event: dict[str, Any]) -> None:
    """Write ``event`` to the running graph's custom stream. A notice is never
    worth failing a step for, so one the stream cannot take is dropped."""
    try:
        get_stream_writer()(event)
    except Exception:  # noqa: BLE001
        logger.debug("[ClaudeCode] %s event not emitted", event.get("type"), exc_info=True)
