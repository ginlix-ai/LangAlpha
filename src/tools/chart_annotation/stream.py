"""Best-effort live events for chart edits.

The row is the truth and the REST list re-syncs any chart that missed an
event, so a failed or absent writer never fails the edit.
"""

import logging
from typing import Any

logger = logging.getLogger(__name__)


def emit(writer, artifact_type: str, artifact_id: str, payload: dict[str, Any]) -> None:
    """Best-effort stream writer call — never raises."""
    if writer is None:
        return
    try:
        writer(
            {
                "artifact_type": artifact_type,
                "artifact_id": artifact_id,
                "payload": payload,
            }
        )
    except Exception:
        logger.warning("[chart_layers] stream writer failed", exc_info=True)


def get_writer():
    """Return the current LangGraph stream writer, or None when unavailable."""
    try:
        from langgraph.config import get_stream_writer

        return get_stream_writer()
    except Exception:
        return None
