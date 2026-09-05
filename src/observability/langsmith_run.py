"""LangSmith run stamping.

Which provider served a call is decided at runtime — a fetch may fall through
a chain, a search engine comes from user prefs. LangSmith filters on run
metadata, not on the payload a tool returns, so that fact has to be written
onto the enclosing run rather than handed back to the caller.
"""

from __future__ import annotations

import logging
from typing import Any, Optional, Sequence

logger = logging.getLogger(__name__)


def stamp_run(tags: Optional[Sequence[str]] = None, **fields: Any) -> None:
    """Attach metadata (and optional tags) to the enclosing LangSmith run.

    No-ops outside a traced run — which is every call when tracing is off — so
    callers stamp unconditionally instead of guarding.
    """
    try:
        from langsmith.run_helpers import get_current_run_tree

        run = get_current_run_tree()
        if run is None:
            return
        populated = {k: v for k, v in fields.items() if v is not None}
        if populated:
            run.metadata.update(populated)
        if tags:
            merged = list(run.tags or [])
            seen = set(merged)
            for t in tags:
                if t not in seen:
                    seen.add(t)
                    merged.append(t)
            run.tags = merged
    except Exception as e:
        # A tool call must never fail because its telemetry did.
        logger.debug("stamp_run failed: %s", e)
