"""The pages on the owner's Dashboard list, as the server reads it.

The list is the web's, which alone writes it: ``SavedDashboards`` in
``web/src/lib/dashboards/saved.ts``, stored at
``user_preferences.other_preference['custom_dashboards']``. Change the two
together.
"""

from __future__ import annotations

import logging
from typing import Any

from src.server.database.user import get_user_preferences

logger = logging.getLogger(__name__)

#: ``SAVED_DASHBOARDS_KEY`` in ``saved.ts``.
SAVED_DASHBOARDS_KEY = "custom_dashboards"


def listed_page_ids(other_preference: Any) -> frozenset[str] | None:
    """The artifact ids the list's pages name, or None when the value is not
    one the web writes, which a caller must read as unknown, never as empty.
    Only a ``page`` item names a record; an ``app`` item names none."""
    if other_preference is None:
        return frozenset()
    if not isinstance(other_preference, dict):
        return None
    saved = other_preference.get(SAVED_DASHBOARDS_KEY)
    if saved is None:
        return frozenset()
    if not isinstance(saved, dict) or not isinstance(saved.get("list"), list):
        return None
    ids = set()
    for item in saved["list"]:
        if isinstance(item, dict) and item.get("kind") == "page":
            artifact_id = item.get("artifactId")
            if isinstance(artifact_id, str) and artifact_id:
                ids.add(artifact_id)
    return frozenset(ids)


async def listed_pages(user_id: str) -> frozenset[str] | None:
    """The user's listed pages from the row itself, or None when it cannot be
    read. A missing row is None too: a settings reset deletes it and the web
    writes its list back after, so for that moment the list is unknown."""
    try:
        row = await get_user_preferences(user_id, cached=False)
    except Exception:
        logger.warning("Reading the Dashboard list of %s failed", user_id, exc_info=True)
        return None
    if row is None:
        return None
    return listed_page_ids(row.get("other_preference"))
