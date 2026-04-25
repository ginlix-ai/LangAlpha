"""Deterministic memo.md rebuild from the authoritative per-memo values.

The router never lets the user edit memo.md directly. Any change to a memo
value — upload, write, delete, metadata regen — triggers a full rebuild
through this module so the agent's catalog stays consistent with the store.
"""

from __future__ import annotations

import asyncio
import logging
import re
from datetime import UTC, datetime
from hashlib import sha256
from typing import Any

from langgraph.store.base import BaseStore

from ptc_agent.agent.memo.schema import (
    METADATA_PLACEHOLDER_DESCRIPTION,
)
from ptc_agent.core.paths import MEMO_INDEX_FILENAME

# Strip the markdown special-cases that would let an LLM-generated description
# inject hyperlinks, headings, code blocks, or HTML when memo.md is rendered
# back to the agent. Description text is itself derived from user content (see
# metadata.py) and must be treated as data when it lands in the agent context.
_MARKDOWN_STRIP_RE = re.compile(r"[\\`*_{}\[\]()<>#|~]+")
_DESCRIPTION_MAX_CHARS = 200

logger = logging.getLogger(__name__)

_STORE_OP_TIMEOUT_S = 2.0
_PAGE_SIZE = 100
_INDEX_KEY = MEMO_INDEX_FILENAME
# Keys we never include as catalog entries — memo.md is itself stored under
# the same namespace and must not self-reference.
_RESERVED_KEYS = frozenset({_INDEX_KEY})


def _sanitize_description(text: str) -> str:
    """Render description as plain prose: strip markdown specials and clamp.

    The description is LLM output derived from user-supplied memo content, and
    it lands in memo.md which the agent reads. Sanitizing here is defense in
    depth so a malicious memo can't inject `[click](https://exfil)`-style
    links or headings into the agent's working context.
    """
    cleaned = _MARKDOWN_STRIP_RE.sub("", text)
    cleaned = cleaned.replace("\n", " ").replace("\r", " ").strip()
    if len(cleaned) > _DESCRIPTION_MAX_CHARS:
        cleaned = cleaned[: _DESCRIPTION_MAX_CHARS - 1].rstrip() + "…"
    return cleaned


def _description_for(value: Any) -> str:
    """Best-effort display description for a memo value."""
    if not isinstance(value, dict):
        return METADATA_PLACEHOLDER_DESCRIPTION
    status = value.get("metadata_status")
    description = value.get("description")
    if status == "failed":
        return "Summary unavailable — use regenerate to retry."
    if status == "ready" and isinstance(description, str) and description.strip():
        return _sanitize_description(description)
    if isinstance(description, str) and description.strip():
        return _sanitize_description(description)
    return METADATA_PLACEHOLDER_DESCRIPTION


def _sort_key(item: Any) -> tuple[str, str]:
    """Sort by created_at ascending, falling back to key name for stability."""
    value = item.value if hasattr(item, "value") else {}
    created = value.get("created_at") if isinstance(value, dict) else ""
    if not isinstance(created, str):
        created = ""
    return (created, item.key)


def _format_entry(item: Any) -> str:
    key = item.key
    description = _description_for(item.value)
    # Escape backslashes and pipes that would break markdown table rendering
    # in some viewers; memo.md is a bullet list so this is just defense in depth.
    safe_description = description.replace("\n", " ").strip()
    return f"- [{key}]({key}) — {safe_description}"


def _render_memo_md(items: list[Any]) -> str:
    """Render the complete memo.md body from sorted memo items."""
    # Items are already sorted by the caller; preserve order.
    now = datetime.now(UTC).isoformat(timespec="seconds").replace("+00:00", "Z")
    count = len(items)
    header = [
        "# Memos",
        "",
        f"_Last updated {now} — {count} memo(s)._",
        "",
    ]
    if count == 0:
        header.append("_No memos yet._")
        return "\n".join(header) + "\n"
    body = [_format_entry(item) for item in items]
    return "\n".join([*header, *body]) + "\n"


async def _collect_items(
    store: BaseStore, namespace: tuple[str, ...]
) -> list[Any]:
    """Page through the namespace and return every non-reserved item.

    ``original_bytes_b64`` may live inside these values. ``asearch`` returns
    whole rows today; the rebuilder only needs metadata fields, but we can't
    project them out through the current BaseStore API. The O(N) cost is
    documented as a known limitation; revisit at 500+ memos per user.
    """
    items: list[Any] = []
    offset = 0
    while True:
        try:
            page = await asyncio.wait_for(
                store.asearch(namespace, limit=_PAGE_SIZE, offset=offset),
                timeout=_STORE_OP_TIMEOUT_S,
            )
        except asyncio.TimeoutError:
            logger.warning(
                "memo asearch timed out during index rebuild",
                extra={"namespace": namespace, "offset": offset},
            )
            break
        if not page:
            break
        for item in page:
            if item.key not in _RESERVED_KEYS:
                items.append(item)
        if len(page) < _PAGE_SIZE:
            break
        offset += _PAGE_SIZE
    items.sort(key=_sort_key)
    return items


def _index_state_hash(items: list[Any]) -> str:
    """Hash the inputs that determine memo.md's *content* (not the timestamp).

    Two rebuilds with the same memos produce the same content hash; only the
    timestamp changes. Persisting this lets us short-circuit the postgres
    write when nothing user-visible has changed — saving a row replacement
    on every metadata regen of an already-displayed memo.
    """
    h = sha256()
    for item in items:
        value = item.value if isinstance(getattr(item, "value", None), dict) else {}
        h.update(item.key.encode("utf-8"))
        h.update(b"\x00")
        h.update(str(value.get("metadata_status") or "").encode("utf-8"))
        h.update(b"\x00")
        h.update(str(value.get("description") or "").encode("utf-8"))
        h.update(b"\x00")
        h.update(str(value.get("created_at") or "").encode("utf-8"))
        h.update(b"\x01")  # row separator
    return h.hexdigest()


async def rebuild_memo_index(
    store: BaseStore, namespace: tuple[str, ...]
) -> None:
    """Rewrite memo.md under ``namespace`` from the current set of memo values.

    Best-effort: logs and swallows any single-write failure so an outage
    during rebuild doesn't block the upload path from returning 202. Skips
    the postgres write entirely when ``_index_state_hash`` matches what is
    already on disk — i.e. metadata regen on a single memo doesn't trigger
    a full memo.md row replacement when nothing visible changed.
    """
    items = await _collect_items(store, namespace)
    state_hash = _index_state_hash(items)

    now = datetime.now(UTC).isoformat(timespec="seconds").replace("+00:00", "Z")
    existing_created = now
    existing_hash: str | None = None
    try:
        existing = await asyncio.wait_for(
            store.aget(namespace, _INDEX_KEY), timeout=_STORE_OP_TIMEOUT_S,
        )
        if existing is not None and isinstance(existing.value, dict):
            prior_created = existing.value.get("created_at")
            if isinstance(prior_created, str) and prior_created:
                existing_created = prior_created
            prior_hash = existing.value.get("state_hash")
            if isinstance(prior_hash, str):
                existing_hash = prior_hash
    except asyncio.TimeoutError:
        logger.warning("memo.md aget timed out during rebuild", extra={"namespace": namespace})

    if existing_hash == state_hash:
        # Catalog unchanged — skip the redundant postgres write.
        return

    body = _render_memo_md(items)
    value = {
        "content": body,
        "encoding": "utf-8",
        "created_at": existing_created,
        "modified_at": now,
        "state_hash": state_hash,
        # Read by MemoAwarenessMiddleware on every model call to skip a full
        # asearch fan-out. Refreshed in lockstep with state_hash.
        "memo_count": len(items),
    }
    try:
        await asyncio.wait_for(
            store.aput(namespace, _INDEX_KEY, value),
            timeout=_STORE_OP_TIMEOUT_S,
        )
    except asyncio.TimeoutError:
        logger.warning("memo.md aput timed out during rebuild", extra={"namespace": namespace})
    except Exception:
        logger.exception("memo.md rebuild failed", extra={"namespace": namespace})
