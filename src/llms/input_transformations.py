"""What the API did to a request's earlier thinking before the model read it.

Under the ``thinking-binding-controls-2026-08-01`` beta, every Anthropic
response lists each thinking block it dropped (``thinking_dropped``) or let
through though it failed the prefix check (``thinking_mismatch_allowed``), by
``path`` and ``reason``. A ``prefix_binding_mismatch`` there is our own edit to
the prefix of a block; the other reasons follow a model switch or a thread that
moved between accounts. The patch in ``patches`` keeps the list on the answer's
``response_metadata``, so it lands in the checkpoint with the message.
"""

from __future__ import annotations

from typing import Any

INPUT_TRANSFORMATIONS = "input_transformations"

PREFIX_BINDING_MISMATCH = "prefix_binding_mismatch"

_DROP_REASONS = frozenset(
    {PREFIX_BINDING_MISMATCH, "model_binding_mismatch", "organization_binding_mismatch"}
)
_ALLOWED = "thinking_mismatch_allowed"


REPEATED = "repeated"


def thinking_drops(message: Any, earlier: Any = None) -> dict[str, int]:
    """How many earlier thinking blocks the API dropped or flagged before
    ``message`` was written, by reason.

    A dropped block counts under its reason, a flagged one under
    ``thinking_mismatch_allowed``, and an entry of a type or reason added
    after this was written under ``other``. A block the answer before it
    (``earlier``) already listed at the same path counts under ``repeated``:
    the request sent it again and the API dropped it again, which loses
    nothing new. The prefix before a dropped block does not change again, so
    its path holds until a compaction summarizes it.
    """
    listed = {
        entry.get("path")
        for entry in _entries(earlier)
        if isinstance(entry, dict) and entry.get("path") is not None
    }
    counts: dict[str, int] = {}
    for entry in _entries(message):
        if not isinstance(entry, dict):
            continue
        kind, reason = entry.get("type"), entry.get("reason")
        if entry.get("path") in listed:
            key = REPEATED
        elif kind == "thinking_dropped" and reason in _DROP_REASONS:
            key = reason
        elif kind == _ALLOWED:
            key = _ALLOWED
        else:
            key = "other"
        counts[key] = counts.get(key, 0) + 1
    return counts


def _entries(message: Any) -> list[Any]:
    entries = (getattr(message, "response_metadata", None) or {}).get(INPUT_TRANSFORMATIONS)
    return entries if isinstance(entries, list) else []
