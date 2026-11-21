"""What the API did to a request's earlier thinking before the model read it.

Under the ``thinking-binding-controls-2026-08-01`` beta, every Anthropic
response lists each thinking block it dropped (``thinking_dropped``) or let
through though it failed the prefix check (``thinking_mismatch_allowed``), by
``path`` and ``reason``. A ``prefix_binding_mismatch`` there is our own edit to
the prefix of a block; the other reasons follow a model switch or a thread that
moved between accounts.

OpenAI's Responses API sends the same field from GPT-6.2 on, listing each
earlier reasoning item it removed (``action: "removed"``) by ``id`` and
``reason``: ``prefix_mismatch`` is our own edit ahead of the item, the others
follow a model, organization or end user change, or a model that reads only
the current turn's reasoning. The patches in ``patches`` keep either list on
the answer's ``response_metadata``, so it lands in the checkpoint with the
message.
"""

from __future__ import annotations

from typing import Any

INPUT_TRANSFORMATIONS = "input_transformations"

_DROP_REASONS = frozenset(
    {"prefix_binding_mismatch", "model_binding_mismatch", "organization_binding_mismatch"}
)
_ALLOWED = "thinking_mismatch_allowed"

#: OpenAI's reasons, ``prefix_mismatch`` being our own edit.
_OPENAI_REASONS = frozenset(
    {"prefix_mismatch", "end_user_mismatch", "model_mismatch", "organization_mismatch", "turn_boundary"}
)

#: Reasons that follow the thread rather than an edit of ours (a model switch,
#: a move between accounts, a model that reads only the current turn), so a
#: drop under them is expected rather than a prefix we broke.
BY_DESIGN_DROPS = frozenset(
    {
        "model_binding_mismatch",
        "organization_binding_mismatch",
        "model_mismatch",
        "organization_mismatch",
        "turn_boundary",
    }
)

REPEATED = "repeated"


def keep_input_transformations(metadata: dict[str, Any], found: list[Any]) -> None:
    """Put *found* on an answer's ``metadata`` as plain JSON.

    An SDK that models the entries hands objects the checkpoint cannot hold.
    """
    metadata[INPUT_TRANSFORMATIONS] = [
        e.model_dump(mode="json") if hasattr(e, "model_dump") else e for e in found
    ]


def thinking_drops(message: Any, earlier: Any = None) -> dict[str, int]:
    """How many earlier reasoning payloads the API dropped or flagged before
    ``message`` was written, by reason.

    A dropped block or removed item counts under its reason, a flagged one
    under ``thinking_mismatch_allowed``, and an entry of a type or reason
    added after this was written under ``other``. One the answer before it
    (``earlier``) already listed, at the same path or by the same item id,
    counts under ``repeated``: the request sent it again and the API dropped
    it again, which loses nothing new. The prefix before a dropped block does
    not change again, so its path holds until a compaction summarizes it.
    """
    listed = {_key(entry) for entry in _entries(earlier) if isinstance(entry, dict)} - {None}
    counts: dict[str, int] = {}
    for entry in _entries(message):
        if not isinstance(entry, dict):
            continue
        kind, reason = entry.get("type"), entry.get("reason")
        if _key(entry) in listed:
            key = REPEATED
        elif kind == "reasoning" and entry.get("action") == "removed" and reason in _OPENAI_REASONS:
            key = reason
        elif kind == "thinking_dropped" and reason in _DROP_REASONS:
            key = reason
        elif kind == _ALLOWED:
            key = _ALLOWED
        else:
            key = "other"
        counts[key] = counts.get(key, 0) + 1
    return counts


def _key(entry: dict[str, Any]) -> Any:
    """What names the payload an entry is about: Anthropic's ``path``,
    OpenAI's item ``id``."""
    return entry.get("path") if entry.get("path") is not None else entry.get("id")


def _entries(message: Any) -> list[Any]:
    entries = (getattr(message, "response_metadata", None) or {}).get(INPUT_TRANSFORMATIONS)
    return entries if isinstance(entries, list) else []
