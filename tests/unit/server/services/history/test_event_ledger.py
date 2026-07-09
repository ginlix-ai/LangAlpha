"""Event-ledger enumeration guard.

The single-source-replay invariant: every SSE event type on the chat wire is
exactly one of

- **checkpoint-projected** — rebuilt from LangGraph checkpoint state,
- **table-sourced** — rebuilt from an authoritative DB table,
- **live-only** — transient signal never replayed (its durable content, if
  any, reaches the client another way),
- **stored-events-only** — replayable ONLY via persisted ``sse_events``
  (must be re-homed or explicitly accepted-lost before the dual-write stops;
  cutover step 5 blocks on this set).

This test scans the chat/replay emit surface for event-type literals and
fails when an emitted type is missing from the ledger (add the type to the
right category — which usually means implementing its replay source first)
or when a ledger entry no longer appears in the source (delete it).
"""

from __future__ import annotations

import re
from pathlib import Path

SRC = Path(__file__).resolve().parents[5] / "src"

# The chat-wire emit surface. The workspace bring-up stream
# (src/server/app/workspaces.py) is a separate SSE surface with its own
# vocabulary and no replay, so it is deliberately out of scope.
_SCAN_ROOTS = (
    SRC / "server" / "handlers",
    SRC / "server" / "services" / "history",
    SRC / "server" / "app" / "threads.py",
)

CHECKPOINT_PROJECTED = {
    "message_chunk",
    "tool_calls",
    "tool_call_result",
    "artifact",
    "context_window",
    "steering_delivered",
    "interrupt",
    "model_fallback",  # ui-channel record pushed by ModelResilienceMiddleware
}

TABLE_SOURCED = {
    "user_message",  # conversation_queries
    "provenance",  # provenance_records
    "credit_usage",  # conversation_usages
    "error",  # conversation_responses.errors + metadata (terminal event)
}

LIVE_ONLY = {
    "metadata",  # per-run header, accumulate=False
    "workspace_status",  # sandbox bring-up progress
    "warning",
    "retry",  # recoverable-error notice, run restarts
    "steering_accepted",  # only on the steering POST's own stream
    "steering_returned",  # undelivered-steering drain at run end
    "model_retry",  # transient, accumulate=False
    "tool_call_chunks",  # replay's consolidated tool_calls carries full args
    "compaction_chunk",  # summarize context_window carries summary_text
    "subagent_stream_end",  # reconnect-stream sentinel
    "replay_done",  # replay sentinel
}

# KNOWN GAP: survives replay only through persisted sse_events. Before
# sse_events writes stop (cutover step 5) each entry here must move to a
# category above or be explicitly accepted as not replayed. Empty since
# model_fallback moved to the ui channel — cutover step 5 is unblocked.
STORED_EVENTS_ONLY: set[str] = set()

_CATEGORIES = {
    "checkpoint": CHECKPOINT_PROJECTED,
    "table": TABLE_SOURCED,
    "live-only": LIVE_ONLY,
    "stored-events-only": STORED_EVENTS_ONLY,
}

_EMIT_PATTERNS = (
    # _format_sse_event("type", ...) — possibly line-wrapped
    re.compile(r'_format_sse_event\(\s*"([a-z_]+)"'),
    # yield f"event: type\n..." and plain "event: type"
    re.compile(r'"(?:id: [^"]*?\\n)?event: ([a-z_]+)'),
    # replay item dicts: {"event": "type", ...}
    re.compile(r'\{"event": "([a-z_]+)"'),
    re.compile(r'"event": "([a-z_]+)",'),
)


def _scan_files() -> list[Path]:
    files: list[Path] = []
    for root in _SCAN_ROOTS:
        if root.is_file():
            files.append(root)
        else:
            files.extend(sorted(root.rglob("*.py")))
    return files


def test_categories_are_disjoint_and_complete():
    names = list(_CATEGORIES)
    for i, a in enumerate(names):
        for b in names[i + 1 :]:
            overlap = _CATEGORIES[a] & _CATEGORIES[b]
            assert not overlap, f"{sorted(overlap)} in both {a!r} and {b!r}"


def test_every_emitted_event_type_is_in_the_ledger():
    ledger = set().union(*_CATEGORIES.values())
    unclassified: dict[str, str] = {}
    for path in _scan_files():
        text = path.read_text()
        for pattern in _EMIT_PATTERNS:
            for event_type in pattern.findall(text):
                if event_type not in ledger:
                    unclassified.setdefault(event_type, str(path))
    assert not unclassified, (
        f"SSE event types emitted but not classified in the event ledger: "
        f"{unclassified}. Add each to the category matching its replay "
        f"source (implementing that source first if it does not exist)."
    )


def test_every_ledger_entry_still_exists_in_source():
    corpus = "\n".join(p.read_text() for p in _scan_files())
    stale = {
        event_type
        for event_type in set().union(*_CATEGORIES.values())
        if f'"{event_type}"' not in corpus and f"event: {event_type}" not in corpus
    }
    assert not stale, (
        f"Ledger entries with no emit site in the scanned sources: "
        f"{sorted(stale)}. Remove them from the ledger."
    )


def test_replay_transition_rules_cover_the_replayable_set():
    """The replay module's passthrough/stored-preferred tuples must stay
    inside the replayable ledger (a stored-preferred type outside the ledger
    would silently vanish post-cutover)."""
    from src.server.services.history.replay import (
        _PASSTHROUGH_EVENTS,
        _STORED_PREFERRED_EVENTS,
    )

    replayable = CHECKPOINT_PROJECTED | TABLE_SOURCED
    assert set(_STORED_PREFERRED_EVENTS) <= replayable
    # Passthrough may additionally carry the stored-events-only legacy set.
    assert set(_PASSTHROUGH_EVENTS) <= replayable | STORED_EVENTS_ONLY
