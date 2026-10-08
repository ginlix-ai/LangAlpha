"""The durable row: what moved, and the message that carries it through history.

A row records something that moved underneath the frozen baseline. It used to
live in a state list that the tail re-rendered inside every envelope, which put
stable, historical content into the one block that is never cached and left the
row's place in time invisible. Written into ``messages`` instead, a row is
written once, sits where it was observed, and is superseded by chronology: a
newer row of the same kind is simply further down.

The persisted form is provider-neutral on purpose. Which role the model actually
reads it under is a per-call decision, made by ``carrier.py`` against the model
in hand, so nothing about one provider's shape ever reaches a checkpoint.
Everything that writes or reads the row's ``additional_kwargs`` lives here, so
no other module has to know their layout.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

from langchain_core.messages import HumanMessage

from ptc_agent.agent.middleware.compaction.utils import resolve_cutoff_index
from ptc_agent.agent.middleware.runtime_context.harness_blocks import (
    harness_block_for,
)
from ptc_agent.agent.middleware.runtime_context.state import state_get
from ptc_agent.agent.middleware.runtime_context.templates import render_template

#: ``lc_source`` tag on a persisted row. Registered in the shared message
#: classifier (``ptc_agent/agent/transcript/classify.py``), so neither replay
#: nor the transcript ever opens a turn or a run on one.
RUNTIME_UPDATE_SOURCE = "runtime_update"

#: The one ``additional_kwargs`` key the row's metadata rides under.
RUNTIME_UPDATE_KEY = "runtime_update"


@dataclass(slots=True)
class DurableUpdate:
    """One runtime-context row: something that moved, and who moved it.

    A durable row is written once, at a turn boundary or, like the notes
    reminder and check-in, between tool batches, and persisted as a message;
    a per-call contributor puts the same shape on the request and it is gone
    with the call. ``schema_version`` travels with the row because a row
    outlives the code that wrote it: a reader replaying an old thread has to
    know which shape it is looking at before it can render it.
    """

    kind: str
    schema_version: int
    text: str
    provenance: dict[str, Any] = field(default_factory=dict)
    created_at: datetime = field(default_factory=lambda: datetime.now(tz=UTC))

    def to_dict(self) -> dict[str, Any]:
        return {
            "kind": self.kind,
            "schema_version": self.schema_version,
            "text": self.text,
            "provenance": dict(self.provenance),
            "created_at": self.created_at.isoformat(),
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> DurableUpdate:
        raw_created = data.get("created_at")
        created_at = datetime.now(tz=UTC)
        if isinstance(raw_created, datetime):
            created_at = raw_created
        elif isinstance(raw_created, str):
            try:
                created_at = datetime.fromisoformat(raw_created)
            except ValueError:
                pass
        provenance = data.get("provenance")
        return cls(
            kind=str(data.get("kind") or "unknown"),
            schema_version=int(data.get("schema_version") or 1),
            text=str(data.get("text") or ""),
            provenance=dict(provenance) if isinstance(provenance, dict) else {},
            created_at=created_at,
        )


def build_update_message(update: DurableUpdate) -> HumanMessage:
    """The message that carries one row through history.

    A ``HumanMessage`` because it is the only role every provider accepts at
    any position in a conversation, and the row has to survive being replayed
    to a model the thread did not start on. Ids are deliberately not stamped:
    the row is returned from a middleware hook, so the Pregel path mints them.
    """
    return HumanMessage(
        content=render_update_row(update),
        additional_kwargs={
            "lc_source": RUNTIME_UPDATE_SOURCE,
            RUNTIME_UPDATE_KEY: {
                "kind": update.kind,
                "schema_version": update.schema_version,
                "provenance": dict(update.provenance),
                "created_at": update.created_at.isoformat(),
            },
        },
    )


def is_runtime_update_message(message: Any) -> bool:
    """True for a message :func:`build_update_message` wrote."""
    return (
        isinstance(message, HumanMessage)
        and (message.additional_kwargs or {}).get("lc_source") == RUNTIME_UPDATE_SOURCE
    )


def runtime_update_from_message(message: Any) -> DurableUpdate | None:
    """The row a message carries, or None when it carries none.

    The inverse of :func:`build_update_message`, and the only reader of the
    stamped kwargs. A row written by an older build may be missing fields;
    ``DurableUpdate.from_dict`` fills those in rather than refusing the row,
    because a row the model already saw must keep rendering.
    """
    if not is_runtime_update_message(message):
        return None
    meta = (message.additional_kwargs or {}).get(RUNTIME_UPDATE_KEY)
    data = dict(meta) if isinstance(meta, dict) else {}
    data["text"] = update_text(message)
    return DurableUpdate.from_dict(data)


def messages_in_view(state: Any) -> list[Any]:
    """The checkpoint's messages from the compaction cutoff onward, resolved
    the way the compaction slice is: what the model still reads after the
    summary, which stands in for everything before."""
    messages = state_get(state, "messages")
    if not isinstance(messages, (list, tuple)):
        return []
    event = state_get(state, "_summarization_event")
    cutoff = resolve_cutoff_index(messages, event) if isinstance(event, dict) else 0
    return list(messages)[cutoff:]


def rows_in_view(state: Any) -> list[DurableUpdate]:
    """The rows the model can still read, oldest first: a row the summary
    swallowed is one the model no longer reads, so whatever it said is no
    longer said."""
    rows = (runtime_update_from_message(message) for message in messages_in_view(state))
    return [row for row in rows if row is not None]


def last_stated(state: Any, kind: str, key: str, default: Any = None) -> Any:
    """``provenance[key]`` of the last row in view of ``kind`` that carries it.

    A row of the kind without the key is passed over rather than read as the
    answer: it comes from a build that did not stamp it, or had nothing to say
    on it.
    """
    for row in reversed(rows_in_view(state)):
        if row.kind == kind and key in row.provenance:
            return row.provenance[key]
    return default


def update_text(message: Any) -> str:
    """The row's rendered text, from a string body or from its text blocks."""
    content = getattr(message, "content", "")
    if isinstance(content, str):
        return content
    if not isinstance(content, list):
        return ""
    parts: list[str] = []
    for block in content:
        if isinstance(block, str):
            parts.append(block)
        elif isinstance(block, dict) and block.get("type") == "text":
            text = block.get("text")
            if isinstance(text, str):
                parts.append(text)
    return "\n".join(p for p in parts if p)


def render_update_row(update: DurableUpdate) -> str:
    """One row, with no framing of its own.

    The ``<system-reminder>`` wrapper, or the operator role that replaces it, is
    added per call by the carrier, because which of the two the model reads is
    not knowable at write time. Every word of the row, the writer attribution
    included, is the template's: a row is prompt surface, and a second renderer
    of the same bytes makes what a thread carries depend on how it was packaged.
    A harness block's words come from its registry entry, so the template has
    one branch for all of them rather than one per block.
    """
    return render_template(
        "envelope/update_row.md.j2",
        update=update.to_dict(),
        harness=harness_block_for(update.kind),
    )
