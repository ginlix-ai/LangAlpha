"""Turn and run slices: what one turn or one subagent run added, stored once.

A slice is a pure function of two checkpoints (the boundary it starts at and
the state it ends at), so it is derived data: a row that disagrees with the
branch walk is rebuilt, and deleting rows loses nothing. Slices encode with
the checkpointer's own serializer, so whatever a checkpoint can hold, a slice
can too, under the same type allowlist.
"""

from __future__ import annotations

import hashlib
import json
import logging
from collections import Counter
from collections.abc import Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Any

from langchain_core.messages import AnyMessage

from ptc_agent.agent.transcript.classify import (
    is_run_boundary_message,
    is_summary_message,
)

if TYPE_CHECKING:
    from src.server.database.conversation.turn_slices import StoredSlice
    from src.server.services.history.reader import CheckpointHistoryReader, TurnAnchor

logger = logging.getLogger(__name__)

# Bump when what a slice holds changes in a way the sources keyed below do
# not show (the walk that picks a slice's two checkpoints, say): every stored
# slice then rebuilds from its checkpoints on the next read.
SLICE_VERSION = 1

_SRC = Path(__file__).resolve().parents[3]

# One namespace's checkpointed state values.
State = Mapping[str, Any]


@dataclass
class SpanDelta:
    """What a namespace added between two of its checkpoints: a turn on the
    main branch, a task run in its own namespace, or a whole namespace."""

    messages: list[AnyMessage] = field(default_factory=list)
    # The ``_summarization_event`` that landed in the span, or None.
    # Compaction's summary message lives in this state key, never in the
    # messages channel, so replay re-emits the summarize signal from here.
    new_summarization_event: dict[str, Any] | None = None
    # Growth of the compaction offload sets: replay re-emits the offload
    # signals from these counts (one aggregated event per kind).
    newly_offloaded_args: int = 0
    newly_offloaded_reads: int = 0
    # ``ui``-channel records that landed in the span (an id diff), such as
    # the model_fallback notices middleware pushes.
    new_ui_records: list[dict[str, Any]] = field(default_factory=list)


@dataclass(kw_only=True)
class TurnSlice(SpanDelta):
    """One conversational turn on the current branch: what it added, and
    where the walk placed it."""

    anchor: TurnAnchor


def _new_summarization_event(start: State, end: State) -> dict[str, Any] | None:
    """The span's freshly landed ``_summarization_event``, or None.

    Events are identified by their summary message id (uuid-stamped at build);
    an end-state event matching the start state predates this span.
    """

    def _identity(values: State) -> tuple[Any, Any] | None:
        event = values.get("_summarization_event")
        if not isinstance(event, dict):
            return None
        summary_message = event.get("summary_message")
        return (getattr(summary_message, "id", None), event.get("cutoff_index"))

    end_event = end.get("_summarization_event")
    if isinstance(end_event, dict) and _identity(end) != _identity(start):
        return end_event
    return None


def _set_growth(start: State, end: State, key: str) -> int:
    """How many ids ``key``'s set gained between the two states."""
    return len(set(end.get(key) or set()) - set(start.get(key) or set()))


def _new_ui_records(start: State, end: State) -> list[dict[str, Any]]:
    """``ui``-channel records the end state has that the start state lacks."""
    start_ids = {r.get("id") for r in (start.get("ui") or []) if isinstance(r, dict)}
    return [
        r
        for r in (end.get("ui") or [])
        if isinstance(r, dict) and r.get("id") not in start_ids
    ]


def _new_messages(
    start_values: State, end_values: State, *, has_input: bool = True
) -> list[AnyMessage]:
    """Messages the end state has that the start state lacks, by id.

    Id-diff, not count-diff, so compaction and REMOVE_ALL are safe. A
    message saved without an id (a full-list write from before DeltaChannel)
    loads id-less from some checkpoints and with an id from others, so it
    matches by content instead, each start message at most once. A message
    with a fresh id matches only an id-less one: a repeated "continue" after
    compaction dropped the first is new.

    Carried messages all precede the span's own input (``has_input``: every
    turn but a HITL resume, every task run), so from that input on nothing
    matches by content: a message there repeating one compaction removed is
    new, however its id loaded. Nor do they come from before where the end
    starts in the start (``_carried_from``): a window trim drops the head of
    the list, possibly inside a resume, where no input bounds the matching.
    """
    start = start_values.get("messages") or []
    end = end_values.get("messages") or []
    start_ids = {m.id for m in start if m.id is not None}
    carried = start[_carried_from(start, end):]
    idless = Counter(_content_key(m) for m in carried if m.id is None)
    # Start messages whose id the end lost: an id-less end message may be one.
    vanished: Counter[tuple[str, str]] | None = None
    carried_until = _input_position(end, start_ids) if has_input else None
    if carried_until is None:
        carried_until = len(end)
    new: list[AnyMessage] = []
    for i, m in enumerate(end):
        if m.id is not None and m.id in start_ids:
            continue
        if i >= carried_until:
            new.append(m)
            continue
        pools = [idless]
        if m.id is None:
            if vanished is None:
                end_ids = {e.id for e in end if e.id is not None}
                vanished = Counter(
                    _content_key(s)
                    for s in carried
                    if s.id is not None and s.id not in end_ids
                )
            pools.append(vanished)
        if any(pools):
            key = _content_key(m)
            pool = next((p for p in pools if p[key] > 0), None)
            if pool is not None:
                pool[key] -= 1
                continue
        new.append(m)
    return new


def _carried_from(start: list[AnyMessage], end: list[AnyMessage]) -> int:
    """Where ``end`` starts in ``start``: the first end message the start
    holds by id, counted back over the end messages before it, which a trim
    kept but stamped fresh ids on. 0 when the end holds none of them."""
    positions = {m.id: i for i, m in enumerate(start) if m.id is not None}
    for offset, message in enumerate(end):
        at = positions.get(message.id) if message.id is not None else None
        if at is not None:
            return max(0, at - offset)
    return 0


def _input_position(end: list[AnyMessage], start_ids: set[str]) -> int | None:
    """Where the span's own input sits in ``end``: the last real input the
    start lacks by id. A span opens on one input and its end state is read
    before the next one lands, so no real input follows it. None when
    compaction took the input with everything before it."""
    for i in range(len(end) - 1, -1, -1):
        m = end[i]
        if (
            (m.id is None or m.id not in start_ids)
            and is_run_boundary_message(m)
            and not is_summary_message(m)
        ):
            return i
    return None


def _content_key(message: AnyMessage) -> tuple[str, str]:
    content = message.content
    if not isinstance(content, str):
        content = json.dumps(content, sort_keys=True, default=str)
    return message.type, content


def delta(start: State, end: State, *, has_input: bool = True) -> SpanDelta:
    """What ``end`` holds that ``start`` lacks, both one namespace's state
    values. ``has_input``: the span opens on an input message of its own
    (every turn but a HITL resume, every task run)."""
    return SpanDelta(
        messages=_new_messages(start, end, has_input=has_input),
        new_summarization_event=_new_summarization_event(start, end),
        newly_offloaded_args=_set_growth(start, end, "_offloaded_tool_call_ids"),
        newly_offloaded_reads=_set_growth(start, end, "_offloaded_read_result_ids"),
        new_ui_records=_new_ui_records(start, end),
    )


def turn_slice(anchor: TurnAnchor, start: State, end: State) -> TurnSlice:
    span = delta(start, end, has_input=not anchor.is_resume)
    return TurnSlice(anchor=anchor, **vars(span))


def encode(serde: Any, span: SpanDelta) -> tuple[str, bytes]:
    return serde.dumps_typed(
        {
            "messages": list(span.messages),
            "new_summarization_event": span.new_summarization_event,
            "newly_offloaded_args": span.newly_offloaded_args,
            "newly_offloaded_reads": span.newly_offloaded_reads,
            "new_ui_records": list(span.new_ui_records),
        }
    )


def decode(serde: Any, codec: str, data: bytes) -> SpanDelta:
    payload = serde.loads_typed((codec, data))
    return SpanDelta(
        messages=list(payload.get("messages") or []),
        new_summarization_event=payload.get("new_summarization_event"),
        newly_offloaded_args=int(payload.get("newly_offloaded_args") or 0),
        newly_offloaded_reads=int(payload.get("newly_offloaded_reads") or 0),
        new_ui_records=list(payload.get("new_ui_records") or []),
    )


def decoded(serde: Any, row: StoredSlice) -> SpanDelta | None:
    """The stored row's span, or None when it no longer decodes: such a row
    is stale like any other, cut again and stored over rather than failing
    every read of its thread."""
    try:
        return decode(serde, row.codec, row.data)
    except Exception:
        logger.warning(
            "stored slice at %s undecodable", row.input_checkpoint_id, exc_info=True
        )
        return None


def matches(row: StoredSlice | None, anchor: TurnAnchor) -> bool:
    """A stored slice is the turn's only while the branch still ends it where
    it ended when stored, and the code that cut it is the code running."""
    return (
        row is not None
        and row.input_checkpoint_id == anchor.input_checkpoint_id
        and row.tail_checkpoint_id == anchor.tail_checkpoint_id
        and row.slice_key == SLICE_KEY
    )


def stored_spans(
    serde: Any,
    anchored: Mapping[Any, TurnAnchor],
    rows: Mapping[Any, StoredSlice],
) -> dict[Any, SpanDelta]:
    """The span of each turn whose stored row ``matches`` its anchor and
    decodes, keyed as both are: what a read may serve without a cut."""
    out: dict[Any, SpanDelta] = {}
    for key, anchor in anchored.items():
        row = rows.get(key)
        if matches(row, anchor) and (span := decoded(serde, row)) is not None:
            out[key] = span
    return out


async def load_turns(
    reader: CheckpointHistoryReader,
    thread_id: str,
    anchored: list[tuple[Any, TurnAnchor]],
    rows: Mapping[Any, StoredSlice],
) -> tuple[dict[Any, TurnSlice], set[Any]]:
    """``(turns, fresh)``, keyed as ``anchored`` and ``rows`` are: each turn
    from its stored row by ``stored_spans``, placed on the branch its anchor
    names, and cut from the checkpoints otherwise (``fresh``)."""
    held = stored_spans(reader.serde, dict(anchored), rows)
    turns = {
        key: TurnSlice(anchor=anchor, **vars(held[key]))
        for key, anchor in anchored
        if key in held
    }
    cut = [(key, anchor) for key, anchor in anchored if key not in held]
    if cut:
        extracted = await reader.aget_turn_slices(thread_id, [a for _, a in cut])
        turns.update((key, turn) for (key, _), turn in zip(cut, extracted))
    return turns, {key for key, _ in cut}


def slice_sources() -> list[Path]:
    """The code that decides a slice's bytes: this module, which cuts and
    encodes it, and the transcript classifier it finds a span's input by.
    Digested whole, and a test holds the list closed over their imports, so
    a fix to any of it reaches slices stored before it."""
    return [Path(__file__).resolve(), _SRC / "ptc_agent/agent/transcript/classify.py"]


def _slice_key() -> str:
    digest = hashlib.blake2b(digest_size=8)
    for path in slice_sources():
        digest.update(str(path.relative_to(_SRC)).encode())
        digest.update(path.read_bytes())
    return f"{SLICE_VERSION}.{digest.hexdigest()}"


# Stored with every slice; a row under another key is rebuilt.
SLICE_KEY = _slice_key()
