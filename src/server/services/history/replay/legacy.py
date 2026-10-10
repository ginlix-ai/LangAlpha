"""Legacy facts: what a turn's stored events add to its projection, resolved.

A turn settled while the live stream was archived replays some rows only that
archive kept: widget data files, a tool result the checkpoint holds as an
eviction pointer, reasoning durations, signal rows (provenance, context
window, credit usage, steering, interrupts, errors) in their streamed
positions, and the URLs its sandbox images were captured to.
``stored_merge.derive_turn`` resolves those against the turn's projection
into facts, and ``apply_turn`` places them.

A row not yet backfilled derives its facts from its stored events on every
projection. A backfilled row carries them in ``replay_facts.legacy``, so its
replay never reads the stored events. A row with no stored events is
backfilled with ``{"v": FACTS_VERSION}`` alone: facts known empty, which is
not the same as no facts.

Backfilled facts outlive the projection code they were resolved against, so
they name projected rows by what the checkpoint gives them, never by where
they fall: a message by its id, a tool call or result by its call id, an
artifact by the id the projector gave it (see ``row_keys``). Each signal row
keeps every anchor that streamed since the previous one, so when a later
projection drops the newest it follows the nearest that remains. A stored
widget payload upgrades the projected widget it was paired with, by id; one
whose widget the projection did not emit then takes a widget no payload
names that falls between its neighbours' widgets, or is placed like a signal
row.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any, Required, TypedDict

from src.server.database.replay_facts import LEGACY_KEY
from src.server.services.history.replay import facts as replay_facts
from src.server.services.history.replay import widgets
from src.server.services.history.replay.lanes import agent_lane

FACTS_VERSION = 3


class StoredRow(TypedDict):
    event: str
    data: dict[str, Any]


class StoredResult(TypedDict, total=False):
    content: Required[str]
    content_type: str


class EventSignal(TypedDict, total=False):
    event: Required[str]
    data: Required[dict[str, Any]]
    at: int


class WidgetSignal(TypedDict, total=False):
    widget: Required[int]
    at: int


Signal = EventSignal | WidgetSignal


class Merge(TypedDict, total=False):
    """The parts ``stored_merge.derive_merge`` documents. Row keys are
    stored as lists: they round-trip through JSONB."""

    widgets: list[list[Any]]
    results: dict[str, StoredResult]
    durations: list[list[Any]]
    anchors: list[list[Any]]
    signals: list[Signal]


class LegacyFacts(TypedDict, total=False):
    """A response row's ``replay_facts[LEGACY_KEY]``."""

    v: Required[int]
    # Sandbox image path -> the URL its stored copy was captured to.
    images: dict[str, str]
    merge: Merge
    verbatim: list[StoredRow]


# Stored events replayed verbatim (anchored to their original position):
# non-derivable payloads, plus resolved-interrupt cards and error markers —
# `interrupt` renders answered HITL cards on replay (a pending interrupt is
# also re-emitted from the checkpoint tip; the frontend dedups by
# interrupt_id), and `error` keeps wire parity with sse replay.
# `context_window` and `steering_delivered` are checkpoint-projected, but a
# turn with stored events prefers those verbatim (see STORED_PREFERRED_EVENTS).
PASSTHROUGH_EVENTS = (
    "context_window",
    "provenance",
    "steering_delivered",
    # A run's replay facts place it on its run lane; a stored copy is kept
    # only for a run settled before those existed.
    "steering_returned",
    "credit_usage",
    "interrupt",
    "error",
    "model_fallback",
)


# Projected/synthesized event types the stored stream also carries in full.
# A turn with stored events drops its projected copies and replays the stored
# ones (richer historical payloads, proven anchoring); a turn without them
# keeps the projected copies. The terminal ``error`` event is NOT here —
# stored events never contain it (persisted before it is yielded), so it is
# synthesized from the response row on both paths unconditionally.
STORED_PREFERRED_EVENTS = (
    "steering_delivered",
    "context_window",
    "provenance",
    "credit_usage",
    "interrupt",
    "model_fallback",
)


# Content types the projector emits: a message's anchorable chunks. Live-only
# accumulation chunks (content_type=None) and tool-only messages have none, so
# both streams enumerate the same messages when keyed on these.
ANCHORABLE_CONTENT_TYPES = frozenset({"reasoning_signal", "reasoning", "text"})


def facts_of(response: dict[str, Any] | None) -> LegacyFacts | None:
    """The row's legacy facts, or None when it has not been backfilled (or
    was backfilled by a version this code does not read)."""
    facts = (response or {}).get("replay_facts")
    legacy = facts.get(LEGACY_KEY) if isinstance(facts, dict) else None
    if isinstance(legacy, dict) and legacy.get("v") == FACTS_VERSION:
        return legacy
    return None


# How a row key names an anchorable message chunk, from its lane, its data
# and how often its id recurred in that lane after another row (``row_keys``).
MessageKey = Callable[[str, dict[str, Any], int], tuple | None]


def own_message_key(lane: str, data: dict[str, Any], occurrence: int) -> tuple:
    """A message chunk by its id and content type, the key facts name it by.
    The projector gives every message without an id the same placeholder, so
    a recurring id also carries its occurrence: a message's own chunks are
    contiguous."""
    key = ("message_chunk", lane, data.get("id"), data["content_type"])
    return key + (occurrence,) if occurrence else key


def row_keys(
    turn_items: list[dict[str, Any]], message_key: MessageKey = own_message_key
) -> list[tuple | None]:
    """The key of each row, or None for a row no key names: a tool call
    round by its first call id, a result by its call id, an artifact by its
    id, and a message chunk by ``message_key``."""
    keys: list[tuple | None] = []
    running: dict[str, Any] = {}
    occurrences: dict[tuple[str, Any], int] = {}
    for item in turn_items:
        event, data = item["event"], item["data"]
        lane = agent_lane(data.get("agent"))
        key: tuple | None = None
        if event == "message_chunk" and data.get("content_type") in ANCHORABLE_CONTENT_TYPES:
            message_id = data.get("id")
            if lane not in running or running[lane] != message_id:
                running[lane] = message_id
                occurrences[lane, message_id] = occurrences.get((lane, message_id), -1) + 1
            key = message_key(lane, data, occurrences[lane, message_id])
        else:
            running.pop(lane, None)
            if event == "tool_calls":
                call_ids = _call_ids(data)
                key = ("tool_calls", call_ids[0]) if call_ids else None
            elif event == "tool_call_result" and data.get("tool_call_id"):
                key = ("tool_call_result", data["tool_call_id"])
            elif event == "artifact" and data.get("artifact_id"):
                key = ("artifact", data["artifact_id"])
        keys.append(key)
    return keys


def key_index(
    turn_items: list[dict[str, Any]], message_key: MessageKey = own_message_key
) -> dict[tuple, int]:
    """Projected position of each row key.

    The last occurrence wins, so a message key covers its whole group (a
    reasoning start and close share one key, and the close is the one a
    duration lands on). The checkpoint holds a parallel round as one row,
    the live archive as one row per call, so any of its calls names it.
    """
    index: dict[tuple, int] = {}
    for idx, key in enumerate(row_keys(turn_items, message_key)):
        if key is None:
            continue
        index[key] = idx
        if key[0] == "tool_calls":
            for call_id in _call_ids(turn_items[idx]["data"]):
                index["tool_calls", call_id] = idx
    return index


def _call_ids(data: dict[str, Any]) -> list[str]:
    return [tc["id"] for tc in data.get("tool_calls") or [] if tc.get("id")]


def apply_turn(
    turn_items: list[dict[str, Any]],
    task_items: list[dict[str, Any]],
    facts: LegacyFacts,
) -> list[dict[str, Any]]:
    """The turn's items with its legacy facts applied.

    A turn whose stored images no basename could settle carries its main
    lane (and every row outside the lanes its runs projected) ``verbatim``;
    its run lanes keep the projection, merged like any other turn's.
    """
    replay_facts.apply_image_url_map(turn_items, facts.get("images") or {})
    merge = facts.get("merge")
    if "verbatim" in facts:
        rows = [
            {"event": row["event"], "data": dict(row["data"])}
            for row in facts["verbatim"]
        ]
        return rows + apply(task_items, merge)
    return apply(turn_items, merge)


def apply(
    turn_items: list[dict[str, Any]], merge: Merge | None
) -> list[dict[str, Any]]:
    """Place a turn's merge facts into its projection.

    ``merge`` is None for a turn with no stored events, whose projected
    signal rows stand. Otherwise those are replaced by the stored ones, in
    this order: widget payloads upgrade the widgets they were paired with,
    evicted results are restored, reasoning durations land on their closes,
    and each signal row follows the newest of its anchors that the
    projection still holds. Returned steering a run's own facts already
    placed is not placed twice.
    """
    if merge is None:
        return turn_items
    placed_returns = replay_facts.returned_keys(turn_items)

    turn_items = [i for i in turn_items if i["event"] not in STORED_PREFERRED_EVENTS]
    # Keyed before the widget upgrade: facts name a widget by its projected id.
    index = key_index(turn_items)

    stored_widgets = merge.get("widgets") or []
    upgraded = _upgrade_widgets(turn_items, stored_widgets)
    for k, idx in upgraded.items():
        index["widget", k] = idx

    results = merge.get("results") or {}
    if results:
        for item in turn_items:
            data = item["data"]
            if item["event"] != "tool_call_result":
                continue
            stored = results.get(data.get("tool_call_id"))
            if stored is None:
                continue
            data["content"] = stored["content"]
            if "content_type" in stored:
                data["content_type"] = stored["content_type"]

    for key, elapsed_ms in merge.get("durations") or ():
        idx = index.get(tuple(key))
        if idx is None:
            continue
        target = turn_items[idx]["data"]
        if (
            target.get("content_type") == "reasoning_signal"
            and target.get("content") == "complete"
        ):
            target["elapsed_ms"] = elapsed_ms

    last_lane_index: dict[str, int] = {}
    for idx, item in enumerate(turn_items):
        last_lane_index[agent_lane(item["data"].get("agent"))] = idx
    # A main lane stopped inside its first model call projects nothing.
    turn_end = len(turn_items) - 1

    anchors = [tuple(key) for key in merge.get("anchors") or ()]
    inserts_after: dict[int, list[dict[str, Any]]] = {}
    anchor_idx = -1  # before the first projected item
    previous_at = -1
    for signal in merge.get("signals") or ():
        at = signal.get("at")
        if at is not None:
            # The anchors streamed since the previous signal, newest first.
            # When none remains the signal follows the previous one.
            for i in range(at, min(at, previous_at + 1) - 1, -1):
                if (idx := index.get(anchors[i])) is not None:
                    anchor_idx = idx
                    break
            previous_at = at
        if "widget" in signal:
            # A stored widget no projected widget took is placed like a
            # signal row.
            k = signal["widget"]
            if k in upgraded or not 0 <= k < len(stored_widgets):
                continue
            row = {"event": "artifact", "data": dict(stored_widgets[k][1])}
        else:
            event, data = signal["event"], signal["data"]
            if event == "error":
                # A run's error is the last thing it streamed, so it follows
                # everything its lane committed even when what streamed just
                # before it (output no checkpoint kept) anchors nothing.
                anchor_idx = max(
                    anchor_idx,
                    last_lane_index.get(agent_lane(data.get("agent")), turn_end),
                )
            if (
                event == "steering_returned"
                and (data.get("agent"), data.get("input_id")) in placed_returns
            ):
                continue
            row = {"event": event, "data": dict(data)}
        inserts_after.setdefault(anchor_idx, []).append(row)

    merged = list(inserts_after.get(-1, []))
    for idx, item in enumerate(turn_items):
        merged.append(item)
        merged.extend(inserts_after.get(idx, ()))
    return merged


def _upgrade_widgets(
    turn_items: list[dict[str, Any]], stored_widgets: list[list[Any]]
) -> dict[int, int]:
    """Give each projected widget its stored payload; the position of each
    payload taken.

    A payload goes to the projected widget it names. One that named none
    (the projection did not emit its widget when the facts were resolved)
    takes the first widget no payload names between the widgets its stored
    neighbours took: both streams order widgets by tool execution, and the
    live artifact id is random.
    """
    projected = [
        idx
        for idx, item in enumerate(turn_items)
        if widgets._is_widget(item["event"], item["data"])
    ]
    named = {
        artifact_id: k
        for k, (artifact_id, _data) in enumerate(stored_widgets)
        if artifact_id is not None
    }
    taken: dict[int, int] = {}  # stored payload -> projected position
    unnamed: list[int] = []
    for idx in projected:
        k = named.get(turn_items[idx]["data"].get("artifact_id"))
        if k is None:
            unnamed.append(idx)
        elif k not in taken:
            taken[k] = idx
    for k, (artifact_id, _data) in enumerate(stored_widgets):
        if artifact_id is not None:
            continue
        after = max((idx for j, idx in taken.items() if j < k), default=-1)
        before = min((idx for j, idx in taken.items() if j > k), default=len(turn_items))
        idx = next((idx for idx in unnamed if after < idx < before), None)
        if idx is not None:
            unnamed.remove(idx)
            taken[k] = idx
    for k, idx in taken.items():
        turn_items[idx]["data"] = dict(stored_widgets[k][1])
    return taken
