"""Legacy facts: what a turn's archived live stream adds to its projection.

A turn settled while the live stream was archived replays some rows only that
archive kept: widget data files, a tool result the checkpoint holds as an
eviction pointer, reasoning durations, signal rows (provenance, context
window, credit usage, steering, interrupts, errors) in their streamed
positions, and the URLs its sandbox images were captured to. Those were
resolved once against the turn's projection, into its row's
``replay_facts.legacy``, and ``apply_turn`` places them. A row whose archive
added nothing holds ``{"v": FACTS_VERSION}`` alone, and a row with no facts
of this version replays as its projection.

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

from typing import Any, Required, TypedDict

from src.server.database.replay_facts import LEGACY_KEY
from src.server.services.history.replay import facts as replay_facts
from src.server.services.history.replay import widgets
from src.server.services.history.replay.lanes import MAIN_LANE, agent_lane

# Facts are data: ``facts_of`` drops any other version and nothing derives
# them again, so a change to their shape reads both versions instead of
# bumping this.
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
    """What a turn's archive adds to its projection, by the projected rows
    it names. Row keys are stored as lists: they round-trip through JSONB.

    - ``widgets``: ``[projected artifact id, payload]`` per archived widget,
      in streamed order, the payload carrying the data files the checkpoint
      leaves out; the id is null for a payload no projected widget took.
    - ``results``: the archived content of each tool result the checkpoint
      holds only as an eviction pointer, by call id.
    - ``durations``: ``[row key, ms]`` per projected reasoning close.
    - ``anchors``: the keys of the projected rows the archived rows found,
      in streamed order, and ``["widget", k]`` for a payload none took.
    - ``signals``: signal rows and archived widgets, in streamed order.
      ``at`` indexes the newest anchor streamed since the previous signal,
      and the signal follows it, or the newest earlier one since then that
      the projection still holds. Without ``at`` a signal follows the
      previous one.
    """

    widgets: list[list[Any]]
    results: dict[str, StoredResult]
    durations: list[list[Any]]
    anchors: list[list[Any]]
    signals: list[Signal]


class LegacyFacts(TypedDict, total=False):
    """A response row's ``replay_facts[LEGACY_KEY]``."""

    v: Required[int]
    # Sandbox image path -> the URL its archived copy was captured to.
    images: dict[str, str]
    merge: Merge
    verbatim: list[StoredRow]


# Projected/synthesized event types a turn's merge facts carry in full. A
# turn with merge facts drops its projected copies and places the archived
# ones (richer historical payloads, proven anchoring); a turn without them
# keeps the projected copies. The terminal ``error`` is not here: nothing
# projects it. An archived frame is placed as a signal row, and the response
# row stamps its kind on it, adding its own item only for a turn whose facts
# hold none (``items.terminal_items``).
STORED_PREFERRED_EVENTS = (
    "steering_delivered",
    "context_window",
    "provenance",
    "credit_usage",
    "interrupt",
    "model_fallback",
)


# Content types the projector emits: a message's anchorable chunks. Live-only
# accumulation chunks (content_type=None) and tool-only messages have none.
# Stored facts name messages by these, so the set is fixed with them.
_ANCHORABLE_CONTENT_TYPES = frozenset({"reasoning_signal", "reasoning", "text"})


def facts_of(response: dict[str, Any] | None) -> LegacyFacts | None:
    """The row's legacy facts, or None when it holds none of the version
    this code reads."""
    facts = (response or {}).get("replay_facts")
    legacy = facts.get(LEGACY_KEY) if isinstance(facts, dict) else None
    if isinstance(legacy, dict) and legacy.get("v") == FACTS_VERSION:
        return legacy
    return None


def archived_error(
    thread_id: str, response: dict[str, Any] | None
) -> dict[str, Any] | None:
    """The main lane's last error frame a legacy turn's archive held, as a
    replay item of its own: what a stub, which places no facts, closes on."""
    facts = facts_of(response)
    if facts is None:
        return None
    rows = (
        facts["verbatim"]
        if "verbatim" in facts
        else (facts.get("merge") or {}).get("signals") or ()
    )
    for row in reversed(rows):
        if row.get("event") == "error" and agent_lane(row["data"].get("agent")) == MAIN_LANE:
            return {"event": "error", "data": {**row["data"], "thread_id": thread_id}}
    return None


def _message_key(lane: str, data: dict[str, Any], occurrence: int) -> tuple:
    """A message chunk by its id and content type, the key facts name it by.
    The projector gives every message without an id the same placeholder, so
    a recurring id also carries its occurrence: a message's own chunks are
    contiguous."""
    key = ("message_chunk", lane, data.get("id"), data["content_type"])
    return key + (occurrence,) if occurrence else key


def row_keys(turn_items: list[dict[str, Any]]) -> list[tuple | None]:
    """The key of each row, or None for a row no key names: a tool call
    round by its first call id, a result by its call id, an artifact by its
    id, and a message chunk by ``_message_key``."""
    keys: list[tuple | None] = []
    running: dict[str, Any] = {}
    occurrences: dict[tuple[str, Any], int] = {}
    for item in turn_items:
        event, data = item["event"], item["data"]
        lane = agent_lane(data.get("agent"))
        key: tuple | None = None
        if event == "message_chunk" and data.get("content_type") in _ANCHORABLE_CONTENT_TYPES:
            message_id = data.get("id")
            if lane not in running or running[lane] != message_id:
                running[lane] = message_id
                occurrences[lane, message_id] = occurrences.get((lane, message_id), -1) + 1
            key = _message_key(lane, data, occurrences[lane, message_id])
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


def _key_index(turn_items: list[dict[str, Any]]) -> dict[tuple, int]:
    """Projected position of each row key.

    The last occurrence wins, so a message key covers its whole group (a
    reasoning start and close share one key, and the close is the one a
    duration lands on). The checkpoint holds a parallel round as one row,
    and facts resolved against the archive's row per call name it by any of
    its calls.
    """
    index: dict[tuple, int] = {}
    for idx, key in enumerate(row_keys(turn_items)):
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

    ``merge`` is None for a turn whose archive added nothing, whose
    projected signal rows stand. Otherwise those are replaced by the
    archived ones, in this order: widget payloads upgrade the widgets they
    were paired with, evicted results are restored, reasoning durations land
    on their closes, and each signal row follows the newest of its anchors
    that the projection still holds. Returned steering a run's own facts
    already placed is not placed twice.
    """
    if merge is None:
        return turn_items
    placed_returns = replay_facts.returned_keys(turn_items)

    turn_items = [i for i in turn_items if i["event"] not in STORED_PREFERRED_EVENTS]
    # Keyed before the widget upgrade: facts name a widget by its projected id.
    index = _key_index(turn_items)

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
