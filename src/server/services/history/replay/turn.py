"""One turn's replay items: what its slice, its runs and its rows project to.

Everything here decides what a turn's stored lines hold, so the module is
digested into the lines epoch (``keys.projection_sources``).
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Protocol

from src.server.database.conversation.replay_rows import ThreadRows
from src.server.services.history import projector
from src.server.services.history import task_status
from src.server.services.history.projector import (
    history_events_to_sse,
    messages_to_history_events,
)
from src.server.services.history.replay import facts as replay_facts
from src.server.services.history.replay import items
from src.server.services.history.replay import legacy
from src.server.services.history.replay import run_lane
from src.server.services.history.replay import stored_merge
from src.server.services.history.slices import TurnSlice
from src.server.services.runs.sse_producer import resolve_token_threshold

# The ui record image capture persists a turn's path -> URL map in.
IMAGE_CAPTURE_UI_NAME = "image_capture"


class Derive(Protocol):
    """A caller's hook on each turn whose legacy facts were derived from its
    stored events (the backfill). Called before the facts are placed, with
    the turn's items still unplaced and whether every run it launched has
    finished writing; what it returns is called with the placed items."""

    def __call__(
        self,
        turn_index: Any,
        response: dict[str, Any],
        facts: legacy.LegacyFacts,
        unplaced: tuple[list[dict[str, Any]], list[dict[str, Any]]],
        sealed: bool,
    ) -> Callable[[list[dict[str, Any]]], None]: ...


@dataclass(frozen=True)
class Inputs:
    """The table rows a turn's projection reads, indexed once per request."""

    thread_id: str
    turn_indexes: list[Any]
    queries_by_turn: dict[Any, list[dict[str, Any]]]
    responses_by_turn: dict[Any, dict[str, Any]]
    usage_by_response: dict[str, Any]
    provenance_by_response: dict[str, Any]
    run_facts: replay_facts.RunFacts

    @classmethod
    def of(cls, rows: ThreadRows) -> Inputs:
        queries_by_turn: dict[Any, list[dict[str, Any]]] = {}
        for q in rows.queries:
            if isinstance(q, dict):
                queries_by_turn.setdefault(q.get("turn_index"), []).append(q)
        return cls(
            thread_id=rows.thread_id,
            turn_indexes=sorted(ti for ti in queries_by_turn if ti is not None),
            queries_by_turn=queries_by_turn,
            responses_by_turn=rows.responses_by_turn,
            usage_by_response=items._usage_rows_by_response(rows.usages),
            provenance_by_response=items._rows_by_response(rows.provenance, many=True),
            run_facts=replay_facts.RunFacts.of(rows.run_facts),
        )

    def response_id(self, turn_index: Any) -> str | None:
        response = self.responses_by_turn.get(turn_index)
        return str(response["conversation_response_id"]) if response else None


def project_turn(
    inputs: Inputs,
    turn_index: Any,
    turn: TurnSlice,
    response: dict[str, Any] | None,
    lane: run_lane.RunLane,
    derive: Derive | None = None,
) -> tuple[list[dict[str, Any]], bool]:
    """One turn's replay items, and whether they are final (every run it
    launched has finished writing)."""
    thread_id = inputs.thread_id
    response_id = str(response.get("conversation_response_id")) if response else None

    segment = [
        items._user_message_item(thread_id, q, response)
        for q in inputs.queries_by_turn.get(turn_index, [])
    ]

    turn_items = history_events_to_sse(
        messages_to_history_events(turn.messages), thread_id=thread_id
    )
    # Compaction signals (offload counts, the summarize event) live in
    # private state keys, not the messages channel — re-emit them at the
    # head of the turn they landed in (live they fire before the first
    # post-compaction model call). Fallback notices follow the same
    # head placement: live they fire before the succeeding model's chunks.
    turn_items[:0] = projector.context_signal_items(
        thread_id, turn
    ) + projector.model_fallback_items(thread_id, turn)
    durations = replay_facts.reasoning_durations(response)
    if durations is not None:
        replay_facts.apply_reasoning_durations(turn_items, durations)
    task_items, sealed, watermarks = lane.project(turn, settled_at(response))
    turn_items.extend(task_items)
    # Legacy path→URL records belong to the turn whose state delta contains
    # them. A thread-global map is incorrect when a sandbox filename is
    # reused later: last-write-wins would rewrite the older turn's image to
    # the newer content-addressed object.
    replay_facts.apply_image_url_map(
        turn_items, _collect_image_url_map(turn.new_ui_records)
    )
    # Table-sourced synthesis rides ahead of the merge: a turn with stored
    # events drops these copies and replays the stored ones instead (the
    # STORED_PREFERRED_EVENTS transition rule).
    turn_items = items._insert_provenance_items(
        turn_items, inputs.provenance_by_response.get(response_id) or []
    )
    turn_items.extend(
        items._interrupt_item(thread_id, intr) for intr in turn.anchor.ending_interrupts
    )
    credit_item = items._credit_usage_item(
        thread_id, response, inputs.usage_by_response.get(response_id)
    )
    if credit_item:
        turn_items.append(credit_item)

    facts = legacy.facts_of(response)
    placed: Callable[[list[dict[str, Any]]], None] | None = None
    if facts is None:
        facts = stored_merge.derive_turn(
            turn_items,
            task_items,
            stored_merge._stored_events(response),
            main_durations_known=durations is not None,
        )
        if derive is not None and response:
            placed = derive(
                turn_index, response, facts, (turn_items, task_items), sealed
            )
    turn_items = legacy.apply_turn(turn_items, task_items, facts)
    if placed is not None:
        placed(turn_items)

    _fill_token_thresholds(turn_items)
    # Terminal error: never in stored events (persisted before it is
    # yielded live), so it appends after the merge on every turn. A user
    # stop's close is read off the same row (see stopped.stop_close_item).
    turn_items += items.terminal_items(thread_id, response, turn_items)

    # After the stored-events merge so both projected and stored-copy
    # artifacts are covered: the watermark is a fact about which runs this
    # turn's lines contain.
    task_status.stamp_projected_watermarks(turn_items, watermarks)
    for item in turn_items:
        items._enrich(item, thread_id, turn_index, response_id)
    segment.extend(turn_items)
    return segment, sealed


def settled_at(response: dict[str, Any] | None) -> datetime | None:
    """When a settled turn's row last settled, as far as the row says."""
    if not response:
        return None
    value = response.get("usage_settled_at") or response.get("created_at")
    return value if isinstance(value, datetime) else None


def _collect_image_url_map(records: list[dict[str, Any]]) -> dict[str, str]:
    url_map: dict[str, str] = {}
    for record in records:
        if not isinstance(record, dict) or record.get("name") != IMAGE_CAPTURE_UI_NAME:
            continue
        path_to_url = (record.get("props") or {}).get("path_to_url")
        if isinstance(path_to_url, dict):
            url_map.update({str(k): str(v) for k, v in path_to_url.items() if k and v})
    return url_map


def _fill_token_thresholds(turn_items: list[dict[str, Any]]) -> None:
    """Stamp the UI-ring threshold on projected token_usage events.

    The live handler adds it server-side (config, not graph state); replay
    uses the same resolver so both wires carry the same value.
    """
    for item in turn_items:
        data = item["data"]
        if (
            item["event"] == "context_window"
            and data.get("action") == "token_usage"
            and "threshold" not in data
        ):
            data["threshold"] = resolve_token_threshold()
