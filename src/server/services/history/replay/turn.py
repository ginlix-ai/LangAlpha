"""One turn's replay items: what its slice, its runs and its rows project to.

Everything here decides what a turn's stored lines hold, so the module is
digested into the lines epoch (``keys.projection_sources``).
"""

from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import datetime
from typing import Any

from ptc_agent.agent.transcript.classify import is_run_boundary_message
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
from src.server.services.history.slices import TurnSlice
from src.server.services.runs.sse_producer import resolve_token_threshold

# The ui record a turn settled before its row held its images keeps its
# path -> URL map in.
IMAGE_CAPTURE_UI_NAME = "image_capture"


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
    # The run of each turn still going (``ThreadRows.open_runs``).
    open_runs: dict[Any, str]

    @classmethod
    def of(cls, rows: ThreadRows) -> Inputs:
        queries_by_turn = group_queries(rows.queries)
        return cls(
            thread_id=rows.thread_id,
            turn_indexes=sorted(ti for ti in queries_by_turn if ti is not None),
            queries_by_turn=queries_by_turn,
            responses_by_turn=rows.responses_by_turn,
            usage_by_response=items._usage_rows_by_response(rows.usages),
            provenance_by_response=items._rows_by_response(rows.provenance, many=True),
            run_facts=replay_facts.RunFacts.of(rows.run_facts),
            open_runs=rows.open_runs,
        )

    def response_id(self, turn_index: Any) -> str | None:
        response = self.responses_by_turn.get(turn_index)
        return str(response["conversation_response_id"]) if response else None


def group_queries(queries: list[Any]) -> dict[Any, list[dict[str, Any]]]:
    """Query rows by ``turn_index``, in row order."""
    out: dict[Any, list[dict[str, Any]]] = {}
    for q in queries:
        if isinstance(q, dict):
            out.setdefault(q.get("turn_index"), []).append(q)
    return out


def project_turn(
    inputs: Inputs,
    turn_index: Any,
    turn: TurnSlice,
    response: dict[str, Any] | None,
    lane: run_lane.RunLane,
) -> tuple[list[dict[str, Any]], bool]:
    """One turn's replay items, and whether they are final (every run it
    launched has finished writing)."""
    thread_id = inputs.thread_id
    response_id = str(response.get("conversation_response_id")) if response else None

    segment = [
        items._user_message_item(thread_id, q, response)
        for q in inputs.queries_by_turn.get(turn_index, [])
    ]

    # A stopped or failed run's last step never committed the fallback
    # notices it streamed, so its row keeps them.
    fallbacks = replay_facts.unsaved_fallbacks(response, turn)
    turn_items = projector.with_model_fallbacks(
        thread_id,
        replace(turn, new_ui_records=[*turn.new_ui_records, *fallbacks]),
        lambda messages: history_events_to_sse(
            messages_to_history_events(messages), thread_id=thread_id
        ),
    )
    ahead = _steers_ahead_of_input(thread_id, turn, turn_items)
    # Compaction signals the turn's messages do not carry live in private
    # state keys: offload counts, which fire as the turn starts, and a
    # summary saved before its response was stamped with it, whose place in
    # the turn is lost. Both open the turn. A stamped summary already sits
    # before the response that answered from it.
    turn_items[:0] = projector.context_signal_items(thread_id, turn)
    durations = replay_facts.reasoning_durations(response)
    if durations is not None:
        replay_facts.apply_reasoning_durations(turn_items, durations)
    task_items, sealed, watermarks = lane.project(turn, settled_at(response))
    turn_items.extend(task_items)
    # A turn's image map is its own: its row's, or the ui record in its state
    # delta for a turn settled before its row held one. A thread-global map is
    # incorrect when a sandbox filename is reused later: last-write-wins would
    # rewrite the older turn's image to the newer content-addressed object.
    replay_facts.apply_image_url_map(
        turn_items,
        {
            **_collect_image_url_map(turn.new_ui_records),
            **replay_facts.images_of((response or {}).get("replay_facts")),
        },
    )
    # Table-sourced synthesis rides ahead of the legacy facts: a turn whose
    # facts carry stored copies drops these and places those instead (the
    # STORED_PREFERRED_EVENTS rule).
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
    if facts is not None:
        turn_items = legacy.apply_turn(turn_items, task_items, facts)
    # A compact or offload the user ran after the turn follows everything the
    # turn streamed. Its record is in the checkpoint alone, so it is added
    # after the legacy facts, which would drop it with the projected signal
    # rows they replace.
    turn_items.extend(projector.manual_context_window_items(thread_id, turn))

    _fill_token_thresholds(turn_items)
    # The row's terminal error and a user stop's close, after the legacy
    # facts so each can defer to the copy they placed: an archived error
    # frame takes the row's kind instead of gaining a twin.
    turn_items += items.terminal_items(thread_id, response, turn_items)

    # After the legacy facts so the artifacts they place are covered too:
    # the watermark is a fact about which runs this turn's lines contain.
    task_status.stamp_projected_watermarks(turn_items, watermarks)
    for item in [*ahead, *turn_items]:
        items._enrich(item, thread_id, turn_index, response_id)
    return [*ahead, *segment, *turn_items], sealed


def _steers_ahead_of_input(
    thread_id: str, turn: TurnSlice, turn_items: list[dict[str, Any]]
) -> list[dict[str, Any]]:
    """Pull out of ``turn_items`` the steers the turn read ahead of its own
    input (sent before it started, so the model read them first), to replay
    them above the table-sourced input as the live view drew them."""
    boundary = next(
        (i for i, m in enumerate(turn.messages) if is_run_boundary_message(m)), 0
    )
    count = sum(
        item["event"] == "steering_delivered"
        for item in history_events_to_sse(
            messages_to_history_events(turn.messages[:boundary]), thread_id=thread_id
        )
    )
    ahead: list[dict[str, Any]] = []
    for item in list(turn_items):
        if len(ahead) == count:
            break
        if item.get("event") == "steering_delivered":
            ahead.append(item)
            turn_items.remove(item)
    return ahead


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
    """Stamp the UI-ring threshold on projected token_usage events that lack
    one: a message saved before the compaction middleware stamped the
    threshold it ran under carries none, so it gets the deployment default.
    """
    for item in turn_items:
        data = item["data"]
        if (
            item["event"] == "context_window"
            and data.get("action") == "token_usage"
            and "threshold" not in data
        ):
            data["threshold"] = resolve_token_threshold()
