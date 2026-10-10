"""Replay from a turn's stored ``sse_events``, and the re-read that brings
them to a turn projected afresh.

A turn that cannot be projected replays from its stored events (or as a
stub), and its lines are never stored, so nothing here is digested into the
lines epoch.
"""

from __future__ import annotations

from typing import Any

from src.server.database import conversation as conversation_db
from src.server.services.history.replay import items
from src.server.services.history.replay import legacy
from src.server.services.history.replay import stored_merge
from src.server.services.history.replay.lines import Line, line_of
from src.server.services.history.replay.turn import Inputs


async def with_stored_events(
    responses_by_turn: dict[Any, dict[str, Any]],
    turn_indexes: list[Any],
    *,
    verbatim: bool = False,
) -> dict[Any, dict[str, Any]]:
    """``responses_by_turn`` with these turns' rows re-read whole.

    Replay rows arrive without ``sse_events`` and ``replay_facts``; only a
    turn projected afresh (or replayed from storage) needs them. The re-read
    replaces the row rather than adding the columns, so the events, the
    facts and the ``xmin`` the lines key carries come from one tuple.

    A row whose legacy facts stand in for its stored events comes without
    them, unless ``verbatim``: a replay of the stored events themselves (the
    fallbacks, a parity check).
    """
    wanted = {
        str(row["conversation_response_id"]): ti
        for ti in turn_indexes
        if (row := responses_by_turn.get(ti)) and "sse_events" not in row
    }
    if not wanted:
        return responses_by_turn
    whole = await conversation_db.get_replay_responses(
        list(wanted), legacy_facts=None if verbatim else legacy.FACTS_VERSION
    )
    out = dict(responses_by_turn)
    for response_id, ti in wanted.items():
        if response_id in whole:
            out[ti] = whole[response_id]
    return out


def build_sse_replay_items(
    thread_id: str,
    queries: list[dict[str, Any]],
    responses_by_turn: dict[Any, dict[str, Any]],
) -> list[dict[str, Any]]:
    """Replay items sourced verbatim from persisted ``sse_events`` (the fallback).

    Same ``{"event", "data"}`` shape as the checkpoint path, so the endpoint
    emits either source through one loop. The terminal error event is
    synthesized from the response row here too — it is yielded live *after*
    the persist snapshot, so stored events never contain it. So is a user
    stop's close when the archive lacks one.
    """
    out: list[dict[str, Any]] = []
    terminals_emitted: set[str] = set()
    for query in queries:
        if not isinstance(query, dict):
            continue
        turn_index = query.get("turn_index")
        response = responses_by_turn.get(turn_index)
        response_id = (
            str(response.get("conversation_response_id")) if response else None
        )
        out.append(items._user_message_item(thread_id, query, response))
        turn_items = [
            {"event": event["event"], "data": dict(event["data"])}
            for event in stored_merge._stored_events(response)
            if stored_merge._valid_stored(event)
        ]
        if response_id and response_id not in terminals_emitted:
            terminals = items.terminal_items(thread_id, response, turn_items)
            if terminals:
                terminals_emitted.add(response_id)
                turn_items += terminals
        for item in turn_items:
            items._enrich(item, thread_id, turn_index, response_id)
        out.extend(turn_items)
    return out


async def fallback_lines(
    inputs: Inputs, turn_indexes: list[Any]
) -> dict[Any, list[Line]]:
    """Each turn's replay from its stored events, or its stub without them."""
    responses = await with_stored_events(
        inputs.responses_by_turn, turn_indexes, verbatim=True
    )
    return {
        ti: [
            line_of(item)
            for item in build_sse_replay_items(
                inputs.thread_id,
                inputs.queries_by_turn.get(ti, []),
                {ti: responses[ti]} if ti in responses else {},
            )
        ]
        for ti in turn_indexes
    }
