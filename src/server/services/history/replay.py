"""Assemble checkpoint-sourced replay items for the replay endpoint.

Produces the same ``{"event": type, "data": dict}`` items as the stored
``sse_events`` path, sourcing the transcript from checkpoints (via
``CheckpointHistoryReader`` + the pure projector) and merging the
non-derivable remainder from persisted events:

- ``steering_delivered`` and ``context_window`` (token_usage, summarize,
  offload) are projected from checkpoint state — steering payloads and
  summarize fields are stamped into message ``additional_kwargs`` at emit
  time, token usage comes from ``usage_metadata``, offload counts and the
  summarize event from private-state deltas. While the sse dual-write is on,
  a turn with stored events replays those verbatim instead (richer historical
  payloads, exact mid-turn positions).
- ``provenance`` / ``credit_usage`` are table-sourced (provenance_records /
  conversation_usages rows, written at persist time); answered ``interrupt``
  cards project from the resume boundary's ``__interrupt__`` pending writes;
  the terminal ``error`` event reconstructs from the response row (both replay
  paths — it is yielded live *after* the persist snapshot, so stored events
  never contain it). While the sse dual-write is on, a turn with stored events
  replays the stored copies instead.
- ``html_widget`` artifacts prefer the stored event when present — the live
  event inlines resolved data files that are deliberately kept out of the
  checkpointer.
- Sandbox image paths in projected text resolve through ``image_capture``
  ui records; a turn whose images cannot be resolved falls back to its stored
  events wholesale (turn-level granularity keeps ordering coherent).
"""

from __future__ import annotations

import asyncio
import inspect
import json
import logging
import re
from dataclasses import dataclass, field
from typing import Any

from langchain_core.messages import HumanMessage, ToolMessage

from ptc_agent.agent.middleware.image_capture import (
    IMAGE_MD_RE,
    is_sandbox_image_path,
)
from ptc_agent.agent.middleware.large_result_eviction import TOO_LARGE_TOOL_MSG
from src.server.database.provenance import provenance_row_to_event
from src.server.handlers.streaming_handler import (
    build_credit_usage_data,
    resolve_token_threshold,
)
from src.server.services.history import projection_cache
from src.server.services.history.projector import (
    MAIN_AGENT,
    HistoryEvent,
    history_events_to_sse,
    is_run_boundary_message,
    messages_to_history_events,
)
from src.server.database import subagent_runs as sr_db
from src.server.services.history.reader import CheckpointHistoryReader
from src.server.services.history.task_status import (
    _artifact_task_id,
    resolve_task_statuses,
)
from src.server.utils.checkpoint_helpers import CheckpointBranchTipNotFound
from src.server.utils.content_normalizer import normalize_text_content
from src.server.utils.error_sanitization import (
    sanitize_error_text as _sanitize_error_text,
)
from src.utils.storage import get_bytes

logger = logging.getLogger(__name__)

# Stable prefix of the pointer LargeResultEvictionMiddleware substitutes for an
# over-threshold tool result (derived from the canonical template so it tracks
# any edit there). The evicted full content is checkpointed as this pointer, so
# a projected result carrying it must be restored from the stored event.
_EVICTED_RESULT_PREFIX = TOO_LARGE_TOOL_MSG.split("{", 1)[0]

# Stored events replayed verbatim (anchored to their original position):
# non-derivable payloads, plus resolved-interrupt cards and error markers —
# `interrupt` renders answered HITL cards on replay (a pending interrupt is
# also re-emitted from the checkpoint tip; the frontend dedups by
# interrupt_id), and `error` keeps wire parity with sse replay.
# `context_window` and `steering_delivered` are checkpoint-projected, but a
# turn with stored events prefers those verbatim (see _merge_stored_payloads).
_PASSTHROUGH_EVENTS = (
    "context_window",
    "provenance",
    "steering_delivered",
    # No projected twin: nothing was injected, so only the captured/stored
    # copy can ever surface a returned steering input.
    "steering_returned",
    "credit_usage",
    "interrupt",
    "error",
    "model_fallback",
)

# Projected/synthesized event types the stored stream also carries in full.
# While the sse dual-write is on, a turn with stored events drops its
# projected copies and replays the stored ones (richer historical payloads,
# proven anchoring); post-cutover turns have no stored events, so the
# projected path serves. The terminal ``error`` event is NOT here — stored
# events never contain it (persisted before it is yielded), so it is
# synthesized from the response row on both paths unconditionally.
_STORED_PREFERRED_EVENTS = (
    "steering_delivered",
    "context_window",
    "provenance",
    "credit_usage",
    "interrupt",
    "model_fallback",
)

IMAGE_CAPTURE_UI_NAME = "image_capture"
MODEL_FALLBACK_UI_NAME = "model_fallback"

# Mirrors the streaming handler's model_fallback field whitelist.
_MODEL_FALLBACK_FIELDS = (
    "from_model",
    "to_model",
    "from_is_primary",
    "status_code",
    "attempts_on_from",
)

class CheckpointReplayUnavailable(Exception):
    """Checkpoint history cannot faithfully cover this thread's replay."""


async def build_checkpoint_replay_items(
    thread_id: str,
    queries: list[dict[str, Any]],
    responses_by_turn: dict[Any, dict[str, Any]],
    branch_tip_checkpoint_id: str | None = None,
    last_n_turns: int | None = None,
    usages: list[dict[str, Any]] | None = None,
    provenance: list[dict[str, Any]] | None = None,
) -> list[dict[str, Any]]:
    """Build the replay item list from checkpoints.

    With ``last_n_turns`` set, only the most recent N turns are materialized
    (windowed initial load — latency bounded by the window, not thread length);
    otherwise the full thread is built.

    Turns pair to persisted query rows by stamped ``turn_index`` metadata
    (ordinal-anchored for pre-stamping threads); a persisted turn with no
    committed boundary — the in-flight active turn — replays as its
    user_message stub only. Raises ``CheckpointReplayUnavailable`` when
    coverage cannot be established (missing checkpoints, inconsistent
    pairing) — the endpoint's ``auto`` mode falls back to stored events on
    that signal. Steered threads replay natively (the steering message is
    checkpointed mid-slice); legacy steering-*backfilled* turns have a
    completed response with no boundary, so pairing raises and they stay on
    the sse path.

    Settled turns serve from the per-turn projection cache when every entry
    is present (no state materialization); any miss rebuilds from checkpoints
    and backfills the cache. Widget ``data_ref`` resolution always runs on
    the way out — entries store the unresolved ref.
    """
    reader = CheckpointHistoryReader.get_instance()
    turn_indexes = sorted(
        {
            q.get("turn_index")
            for q in queries
            if isinstance(q, dict) and q.get("turn_index") is not None
        }
    )
    queries_by_turn: dict[Any, list[dict[str, Any]]] = {}
    for q in queries:
        if isinstance(q, dict):
            queries_by_turn.setdefault(q.get("turn_index"), []).append(q)

    items: list[dict[str, Any]] | None = None
    if projection_cache.cache_active():
        items = await _assemble_from_cache(
            reader,
            thread_id,
            queries_by_turn,
            responses_by_turn,
            turn_indexes,
            branch_tip_checkpoint_id,
            last_n_turns,
        )
    if items is None:
        items = await _build_and_backfill(
            reader,
            thread_id,
            queries_by_turn,
            responses_by_turn,
            turn_indexes,
            branch_tip_checkpoint_id,
            last_n_turns,
            usages,
            provenance,
        )
    await _resolve_widget_data_refs(items)
    return items


async def _assemble_from_cache(
    reader: CheckpointHistoryReader,
    thread_id: str,
    queries_by_turn: dict[Any, list[dict[str, Any]]],
    responses_by_turn: dict[Any, dict[str, Any]],
    turn_indexes: list[Any],
    branch_tip_checkpoint_id: str | None,
    last_n_turns: int | None,
) -> list[dict[str, Any]] | None:
    """Concatenate cached per-turn entries — a light boundary walk plus one
    raw tip read, no state materialization. Returns None on any miss (the
    caller rebuilds and backfills). Pairing guards raise the same
    ``CheckpointReplayUnavailable`` signals as the full build."""
    try:
        anchors, tip_id = await reader.aget_turn_anchors(
            thread_id, branch_tip_checkpoint_id
        )
    except CheckpointBranchTipNotFound as e:
        raise CheckpointReplayUnavailable(str(e)) from e
    if not anchors or tip_id is None or any(
        a.tail_checkpoint_id is None for a in anchors
    ):
        return None
    if last_n_turns is not None:
        anchors = anchors[-max(1, min(last_n_turns, len(anchors))) :]

    pairs = _pair_turns_to_queries(
        anchors, turn_indexes, responses_by_turn, windowed=last_n_turns is not None
    )
    cached = await projection_cache.get_cached_turns(
        thread_id,
        [a.tail_checkpoint_id for _, a in pairs if a is not None],
    )
    if any(v is None for v in cached.values()):
        return None

    items: list[dict[str, Any]] = []
    for turn_index, anchor in pairs:
        if anchor is None:
            items.extend(
                _stub_turn_items(
                    thread_id, turn_index, queries_by_turn, responses_by_turn
                )
            )
        else:
            items.extend(cached[anchor.tail_checkpoint_id])
    for interrupt in await reader.aget_tip_interrupts(thread_id, tip_id):
        items.append(_interrupt_item(thread_id, interrupt))
    return items


async def _build_and_backfill(
    reader: CheckpointHistoryReader,
    thread_id: str,
    queries_by_turn: dict[Any, list[dict[str, Any]]],
    responses_by_turn: dict[Any, dict[str, Any]],
    turn_indexes: list[Any],
    branch_tip_checkpoint_id: str | None,
    last_n_turns: int | None,
    usages: list[dict[str, Any]] | None,
    provenance: list[dict[str, Any]] | None,
) -> list[dict[str, Any]]:
    """Materialize checkpoint state and project every requested turn, storing
    each settled turn's finished segment in the projection cache."""
    try:
        if last_n_turns is not None:
            history = await reader.aget_recent_history(
                thread_id, last_n_turns, branch_tip_checkpoint_id
            )
        else:
            history = await reader.aget_thread_history(
                thread_id, branch_tip_checkpoint_id
            )
    except CheckpointBranchTipNotFound as e:
        raise CheckpointReplayUnavailable(str(e)) from e

    if not history.turns:
        raise CheckpointReplayUnavailable("no checkpoint turns found")
    pairs = _pair_turns_to_queries(
        history.turns,
        turn_indexes,
        responses_by_turn,
        windowed=last_n_turns is not None,
    )

    usage_by_response = _usage_rows_by_response(usages)
    provenance_by_response = _rows_by_response(provenance, many=True)

    items: list[dict[str, Any]] = []
    task_lane = _TaskLaneProjector(thread_id, windowed=last_n_turns is not None)
    await task_lane.prepare(reader, pairs)

    # Stores are deferred past trailing_items(): only then is it known which
    # turns carry trailing salvage and must stay out of the cache.
    cacheable: list[tuple[Any, str | None, list[dict[str, Any]]]] = []
    for turn_index, turn in pairs:
        if turn is None:
            items.extend(
                _stub_turn_items(
                    thread_id, turn_index, queries_by_turn, responses_by_turn
                )
            )
            continue

        response = responses_by_turn.get(turn_index)
        response_id = (
            str(response.get("conversation_response_id")) if response else None
        )
        stored_events = _stored_events(response)

        segment = [
            _user_message_item(thread_id, q)
            for q in queries_by_turn.get(turn_index, [])
        ]

        turn_items = history_events_to_sse(
            messages_to_history_events(turn.messages), thread_id=thread_id
        )
        # Compaction signals (offload counts, the summarize event) live in
        # private state keys, not the messages channel — re-emit them at the
        # head of the turn they landed in (live they fire before the first
        # post-compaction model call). Fallback notices follow the same
        # head placement: live they fire before the succeeding model's chunks.
        turn_items[:0] = _context_signal_items(thread_id, turn) + _model_fallback_items(
            thread_id, turn
        )
        task_items, turn_task_ids = task_lane.project_for_turn(
            turn, turn_index, response_id
        )
        turn_items.extend(task_items)
        # Legacy path→URL records belong to the turn whose state delta contains
        # them. A thread-global map is incorrect when a sandbox filename is
        # reused later: last-write-wins would rewrite the older turn's image to
        # the newer content-addressed object.
        _apply_image_url_map(
            turn_items, _collect_image_url_map(turn.new_ui_records)
        )
        # Table-sourced synthesis rides ahead of the merge: a turn with stored
        # events drops these copies and replays the stored ones instead (the
        # _STORED_PREFERRED_EVENTS transition rule).
        turn_items = _insert_provenance_items(
            turn_items, provenance_by_response.get(response_id) or []
        )
        turn_items.extend(
            _interrupt_item(thread_id, intr) for intr in turn.ending_interrupts
        )
        credit_item = _credit_usage_item(
            thread_id, response, usage_by_response.get(response_id)
        )
        if credit_item:
            turn_items.append(credit_item)

        if _has_unresolved_sandbox_images(turn_items) and stored_events:
            # Non-derivable image URLs live only in the stored events for
            # this turn — replay the MAIN lane from storage wholesale. The
            # task lane keeps the normal path's contract exactly, via the
            # same merge: projection is the transcript authority for the
            # agents it claimed (the checkpoint outranks every stored shape
            # — collector full copy, user-stop partial snapshot, legacy
            # interleave), while _merge_stored_payloads supplies what the
            # checkpoint can't — stored-preferred signal rows anchored in
            # position, evicted tool results restored from the fuller
            # stored copy — and stored transcript rows serve only as
            # anchors, never duplicates. Stored rows outside the claimed
            # lanes replay verbatim: main rows, task-scoped custom
            # artifacts (the projector never emits task artifacts), and
            # unclaimed agents' rows (in_progress, cascade-truncated,
            # unclaimed legacy — nothing projected, nothing to merge).
            # Copy the nested ``data`` (like build_sse_replay_items):
            # _enrich stamps into it, and the source dicts are the
            # request's pristine ``sse_events`` rows.
            projected_task_agents = {
                agent
                for i in task_items
                if (agent := str((i.get("data") or {}).get("agent", "")))
            }
            claimed_rows: list[dict[str, Any]] = []
            verbatim_rows: list[dict[str, Any]] = []
            for e in stored_events:
                if not _valid_stored(e):
                    continue
                agent = str((e.get("data") or {}).get("agent", ""))
                if agent in projected_task_agents and e["event"] != "artifact":
                    claimed_rows.append(e)
                else:
                    verbatim_rows.append(
                        {"event": e["event"], "data": dict(e["data"])}
                    )
            turn_items = verbatim_rows + _merge_stored_payloads(
                task_items, claimed_rows, task_lane.turn_lossy_lanes
            )
        else:
            turn_items = _merge_stored_payloads(
                turn_items, stored_events, task_lane.turn_lossy_lanes
            )

        _fill_token_thresholds(turn_items)
        # Terminal error: never in stored events (persisted before it is
        # yielded live), so it appends after the merge on every turn.
        error_item = _error_item(thread_id, response)
        if error_item:
            turn_items.append(error_item)

        # After the stored-events merge so both projected and stored-copy
        # artifacts are covered, and before caching: the watermark is a fact
        # about what this build claimed, and a cached turn's claims are final
        # (a turn with a still-writing run is never cached).
        _stamp_projected_watermarks(turn_items, task_lane.claimed_watermarks)
        for item in turn_items:
            _enrich(item, thread_id, turn_index, response_id)
        segment.extend(turn_items)
        items.extend(segment)
        # A still-writing subagent transcript (tail mode) must not be frozen:
        # its task-ns writes never move this turn's tail, so a partial entry
        # would never be invalidated. Rebuild-per-read until the task's
        # stream finalizes, then the next read caches the full transcript.
        #
        # Same discipline while a settled lane's archive is still owed: the
        # collector races the refresh-at-finalize, and it never invalidates
        # this cache. A lossy lane's capture-only rows and the fuller stored
        # copy behind a projected eviction pointer exist only in that
        # archive — caching before it lands would freeze the loss for the
        # cache TTL. A stored transcript-class row clears the debt: those
        # classes are written only by the atomic archive writers (collector,
        # stop drain), never by the live root path.
        awaiting_archive = set(task_lane.turn_lossy_lanes)
        for i in turn_items:
            d = i.get("data") or {}
            agent = str(d.get("agent", ""))
            if (
                i.get("event") == "tool_call_result"
                and agent.startswith("task:")
                and isinstance(d.get("content"), str)
                and d["content"].startswith(_EVICTED_RESULT_PREFIX)
            ):
                awaiting_archive.add(agent)
        if awaiting_archive:
            awaiting_archive -= {
                str((e.get("data") or {}).get("agent", ""))
                for e in stored_events or []
                if _valid_stored(e) and e["event"] in _ARCHIVE_EVIDENCE_EVENTS
            }
        if not awaiting_archive and not await projection_cache.task_streams_live(
            thread_id, turn_task_ids
        ):
            cacheable.append((turn_index, turn.tail_checkpoint_id, segment))

    items.extend(await task_lane.trailing_items())
    # Trailing salvage rides items but belongs to no turn's checkpoint range,
    # so the all-cache-hit fast path (which never runs the task lane) would
    # silently drop it. Keep the salvage-stamped turn uncacheable — skip its
    # store and evict any entry from before the orphan appeared — so every
    # read misses there and rebuilds until the salvage resolves.
    salvaged = task_lane.salvaged_turn_indexes
    for turn_index, tail_checkpoint_id, segment in cacheable:
        if turn_index not in salvaged:
            await projection_cache.store_turn(
                thread_id, tail_checkpoint_id, segment
            )
    if salvaged:
        tails_by_turn = {ti: t.tail_checkpoint_id for ti, t in pairs if t is not None}
        await projection_cache.delete_turns(
            thread_id,
            [tails_by_turn[ti] for ti in salvaged if tails_by_turn.get(ti)],
        )

    for interrupt in history.interrupts:
        items.append(_interrupt_item(thread_id, interrupt))

    return items


def _stamp_projected_watermarks(
    items: list[dict[str, Any]], watermarks: dict[str, float]
) -> None:
    """Stamp ``payload.projected_run_started_ms`` onto each task artifact
    whose task has claimed runs: the newest run this payload's transcript
    contains. Replaces items copy-on-write — stored-event items share payload
    dicts with the request's pristine rows."""
    if not watermarks:
        return
    for i, item in enumerate(items):
        data = item.get("data") if isinstance(item, dict) else None
        task_id = _artifact_task_id(data)
        if not task_id or task_id not in watermarks:
            continue
        payload = {
            **(data.get("payload") or {}),
            "projected_run_started_ms": watermarks[task_id],
        }
        items[i] = {**item, "data": {**data, "payload": payload}}


def _stub_turn_items(
    thread_id: str,
    turn_index: Any,
    queries_by_turn: dict[Any, list[dict[str, Any]]],
    responses_by_turn: dict[Any, dict[str, Any]],
) -> list[dict[str, Any]]:
    """A persisted turn with no committed boundary: the in-flight active turn
    (frontend attaches to the live run via /status + run_id) or a run that
    never checkpointed. The user_message stub — plus the terminal error for an
    errored run — is the whole replay. Never cached."""
    items = [
        _user_message_item(thread_id, q) for q in queries_by_turn.get(turn_index, [])
    ]
    response = responses_by_turn.get(turn_index)
    error_item = _error_item(thread_id, response)
    if error_item:
        response_id = (
            str(response.get("conversation_response_id")) if response else None
        )
        _enrich(error_item, thread_id, turn_index, response_id)
        items.append(error_item)
    return items


def _pair_turns_to_queries(
    turns: list[Any],
    turn_indexes: list[Any],
    responses_by_turn: dict[Any, dict[str, Any]],
    windowed: bool,
) -> list[tuple[Any, Any]]:
    """Pair checkpoint turns with persisted turn_indexes, metadata-keyed.

    A ``TurnSlice`` pairs by its stamped ``turn_index`` metadata when present,
    falling back to head-anchored ordinal position (pre-stamping threads;
    resume boundaries never carry metadata). Returns ordered
    ``(turn_index, TurnSlice | None)`` — a ``None`` slice is a persisted turn
    with no committed boundary (the in-flight active turn, or a run that never
    checkpointed), replayed as its user_message stub only. In windowed mode,
    unpaired rows older than the window are dropped, not stubbed.

    Raises ``CheckpointReplayUnavailable`` on anything a projection could
    silently mislabel: a stamped index missing from the rows, non-monotonic
    pairing, or a *completed* response with no boundary (a completed turn
    always persists its boundary pointer, so checkpoints can't cover it).
    """
    known = set(turn_indexes)
    pairs: list[tuple[Any, Any]] = []
    for turn in turns:
        if turn.turn_index is not None:
            ti = turn.turn_index
            if ti not in known:
                raise CheckpointReplayUnavailable(
                    f"checkpoint turn_index {ti} has no persisted turn"
                )
        elif turn.turn_ordinal < len(turn_indexes):
            ti = turn_indexes[turn.turn_ordinal]
        else:
            raise CheckpointReplayUnavailable(
                "more checkpoint turns than persisted turns"
            )
        pairs.append((ti, turn))

    paired_tis = [ti for ti, _ in pairs]
    if paired_tis != sorted(set(paired_tis)):
        raise CheckpointReplayUnavailable("turn pairing is not monotonic")

    window_start = paired_tis[0]
    paired = set(paired_tis)
    for ti in turn_indexes:
        if ti in paired:
            continue
        if windowed and ti < window_start:
            continue
        if (responses_by_turn.get(ti) or {}).get("status") == "completed":
            raise CheckpointReplayUnavailable(
                f"persisted turn {ti} completed but has no checkpoint boundary"
            )
        pairs.append((ti, None))

    pairs.sort(key=lambda p: p[0])
    return pairs


def build_sse_replay_items(
    thread_id: str,
    queries: list[dict[str, Any]],
    responses_by_turn: dict[Any, dict[str, Any]],
) -> list[dict[str, Any]]:
    """Replay items sourced verbatim from persisted ``sse_events`` (the fallback).

    Same ``{"event", "data"}`` shape as the checkpoint path, so the endpoint
    emits either source through one loop. The terminal error event is
    synthesized from the response row here too — it is yielded live *after*
    the persist snapshot, so stored events never contain it.
    """
    items: list[dict[str, Any]] = []
    errors_emitted: set[str] = set()
    for query in queries:
        if not isinstance(query, dict):
            continue
        turn_index = query.get("turn_index")
        response = responses_by_turn.get(turn_index)
        response_id = (
            str(response.get("conversation_response_id")) if response else None
        )
        items.append(_user_message_item(thread_id, query))
        for event in _stored_events(response):
            if not _valid_stored(event):
                continue
            item = {"event": event["event"], "data": dict(event["data"])}
            _enrich(item, thread_id, turn_index, response_id)
            items.append(item)
        if response_id and response_id not in errors_emitted:
            error_item = _error_item(thread_id, response)
            if error_item:
                errors_emitted.add(response_id)
                _enrich(error_item, thread_id, turn_index, response_id)
                items.append(error_item)
    return items


def _user_message_item(thread_id: str, query: dict[str, Any]) -> dict[str, Any]:
    payload = {
        "thread_id": thread_id,
        "turn_index": query.get("turn_index"),
        "content": query.get("content"),
        "timestamp": query.get("created_at"),
        "metadata": query.get("metadata"),
    }
    if query.get("type") == "system":
        payload["query_type"] = "system"
    return {"event": "user_message", "data": payload}


def _interrupt_item(thread_id: str, interrupt: dict[str, Any]) -> dict[str, Any]:
    value = interrupt.get("value")
    action_requests: list[Any] = []
    if isinstance(value, dict):
        action_requests = value.get("action_requests", [])
        if not action_requests and "description" in value:
            action_requests = [{"description": value["description"]}]
    elif isinstance(value, list):
        action_requests = value
    elif isinstance(value, str):
        action_requests = [{"description": value}]
    return {
        "event": "interrupt",
        "data": {
            "thread_id": thread_id,
            "interrupt_id": interrupt.get("id"),
            "action_requests": action_requests,
            "role": "assistant",
            "finish_reason": "interrupt",
        },
    }


def _rows_by_response(
    rows: list[dict[str, Any]] | None, many: bool = False
) -> dict[str, Any]:
    """Key table rows by stringified ``conversation_response_id``.

    ``many=True`` groups into lists (provenance); otherwise last row wins
    (usage — one row per response by construction).
    """
    result: dict[str, Any] = {}
    for row in rows or []:
        response_id = row.get("conversation_response_id")
        if response_id is None:
            continue
        if many:
            result.setdefault(str(response_id), []).append(row)
        else:
            result[str(response_id)] = row
    return result


def _usage_rows_by_response(
    rows: list[dict[str, Any]] | None,
) -> dict[str, dict[str, Any]]:
    """Key main-workflow usage rows by response id.

    Background subagents deliberately persist one ``msg_type='task'`` row per
    task under the parent response id. Those rows are billing records, not the
    terminal ``credit_usage`` payload emitted by the main workflow, so replay
    must never let their later timestamps replace the main row.
    """
    return _rows_by_response(
        [row for row in rows or [] if row.get("msg_type") != "task"]
    )


def _insert_provenance_items(
    turn_items: list[dict[str, Any]], rows: list[dict[str, Any]]
) -> list[dict[str, Any]]:
    """Synthesize ``provenance`` events from table rows, anchored in position.

    Each row inserts after the ``tool_call_result`` matching its
    ``tool_call_id`` (where the live event fired); rows with no matching
    anchor in this projection append at the turn tail in row order.
    """
    if not rows:
        return turn_items
    by_anchor: dict[str, list[dict[str, Any]]] = {}
    unanchored: list[dict[str, Any]] = []
    for row in rows:
        item = {"event": "provenance", "data": provenance_row_to_event(row)}
        tool_call_id = item["data"].get("tool_call_id")
        if tool_call_id:
            by_anchor.setdefault(tool_call_id, []).append(item)
        else:
            unanchored.append(item)

    merged: list[dict[str, Any]] = []
    for item in turn_items:
        merged.append(item)
        if item["event"] == "tool_call_result":
            merged.extend(by_anchor.pop(item["data"].get("tool_call_id"), ()))
    for leftover in by_anchor.values():
        merged.extend(leftover)
    merged.extend(unanchored)
    return merged


_CREDIT_USAGE_STATUSES = ("completed", "interrupted")


def _credit_usage_item(
    thread_id: str,
    response: dict[str, Any] | None,
    usage_row: dict[str, Any] | None,
) -> dict[str, Any] | None:
    """Reconstruct the terminal ``credit_usage`` event from the usage row.

    Only for statuses whose live stream reached the post-workflow credit emit
    (completed / interrupted) — errored and cancelled runs persist usage but
    never emitted the event.
    """
    if not usage_row or not response:
        return None
    if response.get("status") not in _CREDIT_USAGE_STATUSES:
        return None
    total_credits = usage_row.get("total_credits")
    created_at = usage_row.get("created_at")
    return {
        "event": "credit_usage",
        "data": build_credit_usage_data(
            thread_id,
            usage_row.get("token_usage") or {},
            float(total_credits) if total_credits is not None else 0.0,
            timestamp=(
                created_at.isoformat()
                if hasattr(created_at, "isoformat")
                else created_at
            ),
        ),
    }


def _error_item(
    thread_id: str, response: dict[str, Any] | None
) -> dict[str, Any] | None:
    """Reconstruct the terminal ``error`` event from an errored response row."""
    if not response or response.get("status") != "error":
        return None
    errors = response.get("errors")
    if not errors or not isinstance(errors, list):
        return None
    metadata = response.get("metadata") or {}
    data: dict[str, Any] = {
        "thread_id": thread_id,
        # Rows may predate persistence-side sanitization. Scrub again at the
        # trust boundary so historical secrets never reach the replay wire.
        "error": _sanitize_error_text(str(errors[-1])),
        "type": "workflow_error",
    }
    for key in ("error_type", "error_class"):
        if isinstance(metadata, dict) and metadata.get(key):
            data[key] = metadata[key]
    return {"event": "error", "data": data}


def _stored_events(response: dict[str, Any] | None) -> list[dict[str, Any]]:
    if not response:
        return []
    sse_events = response.get("sse_events")
    return sse_events if isinstance(sse_events, list) else []


def _valid_stored(event: Any) -> bool:
    return (
        isinstance(event, dict)
        and bool(event.get("event"))
        and isinstance(event.get("data"), dict)
    )


# Content types the projector emits — a message's anchorable chunks. Live-only
# accumulation chunks (content_type=None) and tool-only messages have none, so
# both streams enumerate the same messages when keyed on these.
_ANCHORABLE_CONTENT_TYPES = frozenset({"reasoning_signal", "reasoning", "text"})

# Task-run terminals where the live capture can exceed the checkpoint: a run
# that raised or was killed mid-write streamed output whose message never
# committed. completed/interrupted runs end on a committed boundary, and a
# completed run's archive may hold phantom partials from a mid-stream model
# retry — resurrecting those would double-render.
_LOSSY_TERMINAL_STATUSES = frozenset({"error", "cancelled"})

# Only these classes prove a lane's archive landed: the collector and the
# stop drain both replay the captured stream from its start (the opener is
# always first), while the live root path persists a task's custom
# artifacts — and nothing else — before any archive write, so an artifact
# row is not evidence.
_ARCHIVE_EVIDENCE_EVENTS = frozenset(
    {"user_message", "message_chunk", "tool_calls", "tool_call_result"}
)

# Image targets are rewritten between capture and checkpoint (sandbox path
# -> durable URL; the checkpoint rewrite sees whole messages while the
# archive rewrite scans row fragments), so copy-matching must ignore them.
_IMAGE_MD_RE = re.compile(r"!\[([^\]]*)\]\([^)]*\)")


def _lane(agent: Any) -> str:
    return agent if isinstance(agent, str) and agent.startswith("task:") else "main"


def _message_lane_ordinals(
    items: list[dict[str, Any]],
) -> tuple[dict[str, int], dict[str, int]]:
    """Ordinal of each message within its lane, over anchorable-content
    messages, plus the per-lane message count.

    Message ids differ between the streams (live chunks carry ``lc_run--…`` run
    ids, checkpointed messages the provider id), so a chunk can't anchor by id.
    But both streams enumerate a lane's messages in the same order, so the
    lane-relative ordinal is a stable cross-stream identity — computed the same
    way on each stream, no translation table needed.
    """
    counters: dict[str, int] = {}
    ordinal_by_id: dict[str, int] = {}
    for item in items:
        data = item["data"]
        if item["event"] != "message_chunk":
            continue
        if data.get("content_type") not in _ANCHORABLE_CONTENT_TYPES:
            continue
        message_id = data.get("id")
        if not message_id or message_id in ordinal_by_id:
            continue
        lane = _lane(data.get("agent"))
        ordinal_by_id[message_id] = counters.get(lane, 0)
        counters[lane] = ordinal_by_id[message_id] + 1
    return ordinal_by_id, counters


def _anchor_key(
    event_type: str, data: dict[str, Any], ordinals: dict[str, int]
) -> tuple | None:
    """Identity shared by a stored event and its projected counterpart.

    ``ordinals`` maps message id → lane ordinal for the same stream ``data``
    came from (see ``_message_lane_ordinals``).
    """
    if event_type == "message_chunk":
        content_type, message_id = data.get("content_type"), data.get("id")
        if content_type not in _ANCHORABLE_CONTENT_TYPES or message_id not in ordinals:
            return None
        return ("message_chunk", _lane(data.get("agent")), ordinals[message_id], content_type)
    if event_type == "tool_calls":
        tool_call_ids = tuple(
            tc.get("id") for tc in data.get("tool_calls") or [] if tc.get("id")
        )
        return ("tool_calls", tool_call_ids[0]) if tool_call_ids else None
    if event_type == "tool_call_result":
        tool_call_id = data.get("tool_call_id")
        return ("tool_call_result", tool_call_id) if tool_call_id else None
    if event_type == "artifact":
        artifact_id = data.get("artifact_id")
        return ("artifact", artifact_id) if artifact_id else None
    return None


def _is_widget(event_type: str, data: dict[str, Any]) -> bool:
    return event_type == "artifact" and data.get("artifact_type") == "html_widget"


async def _resolve_widget_data_refs(turn_items: list[dict[str, Any]]) -> None:
    """Inline widget data referenced by a content-addressed ``data_ref``.

    ShowWidget offloads large resolved data to object storage and checkpoints
    only ``data_ref {key, sha256, size}``. Runs after the stored-payload merge,
    so a widget already carrying ``data`` (stored event, or small inlined
    payload) skips the storage read. Unresolvable refs are left in place — the
    frontend renders the widget without its data files.
    """
    pending: list[tuple[dict[str, Any], dict[str, Any]]] = []  # (payload, data_ref)
    for item in turn_items:
        if not _is_widget(item["event"], item["data"]):
            continue
        payload = item["data"].get("payload")
        if not isinstance(payload, dict) or "data" in payload:
            continue
        ref = payload.get("data_ref")
        if not isinstance(ref, dict) or not ref.get("key"):
            continue
        pending.append((payload, ref))
    if not pending:
        return

    raws = await asyncio.gather(
        *(asyncio.to_thread(get_bytes, ref["key"]) for _payload, ref in pending)
    )
    for (payload, ref), raw in zip(pending, raws):
        if raw is None:
            logger.warning(f"[REPLAY] widget data_ref unreadable: {ref['key']}")
            continue
        try:
            payload["data"] = json.loads(raw)
        except (ValueError, UnicodeDecodeError):
            logger.warning(f"[REPLAY] widget data_ref not valid JSON: {ref['key']}")


def _merge_stored_payloads(
    turn_items: list[dict[str, Any]],
    stored_events: list[dict[str, Any]],
    resurrect_lanes: set[str] | frozenset[str] = frozenset(),
) -> list[dict[str, Any]]:
    """Merge non-derivable stored events into a projected turn, in position.

    Widget payloads upgrade in place — the stored event carries the resolved
    data files the checkpoint deliberately omits. Pairing is ordinal: the live
    widget artifact_id is random and the stored event has no tool_call_id, but
    both streams order widgets by tool execution.

    Passthrough events (context_window, provenance, …) insert after the
    projected position of their nearest preceding stored anchor, reproducing
    their original mid-turn placement instead of piling up at the end.

    ``resurrect_lanes``: lanes whose claimed run died mid-write. Their stored
    rows beyond everything the checkpoint enumerates (see
    ``_is_lost_transcript_row``) are real output the user saw live that no
    checkpoint holds — replayed after the lane's last projected item instead
    of being dropped as unanchored.
    """
    stored = [e for e in stored_events if _valid_stored(e)]
    if not stored:
        return turn_items

    turn_items = [
        i for i in turn_items if i["event"] not in _STORED_PREFERRED_EVENTS
    ]

    projected_widgets = [i for i in turn_items if _is_widget(i["event"], i["data"])]
    stored_widgets = [e for e in stored if _is_widget(e["event"], e["data"])]
    for item, stored_event in zip(projected_widgets, stored_widgets):
        item["data"] = dict(stored_event["data"])
    # Stored widgets beyond the projected count (projection missed the
    # ToolMessage artifact) are inserted by anchor like passthrough events.
    extra_widget_ids = {id(e) for e in stored_widgets[len(projected_widgets):]}

    _restore_evicted_results(turn_items, stored)

    projected_ordinals, projected_lane_counts = _message_lane_ordinals(turn_items)
    stored_ordinals, _ = _message_lane_ordinals(stored)
    committed_stored_ids = (
        _misaligned_committed_ids(turn_items, stored, resurrect_lanes)
        if resurrect_lanes
        else set()
    )

    index_by_key: dict[tuple, int] = {}
    for idx, item in enumerate(turn_items):
        key = _anchor_key(item["event"], item["data"], projected_ordinals)
        if key:
            # Last occurrence wins so an anchor covers its whole message group
            # (e.g. both reasoning-signal items share one key).
            index_by_key[key] = idx

    last_lane_index: dict[str, int] = {}
    for idx, item in enumerate(turn_items):
        last_lane_index[_lane(item["data"].get("agent"))] = idx

    inserts_after: dict[int, list[dict[str, Any]]] = {}
    anchor_idx = -1  # before the first projected item
    for event in stored:
        key = _anchor_key(event["event"], event["data"], stored_ordinals)
        if key is not None and key in index_by_key:
            anchor_idx = index_by_key[key]
            continue
        if _is_lost_transcript_row(
            event, key, stored_ordinals, projected_lane_counts,
            resurrect_lanes, last_lane_index, committed_stored_ids,
        ):
            # Anchor the resurrection point too, so following stored rows
            # (the run's error, further lost rows) keep their relative order.
            anchor_idx = max(
                anchor_idx, last_lane_index[_lane(event["data"].get("agent"))]
            )
            inserts_after.setdefault(anchor_idx, []).append(
                {"event": event["event"], "data": dict(event["data"])}
            )
            continue
        if event["event"] in _PASSTHROUGH_EVENTS or id(event) in extra_widget_ids:
            inserts_after.setdefault(anchor_idx, []).append(
                {"event": event["event"], "data": dict(event["data"])}
            )

    merged = list(inserts_after.get(-1, []))
    for idx, item in enumerate(turn_items):
        merged.append(item)
        merged.extend(inserts_after.get(idx, ()))
    return merged


def _is_lost_transcript_row(
    event: dict[str, Any],
    key: tuple | None,
    stored_ordinals: dict[str, int],
    projected_lane_counts: dict[str, int],
    resurrect_lanes: set[str] | frozenset[str],
    last_lane_index: dict[str, int],
    committed_stored_ids: set[str],
) -> bool:
    """Stored transcript content the checkpoint never committed.

    Reachable only for lanes whose claimed run died mid-write: a message
    whose lane ordinal lies beyond every projected message, or a tool round
    with no projected twin, was streamed live but never checkpointed.
    Anchored rows never reach here (they are consumed as duplicates),
    empty-content chunks (the stop path's synthetic close) stay dropped,
    and a misaligned committed copy (``committed_stored_ids``) is the
    checkpointed message wearing a shifted ordinal, not lost output.
    """
    data = event["data"]
    lane = _lane(data.get("agent"))
    if lane not in resurrect_lanes or lane not in last_lane_index:
        return False
    if event["event"] == "message_chunk":
        content = data.get("content")
        message_id = data.get("id")
        return (
            isinstance(content, str)
            and bool(content)
            and data.get("content_type") in _ANCHORABLE_CONTENT_TYPES
            and message_id in stored_ordinals
            and stored_ordinals[message_id] >= projected_lane_counts.get(lane, 0)
            and message_id not in committed_stored_ids
        )
    if event["event"] in ("tool_calls", "tool_call_result"):
        return key is not None
    return False


def _misaligned_committed_ids(
    turn_items: list[dict[str, Any]],
    stored: list[dict[str, Any]],
    resurrect_lanes: set[str] | frozenset[str],
) -> set[str]:
    """Stored message ids that are a checkpointed message's shifted copy.

    A phantom partial (a model attempt that failed before an in-run retry)
    shifts a lane's stored ordinals, so a committed message's stored copy
    can land beyond the projected count and look trailing. Alignment is the
    match-maximizing in-order pairing (LCS) of stored messages against
    projected messages: a phantom can never consume a projected slot a real
    copy needs, and occurrences stay distinct — a stored message repeating
    a projected text beyond the pairing is lost output, not a copy. Ties
    break toward the earliest stored message, so the copy is the first
    occurrence and later repeats resurrect.
    """
    projected = _lane_message_signatures(turn_items, resurrect_lanes)
    committed: set[str] = set()
    for lane, stored_msgs in _lane_message_signatures(stored, resurrect_lanes).items():
        lane_projected = projected.get(lane, [])
        if not lane_projected:
            continue
        n, m = len(stored_msgs), len(lane_projected)
        # dp[i][j] = most pairs matchable from stored_msgs[i:] x lane_projected[j:]
        dp = [[0] * (m + 1) for _ in range(n + 1)]
        for i in range(n - 1, -1, -1):
            for j in range(m - 1, -1, -1):
                best = max(dp[i + 1][j], dp[i][j + 1])
                if stored_msgs[i][1] == lane_projected[j][1]:
                    best = max(best, 1 + dp[i + 1][j + 1])
                dp[i][j] = best
        i = j = 0
        while i < n and j < m:
            if (
                stored_msgs[i][1] == lane_projected[j][1]
                and 1 + dp[i + 1][j + 1] == dp[i][j]
            ):
                committed.add(stored_msgs[i][0])
                i += 1
                j += 1
            elif dp[i + 1][j] >= dp[i][j + 1]:
                i += 1
            else:
                j += 1
    return committed


def _lane_message_signatures(
    rows: list[dict[str, Any]],
    lanes: set[str] | frozenset[str],
) -> dict[str, list[tuple[str, str]]]:
    """Per lane, ordered (message_id, signature) pairs for content matching.

    The signature is the message's accumulated text with image targets
    normalized; text-less messages fall back to reasoning. Text alone
    decides when present — reasoning persistence is provider-dependent, so
    a reasoning mismatch (or coincidental match) must not override it.
    """
    order: dict[str, list[str]] = {}
    content: dict[tuple[str, str], str] = {}
    lane_of: dict[str, str] = {}
    for row in rows:
        if row["event"] != "message_chunk":
            continue
        data = row["data"]
        message_id, content_type = data.get("id"), data.get("content_type")
        if not message_id or content_type not in ("text", "reasoning"):
            continue
        chunk = data.get("content")
        if not isinstance(chunk, str):
            continue
        lane = _lane(data.get("agent"))
        if lane not in lanes:
            continue
        if message_id not in lane_of:
            lane_of[message_id] = lane
            order.setdefault(lane, []).append(message_id)
        group = (message_id, content_type)
        content[group] = content.get(group, "") + chunk
    return {
        lane: [
            (
                message_id,
                _IMAGE_MD_RE.sub(
                    r"![\1]",
                    text
                    if (text := content.get((message_id, "text"))) is not None
                    else content.get((message_id, "reasoning"), ""),
                ),
            )
            for message_id in ids
        ]
        for lane, ids in order.items()
    }


def _restore_evicted_results(
    turn_items: list[dict[str, Any]], stored: list[dict[str, Any]]
) -> None:
    """Restore full tool-result content the checkpoint holds only as a pointer.

    Large results are evicted to the sandbox filesystem before the ToolMessage is
    checkpointed, so a projected ``tool_call_result`` may carry only the "too
    large, saved to …" pointer. When the live stream captured the full content
    (older turns, where eviction ran after SSE emission), it survives in the
    stored event — restore it in place by tool_call_id. A no-op once the stored
    result is itself the pointer (eviction ran before SSE), so newer turns are
    untouched.
    """
    stored_results = {
        e["data"].get("tool_call_id"): e["data"]
        for e in stored
        if e["event"] == "tool_call_result" and e["data"].get("tool_call_id")
    }
    if not stored_results:
        return
    for item in turn_items:
        if item["event"] != "tool_call_result":
            continue
        content = item["data"].get("content")
        if not (isinstance(content, str) and content.startswith(_EVICTED_RESULT_PREFIX)):
            continue
        stored_data = stored_results.get(item["data"].get("tool_call_id"))
        if not stored_data:
            continue
        stored_content = stored_data.get("content")
        if isinstance(stored_content, str) and not stored_content.startswith(
            _EVICTED_RESULT_PREFIX
        ):
            item["data"]["content"] = stored_content
            item["data"]["content_type"] = stored_data.get(
                "content_type", item["data"].get("content_type")
            )


def _split_run_segments(messages: list[Any]) -> list[list[Any]]:
    """Split a task namespace's transcript at run boundaries (the plain
    HumanMessage each spawn/resume opens with). A defensive leading slice
    with no boundary attaches to the first run."""
    segments: list[list[Any]] = []
    current: list[Any] = []
    saw_boundary = False
    for message in messages:
        if is_run_boundary_message(message):
            if saw_boundary:
                segments.append(current)
                current = []
            saw_boundary = True
        current.append(message)
    if current:
        segments.append(current)
    return segments


@dataclass
class _TaskRuns:
    history: Any
    segments: list[list[Any]]
    cursor: int = 0
    attributed: bool = False
    last_ctx: tuple[Any, str | None] | None = None
    # A live writer's in-flight run is owned by its stream, not replay: the
    # final launch of a live task claims no segment (tail mode commits the
    # launch while the run still writes — projecting it would duplicate the
    # epoch the stream replays from seq 1).
    live: bool = False
    remaining_launches: int = 0
    # Ledger join (M4): stamps[i] is the task_run_id the i-th run segment's
    # input boundary carries (None = pre-ledger run); run_status maps
    # task_run_id -> ledger status. Empty when the walk/ledger read failed —
    # every claim then takes the legacy content-matching path.
    stamps: list[str | None] = field(default_factory=list)
    run_status: dict[str, str] = field(default_factory=dict)


def _segment_opener_text(segment: list[Any]) -> str | None:
    """Text of a segment's run-boundary HumanMessage (its launch input)."""
    for message in segment:
        if is_run_boundary_message(message):
            content = message.content
            if isinstance(content, str):
                return content.strip()
            text, _ = normalize_text_content(content)
            return text.strip() if text else None
    return None


class _TaskLaneProjector:
    """Per-run projection of background-task namespaces.

    A task namespace holds every run's transcript back-to-back; each run
    opens at a plain HumanMessage (its spawn/resume input). Launch artifacts
    (action ``init``/``resume``) in the main transcript attribute to run
    segments in order. Ledgered launches join exactly: the artifact's
    ``task_run_id`` matches the stamp the run's input boundary carries in
    checkpoint metadata, and the run ledger decides projection (``in_progress``
    runs belong to their live stream). Pre-ledger launches verify by content:
    a segment's boundary HumanMessage carries the launch prompt verbatim, so
    a launch only claims a segment whose opener matches its prompt. A launch
    with no matching segment projects nothing — either its boundary isn't
    checkpointed yet (the live stream owns that run until the next rebuild)
    or the run never wrote one (a failed/no-op launch); blind positional
    pairing would hand it the NEXT run's transcript. ``update`` (steering)
    artifacts never launch a run.

    Windowed builds may start after a task's init: the cursor then starts at
    ``offset`` (the leading segments belong to out-of-window turns). Any
    namespace read failure makes checkpoint replay unavailable so
    ``source=auto`` can use the complete stored-SSE fallback.
    """

    def __init__(self, thread_id: str, *, windowed: bool):
        self._thread_id = thread_id
        self._windowed = windowed
        self._tasks: dict[str, _TaskRuns] = {}
        self._run_started: dict[str, float] = {}
        # Turn indexes whose stamps carry trailing salvage (populated by
        # trailing_items) — those turns must not be cached, or the fast path
        # would replay them without the salvage.
        self.salvaged_turn_indexes: set[Any] = set()
        # task_id -> max started_at (epoch ms) over ledgered runs whose
        # segment THIS build claimed. Stamped onto the turn's task artifacts
        # as ``projected_run_started_ms``: the client's authority for which
        # runs its history payload already contains. Derived from the claim
        # act itself — never from a separate ledger read, which can name a
        # run the projection skipped (its skip decision and this watermark
        # must share one snapshot).
        self.claimed_watermarks: dict[str, float] = {}
        # Lanes claimed in the CURRENT turn whose run died mid-write
        # (_LOSSY_TERMINAL_STATUSES): the stored copy may hold output the
        # checkpoint never committed, so the merge may resurrect their
        # trailing rows. Reset by each project_for_turn call.
        self.turn_lossy_lanes: set[str] = set()

    @staticmethod
    def _launches_in(turn: Any) -> list[tuple[str, str, str | None, str | None]]:
        """Ordered ``(task_id, action, prompt, task_run_id)`` launch artifacts
        in a turn. ``task_run_id`` is None on pre-ledger data."""
        launches: list[tuple[str, str, str | None, str | None]] = []
        for message in turn.messages:
            if not isinstance(message, ToolMessage):
                continue
            artifact = (message.additional_kwargs or {}).get("task_artifact")
            if not isinstance(artifact, dict) or not artifact.get("task_id"):
                continue
            action = artifact.get("action", "init")
            if action in ("init", "resume"):
                prompt = artifact.get("prompt")
                run_id = artifact.get("task_run_id")
                launches.append(
                    (
                        str(artifact["task_id"]),
                        action,
                        prompt.strip() if isinstance(prompt, str) else None,
                        str(run_id) if run_id else None,
                    )
                )
        return launches

    async def prepare(
        self, reader: CheckpointHistoryReader, pairs: list[tuple[Any, Any]]
    ) -> None:
        launch_actions: dict[str, list[str]] = {}
        for _, turn in pairs:
            if turn is None:
                continue
            for task_id, action, _prompt, _run_id in self._launches_in(turn):
                launch_actions.setdefault(task_id, []).append(action)
        if not launch_actions:
            return

        task_ids = list(launch_actions)
        histories = await asyncio.gather(
            *(reader.aget_task_history(self._thread_id, tid) for tid in task_ids),
            return_exceptions=True,
        )
        stamps_by_task, run_status = await self._load_ledger(reader, task_ids)
        for task_id, history in zip(task_ids, histories):
            if isinstance(history, BaseException):
                logger.warning(
                    "[REPLAY] Failed to read subagent checkpoint state task:%s",
                    task_id,
                    exc_info=(type(history), history, history.__traceback__),
                )
                # Silent continuation would produce a plausible-looking but
                # incomplete transcript and bypass the endpoint's SSE fallback.
                raise CheckpointReplayUnavailable(
                    f"subagent checkpoint state unavailable for task:{task_id}"
                ) from history
            segments = _split_run_segments(history.messages)
            actions = launch_actions[task_id]
            # A window that opens on a resume is missing the older runs'
            # launches; their segments are skipped, not re-attributed. A full
            # build always sees the init, so its cursor starts at segment 0.
            cursor = (
                max(0, len(segments) - len(actions))
                if self._windowed and actions[0] != "init"
                else 0
            )
            self._tasks[task_id] = _TaskRuns(
                history=history,
                segments=segments,
                cursor=cursor,
                remaining_launches=len(actions),
                stamps=stamps_by_task.get(task_id, []),
                run_status=run_status,
            )

        # Same liveness truth that stamps card status (advisory-lock probe):
        # a task is live only while its writer provably runs, so an expired
        # stream never demotes a settled run's transcript. On probe failure
        # nothing is marked live — availability over precision (a transient
        # duplicate beats a missing transcript).
        try:
            statuses = await resolve_task_statuses(
                self._thread_id, list(self._tasks)
            )
        except Exception:
            logger.warning(
                "[REPLAY] task liveness probe failed for %s",
                self._thread_id,
                exc_info=True,
            )
            statuses = {}
        for task_id, runs in self._tasks.items():
            runs.live = statuses.get(task_id) == "running"

    async def _load_ledger(
        self, reader: CheckpointHistoryReader, task_ids: list[str]
    ) -> tuple[dict[str, list[str | None]], dict[str, str]]:
        """Boundary stamps per task + a thread-wide run_id -> status map.

        Both reads are best-effort: any failure (and readers without the
        stamp walk — test fakes) yields empty results, which routes every
        claim through the legacy content-matching path.
        """
        stamps_by_task: dict[str, list[str | None]] = {}
        run_status: dict[str, str] = {}
        # iscoroutinefunction, not truthiness: test fakes are spec'd mocks
        # whose auto-created attribute is sync — calling it would hand
        # asyncio.gather a non-awaitable. (AsyncMock passes the check.)
        stamp_walk = getattr(reader, "aget_task_run_stamps", None)
        if stamp_walk is not None and inspect.iscoroutinefunction(stamp_walk):
            results = await asyncio.gather(
                *(stamp_walk(self._thread_id, tid) for tid in task_ids),
                return_exceptions=True,
            )
            for task_id, stamps in zip(task_ids, results):
                if isinstance(stamps, BaseException):
                    logger.warning(
                        "[REPLAY] task run-stamp walk failed for task:%s",
                        task_id,
                        exc_info=(type(stamps), stamps, stamps.__traceback__),
                    )
                else:
                    stamps_by_task[task_id] = stamps
        try:
            runs = await sr_db.list_runs_for_thread(self._thread_id)
            run_status = {
                str(r["task_run_id"]): str(r["status"]) for r in runs
            }
            self._run_started = {
                str(r["task_run_id"]): r["started_at"].timestamp() * 1000.0
                for r in runs
                if r.get("started_at") is not None
            }
        except Exception:
            logger.warning(
                "[REPLAY] run-ledger read failed for %s",
                self._thread_id,
                exc_info=True,
            )
        return stamps_by_task, run_status

    def project_for_turn(
        self, turn: Any, turn_index: Any, response_id: str | None
    ) -> tuple[list[dict[str, Any]], set[str]]:
        """Items for every run launched in this turn, plus the launched task
        ids (the caller's cache guard: a turn that launched a still-writing
        run must not be cached)."""
        items: list[dict[str, Any]] = []
        launched: set[str] = set()
        self.turn_lossy_lanes = set()
        for task_id, _action, prompt, run_id in self._launches_in(turn):
            runs = self._tasks.get(task_id)
            if runs is None:
                continue
            launched.add(task_id)
            task_agent = f"task:{task_id}"
            runs.last_ctx = (turn_index, response_id)
            runs.remaining_launches -= 1
            status = runs.run_status.get(run_id) if run_id else None
            if status == "in_progress":
                # The ledger says this exact run is still executing: its
                # stream replays the epoch (opener included) from seq 1, so
                # claiming the segment here would render it twice. The turn
                # stays uncached (launched set + live stream), so the settled
                # rebuild projects it normally.
                continue
            if status is None and runs.live and runs.remaining_launches <= 0:
                # Legacy gate (no ledger row for this launch): without a
                # per-run status, only the final launch of a live task can be
                # the in-flight run.
                continue
            segment = (
                self._claim_segment_by_stamp(runs, run_id)
                if run_id and status is not None
                else None
            )
            if segment is None:
                segment = self._claim_segment(runs, prompt)
            if segment is None:
                continue
            started = self._run_started.get(run_id) if run_id else None
            if started is not None:
                prev = self.claimed_watermarks.get(task_id)
                self.claimed_watermarks[task_id] = (
                    started if prev is None else max(prev, started)
                )
            if status in _LOSSY_TERMINAL_STATUSES:
                self.turn_lossy_lanes.add(task_agent)
            if not runs.attributed:
                # Namespace-scoped signals (compaction, model fallback) are
                # not per-run; they ride with the first projected run.
                runs.attributed = True
                items.extend(
                    _context_signal_items(
                        self._thread_id, runs.history, agent=task_agent
                    )
                )
                items.extend(
                    _model_fallback_items(
                        self._thread_id, runs.history, agent=task_agent
                    )
                )
            items.extend(self._segment_items(task_agent, segment))
        return items, launched

    @staticmethod
    def _claim_segment_by_stamp(runs: _TaskRuns, run_id: str) -> list[Any] | None:
        """Exact ledger join: the segment whose input boundary carries this
        launch's ``task_run_id``.

        Usable only when stamps align 1:1 with segments (both derive from the
        same run boundaries; a mismatch means the walk saw a boundary shape
        the splitter didn't) — misalignment or a missing stamp returns None
        and the caller falls back to the legacy content-matching claim.
        """
        if len(runs.stamps) != len(runs.segments):
            return None
        for idx in range(runs.cursor, len(runs.segments)):
            if runs.stamps[idx] == run_id:
                runs.cursor = idx + 1
                return runs.segments[idx]
        return None

    @staticmethod
    def _claim_segment(runs: _TaskRuns, prompt: str | None) -> list[Any] | None:
        """The launch's run segment, or None if no segment belongs to it.

        Scans forward from the cursor for the segment whose boundary opener
        matches the launch prompt (skipped-over segments belong to earlier,
        already-projected turns and are never revisited). Verification needs
        both sides: a prompt-less artifact or a boundary-less segment (legacy
        data) can't be checked, so those claim positionally at the cursor —
        only a present-but-different opener refuses the pairing.
        """
        if runs.cursor >= len(runs.segments):
            return None
        candidate = runs.segments[runs.cursor]
        if prompt is None or _segment_opener_text(candidate) is None:
            runs.cursor += 1
            return candidate
        for idx in range(runs.cursor, len(runs.segments)):
            if _segment_opener_text(runs.segments[idx]) == prompt:
                segment = runs.segments[idx]
                runs.cursor = idx + 1
                return segment
        return None

    def _segment_items(
        self, task_agent: str, segment: list[Any]
    ) -> list[dict[str, Any]]:
        return [
            item
            for item in history_events_to_sse(
                messages_to_history_events(segment, agent=task_agent),
                thread_id=self._thread_id,
            )
            # Live streams never emit artifact events in the task lane
            # (subagent writer events carry node labels, not task:{id});
            # the frontend subagent handler has no artifact case.
            if item.get("event") != "artifact"
        ]

    async def trailing_items(self) -> list[dict[str, Any]]:
        """Segments beyond the last in-window launch, for settled runs only.

        Covers a launch whose turn never committed (e.g. the launching turn
        errored before persist) — salvaged under the last known launch's
        stamps. Per-segment ledger gate: a stamped segment projects only when
        its run row exists and is terminal — a missing row is a
        cascade-truncated run (its launching turn was deleted; resurrecting
        it would re-attach deleted work), an ``in_progress`` row belongs to
        the live stream. Unstamped (pre-ledger) segments keep the legacy
        whole-task liveness gate."""
        items: list[dict[str, Any]] = []
        for task_id, runs in self._tasks.items():
            if runs.cursor >= len(runs.segments) or runs.last_ctx is None:
                continue
            aligned = len(runs.stamps) == len(runs.segments)
            task_agent = f"task:{task_id}"
            turn_index, response_id = runs.last_ctx
            salvaged_any = False
            for idx in range(runs.cursor, len(runs.segments)):
                stamp = runs.stamps[idx] if aligned else None
                if stamp is not None:
                    if runs.run_status.get(stamp) not in sr_db.TERMINAL_STATUSES:
                        continue
                elif runs.live:
                    continue
                salvaged_any = True
                for item in self._segment_items(task_agent, runs.segments[idx]):
                    _enrich(item, self._thread_id, turn_index, response_id)
                    items.append(item)
            if salvaged_any:
                self.salvaged_turn_indexes.add(turn_index)
        return items


def _collect_image_url_map(records: list[dict[str, Any]]) -> dict[str, str]:
    url_map: dict[str, str] = {}
    for record in records:
        if not isinstance(record, dict) or record.get("name") != IMAGE_CAPTURE_UI_NAME:
            continue
        path_to_url = (record.get("props") or {}).get("path_to_url")
        if isinstance(path_to_url, dict):
            url_map.update(
                {str(k): str(v) for k, v in path_to_url.items() if k and v}
            )
    return url_map


def _apply_image_url_map(
    turn_items: list[dict[str, Any]], url_map: dict[str, str]
) -> list[dict[str, Any]]:
    if not url_map:
        return turn_items

    def replacer(match):
        alt, path = match.group(1), match.group(2)
        if path in url_map:
            return f"![{alt}]({url_map[path]})"
        return match.group(0)

    for item in turn_items:
        if item.get("event") != "message_chunk":
            continue
        data = item.get("data", {})
        if data.get("content_type") != "text":
            continue
        content = data.get("content")
        if content:
            data["content"] = IMAGE_MD_RE.sub(replacer, content)
    return turn_items


def _has_unresolved_sandbox_images(turn_items: list[dict[str, Any]]) -> bool:
    for item in turn_items:
        if item.get("event") != "message_chunk":
            continue
        data = item.get("data", {})
        if data.get("content_type") != "text":
            continue
        content = data.get("content") or ""
        for match in IMAGE_MD_RE.finditer(content):
            if is_sandbox_image_path(match.group(2)):
                return True
    return False


def _context_signal_items(
    thread_id: str, turn: Any, *, agent: str = MAIN_AGENT
) -> list[dict[str, Any]]:
    """Project a turn's compaction signals from its private-state deltas.

    Offload counts become one aggregated event per kind (live may batch them
    across several firings); the summarize event projects through its summary
    message, which carries ``lc_source=summarization`` (+ stamped fields on
    new threads).
    """
    events: list[HistoryEvent] = []
    for count, kind, field_name in (
        (turn.newly_offloaded_args, "args", "offloaded_args"),
        (turn.newly_offloaded_reads, "reads", "offloaded_reads"),
    ):
        if count:
            events.append(
                HistoryEvent(
                    "context-window",
                    agent,
                    None,
                    {
                        "action": "offload",
                        "signal": "complete",
                        "kind": kind,
                        field_name: count,
                    },
                )
            )
    summarization_event = turn.new_summarization_event
    if summarization_event is not None:
        message = summarization_event.get("summary_message")
        if isinstance(message, HumanMessage):
            events.extend(messages_to_history_events([message], agent=agent))
    return history_events_to_sse(events, thread_id=thread_id)


def _model_fallback_items(
    thread_id: str, turn: Any, *, agent: str = MAIN_AGENT
) -> list[dict[str, Any]]:
    """Project a turn's model_fallback notices from its new ``ui`` records.

    Field whitelist and error sanitization mirror the live handler. ``agent``
    identifies the namespace being projected (main or ``task:{id}``).
    """
    items: list[dict[str, Any]] = []
    for record in turn.new_ui_records:
        if record.get("name") != MODEL_FALLBACK_UI_NAME:
            continue
        props = record.get("props") or {}
        data: dict[str, Any] = {"thread_id": thread_id, "agent": agent}
        for key in _MODEL_FALLBACK_FIELDS:
            if key in props:
                data[key] = props[key]
        error_text = props.get("error")
        if isinstance(error_text, str):
            data["error"] = _sanitize_error_text(error_text)
        items.append({"event": "model_fallback", "data": data})
    return items


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


def _enrich(
    item: dict[str, Any],
    thread_id: str,
    turn_index: Any,
    response_id: str | None,
) -> None:
    data = item.setdefault("data", {})
    data.setdefault("thread_id", thread_id)
    data["turn_index"] = turn_index
    if response_id is not None:
        data["response_id"] = response_id
