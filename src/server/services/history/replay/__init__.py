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

Package layout: this root orchestrates; items builds table-sourced events,
stored_merge anchors persisted payloads, task_lane + segment_claim project
background-task namespaces, widgets inlines offloaded payloads on the way out.
"""

from __future__ import annotations

import logging
from typing import Any


from ptc_agent.agent.middleware.image_capture import (
    IMAGE_MD_RE,
    is_sandbox_image_path,
)
from src.server.services.runs.sse_producer import resolve_token_threshold
from src.server.services.history import projection_cache
from src.server.services.history import projector
from src.server.services.history import task_status
from src.server.services.history.projector import (
    history_events_to_sse,
    messages_to_history_events,
)
from src.server.services.history.reader import CheckpointHistoryReader
from src.server.utils.checkpoint_helpers import CheckpointBranchTipNotFound
from src.server.services.history.replay import items
from src.server.services.history.replay import stored_merge
from src.server.services.history.replay import task_lane
from src.server.services.history.replay import widgets

logger = logging.getLogger(__name__)


class CheckpointReplayUnavailable(Exception):
    """Checkpoint history cannot faithfully cover this thread's replay."""


IMAGE_CAPTURE_UI_NAME = "image_capture"

# Mirrors the streaming handler's model_fallback field whitelist.


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

    out: list[dict[str, Any]] | None = None
    if projection_cache.cache_active():
        out = await _assemble_from_cache(
            reader,
            thread_id,
            queries_by_turn,
            responses_by_turn,
            turn_indexes,
            branch_tip_checkpoint_id,
            last_n_turns,
        )
    if out is None:
        out = await _build_and_backfill(
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
    await widgets._resolve_widget_data_refs(out)
    return out


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

    out: list[dict[str, Any]] = []
    for turn_index, anchor in pairs:
        if anchor is None:
            out.extend(
                items._stub_turn_items(
                    thread_id, turn_index, queries_by_turn, responses_by_turn
                )
            )
        else:
            out.extend(cached[anchor.tail_checkpoint_id])
    for interrupt in await reader.aget_tip_interrupts(thread_id, tip_id):
        out.append(items._interrupt_item(thread_id, interrupt))
    return out


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

    usage_by_response = items._usage_rows_by_response(usages)
    provenance_by_response = items._rows_by_response(provenance, many=True)

    out: list[dict[str, Any]] = []
    lane = task_lane.TaskLaneProjector(thread_id, windowed=last_n_turns is not None)
    await lane.prepare(reader, pairs)

    # Stores are deferred past trailing_items(): only then is it known which
    # turns carry trailing salvage and must stay out of the cache.
    cacheable: list[tuple[Any, str | None, list[dict[str, Any]]]] = []
    for turn_index, turn in pairs:
        if turn is None:
            out.extend(
                items._stub_turn_items(
                    thread_id, turn_index, queries_by_turn, responses_by_turn
                )
            )
            continue

        response = responses_by_turn.get(turn_index)
        response_id = (
            str(response.get("conversation_response_id")) if response else None
        )
        stored_events = stored_merge._stored_events(response)

        segment = [
            items._user_message_item(thread_id, q, response)
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
        turn_items[:0] = projector.context_signal_items(thread_id, turn) + projector.model_fallback_items(
            thread_id, turn
        )
        task_items, turn_task_ids = lane.project_for_turn(
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
        turn_items = items._insert_provenance_items(
            turn_items, provenance_by_response.get(response_id) or []
        )
        turn_items.extend(
            items._interrupt_item(thread_id, intr) for intr in turn.ending_interrupts
        )
        credit_item = items._credit_usage_item(
            thread_id, response, usage_by_response.get(response_id)
        )
        if credit_item:
            turn_items.append(credit_item)

        if _has_unresolved_sandbox_images(turn_items) and stored_events:
            # Non-derivable image URLs live only in the stored events for
            # this turn — replay the MAIN lane from storage wholesale.
            turn_items = stored_merge._replay_main_lane_from_storage(
                task_items, stored_events, lane.turn_lossy_lanes
            )
        else:
            turn_items = stored_merge._merge_stored_payloads(
                turn_items, stored_events, lane.turn_lossy_lanes
            )

        _fill_token_thresholds(turn_items)
        # Terminal error: never in stored events (persisted before it is
        # yielded live), so it appends after the merge on every turn.
        error_item = items._error_item(thread_id, response)
        if error_item:
            turn_items.append(error_item)

        # After the stored-events merge so both projected and stored-copy
        # artifacts are covered, and before caching: the watermark is a fact
        # about what this build claimed, and a cached turn's claims are final
        # (a turn with a still-writing run is never cached).
        task_status.stamp_projected_watermarks(turn_items, lane.claimed_watermarks)
        for item in turn_items:
            items._enrich(item, thread_id, turn_index, response_id)
        segment.extend(turn_items)
        out.extend(segment)
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
        awaiting_archive = set(lane.turn_lossy_lanes)
        for i in turn_items:
            d = i.get("data") or {}
            agent = str(d.get("agent", ""))
            if (
                i.get("event") == "tool_call_result"
                and agent.startswith("task:")
                and isinstance(d.get("content"), str)
                and d["content"].startswith(stored_merge._EVICTED_RESULT_PREFIX)
            ):
                awaiting_archive.add(agent)
        if awaiting_archive:
            awaiting_archive -= {
                str((e.get("data") or {}).get("agent", ""))
                for e in stored_events or []
                if stored_merge._valid_stored(e) and e["event"] in stored_merge._ARCHIVE_EVIDENCE_EVENTS
            }
        if not awaiting_archive and not await projection_cache.task_streams_live(
            thread_id, turn_task_ids
        ):
            cacheable.append((turn_index, turn.tail_checkpoint_id, segment))

    out.extend(await lane.trailing_items())
    # Trailing salvage rides items but belongs to no turn's checkpoint range,
    # so the all-cache-hit fast path (which never runs the task lane) would
    # silently drop it. Keep the salvage-stamped turn uncacheable — skip its
    # store and evict any entry from before the orphan appeared — so every
    # read misses there and rebuilds until the salvage resolves.
    salvaged = lane.salvaged_turn_indexes
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
        out.append(items._interrupt_item(thread_id, interrupt))

    return out


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
    out: list[dict[str, Any]] = []
    errors_emitted: set[str] = set()
    for query in queries:
        if not isinstance(query, dict):
            continue
        turn_index = query.get("turn_index")
        response = responses_by_turn.get(turn_index)
        response_id = (
            str(response.get("conversation_response_id")) if response else None
        )
        out.append(items._user_message_item(thread_id, query, response))
        for event in stored_merge._stored_events(response):
            if not stored_merge._valid_stored(event):
                continue
            item = {"event": event["event"], "data": dict(event["data"])}
            items._enrich(item, thread_id, turn_index, response_id)
            out.append(item)
        if response_id and response_id not in errors_emitted:
            error_item = items._error_item(thread_id, response)
            if error_item:
                errors_emitted.add(response_id)
                items._enrich(error_item, thread_id, turn_index, response_id)
                out.append(error_item)
    return out


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


