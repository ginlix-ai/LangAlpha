"""Stored-event merge: anchor stored payloads into a projected turn, restore losses."""

from __future__ import annotations

import re
from typing import Any

from ptc_agent.agent.middleware.large_result_eviction import TOO_LARGE_TOOL_MSG
from src.server.services.history.replay import widgets


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


def _replay_main_lane_from_storage(
    task_items: list[dict[str, Any]],
    stored_events: list[dict[str, Any]],
    turn_lossy_lanes: set[str],
) -> list[dict[str, Any]]:
    """Rebuild a turn's items with the main lane replayed verbatim from
    storage while the task lane keeps the normal projection+merge contract."""
    # The task lane keeps the normal path's contract exactly, via the same
    # merge: projection is the transcript authority for the agents it claimed
    # (the checkpoint outranks every stored shape — collector full copy,
    # user-stop partial snapshot, legacy interleave), while
    # _merge_stored_payloads supplies what the checkpoint can't —
    # stored-preferred signal rows anchored in position, evicted tool results
    # restored from the fuller stored copy — and stored transcript rows serve
    # only as anchors, never duplicates. Stored rows outside the claimed lanes
    # replay verbatim: main rows, task-scoped custom artifacts (the projector
    # never emits task artifacts), and unclaimed agents' rows (in_progress,
    # cascade-truncated, unclaimed legacy — nothing projected, nothing to
    # merge). Copy the nested ``data`` (like build_sse_replay_items): _enrich
    # stamps into it, and the source dicts are the request's pristine
    # ``sse_events`` rows.
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
            verbatim_rows.append({"event": e["event"], "data": dict(e["data"])})
    return verbatim_rows + _merge_stored_payloads(
        task_items, claimed_rows, turn_lossy_lanes
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
# archive rewrite scans row fragments), so copy-matching must not compare
# them verbatim. The rewrite preserves the file's basename (the storage key
# ends /{basename}), so normalizing targets to their basename keeps the two
# copies of one image equal while distinct images stay distinct — stripping
# the target entirely would collapse every same-alt image into one signature.
_IMAGE_MD_RE = re.compile(r"!\[([^\]]*)\]\(([^)]*)\)")


def _normalize_image_targets(text: str) -> str:
    def _basename(match: re.Match) -> str:
        target = match.group(2).split("?", 1)[0].split("#", 1)[0].rstrip("/")
        return f"![{match.group(1)}]({target.rsplit('/', 1)[-1]})"

    return _IMAGE_MD_RE.sub(_basename, text)


def _lane(agent: Any) -> str:
    return agent if isinstance(agent, str) and agent.startswith("task:") else "main"


def _message_lane_ordinals(
    out: list[dict[str, Any]],
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
    for item in out:
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

    projected_widgets = [i for i in turn_items if widgets._is_widget(i["event"], i["data"])]
    stored_widgets = [e for e in stored if widgets._is_widget(e["event"], e["data"])]
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
                _normalize_image_targets(
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
