"""Stored-event merge: resolve what a turn's stored events add to its projection.

``derive_turn`` turns the stored events into the turn's legacy facts, which
``legacy.apply_turn`` places. A row the backfill has not reached derives them
here on every projection; a backfilled row carries them and never reads its
stored events (see ``legacy``).
"""

from __future__ import annotations

from collections.abc import Callable, Iterator
from typing import Any

from ptc_agent.agent.middleware.image_capture import (
    IMAGE_MD_RE,
    is_sandbox_image_path,
)
from src.server.services.history.replay import legacy
from src.server.services.history.replay import widgets
from src.server.services.history.replay.lanes import MAIN_LANE, agent_lane


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


# The pointer LargeResultEvictionMiddleware checkpoints in place of an
# over-threshold tool result. Only a result evicted after it streamed is
# restored, which only older turns are (eviction now runs first), so the
# wording to recognize is the one those turns were recorded under: fixed
# here, not read from the agent-facing template, which may be reworded.
EVICTED_RESULT_PREFIX = "Tool result too large, the result of this tool call "


def is_evicted_pointer(content: Any) -> bool:
    return isinstance(content, str) and content.startswith(EVICTED_RESULT_PREFIX)


def message_lane_ordinals(
    out: list[dict[str, Any]],
) -> tuple[dict[str, int], dict[str, int]]:
    """Ordinal of each message within its lane, over anchorable-content
    messages, plus the per-lane message count.

    Message ids differ between the streams (live chunks carry ``lc_run--…``
    run ids, checkpointed messages the provider id), so a stored chunk can't
    find its projected message by id. But both streams enumerate a lane's
    messages in the same order, so the lane-relative ordinal pairs them,
    computed the same way on each stream. The pairing holds only against the
    projection it was made with, which is why facts keep the projected row's
    own key instead (``legacy.row_keys``).
    """
    counters: dict[str, int] = {}
    ordinal_by_id: dict[str, int] = {}
    for item in out:
        data = item["data"]
        if item["event"] != "message_chunk":
            continue
        if data.get("content_type") not in legacy.ANCHORABLE_CONTENT_TYPES:
            continue
        message_id = data.get("id")
        if not message_id or message_id in ordinal_by_id:
            continue
        lane = agent_lane(data.get("agent"))
        ordinal_by_id[message_id] = counters.get(lane, 0)
        counters[lane] = ordinal_by_id[message_id] + 1
    return ordinal_by_id, counters


def by_ordinal(ordinals: dict[str, int]) -> legacy.MessageKey:
    """Key message chunks by lane ordinal, so a stored row and its projected
    counterpart share a key. ``ordinals`` is ``message_lane_ordinals`` of
    the stream the rows are from."""

    def key(lane: str, data: dict[str, Any], _occurrence: int) -> tuple | None:
        message_id = data.get("id")
        if message_id not in ordinals:
            return None
        return ("message_chunk", lane, ordinals[message_id], data["content_type"])

    return key


# Image targets are rewritten between capture and checkpoint (sandbox path
# -> durable URL; the checkpoint rewrite sees whole messages while the
# archive rewrite scans row fragments), so copy-matching must not compare
# them verbatim. The rewrite preserves the file's basename (the storage key
# ends /{basename}), so normalizing targets to their basename keeps the two
# copies of one image equal while distinct images stay distinct — stripping
# the target entirely would collapse every same-alt image into one signature.
def _image_basename(target: str) -> str:
    return target.split("?", 1)[0].split("#", 1)[0].rstrip("/").rsplit("/", 1)[-1]


def _normalize_image_targets(text: str) -> str:
    return IMAGE_MD_RE.sub(
        lambda match: f"![{match.group(1)}]({_image_basename(match.group(2))})", text
    )


def _image_targets(
    rows: list[dict[str, Any]],
) -> Iterator[tuple[dict[str, Any], str]]:
    """(row, target) of each markdown image in the rows' text chunks."""
    for row in rows:
        data = row["data"]
        content = data.get("content")
        if (
            row["event"] == "message_chunk"
            and data.get("content_type") == "text"
            and isinstance(content, str)
        ):
            for match in IMAGE_MD_RE.finditer(content):
                yield row, match.group(2)


def _stored_image_urls(
    turn_items: list[dict[str, Any]],
    task_items: list[dict[str, Any]],
    stored_events: list[dict[str, Any]],
) -> tuple[dict[str, str], bool]:
    """The stored image URL of each sandbox path the projection still shows,
    and whether a path outside the run lanes has several.

    A legacy turn whose capture map never reached its checkpoint holds the
    URLs only in its stored copy, where capture rewrote each path, one row
    at a time, to a key ending in the file's basename. A path no stored URL
    names was never captured (its stored copy shows the path too) and stays
    as it is.
    """
    urls: dict[str, set[str]] = {}
    for _row, target in _image_targets(stored_events):
        if not is_sandbox_image_path(target):
            urls.setdefault(_image_basename(target), set()).add(target)
    images: dict[str, str] = {}
    ambiguous = False
    run_rows = {id(item) for item in task_items}
    for item, path in _image_targets(turn_items):
        if not is_sandbox_image_path(path):
            continue
        found = urls.get(_image_basename(path), set())
        if len(found) == 1:
            images[path] = next(iter(found))
        elif found and id(item) not in run_rows:
            ambiguous = True
    return images, ambiguous


def derive_turn(
    turn_items: list[dict[str, Any]],
    task_items: list[dict[str, Any]],
    stored_events: list[dict[str, Any]],
    *,
    main_durations_known: bool = False,
) -> legacy.LegacyFacts:
    """The turn's legacy facts, resolved against its projection.

    Sandbox image paths the projection still shows resolve to their stored
    URLs (``images``). Only when several stored URLs share the basename of
    a path outside the run lanes does the main lane replay from its stored
    events ``verbatim``, where each image keeps the URL capture wrote there.
    Rows outside the lanes the turn's runs projected go with it: the main
    lane, task-scoped custom artifacts (the projector never emits task
    artifacts), and unclaimed agents' rows (in_progress, cascade-truncated,
    unclaimed legacy — nothing projected, nothing to merge). The run lanes
    keep the projection, which is the transcript authority for the agents
    it claimed, merged like any other turn.
    """
    facts: legacy.LegacyFacts = {"v": legacy.FACTS_VERSION}
    stored_events = [e for e in stored_events if _valid_stored(e)]
    images, ambiguous = _stored_image_urls(turn_items, task_items, stored_events)
    if images:
        facts["images"] = images
    if ambiguous:
        projected_task_agents = {
            agent
            for i in task_items
            if (agent := str((i.get("data") or {}).get("agent", "")))
        }
        claimed: list[dict[str, Any]] = []
        verbatim: list[legacy.StoredRow] = []
        for e in stored_events:
            agent = str(e["data"].get("agent", ""))
            if agent in projected_task_agents and e["event"] != "artifact":
                claimed.append(e)
            else:
                verbatim.append({"event": e["event"], "data": e["data"]})
        facts["verbatim"] = verbatim
        merge = derive_merge(
            task_items, claimed, main_durations_known=main_durations_known
        )
    else:
        merge = derive_merge(
            turn_items, stored_events, main_durations_known=main_durations_known
        )
    if merge is not None:
        facts["merge"] = merge
    return facts


def derive_merge(
    turn_items: list[dict[str, Any]],
    stored_events: list[dict[str, Any]],
    *,
    main_durations_known: bool = False,
) -> legacy.Merge | None:
    """What the stored events contribute to a projected turn, or None when it
    has none (the projected signal rows then stand).

    Read-only: resolved against the projection as ``legacy.apply`` will see
    it, its stored-preferred rows dropped and its widgets upgraded.

    Stored rows find their projected rows by lane ordinal (``by_ordinal``);
    the facts then name those rows by their own keys (``legacy.row_keys``),
    which a later projection keeps.

    - ``widgets``: ``[projected artifact id, payload]`` per stored widget, in
      order. The stored event carries the resolved data files the
      checkpoint deliberately omits. Pairing is ordinal here (the live
      widget artifact id is random and the stored event has no tool call
      id, but both streams order widgets by tool execution); the id is null
      for a payload beyond the projected count.
    - ``results``: the stored content of each result the checkpoint holds
      only as an eviction pointer, when the live stream captured it whole
      (older turns, where eviction ran after SSE emission).
    - ``durations``: ``[row key, ms]`` per projected reasoning close.
    - ``anchors``: the keys of the stored rows that found a projected row,
      in stored order, and of each payload none took (``["widget", k]``).
    - ``signals``: passthrough rows and stored widgets, in stored order.
      ``at`` indexes the newest anchor streamed since the previous signal,
      and the signal follows it, or the newest earlier one since then that
      the projection still holds. Without ``at`` a signal follows the
      previous one, so its original mid-turn placement survives instead of
      piling up at the end.

    Stored transcript rows the checkpoint never committed (a step stopped or
    failed mid-stream) anchor nothing and are dropped: replay shows what the
    turn committed, which is what the model will see next. The main lane's
    reasoning durations are not taken when ``main_durations_known``: the
    turn's replay facts supplied them.
    """
    stored = [e for e in stored_events if _valid_stored(e)]
    if not stored:
        return None

    # The projection as apply sees it once paired widgets are upgraded, and
    # the key each of its rows has before that (what the facts name it by).
    view = [
        {"event": i["event"], "data": i["data"]}
        for i in turn_items
        if i["event"] not in legacy.STORED_PREFERRED_EVENTS
    ]
    row_keys = legacy.row_keys(view)
    stored_widgets = [e for e in stored if widgets._is_widget(e["event"], e["data"])]
    projected_widgets = [i for i in view if widgets._is_widget(i["event"], i["data"])]
    paired_ids: list[Any] = []
    for item, stored_event in zip(projected_widgets, stored_widgets):
        paired_ids.append(item["data"].get("artifact_id") or None)
        item["data"] = stored_event["data"]
    widget_ordinal = {id(e): k for k, e in enumerate(stored_widgets)}

    merge: legacy.Merge = {}
    if stored_widgets:
        merge["widgets"] = [
            [paired_ids[k] if k < len(paired_ids) else None, e["data"]]
            for k, e in enumerate(stored_widgets)
        ]
    results = _evicted_results(view, stored)
    if results:
        merge["results"] = results

    projected_ordinals, projected_lane_counts = message_lane_ordinals(view)
    stored_ordinals, stored_lane_counts = message_lane_ordinals(stored)
    # A lane whose stored count disagrees with the checkpoint holds a phantom
    # partial or uncommitted output, so its ordinals do not line up across
    # the streams and its messages are paired by content instead.
    shifted_lanes = {
        lane for lane, count in stored_lane_counts.items()
        if count != projected_lane_counts.get(lane, 0)
    }
    committed_pairs = (
        _pair_committed_messages(view, stored, shifted_lanes)
        if shifted_lanes
        else {}
    )
    index = legacy.key_index(view, by_ordinal(projected_ordinals))
    stored_keys = dict(
        zip(map(id, stored), legacy.row_keys(stored, by_ordinal(stored_ordinals)))
    )

    def duration_key(event: dict[str, Any]) -> tuple | None:
        data = event["data"]
        lane = agent_lane(data.get("agent"))
        if lane not in shifted_lanes:
            return stored_keys[id(event)]
        twin = committed_pairs.get(data.get("id"))
        if twin is None:
            return None  # a phantom attempt: its thinking is not the row's
        return ("message_chunk", lane, projected_ordinals[twin], "reasoning_signal")

    durations = _reasoning_durations(
        view,
        [
            e
            for e in stored
            if not (
                main_durations_known
                and agent_lane(e["data"].get("agent")) == MAIN_LANE
            )
        ],
        index,
        duration_key,
    )
    if durations:
        merge["durations"] = [
            [list(row_keys[idx]), elapsed_ms] for idx, elapsed_ms in durations
        ]

    anchors: list[list[Any]] = []
    signals: list[legacy.Signal] = []
    newest: int | None = None  # the anchor streamed since the previous signal
    for event in stored:
        signal: legacy.Signal | None = None
        if event["event"] in legacy.PASSTHROUGH_EVENTS:
            signal = {"event": event["event"], "data": event["data"]}
        elif id(event) in widget_ordinal:
            # Placed only if no projected widget takes the payload.
            signal = {"widget": widget_ordinal[id(event)]}
        if signal is not None:
            if newest is not None:
                signal["at"] = newest
                newest = None
            signals.append(signal)
        key = stored_keys[id(event)]
        if key is not None and (idx := index.get(key)) is not None:
            row_key = row_keys[idx]
        elif id(event) in widget_ordinal:
            # A payload no projected widget took: a widget that takes it
            # later stands where it streamed.
            row_key = ("widget", widget_ordinal[id(event)])
        else:
            continue
        if row_key is None:
            continue
        if not anchors or anchors[-1] != list(row_key):
            anchors.append(list(row_key))
        newest = len(anchors) - 1
    if signals:
        merge["signals"] = signals
        referenced = [signal["at"] for signal in signals if "at" in signal]
        if referenced:
            merge["anchors"] = anchors[: max(referenced) + 1]
    return merge


def _evicted_results(
    view: list[dict[str, Any]], stored: list[dict[str, Any]]
) -> dict[str, legacy.StoredResult]:
    """Full tool-result content the checkpoint holds only as a pointer.

    Large results are evicted to the sandbox filesystem before the
    ToolMessage is checkpointed, so a projected ``tool_call_result`` may
    carry only the "too large, saved to …" pointer. Only results replay
    restores are kept: none once the stored result is itself the pointer
    (eviction ran before SSE), so newer turns keep nothing here.
    """
    stored_results = {
        e["data"].get("tool_call_id"): e["data"]
        for e in stored
        if e["event"] == "tool_call_result" and e["data"].get("tool_call_id")
    }
    out: dict[str, legacy.StoredResult] = {}
    if not stored_results:
        return out
    for item in view:
        data = item["data"]
        if item["event"] != "tool_call_result" or not is_evicted_pointer(
            data.get("content")
        ):
            continue
        tool_call_id = data.get("tool_call_id")
        stored_data = stored_results.get(tool_call_id)
        if not stored_data:
            continue
        content = stored_data.get("content")
        if isinstance(content, str) and not is_evicted_pointer(content):
            result: legacy.StoredResult = {"content": content}
            if "content_type" in stored_data:
                result["content_type"] = stored_data["content_type"]
            out[tool_call_id] = result
    return out


def _reasoning_durations(
    view: list[dict[str, Any]],
    stored: list[dict[str, Any]],
    index: dict[tuple, int],
    key_of: Callable[[dict[str, Any]], tuple | None],
) -> list[tuple[int, int]]:
    """``(projected position, ms)`` for each projected reasoning close the
    stored stream timed.

    How long the model thought is measured on the live stream and exists
    nowhere in the checkpoint, so the projected close would replay without
    it. The anchor key names the message group; its last occurrence is the
    close, because the projector emits start before complete.

    The total is a sum because the two sides count differently. A message
    with interleaved thinking streams one close per block, while
    ``split_content_blocks`` joins those blocks into a single reasoning
    row, so one projected close stands for all of them. Taking any one
    block's time would report a fraction of the thinking the row shows.
    """
    totals: dict[tuple, int] = {}
    for event in stored:
        data = event["data"]
        if (
            event["event"] != "message_chunk"
            or data.get("content_type") != "reasoning_signal"
            or data.get("content") != "complete"
            or not isinstance(data.get("elapsed_ms"), int)
        ):
            continue
        key = key_of(event)
        if key is None or key not in index:
            continue
        totals[key] = totals.get(key, 0) + data["elapsed_ms"]
    out: list[tuple[int, int]] = []
    for key, elapsed_ms in totals.items():
        target = view[index[key]]["data"]
        if (
            target.get("content_type") == "reasoning_signal"
            and target.get("content") == "complete"
        ):
            out.append((index[key], elapsed_ms))
    return out


def _pair_committed_messages(
    turn_items: list[dict[str, Any]],
    stored: list[dict[str, Any]],
    lanes: set[str] | frozenset[str],
) -> dict[str, str]:
    """Stored message id -> the checkpointed message it is a copy of.

    A phantom partial (a model attempt that failed before an in-run retry)
    shifts a lane's stored ordinals, so a committed message's stored copy
    can land beyond the projected count and look trailing. Alignment is the
    match-maximizing in-order pairing (LCS) of stored messages against
    projected messages: a phantom can never consume a projected slot a real
    copy needs, and occurrences stay distinct — a stored message repeating
    a projected text beyond the pairing is uncommitted output, not a copy.
    Ties break toward the earliest stored message, so the copy is the first
    occurrence.
    """
    projected = _lane_message_signatures(turn_items, lanes)
    committed: dict[str, str] = {}
    for lane, stored_msgs in _lane_message_signatures(stored, lanes).items():
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
                committed[stored_msgs[i][0]] = lane_projected[j][0]
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
        lane = agent_lane(data.get("agent"))
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
