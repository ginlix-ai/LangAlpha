"""Replay item builders: table-sourced synthesis and the shared enrich stamp."""

from __future__ import annotations

from datetime import datetime
from typing import Any

from src.server.contracts.run_times import run_completed_at
from src.server.contracts.status import TERMINAL_STATUSES
from src.server.contracts.turn_kinds import NOTIFICATION_QUERY_TYPES
from src.server.database.provenance import provenance_row_to_event
from src.server.services.history.replay import facts as replay_facts
from src.server.services.history.replay import legacy
from src.server.services.history.replay import stopped
from src.server.services.history.replay.lanes import MAIN_LANE, agent_lane
from src.server.services.runs.credit_usage import build_credit_usage_data
from src.server.utils.error_sanitization import (
    sanitize_error_text as _sanitize_error_text,
)


def _user_message_item(
    thread_id: str,
    query: dict[str, Any],
    response: dict[str, Any] | None = None,
    open_run: str | None = None,
) -> dict[str, Any]:
    payload = {
        "thread_id": thread_id,
        "turn_index": query.get("turn_index"),
        "content": query.get("content"),
        "timestamp": query.get("created_at"),
        "metadata": query.get("metadata"),
    }
    # Turns nobody typed: a system notification, or the wake a market
    # target's notice or a wakeup started. The client draws them apart from
    # user bubbles.
    if query.get("type") in NOTIFICATION_QUERY_TYPES:
        payload["query_type"] = query["type"]
    if query.get("type") == "system":
        # A report-back's row is drawn from its query, and a live attach draws
        # it again from the stream's first frame. Naming the run here, ended or
        # not, lets the client keep one. Not ``run_id``, which means rendered.
        # A running turn's run is ``open_run``, the in_progress slot's id: it
        # has no settled response, or only an earlier attempt's.
        run = open_run or (str(response.get("conversation_response_id")) if response else None)
        if run:
            payload["report_run_id"] = run
    # The turn's run id, stamped only once the run is terminal — i.e. when this
    # replay carries the turn's actual content. The client records it as
    # "already rendered" so the report-back catch-up never re-attaches a run
    # whose turn is on screen (a duplicate bubble). A live run's stub must NOT
    # carry it: its content hasn't rendered, and marking it would suppress the
    # attach that streams it — hence the positive terminal check (an unknown
    # or legacy status must not stamp).
    if response is not None and response.get("status") in TERMINAL_STATUSES:
        payload["run_id"] = str(response.get("conversation_response_id"))
    completed_at = run_completed_at(response)
    if completed_at is not None:
        payload["run_completed_at"] = completed_at
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
    kind = value.get("kind") if isinstance(value, dict) else None
    return {
        "event": "interrupt",
        "data": {
            "thread_id": thread_id,
            "interrupt_id": interrupt.get("id"),
            **({"kind": kind} if isinstance(kind, str) else {}),
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
    ``tool_call_id`` (where the live event fired); rows whose call is not in
    this projection append at the turn tail in row order. A row with no tool
    call (a market-watch stamp) has only its time to go by, and so does a
    subagent's: its lane replays after the whole main lane, while the client
    hangs every record on the main bubble current when it arrives. See
    ``_place_by_steer``.
    """
    if not rows:
        return turn_items
    by_anchor: dict[str, list[dict[str, Any]]] = {}
    unanchored: list[tuple[dict[str, Any], float | None]] = []
    for row in rows:
        item = {"event": "provenance", "data": provenance_row_to_event(row)}
        tool_call_id = item["data"].get("tool_call_id")
        if tool_call_id and agent_lane(item["data"].get("agent")) == MAIN_LANE:
            by_anchor.setdefault(tool_call_id, []).append(item)
        else:
            unanchored.append((item, _arrival(row)))

    merged: list[dict[str, Any]] = []
    for item in turn_items:
        merged.append(item)
        if item["event"] == "tool_call_result":
            merged.extend(by_anchor.pop(item["data"].get("tool_call_id"), ()))
    for leftover in by_anchor.values():
        merged.extend(leftover)
    return _place_by_steer(merged, unanchored)


def _place_by_steer(
    merged: list[dict[str, Any]],
    unanchored: list[tuple[dict[str, Any], float | None]],
) -> list[dict[str, Any]]:
    """Put each row placed by its time in the bubble it reached live.

    Live, a record lands in whatever bubble the turn is writing, and a
    delivered steer opens a new one. A market-watch stamp is emitted at the
    model call it feeds, which runs after the steer that call reads, so a
    row that arrived before a steer belongs in the bubble that steer closed:
    it goes just ahead of it. A row that arrived after every steer, or has
    no time at all, stays at the tail. A throttled replay of a market-watch
    block reuses its fetch time; but such a replay carries the same content
    and is the same row, so the row sits where the block was first shown.
    """
    if not unanchored:
        return merged
    steers = [
        (index, at)
        for index, item in enumerate(merged)
        if item["event"] == "steering_delivered"
        and agent_lane(item["data"].get("agent")) == MAIN_LANE
        and (at := _epoch(item["data"].get("timestamp"))) is not None
    ]
    before: dict[int, list[dict[str, Any]]] = {}
    tail: list[dict[str, Any]] = []
    for item, at in unanchored:
        index = next(
            (i for i, steer_at in steers if at is not None and at < steer_at), None
        )
        if index is None:
            tail.append(item)
        else:
            before.setdefault(index, []).append(item)
    if before:
        placed: list[dict[str, Any]] = []
        for index, item in enumerate(merged):
            placed.extend(before.get(index, ()))
            placed.append(item)
        merged = placed
    merged.extend(tail)
    return merged


def _arrival(row: dict[str, Any]) -> float | None:
    """When a row's record reached the client: its ``emitted_at``, or for a
    row written before that was kept, its fetch time. The fetch can be well
    before: code in the sandbox reports what it fetched only when it
    returns."""
    emitted = _epoch(row.get("emitted_at"))
    return emitted if emitted is not None else _epoch(row.get("source_timestamp"))


def _epoch(value: Any) -> float | None:
    """Seconds since the epoch from a row's datetime, an ISO string, or the
    float a steer payload stamps; None for anything else."""
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        try:
            value = datetime.fromisoformat(value)
        except ValueError:
            return None
    if isinstance(value, datetime):
        return value.timestamp()
    return None


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
    """Reconstruct the terminal ``error`` event from an errored response row.

    A row with no error text still gets one, its text empty: the event is what
    marks the turn failed on a reload and offers its Retry, and a failure whose
    exception had no message (or a row from before errors were stored) is as
    failed as any other. The client words an empty one.

    The row's ``error_kind`` rides along, as the live frame carried it. A
    fault of this service's own (``internal``) replays with no text: the
    stored error is a diagnostic, naming the run and its stream, and the
    client words that failure by its kind.
    """
    if not response or response.get("status") != "error":
        return None
    errors = response.get("errors")
    last = errors[-1] if isinstance(errors, list) and errors else ""
    metadata = response.get("metadata")
    if not isinstance(metadata, dict):
        metadata = {}
    kind = metadata.get("error_kind")
    data: dict[str, Any] = {
        "thread_id": thread_id,
        # Rows may predate persistence-side sanitization. Scrub again at the
        # trust boundary so historical secrets never reach the replay wire.
        "error": _sanitize_error_text(str(last)) if last and kind != "internal" else "",
        "type": "workflow_error",
    }
    for key in ("error_type", "error_class"):
        if metadata.get(key):
            data[key] = metadata[key]
    if kind in ("upstream", "internal"):
        data["error_kind"] = kind
        status = metadata.get("error_status_code")
        if kind == "upstream" and isinstance(status, int):
            data["status_code"] = status
    return {"event": "error", "data": data}


def terminal_items(
    thread_id: str,
    response: dict[str, Any] | None,
    turn_items: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """The closing events a turn's response row owes its replay, in wire order.

    A client shows one failure per turn (the web the last error, older
    clients the first), and the ``error`` frame the user saw has the model,
    hints and words the row lacks. So a failed turn replays that frame with
    the row's kind stamped on it: in place, when a legacy turn's facts
    placed its archived copy, or as its row's replay facts kept it or its
    archive held it (a stub places no facts). Only a failure that sent no
    frame (its stream quota spent, a fault outside the producer) gets the
    row's own item. Both read ``turn_items``, so the caller appends after.
    """
    stop = stopped.stop_close_item(thread_id, response, turn_items)
    error = _error_item(thread_id, response)
    if error is not None:
        stored = _stored_error(turn_items)
        frame = (
            stored
            or replay_facts.error_frame(thread_id, response)
            or legacy.archived_error(thread_id, response)
        )
        if frame is not None:
            _stamp_row_kind(frame["data"], error["data"])
            error = None if stored else frame
    return [item for item in (stop, error) if item]


def _stored_error(turn_items: list[dict[str, Any]]) -> dict[str, Any] | None:
    """The main lane's last ``error`` frame: a subagent's failure belongs to
    its card, not to the turn."""
    for item in reversed(turn_items):
        if item["event"] == "error" and agent_lane(item["data"].get("agent")) == MAIN_LANE:
            return item
    return None


def _stamp_row_kind(frame: dict[str, Any], row: dict[str, Any]) -> None:
    """The row's kind and status over the frame's own, as finalize read them
    off the same exception. An ``internal`` frame then drops its text by
    ``_error_item``'s rule, also one whose row predates kinds: its own kind
    names the same fault, and its text is the same diagnostic."""
    for key in ("error_kind", "status_code"):
        if key in row:
            frame[key] = row[key]
    if frame.get("error_kind") == "internal":
        frame["error"] = ""
    elif isinstance(frame.get("error"), str):
        frame["error"] = _sanitize_error_text(frame["error"])


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

