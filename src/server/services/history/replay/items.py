"""Replay item builders: table-sourced synthesis and the shared enrich stamp."""

from __future__ import annotations

from typing import Any

from src.server.database.runs import lifecycle as tl_db
from src.server.database.provenance import provenance_row_to_event
from src.server.services.runs.sse_producer import build_credit_usage_data
from src.server.utils.error_sanitization import (
    sanitize_error_text as _sanitize_error_text,
)


def _user_message_item(
    thread_id: str,
    query: dict[str, Any],
    response: dict[str, Any] | None = None,
) -> dict[str, Any]:
    payload = {
        "thread_id": thread_id,
        "turn_index": query.get("turn_index"),
        "content": query.get("content"),
        "timestamp": query.get("created_at"),
        "metadata": query.get("metadata"),
    }
    if query.get("type") == "system":
        payload["query_type"] = "system"
    # The turn's run id, stamped only once the run is terminal — i.e. when this
    # replay carries the turn's actual content. The client records it as
    # "already rendered" so the report-back catch-up never re-attaches a run
    # whose turn is on screen (a duplicate bubble). A live run's stub must NOT
    # carry it: its content hasn't rendered, and marking it would suppress the
    # attach that streams it — hence the positive terminal check (an unknown
    # or legacy status must not stamp).
    if response is not None and response.get("status") in tl_db.TERMINAL_STATUSES:
        payload["run_id"] = str(response.get("conversation_response_id"))
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
    response = responses_by_turn.get(turn_index)
    items = [
        _user_message_item(thread_id, q, response)
        for q in queries_by_turn.get(turn_index, [])
    ]
    error_item = _error_item(thread_id, response)
    if error_item:
        response_id = (
            str(response.get("conversation_response_id")) if response else None
        )
        _enrich(error_item, thread_id, turn_index, response_id)
        items.append(error_item)
    return items
