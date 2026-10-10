"""The close a user-stopped turn owes its last assistant message."""

from __future__ import annotations

from typing import Any

from src.server.contracts.status import is_user_stop
from src.server.services.history.replay.lanes import MAIN_LANE, agent_lane


def stop_close_item(
    thread_id: str,
    response: dict[str, Any] | None,
    turn_items: list[dict[str, Any]],
) -> dict[str, Any] | None:
    """The ``finish_reason: "stopped"`` close for a user-stopped turn's last
    main-lane assistant message, unless the turn already carries one for it.

    Stopped is a fact about the turn, recorded on its response row, but the
    stop finalize writes a close only for a message still streaming at the
    stop. A stop between messages (during a tool, before the next model call)
    leaves none, and a close for a message the checkpoint committed does not
    survive the merge. A stop during bring-up, before any assistant event,
    has no message to name, so its close carries no ``id``: clients apply a
    close to the turn's bubble by ``turn_index``. System cancels are not
    stops and get no close.
    """
    if not is_user_stop(response):
        return None
    last: dict[str, Any] | None = None
    closed: set[str] = set()
    for item in turn_items:
        data = item.get("data") if isinstance(item, dict) else None
        if (
            not isinstance(data, dict)
            or item.get("event") not in ("message_chunk", "tool_calls")
            or not data.get("id")
            or agent_lane(data.get("agent")) != MAIN_LANE
        ):
            continue
        last = data
        if data.get("finish_reason") == "stopped":
            closed.add(data["id"])
    if last is not None and last["id"] in closed:
        return None
    close: dict[str, Any] = {
        "thread_id": thread_id,
        "role": "assistant",
        "finish_reason": "stopped",
    }
    if last is not None:
        close["id"] = last["id"]
        if last.get("agent"):
            close["agent"] = last["agent"]
    return {"event": "message_chunk", "data": close}
