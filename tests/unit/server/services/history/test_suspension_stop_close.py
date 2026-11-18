"""A turn its account's suspension stopped replays closed, and says so."""

from __future__ import annotations

from src.server.services.history.replay.stopped import stop_close_item

LAST = {"event": "message_chunk", "data": {"id": "m1", "role": "assistant", "content": "Half"}}


def _row(**meta):
    return {"status": "cancelled", "metadata": meta}


def test_a_suspension_stop_closes_its_last_message_as_the_suspensions():
    close = stop_close_item("t", _row(stopped_by_suspension=True), [LAST])
    assert close == {"event": "message_chunk", "data": {
        "thread_id": "t", "role": "assistant", "finish_reason": "stopped",
        "stopped_by": "suspension", "id": "m1",
    }}


def test_a_users_stop_names_no_stopper():
    close = stop_close_item("t", _row(cancelled_by_user=True), [LAST])
    assert close is not None and "stopped_by" not in close["data"]


def test_another_system_cancel_gets_no_close():
    assert stop_close_item("t", _row(), [LAST]) is None
