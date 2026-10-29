"""A wakeup's notices after the fact: a wake turn's query replays typed
``wakeup`` with the notices it delivered, and one a live turn took in replays
from the checkpoint inside the steering message that carried it."""

from __future__ import annotations

from langchain_core.messages import AIMessage, HumanMessage

from src.server.services.history.projector import (
    history_events_to_sse,
    messages_to_history_events,
)
from src.server.services.history.replay.items import _user_message_item

_NOTICES = {
    "v": 1,
    "notices": [
        {
            "wakeup_id": "33333333-3333-3333-3333-333333333333",
            "short_id": "w_3f2a",
            "label": "NVDA pullback entry",
            "kind": "cron",
            "notice": "fire",
            "seq": 7,
            "total": 24,
            "last": False,
            "skipped": 0,
            "fired_at": "2026-10-20T14:30:00+00:00",
            "timezone": "America/New_York",
            "cron": "*/5 10-15 * * 1-5",
            "price": None,
            "order": None,
        }
    ],
}


def test_a_wake_turns_query_replays_as_a_wakeup_with_its_notices():
    item = _user_message_item(
        "t-1",
        {
            "type": "wakeup",
            "turn_index": 3,
            "content": "<wakeup>\nCheck 7 of 24.\n</wakeup>",
            "metadata": {"msg_type": "ptc", "wakeup": _NOTICES},
        },
    )

    data = item["data"]
    assert item["event"] == "user_message"
    assert data["query_type"] == "wakeup"
    assert data["metadata"]["wakeup"] == _NOTICES


def test_a_notice_a_live_turn_took_in_replays_from_the_checkpoint():
    steering = HumanMessage(
        id="m-steer",
        content="<wakeup>\nCheck 7 of 24.\n</wakeup>",
        additional_kwargs={
            "lc_source": "steering",
            "steering_delivered": {
                "count": 0,
                "messages": [
                    {"content": "", "wakeup": _NOTICES["notices"][0], "timestamp": 1.0}
                ],
            },
        },
    )
    events = messages_to_history_events(
        [
            HumanMessage(id="m-0", content="watch NVDA"),
            steering,
            AIMessage(content="ok"),
        ]
    )

    sse = history_events_to_sse(events, thread_id="t-1")

    delivered = [i for i in sse if i["event"] == "steering_delivered"]
    assert len(delivered) == 1
    assert delivered[0]["data"]["messages"][0]["wakeup"] == _NOTICES["notices"][0]
    # The notice never shows up as something the user typed.
    assert not any(i["event"] == "user_message" for i in sse)
