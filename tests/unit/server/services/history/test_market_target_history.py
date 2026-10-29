"""A market target's notice after the fact: a live turn's alert replays from
the checkpoint inside the steering message that carried it, and a wake turn's
query replays with the notice it delivered."""

from __future__ import annotations

from langchain_core.messages import AIMessage, HumanMessage

from src.server.services.history.projector import (
    history_events_to_sse,
    messages_to_history_events,
)
from src.server.services.history.replay.items import _user_message_item

_NOTICE = {
    "target_id": "tg-1",
    "notice": "hit",
    "symbol": "AAPL",
    "kind": "above",
    "value": 230.0,
    "pct_from": None,
    "armed_price": 222.1,
    "fired_price": 230.45,
    "fired_at": "2026-10-12T14:31:00+00:00",
    "expires_at": None,
}


def _steering_with_alert() -> HumanMessage:
    return HumanMessage(
        id="m-steer",
        content="<market-alert>\nAAPL hit your target\n</market-alert>",
        additional_kwargs={
            "lc_source": "steering",
            "steering_delivered": {
                "messages": [
                    {"content": "", "market_alert": _NOTICE, "timestamp": 1.0}
                ],
            },
        },
    )


def test_alert_in_a_steering_message_replays_from_the_checkpoint():
    events = messages_to_history_events(
        [
            HumanMessage(id="m-0", content="watch AAPL"),
            _steering_with_alert(),
            AIMessage(content="ok"),
        ]
    )

    sse = history_events_to_sse(events, thread_id="t-1")

    delivered = [i for i in sse if i["event"] == "steering_delivered"]
    assert len(delivered) == 1
    assert delivered[0]["data"]["messages"][0]["market_alert"] == _NOTICE
    assert delivered[0]["data"]["thread_id"] == "t-1"
    # The alert never shows up as something the user typed.
    assert not any(i["event"] == "user_message" for i in sse)


def test_wake_query_replays_with_its_notice():
    item = _user_message_item(
        "t-1",
        {
            "type": "market_target",
            "turn_index": 3,
            "content": "<market-alert>...</market-alert>",
            "metadata": {"msg_type": "ptc", "market_target": _NOTICE},
        },
    )

    data = item["data"]
    assert item["event"] == "user_message"
    assert data["query_type"] == "market_target"
    assert data["metadata"]["market_target"] == _NOTICE
    assert isinstance(data["metadata"]["market_target"]["fired_price"], float)
