"""A wakeup is a turn the server posts itself, and is marked as one in every
place that tells those turns from a user's."""

from unittest.mock import patch

import pytest

from ptc_agent.agent.transcript.classify import (
    MARKET_TARGET_TRIGGER,
    WAKE_TRIGGER,
    is_steering_trigger,
    reentry_trigger,
)
from src.server.app.threads import messaging
from src.server.contracts.turn_kinds import (
    ANNOUNCED_KEYS,
    NOTIFICATION_QUERY_TYPES,
    WAKE_QUERY_TYPE,
    announced,
)
from src.server.handlers.chat.request_prep import is_notification_turn
from src.server.models.chat import ChatRequest

NOTICES = {"v": 1, "notices": [{"short_id": "w_3f2a"}]}


def test_a_wakeup_is_a_notification_query_type():
    assert "wakeup" in NOTIFICATION_QUERY_TYPES
    assert WAKE_QUERY_TYPE == "wakeup"


def test_announced_picks_the_keys_a_turn_names_in_key_order():
    metadata = {
        "wakeup": NOTICES,
        "unrelated": 1,
        "market_target": {"target_id": "tg-1"},
        "report_source": None,
    }
    assert list(announced(metadata)) == ["market_target", "wakeup"]
    assert announced(metadata)["wakeup"] == NOTICES
    assert "wakeup" in ANNOUNCED_KEYS


def test_a_wakeup_request_is_a_notification_turn():
    assert is_notification_turn(ChatRequest(query_type="wakeup"))
    assert not is_notification_turn(ChatRequest(query_type="wakeup", platform="slack"))


def test_a_client_cannot_claim_a_wakeup_turn():
    assert "wakeup" in messaging._SERVER_ONLY_FIELDS
    request = ChatRequest(query_type="wakeup", wakeup=NOTICES)
    with patch.object(messaging, "_is_service_request", return_value=False):
        cleared = messaging._client_body(request, object())
    assert (cleared.query_type, cleared.wakeup) == (None, None)


def test_the_server_keeps_the_wakeup_it_posts():
    request = ChatRequest(query_type="wakeup", wakeup=NOTICES)
    with patch.object(messaging, "_is_service_request", return_value=True):
        assert messaging._client_body(request, object()) is request


def test_the_wake_trigger_is_a_steering_trigger_so_compaction_drops_it():
    assert is_steering_trigger(WAKE_TRIGGER)
    assert WAKE_TRIGGER == "A wakeup arrived."


@pytest.mark.parametrize(
    "senders, trigger",
    [
        (["wakeup"], WAKE_TRIGGER),
        (["wakeup", "market_alert"], WAKE_TRIGGER),
        (["market_alert"], MARKET_TARGET_TRIGGER),
        (["wakeup", "user"], "Wakeup and User sent additional instructions."),
    ],
)
def test_reentry_takes_the_wakeup_trigger_when_only_notices_wait(senders, trigger):
    assert reentry_trigger(senders) == trigger
