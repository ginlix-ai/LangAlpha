"""Which app a request came from, and what a turn's computer start is for.

Both feed only telemetry, so what matters is that the apps users actually
run land in different buckets and that a turn nobody sent is never counted
as a user's.
"""

from __future__ import annotations

import pytest

from src.server.services.computer_usage.causes import computer_cause, turn_cause
from src.server.utils.request_client import _client, classify_user_agent


@pytest.mark.parametrize(
    "user_agent, client",
    [
        (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"
            " (KHTML, like Gecko) LangAlpha/1.4.0 Chrome/130.0 Electron/33.2.0"
            " Safari/537.36",
            "desktop",
        ),
        (
            "Mozilla/5.0 (iPhone; CPU iPhone OS 18_0 like Mac OS X) AppleWebKit/605.1.15"
            " (KHTML, like Gecko) Version/18.0 Mobile/15E148 Safari/604.1",
            "browser",
        ),
        ("LangAlpha/27 CFNetwork/1568.100.1 Darwin/24.0.0", "mobile"),
        ("okhttp/4.12.0", "mobile"),
        ("langalpha-cli/0.9.1", "cli"),
        ("python-httpx/0.28.1", None),
        (None, None),
    ],
)
def test_each_app_lands_in_its_own_bucket(user_agent, client):
    assert classify_user_agent(user_agent) == client


@pytest.mark.parametrize(
    "origin, platform, delegated, expected",
    [
        ("turn", None, False, ("turn", "browser")),
        ("turn", "web", False, ("turn", "browser")),
        ("turn", "slack", False, ("turn", "channel")),
        ("turn", "cli", False, ("turn", "cli")),
        ("turn", "market_view:AAPL", False, ("turn", "browser")),
        ("turn", None, True, ("delegated", None)),
        # A wake answers where its thread lives, which says nothing of who asked.
        ("wakeup", "slack", False, ("wakeup", None)),
        ("automation", None, False, ("automation", None)),
        ("report", None, False, ("report", None)),
        (None, None, False, ("turn", "browser")),
    ],
)
def test_a_turn_is_its_origin_and_only_a_users_turn_has_an_app(
    origin, platform, delegated, expected
):
    token = _client.set("browser")
    try:
        cause = turn_cause(origin, platform=platform, delegated=delegated)
    finally:
        _client.reset(token)
    assert cause == expected


def test_a_cause_with_no_app_stays_without_one_inside_a_request():
    token = _client.set("browser")
    try:
        with computer_cause("delegated", client=None) as handoff:
            pass
        with computer_cause("button") as button:
            pass
    finally:
        _client.reset(token)
    assert (handoff.client, button.client) == (None, "browser")
