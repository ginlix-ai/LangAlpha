"""Which shared description a brokerage row reads: the one written for the
groups its grant puts in force, read off the connection the denial reads."""

from __future__ import annotations

from src.server.services.brokerages import brokerage_by_name
from src.server.services.mcp_descriptions.keys import Scope, dial_identity

ROBINHOOD = brokerage_by_name("robinhood").url


def _connection(granted, url=ROBINHOOD):
    return {"server_url": url, "granted_capabilities": granted}


def test_users_with_the_same_groups_in_force_share_one_description():
    assert Scope.of("u1", "rh", ROBINHOOD, _connection(["account", "market_data"])) == (
        Scope.of("u2", "my broker", ROBINHOOD, _connection(["market_data", "account", "nope"]))
    )


def test_live_trading_off_reads_a_different_description_than_on():
    on = Scope.of("u1", "rh", ROBINHOOD, _connection(["account", "trading"]))
    off = Scope.of("u1", "rh", ROBINHOOD, _connection(["account"]))

    assert on.key != off.key
    assert Scope.parse(off.key) == Scope("robinhood", ("account",))


def test_a_group_whose_requirement_is_not_granted_is_not_in_force():
    assert Scope.of("u1", "rh", ROBINHOOD, _connection(["trading"])) == (
        Scope.at_brokerage("robinhood", ())
    )


def test_no_connection_is_consent_to_nothing():
    assert Scope.of("u1", "rh", ROBINHOOD, None) == Scope.at_brokerage("robinhood", ())


def test_a_connection_consented_at_another_url_binds_nothing():
    elsewhere = "https://mcp.example.com/mcp"

    assert Scope.of("u1", "rh", elsewhere, _connection(["account"])) == (
        Scope.of("u1", "rh", elsewhere, None)
    )


def test_a_users_server_is_keyed_by_its_address_without_storing_it():
    url = "https://mcp.example.com/mcp?key=secret"
    mine = Scope.of("u1", "mine", url, None)

    assert Scope.parse(mine.key) == mine
    assert "secret" not in mine.key
    assert Scope.of("u1", "mine", "https://another.example.com/mcp", None) != mine


def test_an_edit_that_can_change_the_tools_reads_none_of_the_old_description():
    def scope(**row):
        return Scope.of("u1", "local", row.get("url"), None, dial_identity("local", row))

    stdio = {"transport": "stdio", "command": "uvx", "args": ["a@1"], "env": {"MODE": "read"}}
    old = scope(**stdio)

    assert scope(**{**stdio, "args": ["b@1"]}) != old
    assert scope(**{**stdio, "env": {"MODE": "write"}}) != old
    assert scope(**{**stdio, "description": "Mine."}) == old
    http = {"transport": "http", "url": "https://mcp.example.com/mcp", "headers": {"X-Tools": "a"}}
    assert scope(**{**http, "headers": {"X-Tools": "b"}}) != scope(**http)
