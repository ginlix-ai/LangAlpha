"""What the capability map has to keep true for consent to mean anything.

Deliberately not a count of the curation. A vendor publishing a new tool, or us
curating one, is ordinary and should not fail a suite. What must never drift is
the shape: a tool reachable from a group the user declined, a group nobody
declared, or a write filed where a reader belongs.
"""

import pytest

from src.server.services import brokerage_capabilities as capabilities
from src.server.services import brokerages
from src.server.services.brokerage_capabilities import (
    GROUPS,
    UNCURATED,
    _CURATION,
    denied_tools,
    group_keys_for,
    group_of_tool,
    groups_for,
    is_always_denied,
    tools_for,
)
from src.server.services.brokerages import brokerage_names

VENDORS = sorted(_CURATION)


@pytest.mark.parametrize("vendor", VENDORS)
def test_no_tool_reachable_from_two_groups(vendor: str) -> None:
    """The one that would actually leak.

    A tool listed in both ``market_data`` and ``trading`` is granted by the
    former, so the trading toggle would stop meaning anything for it.
    """
    seen: dict[str, str] = {}
    for key, tools in _CURATION[vendor].items():
        for tool in tools:
            assert tool not in seen, (
                f"{vendor}.{tool} is in both {seen[tool]!r} and {key!r}"
            )
            seen[tool] = key


@pytest.mark.parametrize("vendor", VENDORS)
def test_groups_are_declared_and_ordered(vendor: str) -> None:
    declared = {g.key for g in GROUPS}
    assert set(_CURATION[vendor]) <= declared
    orders = [g.order for g in groups_for(vendor)]
    assert orders == sorted(orders)


def test_curation_covers_exactly_the_shipped_brokerages() -> None:
    """Both directions, and the second one is the load-bearing half.

    A map for a name nothing ships is merely dead. A shipped brokerage with no
    map is an open door: every reader treats "no curated tools" as "nothing to
    refuse", so the connector ships with an empty denial and passes the vendor's
    order tools to the agent whatever the user picked. Adding a brokerage and
    forgetting its curation is one commit, which is why the omission has to fail
    here rather than at the relay.
    """
    assert set(_CURATION) == brokerage_names()


@pytest.mark.parametrize("vendor", VENDORS)
def test_granting_everything_is_the_whole_curation(vendor: str) -> None:
    every = tools_for(vendor, group_keys_for(vendor))
    assert every == frozenset().union(*_CURATION[vendor].values())


@pytest.mark.parametrize("vendor", VENDORS)
def test_granting_nothing_permits_nothing(vendor: str) -> None:
    """Empty is a real answer, and it is not the same as None."""
    assert tools_for(vendor, []) == frozenset()


def test_a_server_we_curate_nothing_for_has_no_policy() -> None:
    """None means "not ours to police", which the relay reads as no allowlist."""
    assert tools_for("some_users_own_server", ["market_data"]) is None


def test_a_shipped_brokerage_we_have_not_curated_yet_is_a_policy_with_no_rules(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The window between listing a brokerage and curating it, stated honestly.

    Listing one in BROKERAGES is what puts a Connect button on it, and that
    lands before anyone has seen the vendor's tool list to group it. Nothing is
    classified, so nothing is refused: the connection carries the vendor's whole
    tool list, exactly as it would in any other client.

    The registry lookup is what still separates that from a server we hold no
    map for at all. Both block nothing, so the difference is no longer
    behavioural at the gate -- it is that this one reports a policy was computed
    (``policy_required``), and the other reports there is no policy to compute.
    Curating the brokerage is what turns the empty rule set into a real one.

    The window is staged rather than found among the shipped names. Every
    brokerage is curated today, so looking for a real one makes this pass by
    finding nothing to check, green while testing nothing at all.
    """
    shipped = brokerages.Brokerage(
        name="not_curated_yet",
        label="Not Curated Yet",
        url="https://api.example.com/mcp",
        site="example.com",
        description="A brokerage listed ahead of its curation.",
    )
    monkeypatch.setattr(
        capabilities,
        "brokerage_by_name",
        lambda name: shipped if name == shipped.name else None,
    )

    assert shipped.name not in _CURATION
    assert tools_for(shipped.name, []) == frozenset()
    assert tools_for(shipped.name, ["market_data", "trading"]) == frozenset()
    # No toggles to offer, which is what keeps the consent dialog away.
    assert group_keys_for(shipped.name) == ()
    # And what the gate reads: nothing is classified, so nothing is refused.
    # The affirmative set being empty is why the two must not be swapped there.
    assert denied_tools(shipped.name, []) == frozenset()


@pytest.mark.parametrize("vendor", VENDORS)
def test_uncurated_tools_are_in_no_group(vendor: str) -> None:
    every = tools_for(vendor, group_keys_for(vendor))
    for tool in UNCURATED.get(vendor, ()):
        assert tool not in every


@pytest.mark.parametrize("vendor", VENDORS)
def test_uncurated_tools_are_refused_however_much_is_granted(vendor: str) -> None:
    """Being in no group stopped meaning "unreachable" when the gate inverted.

    A tool in no group now passes, which is the point: a vendor's new one is
    not refused until we ship. UNCURATED is the opposite case wearing the same
    shape -- read, understood, and deliberately kept away -- so it has to be
    denied explicitly or the list that exists to block a tool becomes the thing
    that lets it through.
    """
    everything = denied_tools(vendor, group_keys_for(vendor))
    for tool in UNCURATED.get(vendor, ()):
        assert tool in everything


def test_the_two_tools_a_prefix_rule_misfiles() -> None:
    """Both read like reads and are not, which is why curation is by hand."""
    quotes = tools_for("moomoo", ["market_data"])
    assert "quote_modify_user_security" not in quotes
    assert "quote_modify_user_security" in tools_for("moomoo", ["watchlists"])

    # IBKR's feedback tool submits a message to the broker in the user's name.
    assert "provide_customer_feedback" in UNCURATED["ibkr"]


def test_placing_an_order_takes_the_trading_group() -> None:
    for vendor, tool in (
        ("moomoo", "trading_order_place"),
        ("robinhood", "place_equity_order"),
        # Crypto rides the same rung as equity: the group answers what the
        # action costs, not what it trades.
        ("robinhood", "place_crypto_order"),
    ):
        without = [k for k in group_keys_for(vendor) if k != "trading"]
        assert tool not in tools_for(vendor, without)
        assert tool in tools_for(vendor, ["trading"])


@pytest.mark.parametrize("vendor", ["ibkr", "webull"])
def test_a_broker_that_places_nothing_is_never_asked_about_trading(
    vendor: str,
) -> None:
    """Neither publishes a tool that places an order, so the toggle would lie."""
    assert "trading" not in group_keys_for(vendor)


def test_webull_has_no_rung_at_all() -> None:
    """Read-only by the vendor's own line, not by our reading of a tool list.

    Webull's consent screen offers account, order query, market data and
    instruments, and no trading capability, so the write scope is not grantable
    and nothing published places, previews or stages an order. IBKR by contrast
    stops one rung down rather than at zero, with a staged order a human submits.
    """
    rungs = {g.key for g in GROUPS if g.rung}
    assert not rungs & set(group_keys_for("webull"))
    assert "staged_orders" in set(group_keys_for("ibkr")) & rungs


@pytest.mark.parametrize("vendor", VENDORS)
def test_a_tool_no_group_names_is_never_denied(vendor: str) -> None:
    """The headline of the policy: regulate the known, do not block the unknown.

    Stated against a name the vendor has not published, which is what a tool
    added between our releases looks like to this map.
    """
    for granted in ([], list(group_keys_for(vendor))):
        assert "a_tool_the_vendor_added_later" not in denied_tools(vendor, granted)


@pytest.mark.parametrize("vendor", VENDORS)
def test_denial_is_the_complement_of_the_grant(vendor: str) -> None:
    """Every curated tool is on exactly one side of the line, for any grant.

    UNCURATED sits outside that split on purpose: it is in neither the grant nor
    the curation, and is denied regardless, so it is excluded before comparing.
    """
    curated = frozenset().union(*_CURATION[vendor].values())
    uncurated = frozenset(UNCURATED.get(vendor, ()))
    for granted in ([], ["market_data"], list(group_keys_for(vendor))):
        allowed = tools_for(vendor, granted)
        refused = denied_tools(vendor, granted) - uncurated
        assert allowed & refused == frozenset()
        assert allowed | refused == curated


@pytest.mark.parametrize("vendor", VENDORS)
def test_granting_everything_denies_only_what_we_set_aside(vendor: str) -> None:
    assert denied_tools(vendor, group_keys_for(vendor)) == frozenset(
        UNCURATED.get(vendor, ())
    )


def test_a_server_we_curate_nothing_for_has_no_denial() -> None:
    """None, not empty: the relay reads it as no policy, same as before."""
    assert denied_tools("some_users_own_server", ["market_data"]) is None


def test_declining_trading_still_refuses_the_tools_that_place_orders() -> None:
    """What the inversion must not have cost."""
    for vendor, tool in (
        ("moomoo", "trading_order_place"),
        ("robinhood", "place_equity_order"),
        ("robinhood", "place_crypto_order"),
    ):
        without = [k for k in group_keys_for(vendor) if k != "trading"]
        assert tool in denied_tools(vendor, without)
        assert tool not in denied_tools(vendor, group_keys_for(vendor))


def test_the_catalog_annotation_reads_a_name_the_way_the_relay_does() -> None:
    """The display and the enforcement have to name the same tool.

    Discovery returns the vendor's spelling, and the relay and the workspace
    filter compare it folded. Annotating on the exact bytes instead let a
    recased or padded name be refused per call and stripped from the composite
    while the detail view still reported it as unclassified, which the client
    draws as callable.
    """
    assert group_of_tool("moomoo", " Trading_Order_Place ") == "trading"
    assert is_always_denied("ibkr", "Provide_Customer_Feedback\u00a0") is True
    # Folding widens what matches; it must not invent a match.
    assert group_of_tool("moomoo", "trading_order_place_v2") is None
    assert is_always_denied("ibkr", "provide_customer_feedback_v2") is False
