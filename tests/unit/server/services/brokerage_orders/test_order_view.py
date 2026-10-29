"""An order said back in words, read off the attempt rows the ledger writes
(``order_json`` is a ``BrokerOrder``'s ``to_json``)."""

import pytest

from src.server.services.brokerage_orders.models import BrokerOrder
from src.server.services.brokerage_orders.view import order_view


def attempt(order: BrokerOrder, **over) -> dict:
    return {
        "attempt_id": "22222222-2222-2222-2222-222222222222",
        "vendor": "moomoo",
        "mode": "live",
        "action": "place",
        "status": "submitted",
        "vendor_order_id": "7712045",
        "order_json": order.to_json(),
        **over,
    }


def equity_limit(**over) -> BrokerOrder:
    fields = dict(
        vendor="moomoo",
        account_ref="acct_test",
        asset_class="equity",
        instrument={"kind": "equity", "symbol": "NVDA", "venue": "NASDAQ"},
        side="buy",
        qty=100,
        order_type="limit",
        limit_price=200,
    )
    return BrokerOrder(**{**fields, **over})


def test_the_summary_names_side_quantity_symbol_price_and_broker():
    view = order_view(attempt(equity_limit()))
    assert view.summary == "buy 100 NVDA, limit $200.00, at moomoo"
    assert "NVDA" in view.summary


def test_the_live_form_puts_the_mode_before_the_broker():
    assert order_view(attempt(equity_limit())).summary_live == (
        "buy 100 NVDA, limit $200.00, live at moomoo"
    )
    paper = order_view(attempt(equity_limit(mode="paper"), mode="paper"))
    assert paper.summary_live.endswith("paper at moomoo")


def test_an_equity_reads_on_its_exchange_clock():
    assert order_view(attempt(equity_limit())).timezone == "America/New_York"


def test_a_market_order_has_no_price_in_its_summary():
    order = equity_limit(order_type="market", limit_price=None, side="sell", qty=50)
    assert order_view(attempt(order)).summary == "sell 50 NVDA, market, at moomoo"


def test_an_option_names_its_underlying_expiry_and_strike():
    order = equity_limit(
        asset_class="option",
        instrument={
            "kind": "option",
            "underlying": "NVDA",
            "expiration": "2026-11-20",
            "strike": 210,
            "right": "C",
        },
        qty=2,
    )
    summary = order_view(attempt(order)).summary
    assert summary.startswith("buy 2 NVDA 2026-11-20 210C")


def test_a_combo_on_one_symbol_names_it_before_its_strategy():
    legs = [
        {"symbol": "AAPL", "leg": "take_profit", "limit_price": "260"},
        {"symbol": "AAPL", "leg": "stop_loss", "stop_price": "200"},
    ]
    order = equity_limit(
        instrument={"kind": "combo", "legs": legs, "strategy": "oco"},
        order_type=None,
        limit_price=None,
        side="sell",
        qty=10,
    )
    assert order_view(attempt(order)).summary.startswith("sell 10 AAPL oco")


@pytest.mark.parametrize("bad", [None, {}])
def test_no_attempt_gives_none(bad):
    assert order_view(bad) is None


@pytest.mark.parametrize(
    "order_json",
    [
        {"side": "sideways"},  # a value no order has
        "not an object",
    ],
)
def test_a_malformed_attempt_gives_none_rather_than_failing_the_line(order_json):
    assert (
        order_view({"vendor": "moomoo", "mode": "live", "order_json": order_json})
        is None
    )
