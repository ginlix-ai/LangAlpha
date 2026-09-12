"""The Robinhood adapter against the shapes a live account returned.

The equity and option fixtures are the broker's own bodies with every id
replaced; the crypto order is the previewed one, which is as far as the
account's region allowed; exercise and combo are still the published schemas.
The tolerant arms matter more than the happy path: a shape the adapter has not
met has to come out ``unknown`` with the body intact, never as an exception.

Every account number, order id and option id is invented.
"""

import json
from datetime import UTC, date, datetime
from decimal import Decimal

import pytest

from src.server.services.brokerage_orders import (
    AttemptStatus,
    ComboRef,
    CryptoRef,
    EquityRef,
    OpaqueRef,
    OptionRef,
    OrderMode,
    adapter_for,
)
from src.server.services.brokerage_orders.base import StatusQuery, StatusReadError

RH = adapter_for("robinhood")

ACCOUNT = "1234567"
ORDER_ID = "4d5e6f70-1111-4000-8000-000000000001"
OPTION_ID = "1f6ccd01-2222-4000-8000-000000000002"

EQUITY_PLACE_ARGS = {
    "account_number": ACCOUNT,
    "symbol": "AAPL",
    "side": "buy",
    "type": "limit",
    "quantity": "3",
    "limit_price": "180.25",
    "time_in_force": "gfd",
    "market_hours": "extended_hours",
    "ref_id": "8f1c0f2e-3333-4000-8000-000000000003",
}

OPTION_PLACE_ARGS = {
    "account_number": ACCOUNT,
    "quantity": "1",
    "type": "limit",
    "price": "2.50",
    "direction": "debit",
    "time_in_force": "gfd",
    "market_hours": "regular_curb_hours",
    "legs": [
        {"option_id": OPTION_ID, "side": "buy", "position_effect": "open"},
        {"option_id": "3a2b1c00-4444-4000-8000-000000000004", "side": "sell",
         "position_effect": "open", "ratio_quantity": 1},
    ],
}

# The place answer, in the envelope every Robinhood tool replies inside.
EQUITY_PLACE_RESULT = {
    "data": {
        "order": {
            "id": ORDER_ID,
            "instrument_id": "5e6f7081-5555-4000-8000-000000000005",
            "symbol": "",
            "side": "buy",
            "type": "market",
            "trigger": "immediate",
            "state": "queued",
            "quantity": "0.003200",
            "cumulative_quantity": "0.000000",
            "price": "311.810000",
            "stop_price": None,
            "average_price": None,
            "fees": "0.000000",
            "dollar_based_amount": {"amount": "1.000000", "currency_code": "USD"},
            "time_in_force": "gfd",
            "market_hours": "regular_hours",
            "placed_agent": "agentic",
            "created_at": "2026-08-06T19:00:00.000000Z",
            "last_transaction_at": "2026-08-06T19:00:01.000000Z",
            "executions": [],
        }
    },
    "guide": "The order was accepted.",
}

CANCEL_RESULT = {
    "data": {"accepted": True},
    "guide": "accepted=true means the broker accepted the cancel request.",
}

# A vendor rejection is prose with the upstream status inlined, not an envelope.
DUPLICATE_REF_RESULT = 'API error 409: {"detail":"Reference ID must be unique."}'


def test_adapter_for_knows_robinhood():
    assert RH is not None
    assert RH.vendor == "robinhood"


def test_equity_place_request():
    order = RH.parse_request("place_equity_order", EQUITY_PLACE_ARGS)

    assert order.vendor == "robinhood"
    assert order.account_ref == ACCOUNT
    assert order.mode is OrderMode.LIVE
    assert order.asset_class == "equity"
    assert order.instrument == EquityRef(symbol="AAPL")
    assert order.side == "buy"
    assert order.qty == Decimal("3")
    assert order.notional is None
    assert order.order_type == "limit"
    assert order.limit_price == Decimal("180.25")
    assert order.time_in_force == "day"
    assert order.session == "rth_plus_ext"
    # The idempotency key is the one arg a reconciler can match on, so it rides
    # out rather than being consumed into a field that cannot hold it.
    assert order.extras == {"ref_id": EQUITY_PLACE_ARGS["ref_id"]}
    assert order.raw == EQUITY_PLACE_ARGS


def test_stop_market_is_one_word_in_a_request():
    order = RH.parse_request(
        "place_equity_order",
        {
            "account_number": ACCOUNT,
            "symbol": "AAPL",
            "side": "sell",
            "type": "stop_market",
            "quantity": "2",
            "stop_price": "170.00",
            "market_hours": "regular_hours",
        },
    )

    assert order.order_type == "stop"
    assert order.stop_price == Decimal("170.00")
    assert order.side == "sell"
    assert order.session == "rth"


def test_notional_place_carries_the_dollar_amount():
    order = RH.parse_request(
        "place_equity_order",
        {
            "account_number": ACCOUNT,
            "symbol": "GPRO",
            "side": "buy",
            "type": "market",
            "dollar_amount": "1.00",
        },
    )

    assert order.qty is None
    assert order.notional is not None
    assert order.notional.amount == Decimal("1.00")
    # A request never names the currency, and the broker is dollars-only.
    assert order.notional.currency == "USD"
    assert order.order_type == "market"


def test_option_place_request_is_a_combo_of_its_legs():
    order = RH.parse_request("place_option_order", OPTION_PLACE_ARGS)

    assert order.asset_class == "option_combo"
    assert order.instrument == ComboRef(
        legs=(
            OPTION_PLACE_ARGS["legs"][0],
            OPTION_PLACE_ARGS["legs"][1],
        )
    )
    assert order.qty == Decimal("1")
    assert order.limit_price == Decimal("2.50")
    assert order.session == "rth_plus_ext"
    assert order.extras == {"direction": "debit"}


def test_single_leg_option_place_is_one_option():
    order = RH.parse_request(
        "place_option_order",
        {
            "account_number": ACCOUNT,
            "quantity": "1",
            "type": "limit",
            "price": "1.05",
            "legs": [{"option_id": OPTION_ID, "side": "buy", "position_effect": "open"}],
        },
    )

    assert order.asset_class == "option"
    # A request leg names only the option id; the expiry and strike come back.
    assert order.instrument == OpaqueRef(raw_code=OPTION_ID)
    # The side is stated inside the leg and nowhere else, and the approval card
    # is unreadable without it.
    assert order.side == "buy"


def test_answered_option_leg_resolves_to_an_option_ref():
    order = RH.parse_request(
        "place_option_order",
        {
            "account_number": ACCOUNT,
            "quantity": "1",
            "legs": [
                {
                    "option_id": OPTION_ID,
                    "chain_symbol": "AAPL",
                    "expiration_date": "2026-01-16",
                    "strike_price": "150.0000",
                    "option_type": "call",
                }
            ],
        },
    )

    assert order.instrument == OptionRef(
        underlying="AAPL",
        expiration=date(2026, 1, 16),
        strike=Decimal("150.0000"),
        right="C",
        vendor_instrument_id=OPTION_ID,
    )


def test_crypto_place_request_names_its_account_differently():
    order = RH.parse_request(
        "place_crypto_order",
        {
            "rhs_account_number": ACCOUNT,
            "symbol": "BTC-USD",
            "side": "buy",
            "type": "market",
            "quantity": "0.001",
        },
    )

    assert order.asset_class == "crypto"
    assert order.instrument == CryptoRef(pair="BTC-USD")
    assert order.qty == Decimal("0.001")
    assert order.order_type == "market"
    # The one account the ledger and every later read are built from, under the
    # only name a crypto tool accepts for it.
    assert order.account_ref == ACCOUNT
    assert order.extras == {}


def test_crypto_spells_a_stop_its_own_way():
    order = RH.parse_request(
        "place_crypto_order",
        {
            "rhs_account_number": ACCOUNT,
            "symbol": "BTC-USD",
            "side": "sell",
            "type": "stop_loss",
            "quantity": "0.001",
            "stop_price": "50000.00",
        },
    )

    assert order.order_type == "stop"
    assert order.stop_price == Decimal("50000.00")


def test_cancel_requests_carry_only_their_target():
    equity = RH.parse_request(
        "cancel_equity_order", {"account_number": ACCOUNT, "order_id": ORDER_ID}
    )
    option = RH.parse_request(
        "cancel_option_order", {"account_number": ACCOUNT, "order_id": ORDER_ID}
    )
    crypto = RH.parse_request(
        "cancel_crypto_order", {"rhs_account_number": ACCOUNT, "order_id": ORDER_ID}
    )

    assert equity.asset_class == "equity"
    assert option.asset_class == "option"
    assert crypto.asset_class == "crypto"
    for order in (equity, option, crypto):
        assert order.mode is OrderMode.LIVE
        assert order.instrument is None
        assert order.extras == {"order_id": ORDER_ID}
        # The order the cancel acts on, which is all a cancel is about.
        assert order.target_ref == ORDER_ID


def test_cancel_exercise_is_addressed_by_the_option():
    order = RH.parse_request(
        "cancel_option_exercise", {"account_number": ACCOUNT, "option_id": OPTION_ID}
    )

    assert order.asset_class == "option"
    assert order.instrument == OpaqueRef(raw_code=OPTION_ID)
    assert order.extras == {"option_id": OPTION_ID}
    # A contract is not an order: naming it as the target would point the
    # ledger's parent lookup at whatever order carries that number as its id.
    assert order.target_ref is None


def test_exercise_request_has_no_side_or_price():
    order = RH.parse_request(
        "exercise_option",
        {
            "account_number": ACCOUNT,
            "option_id": OPTION_ID,
            "quantity": 1,
            "reason": "buying_stocks",
        },
    )

    assert order.asset_class == "option"
    assert order.instrument == OpaqueRef(raw_code=OPTION_ID)
    assert order.qty == Decimal("1")
    assert order.side is None
    assert order.order_type is None
    assert order.time_in_force is None
    assert order.extras == {"reason": "buying_stocks"}


def test_a_read_tool_is_not_an_order():
    assert RH.parse_request("get_equity_orders", {"account_number": ACCOUNT}) is None


def test_equity_place_result():
    request = RH.parse_request("place_equity_order", EQUITY_PLACE_ARGS)
    outcome = RH.parse_result(
        "place_equity_order",
        EQUITY_PLACE_RESULT,
        tool_status="success",
        request=request,
    )

    assert outcome.status is AttemptStatus.SUBMITTED
    assert outcome.raw_status == "queued"
    assert outcome.vendor_order_id == ORDER_ID
    assert outcome.filled_qty == Decimal("0.000000")
    assert outcome.fees is not None
    assert outcome.fees.currency == "USD"
    assert outcome.created_at == datetime(2026, 8, 6, 19, 0, 0, tzinfo=UTC)
    assert outcome.updated_at == datetime(2026, 8, 6, 19, 0, 1, tzinfo=UTC)
    assert outcome.failure is None
    # An order id addresses a Robinhood order on its own.
    assert outcome.route == {}


def test_confirmed_and_filled_states():
    def state(value):
        body = {"data": {"order": {"id": ORDER_ID, "state": value}}}
        return RH.parse_result(
            "place_equity_order", body, tool_status="success"
        ).status

    assert state("unconfirmed") is AttemptStatus.SUBMITTED
    assert state("confirmed") is AttemptStatus.WORKING
    assert state("partially_filled") is AttemptStatus.PARTIALLY_FILLED
    assert state("filled") is AttemptStatus.FILLED
    assert state("cancelled") is AttemptStatus.CANCELLED
    # Terminal at the vendor, so it must not rest in an open state.
    assert state("partially_filled_rest_cancelled") is AttemptStatus.CANCELLED


def test_cancel_result_is_accepted_not_done():
    request = RH.parse_request(
        "cancel_equity_order", {"account_number": ACCOUNT, "order_id": ORDER_ID}
    )
    outcome = RH.parse_result(
        "cancel_equity_order", CANCEL_RESULT, tool_status="success", request=request
    )

    # The broker took the request; the cancel itself is asynchronous, so
    # claiming the order is cancelled here would be a lie the ledger keeps.
    assert outcome.status is AttemptStatus.SUBMITTED
    assert outcome.raw_status == "accepted"
    # The ack identifies nothing, so the order the request named is the only
    # identity this row will ever have -- and without it the reconciler cannot
    # look the cancel up at all.
    assert outcome.vendor_order_id == ORDER_ID


def test_cancel_exercise_ack_is_final():
    request = RH.parse_request(
        "cancel_option_exercise", {"account_number": ACCOUNT, "option_id": OPTION_ID}
    )
    outcome = RH.parse_result(
        "cancel_option_exercise",
        {"data": {"accepted": True}},
        tool_status="success",
        request=request,
    )

    # No tool lists exercise events, so nothing could ever read this row back:
    # an open status here would poll forever.
    assert outcome.status is AttemptStatus.CANCELLED
    assert outcome.raw_status == "accepted"
    # An exercise cancel names the contract, not an order, and an option id in
    # the order column would send the reconciler looking for an order that is
    # not one.
    assert outcome.vendor_order_id is None


def test_cancel_exercise_result_counts_what_it_undid():
    cancelled = RH.parse_result(
        "cancel_option_exercise",
        {"data": {"cancelled_count": 1, "events": []}},
        tool_status="success",
    )
    nothing = RH.parse_result(
        "cancel_option_exercise",
        {"data": {"cancelled_count": 0, "events": []}},
        tool_status="success",
    )

    assert cancelled.status is AttemptStatus.CANCELLED
    # Nothing was queued to undo, which the answer already says; left open it
    # would be listed on every sweep and never settle.
    assert nothing.status is AttemptStatus.REJECTED_BY_VENDOR
    assert nothing.failure is not None and nothing.failure.code == "nothing_queued"


def test_rejected_state_is_a_vendor_rejection():
    outcome = RH.parse_result(
        "place_option_order",
        {
            "data": {
                "order": {
                    "id": ORDER_ID,
                    "state": "rejected",
                    "reject_reason": "Not enough buying power",
                }
            }
        },
        tool_status="success",
    )

    assert outcome.status is AttemptStatus.REJECTED_BY_VENDOR
    assert outcome.failure.kind == "vendor"
    assert outcome.failure.message == "Not enough buying power"
    assert outcome.vendor_order_id == ORDER_ID


def test_api_error_prose_is_a_vendor_rejection():
    outcome = RH.parse_result(
        "place_equity_order", DUPLICATE_REF_RESULT, tool_status="success"
    )

    assert outcome.status is AttemptStatus.REJECTED_BY_VENDOR
    assert outcome.failure.kind == "vendor"
    assert outcome.failure.code == "409"
    assert outcome.failure.message == "Reference ID must be unique."


def test_forbidden_prose_is_a_vendor_rejection():
    outcome = RH.parse_result(
        "exercise_option",
        "error: FORBIDDEN: agent not authorized to access this account",
        tool_status="success",
    )

    assert outcome.status is AttemptStatus.REJECTED_BY_VENDOR
    assert outcome.failure.code is None
    assert "FORBIDDEN" in outcome.failure.message


def test_refused_body_and_tool_error():
    refused = RH.parse_result(
        "place_equity_order",
        "Refused: the connection to robinhood does not permit place_equity_order",
        tool_status="error",
    )
    failed = RH.parse_result(
        "place_equity_order", "upstream timeout", tool_status="error"
    )

    assert refused.status is AttemptStatus.REFUSED
    assert refused.failure.kind == "policy"
    assert failed.status is AttemptStatus.FAILED
    assert failed.failure.kind == "transport"


def test_an_unknown_state_keeps_the_vendor_word():
    outcome = RH.parse_result(
        "place_equity_order",
        {"data": {"order": {"id": ORDER_ID, "state": "locate_pending"}}},
        tool_status="success",
    )

    assert outcome.status is AttemptStatus.UNKNOWN
    assert outcome.raw_status == "locate_pending"
    assert outcome.vendor_order_id == ORDER_ID


def test_an_unrecognized_body_is_unknown_and_survives_whole():
    prose = RH.parse_result(
        "place_equity_order", "no long position found", tool_status="success"
    )
    envelope = RH.parse_result(
        "place_equity_order", {"something": "new"}, tool_status="success"
    )

    assert prose.status is AttemptStatus.UNKNOWN
    assert prose.raw_status == "no long position found"
    assert envelope.status is AttemptStatus.UNKNOWN
    assert envelope.raw == {"something": "new"}


def test_an_unparsable_request_never_raises():
    order = RH.parse_request("place_equity_order", {"side": 7, "type": ["limit"]})

    assert order.asset_class == "other"
    assert order.instrument is None
    assert order.side is None
    assert order.order_type is None
    assert order.account_ref == ""
    assert order.raw == {"side": 7, "type": ["limit"]}


def test_content_blocks_are_unwrapped():
    body = json.dumps(
        [{"type": "text", "text": json.dumps(EQUITY_PLACE_RESULT), "id": "lc_1"}]
    )
    outcome = RH.parse_result("place_equity_order", body, tool_status="success")

    assert outcome.status is AttemptStatus.SUBMITTED
    assert outcome.vendor_order_id == ORDER_ID


# --------------------------------------------------------------- status reads

# What ``get_equity_orders`` answers. The list read is the only place the symbol
# is populated: a place answer leaves it empty.
EQUITY_LIST_RESULT = {
    "data": {
        "orders": [
            {
                "id": ORDER_ID,
                "instrument_id": "1a2b3c4d-6666-4000-8000-000000000006",
                "symbol": "AAPL",
                "side": "buy",
                "type": "limit",
                "state": "cancelled",
                "quantity": "3.000000",
                "cumulative_quantity": "0.000000",
                "price": "180.250000",
                "stop_price": None,
                "average_price": None,
                "fees": "0.000000",
                "dollar_based_amount": None,
                "time_in_force": "gfd",
                "market_hours": "extended_hours",
                "trigger": "immediate",
                "placed_agent": "agentic",
                "created_at": "2026-08-06T19:00:00.000000Z",
                "last_transaction_at": "2026-08-06T19:07:00.000000Z",
                "executions": [],
            }
        ]
    },
    "guide": "state 'cancelled' = the cancel succeeded.",
}

# The same order as an option: the chain symbol sits above the legs, the legs
# carry the contract, and there is no fee field at all.
OPTION_LIST_RESULT = {
    "data": {
        "orders": [
            {
                "id": ORDER_ID,
                "chain_id": "2b3c4d5e-7777-4000-8000-000000000007",
                "chain_symbol": "AAPL",
                "state": "filled",
                "type": "limit",
                "trigger": "immediate",
                "direction": "debit",
                "quantity": "1.00000",
                "processed_quantity": "1.00000",
                "pending_quantity": "0.00000",
                "canceled_quantity": "0.00000",
                "price": "1.05000000",
                "stop_price": None,
                "premium": "105.00000000",
                "processed_premium": "105.00000000",
                "time_in_force": "gfd",
                "market_hours": "regular_hours",
                "opening_strategy": "long_call",
                "closing_strategy": None,
                "placed_agent": "agentic",
                "created_at": "2026-08-06T19:00:00.000000Z",
                "updated_at": "2026-08-06T19:00:02.000000Z",
                "last_transaction_at": None,
                "is_replaceable": False,
                "legs": [
                    {
                        "id": "3c4d5e6f-8888-4000-8000-000000000008",
                        "option_id": OPTION_ID,
                        "side": "buy",
                        "position_effect": "open",
                        "ratio_quantity": 1,
                        "expiration_date": "2026-01-16",
                        "strike_price": "150.0000",
                        "option_type": "call",
                    }
                ],
            }
        ]
    },
    "guide": "Describe orders by chain_symbol.",
}

# A dollar-based order, as the list reports it: the amount is wrapped, the
# share count is whatever the broker divided out of it, and the price is the
# last trade it used rather than anything the request named.
DOLLAR_BASED_LIST_RESULT = {
    "data": {
        "orders": [
            {
                "id": ORDER_ID,
                "instrument_id": "5e6f7081-5555-4000-8000-000000000005",
                "symbol": "AAPL",
                "side": "buy",
                "type": "market",
                "state": "cancelled",
                "quantity": "0.000000",
                "cumulative_quantity": "0.000000",
                "price": "311.810000",
                "average_price": None,
                "fees": "0.000000",
                "dollar_based_amount": {"amount": "1.000000", "currency_code": "USD"},
                "time_in_force": "gfd",
                "market_hours": "regular_hours",
                "trigger": "immediate",
                "created_at": "2026-08-06T19:00:00.000000Z",
                "last_transaction_at": "2026-08-06T19:00:11.000000Z",
                "executions": [],
            }
        ]
    }
}

# Crypto puts its orders under another key and echoes both account numbers.
CRYPTO_LIST_RESULT = {
    "data": {
        "rhs_account_number": ACCOUNT,
        "crypto_account_number": "900000000001",
        "results": [
            {
                "id": ORDER_ID,
                "currency_code": "BTC",
                "currency_pair_id": "6f708192-9999-4000-8000-000000000009",
                "side": "buy",
                "type": "limit",
                "state": "new",
                "derived_state": "new",
                "state_group": "open",
                "quantity": "0.001",
                "cumulative_quantity": "0",
                "price": "50000.00",
                "fee": "0.35",
                "initiator_type": "agentic",
                "created_at": "2026-08-06T15:00:00.000000-04:00",
                "updated_at": "2026-08-06T15:00:00.000000-04:00",
            }
        ]
    },
    "guide": "state_group tells you whether it is still working.",
}


def test_status_query_names_the_account_each_class_wants():
    equity = RH.parse_request("place_equity_order", EQUITY_PLACE_ARGS)
    crypto = RH.parse_request(
        "place_crypto_order",
        {"rhs_account_number": ACCOUNT, "symbol": "BTC-USD", "side": "buy",
         "type": "market", "quantity": "0.001"},
    )
    option = RH.parse_request("place_option_order", OPTION_PLACE_ARGS)

    assert RH.status_query(equity).tool == "get_equity_orders"
    assert RH.status_query(equity).args == {"account_number": ACCOUNT}
    assert RH.status_query(crypto).args == {"rhs_account_number": ACCOUNT}
    # A combo is read by the same tool as a single leg.
    assert RH.status_query(option).tool == "get_option_orders"


def test_status_query_narrows_to_the_one_order_when_it_has_an_id():
    order = RH.parse_request("place_equity_order", EQUITY_PLACE_ARGS)

    query = RH.status_query(order, vendor_order_id=ORDER_ID)

    assert query.args == {"account_number": ACCOUNT, "order_id": ORDER_ID}


def test_status_query_needs_an_account_to_read_anything():
    order = RH.parse_request(
        "place_equity_order", {"symbol": "AAPL", "side": "buy", "type": "market"}
    )

    # The vendor rejects an account-less read, so there is no query to make and
    # the attempt stays open rather than failing a call every pass.
    assert order.account_ref == ""
    assert RH.status_query(order) is None
    assert RH.status_query(None) is None


def test_equity_status_read_settles_a_cancel():
    listed = RH.parse_status(
        StatusQuery("get_equity_orders", {"account_number": ACCOUNT}),
        EQUITY_LIST_RESULT,
    )

    assert len(listed) == 1
    assert listed[0].vendor_order_id == ORDER_ID
    assert listed[0].outcome.status is AttemptStatus.CANCELLED
    assert listed[0].outcome.raw_status == "cancelled"
    assert listed[0].outcome.fees.currency == "USD"
    assert listed[0].placed_at == datetime(2026, 8, 6, 19, 0, 0, tzinfo=UTC)
    # No ``updated_at`` on an equity order; the last transaction is the field.
    assert listed[0].outcome.updated_at == datetime(2026, 8, 6, 19, 7, tzinfo=UTC)


def test_option_status_read_counts_the_processed_contracts():
    listed = RH.parse_status(
        StatusQuery("get_option_orders", {"account_number": ACCOUNT}),
        OPTION_LIST_RESULT,
    )

    assert listed[0].outcome.status is AttemptStatus.FILLED
    assert listed[0].outcome.filled_qty == Decimal("1.00000")
    # An option order answers no fee, and inventing a zero would read as one.
    assert listed[0].outcome.fees is None
    # ``last_transaction_at`` is null while ``updated_at`` is live, the reverse
    # of the equity shape.
    assert listed[0].outcome.updated_at == datetime(2026, 8, 6, 19, 0, 2, tzinfo=UTC)


def test_crypto_status_read_finds_orders_under_its_own_key():
    listed = RH.parse_status(
        StatusQuery("get_crypto_orders", {"rhs_account_number": ACCOUNT}),
        CRYPTO_LIST_RESULT,
    )

    assert listed[0].vendor_order_id == ORDER_ID
    assert listed[0].outcome.status is AttemptStatus.SUBMITTED
    assert listed[0].outcome.fees.amount == Decimal("0.35")
    # ``currency_code`` is the asset, not the money, so it never becomes one.
    assert listed[0].outcome.fees.currency == "USD"


def test_an_empty_page_is_no_orders_not_an_error():
    listed = RH.parse_status(
        StatusQuery("get_crypto_orders", {"rhs_account_number": ACCOUNT}),
        {"data": {"rhs_account_number": ACCOUNT, "results": []}},
    )

    assert listed == []


def test_an_answer_with_no_order_list_is_not_an_empty_account():
    """A read holding no list, or an empty one beside an error, did not succeed."""
    query = StatusQuery("get_crypto_orders", {"rhs_account_number": ACCOUNT})

    for body in (
        {"data": {}},
        {"data": {"error": "account not found"}},
        {"data": {"results": [], "detail": "Not found."}},
    ):
        with pytest.raises(StatusReadError):
            RH.parse_status(query, body)


def test_a_listed_order_with_no_id_fails_the_read():
    """Dropped, it would read as an order the broker never took."""
    query = StatusQuery("get_equity_orders", {"account_number": ACCOUNT})
    order = EQUITY_LIST_RESULT["data"]["orders"][0]
    nameless = {k: v for k, v in order.items() if k != "id"}

    for entry in (nameless, None):
        with pytest.raises(StatusReadError, match="listed order with no id"):
            RH.parse_status(query, {"data": {"orders": [order, entry]}})


# ---------------------------------------------------------------- match keys


def test_a_listed_order_fingerprints_as_its_own_request():
    request = RH.parse_request("place_equity_order", EQUITY_PLACE_ARGS)
    listed = RH.parse_status(
        StatusQuery("get_equity_orders", {"account_number": ACCOUNT}),
        EQUITY_LIST_RESULT,
    )

    # The two sides spell the same order the same way, which is the whole of
    # what pairs an attempt the host never got an id for with a real order.
    assert RH.match_key(request) == listed[0].match_key
    assert RH.match_key(request) is not None


def test_an_option_fingerprints_on_the_contract_its_legs_name():
    request = RH.parse_request(
        "place_option_order",
        {
            "account_number": ACCOUNT,
            "quantity": "1",
            "type": "limit",
            "price": "1.05",
            "legs": [
                {"option_id": OPTION_ID, "side": "buy", "position_effect": "open"}
            ],
        },
    )
    listed = RH.parse_status(
        StatusQuery("get_option_orders", {"account_number": ACCOUNT}),
        OPTION_LIST_RESULT,
    )

    assert RH.match_key(request) == listed[0].match_key
    assert ("token", OPTION_ID) in RH.match_key(request).fields


def test_an_option_the_vendor_typed_pairs_with_the_request_that_left_it_out():
    """``place_option_order`` does not require ``type``; Robinhood defaults it.

    The listing then reports ``limit``, which the request never said. Comparing
    the two as one joined string settles a live option order as never placed,
    and a person reads that as leave to place it again.
    """
    request = RH.parse_request(
        "place_option_order",
        {
            "account_number": ACCOUNT,
            "quantity": "1",
            "price": "1.05",
            "legs": [
                {"option_id": OPTION_ID, "side": "buy", "position_effect": "open"}
            ],
        },
    )
    listed = RH.parse_status(
        StatusQuery("get_option_orders", {"account_number": ACCOUNT}),
        OPTION_LIST_RESULT,
    )

    assert dict(RH.match_key(request).fields).get("order_type") is None
    assert dict(listed[0].match_key.fields)["order_type"] == "limit"
    assert RH.match_key(request).matches(listed[0].match_key)


def test_a_dollar_based_order_leaves_out_what_the_vendor_computes():
    request = RH.parse_request(
        "place_equity_order",
        {
            "account_number": ACCOUNT,
            "symbol": "AAPL",
            "side": "buy",
            "type": "market",
            "dollar_amount": "1.00",
        },
    )
    listed = RH.parse_status(
        StatusQuery("get_equity_orders", {"account_number": ACCOUNT}),
        DOLLAR_BASED_LIST_RESULT,
    )

    # The broker divides the shares out of the amount and fills the price in
    # from the last trade, so neither can be part of a fingerprint -- but the
    # amount it was given can.
    assert RH.match_key(request) == listed[0].match_key
    assert dict(RH.match_key(request).fields) == {
        "asset_class": "equity",
        "token": "AAPL",
        "side": "buy",
        # One size field in the unit the caller chose, so a one-dollar order
        # and a one-share order are not each other.
        "size": "usd 1",
        "order_type": "market",
    }


def test_a_crypto_order_has_no_fingerprint():
    request = RH.parse_request(
        "place_crypto_order",
        {"rhs_account_number": ACCOUNT, "symbol": "BTC-USD", "side": "buy",
         "type": "limit", "quantity": "0.001", "limit_price": "50000.00"},
    )
    listed = RH.parse_status(
        StatusQuery("get_crypto_orders", {"rhs_account_number": ACCOUNT}),
        CRYPTO_LIST_RESULT,
    )

    # A request names the pair and the order names only the asset, so pairing
    # the two would mean inventing the half neither side says.
    assert RH.match_key(request) is None
    assert listed[0].match_key is None


def test_a_cancel_has_no_fingerprint_and_does_not_need_one():
    order = RH.parse_request(
        "cancel_equity_order", {"account_number": ACCOUNT, "order_id": ORDER_ID}
    )

    assert RH.match_key(order) is None


# ------------------------------------------------------- vendor error dialect


def test_an_api_error_under_an_error_status_is_still_the_vendor_speaking():
    outcome = RH.parse_result(
        "place_equity_order",
        'API error 400: {"detail":"You can only purchase 9 shares of AAPL."}',
        tool_status="error",
    )

    # The transport carried the answer, and the answer was a refusal: reading
    # this as a transport failure would say the order never reached the venue.
    assert outcome.status is AttemptStatus.REJECTED_BY_VENDOR
    assert outcome.failure.kind == "vendor"
    assert outcome.failure.code == "400"
    assert outcome.failure.message == "You can only purchase 9 shares of AAPL."


def test_an_unrecognised_error_keeps_its_transport_failure_and_the_words():
    outcome = RH.parse_result(
        "place_equity_order", "instrument not found for symbol ZZQQXX",
        tool_status="error",
    )

    # Nothing in the string proves the broker rather than the plumbing wrote
    # it, and calling it a rejection would claim knowledge of where it stopped.
    assert outcome.status is AttemptStatus.FAILED
    assert outcome.failure.kind == "transport"
    assert outcome.failure.message == "instrument not found for symbol ZZQQXX"


def test_the_vendor_error_hook_names_only_the_two_openings_it_knows():
    """What the shared classifier asks before it calls an error transport."""
    assert RH.vendor_error(None, 'API error 409: {"detail":"nope"}').code == "409"
    assert RH.vendor_error(None, "error: FORBIDDEN: no").kind == "vendor"
    assert RH.vendor_error(None, "connection reset by peer") is None
    assert RH.vendor_error({"data": {"id": ORDER_ID}}, "") is None


def test_a_refusal_outranks_the_vendor_dialect():
    outcome = RH.parse_result(
        "place_equity_order",
        "Refused: the connection to robinhood does not permit place_equity_order",
        tool_status="error",
    )

    assert outcome.status is AttemptStatus.REFUSED
    assert outcome.failure.kind == "policy"


# -------------------------------------------------------------------- fees


def test_a_crypto_fee_sits_beside_the_order_not_inside_it():
    outcome = RH.parse_result(
        "place_crypto_order",
        {
            "data": {
                "order": {"id": ORDER_ID, "state": "new", "currency_code": "BTC"},
                "estimated_fee": "0.35",
                "fee_context": "asset not eligible for fee tiers",
                "crypto_account_number": "900000000001",
            }
        },
        tool_status="success",
    )

    assert outcome.status is AttemptStatus.SUBMITTED
    assert outcome.fees.amount == Decimal("0.35")
    assert outcome.fees.currency == "USD"


def test_a_nested_fee_breakdown_is_read_from_its_total():
    outcome = RH.parse_result(
        "place_option_order",
        {
            "data": {
                "order": {
                    "id": ORDER_ID,
                    "state": "queued",
                    "fees": {
                        "occ_fee": {"fee_rate": "0.02", "fee": "0.02"},
                        "is_gold": True,
                        "total_fee": "0.04",
                    },
                }
            }
        },
        tool_status="success",
    )

    assert outcome.fees.amount == Decimal("0.04")


def test_the_chain_symbol_above_the_legs_names_the_contract():
    outcome = RH.parse_result(
        "place_option_order", OPTION_LIST_RESULT, tool_status="success"
    )
    answered = RH.parse_request(
        "place_option_order", OPTION_LIST_RESULT["data"]["orders"][0]
    )

    assert outcome.vendor_order_id == ORDER_ID
    # The legs carry the expiry, the strike and the right; the symbol they
    # belong to is on the order above them.
    assert answered.instrument == OptionRef(
        underlying="AAPL",
        expiration=date(2026, 1, 16),
        strike=Decimal("150.0000"),
        right="C",
        vendor_instrument_id=OPTION_ID,
    )
