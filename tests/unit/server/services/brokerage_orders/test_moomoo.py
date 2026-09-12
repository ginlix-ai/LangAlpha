"""The moomoo adapter against the request and response bodies actually observed.

Account and order ids here are invented; the bodies are not. The live ones were
captured against a real moomoo account whose ids are replaced with ``1234567``
for the same reason a fixture never carries a real one.
"""

import json
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

import pytest

from src.server.services.brokerage_orders import (
    AttemptStatus,
    BrokerOrder,
    ComboRef,
    EquityRef,
    FutureRef,
    ListingIncomplete,
    Money,
    OpaqueRef,
    OptionRef,
    OrderMode,
    OrderOutcome,
    StatusReadError,
    adapter_for,
)

MOOMOO = adapter_for("moomoo")

LIVE_PLACE_ARGS = {
    "qty": "1",
    "code": "US.AAPL",
    "side": "BUY",
    "price": "100",
    "acc_id": "1234567",
    "session": "RTH",
    "order_type": "LIMIT",
    "time_in_force": "DAY",
}

# Two live rejections, verbatim. ``errcode``/``errmsg`` is the whole of what a
# live failure names itself with, and ``need_order_confirm`` rides beside
# ``s``, in an envelope that has no ``d`` at all.
LIVE_DEVIATION_ERROR = (
    '{"s":"error","errcode":28,"errmsg":"Price deviated too much from the market",'
    '"need_order_confirm":false}'
)
LIVE_FUNDS_ERROR = (
    '{"s":"error","errcode":14,"errmsg":"Insufficient buying power. To proceed you '
    'will need to deposit necessary funds or cancel any open orders.",'
    '"need_order_confirm":false}'
)

# What a live replace, cancel or confirm answers with. Not JSON, and returned
# for an order that does not exist as readily as for one that does.
LIVE_NO_PAYLOAD = "no data"

# Both live order reads answer in this shape.
LIVE_EMPTY_ORDER_LIST = '{"s":"ok","d":{"orders":[],"page_flag":"30","completed":true}}'

# One live order exactly as ``account_orders_active`` and
# ``account_orders_history`` both list it. The vendor spells an order object in
# its own vocabulary, and none of it is the vocabulary of the request that made
# it: the state is ``order_status``, the fill is ``dealt_qty`` /
# ``dealt_avg_price``, the second timestamp is ``updated_time``, the remark
# comes back as ``remark``, and the instrument is one ``exchange.code`` string
# with no exchange field beside it.
LIVE_LISTED_ORDER = {
    "side": "BUY",
    "order_type": "LIMIT",
    "order_status": "FAILED",
    "order_id": "7654321",
    "code": "US.F",
    "stock_name": "Ford Motor",
    "security_type": "STOCK",
    "qty": "1",
    "price": "11.9",
    "currency": "USD",
    "create_time": 1788998621270534,
    "updated_time": 1788998621407419,
    "dealt_qty": "0",
    "dealt_avg_price": "0",
    "last_err_msg": "Insufficient buying power. To proceed you will need to deposit "
    "necessary funds or cancel any open orders.",
    "remark": "test-order",
    "time_in_force": "DAY",
    "session": "RTH",
}
LIVE_ORDER_LIST = {
    "s": "ok",
    "d": {"orders": [LIVE_LISTED_ORDER], "page_flag": "30", "completed": True},
}

PAPER_PLACE_ARGS = {
    "acc_id": "1234567",
    "market": 100,
    "symbol": "AAPL",
    "order_side": 1,
    "order_type": 1,
    "qty": "1",
    "price": "50",
    "text": "Test order",
    "order_trade_time_type": 1,
}

PAPER_PLACE_RESULT = '{"ret_code":0,"ret_msg":"success","data":{"order_id":"900101"}}'
PAPER_ERROR_RESULT = (
    '{"ret_code":-5,"ret_msg":"backend business error",'
    '"error":{"code":"backend_biz_error","message":"backend business error"}}'
)
PAPER_HISTORY_RESULT = {
    "ret_code": 0,
    "ret_msg": "success",
    "data": {
        "orders": [
            {
                "aux_price": None,
                "avg_fill_price": "0",
                "create_time": "1788578026000000",
                "cum_qty": "0",
                "market": 2,
                "order_id": "900102",
                "order_trade_time_type": 1,
                "order_type": 1,
                "price": "321",
                "qty": "10",
                "side": 1,
                "status": 2,
                "symbol": "AAPL",
                "text": "Test order",
                "time_in_force": 0,
                "update_time": "1788578026000000",
            }
        ]
    },
}


# A simulated Hong Kong order, as the account that placed it was sent it. The
# HK book leaves ``market``, ``order_trade_time_type`` and ``time_in_force``
# null where the US one fills them in, and the symbol is the five-digit code
# with no market prefix -- "HK.00700" and "700" are both refused.
PAPER_HK_PLACE_ARGS = {
    "acc_id": "1234567",
    "market": 1,
    "symbol": "02605",
    "order_side": 1,
    "order_type": 1,
    "qty": "24000",
    "price": "1.12",
    "text": "mmfill-partial",
    "order_trade_time_type": 1,
}


def _paper_hk_row(**overrides: Any) -> dict[str, Any]:
    row = {
        "aux_price": None,
        "avg_fill_price": "1.12",
        "create_time": "1789017779000000",
        "cum_qty": "24000",
        "market": None,
        "order_id": "1234567",
        "order_trade_time_type": None,
        "order_type": 1,
        "price": "1.12",
        "qty": "24000",
        "side": 1,
        "status": 4,
        "stock_name": "METALIGHT",
        "symbol": "02605",
        "text": "mmfill-partial",
        "time_in_force": None,
        "update_time": "1789017781000000",
    }
    row.update(overrides)
    return row


# Done: 24,000 of 24,000, against a book showing 1,200 at the ask.
PAPER_HK_FILLED_LIST = {
    "ret_code": 0,
    "ret_msg": "success",
    "data": {"orders": [_paper_hk_row()]},
    "pagination": {"has_more": False, "next_key": "0"},
}

# Cancelled holding part of a fill: the state moved on, the shares did not.
PAPER_HK_CANCELLED_AFTER_FILL_LIST = {
    "ret_code": 0,
    "ret_msg": "success",
    "data": {"orders": [_paper_hk_row(status=5, cum_qty="1200")]},
}

# No empty simulated list was captured. This is the observed list with its one
# order taken out, the shape a book with no orders has to keep reading as.
PAPER_EMPTY_ORDER_LIST = {
    "ret_code": 0,
    "ret_msg": "success",
    "data": {"orders": []},
    "pagination": {"has_more": False, "next_key": "0"},
}


def test_adapter_for_knows_moomoo_and_not_an_unmodeled_vendor():
    assert MOOMOO is not None
    # Webull publishes nothing that places an order, so it never gets one.
    assert adapter_for("webull") is None


def test_live_place_request():
    order = MOOMOO.parse_request("trading_order_place", LIVE_PLACE_ARGS)

    assert order.vendor == "moomoo"
    assert order.account_ref == "1234567"
    assert order.mode is OrderMode.LIVE
    assert order.asset_class == "equity"
    assert order.instrument == EquityRef(symbol="AAPL", venue="US")
    assert order.side == "buy"
    assert order.qty == Decimal("1")
    assert order.order_type == "limit"
    assert order.limit_price == Decimal("100")
    assert order.stop_price is None
    assert order.time_in_force == "day"
    assert order.session == "rth"
    assert order.extras == {}
    assert order.raw == LIVE_PLACE_ARGS


def test_live_place_extended_session_stop_and_extras():
    order = MOOMOO.parse_request(
        "trading_order_place",
        {
            "acc_id": "1234567",
            "code": "SEHK.00700",
            "side": "SELL_SHORT",
            "qty": "100",
            "price": "500",
            "aux_price": "480",
            "order_type": "STOP_LIMIT",
            "time_in_force": "GTC",
            "session": "RTH+Pre/Post-Mkt",
            "lot_type": "ODD",
            "remark": "trim",
            "some_new_field": "kept",
        },
    )

    assert order.instrument == EquityRef(symbol="00700", venue="SEHK")
    assert order.side == "sell_short"
    assert order.order_type == "stop_limit"
    assert order.stop_price == Decimal("480")
    assert order.time_in_force == "gtc"
    assert order.session == "rth_plus_ext"
    assert order.note == "trim"
    assert order.extras == {"lot_type": "ODD", "some_new_field": "kept"}


def test_mleg_request_carries_its_legs():
    legs = [
        {"code": "CBOE.AAPL260116C00150000", "side": "BUY", "ratio": 1},
        {"code": "CBOE.AAPL260116C00160000", "side": "SELL", "ratio": 1},
    ]
    order = MOOMOO.parse_request(
        "trading_order_place",
        {
            "acc_id": "1234567",
            "side": "BUY",
            "qty": "1",
            "order_type": "LIMIT",
            "price": "2.5",
            "time_in_force": "DAY",
            "order_class": "MLEG",
            "multi_leg_info": {"legs": legs},
        },
    )

    assert order.asset_class == "option_combo"
    assert order.instrument == ComboRef(legs=(legs[0], legs[1]))
    assert order.side == "buy"
    assert order.limit_price == Decimal("2.5")


def test_futures_request():
    order = MOOMOO.parse_request(
        "trading_order_place",
        {
            "acc_id": "1234567",
            "code": "CME.ESmain",
            "side": "SELL",
            "qty": "2",
            "order_type": "MARKET",
            "time_in_force": "GTC",
        },
    )

    assert order.asset_class == "future"
    assert order.instrument == FutureRef(symbol="ESmain", venue="CME")
    assert order.order_type == "market"
    assert order.side == "sell"


def test_option_venue_parses_occ_and_falls_back_to_opaque():
    parsed = MOOMOO.parse_request(
        "trading_order_place",
        {"acc_id": "1234567", "code": "CBOE.AAPL260116C00150000", "side": "BUY", "qty": "1"},
    )
    opaque = MOOMOO.parse_request(
        "trading_order_place",
        {"acc_id": "1234567", "code": "CBOE.SOMETHING-ELSE", "side": "BUY", "qty": "1"},
    )

    assert parsed.asset_class == "option"
    assert parsed.instrument == OptionRef(
        underlying="AAPL",
        expiration=datetime(2026, 1, 16, tzinfo=UTC).date(),
        strike=Decimal("150"),
        right="C",
        vendor_instrument_id="CBOE.AAPL260116C00150000",
    )
    assert opaque.asset_class == "option"
    assert opaque.instrument == OpaqueRef(raw_code="CBOE.SOMETHING-ELSE")


def test_live_cancel_and_confirm_requests():
    cancel = MOOMOO.parse_request(
        "trading_order_cancel",
        {"acc_id": "1234567", "exchange": "US", "order_id": "900101"},
    )
    confirm = MOOMOO.parse_request(
        "trading_order_confirm", {"acc_id": "1234567", "confirm_id": "cfm-9"}
    )

    assert cancel.mode is OrderMode.LIVE
    assert cancel.instrument is None
    assert cancel.extras == {"order_id": "900101", "exchange": "US"}
    assert confirm.extras == {"confirm_id": "cfm-9"}
    # What each call acts on, which is the one thing every amend has: the
    # cancel names an order, the confirm names the confirmation it was handed.
    assert cancel.target_ref == "900101"
    assert confirm.target_ref == "cfm-9"
    assert MOOMOO.parse_request("trading_order_confirm", {"acc_id": "1234567"}) is None


def test_a_confirm_with_only_an_order_falls_back_to_it():
    confirm = MOOMOO.parse_request(
        "trading_order_confirm", {"acc_id": "1234567", "order_id": "900101"}
    )

    assert confirm.target_ref == "900101"


def test_the_target_survives_the_json_round_trip():
    """The summary the approval card reads is the stored order, not the call."""
    cancel = MOOMOO.parse_request(
        "sim_trade_cancel_order",
        {"acc_id": "1234567", "market": 100, "order_id": "900101"},
    )

    assert cancel.to_json()["target_ref"] == "900101"
    assert BrokerOrder.from_json(cancel.to_json()).target_ref == "900101"


def test_paper_place_request():
    order = MOOMOO.parse_request("sim_trade_input_order", PAPER_PLACE_ARGS)

    assert order.mode is OrderMode.PAPER
    assert order.asset_class == "equity"
    assert order.instrument == EquityRef(symbol="AAPL", venue="US")
    assert order.side == "buy"
    assert order.order_type == "limit"
    assert order.qty == Decimal("1")
    assert order.limit_price == Decimal("50")
    assert order.session == "rth"
    assert order.note == "Test order"
    # The integer the cancel API wants back, kept exactly as it was sent.
    assert order.extras == {"market": 100}


def test_paper_futures_market_is_a_future():
    order = MOOMOO.parse_request(
        "sim_trade_input_order",
        {
            "acc_id": "1234567",
            "market": 11,
            "symbol": "ESmain",
            "order_side": 2,
            "order_type": 3,
            "qty": "1",
        },
    )

    assert order.asset_class == "future"
    assert order.instrument == FutureRef(symbol="ESmain", venue="US")
    assert order.side == "sell"
    assert order.order_type == "market"


def test_paper_place_result():
    request = MOOMOO.parse_request("sim_trade_input_order", PAPER_PLACE_ARGS)
    outcome = MOOMOO.parse_result(
        "sim_trade_input_order",
        PAPER_PLACE_RESULT,
        tool_status="success",
        request=request,
    )

    assert outcome.status is AttemptStatus.SUBMITTED
    assert outcome.vendor_order_id == "900101"
    assert outcome.failure is None
    assert outcome.route == {"market": "100"}


def test_paper_cancel_result():
    request = MOOMOO.parse_request(
        "sim_trade_cancel_order",
        {"acc_id": "1234567", "market": 100, "order_id": "900101"},
    )
    outcome = MOOMOO.parse_result(
        "sim_trade_cancel_order",
        PAPER_PLACE_RESULT,
        tool_status="success",
        request=request,
    )

    assert outcome.status is AttemptStatus.CANCELLED
    assert outcome.vendor_order_id == "900101"
    assert outcome.route == {"market": "100"}
    assert request.target_ref == "900101"


def test_paper_business_error_is_a_vendor_rejection():
    outcome = MOOMOO.parse_result(
        "sim_trade_input_order", PAPER_ERROR_RESULT, tool_status="success"
    )

    assert outcome.status is AttemptStatus.REJECTED_BY_VENDOR
    assert outcome.failure.kind == "vendor"
    assert outcome.failure.code == "-5"
    assert outcome.failure.message == "backend business error"
    assert outcome.raw["error"]["code"] == "backend_biz_error"


def test_paper_answer_naming_no_code_is_not_a_rejection():
    """A body that states no ``ret_code`` has not stated a rejection.

    Reading absence as one settled the attempt terminal and threw the order id
    away with it, so reconciliation never went and asked.
    """
    stated = MOOMOO.parse_result(
        "sim_trade_input_order",
        '{"data":{"order_id":"900301","status":2}}',
        tool_status="success",
    )
    assert stated.status is AttemptStatus.WORKING
    assert stated.vendor_order_id == "900301"
    assert stated.failure is None

    silent = MOOMOO.parse_result(
        "sim_trade_input_order",
        '{"data":{"order_id":"900302"}}',
        tool_status="success",
    )
    assert silent.status is AttemptStatus.UNKNOWN
    assert silent.vendor_order_id == "900302"

    # A code that is there and not zero still settles a rejection.
    assert (
        MOOMOO.parse_result(
            "sim_trade_input_order", PAPER_ERROR_RESULT, tool_status="success"
        ).status
        is AttemptStatus.REJECTED_BY_VENDOR
    )


def test_live_answer_naming_no_state_is_not_a_rejection():
    """``s`` absent is not ``s`` saying no, and the order id has to survive it."""
    request = MOOMOO.parse_request("trading_order_place", LIVE_PLACE_ARGS)
    outcome = MOOMOO.parse_result(
        "trading_order_place",
        {"d": {"order_id": "77", "order_status": "SUBMITTED"}},
        tool_status="success",
        request=request,
    )

    assert outcome.status is AttemptStatus.UNKNOWN
    assert outcome.vendor_order_id == "77"
    assert outcome.failure is None


def test_paper_history_order_is_working():
    request = MOOMOO.parse_request(
        "sim_trade_history_order_list", {"acc_id": "1234567", "market": 100}
    )
    outcome = MOOMOO.parse_result(
        "sim_trade_history_order_list",
        PAPER_HISTORY_RESULT,
        tool_status="success",
        request=request,
    )

    assert request is None
    assert outcome.status is AttemptStatus.WORKING
    assert outcome.raw_status == "2"
    assert outcome.vendor_order_id == "900102"
    assert outcome.filled_qty == Decimal("0")
    assert outcome.created_at == datetime(2026, 9, 5, 3, 13, 46, tzinfo=UTC)
    # No request to read the sent market from, so the echoed one is all there is.
    assert outcome.route == {"market": "2"}


def test_paper_status_query_lists_the_simulated_book():
    order = MOOMOO.parse_request("sim_trade_input_order", PAPER_HK_PLACE_ARGS)
    query = MOOMOO.status_query(order)

    assert query.tool == "sim_trade_history_order_list"
    assert query.args["acc_id"] == "1234567"
    assert query.args["market"] == 1


def test_paper_listed_fill_is_read_from_cum_qty_and_avg_fill_price():
    """Code 4 is done, and the fill rides the two fields a resting order zeroes."""
    query = MOOMOO.status_query(
        MOOMOO.parse_request("sim_trade_input_order", PAPER_HK_PLACE_ARGS)
    )
    (listed,) = MOOMOO.parse_status(query, PAPER_HK_FILLED_LIST)

    outcome = listed.outcome
    assert outcome.status is AttemptStatus.FILLED
    assert outcome.raw_status == "4"
    assert listed.vendor_order_id == "1234567"
    assert outcome.filled_qty == Decimal("24000")
    assert outcome.avg_fill_price == Decimal("1.12")
    assert outcome.route == {"market": "1"}
    # The simulated row carries no fee field at all, so nothing may invent one.
    assert outcome.fees is None


def test_a_paper_cancel_after_a_partial_keeps_the_shares_it_got():
    """Cancelled is the lifecycle; the fill it already had is not thrown away.

    The simulator would not produce this row -- it fills in full or not at all
    -- but the two halves are each observed, and nothing in the parser makes
    the fill conditional on the state, which is the property worth locking.
    """
    query = MOOMOO.status_query(
        MOOMOO.parse_request("sim_trade_input_order", PAPER_HK_PLACE_ARGS)
    )
    (listed,) = MOOMOO.parse_status(query, PAPER_HK_CANCELLED_AFTER_FILL_LIST)

    outcome = listed.outcome
    assert outcome.status is AttemptStatus.CANCELLED
    assert outcome.raw_status == "5"
    assert outcome.filled_qty == Decimal("1200")
    assert outcome.avg_fill_price == Decimal("1.12")


def test_a_paper_place_answer_carries_only_an_order_id():
    """The simulated place says nothing about the state, so it reads submitted."""
    request = MOOMOO.parse_request("sim_trade_input_order", PAPER_HK_PLACE_ARGS)
    outcome = MOOMOO.parse_result(
        "sim_trade_input_order",
        '{"ret_code":0,"ret_msg":"success","data":{"order_id":"1234567"}}',
        tool_status="success",
        request=request,
    )

    assert outcome.status is AttemptStatus.SUBMITTED
    assert outcome.filled_qty is None
    assert outcome.route == {"market": "1"}


def test_unknown_paper_status_keeps_the_vendor_integer():
    body = {"ret_code": 0, "ret_msg": "success", "data": {"order_id": "1", "status": 77}}
    outcome = MOOMOO.parse_result("sim_trade_input_order", body, tool_status="success")

    assert outcome.status is AttemptStatus.UNKNOWN
    assert outcome.raw_status == "77"


def test_live_place_needing_confirmation_at_the_envelope_top_level():
    """``need_order_confirm`` sits beside ``s``, which is where it was seen."""
    request = MOOMOO.parse_request("trading_order_place", LIVE_PLACE_ARGS)
    outcome = MOOMOO.parse_result(
        "trading_order_place",
        {
            "s": "ok",
            "need_order_confirm": True,
            "confirm_id": "cfm-9",
            "d": {"order_id": "7654321"},
        },
        tool_status="success",
        request=request,
    )

    assert outcome.status is AttemptStatus.PENDING_CONFIRM
    assert outcome.confirm_token == "cfm-9"
    assert outcome.vendor_order_id == "7654321"
    assert outcome.route == {"exchange": "US"}


def test_live_place_needing_confirmation_inside_the_payload():
    """The other placement still reads, since only one of the two was seen."""
    request = MOOMOO.parse_request("trading_order_place", LIVE_PLACE_ARGS)
    outcome = MOOMOO.parse_result(
        "trading_order_place",
        {"s": "ok", "d": {"need_order_confirm": True, "confirm_id": "cfm-9"}},
        tool_status="success",
        request=request,
    )

    assert outcome.status is AttemptStatus.PENDING_CONFIRM
    assert outcome.confirm_token == "cfm-9"


def _live_place_answer(**row: Any) -> OrderOutcome:
    """A live place answered ``ok``, echoing its order in the listed vocabulary.

    No live order has been accepted yet, so this shape is inferred, not captured.
    """
    return MOOMOO.parse_result(
        "trading_order_place",
        {"s": "ok", "d": {"order_id": "900101", **row}},
        tool_status="success",
        request=MOOMOO.parse_request("trading_order_place", LIVE_PLACE_ARGS),
    )


def test_a_live_place_answer_is_taken_at_its_word_past_submission():
    filled = _live_place_answer(order_status="FILLED_ALL", dealt_qty="1")
    working = _live_place_answer(order_status="SUBMITTED")
    rejected = _live_place_answer(
        order_status="SUBMIT_FAILED", last_err_msg="Market is closed"
    )

    assert filled.status is AttemptStatus.FILLED
    assert filled.filled_qty == Decimal("1")
    assert working.status is AttemptStatus.WORKING
    assert rejected.status is AttemptStatus.REJECTED_BY_VENDOR
    assert rejected.failure.message == "Market is closed"


def test_a_live_place_answer_that_vouches_for_no_state_reads_as_submitted():
    """An answer came back, so it is never ``submitting`` and never a guessed end."""
    for word in ("SUBMITTING", "WAITING_SUBMIT", "TIMEOUT"):
        outcome = _live_place_answer(order_status=word)
        assert outcome.status is AttemptStatus.SUBMITTED, word
    assert _live_place_answer().status is AttemptStatus.SUBMITTED


def test_a_live_cancel_answered_ok_is_cancelled_whatever_row_it_echoes():
    request = MOOMOO.parse_request(
        "trading_order_cancel",
        {"acc_id": "1234567", "exchange": "US", "order_id": "900101"},
    )
    outcome = MOOMOO.parse_result(
        "trading_order_cancel",
        {"s": "ok", "d": {"order_id": "900101", "order_status": "SUBMITTED"}},
        tool_status="success",
        request=request,
    )

    assert outcome.status is AttemptStatus.CANCELLED
    assert outcome.vendor_order_id == "900101"


def test_live_rejection_names_the_vendor_errcode_and_errmsg():
    request = MOOMOO.parse_request("trading_order_place", LIVE_PLACE_ARGS)
    deviated = MOOMOO.parse_result(
        "trading_order_place",
        LIVE_DEVIATION_ERROR,
        tool_status="success",
        request=request,
    )
    unfunded = MOOMOO.parse_result(
        "trading_order_place", LIVE_FUNDS_ERROR, tool_status="success", request=request
    )

    assert deviated.status is AttemptStatus.REJECTED_BY_VENDOR
    assert deviated.failure.kind == "vendor"
    assert deviated.failure.code == "28"
    assert deviated.failure.message == "Price deviated too much from the market"
    assert deviated.route == {"exchange": "US"}
    assert unfunded.failure.code == "14"
    assert unfunded.failure.message.startswith("Insufficient buying power")


def test_live_rejection_of_an_amend_names_the_order_it_was_about():
    request = MOOMOO.parse_request(
        "trading_order_cancel",
        {"acc_id": "1234567", "exchange": "US", "order_id": "7654321"},
    )
    outcome = MOOMOO.parse_result(
        "trading_order_cancel",
        LIVE_DEVIATION_ERROR,
        tool_status="success",
        request=request,
    )

    assert outcome.status is AttemptStatus.REJECTED_BY_VENDOR
    assert outcome.vendor_order_id == "7654321"


def test_live_amend_answered_with_no_payload_stays_open_and_keeps_its_route():
    """``no data`` settles nothing, so the row has to stay readable.

    Without the route there is no market to ask, and without the order id there
    is nothing to look up: the row would sit open forever instead of being
    settled by the next reconciliation read.
    """
    for tool in ("trading_order_cancel", "trading_order_replace"):
        request = MOOMOO.parse_request(
            tool,
            {"acc_id": "1234567", "exchange": "US", "order_id": "7654321", "qty": "1"},
        )
        outcome = MOOMOO.parse_result(
            tool, LIVE_NO_PAYLOAD, tool_status="success", request=request
        )

        assert outcome.status is AttemptStatus.UNKNOWN, tool
        assert outcome.raw_status == "no data", tool
        assert outcome.vendor_order_id == "7654321", tool
        assert outcome.route == {"exchange": "US"}, tool


def test_live_confirm_answered_with_no_payload_claims_no_order():
    """A confirm names a confirmation, not an order, so it invents neither."""
    request = MOOMOO.parse_request(
        "trading_order_confirm",
        {"acc_id": "1234567", "confirm_id": "7654321", "exchange": "US"},
    )
    outcome = MOOMOO.parse_result(
        "trading_order_confirm", LIVE_NO_PAYLOAD, tool_status="success", request=request
    )

    assert outcome.status is AttemptStatus.UNKNOWN
    assert outcome.vendor_order_id is None
    assert outcome.route == {"exchange": "US"}


def test_live_status_query_asks_the_history_list():
    query = MOOMOO.status_query(
        MOOMOO.parse_request("trading_order_place", LIVE_PLACE_ARGS)
    )

    assert query.tool == "account_orders_history"
    assert query.args == {"acc_id": "1234567", "trd_market": "US", "page_size": 100}
    assert MOOMOO.parse_status(query, LIVE_EMPTY_ORDER_LIST) == []


def test_live_listed_order_reads_the_vendors_own_vocabulary():
    query = MOOMOO.status_query(
        MOOMOO.parse_request("trading_order_place", LIVE_PLACE_ARGS)
    )
    (listed,) = MOOMOO.parse_status(query, LIVE_ORDER_LIST)

    assert listed.vendor_order_id == "7654321"
    assert listed.outcome.status is AttemptStatus.REJECTED_BY_VENDOR
    assert listed.outcome.raw_status == "FAILED"
    assert listed.outcome.filled_qty == Decimal("0")
    assert listed.outcome.avg_fill_price == Decimal("0")
    # The exchange is only ever the head of ``code``; there is no field for it.
    assert listed.outcome.route == {"exchange": "US"}
    assert listed.outcome.failure.kind == "vendor"
    assert listed.outcome.failure.code == "FAILED"
    assert listed.outcome.failure.message.startswith("Insufficient buying power")
    assert listed.placed_at == datetime.fromtimestamp(1788998621.270534, tz=UTC)
    assert listed.outcome.updated_at == datetime.fromtimestamp(
        1788998621.407419, tz=UTC
    )


def test_live_fingerprint_pairs_a_sent_order_with_the_listed_one():
    """A live place with no id back is still recognizable in the list.

    ``remark`` is echoed verbatim, and it plus the code, side, kind, size and
    price is the whole of what the two sides share.
    """
    sent = MOOMOO.parse_request(
        "trading_order_place",
        {
            "acc_id": "1234567",
            "code": "US.F",
            "side": "BUY",
            "order_type": "LIMIT",
            "qty": "1",
            # ``11.90`` and ``11.9`` are the same price, spelled two ways.
            "price": "11.90",
            "time_in_force": "DAY",
            "session": "RTH",
            "remark": "test-order",
        },
    )
    query = MOOMOO.status_query(sent)
    (listed,) = MOOMOO.parse_status(query, LIVE_ORDER_LIST)

    assert MOOMOO.match_key(sent) == listed.match_key
    assert MOOMOO.match_key(sent) is not None


def test_a_live_limit_is_not_the_listed_stop_limit_beside_it():
    """Same account, code, side, size, price and remark, different instruction.

    The order that never came back was a limit; the one in the list is a stop
    limit someone else placed. Settling one as the other would write a live
    order's id into this row and leave a cancel from here acting on it.
    """
    args = {
        "acc_id": "1234567",
        "code": "US.F",
        "side": "BUY",
        "qty": "1",
        "price": "11.90",
        "time_in_force": "DAY",
        "session": "RTH",
        "remark": "test-order",
    }
    sent = MOOMOO.parse_request(
        "trading_order_place", {**args, "order_type": "STOP_LIMIT", "aux_price": "11.5"}
    )
    query = MOOMOO.status_query(sent)
    (listed,) = MOOMOO.parse_status(query, LIVE_ORDER_LIST)

    assert MOOMOO.match_key(sent) is not None
    assert not MOOMOO.match_key(sent).matches(listed.match_key)
    # The same call as a limit is the listed order, which is what makes the
    # difference above the kind and nothing else.
    limit = MOOMOO.parse_request("trading_order_place", {**args, "order_type": "LIMIT"})
    assert MOOMOO.match_key(limit) == listed.match_key


def test_a_paper_order_still_pairs_with_itself_on_the_hong_kong_book():
    """The HK book nulls fields the request sets, so the kind has to survive it.

    ``order_trade_time_type`` and ``time_in_force`` come back null there while
    the request carried both, which is why neither is in the fingerprint.
    """
    sent = MOOMOO.parse_request("sim_trade_input_order", PAPER_HK_PLACE_ARGS)
    query = MOOMOO.status_query(sent)
    (listed,) = MOOMOO.parse_status(query, PAPER_HK_FILLED_LIST)

    assert MOOMOO.match_key(sent) == listed.match_key
    assert MOOMOO.match_key(sent) is not None


def test_a_live_read_the_vendor_refused_is_not_an_empty_list():
    query = MOOMOO.status_query(
        MOOMOO.parse_request("trading_order_place", LIVE_PLACE_ARGS)
    )
    with pytest.raises(StatusReadError, match="Insufficient buying power"):
        MOOMOO.parse_status(query, LIVE_FUNDS_ERROR)


def test_an_ok_read_with_no_order_list_is_not_an_empty_list():
    """Only a list the vendor sent back empty says the account has no orders."""
    live = MOOMOO.status_query(
        MOOMOO.parse_request("trading_order_place", LIVE_PLACE_ARGS)
    )
    paper = MOOMOO.status_query(
        MOOMOO.parse_request("sim_trade_input_order", PAPER_HK_PLACE_ARGS)
    )

    for query, body in (
        (live, {"s": "ok"}),
        (live, {"s": "ok", "d": {}}),
        (paper, {"ret_code": 0}),
    ):
        with pytest.raises(StatusReadError, match="no order list"):
            MOOMOO.parse_status(query, body)
    assert MOOMOO.parse_status(live, LIVE_EMPTY_ORDER_LIST) == []
    assert MOOMOO.parse_status(paper, PAPER_EMPTY_ORDER_LIST) == []


def test_a_paper_listed_order_with_no_id_fails_the_read():
    """Dropped, it would read as an order that never reached the book."""
    query = MOOMOO.status_query(
        MOOMOO.parse_request("sim_trade_input_order", PAPER_HK_PLACE_ARGS)
    )
    nameless = {k: v for k, v in _paper_hk_row().items() if k != "order_id"}

    for entry in (nameless, None):
        body = {"ret_code": 0, "data": {"orders": [_paper_hk_row(), entry]}}
        with pytest.raises(StatusReadError, match="listed order with no id"):
            MOOMOO.parse_status(query, body)


def test_a_live_listed_order_with_no_id_fails_the_read():
    query = MOOMOO.status_query(
        MOOMOO.parse_request("trading_order_place", LIVE_PLACE_ARGS)
    )
    nameless = {k: v for k, v in LIVE_LISTED_ORDER.items() if k != "order_id"}

    for entry in (nameless, None):
        body = {"s": "ok", "d": {"orders": [LIVE_LISTED_ORDER, entry]}}
        with pytest.raises(StatusReadError, match="listed order with no id"):
            MOOMOO.parse_status(query, body)


def test_paper_history_turns_the_page_by_next_key():
    query = MOOMOO.status_query(
        MOOMOO.parse_request("sim_trade_input_order", PAPER_HK_PLACE_ARGS)
    )
    more = {
        **PAPER_HK_FILLED_LIST,
        "pagination": {"has_more": True, "next_key": "900000"},
    }

    nxt = MOOMOO.next_page(query, more)
    assert nxt is not None
    assert nxt.tool == query.tool
    assert nxt.args == {**query.args, "next_key": "900000"}
    # The observed last page: has_more false, a key still present.
    assert MOOMOO.next_page(query, PAPER_HK_FILLED_LIST) is None
    # More orders and no way to ask for them is not the end of the list. It
    # cannot be read on either, so the listing is incomplete and absence from
    # it stops being evidence.
    with pytest.raises(ListingIncomplete):
        MOOMOO.next_page(nxt, more)
    with pytest.raises(ListingIncomplete):
        MOOMOO.next_page(
            query, {**PAPER_HK_FILLED_LIST, "pagination": {"has_more": True}}
        )
    # A real paper account answers an empty window with pagination all the
    # same, so an answer carrying orders and none at all is malformed rather
    # than a single complete page.
    with pytest.raises(ListingIncomplete):
        MOOMOO.next_page(query, PAPER_HK_CANCELLED_AFTER_FILL_LIST)


def test_live_history_turns_the_page_by_page_flag():
    query = MOOMOO.status_query(
        MOOMOO.parse_request("trading_order_place", LIVE_PLACE_ARGS)
    )
    more = {"s": "ok", "d": {"orders": [], "page_flag": "30", "completed": False}}

    nxt = MOOMOO.next_page(query, more)
    assert nxt is not None
    assert nxt.args == {**query.args, "page_flag": "30"}
    # The observed last pages: completed true, with and without a flag.
    assert MOOMOO.next_page(query, LIVE_EMPTY_ORDER_LIST) is None
    assert MOOMOO.next_page(query, LIVE_ORDER_LIST) is None
    # Not completed, and the flag just sent echoed back or missing entirely.
    with pytest.raises(ListingIncomplete):
        MOOMOO.next_page(nxt, more)
    with pytest.raises(ListingIncomplete):
        MOOMOO.next_page(query, {"s": "ok", "d": {"orders": [], "completed": False}})
    # A page stating no completion at all, or stating it as something other than
    # a boolean, has not said it is the last one.
    with pytest.raises(ListingIncomplete):
        MOOMOO.next_page(query, {"s": "ok", "d": {"orders": [], "page_flag": "31"}})
    with pytest.raises(ListingIncomplete):
        MOOMOO.next_page(
            query,
            {"s": "ok", "d": {"orders": [], "page_flag": "31", "completed": "true"}},
        )


def test_live_status_query_maps_the_exchange_onto_a_trading_market():
    def market(code):
        order = MOOMOO.parse_request(
            "trading_order_place", {**LIVE_PLACE_ARGS, "code": code}
        )
        query = MOOMOO.status_query(order)
        return None if query is None else query.args["trd_market"]

    assert market("SEHK.00700") == "HK"
    assert market("SGX.D05") == "SG"
    assert market("SSE.600519") == "HKCC"
    assert market("CME.ESmain") == "FUTURES"
    # No trd_market names an options book, so an options venue asks nothing
    # rather than reading the wrong one and calling the order missing.
    assert market("CBOE.AAPL260116C00150000") is None


def test_refused_body_and_tool_error():
    refused = MOOMOO.parse_result(
        "sim_trade_input_order",
        "Refused: the connection to moomoo does not permit sim_trade_input_order",
        tool_status="error",
    )
    failed = MOOMOO.parse_result(
        "trading_order_place", "upstream timeout", tool_status="error"
    )

    assert refused.status is AttemptStatus.REFUSED
    assert refused.failure.kind == "policy"
    assert failed.status is AttemptStatus.FAILED
    assert failed.failure.kind == "transport"


def test_content_blocks_are_unwrapped():
    body = json.dumps([{"type": "text", "text": PAPER_PLACE_RESULT, "id": "lc_1"}])
    outcome = MOOMOO.parse_result("sim_trade_input_order", body, tool_status="success")

    assert outcome.status is AttemptStatus.SUBMITTED
    assert outcome.vendor_order_id == "900101"


def test_broker_order_json_round_trip():
    order = BrokerOrder(
        vendor="moomoo",
        account_ref="1234567",
        mode=OrderMode.PAPER,
        asset_class="option",
        instrument=OptionRef(
            underlying="AAPL",
            expiration=datetime(2026, 1, 16, tzinfo=UTC).date(),
            strike=Decimal("150.5"),
            right="C",
            multiplier=100,
            vendor_instrument_id="CBOE.AAPL260116C00150500",
        ),
        side="buy",
        qty=Decimal("1.5"),
        notional=Money(amount=Decimal("250.25"), currency="USD"),
        order_type="stop_limit",
        limit_price=Decimal("100.1234"),
        stop_price=Decimal("99"),
        time_in_force="gtc",
        session="rth_plus_ext",
        currency="USD",
        note="trim",
        extras={"market": 100},
        raw=dict(PAPER_PLACE_ARGS),
    )

    encoded = order.to_json()
    assert json.loads(json.dumps(encoded)) == encoded
    assert encoded["qty"] == "1.5"
    assert encoded["instrument"]["expiration"] == "2026-01-16"
    assert BrokerOrder.from_json(encoded) == order


def test_order_outcome_json_round_trip():
    outcome = OrderOutcome(
        status=AttemptStatus.PARTIALLY_FILLED,
        raw_status="2",
        vendor_order_id="900102",
        route={"market": "100"},
        filled_qty=Decimal("3"),
        avg_fill_price=Decimal("321.25"),
        fees=Money(amount=Decimal("1.05"), currency="USD"),
        confirm_token="cfm-9",
        created_at=datetime(2026, 9, 5, 3, 13, 46, tzinfo=UTC),
        updated_at=datetime(2026, 9, 5, 3, 13, 46, tzinfo=UTC),
        raw={"ret_code": 0},
    )

    encoded = outcome.to_json()
    assert json.loads(json.dumps(encoded)) == encoded
    assert OrderOutcome.from_json(encoded) == outcome

    rejected = OrderOutcome.from_json(
        MOOMOO.parse_result(
            "sim_trade_input_order", PAPER_ERROR_RESULT, tool_status="success"
        ).to_json()
    )
    assert rejected.failure.code == "-5"
