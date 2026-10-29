"""The Webull adapter against Webull's published instruction schemas.

The requests are shaped exactly as the discovered input schemas state them:
an ``account_id`` beside a ``new_orders`` list of one LIMIT order, options
carrying their contracts in ``legs``. No answer shape is published, so the
bodies here are the ones the tool descriptions promise and nothing more: a
create answering an instruction id and a ``message`` holding the confirmation
link, and two lists whose rows carry an id, a status and the order fields.
Ids and links are invented.

The arms worth locking are the ones a guess would get wrong. An instruction
never reads as working or filled, because it is not an order until a person
confirms it in Webull's own client. A call carrying more than one order is
recorded without describing either. And a listing reads the pending list and
then the processed one, so an instruction missing from one is not taken as
gone while the other still names it.
"""

from decimal import Decimal

import pytest

from src.server.services.brokerage_capabilities import (
    denied_tools,
    group_keys_for,
    group_of_tool,
    is_always_denied,
)
from src.server.services.brokerage_orders import (
    AttemptStatus,
    ComboRef,
    CryptoRef,
    EquityRef,
    FutureRef,
    ListingIncomplete,
    Money,
    OptionRef,
    OrderMode,
    StatusQuery,
    StatusReadError,
    adapter_for,
)
from src.server.services.brokerage_orders import webull

WEBULL = adapter_for("webull")

ACCOUNT = "ACCT0001"
INSTRUCTION_ID = "9001"
LINK = "https://www.example.com/instruction/confirm?id=9001"


def stock_order(**overrides) -> dict:
    return {
        "combo_type": "NORMAL",
        "symbol": "AAPL",
        "instrument_type": "EQUITY",
        "market": "US",
        "side": "BUY",
        "order_type": "LIMIT",
        "limit_price": "180.00",
        "time_in_force": "DAY",
        "entrust_type": "QTY",
        "quantity": "10",
        "support_trading_session": "CORE",
        **overrides,
    }


def leg(**overrides) -> dict:
    return {
        "side": "BUY",
        "quantity": "1",
        "symbol": "AAPL",
        "strike_price": "330.00",
        "option_expire_date": "2027-01-15",
        "instrument_type": "OPTION",
        "option_type": "CALL",
        "market": "US",
        **overrides,
    }


def option_order(legs: list[dict], strategy: str = "SINGLE", **overrides) -> dict:
    return {
        "combo_type": "NORMAL",
        "order_type": "LIMIT",
        "limit_price": "11.25",
        "quantity": "1",
        "option_strategy": strategy,
        "side": "BUY",
        "time_in_force": "GTC",
        "entrust_type": "QTY",
        "instrument_type": "OPTION",
        "market": "US",
        "symbol": "AAPL",
        "legs": legs,
        **overrides,
    }


def call(*orders: dict) -> dict:
    return {"account_id": ACCOUNT, "new_orders": list(orders)}


def stage(tool: str, *orders: dict):
    return WEBULL.parse_request(tool, call(*orders))


def test_adapter_for_knows_webull():
    assert WEBULL is not None
    assert WEBULL.vendor == "webull"


# -- the request ------------------------------------------------------------


def test_stock_instruction_request():
    order = stage("place_stock_instruction", stock_order())

    assert order.vendor == "webull"
    assert order.mode is OrderMode.STAGED
    assert order.account_ref == ACCOUNT
    assert order.asset_class == "equity"
    assert order.instrument == EquityRef(symbol="AAPL", venue="US")
    assert order.side == "buy"
    assert order.qty == Decimal("10")
    assert order.notional is None
    assert order.order_type == "limit"
    assert order.limit_price == Decimal("180.00")
    assert order.time_in_force == "day"
    assert order.session == "rth"
    assert order.extras == {}
    assert order.raw == call(stock_order())


def test_a_cash_sized_stock_instruction_is_sized_in_dollars():
    order = stage(
        "place_stock_instruction",
        stock_order(
            entrust_type="AMOUNT",
            quantity=None,
            total_cash_amount="100.40",
            support_trading_session="ALL_DAY",
        ),
    )

    assert order.qty is None
    assert order.notional == Money(amount=Decimal("100.40"), currency="USD")
    assert order.session == "all_day"


def test_a_value_with_no_word_here_is_kept_rather_than_flattened():
    order = stage(
        "place_stock_instruction",
        stock_order(time_in_force="IOC", order_type="TRAILING_STOP_LOSS"),
    )

    assert order.time_in_force is None
    assert order.order_type is None
    assert order.extras == {"time_in_force": "IOC", "order_type": "TRAILING_STOP_LOSS"}


def test_single_option_instruction_names_its_contract_from_its_leg():
    order = stage("place_option_single_instruction", option_order([leg()]))

    assert order.asset_class == "option"
    assert order.instrument == OptionRef(
        underlying="AAPL",
        expiration="2027-01-15",
        strike=Decimal("330.00"),
        right="C",
        multiplier=100,
    )
    assert order.side == "buy"
    assert order.time_in_force == "gtc"
    assert order.extras == {}


def test_a_single_option_with_two_legs_names_no_contract():
    """The tool promises one leg; naming the first of two would be a guess."""
    legs = [leg(), leg(option_type="PUT")]
    order = stage("place_option_single_instruction", option_order(legs))

    assert order.instrument is None
    assert order.extras["legs"] == legs


def test_strategy_instruction_is_a_combo_of_its_legs():
    legs = [leg(strike_price="180"), leg(side="SELL", strike_price="190")]
    order = stage(
        "place_option_strategy_instruction",
        option_order(legs, strategy="VERTICAL", limit_price="3.50"),
    )

    assert order.asset_class == "option_combo"
    assert order.instrument == ComboRef(legs=tuple(legs), strategy="VERTICAL")
    # The order's own side is Webull's net direction, stated by the caller.
    assert order.side == "buy"
    assert order.limit_price == Decimal("3.50")


@pytest.mark.parametrize(
    ("tool", "instrument_type", "symbol", "asset_class", "ref"),
    [
        (
            "place_crypto_instruction",
            "CRYPTO",
            "BTCUSD",
            "crypto",
            CryptoRef(pair="BTCUSD"),
        ),
        (
            "place_futures_instruction",
            "FUTURES",
            "ZNU6",
            "future",
            FutureRef(symbol="ZNU6", venue="US"),
        ),
    ],
)
def test_the_tool_names_the_class(tool, instrument_type, symbol, asset_class, ref):
    order = stage(
        tool,
        stock_order(
            symbol=symbol,
            instrument_type=instrument_type,
            support_trading_session=None,
        ),
    )

    assert order.asset_class == asset_class
    assert order.instrument == ref


def test_a_call_carrying_two_orders_describes_neither():
    """The schema caps the list at one; describing the first would show the
    approval card one of two orders."""
    orders = [stock_order(), stock_order(symbol="MSFT")]
    order = stage("place_stock_instruction", *orders)

    assert order.instrument is None
    assert order.side is None
    assert order.qty is None
    assert order.extras == {"new_orders": orders}


def test_revoke_names_the_instruction_it_ends():
    order = WEBULL.parse_request(
        "revoke_instruction", {"account_id": ACCOUNT, "instruction_id": INSTRUCTION_ID}
    )

    assert order.mode is OrderMode.STAGED
    assert order.account_ref == ACCOUNT
    assert order.target_ref == INSTRUCTION_ID
    assert order.instrument is None


def test_a_read_is_not_an_order():
    assert (
        WEBULL.parse_request("get_pending_instruction", {"account_id": ACCOUNT}) is None
    )


@pytest.mark.parametrize("tool", ["place_event_instruction", "place_algo_instruction"])
def test_what_the_adapter_cannot_describe_is_refused_at_every_grant(tool):
    assert is_always_denied("webull", tool)
    assert tool in denied_tools("webull", group_keys_for("webull"))
    assert WEBULL.parse_request(tool, call(stock_order())) is None


# -- the answer -------------------------------------------------------------


def test_a_created_instruction_is_staged_with_its_confirmation_link():
    request = stage("place_stock_instruction", stock_order())
    body = {
        "instruction_id": INSTRUCTION_ID,
        "status": "PENDING",
        "message": f"Instruction created. Confirm it in Webull: {LINK}",
    }

    outcome = WEBULL.parse_result(
        "place_stock_instruction", body, tool_status="success", request=request
    )

    assert outcome.status is AttemptStatus.SUBMITTED
    assert outcome.vendor_order_id == INSTRUCTION_ID
    assert outcome.action_url == LINK


def test_a_create_naming_no_instruction_is_unknown_not_submitted():
    outcome = WEBULL.parse_result(
        "place_stock_instruction",
        {"message": f"Confirm it in Webull: {LINK}"},
        tool_status="success",
    )

    assert outcome.status is AttemptStatus.UNKNOWN
    assert outcome.vendor_order_id is None
    assert outcome.action_url == LINK


def test_a_prose_answer_is_unknown_and_keeps_its_link():
    outcome = WEBULL.parse_result(
        "place_stock_instruction",
        f"Confirm it in Webull: {LINK}",
        tool_status="success",
    )

    assert outcome.status is AttemptStatus.UNKNOWN
    assert outcome.action_url == LINK


def test_a_revoke_is_cancelled_under_the_id_it_named():
    request = WEBULL.parse_request(
        "revoke_instruction", {"account_id": ACCOUNT, "instruction_id": INSTRUCTION_ID}
    )

    outcome = WEBULL.parse_result(
        "revoke_instruction",
        {"status": "REJECTED"},
        tool_status="success",
        request=request,
    )

    assert outcome.status is AttemptStatus.CANCELLED
    assert outcome.vendor_order_id == INSTRUCTION_ID


@pytest.mark.parametrize("tool_status", ["success", "error"])
def test_webulls_error_object_is_its_rejection(tool_status):
    body = {"error_code": "INVALID_PARAMETER", "message": "limit_price is invalid"}

    outcome = WEBULL.parse_result(
        "place_stock_instruction", body, tool_status=tool_status
    )

    assert outcome.status is AttemptStatus.REJECTED_BY_VENDOR
    assert outcome.failure.code == "INVALID_PARAMETER"


# -- the listings -----------------------------------------------------------


def row(status: str | None, instruction_id: str = INSTRUCTION_ID, **fields) -> dict:
    listed = {
        "instruction_id": instruction_id,
        "instrument_type": "EQUITY",
        "symbol": "AAPL",
        "side": "BUY",
        "quantity": "10",
        "limit_price": "180",
        "time_in_force": "DAY",
        "create_time": "2026-10-26T00:40:00+00:00",
        **fields,
    }
    if status is not None:
        listed["status"] = status
    return listed


PENDING = StatusQuery(webull.LIST_PENDING, {"account_id": ACCOUNT, "page_size": "100"})
PROCESSED = StatusQuery(
    webull.LIST_PROCESSED, {"account_id": ACCOUNT, "page_size": "100"}
)


def test_the_listing_starts_at_the_pending_list_for_the_account():
    order = stage("place_stock_instruction", stock_order())

    assert WEBULL.status_query(order) == PENDING


def test_an_attempt_with_no_account_has_no_read_to_make():
    order = WEBULL.parse_request("place_stock_instruction", {"new_orders": []})

    assert WEBULL.status_query(order) is None


@pytest.mark.parametrize(
    ("query", "status", "expected"),
    [
        (PENDING, "PENDING", AttemptStatus.SUBMITTED),
        (PENDING, None, AttemptStatus.SUBMITTED),
        (PROCESSED, "APPROVED", AttemptStatus.HANDED_OFF),
        (PROCESSED, "REJECTED", AttemptStatus.CANCELLED),
        # Never confirmed in time, which nobody cancelled.
        (PROCESSED, "EXPIRED", AttemptStatus.EXPIRED),
        (PROCESSED, "SOMETHING_NEW", AttemptStatus.UNKNOWN),
        (PROCESSED, None, AttemptStatus.UNKNOWN),
    ],
)
def test_a_listed_instruction_reads_as_its_outcome(query, status, expected):
    [listed] = WEBULL.parse_status(query, {"instructions": [row(status)]})

    assert listed.vendor_order_id == INSTRUCTION_ID
    assert listed.outcome.status is expected


def test_a_listed_instruction_pairs_with_the_request_that_made_it():
    request = stage("place_stock_instruction", stock_order(limit_price="180.00"))
    [listed] = WEBULL.parse_status(PENDING, {"instructions": [row("PENDING")]})

    assert WEBULL.match_key(request).matches(listed.match_key)


def test_an_answer_with_no_list_is_not_an_empty_book():
    with pytest.raises(StatusReadError):
        WEBULL.parse_status(PENDING, {"message": "ok"})


def test_a_listed_instruction_with_no_id_is_an_unreadable_list():
    with pytest.raises(StatusReadError):
        WEBULL.parse_status(PENDING, {"instructions": [{"symbol": "AAPL"}]})


def test_a_short_pending_page_goes_on_to_the_processed_list():
    assert WEBULL.next_page(PENDING, {"instructions": [row("PENDING")]}) == PROCESSED


def test_a_full_page_asks_for_the_next_after_its_last_id():
    rows = [row("PENDING", instruction_id=str(n)) for n in range(webull.PAGE_SIZE)]

    page = WEBULL.next_page(PENDING, {"instructions": rows})

    assert page == StatusQuery(
        webull.LIST_PENDING,
        {**PENDING.args, "last_instruction_id": str(webull.PAGE_SIZE - 1)},
    )


def test_the_processed_list_starts_from_its_first_page():
    cursor = StatusQuery(
        webull.LIST_PENDING, {**PENDING.args, "last_instruction_id": "99"}
    )

    assert WEBULL.next_page(cursor, {"instructions": []}) == PROCESSED


def test_a_full_page_with_no_cursor_is_an_incomplete_list():
    rows = [row("PENDING")] * (webull.PAGE_SIZE - 1) + [{"symbol": "AAPL"}]

    with pytest.raises(ListingIncomplete):
        WEBULL.next_page(PENDING, {"instructions": rows})


def test_a_short_processed_page_ends_the_listing():
    assert WEBULL.next_page(PROCESSED, {"instructions": [row("APPROVED")]}) is None


def test_only_a_staged_instruction_has_an_end_by_absence():
    assert WEBULL.absent_status("stage") is AttemptStatus.HANDED_OFF
    assert WEBULL.absent_status("unstage") is None


def test_two_strategies_on_different_legs_are_different_orders():
    """Same strategy, side, size and price: only the legs tell them apart."""
    lower = [leg(strike_price="180"), leg(side="SELL", strike_price="190")]
    higher = [leg(strike_price="200"), leg(side="SELL", strike_price="210")]
    sent = stage(
        "place_option_strategy_instruction",
        option_order(lower, strategy="VERTICAL", limit_price="3.50"),
    )
    other = stage(
        "place_option_strategy_instruction",
        option_order(higher, strategy="VERTICAL", limit_price="3.50"),
    )
    reordered = stage(
        "place_option_strategy_instruction",
        option_order(lower[::-1], strategy="VERTICAL", limit_price="3.50"),
    )

    assert not WEBULL.match_key(sent).matches(WEBULL.match_key(other))
    assert WEBULL.match_key(sent).matches(WEBULL.match_key(reordered))


def test_the_users_own_posts_are_not_market_data():
    """The market data grant says nothing about the user, so their posts are an
    account read."""
    assert group_of_tool("webull", "get_my_community_posts") == "account"
