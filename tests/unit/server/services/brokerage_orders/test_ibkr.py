"""The IBKR adapter against the shapes the server actually answered.

Every body here was captured from live calls to IBKR's MCP server and then
retyped with invented ids: a create answering ``{"id", "url"}``, a delete
answering ``{}``, a list answering ``{"instructions": [...]}`` whose rows carry
no status word at all, and a refusal arriving as an MCP tool error carrying
``{"code", "message", "data"}``.

The arms worth locking are the ones a guess would have got wrong. An empty body
is a successful unstage rather than an unreadable one. A tool error is the
vendor's rejection rather than a transport failure. A listed instruction is
submitted because it is listed, since nothing in the row says so. And a staged
instruction never reads as working or filled: it is not an order until a human
releases it in IBKR's own client.
"""

import json
from decimal import Decimal

import pytest

from src.server.services.brokerage_orders import (
    AttemptStatus,
    BrokerOrder,
    EquityRef,
    OpaqueRef,
    OrderMode,
    StatusReadError,
    adapter_for,
)

IBKR = adapter_for("ibkr")

CONTRACT = "7712345"
INSTRUCTION_ID = "4417"
DEEP_LINK = "https://www.example.com/sso/resolver?action=ACCT-MGMT-MAIN#/orders"

CREATE_ARGS = {
    "side": "BUY",
    "quantity": 1,
    "order_type": "LIMIT",
    "limit_price": 1,
    "time_in_force": "DAY",
    "contract_id_ex": CONTRACT,
}

# What the server answers a create with, and all it answers: no status word, no
# echo of the order, and a deep link into IBKR's own client.
CREATE_RESULT = {"id": INSTRUCTION_ID, "url": DEEP_LINK}

# One row of ``get_order_instructions``. Numbers come back as floats where the
# request sent ints, the time in force is ``tif`` rather than ``time_in_force``,
# and ``expiration`` is the instruction's own shelf life rather than the order's.
LISTED_ROW = {
    "id": INSTRUCTION_ID,
    "contract_id_ex": CONTRACT,
    "contract_id": int(CONTRACT),
    "side": "BUY",
    "quantity": 1.0,
    "order_type": "LIMIT",
    "limit_price": 1.0,
    "tif": "DAY",
    "creation_time": "2026-09-09T23:59:55.690Z",
    "expiration": "2026-09-16T23:59:55.690Z",
    "is_new": True,
    "symbol": "AAPL",
}

# How the server refuses: on the tool-error channel, never in a body.
REJECTION = {
    "code": -32400,
    "message": "Instruction with id 1 does not exist",
    "data": None,
}


def create_order() -> BrokerOrder:
    return IBKR.parse_request("create_order_instruction", CREATE_ARGS)


def test_adapter_for_knows_ibkr():
    assert IBKR is not None
    assert IBKR.vendor == "ibkr"


# -- the request ------------------------------------------------------------


def test_create_instruction_request():
    order = create_order()

    assert order.vendor == "ibkr"
    assert order.mode is OrderMode.STAGED
    # IBKR takes no account argument and names none in any answer.
    assert order.account_ref == ""
    # One contract_id_ex addresses every class IBKR stages and says which one
    # it named nowhere, so the class stays unclaimed.
    assert order.asset_class == "other"
    assert order.instrument == OpaqueRef(raw_code=CONTRACT)
    assert order.side == "buy"
    assert order.qty == Decimal("1")
    assert order.order_type == "limit"
    assert order.limit_price == Decimal("1")
    assert order.time_in_force == "day"
    assert order.extras == {"contract_id_ex": CONTRACT}
    assert order.raw == CREATE_ARGS


def test_contract_id_ex_outranks_a_zero_filled_contract_id():
    """The shape a model produces: the real id beside a filler for the old key.

    ``contract_id`` is deprecated and ``contract_id_ex`` wins at the vendor, so
    reading the first non-null of the two puts 0 on the approval card as the
    instrument the user is being asked to approve.
    """
    order = IBKR.parse_request(
        "create_order_instruction", {**CREATE_ARGS, "contract_id": 0}
    )

    assert order.instrument == OpaqueRef(raw_code=CONTRACT)
    assert order.extras["contract_id_ex"] == CONTRACT


def test_deprecated_contract_id_is_read_when_it_is_the_only_one():
    order = IBKR.parse_request(
        "create_order_instruction",
        {"side": "BUY", "quantity": 1, "contract_id": 7712345},
    )

    assert order.instrument == OpaqueRef(raw_code=CONTRACT)


def test_a_venue_qualified_contract_stays_whole():
    order = IBKR.parse_request(
        "create_order_instruction",
        {**CREATE_ARGS, "contract_id_ex": "722271932@CBOE"},
    )

    assert order.instrument == OpaqueRef(raw_code="722271932@CBOE")
    assert order.asset_class == "other"


def test_market_order_carries_no_limit_price():
    order = IBKR.parse_request(
        "create_order_instruction",
        {
            "side": "SELL",
            "quantity": 2,
            "order_type": "MARKET",
            "contract_id_ex": CONTRACT,
        },
    )

    assert order.side == "sell"
    assert order.order_type == "market"
    assert order.limit_price is None
    assert order.time_in_force is None


@pytest.mark.parametrize(
    ("tif", "neutral"),
    [("OVT", "overnight"), ("OND", "overnight_next_day"), ("OPG", "at_the_open")],
)
def test_the_three_sessions_ibkr_stages_have_their_own_words(tif, neutral):
    """Flattening any of these onto ``day`` would state a different order.

    An overnight instruction and an opening-auction one expire on their own
    schedule, so the neutral vocabulary names them rather than dropping them
    into ``extras`` for a reader to interpret.
    """
    order = IBKR.parse_request(
        "create_order_instruction", {**CREATE_ARGS, "time_in_force": tif}
    )

    assert order.time_in_force == neutral
    assert "time_in_force" not in order.extras


def test_an_unmapped_time_in_force_still_survives_in_extras():
    order = IBKR.parse_request(
        "create_order_instruction", {**CREATE_ARGS, "time_in_force": "IOC"}
    )

    assert order.time_in_force is None
    assert order.extras["time_in_force"] == "IOC"


def test_delete_instruction_request():
    order = IBKR.parse_request(
        "delete_order_instruction", {"id": INSTRUCTION_ID}
    )

    assert order.vendor == "ibkr"
    assert order.mode is OrderMode.STAGED
    assert order.account_ref == ""
    assert order.extras == {"id": INSTRUCTION_ID}


def test_a_read_is_not_an_order_request():
    assert IBKR.parse_request("get_order_instructions", {}) is None
    assert IBKR.parse_request("get_account_orders", {}) is None


# -- the answer -------------------------------------------------------------


def test_create_result_is_submitted_with_the_instruction_id():
    outcome = IBKR.parse_result(
        "create_order_instruction",
        json.dumps(CREATE_RESULT),
        tool_status="success",
        request=create_order(),
    )

    assert outcome.status is AttemptStatus.SUBMITTED
    assert outcome.vendor_order_id == INSTRUCTION_ID
    assert outcome.route == {"contract_id_ex": CONTRACT}
    # The vendor states no status at all, so inventing one to display would be
    # this file making it up.
    assert outcome.raw_status is None
    assert outcome.raw == CREATE_RESULT


def test_the_deep_link_is_what_the_user_has_to_open():
    """A staged instruction is not an order until a human releases it.

    The link to the page where that happens exists only in this answer, so it
    is carried on the outcome rather than left for a reader to dig out of the
    vendor body.
    """
    outcome = IBKR.parse_result(
        "create_order_instruction", json.dumps(CREATE_RESULT), tool_status="success"
    )

    assert outcome.action_url == DEEP_LINK
    assert outcome.raw["url"] == DEEP_LINK


def test_create_result_with_no_instruction_id_is_unknown():
    outcome = IBKR.parse_result(
        "create_order_instruction",
        json.dumps({"url": DEEP_LINK}),
        tool_status="success",
    )

    assert outcome.status is AttemptStatus.UNKNOWN
    assert outcome.vendor_order_id is None


def test_an_empty_body_is_a_successful_unstage():
    """The delete answers ``{}`` and nothing else.

    Read as "the vendor told us nothing" the attempt settles as ``unknown``
    and, with no id to look it up by, is never reconciled either: a call that
    plainly worked would sit in the ledger saying it might not have.
    """
    request = IBKR.parse_request("delete_order_instruction", {"id": INSTRUCTION_ID})

    outcome = IBKR.parse_result(
        "delete_order_instruction", "{}", tool_status="success", request=request
    )

    assert outcome.status is AttemptStatus.CANCELLED
    # The answer echoes no id, so the only place it exists is the call.
    assert outcome.vendor_order_id == INSTRUCTION_ID
    assert request.target_ref == INSTRUCTION_ID
    assert outcome.action_url is None


def test_an_unstage_with_no_request_still_settles():
    outcome = IBKR.parse_result("delete_order_instruction", "{}", tool_status="success")

    assert outcome.status is AttemptStatus.CANCELLED
    assert outcome.vendor_order_id is None


def test_a_tool_error_carrying_the_vendor_error_object_is_a_rejection():
    """IBKR refuses on the error channel, so the default reading is wrong here.

    ``pre_vendor_outcome`` calls any tool error a transport failure, which is
    right at a vendor that answers refusals in a body. This one does not.
    """
    outcome = IBKR.parse_result(
        "create_order_instruction",
        json.dumps(REJECTION),
        tool_status="error",
        request=create_order(),
    )

    assert outcome.status is AttemptStatus.REJECTED_BY_VENDOR
    assert outcome.failure is not None
    assert outcome.failure.kind == "vendor"
    assert outcome.failure.code == "-32400"
    assert outcome.failure.message == REJECTION["message"]


def test_a_tool_error_with_no_vendor_error_object_stays_a_transport_failure():
    outcome = IBKR.parse_result(
        "create_order_instruction",
        "upstream unreachable",
        tool_status="error",
        request=create_order(),
    )

    assert outcome.status is AttemptStatus.FAILED
    assert outcome.failure is not None
    assert outcome.failure.kind == "transport"


def test_a_policy_refusal_outranks_the_error_channel():
    outcome = IBKR.parse_result(
        "create_order_instruction",
        "Refused: staged instructions are not permitted on this connection",
        tool_status="error",
    )

    assert outcome.status is AttemptStatus.REFUSED
    assert outcome.failure is not None
    assert outcome.failure.kind == "policy"


def test_the_vendor_error_hook_reads_only_the_vendors_error_object():
    """What the shared classifier asks before it calls an error transport."""
    assert IBKR.vendor_error(REJECTION, "").code == "-32400"
    assert IBKR.vendor_error(CREATE_RESULT, "") is None
    assert IBKR.vendor_error(None, "upstream unreachable") is None


def test_a_body_that_is_not_json_is_unknown():
    outcome = IBKR.parse_result(
        "create_order_instruction", "<html>gateway</html>", tool_status="success"
    )

    assert outcome.status is AttemptStatus.UNKNOWN


def test_a_result_wrapped_in_content_blocks_reads_the_same():
    blocks = [{"type": "text", "text": json.dumps(CREATE_RESULT)}]

    outcome = IBKR.parse_result(
        "create_order_instruction", blocks, tool_status="success"
    )

    assert outcome.status is AttemptStatus.SUBMITTED
    assert outcome.vendor_order_id == INSTRUCTION_ID


# -- the status read --------------------------------------------------------


def test_status_query_is_the_bare_list_read():
    query = IBKR.status_query(create_order(), vendor_order_id=INSTRUCTION_ID)

    assert query is not None
    assert query.tool == "get_order_instructions"
    assert query.args == {}


def test_a_listed_instruction_is_submitted_because_it_is_listed():
    """No row carries a status word, so being in the list is the whole state."""
    listed = IBKR.parse_status(
        IBKR.status_query(None), json.dumps({"instructions": [LISTED_ROW]})
    )

    assert len(listed) == 1
    assert listed[0].vendor_order_id == INSTRUCTION_ID
    assert listed[0].outcome.status is AttemptStatus.SUBMITTED
    assert listed[0].outcome.route == {"contract_id_ex": CONTRACT}
    assert listed[0].placed_at is not None
    assert listed[0].placed_at.year == 2026


def test_a_listed_row_names_the_symbol_the_request_could_not():
    """The one thing the listing knows that the call never did.

    A create addresses the contract by ``contract_id_ex`` and the answer echoes
    nothing, so the attempt carries an opaque id; the list is where the symbol
    first appears, and it is what reconciliation writes back onto the order.
    """
    listed = IBKR.parse_status(
        IBKR.status_query(None), json.dumps({"instructions": [LISTED_ROW]})
    )

    assert listed[0].instrument == EquityRef(symbol="AAPL")
    # The contract id is still the route's, so nothing is traded for the symbol.
    assert listed[0].outcome.route == {"contract_id_ex": CONTRACT}


def test_a_listed_row_with_no_symbol_names_no_instrument():
    listed = IBKR.parse_status(
        IBKR.status_query(None),
        json.dumps(
            {"instructions": [{k: v for k, v in LISTED_ROW.items() if k != "symbol"}]}
        ),
    )

    assert listed[0].instrument is None


def test_an_empty_list_is_an_answer_and_not_an_error():
    assert IBKR.parse_status(IBKR.status_query(None), '{"instructions":[]}') == []


def test_an_answer_with_no_instruction_list_is_a_failed_read():
    """Absence of the key is not an account with nothing staged.

    Reading it as one would report every open attempt as gone at the vendor.
    """
    with pytest.raises(StatusReadError):
        IBKR.parse_status(IBKR.status_query(None), '{"detail":"forbidden"}')


def test_a_status_read_that_returned_the_vendor_error_object_raises():
    with pytest.raises(StatusReadError):
        IBKR.parse_status(IBKR.status_query(None), json.dumps(REJECTION))


def test_a_status_read_with_no_envelope_raises():
    with pytest.raises(StatusReadError):
        IBKR.parse_status(IBKR.status_query(None), "gateway timeout")


def test_a_row_with_no_id_fails_the_read():
    """Dropped, it would read as an instruction IBKR no longer has."""
    nameless = {k: v for k, v in LISTED_ROW.items() if k != "id"}

    for entry in (nameless, None):
        with pytest.raises(StatusReadError, match="listed instruction with no id"):
            IBKR.parse_status(
                IBKR.status_query(None),
                json.dumps({"instructions": [LISTED_ROW, entry]}),
            )


def test_no_listed_state_maps_past_submitted():
    """The invariant the whole vendor rests on.

    A staged instruction is not an order, so there is nothing to watch working
    or fill. If a later shape ever reads as one, this is the arm that says so.
    """
    listed = IBKR.parse_status(
        IBKR.status_query(None),
        json.dumps({"instructions": [LISTED_ROW, {**LISTED_ROW, "id": "4418"}]}),
    )

    assert {order.outcome.status for order in listed} == {AttemptStatus.SUBMITTED}


# -- pairing a create with the instruction it made --------------------------


def test_a_create_and_its_listed_row_fingerprint_alike():
    """What settles a create whose answer never came back.

    The request sends ``1`` where the list answers ``1.0``, and the request
    calls the field ``time_in_force`` where the list calls it ``tif``, so the
    two sides only meet if both are normalized the same way.
    """
    listed = IBKR.parse_status(
        IBKR.status_query(None), json.dumps({"instructions": [LISTED_ROW]})
    )

    assert IBKR.match_key(create_order()) == listed[0].match_key
    assert listed[0].match_key is not None


def test_a_different_price_is_a_different_instruction():
    listed = IBKR.parse_status(
        IBKR.status_query(None),
        json.dumps({"instructions": [{**LISTED_ROW, "limit_price": 2.0}]}),
    )

    assert not IBKR.match_key(create_order()).matches(listed[0].match_key)


def test_a_create_that_left_the_optional_fields_to_ibkr_still_pairs():
    """``create_order_instruction`` requires ``side`` and nothing else.

    A call naming only the contract, the side and the size is a whole order at
    IBKR, which chooses the kind, the price and the duration itself and then
    lists the values it chose. Reading those against fields the request never
    sent would report an instruction that was written as one that never reached
    the account.
    """
    request = IBKR.parse_request(
        "create_order_instruction",
        {"side": "BUY", "quantity": 1, "contract_id_ex": CONTRACT},
    )
    listed = IBKR.parse_status(
        IBKR.status_query(None), json.dumps({"instructions": [LISTED_ROW]})
    )

    assert IBKR.match_key(request).matches(listed[0].match_key)
    # And still only where the two agree: the size is one of the three fields
    # that make this a different instruction rather than the same one.
    bigger = IBKR.parse_request(
        "create_order_instruction",
        {"side": "BUY", "quantity": 2, "contract_id_ex": CONTRACT},
    )
    assert not IBKR.match_key(bigger).matches(listed[0].match_key)


def test_an_unstage_has_no_fingerprint():
    """It names none of the fields that tell two instructions apart.

    None is the honest answer: it already carries the id it was called with,
    and a key built from nothing would pair it with somebody else's row.
    """
    request = IBKR.parse_request("delete_order_instruction", {"id": INSTRUCTION_ID})

    assert IBKR.match_key(request) is None


def test_match_key_of_nothing_is_none():
    assert IBKR.match_key(None) is None
