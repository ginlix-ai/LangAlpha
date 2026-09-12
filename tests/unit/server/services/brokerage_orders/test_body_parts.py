"""The shape a tool result actually arrives in, at the one seam that reads it.

Regression: ``body_parts`` handled the JSON *text* of a content-block list but
not a real list, which is what ``ToolMessage.content`` holds. A live paper
order settled as ``unknown`` with no vendor order id because of it, on a call
the vendor had answered ``success``.
"""

from __future__ import annotations

import json

import pytest

from src.server.services.brokerage_orders import adapter_for, body_parts

ENVELOPE = {"ret_code": 0, "ret_msg": "success", "data": {"order_id": "900103"}}
BLOCKS = [{"type": "text", "text": json.dumps(ENVELOPE), "id": "lc_1"}]


@pytest.mark.parametrize(
    "body",
    [
        pytest.param(BLOCKS, id="real content-block list"),
        pytest.param(json.dumps(BLOCKS), id="that list as JSON text"),
        pytest.param(ENVELOPE, id="the mapping itself"),
        pytest.param(json.dumps(ENVELOPE), id="the mapping as JSON text"),
    ],
)
def test_every_shape_a_tool_result_arrives_in_yields_the_envelope(body):
    envelope, _ = body_parts(body)
    assert envelope == ENVELOPE


def test_an_order_answered_through_a_real_block_list_settles():
    outcome = adapter_for("moomoo").parse_result(
        "sim_trade_input_order", BLOCKS, tool_status="success"
    )
    assert outcome.status == "submitted"
    assert outcome.vendor_order_id == "900103"


def test_a_refusal_in_a_real_block_list_is_still_prose():
    envelope, text = body_parts([{"type": "text", "text": "Refused: not allowed"}])
    assert envelope is None
    assert text == "Refused: not allowed"


def test_bare_strings_in_the_list_are_read_as_text():
    envelope, _ = body_parts([json.dumps(ENVELOPE)])
    assert envelope == ENVELOPE


def test_a_list_with_nothing_readable_has_no_envelope():
    assert body_parts([{"type": "image", "url": "x"}]) == (None, "")
