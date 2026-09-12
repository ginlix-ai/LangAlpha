"""The public replay must not carry an order's identifiers off the owner's account.

A share link is readable by anyone who has it. Four things a turn carries name
the owner's brokerage account and the orders placed on it: the ``provenance``
event of a direct tool call, the ``order_receipt`` stamped on that call's tool
artifact, the vendor's own answer to an order call, and the verdict the owner's
resume recorded against the order it answered. All are stripped server-side
here, so the owner-only rule does not depend on a client choosing not to render
them.
"""

from __future__ import annotations

import json
from unittest.mock import AsyncMock, patch

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient

from tests.conftest import create_test_app

pytestmark = pytest.mark.asyncio

_SHARE_TOKEN = "share_abc123"
_THREAD_ID = "44444444-4444-4444-8444-444444444444"

_THREAD_BY_TOKEN = "src.server.app.public.get_thread_by_share_token"
_QUERIES = "src.server.app.public.get_queries_for_thread"
_RESPONSES = "src.server.app.public.get_responses_for_thread"
_TASK_DETAILS = "src.server.services.history.task_status.resolve_task_details"

_RECEIPT = {
    "type": "order_receipt",
    "attempt_id": "11111111-2222-4333-8444-555555555555",
    "order": {"account_ref": "1234567", "side": "buy"},
    "outcome": {"vendor_order_id": "900104"},
}

_SSE_EVENTS = [
    {
        "event": "tool_call_chunks",
        "data": {
            "id": "msg-1",
            "tool_call_chunks": [
                {"args": "{\"acc_id\": \"1234567\"", "index": 0, "type": "tool_call_chunk"}
            ],
        },
    },
    {
        "event": "tool_calls",
        "data": {
            "id": "msg-1",
            "tool_calls": [
                {
                    "name": "moomoo__sim_trade_input_order",
                    "args": {"acc_id": "1234567", "code": "US.AAPL", "qty": 1},
                    "id": "call_abc",
                    "type": "tool_call",
                },
                {
                    "name": "moomoo__get_market_snapshot",
                    "args": {"code_list": ["US.AAPL"]},
                    "id": "call_quote",
                    "type": "tool_call",
                },
            ],
        },
    },
    {
        "event": "interrupt",
        "data": {
            "interrupt_id": "int-1",
            "kind": "order_approval",
            "action_requests": [
                {
                    "name": "moomoo__sim_trade_input_order",
                    "args": {"acc_id": "1234567", "code": "US.AAPL"},
                    "description": "{\"acc_id\": \"1234567\"}",
                    "tool_call_id": "call_abc",
                    "attempt_id": "11111111-2222-4333-8444-555555555555",
                    "order": {"account_ref": "1234567", "side": "buy"},
                }
            ],
        },
    },
    {
        "event": "tool_call_result",
        "data": {
            "tool_call_id": "call_abc",
            "content": "{\"order_id\": \"900104\", \"acc_id\": \"1234567\"}",
            "artifact": {
                "direct_mcp": {"server": "moomoo", "tool": "sim_trade_input_order"},
                "order_receipt": _RECEIPT,
                "provenance": {"account_ref": "1234567"},
            },
        },
    },
    {
        "event": "provenance",
        "data": {"tool_call_id": "call_abc", "args": {"acc_id": "1234567"}},
    },
    {
        "event": "message",
        "data": {"content": "Placed.", "workspace_id": "should-be-stripped"},
    },
]


def _tool_result(stamp: dict, content: str) -> dict:
    """A direct tool's result carrying only the binder's stamp, no receipt."""
    return {
        "event": "tool_call_result",
        "data": {
            "tool_call_id": "call_abc",
            "content": content,
            "artifact": {"direct_mcp": stamp},
        },
    }


def _call(name: str, args: dict, message_id: str | None = "msg-1") -> dict:
    """A ``tool_calls`` event for one call, under the id ``_tool_result`` answers."""
    data: dict = {
        "tool_calls": [
            {"name": name, "args": args, "id": "call_abc", "type": "tool_call"}
        ]
    }
    if message_id is not None:
        data["id"] = message_id
    return {"event": "tool_calls", "data": data}


def _replayed_call(events: list[dict]) -> dict:
    return next(e for e in events if e["event"] == "tool_calls")["data"]["tool_calls"][0]


def _replayed_calls(events: list[dict]) -> list[dict]:
    return [
        call
        for e in events
        if e["event"] == "tool_calls"
        for call in e["data"]["tool_calls"]
    ]


@pytest_asyncio.fixture
async def client():
    from src.server.app.public import router

    app = create_test_app(router)
    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as c:
        yield c


async def _replay(
    client,
    sse_events: list[dict] | None = None,
    later_events: list[dict] | None = None,
    later_metadata: dict | None = None,
) -> tuple[str, list[dict]]:
    thread = {
        "conversation_thread_id": _THREAD_ID,
        "workspace_id": "ws-1",
        "share_permissions": {},
    }
    queries = [{"turn_index": 0, "content": "buy one share", "metadata": {}}]
    responses = [
        {
            "turn_index": 0,
            "conversation_response_id": "55555555-5555-4555-8555-555555555555",
            "sse_events": sse_events if sse_events is not None else _SSE_EVENTS,
        }
    ]
    if later_events is not None:
        queries.append(
            {
                "turn_index": 1,
                "content": "approve",
                "metadata": later_metadata if later_metadata is not None else {},
            }
        )
        responses.append(
            {
                "turn_index": 1,
                "conversation_response_id": "66666666-6666-4666-8666-666666666666",
                "sse_events": later_events,
            }
        )
    with (
        patch(_THREAD_BY_TOKEN, new=AsyncMock(return_value=thread)),
        patch(_QUERIES, new=AsyncMock(return_value=(queries, None))),
        patch(_RESPONSES, new=AsyncMock(return_value=(responses, None))),
        patch(_TASK_DETAILS, new=AsyncMock(return_value={})),
    ):
        resp = await client.get(f"/api/v1/public/shared/{_SHARE_TOKEN}/replay")
        assert resp.status_code == 200
        body = resp.text
    events = []
    for block in body.split("\n\n"):
        kind = payload = None
        for line in block.splitlines():
            if line.startswith("event: "):
                kind = line[len("event: ") :]
            elif line.startswith("data: "):
                payload = json.loads(line[len("data: ") :])
        if kind:
            events.append({"event": kind, "data": payload})
    return body, events


async def test_replay_drops_provenance_events(client):
    body, events = await _replay(client)
    assert [e["event"] for e in events if e["event"] == "provenance"] == []
    assert "acc_id" not in body


async def test_replay_strips_the_order_receipt_from_the_tool_artifact(client):
    body, events = await _replay(client)
    result = next(e for e in events if e["event"] == "tool_call_result")
    artifact = result["data"]["artifact"]
    assert "order_receipt" not in artifact
    assert "provenance" not in artifact
    # The vendor's own names still travel: the receipt is what is private,
    # not the fact that a direct tool ran.
    assert artifact["direct_mcp"] == {
        "server": "moomoo",
        "tool": "sim_trade_input_order",
    }
    assert "1234567" not in body
    assert "900104" not in body


async def test_replay_drops_the_order_interrupt_and_the_vendor_answer(client):
    body, events = await _replay(client)
    assert [e["event"] for e in events if e["event"] == "interrupt"] == []
    result = next(e for e in events if e["event"] == "tool_call_result")
    assert result["data"]["content"] == ""
    assert "attempt_id" not in body
    assert "acc_id" not in body


async def test_replay_blanks_an_order_answer_the_ledger_never_receipted(client):
    """A ledger write that failed leaves no receipt; the stamp still names an order."""
    stamp = {
        "server": "moomoo",
        "tool": "sim_trade_input_order",
        "order": {"action": "place", "mode": "paper"},
    }
    body, events = await _replay(
        client,
        [_tool_result(stamp, "{\"order_id\": \"900104\", \"acc_id\": \"1234567\"}")],
    )
    result = next(e for e in events if e["event"] == "tool_call_result")
    assert result["data"]["content"] == ""
    assert "1234567" not in body
    assert "900104" not in body


async def test_replay_keeps_the_answer_of_a_direct_tool_that_is_not_an_order(client):
    stamp = {"server": "moomoo", "tool": "get_market_snapshot", "order": None}
    _, events = await _replay(
        client, [_tool_result(stamp, "{\"last_price\": \"318.62\"}")]
    )
    result = next(e for e in events if e["event"] == "tool_call_result")
    assert "318.62" in result["data"]["content"]


async def test_replay_empties_the_arguments_of_an_order_call(client):
    body, events = await _replay(client)
    calls = {
        call["id"]: call
        for e in events
        if e["event"] == "tool_calls"
        for call in e["data"]["tool_calls"]
    }
    assert calls["call_abc"]["args"] == {}
    assert calls["call_abc"]["name"] == "moomoo__sim_trade_input_order"
    # A call that is not an order keeps what it asked for.
    assert calls["call_quote"]["args"] == {"code_list": ["US.AAPL"]}
    assert [e for e in events if e["event"] == "tool_call_chunks"] == []
    assert "1234567" not in body


async def test_replay_empties_an_order_call_whose_result_lands_in_a_later_turn(client):
    """An approval ends the turn that made the call; its result follows the resume."""
    call = {
        "event": "tool_calls",
        "data": {
            "tool_calls": [
                {
                    "name": "moomoo__sim_trade_input_order",
                    "args": {"acc_id": "1234567"},
                    "id": "call_abc",
                    "type": "tool_call",
                }
            ]
        },
    }
    stamp = {
        "server": "moomoo",
        "tool": "sim_trade_input_order",
        "order": {"action": "place", "mode": "paper"},
    }
    body, events = await _replay(
        client, [call], later_events=[_tool_result(stamp, "{}")]
    )
    replayed = next(e for e in events if e["event"] == "tool_calls")
    assert replayed["data"]["tool_calls"][0]["args"] == {}
    assert "1234567" not in body


@pytest.mark.parametrize("later", [False, True], ids=["same_turn", "later_turn"])
async def test_replay_keeps_the_arguments_of_a_direct_call_answered_as_no_order(
    client, later
):
    """An answer stamped with no order is the one proof a direct call was not one."""
    call = _call("mcp__moomoo__get_market_snapshot", {"code_list": ["US.AAPL"]})
    stamp = {"server": "moomoo", "tool": "get_market_snapshot", "order": None}
    answer = _tool_result(stamp, "{\"last_price\": \"318.62\"}")
    if later:
        _, events = await _replay(client, [call], later_events=[answer])
    else:
        _, events = await _replay(client, [call, answer])
    assert _replayed_call(events)["args"] == {"code_list": ["US.AAPL"]}


async def test_replay_empties_a_direct_call_that_was_never_answered(client):
    """A Stop or a lost worker leaves an order with no answer and no approval card."""
    name = "mcp__moomoo__sim_trade_input_order"
    first = {
        "event": "tool_call_chunks",
        "data": {
            "id": "msg-1",
            "tool_call_chunks": [
                {"name": name, "args": "", "id": "call_abc", "index": 0}
            ],
        },
    }
    # Later fragments carry only the index, so no name or id to match them by.
    rest = {
        "event": "tool_call_chunks",
        "data": {
            "id": "msg-1",
            "tool_call_chunks": [
                {"name": None, "args": "{\"acc_id\": \"1234567\"}", "id": None, "index": 0}
            ],
        },
    }
    call = _call(name, {"acc_id": "1234567", "code": "US.AAPL", "qty": 5})
    body, events = await _replay(client, [first, rest, call])
    replayed = _replayed_call(events)
    assert replayed["args"] == {}
    assert replayed["name"] == name
    assert [e for e in events if e["event"] == "tool_call_chunks"] == []
    assert "1234567" not in body


@pytest.mark.parametrize("later", [False, True], ids=["same_turn", "later_turn"])
async def test_replay_empties_an_order_call_that_reuses_a_cleared_call_id(
    client, later
):
    """A provider may repeat a call id, so an answer clears only the message that made it.

    The order was stopped before any approval card or answer, with approval off
    for its mode, so nothing else marks it.
    """
    quote = _call("mcp__moomoo__get_market_snapshot", {"code_list": ["US.AAPL"]}, "msg-1")
    stamp = {"server": "moomoo", "tool": "get_market_snapshot", "order": None}
    answer = _tool_result(stamp, "{\"last_price\": \"318.62\"}")
    order = _call(
        "mcp__moomoo__sim_trade_input_order",
        {"acc_id": "1234567", "code": "US.AAPL", "qty": 5, "price": 190.5},
        "msg-2",
    )
    if later:
        body, events = await _replay(client, [quote, answer], later_events=[order])
    else:
        body, events = await _replay(client, [quote, answer, order])
    replayed_quote, replayed_order = _replayed_calls(events)
    assert replayed_quote["args"] == {"code_list": ["US.AAPL"]}
    assert replayed_order["args"] == {}
    assert replayed_order["id"] == "call_abc"
    assert "1234567" not in body


async def test_replay_keeps_each_call_that_reuses_an_id_when_each_is_answered(client):
    stamp = {"server": "moomoo", "tool": "get_market_snapshot", "order": None}
    answer = _tool_result(stamp, "{\"last_price\": \"318.62\"}")
    first = _call("mcp__moomoo__get_market_snapshot", {"code_list": ["US.AAPL"]}, "msg-1")
    second = _call("mcp__moomoo__get_market_snapshot", {"code_list": ["US.MSFT"]}, "msg-2")
    _, events = await _replay(client, [first, answer], later_events=[second, answer])
    assert [call["args"] for call in _replayed_calls(events)] == [
        {"code_list": ["US.AAPL"]},
        {"code_list": ["US.MSFT"]},
    ]


@pytest.mark.parametrize("message_id", [None, "unknown"], ids=["missing", "unknown"])
async def test_replay_empties_a_direct_call_whose_message_has_no_id(client, message_id):
    """Without a message id an answer cannot be tied to one call, so it clears none.

    ``unknown`` is what the stream writes for a message that came with no id.
    """
    call = _call(
        "mcp__moomoo__get_market_snapshot", {"code_list": ["US.AAPL"]}, message_id
    )
    stamp = {"server": "moomoo", "tool": "get_market_snapshot", "order": None}
    _, events = await _replay(
        client, [call, _tool_result(stamp, "{\"last_price\": \"318.62\"}")]
    )
    assert _replayed_call(events)["args"] == {}


async def test_replay_empties_a_direct_call_whose_answer_carries_no_stamp(client):
    """An answer without the binder's stamp says nothing about what the call did."""
    call = _call("mcp__moomoo__get_market_snapshot", {"code_list": ["US.AAPL"]})
    error = {
        "event": "tool_call_result",
        "data": {
            "tool_call_id": "call_abc",
            "content": "Error: the relay timed out",
            "status": "error",
        },
    }
    _, events = await _replay(client, [call, error])
    assert _replayed_call(events)["args"] == {}


async def test_replay_keeps_the_arguments_of_an_unanswered_call_that_is_not_direct(
    client,
):
    """Only a direct call can place an order, so the rule stops at the prefix."""
    call = _call("web_search", {"query": "AAPL earnings date"})
    _, events = await _replay(client, [call])
    assert _replayed_call(events)["args"] == {"query": "AAPL earnings date"}


async def test_replay_strips_the_order_verdicts_the_owners_resume_recorded(client):
    """The resume that answers an approval names the order by its ledger id.

    Everything else on the order is already stripped, so this field was the one
    place an attempt id still reached a viewer. The suite asserted no attempt id
    appears in a public body long before that was true of this path: every query
    row here carried empty metadata, so the assertion never met one.
    """
    attempt = "11111111-2222-4333-8444-555555555555"
    stamp = {
        "server": "moomoo",
        "tool": "sim_trade_input_order",
        "order": {"action": "place", "mode": "paper"},
    }
    body, events = await _replay(
        client,
        [_call("mcp__moomoo__sim_trade_input_order", {"acc_id": "1234567"})],
        later_events=[_tool_result(stamp, "{}")],
        later_metadata={
            "order_decisions": {attempt: {"type": "approve", "message": None}},
            "attachments": [],
        },
    )
    resume = [e for e in events if e["event"] == "user_message"][1]
    assert "order_decisions" not in resume["data"]["metadata"]
    # What a viewer legitimately needs is untouched: one key is removed, not the
    # whole dict, so the strip cannot quietly break the shared view's rendering.
    assert resume["data"]["metadata"] == {"attachments": []}
    assert attempt not in body
    assert "attempt_id" not in body


async def test_replay_still_carries_the_rest_of_the_turn(client):
    _, events = await _replay(client)
    kinds = [e["event"] for e in events]
    assert kinds.count("user_message") == 1
    assert "message" in kinds
    assert kinds[-1] == "replay_done"
    message = next(e for e in events if e["event"] == "message")
    assert message["data"]["content"] == "Placed."
    assert "workspace_id" not in message["data"]
