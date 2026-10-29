"""The public replay must not carry an order's identifiers off the owner's account.

A share link is readable by anyone who has it. Five things a turn carries name
the owner's brokerage account and the orders placed on it: the ``provenance``
event of a direct tool call, an order call's own arguments, the ``order_receipt``
stamped on that call's tool artifact, the vendor's own answer to an order call,
and the verdict the owner's resume recorded against the order it answered. All
are stripped server-side, so the owner-only rule does not depend on a client
choosing not to render them. The rule itself is pinned in
``tests/unit/server/services/test_share_redaction.py``; these run it through
the route, across turns and onto the wire.
"""

from __future__ import annotations

import json
from unittest.mock import AsyncMock, patch

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient

from src.server.services.history.replay import ReplayPage
from src.server.services.history.replay.items import _user_message_item
from src.server.services.history.replay.lines import line_of
from tests.conftest import create_test_app
from tests.unit.server.services.history.replay_builders import replay_rows

pytestmark = pytest.mark.asyncio

_SHARE_TOKEN = "share_abc123"
_THREAD_ID = "44444444-4444-4444-8444-444444444444"

_THREAD_BY_TOKEN = "src.server.app.share_access.get_thread_by_share_token"
_REPLAY_DATA = "src.server.app.public.get_replay_thread_data"
_READ_PAGE = "src.server.services.history.replay.read_replay_page"
_TASK_DETAILS = "src.server.services.history.task_status.resolve_task_details"

_RECEIPT = {
    "type": "order_receipt",
    "attempt_id": "11111111-2222-4333-8444-555555555555",
    "order": {"account_ref": "1234567", "side": "buy"},
    "outcome": {"vendor_order_id": "900104"},
}

_TURN_EVENTS = [
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
        "event": "message_chunk",
        "data": {
            "content_type": "text",
            "content": "Placed.",
            "workspace_id": "should-be-stripped",
        },
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
    events: list[dict] | None = None,
    later_events: list[dict] | None = None,
    later_metadata: dict | None = None,
    later_query: dict | None = None,
) -> tuple[str, list[dict]]:
    """The shared stream of a page holding ``events`` as turn 0, and
    ``later_events`` as a turn 1 the owner's ``later_metadata`` resumed
    (``later_query`` overriding its query row's fields).

    The page is handed over as the owner's assembly returns it, so these pin
    what the route sends of it, not how it was projected."""
    thread = {
        "conversation_thread_id": _THREAD_ID,
        "workspace_id": "ws-1",
        "share_permissions": {},
    }
    queries = [{"turn_index": 0, "content": "buy one share", "metadata": {}}]
    turns = [events if events is not None else _TURN_EVENTS]
    if later_events is not None:
        queries.append(
            {
                "turn_index": 1,
                "content": "approve",
                "metadata": later_metadata if later_metadata is not None else {},
                **(later_query or {}),
            }
        )
        turns.append(later_events)
    items = [
        item
        for query, turn in zip(queries, turns)
        for item in (_user_message_item(_THREAD_ID, query), *turn)
    ]
    page = ReplayPage(
        lines=[line_of(item) for item in items], first_turn_index=0, has_more=False
    )
    with (
        patch(_THREAD_BY_TOKEN, new=AsyncMock(return_value=thread)),
        patch(
            _REPLAY_DATA,
            new=AsyncMock(
                return_value=replay_rows(
                    {
                        "conversation_thread_id": _THREAD_ID,
                        "latest_checkpoint_id": "cp-1",
                    },
                    queries,
                    [],
                )
            ),
        ),
        patch(_READ_PAGE, new=AsyncMock(return_value=(page, "checkpoint"))),
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


async def test_replay_carries_no_identifier_of_the_order(client):
    body, events = await _replay(client)
    kinds = {e["event"] for e in events}
    assert not kinds & {"provenance", "interrupt"}
    result = next(e for e in events if e["event"] == "tool_call_result")
    assert result["data"]["content"] == ""
    assert "order_receipt" not in result["data"]["artifact"]
    calls = {
        call["id"]: call["args"]
        for e in events
        if e["event"] == "tool_calls"
        for call in e["data"]["tool_calls"]
    }
    assert calls == {"call_abc": {}, "call_quote": {"code_list": ["US.AAPL"]}}
    for value in ("1234567", "900104", "attempt_id", "acc_id"):
        assert value not in body


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
    assert "message_chunk" in kinds
    assert kinds[-1] == "replay_done"
    message = next(e for e in events if e["event"] == "message_chunk")
    assert message["data"]["content"] == "Placed."
    assert "workspace_id" not in message["data"]


@pytest.mark.parametrize(
    "request_",
    [
        {
            "type": "ask_user_question",
            "question": "Which account should I use?",
            "options": ["IRA", "Brokerage"],
            "allow_multiple": False,
        },
        {"type": "credit_pause", "message": "You have used this month's credits."},
    ],
    ids=["question", "credit_pause"],
)
async def test_replay_drops_every_interrupt(client, request_):
    """An interrupt asks the owner, and no share renders or answers one."""
    interrupt = {
        "event": "interrupt",
        "data": {
            "interrupt_id": "int-2",
            "action_requests": [request_],
            "role": "assistant",
            "finish_reason": "interrupt",
        },
    }
    before = {
        "event": "message_chunk",
        "data": {"content_type": "text", "content": "Checking."},
    }
    after = {
        "event": "message_chunk",
        "data": {"content_type": "text", "content": "Done."},
    }
    body, events = await _replay(client, [before, interrupt, after])
    assert [e["event"] for e in events] == [
        "user_message",
        "message_chunk",
        "message_chunk",
        "replay_done",
    ]
    assert request_["type"] not in body
    ids = [line[len("id: ") :] for line in body.splitlines() if line.startswith("id: ")]
    assert ids == ["1", "2", "3", "4"]


async def test_replay_keeps_a_wakeup_on_an_order_to_its_label(client):
    """A wakeup on an order names the order and its fill wherever it reaches a
    turn: the wake turn's own prompt, the notice on its query row and on a
    running turn's steering entry, and the agent's ``schedule_wakeup`` call
    and answer. A viewer keeps what names the row: the label and the kind."""
    order = {
        "vendor_order_id": "900104",
        "summary": "Buy 7 AAPL",
        "outcome": "filled",
        "filled_qty": 7,
        "qty": 7,
        "avg_fill_price": 187.25,
        "currency": "USD",
        "reason": None,
    }
    notice = {
        "wakeup_id": "w-1",
        "short_id": "w_ab12",
        "label": "AAPL buy fill",
        "kind": "order",
        "notice": "fire",
        "order": order,
    }
    steered = {
        "event": "steering_delivered",
        "data": {"messages": [{"content": "", "wakeup": notice, "timestamp": 1.0}]},
    }
    set_call = _call(
        "schedule_wakeup",
        {"triggers": [{"order": "900104", "label": "AAPL buy fill"}]},
    )
    set_answer = {
        "event": "tool_call_result",
        "data": {
            "tool_call_id": "call_abc",
            "content": "- w_ab12 (AAPL buy fill): order 900104, Buy 7 AAPL.",
            "artifact": {"quiet": False, "changed": True},
        },
    }
    body, events = await _replay(
        client,
        [steered, set_call, set_answer],
        later_events=[],
        later_metadata={"wakeup": {"v": 1, "notices": [notice]}},
        later_query={
            "type": "wakeup",
            "content": "<wakeup>order 900104 filled 7 at 187.25</wakeup>",
        },
    )
    for value in ("900104", "187.25", "Buy 7 AAPL"):
        assert value not in body
    wake = [e for e in events if e["event"] == "user_message"][1]
    assert wake["data"]["content"] == ""
    shown = {k: v for k, v in notice.items() if k != "order"}
    assert wake["data"]["metadata"]["wakeup"] == {"v": 1, "notices": [shown]}
    entry = next(e for e in events if e["event"] == "steering_delivered")
    assert entry["data"]["messages"][0]["wakeup"] == shown
    call = next(e for e in events if e["event"] == "tool_calls")
    assert call["data"]["tool_calls"][0]["args"] == {
        "triggers": [{"label": "AAPL buy fill"}]
    }
