"""What a public share carries of a thread's stored turns.

A share link is readable by anyone who has it, so these pin each owner-only
mark the policy takes out: the owner's account ids at any depth, an order's
identifiers (its call's arguments, the receipt on its artifact, the vendor's
answer, the verdict the owner's resume recorded), the account tools' calls and
answers, and the user_id the user data tools' stored answers still spell out.
"""

from __future__ import annotations

import copy
import json

import pytest

from src.server.services.share_redaction import ShareRedaction

_OWNER_WS = "0f1e2d3c-4b5a-4968-8776-655443322110"
_OWNER_USER = "user-2f9e8d7c"
_ATTEMPT = "11111111-2222-4333-8444-555555555555"

_RECEIPT = {
    "type": "order_receipt",
    "attempt_id": _ATTEMPT,
    "order": {"account_ref": "1234567", "side": "buy"},
    "outcome": {"vendor_order_id": "900104"},
}
_ORDER_STAMP = {
    "server": "moomoo",
    "tool": "sim_trade_input_order",
    "order": {"action": "place", "mode": "paper"},
}
_NO_ORDER_STAMP = {"server": "moomoo", "tool": "get_market_snapshot", "order": None}


def _call(
    name: str,
    args: dict,
    message_id: str | None = "msg-1",
    call_id: str = "call_abc",
) -> dict:
    data: dict = {
        "tool_calls": [{"name": name, "args": args, "id": call_id, "type": "tool_call"}]
    }
    if message_id is not None:
        data["id"] = message_id
    return {"event": "tool_calls", "data": data}


def _answer(content: str, artifact: dict | None = None, call_id: str = "call_abc") -> dict:
    data: dict = {"tool_call_id": call_id, "content": content}
    if artifact is not None:
        data["artifact"] = artifact
    return {"event": "tool_call_result", "data": data}


def _shown(events: list[dict]) -> list[dict]:
    """Each event a share sends, as it sends it, in stream order."""
    redaction = ShareRedaction(events)
    shown = []
    for item in events:
        data = redaction.event(item["event"], item["data"])
        if data is not None:
            shown.append({"event": item["event"], "data": data})
    return shown


def _calls(shown: list[dict]) -> list[dict]:
    return [
        call
        for e in shown
        if e["event"] == "tool_calls"
        for call in e["data"]["tool_calls"]
    ]


def _answers(shown: list[dict]) -> list[str]:
    return [e["data"]["content"] for e in shown if e["event"] == "tool_call_result"]


# --- Events no share carries -------------------------------------------------


@pytest.mark.parametrize(
    "event",
    [
        {"event": "provenance", "data": {"tool_call_id": "call_abc", "args": {"acc_id": "1"}}},
        {
            "event": "tool_call_chunks",
            "data": {"id": "msg-1", "tool_call_chunks": [{"args": "{\"acc_id\": \"1\""}]},
        },
        {
            "event": "interrupt",
            "data": {
                "interrupt_id": "int-1",
                "action_requests": [{"type": "ask_user_question", "question": "Which?"}],
            },
        },
    ],
    ids=["provenance", "tool_call_chunks", "interrupt"],
)
def test_drops_the_events_no_share_carries(event):
    assert ShareRedaction([event]).event(event["event"], event["data"]) is None


def test_never_changes_the_stored_event():
    events = [
        _call("mcp__moomoo__sim_trade_input_order", {"acc_id": "1234567"}),
        _answer("{}", {"direct_mcp": _ORDER_STAMP, "order_receipt": _RECEIPT}),
        {"event": "workspace_status", "data": {"workspace_id": _OWNER_WS, "sandbox_state": "x"}},
    ]
    stored = copy.deepcopy(events)
    _shown(events)
    assert events == stored


# --- The owner's account ids -------------------------------------------------


def test_strips_owner_ids_at_any_depth():
    artifact = {
        "event": "artifact",
        "data": {
            "artifact_type": "chart_annotation",
            "payload": {
                "workspace_id": _OWNER_WS,
                "symbol": "NVDA",
                "annotations": [{"id": "ann-1", "workspace_id": _OWNER_WS}],
            },
        },
    }
    steering = {
        "event": "steering_delivered",
        "data": {
            "count": 1,
            "messages": [{"content": "Focus on margins", "user_id": _OWNER_USER}],
        },
    }
    status = {
        "event": "workspace_status",
        "data": {"status": "ready", "workspace_id": _OWNER_WS, "sandbox_state": "archived"},
    }
    shown = _shown([artifact, steering, status])
    # The card still has what it draws from.
    assert shown[0]["data"]["payload"] == {"symbol": "NVDA", "annotations": [{"id": "ann-1"}]}
    assert shown[1]["data"]["messages"] == [{"content": "Focus on margins"}]
    assert shown[2]["data"] == {"status": "ready"}


def test_strips_owner_ids_and_order_verdicts_from_query_metadata():
    """The resume that answers an approval names each order by its ledger id."""
    redaction = ShareRedaction([])
    content, metadata = redaction.query(
        {
            "content": "approve",
            "metadata": {
                "workspace_id": _OWNER_WS,
                "order_decisions": {_ATTEMPT: {"type": "approve", "message": None}},
                "additional_context": [
                    {"type": "widget", "data": {"workspace_id": _OWNER_WS, "symbol": "NVDA"}}
                ],
                "attachments": [],
            },
        }
    )
    assert content == "approve"
    # One key goes, not the whole dict, so the strip cannot break the shared
    # view's rendering.
    assert metadata == {
        "additional_context": [{"type": "widget", "data": {"symbol": "NVDA"}}],
        "attachments": [],
    }


def test_blanks_the_text_of_a_system_query():
    """A report-back names the dispatched thread and its workspace."""
    content, metadata = ShareRedaction([]).query(
        {
            "type": "system",
            "content": f"The analysis in workspace {_OWNER_WS} completed.",
            "metadata": {
                "msg_type": "ptc",
                "report_source": {
                    "kind": "analyst",
                    "thread_id": "t-1",
                    "workspace_id": _OWNER_WS,
                    "workspace_name": "Chips",
                },
            },
        }
    )
    assert content == ""
    # Its label names the same, so it goes too.
    assert metadata == {"msg_type": "ptc"}


# --- An order's identifiers --------------------------------------------------


def test_strips_the_receipt_and_blanks_the_vendor_answer_of_an_order():
    shown = _shown(
        [
            _call("moomoo__sim_trade_input_order", {"acc_id": "1234567", "qty": 1}),
            _answer(
                "{\"order_id\": \"900104\", \"acc_id\": \"1234567\"}",
                {
                    "direct_mcp": {"server": "moomoo", "tool": "sim_trade_input_order"},
                    "order_receipt": _RECEIPT,
                    "provenance": {"account_ref": "1234567"},
                },
            ),
        ]
    )
    result = shown[1]["data"]
    # The vendor's own names still travel: the receipt is what is private, not
    # the fact that a direct tool ran.
    assert result["artifact"] == {
        "direct_mcp": {"server": "moomoo", "tool": "sim_trade_input_order"}
    }
    assert result["content"] == ""
    assert _calls(shown)[0]["args"] == {}


def test_blanks_an_order_answer_the_ledger_never_receipted():
    """A ledger write that failed leaves no receipt; the stamp still names an order."""
    shown = _shown([_answer("{\"order_id\": \"900104\"}", {"direct_mcp": _ORDER_STAMP})])
    assert _answers(shown) == [""]


def test_empties_an_order_call_named_only_by_its_approval():
    approval = {
        "event": "interrupt",
        "data": {"action_requests": [{"tool_call_id": "call_abc", "attempt_id": _ATTEMPT}]},
    }
    shown = _shown([_call("moomoo__sim_trade_input_order", {"acc_id": "1234567"}), approval])
    assert _calls(shown)[0]["args"] == {}


def test_keeps_a_direct_call_answered_as_no_order():
    """An answer stamped with no order is the one proof a direct call was not one."""
    shown = _shown(
        [
            _call("mcp__moomoo__get_market_snapshot", {"code_list": ["US.AAPL"]}),
            _answer("{\"last_price\": \"318.62\"}", {"direct_mcp": _NO_ORDER_STAMP}),
        ]
    )
    assert _calls(shown)[0]["args"] == {"code_list": ["US.AAPL"]}
    assert _answers(shown) == ["{\"last_price\": \"318.62\"}"]


def test_empties_a_direct_call_that_was_never_answered():
    """A Stop or a lost worker leaves an order with no answer and no approval card."""
    name = "mcp__moomoo__sim_trade_input_order"
    call = _call(name, {"acc_id": "1234567", "code": "US.AAPL", "qty": 5})
    shown = _shown([call])
    assert _calls(shown) == [{**call["data"]["tool_calls"][0], "args": {}}]


def test_empties_an_order_call_that_reuses_a_cleared_call_id():
    """A provider may repeat a call id, so an answer clears only the message that made it.

    The order was stopped before any approval card or answer, with approval off
    for its mode, so nothing else marks it.
    """
    shown = _shown(
        [
            _call("mcp__moomoo__get_market_snapshot", {"code_list": ["US.AAPL"]}, "msg-1"),
            _answer("{\"last_price\": \"318.62\"}", {"direct_mcp": _NO_ORDER_STAMP}),
            _call("mcp__moomoo__sim_trade_input_order", {"acc_id": "1234567"}, "msg-2"),
        ]
    )
    assert [c["args"] for c in _calls(shown)] == [{"code_list": ["US.AAPL"]}, {}]


def test_keeps_each_call_that_reuses_an_id_when_each_is_answered():
    answer = _answer("{\"last_price\": \"318.62\"}", {"direct_mcp": _NO_ORDER_STAMP})
    shown = _shown(
        [
            _call("mcp__moomoo__get_market_snapshot", {"code_list": ["US.AAPL"]}, "msg-1"),
            answer,
            _call("mcp__moomoo__get_market_snapshot", {"code_list": ["US.MSFT"]}, "msg-2"),
            answer,
        ]
    )
    assert [c["args"] for c in _calls(shown)] == [
        {"code_list": ["US.AAPL"]},
        {"code_list": ["US.MSFT"]},
    ]


@pytest.mark.parametrize("message_id", [None, "unknown"], ids=["missing", "unknown"])
def test_empties_a_direct_call_whose_message_has_no_id(message_id):
    """Without a message id an answer cannot be tied to one call, so it clears none.

    ``unknown`` is what the stream writes for a message that came with no id.
    """
    shown = _shown(
        [
            _call("mcp__moomoo__get_market_snapshot", {"code_list": ["US.AAPL"]}, message_id),
            _answer("{\"last_price\": \"318.62\"}", {"direct_mcp": _NO_ORDER_STAMP}),
        ]
    )
    assert _calls(shown)[0]["args"] == {}


def test_empties_a_direct_call_whose_answer_carries_no_stamp():
    """An answer without the binder's stamp says nothing about what the call did."""
    shown = _shown(
        [
            _call("mcp__moomoo__get_market_snapshot", {"code_list": ["US.AAPL"]}),
            _answer("Error: the relay timed out"),
        ]
    )
    assert _calls(shown)[0]["args"] == {}


def test_keeps_the_arguments_of_an_unanswered_call_that_is_not_direct():
    """Only a direct call can place an order, so the rule stops at the prefix."""
    shown = _shown([_call("web_search", {"query": "AAPL earnings date"})])
    assert _calls(shown)[0]["args"] == {"query": "AAPL earnings date"}


# --- The account tools -------------------------------------------------------


def test_sends_each_secretary_call_as_its_name_and_id_alone():
    rows = {"success": True, "workspaces": [{"workspace_id": _OWNER_WS, "sandbox_id": "sbx-1"}]}
    shown = _shown(
        [
            _call("manage_workspaces", {"action": "list"}, "msg-1", "call_list"),
            {
                "event": "tool_call_result",
                "data": {
                    "tool_call_id": "call_list",
                    "content": json.dumps(rows),
                    "status": "success",
                },
            },
            # The retired Flash agent's hand-off, still in the threads it ran.
            _call("ptc_agent", {"question": "NVDA vs AMD"}, "msg-2", "call_ptc"),
            _answer(json.dumps({"thread_id": "t-1"}), call_id="call_ptc"),
        ]
    )
    assert [(c["name"], c["id"], c["args"]) for c in _calls(shown)] == [
        ("manage_workspaces", "call_list", {}),
        ("ptc_agent", "call_ptc", {}),
    ]
    results = [e["data"] for e in shown if e["event"] == "tool_call_result"]
    assert [(r["tool_call_id"], r["content"]) for r in results] == [
        ("call_list", ""),
        ("call_ptc", ""),
    ]
    assert results[0]["status"] == "success"


def test_sends_a_chief_of_staff_hand_off_as_its_name_and_id_alone():
    shown = _shown(
        [
            _call("delegate_to_analyst", {"question": "NVDA vs AMD"}, "msg-1", "call_rb"),
            _answer(json.dumps({"workspace_id": _OWNER_WS, "thread_id": "t-1"}), call_id="call_rb"),
        ]
    )
    assert [(c["name"], c["args"]) for c in _calls(shown)] == [("delegate_to_analyst", {})]
    assert _answers(shown) == [""]


def test_keeps_the_answer_of_a_call_that_reuses_a_secretary_id():
    shown = _shown(
        [
            _call("manage_workspaces", {"action": "list"}, "msg-1", "call_list"),
            _answer("[]", call_id="call_list"),
            _call("get_quote", {"symbol": "NVDA"}, "msg-2", "call_list"),
            _answer("NVDA 181.20", call_id="call_list"),
        ]
    )
    assert _answers(shown) == ["", "NVDA 181.20"]
    assert _calls(shown)[1]["args"] == {"symbol": "NVDA"}


def test_shows_nothing_of_a_second_answer_to_an_id_no_call_renamed():
    # The stream drops a call that repeats an id earlier in its run, so the
    # account tool's answer below arrives under the search's name.
    rows = {"workspaces": [{"workspace_id": _OWNER_WS, "sandbox_id": "sbx-1"}]}
    shown = _shown(
        [
            _call("web_search", {"query": "NVDA"}, "msg-1", "call_0"),
            _answer("NVDA 181.20", call_id="call_0"),
            _answer(json.dumps(rows), call_id="call_0"),
        ]
    )
    assert _answers(shown) == ["NVDA 181.20", ""]


def test_shows_nothing_of_an_answer_whose_call_the_stored_stream_lacks():
    # A run salvaged after its worker was lost keeps the frames it could read,
    # so an account tool's answer can survive the call that named it.
    rows = {"workspaces": [{"workspace_id": _OWNER_WS, "sandbox_id": "sbx-1"}]}
    shown = _shown(
        [
            _answer(json.dumps(rows), call_id="call_list"),
            {"event": "tool_call_result", "data": {"content": json.dumps(rows)}},
            _call("web_search", {"query": "NVDA"}, "msg-1", "call_0"),
            _answer("NVDA 181.20", call_id="call_0"),
        ]
    )
    assert _answers(shown) == ["", "", "NVDA 181.20"]


# --- The user_id a stored answer spells out ----------------------------------


def test_drops_the_user_id_a_stored_user_data_answer_spells_out():
    # The user data tools once answered with raw rows, so a thread stored then
    # holds the owner's user_id inside the answer text, as a Python repr (the
    # rows carry UUID and Decimal values) or as JSON.
    repr_answer = (
        "{'watchlists': [{'watchlist_id': UUID('wl-0'), 'user_id': 'owner-7', "
        "'name': 'Default'}], 'portfolio': [{'symbol': 'NVDA', 'user_id': 'owner-7'}], "
        "'profile': {\"user_id\": \"owner-7\"}}"
    )
    json_answer = json.dumps(
        {"success": True, "watchlist": {"user_id": "owner-7", "watchlist_id": "wl-1"}}
    )
    shown = _shown(
        [
            _call("get_user_data", {}, "msg-1", "call_get"),
            _answer(repr_answer, call_id="call_get"),
            _call("update_user_data", {}, "msg-2", "call_update"),
            _answer(json_answer, call_id="call_update"),
        ]
    )
    got, updated = _answers(shown)
    # The rest of each answer, its row ids included, reads as it did.
    assert got == (
        "{'watchlists': [{'watchlist_id': UUID('wl-0'), 'name': 'Default'}], "
        "'portfolio': [{'symbol': 'NVDA'}], 'profile': {}}"
    )
    assert json.loads(updated) == {"success": True, "watchlist": {"watchlist_id": "wl-1"}}


def test_leaves_another_tools_answer_as_it_reads():
    """Only the user data tools' text is rewritten; any other is its own content."""
    answer = json.dumps({"data": [{"user_id": "gh-octocat", "login": "octocat"}]})
    shown = _shown(
        [
            _call("web_fetch", {"url": "https://api.example.com/users"}),
            _answer(answer),
        ]
    )
    assert _answers(shown) == [answer]
