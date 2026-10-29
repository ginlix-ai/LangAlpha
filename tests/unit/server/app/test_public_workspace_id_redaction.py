"""A shared replay never carries the owner's account ids, however deep they sit."""

from __future__ import annotations

import json

import pytest

from tests.unit.server.app import test_public_order_privacy as replay_suite

_replay = replay_suite._replay
client = replay_suite.client

pytestmark = pytest.mark.asyncio

_OWNER_WS = "0f1e2d3c-4b5a-4968-8776-655443322110"

# The account tools answer with the owner's own rows, serialized into the tool
# message's text, where no key-based strip reaches them. This turn is the
# retired Flash agent's, whose threads are still shared.
_DISPATCHED_WS = "7a6b5c4d-3e2f-4a1b-9c8d-7e6f5a4b3c2d"
_DISPATCHED_THREAD = "8b7c6d5e-4f3a-4b2c-8d1e-0f9a8b7c6d5e"
_OWNER_USER = "user-2f9e8d7c"
_OWNER_SANDBOX = "sbx-3c4d5e6f"
_OWNER_COMPUTER = "cmp-9a8b7c6d"
_ACCOUNT_VALUES = (
    _OWNER_WS,
    _DISPATCHED_WS,
    _OWNER_USER,
    _OWNER_SANDBOX,
    _OWNER_COMPUTER,
)

_WORKSPACE_ROW = {
    "workspace_id": _OWNER_WS,
    "user_id": _OWNER_USER,
    "name": "Semiconductors",
    "description": "Chip makers",
    "sandbox_id": _OWNER_SANDBOX,
    "computer_id": _OWNER_COMPUTER,
    "dir_name": "semiconductors",
    "config": {"sandbox_id": _OWNER_SANDBOX},
    "status": "running",
}

_DISPATCH_TURN = [
    {
        "event": "tool_calls",
        "data": {
            "id": "msg-flash-1",
            "tool_calls": [
                {
                    "name": "manage_workspaces",
                    "args": {"action": "list"},
                    "id": "call_list",
                    "type": "tool_call",
                }
            ],
        },
    },
    {
        "event": "tool_call_result",
        "data": {
            "tool_call_id": "call_list",
            "content": json.dumps(
                {"success": True, "workspaces": [_WORKSPACE_ROW], "total": 1}
            ),
            "content_type": "text",
            "status": "success",
        },
    },
    {"event": "message", "data": {"content": "Dispatching a research run."}},
    {
        "event": "steering_delivered",
        "data": {
            "count": 1,
            "messages": [
                {"content": "Focus on margins", "user_id": _OWNER_USER, "timestamp": 1.0}
            ],
            "timestamp": 1.0,
        },
    },
    {
        "event": "tool_calls",
        "data": {
            "id": "msg-flash-2",
            "tool_calls": [
                {
                    "name": "ptc_agent",
                    "args": {"question": "NVDA vs AMD", "workspace_id": _OWNER_WS},
                    "id": "call_ptc",
                    "type": "tool_call",
                }
            ],
        },
    },
    {
        "event": "interrupt",
        "data": {
            "interrupt_id": "int-ptc",
            "action_requests": [
                {
                    "type": "ptc_agent",
                    "workspace_id": _OWNER_WS,
                    "workspace_name": "Semiconductors",
                    "thread_id": None,
                    "question": "NVDA vs AMD",
                    "report_back": True,
                    "tool_call_id": "call_ptc",
                }
            ],
            "role": "assistant",
            "finish_reason": "interrupt",
        },
    },
]

_DISPATCH_RESULT = {
    "event": "tool_call_result",
    "data": {
        "tool_call_id": "call_ptc",
        "content": json.dumps(
            {
                "success": True,
                "workspace_id": _DISPATCHED_WS,
                "thread_id": _DISPATCHED_THREAD,
                "status": "dispatched",
                "report_back": True,
            }
        ),
        "content_type": "text",
        "status": "success",
    },
}


async def test_replay_carries_no_account_values_from_the_secretary_tools(client):
    body, events = await _replay(client, _DISPATCH_TURN, later_events=[_DISPATCH_RESULT])
    for value in _ACCOUNT_VALUES:
        assert value not in body
    assert _DISPATCHED_THREAD not in body
    # Each call still lands as a card the share hides, and the rest reads.
    assert [e["event"] for e in events] == [
        "user_message",
        "tool_calls",
        "tool_call_result",
        "message",
        "steering_delivered",
        "tool_calls",
        "user_message",
        "tool_call_result",
        "replay_done",
    ]
    message = next(e for e in events if e["event"] == "message")
    assert message["data"]["content"] == "Dispatching a research run."


async def test_replay_blanks_the_report_back_prompt_naming_the_dispatch(client):
    report_back = (
        "<system>\nThe analysis you dispatched (thread "
        f"{_DISPATCHED_THREAD} in workspace {_DISPATCHED_WS}) has completed. "
        "Use agent_output to retrieve and summarize the results for the user.\n"
        "</system>"
    )
    read_output = [
        {
            "event": "tool_calls",
            "data": {
                "id": "msg-flash-4",
                "tool_calls": [
                    {
                        "name": "agent_output",
                        "args": {"thread_id": _DISPATCHED_THREAD},
                        "id": "call_output",
                        "type": "tool_call",
                    }
                ],
            },
        },
        {
            "event": "tool_call_result",
            "data": {"tool_call_id": "call_output", "content": "NVDA leads."},
        },
        {"event": "message", "data": {"content": "NVDA leads on share."}},
    ]
    body, events = await _replay(
        client,
        [*_DISPATCH_TURN[4:], _DISPATCH_RESULT],
        later_events=read_output,
        later_query={"type": "system", "content": report_back},
    )
    assert _DISPATCHED_WS not in body
    assert _DISPATCHED_THREAD not in body
    wake = [e for e in events if e["event"] == "user_message"][1]["data"]
    # The tag is what hides the bubble on the share.
    assert wake["query_type"] == "system"
    assert wake["content"] == ""
    message = next(e for e in events if e["event"] == "message")
    assert message["data"]["content"] == "NVDA leads on share."
