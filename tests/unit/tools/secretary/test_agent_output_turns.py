"""The ``turns`` arg must flow agent_output -> extract_text_from_thread.

agent_output exposes an optional ``turns`` window (default 1 = latest turn).
These pin that the value reaches the extraction layer instead of being
silently dropped at the tool boundary.
"""

from __future__ import annotations

import json
from unittest.mock import AsyncMock, patch

import pytest

from src.tools.secretary.chief_of_staff import agent_output

USER_ID = "u-1"
THREAD_ID = "33333333-3333-3333-3333-333333333333"


def _extract_stub():
    """AsyncMock standing in for extract_text_from_thread."""
    return AsyncMock(
        return_value={
            "text": "ok",
            "status": "completed",
            "thread_id": THREAD_ID,
            "workspace_id": "",
        }
    )


def _payload(result) -> dict:
    return json.loads(result.update["messages"][0].content)


@pytest.mark.asyncio
async def test_agent_output_defaults_to_one_turn():
    extract = _extract_stub()
    with patch(
        "src.tools.secretary.chief_of_staff.verify_thread_owner", AsyncMock(return_value=None)
    ), patch(
        "src.tools.secretary.utils.extract_text_from_thread", extract
    ):
        await agent_output.ainvoke(
            {"name": "agent_output", "args": {"thread_id": THREAD_ID},
             "id": "c1", "type": "tool_call"},
            config={"configurable": {"user_id": USER_ID}},
        )

    extract.assert_awaited_once_with(THREAD_ID, 1, timezone="UTC")


@pytest.mark.asyncio
async def test_agent_output_forwards_turns():
    extract = _extract_stub()
    with patch(
        "src.tools.secretary.chief_of_staff.verify_thread_owner", AsyncMock(return_value=None)
    ), patch(
        "src.tools.secretary.utils.extract_text_from_thread", extract
    ):
        await agent_output.ainvoke(
            {"name": "agent_output", "args": {"thread_id": THREAD_ID, "turns": 5},
             "id": "c2", "type": "tool_call"},
            config={"configurable": {"user_id": USER_ID, "timezone": "Asia/Tokyo"}},
        )

    extract.assert_awaited_once_with(THREAD_ID, 5, timezone="Asia/Tokyo")


@pytest.mark.asyncio
async def test_agent_output_surfaces_read_failure_as_error():
    """A read failure becomes an error payload, not an empty success result."""
    extract = AsyncMock(side_effect=RuntimeError("db down"))
    with patch(
        "src.tools.secretary.chief_of_staff.verify_thread_owner", AsyncMock(return_value=None)
    ), patch(
        "src.tools.secretary.utils.extract_text_from_thread", extract
    ):
        result = await agent_output.ainvoke(
            {"name": "agent_output", "args": {"thread_id": THREAD_ID},
             "id": "c4", "type": "tool_call"},
            config={"configurable": {"user_id": USER_ID}},
        )

    assert _payload(result)["success"] is False

