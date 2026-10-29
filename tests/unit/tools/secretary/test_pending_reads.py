"""A read of a thread mid-run is marked on its artifact, for the loop guard.

The guard cannot tell a poll from progress by the text, since a live run's
text grows between reads, so the tool says it on the result's artifact, which
the model never sees. A listing is marked only for a thread other than the
caller's own, which is always running while it lists.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, patch

import pytest

from ptc_agent.agent.limits import is_pending_read
from src.tools.secretary.chief_of_staff import agent_output, chief_of_staff_threads

pytestmark = pytest.mark.asyncio

USER_ID = "u-1"
OWN = "11111111-1111-1111-1111-111111111111"
OTHER = "22222222-2222-2222-2222-222222222222"


def _result(command):
    return command.update["messages"][0]


async def _read(tool, status: str, args: dict):
    extract = AsyncMock(
        return_value={"text": "so far", "status": status, "thread_id": OTHER, "workspace_id": ""}
    )
    with patch(
        "src.tools.secretary.chief_of_staff.verify_thread_owner", AsyncMock(return_value=None)
    ), patch("src.tools.secretary.utils.extract_text_from_thread", extract):
        return _result(
            await tool.ainvoke(
                {"name": tool.name, "args": args, "id": "c1", "type": "tool_call"},
                config={"configurable": {"user_id": USER_ID, "thread_id": OWN}},
            )
        )


async def test_a_read_of_a_running_thread_is_pending_and_a_settled_one_is_not():
    running = await _read(agent_output, "running", {"thread_id": OTHER})
    settled = await _read(agent_output, "completed", {"thread_id": OTHER})

    assert is_pending_read(running)
    assert not is_pending_read(settled)
    # Only the artifact differs: what the model reads is the tool's own JSON.
    assert running.content.replace("running", "completed") == settled.content


async def _list(tool, rows: list[dict]):
    with patch(
        "src.server.database.conversation.threads_read.get_threads_for_user",
        AsyncMock(return_value=(rows, len(rows))),
    ):
        return _result(
            await tool.ainvoke(
                {"name": tool.name, "args": {"action": "list"}, "id": "c1", "type": "tool_call"},
                config={"configurable": {"user_id": USER_ID, "thread_id": OWN}},
            )
        )


@pytest.mark.parametrize(
    ("other_status", "pending"),
    [("in_progress", True), ("active", True), ("completed", False), ("interrupted", False)],
)
async def test_a_listing_is_pending_only_while_another_thread_runs(other_status, pending):
    rows = [
        # The caller's own thread, which is always mid-run while it lists, and
        # spelled as the database returns it rather than as the config does.
        {"conversation_thread_id": OWN.upper(), "current_status": "in_progress"},
        {"conversation_thread_id": OTHER, "current_status": other_status},
    ]
    assert is_pending_read(await _list(chief_of_staff_threads, rows)) is pending
