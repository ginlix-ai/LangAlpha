"""The Chief of Staff stops a thread's turn the way the stop button does.

A stop that names its run never ends another one: a hand-off that finished
leaves the thread free for a turn the user starts there, and a late stop for
that hand-off must find nothing to end rather than ending the user's turn.
"""

from __future__ import annotations

import json
from unittest.mock import AsyncMock, patch

import pytest
from fastapi import HTTPException

from src.tools.secretary._commands import error_command
from src.tools.secretary.chief_of_staff import chief_of_staff_threads
from src.tools.secretary.tools import _STOPPING

USER_ID = "user-1"
HOME_THREAD_ID = "22222222-2222-2222-2222-222222222222"
THREAD_ID = "11111111-1111-1111-1111-111111111111"
RUN_ID = "55555555-5555-5555-5555-555555555555"

_CANCELLED = {
    "cancelled": True,
    "thread_id": THREAD_ID,
    "message": "Cancellation signal sent. The turn will stop shortly.",
}


async def _stop(cancel: AsyncMock, owner_err=None, **args) -> dict:
    with (
        patch(
            "src.tools.secretary.tools.verify_thread_owner",
            AsyncMock(return_value=owner_err),
        ),
        patch("src.server.services.cancel_dispatch.cancel_workflow", cancel),
    ):
        result = await chief_of_staff_threads.ainvoke(
            {
                "name": "manage_threads",
                "args": {"action": "stop", **args},
                "id": "call_1",
                "type": "tool_call",
            },
            config={"configurable": {"user_id": USER_ID, "thread_id": HOME_THREAD_ID}},
        )
    return json.loads(result.update["messages"][0].content)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "args, error",
    [
        ({"thread_id": HOME_THREAD_ID}, "end your turn instead"),
        ({"thread_id": "not-a-thread"}, "thread not found"),
        ({"thread_id": THREAD_ID, "run_id": "the NVDA run"}, "run_id is not a run id"),
    ],
    ids=["its own thread", "non-UUID thread_id", "non-UUID run_id"],
)
async def test_a_stop_it_cannot_aim_is_refused_before_anything_is_cancelled(
    args, error
):
    cancel = AsyncMock(side_effect=AssertionError("nothing may be cancelled"))
    payload = await _stop(cancel, **args)

    assert payload["success"] is False
    assert error in payload["error"]


@pytest.mark.asyncio
async def test_a_thread_the_user_does_not_own_is_not_stopped():
    cancel = AsyncMock(side_effect=AssertionError("nothing may be cancelled"))
    denied = error_command("thread not found or not owned by user", "call_1")
    payload = await _stop(cancel, owner_err=denied, thread_id=THREAD_ID)

    assert payload == {
        "success": False,
        "error": "thread not found or not owned by user",
    }


@pytest.mark.asyncio
async def test_a_stop_names_its_run_and_tells_the_model_to_wait_for_the_report():
    cancel = AsyncMock(return_value=_CANCELLED)
    payload = await _stop(cancel, thread_id=THREAD_ID, run_id=RUN_ID.upper())

    cancel.assert_awaited_once_with(THREAD_ID, run_id=RUN_ID, by_user=False)
    assert payload == {"success": True, "cancelled": True, "message": _STOPPING}


@pytest.mark.asyncio
async def test_without_a_run_id_the_stop_takes_whatever_the_thread_runs():
    cancel = AsyncMock(return_value=_CANCELLED)
    await _stop(cancel, thread_id=THREAD_ID, run_id="")

    cancel.assert_awaited_once_with(THREAD_ID, run_id=None, by_user=False)


@pytest.mark.asyncio
async def test_a_run_that_already_finished_is_reported_not_stopped():
    cancel = AsyncMock(
        return_value={
            "cancelled": False,
            "thread_id": THREAD_ID,
            "state": "already_finished",
            "message": "Run already finished; nothing to cancel.",
        }
    )
    payload = await _stop(cancel, thread_id=THREAD_ID, run_id=RUN_ID)

    assert payload == {
        "success": True,
        "cancelled": False,
        "state": "already_finished",
        "message": "Run already finished; nothing to cancel.",
    }


@pytest.mark.asyncio
async def test_a_cancel_failure_is_stop_failed():
    cancel = AsyncMock(
        side_effect=HTTPException(status_code=500, detail="Failed to cancel the turn.")
    )
    payload = await _stop(cancel, thread_id=THREAD_ID)

    assert payload == {"success": False, "error": "stop_failed"}


@pytest.mark.asyncio
async def test_the_users_steers_never_reach_the_chief_of_staff():
    """They are the user's to resend; in a tool result the model would act on
    them or drop them."""
    cancel = AsyncMock(
        return_value={
            "cancelled": False,
            "thread_id": THREAD_ID,
            "state": "already_finished",
            "message": "Run already finished; nothing to cancel.",
            "returned_steers": [{"content": "also cover bonds"}],
        }
    )
    payload = await _stop(cancel, thread_id=THREAD_ID, run_id=RUN_ID)

    assert "returned_steers" not in payload
    cancel.assert_awaited_once_with(THREAD_ID, run_id=RUN_ID, by_user=False)
