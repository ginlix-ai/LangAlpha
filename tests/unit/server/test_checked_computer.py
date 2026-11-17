"""A turn a computer's token sends is held, at its START, to the computer
the approval gate checked it to run on: a workspace moved off that computer
in between refuses the run instead of running it where nobody checked."""

from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest

from src.server.app.threads import messaging
from src.server.database.workspace import WorkspaceMovingError, refuse_moved_off
from src.server.handlers.chat.steering import steer_payload
from src.server.models.chat import ChatRequest
from src.server.services.runs import admission

THREAD = "b2200000-0000-4000-8000-000000000001"
WORKSPACE = "c3300000-0000-4000-8000-000000000001"
CHECKED = "a1100000-0000-4000-8000-000000000001"
MOVED_TO = "a1100000-0000-4000-8000-000000000002"


class _Conn:
    """A connection whose one read answers ``row``."""

    def __init__(self, row):
        self.row = row

    def cursor(self, **_):
        conn = self

        class _Cursor:
            async def __aenter__(self):
                return self

            async def __aexit__(self, *exc):
                return False

            async def execute(self, *args):
                pass

            async def fetchone(self):
                return conn.row

        return _Cursor()


def _say(**fields) -> ChatRequest:
    return ChatRequest(messages=[{"role": "user", "content": "Hi"}], **fields)


def test_a_client_cannot_name_the_computer_it_was_checked_on():
    raw = SimpleNamespace(headers={})
    with patch.object(messaging, "_is_service_request", return_value=False):
        cleared = messaging._client_body(_say(checked_computer_id=CHECKED), raw)

    assert cleared.checked_computer_id is None


@pytest.mark.asyncio
@pytest.mark.parametrize("on", [CHECKED, None])
async def test_a_workspace_still_on_the_checked_computer_or_on_none_starts(on):
    await refuse_moved_off(_Conn({"workspace_id": WORKSPACE, "computer_id": on}), THREAD, CHECKED)


@pytest.mark.asyncio
async def test_a_workspace_moved_off_the_checked_computer_refuses_the_run():
    with pytest.raises(WorkspaceMovingError):
        await refuse_moved_off(
            _Conn({"workspace_id": WORKSPACE, "computer_id": MOVED_TO}), THREAD, CHECKED
        )


@pytest.mark.asyncio
async def test_start_holds_only_a_stamped_turn_to_its_computer():
    refuse = AsyncMock()
    with patch("src.server.database.workspace.refuse_moved_off", refuse):
        await admission._start_checks(_say(), object(), thread_id=THREAD, run_id="r")
        await admission._start_checks(
            _say(checked_computer_id=CHECKED), "conn", thread_id=THREAD, run_id="r"
        )

    refuse.assert_awaited_once_with("conn", THREAD, CHECKED, runs_in=None)


@pytest.mark.asyncio
async def test_a_home_turn_in_the_stand_in_is_held_to_the_stand_ins_computer():
    # Read after the turn's mark, which takes the lock a removal of the
    # stand-in's computer holds.
    order: list[str] = []

    async def refuse(*args, **kwargs):
        order.append(f"refuse:{kwargs['runs_in']}")

    async def mark(conn):
        order.append("mark")

    with patch("src.server.database.workspace.refuse_moved_off", refuse):
        await admission._start_checks(
            _say(checked_computer_id=CHECKED),
            "conn",
            thread_id=THREAD,
            run_id="r",
            then=mark,
            runs_in=WORKSPACE,
        )

    assert order == ["mark", f"refuse:{WORKSPACE}"]


def test_a_parked_send_keeps_the_computer_it_was_checked_on():
    assert steer_payload("also bonds", "u-1", checked_computer_id=CHECKED)[
        "checked_computer_id"
    ] == CHECKED
    assert "checked_computer_id" not in steer_payload("also bonds", "u-1")
