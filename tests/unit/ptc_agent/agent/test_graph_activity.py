"""The role's harness blocks are read before the build, and only for that role.

An empty mapping is a build without the block, so the analyst does no
database round trip on a workspace turn. A None value is a read that did not
answer, which the baseline retries next turn; the two must not collapse.
"""

from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest

from ptc_agent.agent.graph import _read_harness_blocks

HOME = "a0000001-0000-4000-8000-000000000001"
SESSION = SimpleNamespace(computer_id="c-home")
ROSTER = [{"computer_id": "c-home"}, {"computer_id": "c-mac"}]


def _reads(activity="Recent threads, this one aside:", roster=ROSTER):
    read_roster = AsyncMock(
        side_effect=roster if isinstance(roster, Exception) else None,
        return_value=roster,
    )
    return (
        AsyncMock(return_value=activity),
        read_roster,
        patch("src.server.database.computer.get_computer_roster", read_roster),
    )


@pytest.mark.asyncio
async def test_the_analyst_reads_no_block():
    reader, roster, roster_patch = _reads()
    with patch("src.tools.secretary.activity.read_activity", reader), roster_patch:
        assert await _read_harness_blocks("u", SESSION, HOME, "t", None, "analyst") == ({}, None)
    reader.assert_not_awaited()
    roster.assert_not_awaited()


@pytest.mark.asyncio
async def test_the_chief_of_staff_reads_activity_in_the_users_timezone():
    reader, _, roster_patch = _reads()
    turn = SimpleNamespace(tool_timezone="Asia/Shanghai")
    with patch("src.tools.secretary.activity.read_activity", reader), roster_patch:
        blocks, _ = await _read_harness_blocks("u", SESSION, HOME, "t", turn, "chief_of_staff")
    assert blocks == {"activity": "Recent threads, this one aside:"}
    reader.assert_awaited_once_with(
        "u", home_id=HOME, thread_id="t", timezone="Asia/Shanghai", computers=ROSTER
    )


@pytest.mark.asyncio
async def test_activity_and_the_tools_read_one_roster():
    """A computer added or removed while the build reads must not leave
    ``<activity>`` naming a computer the tools take no argument for."""
    reader, roster, roster_patch = _reads()
    with patch("src.tools.secretary.activity.read_activity", reader), roster_patch:
        _, reach = await _read_harness_blocks("u", SESSION, HOME, "t", None, "chief_of_staff")
    assert reach is not None
    assert roster.await_count == 1
    assert reader.await_args.kwargs["computers"] is ROSTER


@pytest.mark.asyncio
async def test_one_computer_gives_the_tools_no_reach():
    reader, _, roster_patch = _reads(roster=ROSTER[:1])
    with patch("src.tools.secretary.activity.read_activity", reader), roster_patch:
        _, reach = await _read_harness_blocks("u", SESSION, HOME, "t", None, "chief_of_staff")
    assert reach is None


@pytest.mark.asyncio
async def test_a_read_that_did_not_answer_stays_a_hole():
    """The block is kept with no text, so the baseline marks the epoch
    incomplete and the next turn reads again, instead of losing the block."""
    reader, _, roster_patch = _reads(activity=None)
    with patch("src.tools.secretary.activity.read_activity", reader), roster_patch:
        blocks, _ = await _read_harness_blocks("u", SESSION, HOME, "t", None, "chief_of_staff")
    assert blocks == {"activity": None}


@pytest.mark.asyncio
async def test_a_roster_that_did_not_answer_is_a_hole_and_no_reach():
    reader, _, roster_patch = _reads(roster=RuntimeError("db"))
    with patch("src.tools.secretary.activity.read_activity", reader), roster_patch:
        assert await _read_harness_blocks(
            "u", SESSION, HOME, "t", None, "chief_of_staff"
        ) == ({"activity": None}, None)
    reader.assert_not_awaited()
