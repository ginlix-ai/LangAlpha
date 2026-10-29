"""Notices that land together read as one ``<wakeup>`` block: inline in a
running turn (with no quiet hint, since a user is there to answer) and as a
wake turn's own message (with it)."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest

from ptc_agent.agent.middleware.steering import SteeredReport, SteeringMiddleware
from src.server.services.report_back.steer import SteeredReportInbox
from src.server.services.wakeups.inline import inline_block
from src.tools.wakeups.wording import render_wake_block

NOTE = "Automated wakeup you set with schedule_wakeup (not a user message)."
HINT = (
    "This turn started on its own: if it finds nothing the user needs to hear, end it with"
    ' schedule_wakeup(quiet="<what you checked and found>") instead of replying.'
)
LINES = [
    "NVDA pullback: Check 7 of 24 of the series you set at 09:57 EDT. Next check at 10:35 EDT.",
    "NVDA breakdown: NVDA hit your trigger (below $200.00): it traded at $199.85 at 10:31 EDT.",
]


def test_several_notices_share_one_block_with_no_quiet_hint():
    block = inline_block(LINES)
    assert block == "\n".join(["<wakeup>", NOTE, *LINES, "</wakeup>"])
    assert block.count("<wakeup>") == 1
    assert "quiet" not in block


def test_a_turn_the_wakeup_started_adds_the_quiet_hint_before_the_close():
    block = render_wake_block(LINES, started_turn=True)
    assert block == "\n".join(["<wakeup>", NOTE, *LINES, HINT, "</wakeup>"])


def test_the_report_inbox_renders_a_wakeup_group_as_that_block():
    assert SteeredReportInbox().render_group("wakeup", LINES) == inline_block(LINES)


def _notice(job_id: str, line: str, short_id: str) -> SteeredReport:
    return SteeredReport(
        job_id=job_id,
        content=line,
        kind="wakeup",
        shown={"wakeup": {"short_id": short_id}},
    )


@pytest.mark.asyncio
async def test_a_model_call_takes_two_notices_in_as_one_block_ahead_of_the_steer():
    steer = SteeredReport(
        job_id="j3",
        content="also check AMD",
        kind="steer",
        sender="user",
        timestamp=1.0,
    )
    inbox = SteeredReportInbox()
    inbox.take = AsyncMock(
        return_value=[
            _notice("j1", LINES[0], "w_3f2a"),
            _notice("j2", LINES[1], "w_91c0"),
            steer,
        ]
    )
    inbox.confirm = AsyncMock()
    inbox.take_resumes = AsyncMock(return_value=[])
    config = {"configurable": {"thread_id": "t-1"}, "metadata": {"run_id": "run-A"}}

    result = await SteeringMiddleware(inbox).abefore_model(
        {}, MagicMock(), config=config
    )

    (message,) = result["messages"]
    assert message.content.startswith(inline_block(LINES) + "\n\n")
    assert message.content.count("<wakeup>") == 1
    assert message.content.index("</wakeup>") < message.content.index("also check AMD")
    delivered = message.additional_kwargs["steering_delivered"]
    # One entry per notice, each carrying its own WakeNotice for the reader's row.
    assert [m.get("wakeup") for m in delivered["messages"][:2]] == [
        {"short_id": "w_3f2a"},
        {"short_id": "w_91c0"},
    ]
    assert delivered["count"] == 1
    assert message.additional_kwargs["held_jobs"]["job_ids"] == ["j1", "j2", "j3"]
