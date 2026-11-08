"""The state a fired wakeup shows while its notice waits: on the thread's next
turn, on usage, or on the Claude plan's reset, which it names."""

from __future__ import annotations

from datetime import datetime, timedelta

import pytest

from src.server.services.wakeups.listing import wakeup_out
from tests.unit.server.services.wakeups.rows import UTC, base_row

NOW = datetime(2026, 10, 21, 14, 0, tzinfo=UTC)
RESET = NOW + timedelta(hours=2)
HOLD = {"reset_at": RESET.isoformat(), "window": "five_hour", "known": True}


def _fired(**over) -> dict:
    return base_row(
        kind="at",
        timezone="UTC",
        status="fired",
        schedule=[NOW - timedelta(hours=1)],
        fires_done=1,
        last_fired_seq=1,
        last_fired_at=NOW - timedelta(hours=1),
        **over,
    )


@pytest.mark.parametrize(
    ("over", "state", "reset"),
    [
        ({}, "held", None),
        ({"notice_waiting": True}, "held_usage", None),
        ({"notice_waiting": True, "notice_plan_hold": HOLD}, "held_plan", RESET),
        # Claude named no reset: the hold runs to its own bound, which is no reset time.
        ({"notice_waiting": True, "notice_plan_hold": {**HOLD, "known": False}}, "held_plan", None),
        (
            {
                "notice_waiting": True,
                "notice_plan_reset": RESET,
                "notice_too_late": True,
            },
            "held",
            None,
        ),
        ({"notice_plan_hold": HOLD, "delivered_seq": 1}, "fired", None),
    ],
)
def test_a_held_notice_names_what_it_waits_for(over, state, reset):
    out = wakeup_out(_fired(**over), NOW)
    assert (out.state, out.plan_reset_at) == (state, reset)
