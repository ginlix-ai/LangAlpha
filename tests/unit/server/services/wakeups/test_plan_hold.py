"""A wake the Claude plan refused is held until a minute past the reset rather
than for the thread's next turn, unless an earlier POST of it was admitted."""

from __future__ import annotations

from datetime import datetime
from unittest.mock import AsyncMock

import pytest

from src.server.database import conversation as conv_db
from src.server.database import workspace as ws_db
from src.server.database.runs import lifecycle as tl_db
from src.server.database.runs import outbox as outbox_db
from src.server.database.wakeups import fire as fire_db
from src.server.database.wakeups import riders as riders_db
from src.server.services import features
from src.server.services.hook_outbox import MAX_ATTEMPTS
from src.server.services.report_back import notify_turn
from src.server.services.wakeups import delivery
from tests.unit.server.services.wakeups.rows import THREAD_ID, UTC, WAKEUP_ID

JOB = {
    "hook_outbox_id": "66666666-6666-6666-6666-666666666666",
    "hook_type": "wakeup",
    "attempts": 1,
    "created_at": datetime(2026, 11, 8, 2, 40, tzinfo=UTC),
    "payload": {
        "wakeup_id": WAKEUP_ID,
        "notice": "fire",
        "seq": 1,
        "user_id": "u_test",
    },
}
RESET = "2026-11-08T02:43:31+00:00"


@pytest.fixture
def refused(monkeypatch):
    async def post(**kw):
        await kw["on_plan_limited"](
            {
                "held_until": RESET,
                "held_known": False,
                "held_credential": "saved",
                "held_model": "claude-haiku-5-5",
                "window": None,
                "reason": "plan_limited",
            }
        )
        return "plan_limited", None

    monkeypatch.setattr(tl_db, "get_latest_attempt", AsyncMock(return_value=None))
    monkeypatch.setattr(fire_db, "job_owed", AsyncMock(return_value=True))
    monkeypatch.setattr(delivery, "showing_run", AsyncMock(return_value=None))
    monkeypatch.setattr(
        conv_db,
        "get_thread_by_id",
        AsyncMock(
            return_value={
                "workspace_id": "77777777-7777-7777-7777-777777777777",
                "archived_at": None,
            }
        ),
    )
    monkeypatch.setattr(
        ws_db, "get_workspace", AsyncMock(return_value={"workspace_id": "w"})
    )
    monkeypatch.setattr(features, "user_feature_enabled", AsyncMock(return_value=True))
    monkeypatch.setattr(outbox_db, "extend_job_lease", AsyncMock(return_value=True))
    monkeypatch.setattr(
        notify_turn, "park_while_thread_busy", AsyncMock(return_value=False)
    )
    monkeypatch.setattr(riders_db, "gather_wakeup_riders", AsyncMock(return_value=[]))
    monkeypatch.setattr(
        delivery, "render_wake", AsyncMock(return_value=("<wakeup/>", [{"seq": 1}]))
    )
    monkeypatch.setattr(notify_turn, "post_notification_turn", post)
    hold = AsyncMock(return_value=True)
    monkeypatch.setattr(outbox_db, "hold_alert_for_plan", hold)
    return hold


def _wake() -> delivery._Wake:
    return delivery._Wake(
        job=JOB, thread_id=THREAD_ID, wakeup_id=WAKEUP_ID, subject="fire 1"
    )


@pytest.mark.asyncio
async def test_the_wake_is_held_until_the_plan_resets(monkeypatch, refused):
    monkeypatch.setattr(delivery, "_adopt_own_wake", AsyncMock(return_value=False))

    assert await delivery._deliver(_wake()) == "held"
    refused.assert_awaited_once_with(
        JOB["hook_outbox_id"],
        attempts=1,
        plan_hold={
            "reset_at": RESET, "window": None, "known": False, "credential": "saved", "model_id": "claude-haiku-5-5",
        },
    )


@pytest.mark.asyncio
async def test_a_wake_an_earlier_post_started_is_settled_to_it_instead(
    monkeypatch, refused
):
    monkeypatch.setattr(delivery, "_adopt_own_wake", AsyncMock(return_value=True))

    assert await delivery._deliver(_wake()) == "settled"
    refused.assert_not_awaited()


@pytest.mark.asyncio
async def test_a_hold_whose_lease_was_lost_leaves_the_job_to_its_new_owner(
    monkeypatch, refused
):
    monkeypatch.setattr(delivery, "_adopt_own_wake", AsyncMock(return_value=False))
    refused.return_value = False

    assert await delivery._deliver(_wake()) == "fence_lost"


HELD = {
    **JOB,
    "payload": {
        **JOB["payload"],
        "exhausted": True,
        "plan_hold": {"reset_at": RESET, "window": "five_hour", "known": True},
    },
}


@pytest.mark.asyncio
@pytest.mark.parametrize("attempts", [1, MAX_ATTEMPTS])
async def test_a_held_wake_that_came_due_gets_a_fresh_wake_s_retries(
    monkeypatch, refused, attempts
):
    # Its hold ends at the claim, so a refusal that lifts by itself is
    # retried under backoff rather than held for the thread's next turn,
    # however many tries it took before the hold.
    ended = AsyncMock(return_value=True)
    exhaust = AsyncMock(return_value=True)
    monkeypatch.setattr(outbox_db, "end_plan_hold", ended)
    monkeypatch.setattr(outbox_db, "exhaust_alert", exhaust)

    async def capped(**kw):
        return "cap", None

    monkeypatch.setattr(notify_turn, "post_notification_turn", capped)
    wake = delivery._Wake(
        job={**HELD, "attempts": attempts}, thread_id=THREAD_ID, wakeup_id=WAKEUP_ID, subject="fire 1"
    )

    with pytest.raises(RuntimeError, match="not admitted"):
        await delivery._deliver(wake)
    ended.assert_awaited_once_with(HELD["hook_outbox_id"], attempts=attempts)
    exhaust.assert_not_awaited()


@pytest.mark.asyncio
async def test_a_wake_with_no_room_for_another_claude_code_turn_asks_again_by_the_clock(
    monkeypatch, refused
):
    # Room opens as the user's turns on other threads end, which release
    # nothing on this one, so it is neither held for this thread's next turn
    # nor spent at its attempts ceiling.
    room = AsyncMock(return_value=True)
    exhaust = AsyncMock(return_value=True)
    unfreeze = AsyncMock()
    rider = {**JOB, "hook_outbox_id": "88888888-8888-8888-8888-888888888888"}
    monkeypatch.setattr(outbox_db, "wait_for_room", room)
    monkeypatch.setattr(outbox_db, "exhaust_alert", exhaust)
    monkeypatch.setattr(riders_db, "gather_wakeup_riders", AsyncMock(return_value=[rider]))
    monkeypatch.setattr(riders_db, "unfreeze_wakeup_riders", unfreeze)

    async def full(**kw):
        assert kw["room_fails_fast"]
        return "full", None

    monkeypatch.setattr(notify_turn, "post_notification_turn", full)
    wake = delivery._Wake(
        job={**JOB, "attempts": MAX_ATTEMPTS}, thread_id=THREAD_ID, wakeup_id=WAKEUP_ID, subject="fire 1"
    )

    assert await delivery._deliver(wake) == "parked"
    room.assert_awaited_once_with(JOB["hook_outbox_id"], attempts=MAX_ATTEMPTS, wait_s=notify_turn.TURN_ROOM_WAIT_S)
    unfreeze.assert_awaited_once_with(JOB["hook_outbox_id"], attempts=MAX_ATTEMPTS)
    assert wake.riders == []
    exhaust.assert_not_awaited()
    refused.assert_not_awaited()


@pytest.mark.asyncio
async def test_a_held_wake_whose_lease_was_lost_at_the_claim_leaves_it_to_its_new_owner(
    monkeypatch, refused
):
    monkeypatch.setattr(outbox_db, "end_plan_hold", AsyncMock(return_value=False))
    wake = delivery._Wake(
        job=HELD, thread_id=THREAD_ID, wakeup_id=WAKEUP_ID, subject="fire 1"
    )

    assert await delivery._deliver(wake) == "fence_lost"
    refused.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("still_held", [False, True])
async def test_a_wake_turn_names_the_hold_its_notice_waited_out_after_the_claim_ended_it(
    monkeypatch, still_held
):
    # Only where the plan let the credential and model that held it through
    # again: one still held ran on usage credits, not after a reset.
    from src.server.services.llm import claude_code

    from datetime import datetime as real_datetime

    from src.server.database.wakeups import read as read_db
    from src.server.services.wakeups import message
    from tests.unit.server.services.wakeups.rows import base_row

    class _Clock(real_datetime):
        @classmethod
        def now(cls, tz=None):
            return real_datetime(2026, 11, 8, 2, 45, tzinfo=UTC)

    fired = real_datetime(2026, 11, 8, 2, 40, tzinfo=UTC)
    row = base_row(
        kind="at",
        timezone="UTC",
        schedule=[fired],
        status="fired",
        fires_done=1,
        last_fired_seq=1,
        last_fired_at=fired,
    )
    monkeypatch.setattr(message, "datetime", _Clock)
    monkeypatch.setattr(read_db, "get_wakeup", AsyncMock(return_value=row))
    monkeypatch.setattr(message, "user_timezone", AsyncMock(return_value="UTC"))
    ended = {
        **JOB,
        "payload": {
            **JOB["payload"],
            "plan_waited": {
                "reset_at": RESET, "window": "five_hour", "credential": "saved", "model_id": "claude-haiku-5-5",
            },
        },
    }
    holds = AsyncMock(return_value=still_held)
    monkeypatch.setattr(claude_code, "plan_still_holds", holds)

    text, _ = await delivery.render_wake([ended], started_turn=True)
    inline, _ = await delivery.render_wake([ended], started_turn=False)

    named = "This wakeup was due at 02:40 UTC. It ran at 02:45 UTC, after the Claude plan's 5-hour limit reset."
    assert (named in text) is not still_held
    assert "Claude plan" not in inline
    holds.assert_awaited_once_with("claude-haiku-5-5", row["user_id"], "saved")
