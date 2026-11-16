"""A wake whose computer cannot be reached is held, not posted.

Posted while the user's machine sleeps, the wake turn would fail at its
connect after its notice was stamped delivered, and nothing would deliver it
again.
"""

from __future__ import annotations

import contextlib
from datetime import datetime, timezone
from unittest.mock import AsyncMock, patch

import pytest

from src.server.services.wakeups import delivery

from .rows import THREAD_ID, WAKEUP_ID

_WS = "22222222-2222-2222-2222-222222222222"


def _job() -> dict:
    return {
        "hook_outbox_id": "44444444-4444-4444-4444-444444444444",
        "attempts": 1,
        "conversation_thread_id": THREAD_ID,
        "created_at": datetime(2026, 11, 10, tzinfo=timezone.utc),
        "payload": {"wakeup_id": WAKEUP_ID, "notice": "fire", "seq": 3, "user_id": "u-1"},
    }


@contextlib.contextmanager
def _env(*, away: bool):
    notify = "src.server.services.report_back.notify_turn"
    park = AsyncMock(return_value=away)
    post = AsyncMock(return_value=("dispatched", "run-1"))
    settled = AsyncMock()
    with (
        patch("src.server.database.runs.lifecycle.get_latest_attempt", AsyncMock(return_value=None)),
        patch("src.server.database.wakeups.fire.job_owed", AsyncMock(return_value=True)),
        patch.object(delivery, "showing_run", AsyncMock(return_value=None)),
        patch(
            "src.server.database.conversation.get_thread_by_id",
            AsyncMock(return_value={"workspace_id": _WS, "archived_at": None}),
        ),
        patch("src.server.database.workspace.get_workspace", AsyncMock(return_value={"workspace_id": _WS})),
        patch("src.server.services.features.user_feature_enabled", AsyncMock(return_value=True)),
        patch("src.server.database.runs.outbox.extend_job_lease", AsyncMock(return_value=True)),
        patch(f"{notify}.park_while_thread_busy", AsyncMock(return_value=False)),
        patch(f"{notify}.park_while_computer_away", park),
        patch(
            "src.server.database.wakeups.riders.gather_wakeup_riders",
            AsyncMock(return_value=[]),
        ),
        patch.object(
            delivery, "render_wake", AsyncMock(return_value=("<wakeup/>", [{"seq": 3}]))
        ),
        patch(f"{notify}.post_notification_turn", post),
        patch.object(delivery, "_settle_wake", settled),
    ):
        yield park, post, settled


def _wake() -> delivery._Wake:
    return delivery._Wake(
        job=_job(), thread_id=THREAD_ID, wakeup_id=WAKEUP_ID, subject="fire 3"
    )


@pytest.mark.asyncio
async def test_a_wake_waits_for_its_computer_unread():
    with _env(away=True) as (park, post, settled):
        exit_ = await delivery._deliver(_wake())

    assert exit_ == "parked"
    assert park.await_args.kwargs["workspace_id"] == _WS
    post.assert_not_awaited()
    # Unread, so a series' slots due meanwhile are skipped, not delivered.
    settled.assert_not_awaited()


@pytest.mark.asyncio
async def test_a_wake_whose_computer_answers_is_posted():
    with _env(away=False) as (_, post, _settled):
        exit_ = await delivery._deliver(_wake())

    assert exit_ == "settled"
    post.assert_awaited_once()
