"""A notice dropped with its wakeup ended keeps its job when the end fails:
the job is the notice's one durable record, and closing it with the wakeup
still open leaves a series skipping every slot but its last."""

from __future__ import annotations

from datetime import datetime
from unittest.mock import AsyncMock

import psycopg
import pytest

from src.server.database import conversation as conv_db
from src.server.database.runs import lifecycle as tl_db
from src.server.database.wakeups import cancel as cancel_db
from src.server.database.wakeups import fire as fire_db
from src.server.database.wakeups import read as read_db
from src.server.services import features
from src.server.services.report_back import steer
from src.server.services.wakeups import delivery
from tests.unit.server.services.wakeups.rows import THREAD_ID, UTC, WAKEUP_ID, base_row

JOB = {
    "hook_outbox_id": "66666666-6666-6666-6666-666666666666",
    "hook_type": "wakeup",
    "attempts": 1,
    "payload": {"wakeup_id": WAKEUP_ID, "notice": "fire", "seq": 1, "user_id": "u_test"},
}


@pytest.fixture
def cancel_fails(monkeypatch):
    failing = AsyncMock(side_effect=psycopg.OperationalError("connection lost"))
    monkeypatch.setattr(cancel_db, "cancel_wakeups", failing)
    monkeypatch.setattr(cancel_db, "cancel_open", failing)
    monkeypatch.setattr(fire_db, "job_owed", AsyncMock(return_value=True))
    return failing


@pytest.mark.asyncio
async def test_a_live_turn_leaves_the_notice_to_its_executor(monkeypatch, cancel_fails):
    monkeypatch.setattr(read_db, "get_wakeup", AsyncMock(return_value=base_row()))
    monkeypatch.setattr(features, "user_feature_enabled", AsyncMock(return_value=False))

    assert await steer._decide(JOB, THREAD_ID) is None
    cancel_fails.assert_awaited_once()


@pytest.mark.asyncio
async def test_delivery_on_an_archived_thread_retries_the_notice(monkeypatch, cancel_fails):
    monkeypatch.setattr(tl_db, "get_latest_attempt", AsyncMock(return_value=None))
    monkeypatch.setattr(delivery, "showing_run", AsyncMock(return_value=None))
    monkeypatch.setattr(
        conv_db,
        "get_thread_by_id",
        AsyncMock(
            return_value={
                "workspace_id": "77777777-7777-7777-7777-777777777777",
                "archived_at": datetime(2026, 10, 20, 15, 0, tzinfo=UTC),
            }
        ),
    )
    wake = delivery._Wake(
        job=JOB, thread_id=THREAD_ID, wakeup_id=WAKEUP_ID, subject="fire 1"
    )

    with pytest.raises(psycopg.OperationalError):
        await delivery._deliver(wake)
    cancel_fails.assert_awaited_once()
