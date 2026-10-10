"""A subagent archive records its lanes' captured images as run facts.

The lanes settle after their turn did, so a checkpoint ui record appended now
would land on whatever turn is newest. Each lane's path-to-URL map goes onto
the runs the turn launched for that task instead, limited to the paths that
lane referenced, and the launching turn is re-projected once a run took it.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.server.services import subagent_run_coordinator
from src.server.services.runs.subagent_archive import persist_collected_events

_A = "/home/workspace/a.png"
_B = "/home/workspace/b.png"
_URL_A = "https://cdn.example.com/a.png"
_URL_B = "https://cdn.example.com/b.png"


def _text(agent: str | None, content: str) -> dict:
    return {
        "event": "message_chunk",
        "data": {"agent": agent, "content_type": "text", "content": content},
    }


async def _persist(events: list[dict], captured: dict[str, str]):
    record = AsyncMock()
    reader = MagicMock()
    reader.append_ui_record = AsyncMock()
    with (
        patch(
            "src.server.services.persistence.image_capture.capture_images",
            AsyncMock(return_value=captured),
        ),
        patch.object(subagent_run_coordinator, "record_lane_images", record),
        patch(
            "src.server.services.runs.subagent_archive.replace_agent_events",
            AsyncMock(return_value=True),
        ),
        patch(
            "src.server.services.history.reader.CheckpointHistoryReader.get_instance",
            return_value=reader,
        ),
    ):
        ok = await persist_collected_events(
            events, "resp-1", "thread-1", "ws-1", sandbox=object()
        )
    return ok, record, reader


@pytest.mark.asyncio
async def test_each_lane_records_only_the_images_it_referenced():
    events = [_text("task:k1", f"see ![a]({_A})"), _text("task:k2", f"see ![b]({_B})")]

    ok, record, reader = await _persist(events, {_A: _URL_A, _B: _URL_B})

    assert ok is True
    assert sorted(c.args for c in record.await_args_list) == [
        ("thread-1", "resp-1", "k1", {_A: _URL_A}),
        ("thread-1", "resp-1", "k2", {_B: _URL_B}),
    ]
    reader.append_ui_record.assert_not_called()


@pytest.mark.asyncio
async def test_a_lane_with_nothing_captured_records_nothing():
    events = [_text("task:k1", f"see ![a]({_A})"), _text("task:k2", "no image")]

    _, record, _ = await _persist(events, {})

    record.assert_not_awaited()


@pytest.mark.asyncio
async def test_rows_outside_a_task_lane_record_nothing():
    _, record, _ = await _persist([_text(None, f"see ![a]({_A})")], {_A: _URL_A})

    record.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("landed", "refreshed"), [(True, True), (False, False)]
)
async def test_the_launching_turn_is_reprojected_once_a_run_took_the_images(
    landed, refreshed
):
    append = AsyncMock(return_value=landed)
    refresh = MagicMock()
    with (
        patch.object(subagent_run_coordinator.facts_db, "append_lane_images", append),
        patch(
            "src.server.services.history.replay.refresh.schedule_replay_refresh",
            refresh,
        ),
    ):
        await subagent_run_coordinator.record_lane_images(
            "thread-1", "resp-1", "k1", {_A: _URL_A}
        )

    append.assert_awaited_once_with("thread-1", "resp-1", "k1", {_A: _URL_A})
    assert refresh.called is refreshed
    if refreshed:
        refresh.assert_called_once_with("thread-1", "resp-1")


@pytest.mark.asyncio
async def test_images_that_fail_to_land_never_fail_the_archive():
    refresh = MagicMock()
    with (
        patch.object(
            subagent_run_coordinator.facts_db,
            "append_lane_images",
            AsyncMock(side_effect=RuntimeError("pool closed")),
        ),
        patch(
            "src.server.services.history.replay.refresh.schedule_replay_refresh",
            refresh,
        ),
    ):
        await subagent_run_coordinator.record_lane_images(
            "thread-1", "resp-1", "k1", {_A: _URL_A}
        )

    refresh.assert_not_called()
