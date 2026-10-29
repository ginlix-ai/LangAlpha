"""A subagent archive records its lanes' captured images as run facts.

The lanes settle after their turn's row did, so each lane's path-to-URL map
goes onto the runs the turn launched for that task, limited to the paths that
lane referenced, and the launching turn is re-projected once a run took it.
A map that never lands keeps the lane's capture stream for a later write.
"""

from __future__ import annotations

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.server.services import subagent_run_coordinator
from src.server.services.background_registry_store import BackgroundRegistryStore
from src.server.services.runs import subagent_archive, subagent_collection
from src.server.services.runs.subagent_archive import persist_collected_events

_A = "/home/workspace/a.png"
_B = "/home/workspace/b.png"
_URL_A = "https://cdn.example.com/a.png"
_URL_B = "https://cdn.example.com/b.png"


def _text(agent: str | None, content: str, message_id: str | None = None) -> dict:
    return {
        "event": "message_chunk",
        "data": {
            "agent": agent,
            "id": message_id,
            "content_type": "text",
            "content": content,
        },
    }


async def _persist(events: list[dict], captured: dict[str, str]):
    record = AsyncMock()
    with (
        patch(
            "src.server.services.persistence.image_capture.capture_images",
            AsyncMock(return_value=captured),
        ),
        patch.object(subagent_run_coordinator, "record_lane_images", record),
        patch(
            "src.server.services.runs.subagent_archive.replace_lane_provenance",
            AsyncMock(return_value=True),
        ),
    ):
        ok = await persist_collected_events(
            events, "resp-1", "thread-1", "ws-1", sandbox=object()
        )
    return ok, record


@pytest.mark.asyncio
async def test_each_lane_records_only_the_images_it_referenced():
    events = [_text("task:k1", f"see ![a]({_A})"), _text("task:k2", f"see ![b]({_B})")]

    ok, record = await _persist(events, {_A: _URL_A, _B: _URL_B})

    assert ok is True
    assert sorted(c.args for c in record.await_args_list) == [
        ("thread-1", "resp-1", "k1", {_A: _URL_A}),
        ("thread-1", "resp-1", "k2", {_B: _URL_B}),
    ]


@pytest.mark.asyncio
async def test_a_reference_split_across_streamed_pieces_is_recorded():
    """A message streams in pieces that can split a reference anywhere;
    pieces of different messages never join into one."""
    events = [
        _text("task:k1", "see ![a](/home/wor", "m1"),
        _text("task:k1", "kspace/a.png) and ![b](/home/", "m1"),
        _text("task:k1", "workspace/b.png)", "m2"),
    ]

    _, record = await _persist(events, {_A: _URL_A, _B: _URL_B})

    record.assert_awaited_once_with("thread-1", "resp-1", "k1", {_A: _URL_A})


@pytest.mark.asyncio
async def test_a_lane_with_nothing_captured_records_nothing():
    events = [_text("task:k1", f"see ![a]({_A})"), _text("task:k2", "no image")]

    _, record = await _persist(events, {})

    record.assert_not_awaited()


@pytest.mark.asyncio
async def test_rows_outside_a_task_lane_record_nothing():
    _, record = await _persist([_text(None, f"see ![a]({_A})")], {_A: _URL_A})

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


def _settled_task() -> SimpleNamespace:
    drained = asyncio.Event()
    drained.set()
    return SimpleNamespace(
        task_id="k1",
        tool_call_id="tc-1",
        task_run_id="tr-1",
        spawned_run_id=None,
        collector_response_id="resp-1",
        completed=True,
        is_pending=False,
        asyncio_task=None,
        captured_event_count=1,
        sse_drain_complete=drained,
        redis_spill_lock=asyncio.Lock(),
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("failure", "retired"), [(None, True), (RuntimeError("pool closed"), False)]
)
async def test_images_that_fail_to_land_keep_the_lanes_capture_stream(
    failure, retired
):
    """Once the capture stream retires, the run's map is the only record of
    where the lane's images went, so a write that never lands fails the
    archive and the collector keeps the stream. The lanes' sources land
    either way."""

    async def stream(thread_id, task):
        yield {**_text("task:k1", f"see ![a]({_A})", "m1"), "run": "resp-1"}

    append = AsyncMock(return_value=True, side_effect=failure)
    registries = MagicMock()
    registries.get_registry = AsyncMock(return_value=None)
    retire = AsyncMock()
    provenance = AsyncMock(return_value=True)
    with (
        patch.object(subagent_collection, "iter_subagent_events_full", stream),
        patch.object(
            BackgroundRegistryStore, "get_instance", return_value=registries
        ),
        patch(
            "src.server.services.persistence.image_capture.capture_images",
            AsyncMock(return_value={_A: _URL_A}),
        ),
        patch.object(subagent_run_coordinator.facts_db, "append_lane_images", append),
        patch(
            "src.server.services.history.replay.refresh.schedule_replay_refresh",
            MagicMock(),
        ),
        patch(
            "src.server.services.runs.subagent_archive.replace_lane_provenance",
            provenance,
        ),
        patch.object(subagent_archive.asyncio, "sleep", AsyncMock()),
        patch.object(subagent_collection, "persist_subagent_usage", AsyncMock()),
        patch.object(subagent_collection, "get_cache_client", return_value=MagicMock()),
        patch.object(subagent_collection, "delete_task_keys_if_owned", retire),
    ):
        await subagent_collection.collect_subagent_results_for_turn(
            "thread-1", "resp-1", [_settled_task()], "ws-1", "user-1",
            sandbox=object(), track_orphan_collector=lambda *_: None,
        )

    append.assert_awaited_with("thread-1", "resp-1", "k1", {_A: _URL_A})
    assert retire.await_count == (1 if retired else 0)
    provenance.assert_awaited()
