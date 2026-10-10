"""A caller that waits on a refresh never pays for a whole-thread pass: the
turns that need one go to the background runner."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, call

import pytest

from src.server.services.history import replay
from src.server.services.history.replay import refresh
from tests.unit.server.services.history.replay_builders import thread_rows

pytestmark = pytest.mark.asyncio

_ROWS = thread_rows([], {3: {"turn_index": 3, "conversation_response_id": "r-3"}})


@pytest.fixture
def wired(monkeypatch):
    monkeypatch.setattr(
        "src.server.database.conversation.get_replay_thread_data",
        AsyncMock(return_value=_ROWS),
    )
    project = AsyncMock(return_value={})
    monkeypatch.setattr(replay, "project_turns", project)
    schedule = MagicMock()
    monkeypatch.setattr(refresh, "schedule_replay_refresh", schedule)
    return project, schedule


async def test_turns_that_need_the_whole_thread_are_left_to_the_runner(wired):
    project, schedule = wired
    project.side_effect = replay.ClaimsPassNeeded("tsk1")

    await refresh.refresh_thread("t-1", {"r-3"}, claims_pass=False)

    assert project.await_args.kwargs["claims_pass"] is False
    assert project.await_args.kwargs["turn_indexes"] == [3]
    schedule.assert_called_once_with("t-1", "r-3")


async def test_a_thread_the_checkpoints_cannot_cover_is_not_retried(wired):
    project, schedule = wired
    project.side_effect = replay.CheckpointReplayUnavailable("unpaired")

    await refresh.refresh_thread("t-1", {"r-3"}, claims_pass=False)

    schedule.assert_not_called()


async def test_the_runner_takes_the_pass_itself(wired):
    project, _schedule = wired

    await refresh.refresh_thread("t-1", {"r-3"})

    assert project.await_args.kwargs["claims_pass"] is True


async def test_a_newest_pass_that_needs_the_whole_thread_is_rescheduled(wired):
    project, schedule = wired
    project.side_effect = replay.ClaimsPassNeeded("tsk1")

    await refresh.refresh_thread("t-1", {"r-3"}, newest=True, claims_pass=False)

    assert project.await_args.kwargs["last_n_turns"] == 2
    assert schedule.call_args_list == [call("t-1"), call("t-1", "r-3")]


async def test_triggers_landing_before_the_runner_starts_fold_into_one_pass(
    monkeypatch,
):
    monkeypatch.setattr(refresh, "_runners", {})
    monkeypatch.setattr(refresh, "_pending", {})
    passes = []

    async def refresh_thread(thread_id, response_ids, *, newest=False):
        passes.append((thread_id, set(response_ids), newest))

    monkeypatch.setattr(refresh, "refresh_thread", refresh_thread)

    refresh.schedule_replay_refresh("t-1")
    refresh.schedule_replay_refresh("t-1", "r-2")
    await refresh._runners["t-1"]

    assert passes == [("t-1", {"r-2"}, True)]
