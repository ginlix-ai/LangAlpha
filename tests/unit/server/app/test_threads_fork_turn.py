"""The send route settles a fork's turn from its checkpoint before any run.

Every reader after the route (the truncation, the regenerate's turn, the
prior-turn read) takes the turn from the request, so the route is where the
client's count gives way to the branch. The branch tip the turn was read off
rides along, for START to refuse the fork if the branch moved since.
"""

import uuid
from contextlib import ExitStack
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi import HTTPException
from httpx import ASGITransport, AsyncClient

from tests.unit.server.app.test_threads_authz import (
    CALLER,
    _app_for,
    _empty_async_gen,
    _stub_downstream,
)

_RESOLVE = "src.server.handlers.checkpoint_handler.resolve_fork_turn"


def _point(turn_index, branch_tip="cp-tip"):
    from src.server.handlers.checkpoint_handler import ForkPoint

    return ForkPoint(turn_index, branch_tip)


async def _post(body):
    app = _app_for(CALLER)
    workflow = MagicMock(return_value=_empty_async_gen())
    with (
        _stub_downstream(owner_id=CALLER),
        patch("src.server.handlers.chat.astream_ptc_workflow", workflow),
    ):
        async with AsyncClient(
            transport=ASGITransport(app=app), base_url="http://test"
        ) as client:
            resp = await client.post(
                "/api/v1/threads/tid-mine/messages",
                json={"workspace_id": "ws-placeholder", **body},
            )
    return resp, workflow


@pytest.mark.asyncio
async def test_an_edit_runs_as_the_turn_its_checkpoint_forks():
    resolve = AsyncMock(return_value=_point(3))
    with patch(_RESOLVE, resolve):
        resp, workflow = await _post(
            {
                "messages": [{"role": "user", "content": "edited"}],
                "checkpoint_id": "cp-before-3",
                "fork_from_turn": 2,
            }
        )

    assert resp.status_code == 200
    resolve.assert_awaited_once_with(
        "tid-mine", "cp-before-3", regenerate=False, requested=2
    )
    assert workflow.call_args.kwargs["request"].fork_from_turn == 3
    assert workflow.call_args.kwargs["request"].fork_branch_tip == "cp-tip"


@pytest.mark.asyncio
async def test_the_branch_tip_is_the_routes_never_the_clients():
    resolve = AsyncMock(return_value=_point(3, branch_tip=None))
    with patch(_RESOLVE, resolve):
        resp, workflow = await _post(
            {
                "messages": [],
                "checkpoint_id": "cp-3",
                "fork_from_turn": 3,
                "fork_branch_tip": "cp-client",
            }
        )

    assert resp.status_code == 200
    # Same turn as the client's: the tip is stamped all the same.
    assert workflow.call_args.kwargs["request"].fork_branch_tip is None


@pytest.mark.asyncio
async def test_a_regenerate_is_resolved_at_its_boundary():
    resolve = AsyncMock(return_value=_point(3))
    with patch(_RESOLVE, resolve):
        resp, workflow = await _post(
            {"messages": [], "checkpoint_id": "cp-3", "fork_from_turn": 3}
        )

    assert resp.status_code == 200
    resolve.assert_awaited_once_with(
        "tid-mine", "cp-3", regenerate=True, requested=3
    )
    assert workflow.call_args.kwargs["request"].fork_from_turn == 3


@pytest.mark.asyncio
async def test_a_fork_at_no_turn_of_the_branch_starts_nothing():
    resolve = AsyncMock(
        side_effect=HTTPException(status_code=409, detail={"code": "stale_fork"})
    )
    with patch(_RESOLVE, resolve):
        resp, workflow = await _post(
            {"messages": [], "checkpoint_id": "cp-gone", "fork_from_turn": 1}
        )

    assert resp.status_code == 409
    workflow.assert_not_called()


@pytest.mark.asyncio
async def test_a_plain_send_reads_no_branch():
    resolve = AsyncMock()
    with patch(_RESOLVE, resolve):
        resp, _ = await _post({"messages": [{"role": "user", "content": "hi"}]})

    assert resp.status_code == 200
    resolve.assert_not_awaited()


_LATEST = "src.server.database.runs.lifecycle.get_latest_attempt"
_NEWEST_RUN = "5b0f3f7e-9a51-4c3e-8d7a-2f6c1e0b9a44"


def _latest(status="completed"):
    return AsyncMock(
        return_value={
            "conversation_response_id": uuid.UUID(_NEWEST_RUN),
            "status": status,
            "turn_index": 2,
        }
    )


def _edit(**body):
    return {
        "messages": [{"role": "user", "content": "edited"}],
        "checkpoint_id": "cp-before-1",
        "fork_from_turn": 1,
        **body,
    }


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "seen", ["0d9e8f1a-3b2c-4d5e-9f60-718293a4b5c6", "not-a-run-id"]
)
async def test_an_edit_from_a_view_that_missed_a_newer_run_starts_nothing(seen):
    """Its checkpoint still forks a turn of the branch, so only the run the
    client last saw tells the fork would delete a turn it never showed."""
    latest = _latest()
    with patch(_RESOLVE, AsyncMock(return_value=_point(1))), patch(_LATEST, latest):
        resp, workflow = await _post(_edit(expected_latest_run_id=seen))

    assert resp.status_code == 409
    assert resp.json()["detail"]["code"] == "stale_fork"
    workflow.assert_not_called()


@pytest.mark.asyncio
async def test_an_edit_expecting_a_run_on_a_thread_with_none_starts_nothing():
    with (
        patch(_RESOLVE, AsyncMock(return_value=_point(1))),
        patch(_LATEST, AsyncMock(return_value=None)),
    ):
        resp, workflow = await _post(_edit(expected_latest_run_id=_NEWEST_RUN))

    assert resp.status_code == 409
    assert resp.json()["detail"]["code"] == "stale_fork"
    workflow.assert_not_called()


@pytest.mark.asyncio
async def test_an_edit_from_a_view_of_the_newest_run_is_admitted():
    resolve = AsyncMock(return_value=_point(1)._replace(run_horizon=7))
    with patch(_RESOLVE, resolve), patch(_LATEST, _latest()):
        resp, workflow = await _post(
            _edit(expected_latest_run_id=_NEWEST_RUN.upper())
        )

    assert resp.status_code == 200
    request = workflow.call_args.kwargs["request"]
    assert request.fork_from_turn == 1
    assert request.fork_branch_tip == "cp-tip"
    assert request.fork_run_horizon == 7


@pytest.mark.asyncio
async def test_an_edit_naming_no_run_reads_no_run():
    latest = _latest()
    with patch(_RESOLVE, AsyncMock(return_value=_point(1))), patch(_LATEST, latest):
        resp, _ = await _post(_edit())

    assert resp.status_code == 200
    latest.assert_not_awaited()


@pytest.mark.asyncio
async def test_an_unstarted_regenerate_checks_the_run_it_read_once():
    """The newest run the client saw is the unstarted turn's, still starting:
    the same read finds it running."""
    latest = _latest(status="in_progress")
    resolve = AsyncMock(return_value=_point(2)._replace(unstarted=True))
    with patch(_RESOLVE, resolve), patch(_LATEST, latest):
        resp, workflow = await _post(
            {
                "messages": [],
                "checkpoint_id": "cp-tip",
                "fork_from_turn": 2,
                "expected_latest_run_id": _NEWEST_RUN,
            }
        )

    assert resp.status_code == 409
    assert resp.json()["detail"]["code"] == "running"
    latest.assert_awaited_once()
    workflow.assert_not_called()


_GET_QUERY = "src.server.database.conversation.queries.get_query_at_turn"
_REPORT = {"kind": "task", "task_id": "k3x9", "display_id": "Task-k3x9"}


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "row, sent, query_type, report_task",
    [
        (
            {"type": "system", "metadata": {"report_source": _REPORT}},
            {},
            "system",
            "k3x9",
        ),
        (
            {"type": "initial", "metadata": {}},
            {"query_type": "system", "report_source": _REPORT},
            "initial",
            None,
        ),
    ],
    ids=["report-back", "client-claims-system"],
)
async def test_an_unstarted_regenerate_runs_as_the_turn_its_row_stored(
    monkeypatch, row, sent, query_type, report_task
):
    """The edit it becomes takes the row's type and report, after the
    client's own copies are cleared: a report-back stays one, and a person
    cannot make their message a system turn."""
    monkeypatch.setattr("src.config.settings.HOST_MODE", "platform")
    latest = AsyncMock(
        return_value={
            "conversation_response_id": uuid.UUID(_NEWEST_RUN),
            "status": "error",
            "turn_index": 2,
            "metadata": {"pending_input": {"checkpoint_id": None}},
        }
    )
    resolve = AsyncMock(return_value=_point(2)._replace(unstarted=True))
    query = AsyncMock(return_value={"content": "report back", **row})
    with patch(_RESOLVE, resolve), patch(_LATEST, latest), patch(_GET_QUERY, query):
        resp, workflow = await _post(
            {
                "messages": [],
                "checkpoint_id": "cp-tip",
                "fork_from_turn": 2,
                "expected_latest_run_id": _NEWEST_RUN,
                **sent,
            }
        )

    assert resp.status_code == 200
    request = workflow.call_args.kwargs["request"]
    assert [m.content for m in request.messages] == ["report back"]
    assert request.query_type == query_type
    assert getattr(request.report_source, "task_id", None) == report_task


_TURN_BASE = "src.server.handlers.checkpoint_handler.turn_base_checkpoint_id"
_BRANCH_READ = "src.server.database.conversation.threads_read.get_thread_branch_read"
_ANSWER = {
    "messages": [],
    "hitl_response": {"pause": {"decisions": [{"type": "approve"}]}},
}


def _branch_read(run_horizon):
    from src.server.database.conversation.threads_read import ThreadBranchRead

    return ThreadBranchRead("cp-tip", run_horizon)


@pytest.mark.asyncio
async def test_an_answer_to_a_pause_a_fork_replaced_starts_nothing():
    """The answer would resume the newest checkpoint, off the thread's
    branch: refused before START opens a run for it."""
    with (
        patch(_BRANCH_READ, AsyncMock(return_value=_branch_read(7))),
        patch(_TURN_BASE, AsyncMock(return_value="cp-pointer")),
        patch("src.server.handlers.checkpoint_handler.get_checkpointer"),
    ):
        resp, workflow = await _post(_ANSWER)

    assert resp.status_code == 409
    assert resp.json()["detail"]["code"] == "stale_fork"
    workflow.assert_not_called()


@pytest.mark.asyncio
async def test_an_answer_on_the_threads_branch_resumes():
    """It resumes held to the run horizon the route read, never the client's."""
    with (
        patch(_BRANCH_READ, AsyncMock(return_value=_branch_read(7))),
        patch(_TURN_BASE, AsyncMock(return_value=None)),
        patch("src.server.handlers.checkpoint_handler.get_checkpointer"),
    ):
        resp, workflow = await _post({**_ANSWER, "resume_run_horizon": 99})

    assert resp.status_code == 200
    request = workflow.call_args.kwargs["request"]
    assert request.hitl_response
    assert request.resume_run_horizon == 7


_FIND_KEY = "src.server.database.runs.lifecycle.find_run_by_request_key"
_KEY = "8c1d2e3f-4a5b-4c6d-8e7f-90a1b2c3d4e5"
_FIRST_COPY = "2e4f6a8b-0c1d-4e2f-8a3b-5c6d7e8f9a0b"


def _refused(body):
    """A request the route refuses on the branch: an edit from a view that
    missed the newest run, or an answer to a pause a fork replaced."""
    stack = ExitStack()
    # No copy of the send went in as a steer.
    stack.enter_context(
        patch(
            "src.server.database.runs.outbox.get_steer_job",
            AsyncMock(return_value=None),
        )
    )
    if "hitl_response" in body:
        stack.enter_context(
            patch(_BRANCH_READ, AsyncMock(return_value=_branch_read(7)))
        )
        stack.enter_context(patch(_TURN_BASE, AsyncMock(return_value="cp-pointer")))
        stack.enter_context(
            patch("src.server.handlers.checkpoint_handler.get_checkpointer")
        )
    else:
        stack.enter_context(patch(_RESOLVE, AsyncMock(return_value=_point(1))))
        stack.enter_context(patch(_LATEST, _latest()))
    return stack


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "body",
    [
        _edit(expected_latest_run_id="0d9e8f1a-3b2c-4d5e-9f60-718293a4b5c6"),
        _ANSWER,
    ],
    ids=["edit", "answer"],
)
async def test_a_retransmit_whose_first_copy_started_after_the_probe_reconnects(
    body,
):
    """The first copy's run is what the branch check saw move, so the key
    names it: the copy reconnects to it instead of being refused."""
    find = AsyncMock(
        side_effect=[
            None,
            {
                "conversation_response_id": uuid.UUID(_FIRST_COPY),
                "conversation_thread_id": "tid-mine",
                "status": "in_progress",
            },
        ]
    )
    with (
        _refused(body),
        patch(_FIND_KEY, find),
        patch(
            "src.server.app.threads.messaging.get_thread_owner_id",
            AsyncMock(return_value=CALLER),
        ),
    ):
        resp, workflow = await _post({**body, "request_key": _KEY})

    assert resp.status_code == 409
    detail = resp.json()["detail"]
    assert (detail["code"], detail["run_id"]) == ("duplicate_request", _FIRST_COPY)
    assert find.await_count == 2
    workflow.assert_not_called()


@pytest.mark.asyncio
async def test_a_refused_edit_whose_key_ran_nowhere_stays_refused():
    find = AsyncMock(return_value=None)
    body = _edit(expected_latest_run_id="0d9e8f1a-3b2c-4d5e-9f60-718293a4b5c6")
    with _refused(body), patch(_FIND_KEY, find):
        resp, workflow = await _post({**body, "request_key": _KEY})

    assert resp.status_code == 409
    assert resp.json()["detail"]["code"] == "stale_fork"
    assert find.await_count == 2
    workflow.assert_not_called()
