"""A failed message that never reached the graph runs again on /retry.

Pins the seams of that contract: what the failed run row carries, what
``/retry`` builds from it (the query row's text, never the client's), that
the re-run chains onto the turn without a second query row, and that a START
which raised after committing still leaves its run to the turn to settle.
"""

from __future__ import annotations

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi import HTTPException

from src.server.models.workflow import RetryRequest
from src.server.services.runs import admission
from src.server.services.runs.pending_input import (
    PendingInput,
    input_rerun_request,
    read_pending_input,
)

GET_QUERY = "src.server.database.conversation.queries.get_query_at_turn"
REPORT = {"kind": "task", "task_id": "k3x9", "display_id": "Task-k3x9"}
IMAGE = {
    "type": "image",
    "data": "data:image/png;base64,iVBORw0KGgo=",
    "description": "chart.png",
}


def _failed_run(**metadata):
    return {
        "conversation_response_id": "run-1",
        "turn_index": 4,
        "attempt_no": 1,
        "metadata": metadata,
    }


@pytest.mark.parametrize(
    "metadata, pending",
    [
        ({"pending_input": {"checkpoint_id": None}}, PendingInput(None)),
        ({"pending_input": {"checkpoint_id": "cp-base"}}, PendingInput("cp-base")),
        (
            {"pending_input": {"checkpoint_id": "cp-fork", "replay": True}},
            PendingInput("cp-fork", replay=True),
        ),
        ({"pending_input": {"resend": True}}, PendingInput(None, resend=True)),
        (
            {"pending_input": {"checkpoint_id": None, "context": True}},
            PendingInput(None, context=True),
        ),
        ({"retryable": True}, None),
        ({"pending_input": "garbled"}, None),
    ],
)
def test_read_pending_input(metadata, pending):
    assert read_pending_input(_failed_run(**metadata)) == pending


@pytest.mark.asyncio
async def test_rerun_takes_the_text_from_the_query_row():
    body = RetryRequest(
        workspace_id="ws-1",
        run_id="run-1",
        checkpoint_id="cp-other",
        request_key="9f2c0d1e-7b4a-4c3e-9a51-2d6f8e0b1c47",
        llm_model="model-a",
        additional_context=[IMAGE],
    )
    failed = _failed_run(locale="zh-CN", timezone="Asia/Shanghai")

    with patch(GET_QUERY, new=AsyncMock(return_value={"content": "what moved NVDA?"})) as get_query:
        request = await input_rerun_request(
            "t-1", failed, PendingInput("cp-base", context=True), workspace_id="ws-1", body=body
        )

    get_query.assert_awaited_once_with("t-1", 4)
    assert [(m.role, m.content) for m in request.messages] == [
        ("user", "what moved NVDA?")
    ]
    # The checkpoint the message was sent against, not the body's: a
    # replay's checkpoint means nothing to a message no checkpoint holds.
    assert request.checkpoint_id == "cp-base"
    assert request.fork_from_turn is None
    assert [c.type for c in request.additional_context] == ["image"]
    assert (request.locale, request.timezone) == ("zh-CN", "Asia/Shanghai")
    assert (request.request_key, request.llm_model) == ("9f2c0d1e-7b4a-4c3e-9a51-2d6f8e0b1c47", "model-a")


@pytest.mark.asyncio
async def test_a_system_turn_reruns_as_a_system_turn():
    """A report-back's message is the system's, not the user's question."""
    from src.server.handlers.chat.request_prep import _resolve_fork

    query = {"content": "report back", "type": "system", "metadata": {"report_source": REPORT}}
    with patch(GET_QUERY, new=AsyncMock(return_value=query)):
        request = await input_rerun_request(
            "t-1", _failed_run(), PendingInput(None), workspace_id="ws-1", body=None
        )

    assert request.query_type == "system"
    assert _resolve_fork(request=request, thread_id="t-1")[0] == "system"
    assert request.report_source.task_id == "k3x9"


@pytest.mark.asyncio
async def test_a_regenerated_system_turn_that_never_ran_stays_a_system_turn():
    from src.server.handlers.chat.request_prep import _resolve_fork
    from src.server.models.chat import ChatRequest
    from src.server.services.runs.pending_input import unstarted_regenerate_request

    query = {"content": "report back", "type": "system", "metadata": {"report_source": REPORT}}
    latest = _failed_run(pending_input={"checkpoint_id": None})
    regenerate = ChatRequest(messages=[], checkpoint_id="cp-tip", fork_from_turn=4)
    with patch(GET_QUERY, new=AsyncMock(return_value=query)):
        request = await unstarted_regenerate_request("t-1", regenerate, 4, latest)

    assert [m.content for m in request.messages] == ["report back"]
    assert _resolve_fork(request=request, thread_id="t-1")[0] == "system"
    # The edit replaces the row, and history labels a report-back by it.
    assert request.report_source.task_id == "k3x9"


@pytest.mark.asyncio
async def test_a_market_target_wake_reruns_with_its_notice():
    """Without it the rerun replays as a generic alert row."""
    notice = {"target_id": "tg-1", "notice": "hit", "symbol": "AAPL"}
    query = {
        "content": "<market-alert>",
        "type": "market_target",
        "metadata": {"market_target": notice},
    }
    with patch(GET_QUERY, new=AsyncMock(return_value=query)):
        request = await input_rerun_request(
            "t-1", _failed_run(), PendingInput(None), workspace_id="ws-1", body=None
        )

    assert request.query_type == "market_target"
    assert request.market_target == notice


@pytest.mark.asyncio
async def test_a_stored_report_that_no_longer_reads_is_dropped():
    query = {"content": "report back", "type": "system", "metadata": {"report_source": {"kind": "gone"}}}
    with patch(GET_QUERY, new=AsyncMock(return_value=query)):
        request = await input_rerun_request(
            "t-1", _failed_run(), PendingInput(None), workspace_id="ws-1", body=None
        )

    assert request.query_type == "system"
    assert request.report_source is None


@pytest.mark.asyncio
async def test_rerun_without_its_query_row_is_not_retryable():
    with patch(GET_QUERY, new=AsyncMock(return_value=None)):
        with pytest.raises(HTTPException) as exc:
            await input_rerun_request(
                "t-1", _failed_run(), PendingInput(None), workspace_id="ws-1", body=None
            )

    assert exc.value.status_code == 409
    assert exc.value.detail["code"] == "not_retryable"


@pytest.mark.asyncio
async def test_input_the_query_row_does_not_hold_is_not_rerun():
    with patch(GET_QUERY, new=AsyncMock(return_value={"content": "what is in this chart?"})):
        with pytest.raises(HTTPException) as exc:
            await input_rerun_request(
                "t-1",
                _failed_run(),
                PendingInput(None, resend=True),
                workspace_id="ws-1",
                body=None,
            )

    assert exc.value.status_code == 409
    assert exc.value.detail["code"] == "resend_required"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "body",
    [None, RetryRequest(workspace_id="ws-1", run_id="run-1")],
    ids=["no-body", "no-context"],
)
async def test_a_retry_without_the_context_its_turn_carried_is_not_rerun(body):
    """After a reload the client no longer holds the attachment, and the
    stored text alone would answer a different question."""
    with patch(GET_QUERY, new=AsyncMock(return_value={"content": "summarize this"})):
        with pytest.raises(HTTPException) as exc:
            await input_rerun_request(
                "t-1",
                _failed_run(),
                PendingInput(None, context=True),
                workspace_id="ws-1",
                body=body,
            )

    assert exc.value.status_code == 409
    assert exc.value.detail["code"] == "resend_required"


@pytest.mark.asyncio
async def test_rerun_chains_onto_the_turn_without_a_second_query_row():
    from src.server.models.chat import ChatRequest

    request = ChatRequest(
        workspace_id="ws-1",
        messages=[{"role": "user", "content": "what moved NVDA?"}],
        retry_of_run_id="run-1",
        subagents_enabled=["research"],
    )
    coordinator = SimpleNamespace(start_run=AsyncMock(return_value="handle"))

    with patch.object(
        admission, "resolve_retry_of", new=AsyncMock(return_value=_failed_run())
    ), patch(
        "src.server.services.runs.coordinator.RunCoordinator.get_instance",
        return_value=coordinator,
    ):
        await admission.begin_run(
            request,
            scope=admission.RunScope(user_id="u-1", burst_slot_id=None),
            thread_id="t-1",
            run_id="run-2",
            workspace_id="ws-1",
            user_id="u-1",
            is_byok=False,
            query_content="what moved NVDA?",
            query_type="follow_up",
            feedback_action=None,
            query_metadata={},
            fork=None,
            is_checkpoint_replay=False,
        )

    kwargs = coordinator.start_run.await_args.kwargs
    assert kwargs["query"] is None
    assert kwargs["run_metadata"]["subagents_enabled"] == ["research"]
    assert (kwargs["turn_index"], kwargs["attempt_no"], kwargs["retry_of_run_id"]) == (
        4,
        2,
        "run-1",
    )



def _begin(scope):
    from src.server.models.chat import ChatRequest

    return admission.begin_run(
        ChatRequest(workspace_id="ws-1", messages=[{"role": "user", "content": "hi"}]),
        scope=scope,
        thread_id="t-1",
        run_id="run-2",
        workspace_id="ws-1",
        user_id="u-1",
        is_byok=False,
        query_content="hi",
        query_type="follow_up",
        feedback_action=None,
        query_metadata={},
        fork=None,
        is_checkpoint_replay=False,
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "row, owned",
    [
        (
            {
                "status": "in_progress",
                "turn_index": 5,
                "attempt_no": 1,
                "created_at": "2026-10-08T20:00:00Z",
            },
            True,
        ),
        (None, False),
    ],
    ids=["committed", "rolled-back"],
)
async def test_a_start_whose_commit_was_not_acknowledged_leaves_its_run_to_the_turn(
    row, owned
):
    """No executor ever takes a run whose START raised, so a committed one is
    the turn's to settle as the failed attempt ``/retry`` chains onto. Left
    to the client's resend instead, the message would steer into it. A lost
    COMMIT acknowledgement raises before START hands back any run, so only
    the ledger knows."""
    scope = admission.RunScope(user_id="u-1", burst_slot_id=None)
    coordinator = SimpleNamespace(
        start_run=AsyncMock(side_effect=RuntimeError("connection lost in COMMIT"))
    )

    with patch.object(
        admission, "resolve_retry_of", new=AsyncMock(return_value=None)
    ), patch(
        "src.server.services.runs.coordinator.RunCoordinator.get_instance",
        return_value=coordinator,
    ), patch(
        "src.server.database.runs.lifecycle.get_run", new=AsyncMock(return_value=row)
    ), pytest.raises(RuntimeError, match="connection lost in COMMIT"):
        await _begin(scope)

    handle = scope.owned_run_handle
    if not owned:
        assert handle is None
        return
    assert (handle.run_id, handle.thread_id, handle.turn_index, handle.attempt_no) == (
        "run-2",
        "t-1",
        5,
        1,
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("acknowledged", [True, False], ids=["committed", "ack-lost"])
async def test_a_start_whose_request_is_cancelled_commits_and_is_settled_stopped(
    acknowledged,
):
    """A keyed START's check may have taken its message back from the steer
    queue, and only the commit keeps it from there, so a client leaving
    mid-START does not cut START short. Its run is settled as stopped once
    START is done, and the thread's admission waits for that."""
    scope = admission.RunScope(user_id="u-1", burst_slot_id=None)
    lock = asyncio.Lock()
    await lock.acquire()
    scope.hold_admission(lock)
    in_transaction, commit = asyncio.Event(), asyncio.Event()
    committed = SimpleNamespace(run_id="run-2")

    async def start_run(**kwargs):
        in_transaction.set()
        await commit.wait()
        if not acknowledged:
            raise RuntimeError("connection lost in COMMIT")
        kwargs["on_started"](committed)
        return committed

    coordinator = SimpleNamespace(start_run=start_run, fail_open_run=AsyncMock())
    row = {
        "status": "in_progress",
        "turn_index": 5,
        "attempt_no": 1,
        "created_at": "2026-10-08T20:00:00Z",
    }

    with patch.object(
        admission, "resolve_retry_of", new=AsyncMock(return_value=None)
    ), patch(
        "src.server.services.runs.coordinator.RunCoordinator.get_instance",
        return_value=coordinator,
    ), patch(
        "src.server.database.runs.lifecycle.get_run", new=AsyncMock(return_value=row)
    ):
        request = asyncio.create_task(_begin(scope))
        await in_transaction.wait()
        request.cancel()
        with pytest.raises(asyncio.CancelledError):
            await request
        scope.release_admission()  # the turn's own release, in its finally
        assert lock.locked()
        coordinator.fail_open_run.assert_not_awaited()

        commit.set()
        for _ in range(100):
            if not lock.locked():
                break
            await asyncio.sleep(0)

    assert not lock.locked()
    assert scope.owned_run_handle is None
    (handle, _reason), kwargs = coordinator.fail_open_run.await_args
    assert kwargs["status"] == "cancelled"
    if acknowledged:
        assert handle is committed
    else:
        assert (handle.run_id, handle.turn_index) == ("run-2", 5)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "pending, replays_from",
    [
        ({"checkpoint_id": None}, None),
        # The failed finalize re-pinned the thread to the old branch tip.
        ({"checkpoint_id": "cp-fork", "replay": True}, "cp-fork"),
        (None, None),
    ],
    ids=["pending-input", "regenerate", "replay"],
)
async def test_retry_route_reruns_a_pending_input_and_replays_otherwise(
    pending, replays_from
):
    from src.server.app.threads import messaging

    # START recorded the build options a retry body cannot carry.
    metadata = {"subagents_enabled": ["research"]}
    if pending:
        metadata["pending_input"] = pending
    failed = {**_failed_run(**metadata), "status": "error"}
    send = AsyncMock(return_value="stream")
    auth = SimpleNamespace(user_id="u-1", burst_slot_id=None)

    with patch.object(messaging.auth_api, "require_thread_owner", new=AsyncMock()), \
         patch.object(messaging, "_assert_stream_transport_ready", new=AsyncMock()), \
         patch.object(messaging, "_handle_send_message", new=send), \
         patch(
             "src.server.database.runs.lifecycle.get_latest_attempt",
             new=AsyncMock(return_value=failed),
         ), \
         patch(
             "src.server.handlers.checkpoint_handler.get_retry_checkpoint",
             new=AsyncMock(return_value="cp-tip"),
         ) as checkpoint, \
         patch(GET_QUERY, new=AsyncMock(return_value={"content": "what moved NVDA?"})):
        await messaging.retry_thread(
            "t-1", auth, RetryRequest(workspace_id="ws-1", run_id="run-1")
        )

    request = send.await_args.args[0]
    assert send.await_args.kwargs == {"retry_of_run_id": "run-1"}
    assert request.subagents_enabled == ["research"]
    if pending and not pending.get("replay"):
        # A new thread's first send has no checkpoint to look up at all.
        checkpoint.assert_not_awaited()
        assert [m.content for m in request.messages] == ["what moved NVDA?"]
        assert request.checkpoint_id is None
    else:
        checkpoint.assert_awaited_once_with("t-1", replays_from)
        assert request.messages == []
        assert request.checkpoint_id == "cp-tip"


@pytest.mark.asyncio
async def test_a_retried_report_back_reaches_its_run_as_one(monkeypatch):
    """/retry builds its request from the stored rows, so the send handler
    keeps the type and report a client's body could not set."""
    from httpx import ASGITransport, AsyncClient

    from src.server.app.threads import messaging
    from tests.unit.server.app.test_threads_authz import (
        CALLER,
        _app_for,
        _empty_async_gen,
        _stub_downstream,
    )

    monkeypatch.setattr("src.config.settings.HOST_MODE", "platform")
    failed = {
        **_failed_run(pending_input={"checkpoint_id": None}),
        "conversation_response_id": "5b0f3f7e-9a51-4c3e-8d7a-2f6c1e0b9a44",
        "status": "error",
    }
    query = {"content": "report back", "type": "system", "metadata": {"report_source": REPORT}}
    workflow = MagicMock(return_value=_empty_async_gen())
    with (
        _stub_downstream(owner_id=CALLER),
        patch("src.server.handlers.chat.astream_ptc_workflow", workflow),
        patch.object(messaging.auth_api, "require_thread_owner", new=AsyncMock()),
        patch(
            "src.server.database.runs.lifecycle.get_latest_attempt",
            new=AsyncMock(return_value=failed),
        ),
        patch(GET_QUERY, new=AsyncMock(return_value=query)),
    ):
        async with AsyncClient(
            transport=ASGITransport(app=_app_for(CALLER)), base_url="http://test"
        ) as client:
            resp = await client.post(
                "/api/v1/threads/tid-mine/retry",
                json={"workspace_id": "ws-placeholder"},
            )

    assert resp.status_code == 200
    request = workflow.call_args.kwargs["request"]
    assert request.query_type == "system"
    assert request.report_source.task_id == "k3x9"
