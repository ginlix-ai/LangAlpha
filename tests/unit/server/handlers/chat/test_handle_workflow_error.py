"""Tests for ``handle_workflow_error`` terminal wiring (v4).

Pins the contract that both terminal branches (max-retries-exceeded and
non-recoverable) terminal-write the open run through
``RunCoordinator.finalize_run`` — and that a finalize failure or a
deterministic protocol conflict never suppresses the client-facing SSE
error. Without these tests a future refactor that drops the finalize call
would silently restore the original "in_progress forever" zombie after a
setup-error workflow dies.
"""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.server.handlers.chat import error_handling
from src.server.services.runs.admission import RunScope

# The scope routes releases through the definition site — patch it there.
RELEASE = "src.server.dependencies.usage_limits.release_burst_slot"


def _consume(agen):
    async def _drain():
        events = []
        async for event in agen:
            events.append(event)
        return events
    return _drain()


def _make_request(*, messages=(), checkpoint_id=None, hitl_response=None, additional_context=None):
    from src.server.models.chat import ChatMessage

    return SimpleNamespace(
        workspace_id="ws-1",
        locale=None,
        timezone=None,
        messages=[ChatMessage(**m) for m in messages],
        checkpoint_id=checkpoint_id,
        hitl_response=hitl_response,
        additional_context=additional_context,
    )


def _run_handle(attempt_no: int = 1):
    return SimpleNamespace(
        run_id="r-1",
        attempt_no=attempt_no,
        finalized=False,
        workspace_id=None,
        user_id=None,
    )


def _scope(run_handle=None):
    scope = RunScope(user_id="u-1", burst_slot_id=None)
    if run_handle is not None:
        scope.attach_run(run_handle)
    return scope


def _handler():
    handler = MagicMock()
    handler.get_tool_usage.return_value = None
    handler.get_sse_events.return_value = None
    handler._format_sse_event.side_effect = (
        lambda ev, data: f"event: {ev}\ndata: {data}\n\n"
    )
    return handler


@pytest.fixture
def coordinator():
    """Patch RunCoordinator.get_instance to return a recordable mock."""
    coord = AsyncMock()
    coord.finalize_run.return_value = SimpleNamespace(run={"status": "error"})
    with patch("src.server.services.runs.coordinator.RunCoordinator") as coord_cls:
        coord_cls.get_instance.return_value = coord
        yield coord


@pytest.mark.asyncio
async def test_max_retries_branch_finalizes_error(coordinator):
    # Recoverable error past MAX_RETRIES → terminal-writes the run as error
    # with the retry-limit message. v4: the retry count is the run's
    # attempt_no, so the branch is driven via run_handle.attempt_no > MAX_RETRIES.
    run_handle = _run_handle(attempt_no=99)
    err = ConnectionError("connection refused")

    with patch(RELEASE, new=AsyncMock()), \
         patch.object(error_handling, "get_max_workflow_retries", return_value=3):
        await _consume(error_handling.handle_workflow_error(
            e=err,
            thread_id="t-max-retry",
            user_id="u-1",
            workspace_id="ws-1",
            handler=_handler(),
            token_callback=None,
            scope=_scope(run_handle),
            start_time=0.0,
            request=_make_request(),
            is_byok=False,
            msg_type="user",
            log_prefix="CHAT",
        ))

    coordinator.finalize_run.assert_awaited_once()
    handle, outcome = coordinator.finalize_run.await_args.args
    assert handle is run_handle
    assert outcome.status == "error"
    assert "Max retries exceeded" in outcome.errors[0]
    assert "ConnectionError" in outcome.errors[0]


@pytest.mark.asyncio
async def test_non_recoverable_branch_finalizes_error(coordinator):
    # Non-recoverable error (AttributeError) → terminal-writes the run with
    # the error's message.
    err = AttributeError("'NoneType' has no attribute 'foo'")

    with patch(RELEASE, new=AsyncMock()), \
         patch.object(error_handling, "get_max_workflow_retries", return_value=3):
        await _consume(error_handling.handle_workflow_error(
            e=err,
            thread_id="t-non-recov",
            user_id="u-1",
            workspace_id="ws-1",
            handler=_handler(),
            token_callback=None,
            scope=_scope(_run_handle()),
            start_time=0.0,
            request=_make_request(),
            is_byok=False,
            msg_type="user",
            log_prefix="CHAT",
        ))

    coordinator.finalize_run.assert_awaited_once()
    _, outcome = coordinator.finalize_run.await_args.args
    assert outcome.status == "error"
    assert outcome.errors == ["'NoneType' has no attribute 'foo'"]


@pytest.mark.asyncio
async def test_finalize_failure_does_not_break_error_flow(coordinator):
    # If the terminal write itself raises, the handler logs CRITICAL (the row
    # stays in_progress for recovery) but must still emit the SSE error event.
    coordinator.finalize_run.side_effect = RuntimeError("db down")

    err = AttributeError("boom")
    handler = _handler()

    with patch(RELEASE, new=AsyncMock()), \
         patch.object(error_handling, "get_max_workflow_retries", return_value=3):
        events = await _consume(error_handling.handle_workflow_error(
            e=err,
            thread_id="t-fail",
            user_id="u-1",
            workspace_id="ws-1",
            handler=handler,
            token_callback=None,
            scope=_scope(_run_handle()),
            start_time=0.0,
            request=_make_request(),
            is_byok=False,
            msg_type="user",
            log_prefix="CHAT",
        ))

    assert any(ev.startswith("event: error\n") for ev in events)


@pytest.mark.asyncio
async def test_external_id_conflict_branch_emits_conflict_and_skips_finalize(
    coordinator,
):
    # A cross-user (platform, external_id) create race surfaces as a clean SSE
    # error carrying error_type=external_id_conflict, and (like the admission-
    # conflict path) must NOT finalize anything as a turn failure.
    import json as _json

    from src.server.database.conversation import ExternalIdConflictError

    err = ExternalIdConflictError(platform="telegram", external_id="chat:42")

    with patch(RELEASE, new=AsyncMock()), \
         patch.object(error_handling, "get_max_workflow_retries", return_value=3):
        # handler=None takes the json.dumps SSE branch, easy to parse.
        events = await _consume(error_handling.handle_workflow_error(
            e=err,
            thread_id="t-ext",
            user_id="u-1",
            workspace_id="ws-1",
            handler=None,
            token_callback=None,
            scope=_scope(),
            start_time=0.0,
            request=_make_request(),
            is_byok=False,
            msg_type="user",
            log_prefix="CHAT",
        ))

    assert len(events) == 1
    assert events[0].startswith("event: error\n")
    payload = _json.loads(events[0].split("data: ", 1)[1].strip())
    assert payload["error_type"] == "external_id_conflict"
    assert payload["platform"] == "telegram"
    assert payload["external_id"] == "chat:42"
    # Deterministic protocol conflict — not a workflow failure.
    coordinator.finalize_run.assert_not_awaited()
    coordinator.fail_open_run.assert_not_awaited()


@pytest.mark.asyncio
async def test_query_conflict_branch_emits_turn_conflict_and_skips_finalize(
    coordinator,
):
    # The differing-content query collision (unfenced cross-instance race —
    # START rolled back) must surface as a structured turn_conflict SSE error,
    # never a 500 or a finalized turn failure.
    import json as _json

    from src.server.database.conversation import QueryConflictError

    err = QueryConflictError(
        thread_id="t-qc", turn_index=3, existing_content="other content"
    )

    with patch(RELEASE, new=AsyncMock()), \
         patch.object(error_handling, "get_max_workflow_retries", return_value=3):
        events = await _consume(error_handling.handle_workflow_error(
            e=err,
            thread_id="t-qc",
            user_id="u-1",
            workspace_id="ws-1",
            handler=None,
            token_callback=None,
            scope=_scope(),
            start_time=0.0,
            request=_make_request(),
            is_byok=False,
            msg_type="user",
            log_prefix="CHAT",
        ))

    assert len(events) == 1
    assert events[0].startswith("event: error\n")
    payload = _json.loads(events[0].split("data: ", 1)[1].strip())
    assert payload["error_type"] == "admission_conflict"
    assert payload["code"] == "turn_conflict"
    # The existing row's content must never leak into the SSE payload.
    assert "other content" not in events[0]
    coordinator.finalize_run.assert_not_awaited()
    coordinator.fail_open_run.assert_not_awaited()


# --- Which recovery a failure names, and what the failed run row records ---


def _frames(events):
    import json as _json

    frames = []
    for ev in events:
        head, data = ev.split("\ndata: ", 1)
        frames.append((head.removeprefix("event: "), _json.loads(data.strip())))
    return frames


async def _fail(err, *, scope, request):
    with patch(RELEASE, new=AsyncMock()), \
         patch.object(error_handling, "get_max_workflow_retries", return_value=3):
        return _frames(await _consume(error_handling.handle_workflow_error(
            e=err,
            thread_id="t-1",
            user_id="u-1",
            workspace_id="ws-1",
            handler=None,
            token_callback=None,
            scope=scope,
            start_time=0.0,
            request=request,
            is_byok=False,
            msg_type="ptc",
            log_prefix="CHAT",
        )))


_SEND = {"messages": [{"role": "user", "content": "what moved NVDA?"}]}


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "request_kwargs, pending, recovery",
    [
        # A send runs on the thread's latest checkpoint, as its re-run will.
        (_SEND, {"checkpoint_id": None}, "retry"),
        # An edit keeps its fork base: the failed finalize re-pins the
        # thread to the old branch tip.
        ({**_SEND, "checkpoint_id": "cp-base"}, {"checkpoint_id": "cp-base"}, "retry"),
        # A regenerate keeps its fork base for the same reason, and has no
        # message to run again.
        ({"checkpoint_id": "cp-1"}, {"checkpoint_id": "cp-1", "replay": True}, "retry"),
        # A resume's answers are the client's to send again.
        (
            {**_SEND, "hitl_response": {"i-1": {"decision": "approve"}}},
            {"resend": True},
            "resend",
        ),
        # Context sent beside the text is kept by no row: a retry brings it.
        (
            {**_SEND, "additional_context": [{"type": "skills", "name": "dcf-model"}]},
            {"checkpoint_id": None, "context": True},
            "retry",
        ),
        # The query row keeps one text: anything more is only the client's.
        (
            {"messages": [
                {"role": "system", "content": "Answer briefly."},
                {"role": "user", "content": "what moved NVDA?"},
            ]},
            {"resend": True},
            "resend",
        ),
        (
            {"messages": [{"role": "user", "content": [
                {"type": "text", "text": "what is in this chart?"},
                {"type": "image", "image_url": "data:image/png;base64,iVBORw0KGgo="},
            ]}]},
            {"resend": True},
            "resend",
        ),
    ],
    ids=["send", "edit", "regenerate", "hitl-resume", "context", "history", "image-part"],
)
async def test_pre_graph_failure_records_pending_input_on_the_run(
    coordinator, request_kwargs, pending, recovery
):
    frames = await _fail(
        ConnectionError("connection refused"),
        scope=_scope(_run_handle()),
        request=_make_request(**request_kwargs),
    )

    _, outcome = coordinator.finalize_run.await_args.args
    assert outcome.metadata.get("pending_input") == pending
    assert [(name, data["recovery"]) for name, data in frames] == [("retry", recovery)]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "err, attempt_no, event",
    [
        (ConnectionError("connection refused"), 1, "retry"),
        (ConnectionError("connection refused"), 99, "error"),
        (AttributeError("boom"), 1, "error"),
    ],
    ids=["recoverable", "retries-exhausted", "non-recoverable"],
)
async def test_failure_of_a_started_run_names_retry(
    coordinator, err, attempt_no, event
):
    frames = await _fail(
        err, scope=_scope(_run_handle(attempt_no)), request=_make_request(**_SEND)
    )

    assert [(name, data["recovery"]) for name, data in frames] == [(event, "retry")]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "err, event",
    [(ConnectionError("connection refused"), "retry"), (AttributeError("boom"), "error")],
    ids=["recoverable", "non-recoverable"],
)
async def test_failure_before_any_run_names_resend(coordinator, err, event):
    # No run row exists for /retry to chain onto: the client sends again.
    frames = await _fail(err, scope=_scope(), request=_make_request(**_SEND))

    assert [(name, data["recovery"]) for name, data in frames] == [(event, "resend")]
    coordinator.finalize_run.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "err", [ConnectionError("stream read failed"), AttributeError("boom")]
)
async def test_reader_failure_after_handoff_emits_nothing(coordinator, err):
    # The executor holds the run and writes its end; a frame here would fail
    # a turn that is still running instead of letting the client reconnect.
    scope = _scope(_run_handle())
    scope.transfer_to_executor()

    frames = await _fail(err, scope=scope, request=_make_request(**_SEND))

    assert frames == []
    coordinator.finalize_run.assert_not_awaited()


# --- The HTTP body ends at the failure frame ---


async def _turn(*frames, raises):
    for frame in frames:
        yield frame
    raise raises


@pytest.mark.asyncio
@pytest.mark.parametrize("event", ["retry", "error"])
async def test_http_body_ends_cleanly_after_the_failure_frame(event):
    # The turn re-raises for in-process drainers; over HTTP that abort would
    # leave a proxied client waiting on a turn that is already over.
    frame = f'event: {event}\ndata: {{"recovery": "retry"}}\n\n'
    stream = _turn("event: metadata\ndata: {}\n\n", frame, raises=ConnectionError())

    events = await _consume(error_handling.end_after_failure_frame(stream))

    assert events[-1] == frame


@pytest.mark.asyncio
async def test_http_body_still_aborts_on_a_failure_with_no_frame():
    stream = _turn("event: message_chunk\ndata: {}\n\n", raises=ConnectionError())

    with pytest.raises(ConnectionError):
        await _consume(error_handling.end_after_failure_frame(stream))
