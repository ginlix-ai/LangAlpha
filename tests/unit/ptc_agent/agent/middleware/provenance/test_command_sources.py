"""A background command's data sources reach the chat with its notice.

A command that outlived its tool call reports back later: as a notice the
steering middleware joins to a live turn, or as a report-back turn of its
own. The model call that first reads the notice emits the command's MCP
sources from the trace stored on its row, through the same verification,
redaction and body store as a code run's result. Neutral placeholder data.
"""

import hashlib
import json
import uuid
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest
from langchain_core.messages import AIMessage, HumanMessage, ToolMessage

from ptc_agent.agent.middleware.background_subagent.context import (
    current_background_agent_id,
)
from ptc_agent.agent.middleware.provenance import ProvenanceMiddleware
from ptc_agent.agent.middleware.provenance.command_trace import (
    REPORT_COMMAND_KEY,
    read_command_trace,
    reported_commands,
)
from ptc_agent.agent.provenance import sources_from_trace

_MW = "ptc_agent.agent.middleware.provenance.middleware"
_STORE_PATH = "src.server.database.provenance_bodies.store_result_bodies"

_BODY = json.dumps({"count": 1, "data": [{"close": 101.5, "date": "2026-01-02"}]})
_ENTRY = {
    "server": "price_data",
    "tool": "get_stock_data",
    "args": {"symbol": "TST", "interval": "1day"},
    "result_sha256": hashlib.sha256(_BODY.encode()).hexdigest(),
    "result_size": len(_BODY),
    "result_snippet": _BODY[:40],
    "result_body": _BODY,
    "timestamp": "2026-01-02T15:00:00+00:00",
}


def _notice(*command_ids: str) -> HumanMessage:
    """The steering message a live turn takes a command's notice in with."""
    return HumanMessage(
        content="<system>Background command ...</system>",
        id=f"steering-{uuid.uuid4().hex}",
        additional_kwargs={
            "lc_source": "steering",
            "steering_delivered": {
                "count": 0,
                "messages": [
                    {
                        "content": "",
                        "report_source": {
                            "kind": "command_report",
                            "command_id": cid,
                            "tool": "ExecuteCode",
                            "status": "completed",
                        },
                    }
                    for cid in command_ids
                ],
            },
        },
    )


def _task_notice() -> HumanMessage:
    return HumanMessage(
        content="<system>Task ...</system>",
        additional_kwargs={
            "lc_source": "steering",
            "steering_delivered": {
                "messages": [{"report_source": {"kind": "task", "task_id": "t1"}}]
            },
        },
    )


def _answered() -> list:
    return [
        HumanMessage(content="earlier question", id="in-0"),
        AIMessage(content="", tool_calls=[{"name": "Bash", "args": {}, "id": "c1"}]),
        ToolMessage(content="ok", tool_call_id="c1"),
    ]


# ----- the trace reader shared by a tool result and a stored trace -----------


def test_stored_trace_sources_name_no_call_and_keep_their_ids():
    first = sources_from_trace([_ENTRY, _ENTRY], record_seed="bg-0123456789ab")
    again = sources_from_trace([_ENTRY, _ENTRY], record_seed="bg-0123456789ab")

    assert [s.tool_call_id for s in first] == [None, None]
    assert [s.record_id for s in first] == [s.record_id for s in again]
    assert len({s.record_id for s in first}) == 2
    assert all(uuid.UUID(s.record_id).version == 5 for s in first)
    other = sources_from_trace([_ENTRY], record_seed="bg-ba9876543210")
    assert other[0].record_id != first[0].record_id


def test_a_tool_results_trace_names_its_call_with_fresh_ids():
    a = sources_from_trace([_ENTRY], "call-1")
    b = sources_from_trace([_ENTRY], "call-1")

    assert a[0].tool_call_id == "call-1"
    assert uuid.UUID(a[0].record_id).version == 4
    assert a[0].record_id != b[0].record_id
    assert a[0].identifier == "price_data:get_stock_data"
    assert a[0].provider == "mcp:price_data"
    assert a[0].result_body == _BODY


def test_a_text_field_that_is_not_a_string_is_dropped_not_passed_through():
    """The trace is the agent's to write: a list where a snippet goes would
    reach the event with no bound on its size."""
    forged = {**_ENTRY, "result_sha256": ["f" * 64] * 1000, "result_snippet": {"a": "b" * 5000}}
    (source,) = sources_from_trace([forged], "call-1")

    assert source.result_sha256 is None
    assert source.result_snippet is None


def test_a_trace_that_is_not_a_list_yields_nothing():
    assert sources_from_trace(None, record_seed="bg-0123456789ab") == []
    assert sources_from_trace({"server": "x"}) == []


# ----- which commands a model call reads the notice of -----------------------


def test_a_steered_notice_is_read_at_the_call_after_it():
    messages = [*_answered(), _notice("bg-aaaaaaaaaaaa", "bg-bbbbbbbbbbbb")]

    assert reported_commands(messages, {}) == ["bg-aaaaaaaaaaaa", "bg-bbbbbbbbbbbb"]


def test_a_notice_the_model_already_answered_is_not_read_again():
    messages = [*_answered(), _notice("bg-aaaaaaaaaaaa"), AIMessage(content="done")]

    assert reported_commands(messages, {}) == []


def test_a_resume_the_steering_middleware_called_does_not_close_the_window():
    resume = AIMessage(
        content="",
        tool_calls=[{"name": "Task", "args": {}, "id": "r1"}],
        additional_kwargs={"lc_source": "steering"},
    )
    messages = [*_answered(), _notice("bg-aaaaaaaaaaaa"), resume]

    assert reported_commands(messages, {}) == ["bg-aaaaaaaaaaaa"]


def test_a_task_report_names_no_command():
    assert reported_commands([*_answered(), _task_notice()], {}) == []


def test_a_report_back_turn_reads_its_command_with_its_input_only():
    config = {REPORT_COMMAND_KEY: "bg-cccccccccccc"}
    first_call = [*_answered(), AIMessage(content="earlier answer"), HumanMessage(content="<system>Background command ...</system>")]
    later_call = [*first_call, AIMessage(content="", tool_calls=[{"name": "Read", "args": {}, "id": "c2"}]), ToolMessage(content="log", tool_call_id="c2")]

    assert reported_commands(first_call, config) == ["bg-cccccccccccc"]
    assert reported_commands(later_call, config) == []


def test_one_command_named_twice_is_read_once():
    config = {REPORT_COMMAND_KEY: "bg-cccccccccccc"}
    messages = [HumanMessage(content="input"), _notice("bg-cccccccccccc")]

    assert reported_commands(messages, config) == ["bg-cccccccccccc"]


# ----- the model-call hook ----------------------------------------------------


async def _call(middleware, messages, *, thread_id="thread-1", configurable=None):
    emitted: list[dict] = []
    store = AsyncMock()
    handler = AsyncMock(return_value="model-response")
    config = {"configurable": {"thread_id": thread_id, **(configurable or {})}}
    with (
        patch(f"{_MW}.get_config", return_value=config),
        patch(f"{_MW}.get_stream_writer", return_value=emitted.append),
        patch(_STORE_PATH, store),
    ):
        out = await middleware.awrap_model_call(
            SimpleNamespace(state={"messages": messages}), handler
        )
        await middleware._drain_body_flushes()
    handler.assert_awaited_once()
    return out, emitted, store


@pytest.mark.asyncio
async def test_a_steered_notice_brings_its_commands_sources():
    traces = AsyncMock(return_value=[_ENTRY])
    middleware = ProvenanceMiddleware(command_traces=traces)

    out, emitted, store = await _call(
        middleware, [*_answered(), _notice("bg-aaaaaaaaaaaa")]
    )

    assert out == "model-response"
    traces.assert_awaited_once_with("thread-1", "bg-aaaaaaaaaaaa")
    assert len(emitted) == 1
    event = emitted[0]
    assert event["type"] == "provenance"
    assert event["source_type"] == "mcp_tool"
    assert event["identifier"] == "price_data:get_stock_data"
    assert event["tool_call_id"] is None
    assert event["record_id"] == sources_from_trace(
        [_ENTRY], record_seed="bg-aaaaaaaaaaaa"
    )[0].record_id
    # The body hashes to the trace's sha, so both are kept and stored.
    assert event["result_sha256"] == _ENTRY["result_sha256"]
    assert event["result_size"] == len(_BODY)
    assert [item[0] for call in store.await_args_list for item in call.args[0]] == [
        _ENTRY["result_sha256"]
    ]


@pytest.mark.asyncio
async def test_a_report_back_turn_brings_its_commands_sources():
    traces = AsyncMock(return_value=[_ENTRY])
    middleware = ProvenanceMiddleware(command_traces=traces)

    _, emitted, _ = await _call(
        middleware,
        [HumanMessage(content="<system>Background code run ...</system>")],
        configurable={REPORT_COMMAND_KEY: "bg-cccccccccccc"},
    )

    traces.assert_awaited_once_with("thread-1", "bg-cccccccccccc")
    assert [e["tool_call_id"] for e in emitted] == [None]


@pytest.mark.asyncio
async def test_snippets_are_redacted_and_a_forged_sha_is_dropped():
    forged = {**_ENTRY, "result_sha256": "f" * 64, "result_snippet": "key=SECRETVALUE1"}
    middleware = ProvenanceMiddleware(
        redactor=lambda s: s.replace("SECRETVALUE1", "[REDACTED]") if s else s,
        command_traces=AsyncMock(return_value=[forged]),
    )

    _, emitted, store = await _call(
        middleware, [*_answered(), _notice("bg-aaaaaaaaaaaa")]
    )

    assert emitted[0]["result_snippet"] == "key=[REDACTED]"
    assert emitted[0]["result_sha256"] is None
    store.assert_not_awaited()


@pytest.mark.asyncio
async def test_a_subagent_never_emits_a_commands_sources():
    traces = AsyncMock(return_value=[_ENTRY])
    middleware = ProvenanceMiddleware(command_traces=traces)
    token = current_background_agent_id.set("research:abc")
    try:
        _, emitted, _ = await _call(
            middleware,
            [HumanMessage(content="task prompt")],
            configurable={REPORT_COMMAND_KEY: "bg-cccccccccccc"},
        )
    finally:
        current_background_agent_id.reset(token)

    traces.assert_not_awaited()
    assert emitted == []


@pytest.mark.asyncio
async def test_a_failed_read_never_blocks_the_model_call():
    middleware = ProvenanceMiddleware(
        command_traces=AsyncMock(side_effect=RuntimeError("db down"))
    )

    out, emitted, _ = await _call(
        middleware, [*_answered(), _notice("bg-aaaaaaaaaaaa")]
    )

    assert out == "model-response"
    assert emitted == []


@pytest.mark.asyncio
async def test_a_call_with_no_notice_reads_nothing():
    traces = AsyncMock(return_value=[_ENTRY])
    middleware = ProvenanceMiddleware(command_traces=traces)

    _, emitted, _ = await _call(middleware, _answered())

    traces.assert_not_awaited()
    assert emitted == []


# ----- the stored trace ------------------------------------------------------


@pytest.mark.asyncio
async def test_the_trace_is_read_only_for_the_commands_own_thread():
    read = AsyncMock(return_value={"mcp_trace": [_ENTRY]})
    with patch("src.server.database.runs.command_runs.get_thread_command", read):
        assert await read_command_trace("thread-1", "bg-x") == [_ENTRY]
    read.assert_awaited_once_with("thread-1", "bg-x", with_trace=True)


@pytest.mark.asyncio
async def test_a_missing_row_or_failed_read_has_no_trace():
    with patch(
        "src.server.database.runs.command_runs.get_thread_command",
        AsyncMock(return_value=None),
    ):
        assert await read_command_trace("t", "bg-x") == []
    with patch(
        "src.server.database.runs.command_runs.get_thread_command",
        AsyncMock(side_effect=RuntimeError("pool closed")),
    ):
        assert await read_command_trace("t", "bg-x") == []


# ----- a ThreadStatus look that showed the end first --------------------------


async def _status_call(middleware, artifact):
    emitted: list[dict] = []
    result = ToolMessage(content="listing", tool_call_id="s1", artifact=artifact)
    request = SimpleNamespace(tool_call={"name": "ThreadStatus", "args": {}, "id": "s1"})
    with (
        patch(f"{_MW}.get_config", return_value={"configurable": {"thread_id": "thread-1"}}),
        patch(f"{_MW}.get_stream_writer", return_value=emitted.append),
        patch(_STORE_PATH, AsyncMock()),
    ):
        out = await middleware.awrap_tool_call(request, AsyncMock(return_value=result))
        await middleware._drain_body_flushes()
    return out, emitted


@pytest.mark.asyncio
async def test_a_status_look_that_stamped_a_command_brings_its_sources_with_the_notices_ids():
    """The stamp dropped the notice, so the look brings what it would have."""
    traces = AsyncMock(return_value=[_ENTRY])
    middleware = ProvenanceMiddleware(command_traces=traces)

    out, emitted = await _status_call(
        middleware, {"reported_commands": ["bg-aaaaaaaaaaaa"], "pending_read": True}
    )

    assert out.content == "listing"
    traces.assert_awaited_once_with("thread-1", "bg-aaaaaaaaaaaa")
    assert [e["record_id"] for e in emitted] == [
        s.record_id for s in sources_from_trace([_ENTRY], record_seed="bg-aaaaaaaaaaaa")
    ]
    assert emitted[0]["tool_call_id"] is None


@pytest.mark.asyncio
async def test_a_status_look_that_stamped_nothing_reads_no_trace():
    traces = AsyncMock(return_value=[_ENTRY])
    middleware = ProvenanceMiddleware(command_traces=traces)

    _, emitted = await _status_call(middleware, None)

    traces.assert_not_awaited()
    assert emitted == []


@pytest.mark.asyncio
async def test_a_subagents_status_look_emits_nothing():
    traces = AsyncMock(return_value=[_ENTRY])
    middleware = ProvenanceMiddleware(command_traces=traces)
    token = current_background_agent_id.set("research:abc")
    try:
        _, emitted = await _status_call(middleware, {"reported_commands": ["bg-aaaaaaaaaaaa"]})
    finally:
        current_background_agent_id.reset(token)
    traces.assert_not_awaited()
    assert emitted == []
