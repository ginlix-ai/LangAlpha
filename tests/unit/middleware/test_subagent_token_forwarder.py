"""Per-token streaming forwarder tests.

Locks in the contract that ``_SubagentTokenForwarder`` mirrors the main
streaming handler's reasoning lifecycle (start on first reasoning chunk,
complete on transition to text or message-id change) and forwards each
``messages``-mode chunk as one captured-event record on the registry.
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from ptc_agent.agent.middleware.background_subagent.registry import (
    BackgroundTaskRegistry,
)
from ptc_agent.agent.middleware.background_subagent.subagent import (
    _SubagentTokenForwarder,
)


def _chunk(content, msg_id="msg-1", reasoning_kw=None):
    """Build a fake message chunk with content and optional reasoning kwarg."""
    chunk = MagicMock()
    chunk.content = content
    chunk.id = msg_id
    chunk.additional_kwargs = {}
    if reasoning_kw is not None:
        chunk.additional_kwargs["reasoning_content"] = reasoning_kw
    return chunk


async def _register(registry: BackgroundTaskRegistry, task_id_override="abc"):
    task = await registry.register(
        tool_call_id=f"tc-{task_id_override}",
        description="d",
        prompt="p",
        subagent_type="general-purpose",
    )
    if task.task_id != task_id_override:
        registry._task_id_to_tool_call_id.pop(task.task_id, None)
        task.task_id = task_id_override
        registry._task_id_to_tool_call_id[task_id_override] = task.tool_call_id
    _patch_capture(registry, task)
    return task


def _patch_capture(registry: BackgroundTaskRegistry, task) -> None:
    """Patch ``registry.append_captured_event`` to record into ``task._test_records``.

    The production path spills to Redis; the registry under test has no
    thread_id so the spill no-ops, and the deque it used to write to is
    gone. The patched method preserves the bookkeeping side-effects
    (seq counter, count, last_updated_at) and exposes the records on
    ``task._test_records`` so assertions can read them.
    """
    import time as _time

    task._test_records: list[dict] = []

    async def recording_append(tool_call_id, event):
        async with registry._lock:
            t = registry._tasks.get(tool_call_id)
            if not t:
                return
            t.captured_event_seq += 1
            seq = t.captured_event_seq
            ts = event.get("ts")
            record = {
                "seq": seq,
                "event": event.get("event"),
                "data": event.get("data") or {},
                "agent_id": t.agent_id,
            }
            if ts is not None:
                record["ts"] = ts
            t.captured_event_count = seq
            t._test_records.append(record)
            if (
                event.get("event") == "message_chunk"
                and (event.get("data") or {}).get("content_type") == "text"
            ):
                t.last_updated_at = _time.time()

    registry.append_captured_event = recording_append  # type: ignore[method-assign]


@pytest.mark.asyncio
async def test_forwards_text_chunks_one_per_token():
    registry = BackgroundTaskRegistry()
    task = await _register(registry)
    fw = _SubagentTokenForwarder(registry, task.tool_call_id, "task:abc")

    await fw.forward(_chunk("Hel"))
    await fw.forward(_chunk("lo"))
    await fw.forward(_chunk(", world"))
    await fw.finalize()

    events = task._test_records
    text_chunks = [
        e for e in events
        if e["event"] == "message_chunk"
        and e["data"].get("content_type") == "text"
    ]
    # Three forwarded chunks → three records, each carrying its own slice.
    assert [e["data"]["content"] for e in text_chunks] == ["Hel", "lo", ", world"]
    # Every record carries the canonical agent_id injected by the forwarder.
    assert {e["data"]["agent"] for e in text_chunks} == {"task:abc"}
    # No reasoning lifecycle for pure-text streams.
    sig_chunks = [
        e for e in events
        if e["data"].get("content_type") == "reasoning_signal"
    ]
    assert sig_chunks == []


@pytest.mark.asyncio
async def test_reasoning_lifecycle_emits_inline_start_and_complete_on_transition():
    """First reasoning chunk → emit start signal. Transition to text → emit
    complete signal. Mirrors WorkflowStreamHandler._process_message_chunk."""
    registry = BackgroundTaskRegistry()
    task = await _register(registry)
    fw = _SubagentTokenForwarder(registry, task.tool_call_id, "task:abc")

    await fw.forward(_chunk({"type": "thinking", "thinking": "step one"}))
    await fw.forward(_chunk({"type": "thinking", "thinking": " step two"}))
    await fw.forward(_chunk("here is the answer"))
    await fw.finalize()

    events = task._test_records
    timeline = [
        (e["data"].get("content_type"), e["data"].get("content"))
        for e in events
        if e["event"] == "message_chunk"
    ]

    # Expected sequence:
    # 1. reasoning_signal "start" (inline with first reasoning chunk)
    # 2. reasoning chunk "step one"
    # 3. reasoning chunk " step two"
    # 4. reasoning_signal "complete" (transition reasoning → text)
    # 5. text chunk "here is the answer"
    assert timeline == [
        ("reasoning_signal", "start"),
        ("reasoning", "step one"),
        ("reasoning", " step two"),
        ("reasoning_signal", "complete"),
        ("text", "here is the answer"),
    ]


@pytest.mark.asyncio
async def test_finalize_closes_dangling_reasoning_signal():
    """If a run ends while reasoning is still active (LLM returned reasoning
    only, no text), finalize must emit the complete signal so the frontend's
    reasoning UI doesn't stay open forever."""
    registry = BackgroundTaskRegistry()
    task = await _register(registry)
    fw = _SubagentTokenForwarder(registry, task.tool_call_id, "task:abc")

    await fw.forward(_chunk({"type": "thinking", "thinking": "lone thought"}))
    await fw.finalize()

    events = task._test_records
    timeline = [
        (e["data"].get("content_type"), e["data"].get("content"))
        for e in events
        if e["event"] == "message_chunk"
    ]
    assert timeline == [
        ("reasoning_signal", "start"),
        ("reasoning", "lone thought"),
        ("reasoning_signal", "complete"),
    ]


@pytest.mark.asyncio
async def test_finalize_emits_stream_end_sentinel():
    """The per-task SSE consumer's only signal that the subagent has finished
    streaming is a ``subagent_stream_end`` sentinel record on the per-task
    Redis Stream. ``finalize()`` must write it via
    ``append_sentinel_to_stream`` — without that the consumer falls back to
    polling ``task.asyncio_task.done()`` between BLOCK timeouts and the
    frontend card stays "Running" until the post-turn collector flips
    ``task.completed``.

    The sentinel must NOT land in ``captured_events_tail`` (which gets
    persisted to Postgres + replayed on history load) — it's a transport
    signal, not content.
    """
    registry = BackgroundTaskRegistry()
    task = await _register(registry)
    fw = _SubagentTokenForwarder(registry, task.tool_call_id, "task:abc")

    sentinel_calls = []

    async def fake_sentinel(tool_call_id):
        sentinel_calls.append(tool_call_id)

    registry.append_sentinel_to_stream = fake_sentinel  # type: ignore[method-assign]

    await fw.forward(_chunk("Hello"))
    await fw.finalize()

    assert sentinel_calls == [task.tool_call_id]
    # The deque should hold only the real text chunk — no sentinel record.
    assert all(
        e["event"] != "subagent_stream_end" for e in task._test_records
    )


@pytest.mark.asyncio
async def test_finalize_sentinel_failure_does_not_propagate():
    """Sentinel write is best-effort — if Redis is degraded or
    ``append_sentinel_to_stream`` raises, ``finalize`` must still return
    normally so the parent ``_arun_subagent_streaming`` finally-block
    completes. The terminal_check fallback closes the stream eventually.
    """
    registry = BackgroundTaskRegistry()
    task = await _register(registry)
    fw = _SubagentTokenForwarder(registry, task.tool_call_id, "task:abc")

    async def boom(_tool_call_id):
        raise RuntimeError("redis is on fire")

    registry.append_sentinel_to_stream = boom  # type: ignore[method-assign]

    # Should not raise.
    await fw.finalize()


@pytest.mark.asyncio
async def test_forward_error_appends_error_record():
    """forward_error spills an SSE error record with the canonical agent_id,
    the exception message, and the exception type name. This is what lets
    per-task SSE consumers distinguish a crashed subagent from a clean close.
    """
    registry = BackgroundTaskRegistry()
    task = await _register(registry)
    fw = _SubagentTokenForwarder(registry, task.tool_call_id, "task:abc")

    await fw.forward_error(RuntimeError("upstream blew up"))

    error_records = [e for e in task._test_records if e["event"] == "error"]
    assert len(error_records) == 1
    assert error_records[0]["data"] == {
        "agent": "task:abc",
        "message": "upstream blew up",
        "error_type": "RuntimeError",
    }


@pytest.mark.asyncio
async def test_forward_error_absorbs_registry_failure():
    """If append_captured_event raises (degraded Redis), forward_error must
    not propagate — it is always called inside an existing exception flow
    and must not mask the original error.
    """
    registry = BackgroundTaskRegistry()
    task = await _register(registry)
    fw = _SubagentTokenForwarder(registry, task.tool_call_id, "task:abc")

    async def boom(_tool_call_id, _record):
        raise RuntimeError("registry on fire")

    registry.append_captured_event = boom  # type: ignore[method-assign]

    # Should not raise.
    await fw.forward_error(ValueError("original error"))


@pytest.mark.asyncio
async def test_arun_subagent_streaming_emits_error_event_on_exception(monkeypatch):
    """End-to-end: when the subagent.astream raises, the per-task SSE stream
    sees an `error` event carrying the exception message + type before the
    subagent_stream_end sentinel, and the original exception propagates out
    of _arun_subagent_streaming so the registry's outer wrapper can record it
    on task.error.
    """
    from ptc_agent.agent.middleware.background_subagent import subagent as sa
    from ptc_agent.agent.middleware.background_subagent.middleware import (
        current_background_tool_call_id,
    )

    parent_config = {"configurable": {"thread_id": "t1"}}
    monkeypatch.setattr(
        "ptc_agent.agent.middleware.background_subagent.subagent.get_config",
        lambda: parent_config,
    )

    registry = BackgroundTaskRegistry()
    task = await _register(registry, task_id_override="taskerr")

    async def fake_astream(state, config, stream_mode=None):
        yield ("messages", (_chunk("partial"), {}))
        raise RuntimeError("model crashed mid-stream")

    fake_subagent = MagicMock()
    fake_subagent.astream = fake_astream

    tool = sa._create_task_tool(
        default_model=MagicMock(),
        default_tools=[],
        default_middleware=[],
        default_interrupt_on=None,
        subagents=[],
        general_purpose_agent=False,
        registry=registry,
        checkpointer=None,
    )
    coroutine = tool.coroutine
    closure_vars = {
        cell_name: cell.cell_contents
        for cell_name, cell in zip(
            coroutine.__code__.co_freevars,
            coroutine.__closure__ or (),
        )
    }
    sg = closure_vars["subagent_graphs"]
    sg["general-purpose"] = fake_subagent

    runtime = MagicMock()
    runtime.state = {"messages": []}
    runtime.tool_call_id = "tc-err"

    # The Task tool's "init" path awaits _arun_subagent_streaming directly
    # rather than scheduling it as a background asyncio task, so the
    # RuntimeError propagates out of the coroutine. The contract under test
    # is that forward_error was called BEFORE the re-raise: the captured
    # events on the registered task should already contain the error record.
    token = current_background_tool_call_id.set(task.tool_call_id)
    try:
        with pytest.raises(RuntimeError, match="model crashed mid-stream"):
            await coroutine(
                description="d",
                prompt="p",
                subagent_type="general-purpose",
                action="init",
                task_id=None,
                runtime=runtime,
            )
    finally:
        current_background_tool_call_id.reset(token)

    events = task._test_records
    error_records = [e for e in events if e["event"] == "error"]
    assert len(error_records) == 1, f"expected 1 error event, got events={events}"
    assert error_records[0]["data"]["message"] == "model crashed mid-stream"
    assert error_records[0]["data"]["error_type"] == "RuntimeError"
    assert error_records[0]["data"]["agent"] == "task:taskerr"


@pytest.mark.asyncio
async def test_message_id_change_closes_prior_reasoning():
    """A new message_id with reasoning still active means the prior LLM call
    finished mid-reasoning. Close the old lifecycle before starting fresh."""
    registry = BackgroundTaskRegistry()
    task = await _register(registry)
    fw = _SubagentTokenForwarder(registry, task.tool_call_id, "task:abc")

    await fw.forward(
        _chunk({"type": "thinking", "thinking": "first call"}, msg_id="msg-A")
    )
    # New message id with reasoning still active.
    await fw.forward(
        _chunk({"type": "thinking", "thinking": "second call"}, msg_id="msg-B")
    )
    await fw.finalize()

    events = task._test_records
    msg_ids_and_types = [
        (e["data"]["id"], e["data"].get("content_type"), e["data"].get("content"))
        for e in events
        if e["event"] == "message_chunk"
    ]
    assert msg_ids_and_types == [
        ("msg-A", "reasoning_signal", "start"),
        ("msg-A", "reasoning", "first call"),
        ("msg-A", "reasoning_signal", "complete"),  # closed on msg-id change
        ("msg-B", "reasoning_signal", "start"),
        ("msg-B", "reasoning", "second call"),
        ("msg-B", "reasoning_signal", "complete"),  # closed by finalize
    ]


@pytest.mark.asyncio
async def test_reasoning_via_additional_kwargs_is_normalized():
    """Some providers stream reasoning under ``additional_kwargs.reasoning_content``
    rather than as content. Forwarder must promote it to a reasoning chunk."""
    registry = BackgroundTaskRegistry()
    task = await _register(registry)
    fw = _SubagentTokenForwarder(registry, task.tool_call_id, "task:abc")

    await fw.forward(_chunk("", reasoning_kw="kw-only thought"))
    await fw.finalize()

    events = task._test_records
    types_and_content = [
        (e["data"].get("content_type"), e["data"].get("content"))
        for e in events
        if e["event"] == "message_chunk"
    ]
    assert types_and_content == [
        ("reasoning_signal", "start"),
        ("reasoning", "kw-only thought"),
        ("reasoning_signal", "complete"),
    ]


@pytest.mark.asyncio
async def test_empty_chunks_are_skipped():
    """Provider keepalive / empty chunks must not produce records."""
    registry = BackgroundTaskRegistry()
    task = await _register(registry)
    fw = _SubagentTokenForwarder(registry, task.tool_call_id, "task:abc")

    await fw.forward(_chunk(""))
    await fw.forward(_chunk(None))
    await fw.finalize()

    assert task._test_records == []


@pytest.mark.asyncio
async def test_tool_node_inner_llm_chunks_skipped():
    """Inner-LLM chunks streamed from inside a tool body (e.g. WebFetch's
    extraction model) must not be forwarded as subagent reasoning. The tool's
    user-facing output arrives via ``tool_call_result``; surfacing the inner
    model's CoT here renders the extraction prompt's analysis as the
    subagent's own reasoning. Gate is keyed on ``langgraph_node="tools"``,
    matching the gate in ``streaming_handler._process_message_chunk``."""
    registry = BackgroundTaskRegistry()
    task = await _register(registry)
    fw = _SubagentTokenForwarder(registry, task.tool_call_id, "task:abc")

    # Inner extraction-LLM reasoning that previously leaked into the chat.
    await fw.forward(
        _chunk("We need to extract top 5-10 market news headlines"),
        metadata={"langgraph_node": "tools"},
    )
    await fw.forward(
        _chunk("Format as markdown list."),
        metadata={"langgraph_node": "tools"},
    )
    # And the subagent's own model-node chunk that must still flow through.
    await fw.forward(
        _chunk("Subagent's own thinking", reasoning_kw="Subagent's own thinking"),
        metadata={"langgraph_node": "model"},
    )
    await fw.finalize()

    text_chunks = [
        e for e in task._test_records
        if e["event"] == "message_chunk"
        and e["data"].get("content_type") in ("text", "reasoning")
    ]
    contents = [e["data"]["content"] for e in text_chunks]
    # Tool-node chunks dropped; model-node reasoning kept.
    assert "We need to extract top 5-10 market news headlines" not in contents
    assert "Format as markdown list." not in contents
    assert "Subagent's own thinking" in contents


@pytest.mark.asyncio
async def test_no_metadata_does_not_skip():
    """Backwards compat: without metadata (legacy callers, mocks), forward
    normally — no false suppression."""
    registry = BackgroundTaskRegistry()
    task = await _register(registry)
    fw = _SubagentTokenForwarder(registry, task.tool_call_id, "task:abc")

    await fw.forward(_chunk("Plain content"))
    await fw.forward(_chunk("More content"), metadata=None)
    await fw.finalize()

    text = [
        e["data"]["content"]
        for e in task._test_records
        if e["data"].get("content_type") == "text"
    ]
    assert text == ["Plain content", "More content"]


@pytest.mark.asyncio
async def test_atask_pipeline_forwards_messages_chunks_to_registry(monkeypatch):
    """End-to-end: when the Task tool drives the subagent through astream,
    each ``messages``-mode chunk lands as a captured-event record on the
    registry — i.e. on the per-task SSE stream the frontend will read."""
    from ptc_agent.agent.middleware.background_subagent import subagent as sa
    from ptc_agent.agent.middleware.background_subagent.middleware import (
        current_background_tool_call_id,
    )

    parent_config = {"configurable": {"thread_id": "t1"}}
    monkeypatch.setattr(
        "ptc_agent.agent.middleware.background_subagent.subagent.get_config",
        lambda: parent_config,
    )

    registry = BackgroundTaskRegistry()
    task = await _register(registry, task_id_override="taskpipe")

    async def fake_astream(state, config, stream_mode=None):
        # Three text-token chunks then a final values yield.
        yield ("messages", (_chunk("Hel"), {}))
        yield ("messages", (_chunk("lo"), {}))
        yield ("messages", (_chunk(", world"), {}))
        yield ("values", {"messages": [MagicMock(text="final")]})

    fake_subagent = MagicMock()
    fake_subagent.astream = fake_astream

    tool = sa._create_task_tool(
        default_model=MagicMock(),
        default_tools=[],
        default_middleware=[],
        default_interrupt_on=None,
        subagents=[],
        general_purpose_agent=False,
        registry=registry,
        checkpointer=None,
    )
    coroutine = tool.coroutine
    closure_vars = {
        cell_name: cell.cell_contents
        for cell_name, cell in zip(
            coroutine.__code__.co_freevars,
            coroutine.__closure__ or (),
        )
    }
    sg = closure_vars["subagent_graphs"]
    sg["general-purpose"] = fake_subagent

    runtime = MagicMock()
    runtime.state = {"messages": []}
    runtime.tool_call_id = "tc-pipe"

    # current_background_tool_call_id must point at the registered task —
    # the forwarder uses it to resolve the agent_id.
    token = current_background_tool_call_id.set(task.tool_call_id)
    try:
        await coroutine(
            description="d",
            prompt="p",
            subagent_type="general-purpose",
            action="init",
            task_id=None,
            runtime=runtime,
        )
    finally:
        current_background_tool_call_id.reset(token)

    text_chunks = [
        e for e in task._test_records
        if e["event"] == "message_chunk"
        and e["data"].get("content_type") == "text"
    ]
    assert [e["data"]["content"] for e in text_chunks] == ["Hel", "lo", ", world"]
    assert {e["data"]["agent"] for e in text_chunks} == {"task:taskpipe"}


# ---------------------------------------------------------------------------
# custom-mode forwarding — surfaces compaction's get_stream_writer events
# (context_window token_usage / summarize / offload) that ride a separate
# stream channel from messages-mode chunks.
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_forward_custom_appends_context_window_event() -> None:
    """A ``custom``-mode dict with ``type=context_window`` lands as a captured
    record with the stable ``task:{task_id}`` agent_id."""
    registry = BackgroundTaskRegistry()
    task = await _register(registry, task_id_override="abc123")
    fwd = _SubagentTokenForwarder(registry, task.tool_call_id, "task:abc123")

    await fwd.forward_custom(
        {
            "type": "context_window",
            "action": "token_usage",
            "signal": "complete",
            "input_tokens": 100,
            "output_tokens": 40,
            "total_tokens": 140,
        }
    )

    records = task._test_records
    assert len(records) == 1
    rec = records[0]
    assert rec["event"] == "context_window"
    data = rec["data"]
    assert data["agent"] == "task:abc123"
    assert data["action"] == "token_usage"
    assert data["input_tokens"] == 100
    assert data["output_tokens"] == 40
    assert data["total_tokens"] == 140
    # ``type`` is consumed as the event name and stripped from data.
    assert "type" not in data


@pytest.mark.asyncio
async def test_forward_custom_ignores_non_dict() -> None:
    """Non-dict custom payloads (e.g. legacy strings) are dropped silently."""
    registry = BackgroundTaskRegistry()
    task = await _register(registry, task_id_override="abc")
    fwd = _SubagentTokenForwarder(registry, task.tool_call_id, "task:abc")

    await fwd.forward_custom("not-a-dict")
    await fwd.forward_custom(None)
    await fwd.forward_custom(42)

    assert task._test_records == []


@pytest.mark.asyncio
async def test_forward_custom_drops_non_whitelisted_event_types() -> None:
    """Custom payloads with unrecognized ``type`` values are dropped to avoid
    bloating the per-task buffer with file-op / widget payloads and to close
    a frontend protocol-injection vector — a custom emitter could otherwise
    send ``type: "message_chunk"`` and spoof a real subagent SSE event."""
    registry = BackgroundTaskRegistry()
    task = await _register(registry, task_id_override="wl")
    fwd = _SubagentTokenForwarder(registry, task.tool_call_id, "task:wl")

    # Frontend protocol events — must not pass through.
    await fwd.forward_custom({"type": "message_chunk", "content": "boo"})
    await fwd.forward_custom({"type": "tool_call_result", "result": "x"})
    await fwd.forward_custom({"type": "tool_calls", "tool_calls": []})
    # File-op / widget-style payloads — must not pass through.
    await fwd.forward_custom({"type": "file_op", "old_string": "a" * 10000})
    await fwd.forward_custom({"type": "widget", "html": "<div/>"})
    # Missing ``type`` entirely — must not pass through (no implicit "custom").
    await fwd.forward_custom({"foo": "bar"})

    assert task._test_records == []


@pytest.mark.asyncio
async def test_atask_pipeline_forwards_custom_events_to_registry(monkeypatch):
    """End-to-end: when the subagent emits a ``custom``-mode payload, the
    Task-tool driver routes it through ``forward_custom`` so the per-task
    buffer carries it for SSE replay."""
    from ptc_agent.agent.middleware.background_subagent import subagent as sa
    from ptc_agent.agent.middleware.background_subagent.middleware import (
        current_background_tool_call_id,
    )

    parent_config = {"configurable": {"thread_id": "t1"}}
    monkeypatch.setattr(
        "ptc_agent.agent.middleware.background_subagent.subagent.get_config",
        lambda: parent_config,
    )

    registry = BackgroundTaskRegistry()
    task = await _register(registry, task_id_override="custompipe")

    async def fake_astream(state, config, stream_mode=None):
        # The driver must subscribe to ``custom`` for this to surface.
        assert "custom" in (stream_mode or [])
        yield (
            "custom",
            {
                "type": "context_window",
                "action": "token_usage",
                "signal": "complete",
                "input_tokens": 50,
                "output_tokens": 10,
                "total_tokens": 60,
            },
        )
        yield ("values", {"messages": [MagicMock(text="final")]})

    fake_subagent = MagicMock()
    fake_subagent.astream = fake_astream

    tool = sa._create_task_tool(
        default_model=MagicMock(),
        default_tools=[],
        default_middleware=[],
        default_interrupt_on=None,
        subagents=[],
        general_purpose_agent=False,
        registry=registry,
        checkpointer=None,
    )
    coroutine = tool.coroutine
    closure_vars = {
        cell_name: cell.cell_contents
        for cell_name, cell in zip(
            coroutine.__code__.co_freevars,
            coroutine.__closure__ or (),
        )
    }
    sg = closure_vars["subagent_graphs"]
    sg["general-purpose"] = fake_subagent

    runtime = MagicMock()
    runtime.state = {"messages": []}
    runtime.tool_call_id = "tc-pipe"

    token = current_background_tool_call_id.set(task.tool_call_id)
    try:
        await coroutine(
            description="d",
            prompt="p",
            subagent_type="general-purpose",
            action="init",
            task_id=None,
            runtime=runtime,
        )
    finally:
        current_background_tool_call_id.reset(token)

    cw_events = [
        e for e in task._test_records if e["event"] == "context_window"
    ]
    assert len(cw_events) == 1
    data = cw_events[0]["data"]
    assert data["agent"] == "task:custompipe"
    assert data["action"] == "token_usage"
    assert data["total_tokens"] == 60
