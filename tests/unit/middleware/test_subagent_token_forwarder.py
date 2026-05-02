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
    return task


@pytest.mark.asyncio
async def test_forwards_text_chunks_one_per_token():
    registry = BackgroundTaskRegistry()
    task = await _register(registry)
    fw = _SubagentTokenForwarder(registry, task.tool_call_id, "task:abc")

    await fw.forward(_chunk("Hel"))
    await fw.forward(_chunk("lo"))
    await fw.forward(_chunk(", world"))
    await fw.finalize()

    events = list(task.captured_events_tail)
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

    events = list(task.captured_events_tail)
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

    events = list(task.captured_events_tail)
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
        e["event"] != "subagent_stream_end" for e in task.captured_events_tail
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

    events = list(task.captured_events_tail)
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

    events = list(task.captured_events_tail)
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

    assert list(task.captured_events_tail) == []


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
        e for e in task.captured_events_tail
        if e["event"] == "message_chunk"
        and e["data"].get("content_type") == "text"
    ]
    assert [e["data"]["content"] for e in text_chunks] == ["Hel", "lo", ", world"]
    assert {e["data"]["agent"] for e in text_chunks} == {"task:taskpipe"}
