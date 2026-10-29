"""Anthropic's streamed tool arguments name their call.

Anthropic sends a call's name and id once, on its ``tool_use`` block, then
streams the arguments as ``input_json_delta`` blocks that carry neither. The
client hides and labels a preparing call by name, so the producer puts the
name and id on every chunk it emits. Block indexes restart at 0 on every model
call, so a call must never inherit what an earlier one left at its index.
"""

import pytest
from langchain_core.messages import AIMessageChunk

from src.server.services.runs.sse_producer import RunSSEProducer

# A retry or fallback inside one model task streams in the same lane.
LANE = "model:task-1"


def _tool_use(msg_id, index, name, call_id):
    return AIMessageChunk(
        content=[{"type": "tool_use", "id": call_id, "name": name, "input": {}, "index": index}],
        id=msg_id,
    )


def _delta(msg_id, index, partial_json):
    return AIMessageChunk(
        content=[{"type": "input_json_delta", "partial_json": partial_json, "index": index}],
        id=msg_id,
    )


def _finish(msg_id):
    return AIMessageChunk(content=[], id=msg_id, response_metadata={"stop_reason": "tool_use"})


async def _emit(producer, *chunks):
    frames = []
    for chunk in chunks:
        async for frame in producer._process_message_chunk(chunk, LANE, {"langgraph_node": "model"}):
            frames.append(frame)
    return frames


def _arg_chunks(frames):
    return [
        {k: chunk[k] for k in ("name", "id", "index", "args")}
        for f in frames
        if f.event == "tool_call_chunks"
        for chunk in f.data["tool_call_chunks"]
    ]


def _tool_calls(frames):
    return [call for f in frames if f.event == "tool_calls" for call in f.data["tool_calls"]]


def _producer():
    return RunSSEProducer(thread_id="t-anthropic", run_id="r-anthropic")


@pytest.mark.asyncio
async def test_each_argument_chunk_names_its_call_and_the_next_call_its_own():
    producer = _producer()
    first = await _emit(
        producer,
        _tool_use("m1", 0, "TodoWrite", "toolu_a"),
        _delta("m1", 0, ""),
        _delta("m1", 0, '{"todos": '),
        _delta("m1", 0, "[]}"),
        _finish("m1"),
    )
    second = await _emit(
        producer,
        _tool_use("m2", 0, "Bash", "toolu_b"),
        _delta("m2", 0, '{"command": '),
        _delta("m2", 0, '"ls"}'),
        _finish("m2"),
    )

    assert _arg_chunks(first) == [
        {"name": "TodoWrite", "id": "toolu_a", "index": 0, "args": ""},
        {"name": "TodoWrite", "id": "toolu_a", "index": 0, "args": '{"todos": '},
        {"name": "TodoWrite", "id": "toolu_a", "index": 0, "args": "[]}"},
    ]
    assert _arg_chunks(second) == [
        {"name": "Bash", "id": "toolu_b", "index": 0, "args": '{"command": '},
        {"name": "Bash", "id": "toolu_b", "index": 0, "args": '"ls"}'},
    ]
    assert [(c["name"], c["id"], c["args"]) for c in _tool_calls(first + second)] == [
        ("TodoWrite", "toolu_a", {"todos": []}),
        ("Bash", "toolu_b", {"command": "ls"}),
    ]
    assert producer.anthropic_tool_call_state == {}


@pytest.mark.asyncio
async def test_a_retry_takes_nothing_from_the_call_whose_stream_died():
    producer = _producer()
    # The first attempt dies mid-arguments, with no finish, leaving calls at
    # index 0 and 1; the retry writes one call, at index 0.
    await _emit(
        producer,
        _tool_use("m1", 0, "TodoWrite", "toolu_dead_0"),
        _delta("m1", 0, '{"todos": [{"content": "Che'),
        _tool_use("m1", 1, "Read", "toolu_dead_1"),
        _delta("m1", 1, '{"file_path": "/tmp/a'),
    )
    retry = await _emit(
        producer,
        _tool_use("m2", 0, "Bash", "toolu_retry"),
        _delta("m2", 0, '{"command": "ls"}'),
        _finish("m2"),
    )

    assert _arg_chunks(retry) == [
        {"name": "Bash", "id": "toolu_retry", "index": 0, "args": '{"command": "ls"}'},
    ]
    assert [(c["name"], c["id"], c["args"]) for c in _tool_calls(retry)] == [
        ("Bash", "toolu_retry", {"command": "ls"}),
    ]
    assert producer.anthropic_tool_call_state == {}


@pytest.mark.asyncio
async def test_lanes_keep_their_own_calls_at_the_same_index():
    # Another lane's message is not an earlier attempt at this one's call.
    producer = _producer()
    frames = []
    for lane, chunk in (
        ("model:task-1", _tool_use("m1", 0, "Bash", "toolu_main")),
        ("research:sub-1", _tool_use("s1", 0, "WebSearch", "toolu_sub")),
        ("model:task-1", _delta("m1", 0, '{"command": "ls"}')),
        ("research:sub-1", _delta("s1", 0, '{"query": "x"}')),
    ):
        async for frame in producer._process_message_chunk(chunk, lane, {}):
            frames.append(frame)

    assert [
        (f.data["agent"], f.data["tool_call_chunks"][0]["name"], f.data["tool_call_chunks"][0]["id"])
        for f in frames
    ] == [
        ("model:task-1", "Bash", "toolu_main"),
        ("research:sub-1", "WebSearch", "toolu_sub"),
    ]


async def _claude_call_dies(producer):
    # A Claude call streams its arguments and dies before its finish.
    await _emit(
        producer,
        _tool_use("m1", 0, "Bash", "toolu_dead"),
        _delta("m1", 0, '{"command": "ls"}'),
    )


@pytest.mark.asyncio
async def test_a_plain_answer_from_a_fallback_sends_no_call_for_the_dead_one():
    # The fallback is another provider (a Responses API model) in the same lane.
    producer = _producer()
    await _claude_call_dies(producer)
    fallback = await _emit(
        producer,
        AIMessageChunk(content=[{"type": "text", "text": "Done.", "index": 0}], id="resp_1"),
        AIMessageChunk(content=[], id="resp_1", response_metadata={"status": "completed"}),
    )

    assert _tool_calls(fallback) == []
    assert [f.data["finish_reason"] for f in fallback if "finish_reason" in f.data] == ["stop"]
    assert producer.anthropic_tool_call_state == {}


@pytest.mark.asyncio
async def test_a_fallback_sends_its_own_call_and_not_the_dead_one():
    producer = _producer()
    await _claude_call_dies(producer)
    fallback = await _emit(
        producer,
        AIMessageChunk(
            content=[{
                "type": "function_call", "name": "Read", "call_id": "call_1",
                "arguments": '{"file_path": "a.md"}', "index": 0,
            }],
            id="resp_1",
        ),
        AIMessageChunk(content=[], id="resp_1", response_metadata={"status": "completed"}),
    )

    assert [(c["name"], c["id"], c["args"]) for c in _tool_calls(fallback)] == [
        ("Read", "call_1", {"file_path": "a.md"}),
    ]
    assert producer.anthropic_tool_call_state == {}
