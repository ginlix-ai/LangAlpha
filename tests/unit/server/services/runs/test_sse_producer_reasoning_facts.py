"""How long the main agent thought, kept for replay.

The checkpoint keeps a turn's reasoning but not its duration, so the producer
records each closed reasoning block's ``elapsed_ms`` and the finalize stores
them on the run's row. A model call streams under a chunk id the checkpoint
never sees, so a close is attributed only when the node's update names the
message it committed, and a call that committed nothing takes its closes with
it. Each test drives the real ``stream_workflow`` loop over a scripted graph.
"""

import json
import time
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest
from langchain_core.messages import AIMessage, AIMessageChunk, HumanMessage, ToolMessage
from langgraph.types import Overwrite

from src.server.services.runs import sse_producer
from src.server.services.runs.sse_producer import RunSSEProducer

_RUNNER = "src.server.services.thread_mutation.ThreadMutationRunner.get_instance"
_SUMMARIZE_NODE = "SummarizationMiddleware.before_model"


class _Clock:
    def __init__(self):
        self.now = 100.0

    def monotonic(self):
        return self.now

    def advance(self, seconds):
        return lambda: setattr(self, "now", self.now + seconds)


class _ScriptedGraph:
    """Yields each ``(namespace, stream_mode, payload)`` tuple of the script
    and runs each callable in place, so a test can move the clock or look at
    the producer between two events."""

    def __init__(self, script):
        self._script = script

    def astream(
        self,
        _input_state,
        config=None,
        stream_mode=None,
        subgraphs=None,
        durability=None,
    ):
        async def _gen():
            for step in self._script:
                if callable(step):
                    step()
                else:
                    yield step

        return _gen()


@pytest.fixture
def clock(monkeypatch):
    # Swap the module's ``time`` rather than ``time.monotonic`` itself, which
    # the event loop reads too.
    fake = _Clock()
    monkeypatch.setattr(
        sse_producer, "time", SimpleNamespace(monotonic=fake.monotonic, time=time.time)
    )
    return fake


def _producer():
    return RunSSEProducer(thread_id="t-facts", run_id="r-facts")


async def _run(handler, script):
    return [
        event
        async for event in handler.stream_workflow(
            _ScriptedGraph(script), input_state={}, config={}
        )
    ]


def _think(chunk_id, *, ns=(), node="model"):
    chunk = AIMessageChunk(
        content="",
        id=chunk_id,
        additional_kwargs={"reasoning_content": "Weighing the filing."},
    )
    return (ns, "messages", (chunk, {"langgraph_node": node}))


def _say(chunk_id, *, ns=(), node="model"):
    chunk = AIMessageChunk(content="The answer.", id=chunk_id)
    return (ns, "messages", (chunk, {"langgraph_node": node}))


def _update(node, messages, *, ns=()):
    return (ns, "updates", {node: {"messages": messages}})


def _committed(message_id):
    return AIMessage(content="The answer.", id=message_id)


def _reasoning_closes(events):
    """The ``complete`` reasoning signals the stream emitted, as payloads."""
    closes = []
    for event in events:
        data = json.loads(event.split("data: ", 1)[1])
        if (
            data.get("content_type") == "reasoning_signal"
            and data.get("content") == "complete"
        ):
            closes.append((event.split("\n")[1], data))
    return closes


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "messages",
    [
        [HumanMessage(content="What changed?", id="h-1"), _committed("msg_123")],
        Overwrite(
            [
                _committed("msg_old"),
                HumanMessage(content="And now?"),
                _committed("msg_123"),
            ]
        ),
        _committed("msg_123"),
    ],
    ids=["list", "overwrite", "single"],
)
async def test_a_close_lands_on_the_message_the_node_committed(clock, messages):
    handler = _producer()
    await _run(
        handler,
        [
            _think("lc_run--abc"),
            clock.advance(0.25),
            _say("lc_run--abc"),
            _update("model", messages),
        ],
    )
    assert handler.record().reasoning_ms == {"msg_123": [250]}


@pytest.mark.asyncio
async def test_two_thinking_blocks_in_one_call_land_in_order(clock):
    handler = _producer()
    await _run(
        handler,
        [
            _think("lc_run--abc"),
            clock.advance(0.25),
            _say("lc_run--abc"),
            _think("lc_run--abc"),
            clock.advance(0.5),
            _say("lc_run--abc"),
            _update("model", [_committed("msg_123")]),
        ],
    )
    assert handler.record().reasoning_ms == {"msg_123": [250, 500]}


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("retry", "expected"),
    [
        (
            lambda clock: [
                _think("lc_run--second"),
                clock.advance(0.5),
                _say("lc_run--second"),
            ],
            {"msg_2": [500]},
        ),
        (lambda clock: [_say("lc_run--second")], {}),
    ],
    ids=["retry_thought", "retry_did_not_think"],
)
async def test_a_replaced_attempt_takes_its_closes_with_it(clock, retry, expected):
    """A retry inside the node streams under a new chunk id; only the newest
    call's thinking belongs to the message the node commits."""
    handler = _producer()
    await _run(
        handler,
        [
            _think("lc_run--first"),
            clock.advance(0.25),
            _say("lc_run--first"),
            *retry(clock),
            _update("model", [_committed("msg_2")]),
        ],
    )
    assert handler.record().reasoning_ms == expected


@pytest.mark.asyncio
async def test_another_nodes_update_leaves_the_closes_waiting(clock):
    handler = _producer()
    seen = []
    await _run(
        handler,
        [
            _think("lc_run--abc"),
            clock.advance(0.25),
            _say("lc_run--abc"),
            _update(
                "tools", [ToolMessage(content="42", tool_call_id="call-1", id="tool-1")]
            ),
            lambda: seen.append(handler.record().reasoning_ms),
            _update("model", [_committed("msg_123")]),
        ],
    )
    assert seen == [{}]
    assert handler.record().reasoning_ms == {"msg_123": [250]}


@pytest.mark.asyncio
async def test_a_subagents_thinking_records_nothing(clock):
    ns = ("task:k7",)
    handler = _producer()
    events = await _run(
        handler,
        [
            _think("lc_run--sub", ns=ns),
            clock.advance(0.25),
            _say("lc_run--sub", ns=ns),
            _update("model", [_committed("msg_sub")], ns=ns),
            _say("lc_run--main"),
            _update("model", [_committed("msg_main")]),
        ],
    )
    # The block still closed with its duration on the wire; it just is not
    # the main reply's thinking, not even the reply the main agent commits
    # after it.
    assert [
        (line, data["agent"], data["elapsed_ms"])
        for line, data in _reasoning_closes(events)
    ] == [("event: message_chunk", "task:k7", 250)]
    assert handler.record().reasoning_ms == {}


@pytest.mark.asyncio
async def test_a_compactions_thinking_records_nothing(clock):
    summarize = {"type": "context_window", "action": "summarize"}
    handler = _producer()
    with patch(_RUNNER, return_value=MagicMock()):
        events = await _run(
            handler,
            [
                ((), "custom", {**summarize, "signal": "start"}),
                _think("lc_run--sum", node=_SUMMARIZE_NODE),
                clock.advance(0.25),
                _say("lc_run--sum", node=_SUMMARIZE_NODE),
                ((), "custom", {**summarize, "signal": "complete"}),
                _update(_SUMMARIZE_NODE, [_committed("msg_sum")]),
            ],
        )
    assert [(line, data["elapsed_ms"]) for line, data in _reasoning_closes(events)] == [
        ("event: compaction_chunk", 250)
    ]
    assert handler.record().reasoning_ms == {}


@pytest.mark.asyncio
async def test_nothing_measured_is_an_empty_record(clock):
    assert _producer().record().reasoning_ms == {}

    handler = _producer()
    await _run(
        handler, [_say("lc_run--abc"), _update("model", [_committed("msg_123")])]
    )
    assert handler.record().reasoning_ms == {}
