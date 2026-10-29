"""A finalize hands the CAS the turn's provenance and no copy of its stream.

Replay projects a settled turn from its checkpoint, so what the run streamed
has no reader on a new row; only its provenance and replay facts are kept
apart for it. A run that stopped or failed keeps the provenance of only the
tool calls its checkpoints saved: one of a step's parallel calls can stream
its sources and finish before the stop, and its result is never saved. A
source no tool call read (market watch) has no result to lose and is kept,
and a failed checkpoint read keeps every source, since the rows are their
only record.
"""

from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from langchain_core.messages import AIMessage, ToolMessage

from src.server.services.runs.finalization import assemble_finalize_artifacts
from src.server.services.runs.sse_producer import RunSSEProducer

_KEY = ("t-outcome", "r-outcome")


def _found(tool_call_id: str, identifier: str) -> dict:
    return {
        "type": "provenance",
        "record_id": f"rec-{tool_call_id}",
        "source_type": "web",
        "identifier": identifier,
        "tool_call_id": tool_call_id,
        "agent": None,
    }


_SOURCE = _found("call-1", "https://example.com/a")


class _ScriptedGraph:
    def __init__(self, script):
        self._script = script

    def astream(self, _input_state, **_kwargs):
        async def _gen():
            for step in self._script:
                yield step

        return _gen()


async def _streamed(*sources):
    handler = RunSSEProducer(thread_id=_KEY[0], run_id=_KEY[1])
    script = [((), "custom", dict(source)) for source in sources or (_SOURCE,)]
    script.append(
        ((), "updates", {"model": {"messages": [AIMessage("done", id="ai-1")]}})
    )
    async for _ in handler.stream_workflow(
        _ScriptedGraph(script), input_state={}, config={}
    ):
        pass
    return handler


async def _assemble(handler, status, *, stop_events=None):
    return await assemble_finalize_artifacts(
        _KEY,
        metadata={"handler": handler},
        status=status,
        phase="completion",
        interrupt_reason=None,
        pause_message=None,
        error=None,
        handler=handler,
        cancelled_by_user=status == "cancelled",
        workspace_id=None,
        user_id=None,
        stop_events=stop_events,
    )


def _saved(*tool_call_ids: str) -> list:
    messages: list = [
        AIMessage(
            content="",
            tool_calls=[
                {"name": "WebFetch", "args": {}, "id": tc} for tc in tool_call_ids
            ],
        )
    ]
    return messages + [
        ToolMessage(content="page", tool_call_id=tc) for tc in tool_call_ids
    ]


def _reader(run_messages=(), task_messages=None, *, error=None):
    reader = MagicMock()
    if error is not None:
        reader.aget_run_state = AsyncMock(side_effect=error)
    else:
        reader.aget_run_state = AsyncMock(
            return_value=SimpleNamespace(values={"messages": list(run_messages)})
        )
    reader.aget_task_history = AsyncMock(
        side_effect=lambda _thread, task_id: SimpleNamespace(
            messages=(task_messages or {})[task_id]
        )
    )
    return reader


def _lane_source(agent: str, tool_call_id: str, identifier: str) -> dict:
    return {
        "event": "provenance",
        "data": {
            "agent": agent,
            "tool_call_id": tool_call_id,
            "source_type": "web",
            "identifier": identifier,
        },
    }


def _identifiers(outcome) -> list[str]:
    return [e["data"]["identifier"] for e in outcome.record.provenance]


@pytest.mark.asyncio
async def test_the_outcome_carries_provenance_and_no_events():
    handler = await _streamed()
    reader = _reader()
    with patch(
        "src.server.services.history.reader.CheckpointHistoryReader.get_instance",
        return_value=reader,
    ):
        outcome = await _assemble(handler, "completed")

    assert getattr(outcome, "sse_events", None) is None
    [record] = outcome.record.provenance
    assert record["event"] == "provenance"
    assert record["data"]["identifier"] == "https://example.com/a"
    assert record["data"]["agent"] == "main"
    # A completed run saved everything it streamed.
    reader.aget_run_state.assert_not_awaited()


# The stop landed after the second fetch streamed its source and before its
# step saved its result; the first fetch's result was saved a step earlier.
_TWO = (_found("call-1", "https://one.test"), _found("call-2", "https://two.test"))


@pytest.mark.asyncio
@pytest.mark.parametrize("status", ["cancelled", "error"])
async def test_a_stopped_or_failed_run_keeps_only_its_saved_calls_sources(status):
    handler = await _streamed(*_TWO)
    lanes = [
        _lane_source("task:k1", "tc-a", "https://a.test"),
        _lane_source("task:k1", "tc-b", "https://b.test"),
    ]
    reader = _reader(_saved("call-1"), {"k1": _saved("tc-a")})
    with patch(
        "src.server.services.history.reader.CheckpointHistoryReader.get_instance",
        return_value=reader,
    ):
        outcome = await _assemble(handler, status, stop_events=lanes)

    assert _identifiers(outcome) == ["https://one.test", "https://a.test"]
    reader.aget_run_state.assert_awaited_once_with(*_KEY)


@pytest.mark.asyncio
@pytest.mark.parametrize("status", ["completed", "interrupted"])
async def test_a_run_whose_results_are_saved_or_resumed_keeps_every_source(status):
    # An interrupted run's finished calls are committed by its resume.
    handler = await _streamed(*_TWO)
    reader = _reader(_saved("call-1"))
    with patch(
        "src.server.services.history.reader.CheckpointHistoryReader.get_instance",
        return_value=reader,
    ):
        outcome = await _assemble(handler, status)

    assert _identifiers(outcome) == ["https://one.test", "https://two.test"]
    reader.aget_run_state.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("status", ["cancelled", "error"])
async def test_a_source_no_tool_call_read_is_kept(status):
    watched = {
        **_found("unused", "AAPL"),
        "tool_call_id": None,
        "detail": "market_watch",
    }
    handler = await _streamed(*_TWO, watched)
    with patch(
        "src.server.services.history.reader.CheckpointHistoryReader.get_instance",
        return_value=_reader(_saved("call-1")),
    ):
        outcome = await _assemble(handler, status)

    assert _identifiers(outcome) == ["https://one.test", "AAPL"]


@pytest.mark.asyncio
async def test_a_failed_checkpoint_read_keeps_every_source_and_still_finalizes():
    # The stop's drained lane goes with them: no later archive takes it.
    handler = await _streamed(*_TWO)
    lane = [_lane_source("task:k1", "tc-a", "https://a.test")]
    with patch(
        "src.server.services.history.reader.CheckpointHistoryReader.get_instance",
        return_value=_reader(error=RuntimeError("pool closed")),
    ):
        outcome = await _assemble(handler, "cancelled", stop_events=lane)

    assert outcome.status == "cancelled"
    assert _identifiers(outcome) == [
        "https://one.test",
        "https://two.test",
        "https://a.test",
    ]
