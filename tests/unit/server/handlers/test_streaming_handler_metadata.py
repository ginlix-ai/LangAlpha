"""Tests for the per-stream ``metadata`` SSE frame produced by
``RunSSEProducer.stream_workflow``.

Contract: ``run_id`` is REQUIRED at construction time. Every stream
announces that ``run_id`` as the FIRST yielded SSE chunk (``event:
metadata``). Frontend reconnect / demotion / steering boundary logic
latches onto this frame, so it must appear before any model output, and
the handler must not be constructible without a real ``run_id`` (a
``None`` would leak through the Redis event log and poison the
frontend reconnect URL).
"""

import json

import pytest

from src.server.services.runs.sse_producer import RunSSEProducer


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


class _EmptyGraph:
    """LangGraph stand-in: ``astream`` yields nothing.

    The handler still drains the post-loop blocks (credit_usage etc.) but
    those run only when token_callback / tool_tracker contributed records.
    With both None the handler emits the metadata frame and exits cleanly.
    """

    def astream(
        self, _input_state, config=None, stream_mode=None, subgraphs=None,
        durability=None,
    ):
        async def _gen():
            if False:
                yield  # pragma: no cover — empty async generator
            return

        return _gen()


def _parse_sse(chunk: str):
    """Return (event_name, parsed_data_dict) for a well-formed SSE chunk."""
    event_line = next(
        line for line in chunk.split("\n") if line.startswith("event: ")
    )
    data_line = next(
        line for line in chunk.split("\n") if line.startswith("data: ")
    )
    return event_line[len("event: "):], json.loads(data_line[len("data: "):])


async def _collect_stream(handler: RunSSEProducer):
    return [
        chunk
        async for chunk in handler.stream_workflow(
            _EmptyGraph(), input_state={}, config={}
        )
    ]


# ---------------------------------------------------------------------------
# Metadata frame contract
# ---------------------------------------------------------------------------


class TestMetadataFrameFirst:
    """Per-stream metadata announces the canonical run_id as event #1."""

    @pytest.mark.asyncio
    async def test_metadata_is_first_event_when_run_id_set(self):
        handler = RunSSEProducer(thread_id="t-1", run_id="r-abc")
        chunks = await _collect_stream(handler)
        assert chunks, "expected at least the metadata frame"
        event_name, data = _parse_sse(chunks[0])
        assert event_name == "metadata"
        assert data == {"thread_id": "t-1", "run_id": "r-abc"}

    @pytest.mark.asyncio
    async def test_report_source_rides_the_metadata_frame(self):
        """A report-back's stream names its report up front: a client that
        attaches live gets no user message to draw the report's row from."""
        source = {"kind": "task", "task_id": "k3", "display_id": "Task-k3", "failed": False}
        handler = RunSSEProducer(
            thread_id="t-1", run_id="r-abc", announced={"report_source": source}
        )
        event_name, data = _parse_sse((await _collect_stream(handler))[0])
        assert event_name == "metadata"
        assert data == {"thread_id": "t-1", "run_id": "r-abc", "report_source": source}

    @pytest.mark.asyncio
    async def test_market_target_rides_the_metadata_frame(self):
        """A target's wake names its notice up front: a client that attaches
        live reads no query row to draw the alert's row from."""
        notice = {"target_id": "tg-1", "notice": "hit", "symbol": "ACME", "kind": "above", "value": 205}
        handler = RunSSEProducer(
            thread_id="t-1", run_id="r-abc", turn_index=3, announced={"market_target": notice}
        )
        event_name, data = _parse_sse((await _collect_stream(handler))[0])
        assert event_name == "metadata"
        assert data == {
            "thread_id": "t-1",
            "run_id": "r-abc",
            "turn_index": 3,
            "market_target": notice,
        }

    @pytest.mark.asyncio
    async def test_metadata_names_the_admitted_turn(self):
        """The client stamps a live turn's bubbles from this value: a view
        that missed turns added elsewhere would otherwise count to the wrong
        one, and edit, regenerate and feedback would address it."""
        handler = RunSSEProducer(thread_id="t-1", run_id="r-abc", turn_index=45)
        chunks = await _collect_stream(handler)
        event_name, data = _parse_sse(chunks[0])
        assert event_name == "metadata"
        assert data == {"thread_id": "t-1", "run_id": "r-abc", "turn_index": 45}
