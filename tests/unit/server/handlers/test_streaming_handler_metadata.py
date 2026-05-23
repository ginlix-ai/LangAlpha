"""Tests for the per-stream ``metadata`` SSE frame produced by
``WorkflowStreamHandler.stream_workflow``.

Contract: ``run_id`` is REQUIRED at construction time. Every stream
announces that ``run_id`` as the FIRST yielded SSE chunk (``event:
metadata``). Frontend reconnect / demotion / steering boundary logic
latches onto this frame, so it must appear before any model output, and
the handler must not be constructible without a real ``run_id`` (a
``None`` would leak through the persisted SSE log and poison the
frontend reconnect URL).

The metadata frame is also written with ``accumulate=False`` so it does
NOT end up in the response's persisted SSE event log. (Replay paths
synthesise it on demand from the response row's ``conversation_response_id``.)
"""

import json

import pytest

from src.server.handlers.streaming_handler import WorkflowStreamHandler


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


class _EmptyGraph:
    """LangGraph stand-in: ``astream`` yields nothing.

    The handler still drains the post-loop blocks (credit_usage etc.) but
    those run only when token_callback / tool_tracker contributed records.
    With both None the handler emits the metadata frame and exits cleanly.
    """

    def astream(self, _input_state, config=None, stream_mode=None, subgraphs=None):
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


async def _collect_stream(handler: WorkflowStreamHandler):
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
        handler = WorkflowStreamHandler(thread_id="t-1", run_id="r-abc")
        chunks = await _collect_stream(handler)
        assert chunks, "expected at least the metadata frame"
        event_name, data = _parse_sse(chunks[0])
        assert event_name == "metadata"
        assert data == {"thread_id": "t-1", "run_id": "r-abc"}

    @pytest.mark.asyncio
    async def test_metadata_frame_is_not_persisted(self):
        """Persisted SSE log must NOT include the metadata frame — it's a
        per-connection identity hand-shake. The accumulator owns persistence,
        so a missing accumulated entry proves ``accumulate=False`` flowed
        through ``_format_sse_event``."""
        handler = WorkflowStreamHandler(thread_id="t-1", run_id="r-abc")
        chunks = await _collect_stream(handler)
        assert chunks  # sanity — metadata was emitted to the wire
        persisted = handler.get_sse_events()
        # Either no events were accumulated at all (None), or — if some
        # later credit event slipped through — metadata is not among them.
        if persisted is None:
            return
        kinds = [e["event"] for e in persisted]
        assert "metadata" not in kinds, (
            f"metadata frame must not be persisted; got accumulated events={kinds!r}"
        )

    def test_run_id_is_required_at_construction(self):
        """``run_id`` has no default — forgetting it is a TypeError, not a
        silently-malformed stream. This is the structural guarantee that
        replaces the old ``run_id=None`` suppression branch: there's no
        way for a caller to construct the handler without committing to a
        canonical per-turn identity."""
        with pytest.raises(TypeError):
            WorkflowStreamHandler(thread_id="t-1")  # type: ignore[call-arg]
