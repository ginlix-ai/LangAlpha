"""market_watch_update custom-stream events are forwarded as SSE frames.

MarketWatchMiddleware emits {"type": "market_watch_update", ...} via
runtime.stream_writer; the streaming handler's custom-event block must forward
it as its own SSE frame (previously unhandled types fell through and were
dropped). Drives WorkflowStreamHandler.stream_workflow with a minimal fake
graph — no heavyweight harness. Neutral placeholder tickers only.
"""

import json

import pytest


class _FakeGraph:
    """Minimal graph whose astream replays a fixed list of (ns, mode, data) events."""

    def __init__(self, events):
        self._events = events

    def astream(
        self, input_state, config=None, stream_mode=None, subgraphs=None, durability=None
    ):
        events = self._events

        async def _gen():
            for ev in events:
                yield ev

        return _gen()


def _parse_frames(raw_frames):
    """Parse raw SSE strings into {event, data} dicts."""
    parsed = []
    for frame in raw_frames:
        event_type = None
        data = None
        for line in frame.splitlines():
            if line.startswith("event: "):
                event_type = line[len("event: "):]
            elif line.startswith("data: "):
                data = json.loads(line[len("data: "):])
        parsed.append({"event": event_type, "data": data})
    return parsed


@pytest.mark.asyncio
async def test_market_watch_update_forwarded_to_sse():
    from src.server.handlers.streaming_handler import WorkflowStreamHandler

    handler = WorkflowStreamHandler(thread_id="t-1", run_id="r-1")
    custom_event = (
        (),
        "custom",
        {
            "type": "market_watch_update",
            "symbols": ["NVDA"],
            "content": "NVDA $123.45",
            "timestamp": 123.0,
        },
    )
    graph = _FakeGraph([custom_event])

    frames = [frame async for frame in handler.stream_workflow(graph, {}, {})]
    parsed = _parse_frames(frames)

    matches = [p for p in parsed if p["event"] == "market_watch_update"]
    assert len(matches) == 1
    data = matches[0]["data"]
    assert data["thread_id"] == "t-1"
    assert data["symbols"] == ["NVDA"]
    assert data["content"] == "NVDA $123.45"
    assert data["timestamp"] == 123.0

    # The stamp is transient (accumulate=False) — it streams live but must not
    # land in the persisted accumulator, matching the LIVE_ONLY ledger contract.
    accumulated = handler.get_sse_events() or []
    assert not any(e["event"] == "market_watch_update" for e in accumulated)
