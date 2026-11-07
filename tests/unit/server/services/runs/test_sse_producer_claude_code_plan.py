"""A Claude Code reply's plan notice (``claude_code_plan``) is forwarded as its own SSE frame."""

import json

import pytest


class _FakeGraph:
    def __init__(self, events):
        self._events = events

    def astream(self, input_state, config=None, stream_mode=None, subgraphs=None, durability=None):
        async def _gen():
            for ev in self._events:
                yield ev

        return _gen()


def _frames(raw):
    out = []
    for frame in raw:
        lines = dict(line.split(": ", 1) for line in frame.splitlines() if line.startswith(("event: ", "data: ")))
        out.append((lines.get("event"), json.loads(lines["data"]) if "data" in lines else None))
    return out


@pytest.mark.asyncio
async def test_a_plan_notice_reaches_the_client_with_its_window_and_reset():
    from src.server.services.runs.sse_producer import RunSSEProducer

    notice = {"reason": "billed_credits", "window": "five_hour", "until": "2026-11-06T20:40:00+00:00"}
    graph = _FakeGraph([((), "custom", {"type": "claude_code_plan", **notice})])
    frames = [f async for f in RunSSEProducer(thread_id="t-1", run_id="r-1").stream_workflow(graph, {}, {})]
    assert [data for event, data in _frames(frames) if event == "claude_code_plan"] == [
        {"thread_id": "t-1", "agent": "main", **notice},
    ]
